//! CLI command and subcommand implementations, dispatched from the interactive
//! loop in main.rs. Each item below is tagged `Command:` (a top-level verb like
//! `selftest`) or `Subcommand:` (a verb nested under a top-level one, like
//! `configure tcp bytes`), to distinguish them from ordinary helper functions.

use crate::core::{Config, DEFAULT_REMOTE, Metrics, PacketType, SqlState, StartGate};
use crate::metrics::ExternalSqlMetrics;
use crate::{cli_textout, configuration, core};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

/// Wall-clock length of `selftest`.
pub const SELFTEST_SECONDS: u64 = 3;

/// The command stack behind the arrow keys. Position 0 is the line being typed;
/// stepping back walks into older commands and stepping forward returns to
/// position 0, restoring whatever was half-typed there.
#[derive(Default)]
pub struct History {
    entries: Vec<String>,
    position: usize,
    draft: String,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a submitted command and returns to position 0. Blank lines and
    /// immediate repeats are not stacked.
    pub fn push(&mut self, line: &str) {
        let line = line.trim();
        if !line.is_empty() && self.entries.last().map(String::as_str) != Some(line) {
            self.entries.push(line.to_string());
        }
        self.position = 0;
        self.draft.clear();
    }

    pub fn step_back(&mut self, input: &mut String) {
        if self.position == self.entries.len() {
            return;
        }
        if self.position == 0 {
            self.draft = input.clone();
        }
        self.position += 1;
        *input = self.entries[self.entries.len() - self.position].clone();
    }

    pub fn step_forward(&mut self, input: &mut String) {
        match self.position {
            0 => {}
            1 => {
                self.position = 0;
                *input = std::mem::take(&mut self.draft);
            }
            _ => {
                self.position -= 1;
                *input = self.entries[self.entries.len() - self.position].clone();
            }
        }
    }
}

/// The set of clients this instance drives, each with its own id starting at 0.
pub struct Clients {
    clients: Mutex<Vec<configuration::ClientConfig>>,
}

impl Clients {
    pub fn new(mut clients: Vec<configuration::ClientConfig>) -> Self {
        if clients.is_empty() {
            clients.push(configuration::ClientConfig::new(0));
        }
        clients.sort_by_key(|client| client.id);
        Self {
            clients: Mutex::new(clients),
        }
    }
    pub fn list(&self) -> Vec<configuration::ClientConfig> {
        self.clients.lock().unwrap().clone()
    }
    pub fn enabled(&self) -> Vec<configuration::ClientConfig> {
        self.clients
            .lock()
            .unwrap()
            .iter()
            .filter(|client| client.enabled)
            .cloned()
            .collect()
    }
    pub fn any_enabled(&self) -> bool {
        self.clients.lock().unwrap().iter().any(|c| c.enabled)
    }
    /// Applies `change` to one client, reporting whether that id exists.
    pub fn update(&self, id: u64, change: impl FnOnce(&mut configuration::ClientConfig)) -> bool {
        let mut clients = self.clients.lock().unwrap();
        match clients.iter_mut().find(|client| client.id == id) {
            Some(client) => {
                change(client);
                true
            }
            None => false,
        }
    }
    pub fn get(&self, id: u64) -> Option<configuration::ClientConfig> {
        self.clients
            .lock()
            .unwrap()
            .iter()
            .find(|client| client.id == id)
            .cloned()
    }
    /// Adds a client using the lowest free id and returns it.
    pub fn add(&self) -> u64 {
        let mut clients = self.clients.lock().unwrap();
        let id = (0..).find(|id| !clients.iter().any(|c| c.id == *id)).unwrap();
        clients.push(configuration::ClientConfig::new(id));
        clients.sort_by_key(|client| client.id);
        id
    }
    pub fn remove(&self, id: u64) -> bool {
        let mut clients = self.clients.lock().unwrap();
        let before = clients.len();
        clients.retain(|client| client.id != id);
        clients.len() != before
    }
    pub fn replace(&self, clients: Vec<configuration::ClientConfig>) {
        *self.clients.lock().unwrap() = Clients::new(clients).list();
    }
}

/// Subcommand dispatcher for `client <id> <...>`; returns the reply text so the
/// terminal and the web CLI print the same wording.
pub fn client_command(clients: &Clients, id: u64, args: &[&str], log_dir: &std::path::Path) -> String {
    if clients.get(id).is_none() {
        return format!("no client {id}; use: client add");
    }
    match args {
        ["enable"] => {
            clients.update(id, |client| client.enabled = true);
            format!("client {id} enabled")
        }
        ["disable"] => {
            clients.update(id, |client| client.enabled = false);
            format!("client {id} disabled")
        }
        ["remote", host] => {
            clients.update(id, |client| client.remote = (*host).to_string());
            format!("client {id} remote set to {host}")
        }
        ["runtime", value] => match value.parse::<u64>() {
            Ok(value) => {
                clients.update(id, |client| client.runtime = Some(value));
                format!("client {id} runtime set to {value} seconds")
            }
            Err(_) => "runtime must be a non-negative integer".to_string(),
        },
        ["jitter", value] => match value.parse::<u64>() {
            Ok(value) => {
                clients.update(id, |client| client.jitter_millis = Some(value));
                format!("client {id} jitter set to {value} ms")
            }
            Err(_) => "jitter must be milliseconds".to_string(),
        },
        ["webrtc", value] => match *value {
            "on" | "enable" | "true" => {
                clients.update(id, |client| client.webrtc = Some(true));
                format!("client {id} sends WebRTC data channels")
            }
            "off" | "disable" | "false" => {
                clients.update(id, |client| client.webrtc = Some(false));
                format!("client {id} sends plain traffic")
            }
            "follow" => {
                clients.update(id, |client| client.webrtc = None);
                format!("client {id} follows the webrtc command")
            }
            _ => "client <id> webrtc: on | off | follow".to_string(),
        },
        ["http", "check", url] => client_http_check(log_dir, url),
        ["status"] => clients.get(id).map(|c| c.summary()).unwrap_or_default(),
        _ => CLIENT_USAGE.to_string(),
    }
}

pub const CLIENT_USAGE: &str = "client: list | add | delete <id> | <id> enable | <id> disable | <id> remote <ip> | <id> runtime <seconds> | <id> jitter <ms> | <id> webrtc <on|off|follow> | <id> http check <url> | <id> status";

/// Subcommand: client list — every client and its settings, one per line.
pub fn list_clients(clients: &Clients) -> String {
    clients
        .list()
        .iter()
        .map(|client| client.summary())
        .collect::<Vec<_>>()
        .join("\n")
}

pub const WEBRTC_USAGE: &str = "configure webrtc: enable | disable | channels <n> | label <name> | ordered <true|false> | status";

/// Subcommand dispatcher for `webrtc <...>`; returns the resulting settings line.
pub fn webrtc_command(
    settings: &Arc<Mutex<crate::webrtc::Settings>>,
    args: &[&str],
) -> Result<String, String> {
    let mut settings = settings.lock().unwrap();
    match args {
        ["enable"] => settings.enabled = true,
        ["disable"] => settings.enabled = false,
        ["channels", value] => {
            let value: u16 = value.parse().map_err(|_| "channels must be 1 or more")?;
            if value == 0 {
                return Err("channels must be 1 or more".into());
            }
            settings.channels = value;
        }
        ["label", value] => settings.label = (*value).to_string(),
        ["ordered", value] => {
            settings.ordered = match *value {
                "true" | "on" | "yes" => true,
                "false" | "off" | "no" => false,
                _ => return Err("ordered must be true or false".into()),
            }
        }
        ["status"] | [] => {}
        _ => return Err(WEBRTC_USAGE.into()),
    }
    Ok(settings.summary())
}

/// Subcommand: client runtime <seconds> | server runtime <seconds>
pub fn set_runtime(config: &Arc<Mutex<Config>>, client: bool, value: &str) -> String {
    match value.parse::<u64>() {
        Ok(value) => {
            if client {
                config.lock().unwrap().client_runtime = value;
            } else {
                config.lock().unwrap().server_runtime = value;
            }
            format!("runtime set to {value} seconds")
        }
        Err(_) => "runtime must be a non-negative integer".to_string(),
    }
}

/// Subcommand: admin add email <address> | admin delete email <address>
pub fn update_admin_email(
    path: &std::path::Path,
    config: &Arc<Mutex<Config>>,
    address: &str,
    add: bool,
) -> String {
    let mut config = config.lock().unwrap();
    if add {
        if !config.admin_emails.iter().any(|email| email == address) {
            config.admin_emails.push(address.to_string());
        }
    } else {
        config.admin_emails.retain(|email| email != address);
    }
    let mut document = configuration::load(path).unwrap_or_default();
    document.admin.emails = config.admin_emails.clone();
    match serde_yaml::to_string(&document) {
        Ok(contents) => match std::fs::write(path, contents) {
            Ok(()) => if add {
                "administrator email added"
            } else {
                "administrator email deleted"
            }
            .to_string(),
            Err(error) => format!("config write error: {error}"),
        },
        Err(error) => format!("config serialize error: {error}"),
    }
}

/// Helper for Subcommand: metrics enable
pub fn load_default_metrics_sink() -> Option<Arc<ExternalSqlMetrics>> {
    let path = configuration::path_near_executable()?;
    let file_config = configuration::load(&path).ok()?;
    file_config
        .metrics
        .sql
        .as_deref()
        .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
        .map(Arc::new)
}

/// Subcommand: configure save — writes the current in-memory configuration
/// (traffic settings, admin emails, external metrics target, SMTP server) to netmark.config.
pub fn save_configuration(
    path: &std::path::Path,
    config: &Config,
    clients: &Clients,
    external: &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    smtp: &Arc<Mutex<configuration::SmtpConfig>>,
    restapi: &Arc<Mutex<configuration::RestApiConfig>>,
    webrtc: &Arc<Mutex<crate::webrtc::Settings>>,
) -> Result<(), String> {
    let mut document = configuration::load(path).unwrap_or_default();
    document.traffic = crate::traffic_config_from(config);
    document.clients = clients.list();
    document.admin.emails = config.admin_emails.clone();
    document.metrics.sql = external
        .lock()
        .unwrap()
        .as_ref()
        .map(|sink| sink.connection_string().to_string());
    document.smtp = smtp.lock().unwrap().clone();
    document.restapi = restapi.lock().unwrap().clone();
    document.webrtc = crate::webrtc_config_from(&webrtc.lock().unwrap());
    let contents = serde_yaml::to_string(&document).map_err(|error| error.to_string())?;
    std::fs::write(path, contents).map_err(|error| error.to_string())
}

/// Subcommand: configure reset — discards in-session `configure` changes by
/// reloading netmark.config (or the built-in defaults if it is missing or incomplete).
pub fn reset_configuration(
    path: &std::path::Path,
    config: &Arc<Mutex<Config>>,
    clients: &Clients,
    external: &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    smtp: &Arc<Mutex<configuration::SmtpConfig>>,
    restapi: &Arc<Mutex<configuration::RestApiConfig>>,
    webrtc: &Arc<Mutex<crate::webrtc::Settings>>,
) {
    let file_config = configuration::load(path).unwrap_or_default();
    *config.lock().unwrap() = crate::config_from_file(&file_config);
    clients.replace(file_config.clients);
    *external.lock().unwrap() = file_config
        .metrics
        .sql
        .as_deref()
        .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
        .map(Arc::new);
    *smtp.lock().unwrap() = file_config.smtp;
    *restapi.lock().unwrap() = file_config.restapi;
    *webrtc.lock().unwrap() = crate::webrtc_settings(&file_config.webrtc);
}

/// Subcommand: admin smtp enabled | admin smtp disabled — turns SMTP use on or
/// off and persists the setting. Enabling first verifies the server answers.
pub fn set_smtp_enabled(
    path: &std::path::Path,
    smtp: &Arc<Mutex<configuration::SmtpConfig>>,
    enabled: bool,
) -> String {
    let server = smtp.lock().unwrap().server.clone();
    let mut lines = Vec::new();
    if enabled {
        let Some(server) = server.filter(|server| !server.trim().is_empty()) else {
            return "no SMTP server configured; use: configure smtp <host[:port]>".to_string();
        };
        match crate::smtp::check(&server) {
            Ok(result) => lines.push(format!(
                "SMTP check succeeded: {} ({} ms, {})",
                result.target, result.elapsed_millis, result.greeting
            )),
            Err(error) => {
                lines.push(format!("SMTP check failed: {error}"));
                lines.push("SMTP not enabled".to_string());
                return lines.join("\n");
            }
        }
    }
    smtp.lock().unwrap().enabled = enabled;
    let mut document = configuration::load(path).unwrap_or_default();
    document.smtp = smtp.lock().unwrap().clone();
    match serde_yaml::to_string(&document).map_err(|error| error.to_string()) {
        Ok(contents) => match std::fs::write(path, contents) {
            Ok(()) => lines.push(if enabled { "SMTP enabled" } else { "SMTP disabled" }.to_string()),
            Err(error) => lines.push(format!("config write error: {error}")),
        },
        Err(error) => lines.push(format!("config serialize error: {error}")),
    }
    lines.join("\n")
}

/// Subcommand: admin smtp status — reports the configured server, whether SMTP is
/// enabled, and whether the server currently answers.
pub fn smtp_status(smtp: &Arc<Mutex<configuration::SmtpConfig>>) -> String {
    let settings = smtp.lock().unwrap().clone();
    let state = if settings.enabled {
        "enabled"
    } else {
        "disabled"
    };
    match settings.server.as_deref().filter(|s| !s.trim().is_empty()) {
        None => format!("SMTP {state}, no server configured"),
        Some(server) => match crate::smtp::check(server) {
            Ok(result) => format!(
                "SMTP {state}, {} reachable ({} ms, {})",
                result.target, result.elapsed_millis, result.greeting
            ),
            Err(error) => format!("SMTP {state}, {server} unreachable: {error}"),
        },
    }
}

/// Helper for Subcommand: client http check <url> | monitor IP <url>
pub fn normalize_http_target(target: &str) -> String {
    if target.starts_with("http://") || target.starts_with("https://") {
        target.to_string()
    } else {
        format!("http://{target}")
    }
}

/// The transports `selftest` exercises, in this order.
pub const SELFTEST_PROTOCOLS: [PacketType; 4] = [
    PacketType::Udp,
    PacketType::Tcp,
    PacketType::Sctp,
    PacketType::Ip,
];

/// Whether this host can carry a transport at all, so `selftest` can skip one
/// with a reason (no SCTP in the kernel, no CAP_NET_RAW) instead of failing.
pub fn selftest_availability(packet_type: PacketType) -> Result<(), String> {
    match packet_type {
        PacketType::Sctp => crate::sctp::availability(),
        PacketType::Ip => crate::rawip::availability(),
        PacketType::Tcp | PacketType::Udp => Ok(()),
    }
}

/// Command: selftest — sends traffic to localhost over every protocol in turn,
/// each for a few seconds, and stops automatically. Returns the line announcing
/// the sequence; each leg and the completion line are printed as they happen.
pub fn run_selftest(
    config: &Arc<Mutex<Config>>,
    stopping: &Arc<AtomicBool>,
    running: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    sql: &Arc<SqlState>,
    external: &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    log_dir: &std::path::Path,
) -> String {
    if running.swap(true, Ordering::Relaxed) {
        return "already running".to_string();
    }
    let mut planned = Vec::new();
    let mut skipped = Vec::new();
    let switches = config.lock().unwrap().protocols;
    for packet_type in SELFTEST_PROTOCOLS {
        if !switches.enabled(packet_type) {
            skipped.push(format!("{} (disabled)", packet_type.as_str()));
            continue;
        }
        match selftest_availability(packet_type) {
            Ok(()) => planned.push(packet_type),
            Err(error) => skipped.push(format!("{} ({error})", packet_type.as_str())),
        }
    }
    if planned.is_empty() {
        running.store(false, Ordering::Relaxed);
        return format!("selftest cannot run: no transport is available; {}", skipped.join(", "));
    }
    let announcement = format!(
        "selftest running {}{}",
        planned
            .iter()
            .map(|packet_type| packet_type.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        if skipped.is_empty() {
            String::new()
        } else {
            format!("; skipping {}", skipped.join(", "))
        }
    );
    let stop = Arc::clone(stopping);
    let state = Arc::clone(running);
    let sql_state = Arc::clone(sql);
    let state_metrics = Arc::clone(metrics);
    let state_external = Arc::clone(external);
    let state_config = Arc::clone(config);
    let logs = log_dir.to_path_buf();
    thread::spawn(move || {
        let mut failures = 0;
        for (index, packet_type) in planned.iter().enumerate() {
            if index > 0 {
                // Let the previous leg's sockets close before the next one binds.
                thread::sleep(Duration::from_secs(1));
            }
            if !selftest_leg(
                *packet_type,
                &state_config,
                &stop,
                &state_metrics,
                &sql_state,
                &state_external,
                &logs,
            ) {
                failures += 1;
            }
        }
        state.store(false, Ordering::Relaxed);
        cli_textout::raw("\r\n");
        cli_textout::line(format!(
            "selftest completed {} transports, {failures} failed",
            planned.len()
        ));
    });
    announcement
}

/// One `selftest` leg: a short localhost run over a single transport. Returns
/// whether the run passed.
fn selftest_leg(
    packet_type: PacketType,
    config: &Arc<Mutex<Config>>,
    stopping: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    sql: &Arc<SqlState>,
    external: &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    log_dir: &std::path::Path,
) -> bool {
    let run_id = sql.next_run_id(1);
    sql.set_role(core::ROLE_BOTH);
    {
        let mut config = config.lock().unwrap();
        config.packet_type = packet_type;
        config.udp_rate = 1;
        config.udp_packet_size = 1024;
        config.client_runtime = SELFTEST_SECONDS;
        config.server_runtime = SELFTEST_SECONDS;
    }
    stopping.store(false, Ordering::Relaxed);
    metrics.snapshot();
    metrics.reset_run();
    let gate = Arc::new(StartGate::new());
    crate::write_run_event(log_dir, run_id, "Starting");
    core::spawn_server(
        Arc::clone(config),
        Arc::clone(&gate),
        Arc::clone(stopping),
        Arc::clone(metrics),
        log_dir.to_path_buf(),
    );
    core::spawn_client(
        Arc::clone(config),
        Arc::clone(&gate),
        Arc::clone(stopping),
        Arc::clone(metrics),
        DEFAULT_REMOTE.to_string(),
        log_dir.to_path_buf(),
        0,
    );
    core::spawn_debrief_responder(
        Arc::clone(stopping),
        Arc::clone(metrics),
        Arc::clone(sql),
        log_dir.to_path_buf(),
    );
    sql.start_run(run_id);
    gate.start();
    cli_textout::raw("\r\n");
    cli_textout::line(format!(
        "selftest {} started run {run_id}",
        packet_type.as_str()
    ));
    thread::sleep(Duration::from_secs(SELFTEST_SECONDS));
    stopping.store(true, Ordering::Relaxed);
    cli_textout::raw("\r\n");
    let mut outcome = crate::evaluate_run(
        metrics,
        &config.lock().unwrap(),
        Some(Duration::from_secs(SELFTEST_SECONDS)),
    );
    match core::run_debrief(
        DEFAULT_REMOTE,
        run_id,
        packet_type,
        metrics,
        sql,
        log_dir,
    ) {
        Ok(debrief) => {
            cli_textout::line(debrief.summary());
            if let Some(reason) = debrief.mismatch_reason() {
                outcome.add_failure(format!("debrief mismatch: {reason}"));
            }
        }
        Err(error) => {
            cli_textout::line(&error);
            outcome.add_failure(error);
        }
    }
    sql.complete_run(run_id, &outcome.summary());
    crate::record_final_metrics(external.lock().unwrap().as_ref(), run_id, metrics, &outcome);
    crate::write_run_event(log_dir, run_id, "Completed");
    crate::write_log_line(log_dir, &outcome.report_line(run_id));
    cli_textout::line(format!(
        "selftest {} {}",
        packet_type.as_str(),
        outcome.report_line(run_id)
    ));
    outcome.result == "ok"
}

/// Command: benchmark duration <seconds> — floods the remote server with TCP and
/// reports bandwidth. Returns the summary line.
pub fn run_benchmark(remote: &str, seconds: u64, sql: &SqlState, log_dir: &std::path::Path) -> String {
    let run_id = sql.next_run_id(1);
    crate::write_run_event(log_dir, run_id, "Starting");
    sql.start_run(run_id);
    let mut stream = match TcpStream::connect(format!("{remote}:9000")) {
        Ok(stream) => stream,
        Err(error) => {
            sql.complete_run(
                run_id,
                &core::RunSummary {
                    result: "error",
                    sent_bytes: 0,
                    received_bytes: 0,
                    sent_bytes_per_second: 0,
                    received_bytes_per_second: 0,
                    failure_reason: Some(&error.to_string()),
                    protocol: PacketType::Tcp.as_str(),
                    detail: core::RunDetail::default(),
                },
            );
            crate::write_run_event(log_dir, run_id, "Completed");
            return format!("benchmark run {run_id} failed: {error}");
        }
    };
    let packet = [0u8; 64 * 1024];
    let started = Instant::now();
    let mut bytes = 0u64;
    while started.elapsed() < Duration::from_secs(seconds) {
        if stream.write_all(&packet).is_err() {
            break;
        }
        bytes += packet.len() as u64;
    }
    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let bytes_per_second = bytes.saturating_mul(1000) / elapsed_ms;
    if let Ok(mut log) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("client.log"))
    {
        let _ = writeln!(
            log,
            "{} TCP benchmark run={} bytes={} elapsed_ms={} bytes_per_second={}",
            core::timestamp(),
            run_id,
            bytes,
            elapsed_ms,
            bytes_per_second
        );
    }
    sql.complete_run(
        run_id,
        &core::RunSummary {
            result: if bytes > 0 { "ok" } else { "error" },
            sent_bytes: bytes,
            received_bytes: 0,
            sent_bytes_per_second: bytes_per_second,
            received_bytes_per_second: 0,
            failure_reason: if bytes > 0 { None } else { Some("no bytes sent") },
            protocol: PacketType::Tcp.as_str(),
            detail: core::RunDetail {
                sent_tcp_bytes: bytes,
                ..core::RunDetail::default()
            },
        },
    );
    crate::write_run_event(log_dir, run_id, "Completed");
    let line = format!(
        "benchmark run {run_id}: {bytes} bytes in {elapsed_ms} ms up={bytes_per_second} bytes/sec down=0 bytes/sec"
    );
    crate::write_log_line(log_dir, &line);
    line
}

/// Subcommand dispatcher for `configure <...>`: tcp bytes, tcp/udp jitter,
/// udp packetsize, type, and bandwidth limit.
pub fn configure(config: &Arc<Mutex<Config>>, args: &[&str]) -> Result<(), String> {
    if let ["jitter", value] = args {
        let value = value.parse().map_err(|_| "jitter must be milliseconds")?;
        config.lock().unwrap().jitter_millis = value;
        return Ok(());
    }
    if let [protocol, "jitter", value] = args {
        let value = value.parse().map_err(|_| "jitter must be milliseconds")?;
        match *protocol {
            "tcp" => config.lock().unwrap().client_jitter_millis = value,
            "udp" => config.lock().unwrap().client_jitter_millis = value,
            _ => return Err("use configure tcp jitter <ms> or configure udp jitter <ms>".into()),
        }
        return Ok(());
    }
    if let ["tcp", "bytes", value] = args {
        let value = value
            .parse()
            .map_err(|_| "TCP bytes/sec must be positive")?;
        if value == 0 {
            return Err("TCP bytes/sec must be positive".into());
        }
        config.lock().unwrap().tcp_bytes_per_second = value;
        return Ok(());
    }
    if let ["tcp", "window", value] = args {
        let value = value
            .parse()
            .map_err(|_| "TCP window must be a number of bytes (0 uses the OS default)")?;
        config.lock().unwrap().tcp_window_size = value;
        return Ok(());
    }
    if let ["udp", "packetsize", value] = args {
        let value = value
            .parse()
            .map_err(|_| "UDP packet size must be at least 8")?;
        if value < 8 {
            return Err("UDP packet size must be at least 8".into());
        }
        config.lock().unwrap().udp_packet_size = value;
        return Ok(());
    }
    if let ["udp_rate", value] = args {
        let value = value
            .parse()
            .map_err(|_| "UDP rate must be packets per second")?;
        config.lock().unwrap().udp_rate = value;
        return Ok(());
    }
    if let ["type", value] = args {
        let packet_type = PacketType::parse(value).ok_or("type must be tcp, sctp, udp or ip")?;
        let mut config = config.lock().unwrap();
        if !config.protocols.enabled(packet_type) {
            return Err(format!(
                "{value} is disabled; turn it on with: configure {value} enable"
            ));
        }
        config.packet_type = packet_type;
        return Ok(());
    }
    if let ["bandwidth", "limit", value] = args {
        let value = value
            .parse()
            .map_err(|_| "bandwidth limit must be bytes/sec (0 disables the check)")?;
        config.lock().unwrap().limit_bytes_per_second = value;
        return Ok(());
    }
    Err(CONFIGURE_USAGE.into())
}

/// Subcommand: configure <tcp|sctp|udp|ip> enable | disable — a disabled
/// transport cannot be selected, started or selftested. Returns the line to print.
pub fn set_protocol_enabled(
    config: &Arc<Mutex<Config>>,
    protocol: &str,
    enabled: bool,
) -> Result<String, String> {
    let packet_type = PacketType::parse(protocol)
        .ok_or_else(|| "protocols are tcp, sctp, udp, ip and webrtc".to_string())?;
    let mut config = config.lock().unwrap();
    config.protocols.set(packet_type, enabled);
    let state = if enabled { "enabled" } else { "disabled" };
    let mut line = format!("{} {state}", packet_type.as_str());
    // The selected transport cannot stay selected once it is turned off.
    if !enabled && config.packet_type == packet_type {
        if let Some(fallback) = [
            PacketType::Tcp,
            PacketType::Udp,
            PacketType::Sctp,
            PacketType::Ip,
        ]
        .into_iter()
        .find(|candidate| config.protocols.enabled(*candidate))
        {
            config.packet_type = fallback;
            line.push_str(&format!("; transport is now {}", fallback.as_str()));
        } else {
            line.push_str("; no transport is enabled, so runs cannot start");
        }
    }
    Ok(line)
}

/// The table `configure protocols` prints: every transport, whether it is
/// enabled, whether this host can carry it and its live counters.
pub fn protocols_table(config: &Arc<Mutex<Config>>, webrtc: &crate::webrtc::Settings) -> String {
    let config = config.lock().unwrap();
    let mut rows = Vec::new();
    for packet_type in [
        PacketType::Tcp,
        PacketType::Sctp,
        PacketType::Udp,
        PacketType::Ip,
    ] {
        let state = if config.protocols.enabled(packet_type) {
            "enabled"
        } else {
            "disabled"
        };
        let selected = if config.packet_type == packet_type {
            ", selected"
        } else {
            ""
        };
        let host = match selftest_availability(packet_type) {
            Ok(()) => "available on this host".to_string(),
            Err(error) => format!("unavailable: {error}"),
        };
        rows.push(vec![
            packet_type.as_str().to_string(),
            format!("{state}{selected}, {host}"),
        ]);
    }
    rows.push(vec!["webrtc".to_string(), webrtc.summary()]);
    cli_textout::table_lines(&rows, &[16, 80]).join("\n")
}

/// Subcommand: configure limits — per-protocol thresholds that fail a run.
/// Returns the line or table to print.
pub fn configure_limits(config: &Arc<Mutex<Config>>, args: &[&str]) -> Result<String, String> {
    match args {
        [] | ["status"] => Ok(limits_table(&config.lock().unwrap().limits, None)),
        [protocol] | [protocol, "status"] => {
            let packet_type = parse_limit_protocol(protocol)?;
            Ok(limits_table(
                &config.lock().unwrap().limits,
                Some(packet_type),
            ))
        }
        [protocol, "clear"] => {
            let packet_type = parse_limit_protocol(protocol)?;
            *config.lock().unwrap().limits.get_mut(packet_type) = crate::core::Limits::default();
            Ok(format!("{} limits cleared", packet_type.as_str()))
        }
        [protocol, parameter, value] => {
            let packet_type = parse_limit_protocol(protocol)?;
            if !crate::core::Limits::supports(packet_type, parameter) {
                return Err(format!(
                    "{} has no limit called \"{parameter}\"; it accepts: {}",
                    packet_type.as_str(),
                    limit_parameters(packet_type).join(", ")
                ));
            }
            let value: u64 = value
                .parse()
                .map_err(|_| format!("{parameter} must be a whole number (0 removes the limit)"))?;
            config
                .lock()
                .unwrap()
                .limits
                .get_mut(packet_type)
                .set(parameter, value);
            Ok(if value == 0 {
                format!("{} {parameter} limit removed", packet_type.as_str())
            } else {
                format!("{} {parameter} limit set to {value}", packet_type.as_str())
            })
        }
        _ => Err(LIMITS_USAGE.into()),
    }
}

fn parse_limit_protocol(protocol: &str) -> Result<PacketType, String> {
    PacketType::parse(protocol)
        .ok_or_else(|| "limits are set per protocol: tcp, sctp, udp or ip".to_string())
}

/// The limit parameters that mean something for a transport.
pub fn limit_parameters(packet_type: PacketType) -> Vec<&'static str> {
    crate::core::LIMIT_PARAMETERS
        .into_iter()
        .filter(|parameter| crate::core::Limits::supports(packet_type, parameter))
        .collect()
}

/// The table `configure limits [protocol]` prints: every limit, with `not set`
/// where a limit is disabled.
pub fn limits_table(limits: &crate::core::LimitSet, only: Option<PacketType>) -> String {
    let protocols: Vec<PacketType> = match only {
        Some(packet_type) => vec![packet_type],
        None => vec![
            PacketType::Tcp,
            PacketType::Sctp,
            PacketType::Udp,
            PacketType::Ip,
        ],
    };
    let mut rows = Vec::new();
    for packet_type in protocols {
        let values = limits.get(packet_type);
        for parameter in limit_parameters(packet_type) {
            let value = values.get(parameter).unwrap_or(0);
            rows.push(vec![
                format!("{} {parameter}", packet_type.as_str()),
                if value == 0 {
                    "not set".to_string()
                } else {
                    value.to_string()
                },
            ]);
        }
    }
    cli_textout::table_lines(&rows, &[40, 40]).join("\n")
}

pub const LIMITS_USAGE: &str = "configure limits: [<tcp|sctp|udp|ip>] status | <tcp|sctp|udp|ip> <parameter> <value> | <tcp|sctp|udp|ip> clear";

pub const CONFIGURE_USAGE: &str = "configure: metrics <connection> | save | reset | smtp <host[:port]> | type <tcp|sctp|udp|ip> | tcp bytes <bytes/sec> | tcp window <bytes> | tcp jitter <ms> | tcp maxjitter <ms> | udp_rate <packets/sec> | udp packetsize <bytes> | udp jitter <ms> | udp max jitter <ms> | bandwidth limit <bytes/sec> | limits <tcp|sctp|udp|ip> <parameter> <value>";

/// Subcommand: client http check <url>
pub fn client_http_check(log_dir: &std::path::Path, url: &str) -> String {
    let url = normalize_http_target(url);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => return format!("HTTP client error: {error}"),
    };
    let started = Instant::now();
    match client
        .get(&url)
        .send()
        .and_then(|response| response.error_for_status())
    {
        Ok(response) => match response.bytes() {
            Ok(body) => {
                let elapsed = started.elapsed().as_millis();
                if let Ok(mut log) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_dir.join("client.log"))
                {
                    let _ = writeln!(
                        log,
                        "{} HTTP {} tcp_connection_ms={} bytes={}",
                        core::timestamp(),
                        url,
                        elapsed,
                        body.len()
                    );
                }
                format!(
                    "HTTP check succeeded: {url} ({elapsed} ms, {} bytes)",
                    body.len()
                )
            }
            Err(error) => format!("HTTP body error: {error}"),
        },
        Err(error) => format!("HTTP check failed: {error}"),
    }
}

/// Everything `status` needs to know about the rest of the process.
pub struct StatusContext<'a> {
    pub running: bool,
    pub elapsed: Option<Duration>,
    pub run_id: u64,
    pub server_enabled: bool,
    pub clients: &'a Clients,
    pub webrtc: &'a crate::webrtc::Settings,
    pub packet_type: PacketType,
    /// running, monitor id, calls, successes, failures
    pub monitor: (bool, u64, u64, u64, u64),
    pub metrics_sql: String,
    pub restapi: String,
    /// Where the web interface is listening, from [`web_server_status`].
    pub web_server: String,
    pub smtp: bool,
    /// Which transports `configure <protocol> enable|disable` allows.
    pub protocols: crate::core::ProtocolSwitches,
}

/// Where the web interface (and with it the REST API) is listening. Printed when
/// netmark starts and shown by `status`, so the port never has to be guessed.
pub fn web_server_status(address: &str, configured: &str) -> String {
    match address.rsplit_once(':') {
        Some((_, port)) if !address.is_empty() => {
            format!("listening on http://{address} (port {port})")
        }
        _ => format!("not listening (configured {configured}; start it with: restapi enable)"),
    }
}

/// Command: status — run-scoped totals, so the counts correlate with the final
/// report rather than with the periodically-reset live display counters.
pub fn status_rows(metrics: &Metrics, context: &StatusContext<'_>) -> Vec<Vec<String>> {
    let values = metrics.run_totals();
    let (lost, order) = metrics.udp_status();
    let (webrtc_sent, webrtc_received, webrtc_invalid) = metrics.webrtc_counts();
    let (mss, mtu, window_size) = metrics.tcp_transport();
    let elapsed = context.elapsed.unwrap_or_default();
    let sent = metrics.run_sent_total();
    let received = metrics.run_received_total();
    let (monitor_on, monitor_id, calls, successes, failures) = context.monitor;
    let mut rows = vec![
        vec![
            "Traffic".into(),
            format!(
                "{} ({})",
                if context.running { "running" } else { "stopped" },
                context.packet_type.as_str()
            ),
        ],
        vec![
            "Run".into(),
            if context.run_id == 0 {
                "none yet".to_string()
            } else {
                format!("{} ({} s elapsed)", context.run_id, elapsed.as_secs())
            },
        ],
        vec![
            "Bandwidth up".into(),
            format!("{} bytes/sec", core::bandwidth(sent, elapsed)),
        ],
        vec![
            "Bandwidth down".into(),
            format!("{} bytes/sec", core::bandwidth(received, elapsed)),
        ],
        vec![
            "Sent".into(),
            format!(
                "{} bytes  (TCP/SCTP {}, UDP {}, IP {})",
                sent, values[1], values[3], values[9]
            ),
        ],
        vec![
            "Received".into(),
            format!(
                "{} bytes  (TCP/SCTP {}, UDP {}, IP {})",
                received, values[5], values[7], values[11]
            ),
        ],
        vec![
            "UDP loss".into(),
            format!("{lost} lost, {order} out of order"),
        ],
        vec![
            "Jitter".into(),
            format!(
                "TCP {} ms, UDP {} ms",
                metrics.tcp_jitter_millis(),
                metrics.udp_jitter_millis()
            ),
        ],
        vec!["Server".into(), enabled_word(context.server_enabled).into()],
    ];
    for client in context.clients.list() {
        rows.push(vec![format!("Client {}", client.id), client.summary()]);
    }
    rows.push(vec!["WebRTC".into(), context.webrtc.summary()]);
    if context.webrtc.enabled || webrtc_sent > 0 || webrtc_received > 0 {
        rows.push(vec![
            "WebRTC messages".into(),
            format!("{webrtc_sent} sent, {webrtc_received} received, {webrtc_invalid} invalid"),
        ]);
    }
    rows.push(vec![
        "Monitor".into(),
        format!(
            "{} id {monitor_id}, {calls} calls, {successes} ok, {failures} failed",
            if monitor_on { "on" } else { "off" }
        ),
    ]);
    rows.push(vec!["Metrics SQL".into(), context.metrics_sql.clone()]);
    rows.push(vec![
        "TCP transport".into(),
        if mss == 0 && mtu == 0 && window_size == 0 {
            "not available (TCP not connected)".into()
        } else {
            format!("MSS {mss} bytes, MTU {mtu} bytes, window {window_size} bytes")
        },
    ]);
    rows.push(vec!["REST API".into(), context.restapi.clone()]);
    rows.push(vec!["Web server".into(), context.web_server.clone()]);
    rows.push(vec![
        "SCTP".into(),
        format!(
            "{} ({})",
            if context.packet_type == PacketType::Sctp {
                "selected transport"
            } else {
                "not selected"
            },
            crate::restapi::sctp_status()
        ),
    ]);
    rows.push(vec![
        "Protocols".into(),
        [
            PacketType::Tcp,
            PacketType::Sctp,
            PacketType::Udp,
            PacketType::Ip,
        ]
        .into_iter()
        .map(|packet_type| {
            format!(
                "{} {}",
                packet_type.as_str(),
                if context.protocols.enabled(packet_type) {
                    "enabled"
                } else {
                    "disabled"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", "),
    ]);
    rows.push(vec!["SMTP".into(), enabled_word(context.smtp).into()]);
    rows
}

fn enabled_word(enabled: bool) -> &'static str {
    if enabled { "enabled" } else { "disabled" }
}

/// Command: sctp — the detailed help page for the SCTP transport, shown by the
/// CLI as a table and by the web CLI as text.
pub fn sctp_help_rows() -> Vec<Vec<String>> {
    vec![
        vec![
            "what it is".into(),
            "SCTP (IP protocol 132) as a one-to-one stream, framed and counted exactly like TCP".into(),
        ],
        vec![
            "kernel support".into(),
            format!(
                "{}; Linux needs the sctp module (modprobe sctp)",
                crate::restapi::sctp_status()
            ),
        ],
        vec![
            "select it".into(),
            "configure type sctp (or packet_type: sctp in a test profile)".into(),
        ],
        vec![
            "turn it on or off".into(),
            "configure sctp enable | configure sctp disable; a disabled transport cannot be selected, started or selftested".into(),
        ],
        vec![
            "run it".into(),
            "server enable and/or client <id> enable, then start; both roles use port 9000".into(),
        ],
        vec![
            "stop it".into(),
            "stop ends the association, debriefs the server over protocol=sctp and prints transport=sctp".into(),
        ],
        vec![
            "pacing".into(),
            "configure tcp bytes <bytes/sec> paces SCTP too; udp_rate and packet size do not apply".into(),
        ],
        vec![
            "jitter limits".into(),
            "SCTP shares the TCP jitter budget: configure tcp jitter <ms> and configure tcp maxjitter <ms>".into(),
        ],
        vec![
            "counters".into(),
            "SCTP bytes are reported in the TCP/SCTP columns of status, debriefs and metrics".into(),
        ],
        vec![
            "limits".into(),
            "IPv4 destinations only, one association per client, and no multi-homing or multi-streaming".into(),
        ],
        vec![
            "status".into(),
            "status shows the SCTP row here and on the web page, live while a run is going".into(),
        ],
    ]
}

pub fn print_sctp_help(
    stdout_guard: &Arc<Mutex<()>>,
    output: &Arc<Mutex<std::process::ChildStdin>>,
) {
    let mut output = output.lock().unwrap();
    let _ = writeln!(output, "HIDE");
    let _ = output.flush();
    let _guard = stdout_guard.lock().unwrap();
    cli_textout::table(&sctp_help_rows(), &[16, 84]);
}

pub fn print_status(metrics: &Metrics, context: &StatusContext<'_>) {
    cli_textout::table(&status_rows(metrics, context), &[16, 80]);
}

/// Subcommand: monitor history — monitor events and alarms, newest last.
pub fn show_monitor_history(log_dir: &std::path::Path) -> String {
    let mut lines = Vec::new();
    for name in ["netmark.log", "alarm.log"] {
        if let Ok(contents) = std::fs::read_to_string(log_dir.join(name)) {
            for line in contents.lines() {
                if name == "alarm.log"
                    || line.contains("Monitor started")
                    || line.contains("Monitor stopped")
                {
                    lines.push(line.to_string());
                }
            }
        }
    }
    lines.join("\n")
}

/// Command: help [<command> ...]
pub fn print_help(
    stdout_guard: &Arc<Mutex<()>>,
    output: &Arc<Mutex<std::process::ChildStdin>>,
    topic: &[&str],
) {
    let mut output = output.lock().unwrap();
    let _ = writeln!(output, "HIDE");
    let _ = output.flush();
    let _guard = stdout_guard.lock().unwrap();
    match help_for(topic) {
        Ok(rows) => cli_textout::table(&rows, &[36, 64]),
        Err(message) => cli_textout::line(message),
    }
}

/// `help` with no arguments lists every command; with arguments it shows the
/// rows for that command, so `help start`, `help configure tcp` and
/// `help client 0 remote` all answer with the same table the full list uses.
pub fn help_for(topic: &[&str]) -> Result<Vec<Vec<String>>, String> {
    if topic.is_empty() {
        return Ok(help_rows());
    }
    // The transport has a page of its own, which is more useful than its one line.
    if topic == ["sctp"] || topic == ["configure", "sctp"] {
        return Ok(sctp_help_rows());
    }
    // "help configure limits <protocol>" lists the parameters that protocol has.
    if let ["limits", protocol] | ["configure", "limits", protocol] = topic
        && let Some(packet_type) = PacketType::parse(protocol)
    {
        return Ok(limit_parameter_help_rows(packet_type));
    }
    let rows: Vec<Vec<String>> = help_rows()
        .into_iter()
        .filter(|row| row[0].split('|').any(|label| label_matches(label, topic)))
        .collect();
    if rows.is_empty() {
        Err(format!(
            "no help for \"{}\"; type 'help' for every command",
            topic.join(" ")
        ))
    } else {
        Ok(rows)
    }
}

/// The parameters one protocol accepts under `configure limits`, with what each
/// one means, so the operator does not have to guess.
pub fn limit_parameter_help_rows(packet_type: PacketType) -> Vec<Vec<String>> {
    let protocol = packet_type.as_str();
    limit_parameters(packet_type)
        .into_iter()
        .map(|parameter| {
            let meaning = match parameter {
                "min-sent-bytes" => "fail below this many bytes sent in the run",
                "min-received-bytes" => "fail below this many bytes received in the run",
                "min-sent-bytes-per-second" => "fail below this sending throughput",
                "min-received-bytes-per-second" => "fail below this receiving throughput",
                "max-jitter-millis" => "fail above this measured jitter",
                "max-lost-packets" => "fail above this many lost packets",
                _ => "fail above this many out-of-order packets",
            };
            vec![
                format!("configure limits {protocol} {parameter} <value>"),
                meaning.to_string(),
            ]
        })
        .collect()
}

/// A help row answers a topic when its command words match word for word.
/// Placeholders such as `<id>` match whatever the operator typed there.
fn label_matches(label: &str, topic: &[&str]) -> bool {
    let words: Vec<&str> = label.split_whitespace().collect();
    topic.iter().enumerate().all(|(index, word)| {
        words.get(index).is_some_and(|candidate| {
            candidate.starts_with('<') || candidate.eq_ignore_ascii_case(word)
        })
    })
}

pub fn help_rows() -> Vec<Vec<String>> {
    vec![
        vec!["server".into(), "receiving side of traffic".into()],
        vec!["server enable".into(), "enable server traffic".into()],
        vec!["server disable".into(), "disable server traffic".into()],
        vec![
            "server runtime <seconds>".into(),
            "limit server runtime; zero is unlimited".into(),
        ],
        vec!["client".into(), "sending side of traffic".into()],
        vec![
            "client list".into(),
            "show every client and its settings".into(),
        ],
        vec![
            "client add | client delete <id>".into(),
            "add a client with the next free id, or remove one".into(),
        ],
        vec![
            "client <id> enable | disable".into(),
            "enable or disable one client; <id> starts at 0".into(),
        ],
        vec![
            "client <id> remote <ip>".into(),
            "set that client's destination".into(),
        ],
        vec![
            "client <id> runtime <seconds>".into(),
            "limit that client's runtime; zero is unlimited".into(),
        ],
        vec![
            "client <id> jitter <ms>".into(),
            "set that client's send jitter".into(),
        ],
        vec![
            "client <id> webrtc on | off | follow".into(),
            "connect the WebRTC layer to that client alone; follow (the default) uses `webrtc enable`".into(),
        ],
        vec![
            "client <id> http check <url>".into(),
            "load one HTTP or HTTPS page".into(),
        ],
        vec![
            "client <id> status".into(),
            "show one client's settings".into(),
        ],
        vec![
            "selftest".into(),
            "run udp, tcp, sctp and ip to localhost in turn and stop automatically".into(),
        ],
        vec![
            "benchmark duration <seconds>".into(),
            "flood the remote server with TCP and report bandwidth".into(),
        ],
        vec![
            "configure type <tcp|sctp|udp|ip>".into(),
            "pick the transport; SCTP needs kernel support and raw IP needs CAP_NET_RAW".into(),
        ],
        vec![
            "configure sctp".into(),
            "detailed SCTP help: kernel support, how to select, run and stop it".into(),
        ],
        vec![
            "configure sctp status".into(),
            "whether this kernel can open an SCTP socket".into(),
        ],
        vec![
            "configure protocols".into(),
            "every transport: enabled or disabled, selected, host support and WebRTC".into(),
        ],
        vec![
            "configure <tcp|sctp|udp|ip> enable".into(),
            "allow the transport to be selected, started and selftested".into(),
        ],
        vec![
            "configure <tcp|sctp|udp|ip> disable".into(),
            "turn the transport off; a disabled transport cannot run".into(),
        ],
        vec![
            "configure tcp bytes <bytes/sec>".into(),
            "set TCP bytes per second".into(),
        ],
        vec![
            "configure tcp window <bytes>".into(),
            "set TCP send/receive window; 0 uses the OS default".into(),
        ],
        vec![
            "configure tcp jitter <ms>".into(),
            "set TCP send jitter".into(),
        ],
        vec![
            "configure tcp maxjitter <ms>".into(),
            "fail a run above this measured TCP jitter".into(),
        ],
        vec![
            "configure udp_rate <packets/sec>".into(),
            "set UDP packets per second; also paces raw IP".into(),
        ],
        vec![
            "configure udp packetsize <bytes>".into(),
            "set UDP packet size".into(),
        ],
        vec![
            "configure udp jitter <ms>".into(),
            "set UDP send jitter".into(),
        ],
        vec![
            "configure udp max jitter <ms>".into(),
            "fail a run above this measured UDP jitter".into(),
        ],
        vec![
            "configure bandwidth limit <bytes/sec>".into(),
            "minimum acceptable throughput; 0 disables the check".into(),
        ],
        vec![
            "configure limits".into(),
            "show every per-protocol limit that can fail a run".into(),
        ],
        vec![
            "configure limits <tcp|sctp|udp|ip> status".into(),
            "show the limits for one protocol".into(),
        ],
        vec![
            "configure limits <tcp|sctp|udp|ip> <parameter> <value>".into(),
            "fail a run when the protocol misses this limit; 0 removes it".into(),
        ],
        vec![
            "configure limits <tcp|sctp|udp|ip> clear".into(),
            "remove every limit for one protocol".into(),
        ],
        vec![
            "configure webrtc enable | disable".into(),
            "wrap traffic in WebRTC data-channel frames for every client that follows".into(),
        ],
        vec![
            "configure webrtc channels <n>".into(),
            "number of data channels to spread messages over".into(),
        ],
        vec!["configure webrtc label <name>".into(), "data-channel label".into()],
        vec![
            "configure webrtc ordered <true|false>".into(),
            "ordered or unordered delivery".into(),
        ],
        vec![
            "configure webrtc status".into(),
            "show the current WebRTC settings".into(),
        ],
        vec![
            "  connecting WebRTC".into(),
            "`webrtc enable` turns it on for every client whose `webrtc` is `follow`; `client <id> webrtc on|off` overrides one client, so you can run WebRTC and plain traffic side by side".into(),
        ],
        vec![
            "metrics enable".into(),
            "enable external SQL metrics using netmark.config".into(),
        ],
        vec![
            "metrics disable".into(),
            "disable external SQL metrics".into(),
        ],
        vec![
            "metrics status".into(),
            "show the external SQL connection state".into(),
        ],
        vec![
            "configure metrics <connection>".into(),
            "set the external SQL target; off until you set one".into(),
        ],
        vec![
            "configure save".into(),
            "write current configuration into netmark.config".into(),
        ],
        vec![
            "configure reset".into(),
            "reset configuration to defaults and reload netmark.config".into(),
        ],
        vec![
            "monitor IP <url>".into(),
            "set HTTP or HTTPS monitor target".into(),
        ],
        vec![
            "monitor start | stop".into(),
            "start or stop 30-second checks".into(),
        ],
        vec![
            "monitor history".into(),
            "show monitor events and alarms".into(),
        ],
        vec![
            "admin add email <address>".into(),
            "add an administrator email address".into(),
        ],
        vec![
            "admin delete email <address>".into(),
            "remove an administrator email address".into(),
        ],
        vec![
            "configure smtp <host[:port]>".into(),
            "set the SMTP server used for alerts".into(),
        ],
        vec![
            "admin smtp enabled | disabled".into(),
            "turn SMTP on (after a connection check) or off".into(),
        ],
        vec![
            "admin smtp status".into(),
            "check the connection to the SMTP server".into(),
        ],
        vec![
            "restapi enable [<address>]".into(),
            "serve the REST API; defaults to 127.0.0.1:8081".into(),
        ],
        vec!["restapi disable".into(), "stop serving the REST API".into()],
        vec![
            "restapi status".into(),
            "show whether the REST API is serving and where".into(),
        ],
        vec!["start".into(), "start a traffic run".into()],
        vec!["stop".into(), "stop the traffic run".into()],
        vec![
            "status".into(),
            "show current counters and bandwidth up and down".into(),
        ],
        vec![
            "clean".into(),
            "delete data while preserving the run ID counter".into(),
        ],
        vec![
            "show run <id>".into(),
            "show one run, its bandwidth and both debriefs".into(),
        ],
        vec![
            "list".into(),
            "list all runs with role, result and bandwidth".into(),
        ],
        vec!["help".into(), "show this help".into()],
        vec!["quit | exit".into(), "stop workers and exit".into()],
    ]
}

/// Every command word the interactive loop accepts, so `help` can be checked
/// against the dispatcher instead of drifting from it.
pub const COMMANDS: &[&str] = &[
    "server",
    "client",
    "selftest",
    "benchmark",
    "configure",
    "metrics",
    "monitor",
    "admin",
    "restapi",
    "start",
    "stop",
    "status",
    "clean",
    "show run",
    "list",
    "help",
    "quit",
    "exit",
];

/// The first word of every row in the help table, used to prove `help` covers
/// every command in [`COMMANDS`].
pub fn help_topics() -> Vec<String> {
    help_rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}
