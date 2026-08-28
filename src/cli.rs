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

/// Subcommand dispatcher for `client <id> <...>`.
pub fn client_command(clients: &Clients, id: u64, args: &[&str], log_dir: &std::path::Path) {
    if clients.get(id).is_none() {
        cli_textout::line(format!("no client {id}; use: client add"));
        return;
    }
    match args {
        ["enable"] => {
            clients.update(id, |client| client.enabled = true);
            cli_textout::line(format!("client {id} enabled"));
        }
        ["disable"] => {
            clients.update(id, |client| client.enabled = false);
            cli_textout::line(format!("client {id} disabled"));
        }
        ["remote", host] => {
            clients.update(id, |client| client.remote = (*host).to_string());
            cli_textout::line(format!("client {id} remote set to {host}"));
        }
        ["runtime", value] => match value.parse::<u64>() {
            Ok(value) => {
                clients.update(id, |client| client.runtime = Some(value));
                cli_textout::line(format!("client {id} runtime set to {value} seconds"));
            }
            Err(_) => cli_textout::line("runtime must be a non-negative integer"),
        },
        ["jitter", value] => match value.parse::<u64>() {
            Ok(value) => {
                clients.update(id, |client| client.jitter_millis = Some(value));
                cli_textout::line(format!("client {id} jitter set to {value} ms"));
            }
            Err(_) => cli_textout::line("jitter must be milliseconds"),
        },
        ["http", "check", url] => client_http_check(log_dir, url),
        ["status"] => {
            cli_textout::line(clients.get(id).map(|c| c.summary()).unwrap_or_default())
        }
        _ => cli_textout::line(CLIENT_USAGE),
    }
}

pub const CLIENT_USAGE: &str = "client: list | add | delete <id> | <id> enable | <id> disable | <id> remote <ip> | <id> runtime <seconds> | <id> jitter <ms> | <id> http check <url> | <id> status";

/// Subcommand: client list
pub fn list_clients(clients: &Clients) {
    for client in clients.list() {
        cli_textout::line(client.summary());
    }
}

/// Subcommand: client runtime <seconds> | server runtime <seconds>
pub fn set_runtime(config: &Arc<Mutex<Config>>, client: bool, value: &str) {
    match value.parse::<u64>() {
        Ok(value) => {
            if client {
                config.lock().unwrap().client_runtime = value;
            } else {
                config.lock().unwrap().server_runtime = value;
            }
            cli_textout::line(format!("runtime set to {value} seconds"));
        }
        Err(_) => cli_textout::line("runtime must be a non-negative integer"),
    }
}

/// Subcommand: admin add email <address> | admin delete email <address>
pub fn update_admin_email(
    path: &std::path::Path,
    config: &Arc<Mutex<Config>>,
    address: &str,
    add: bool,
) {
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
            Ok(()) => cli_textout::line(if add {
                "administrator email added"
            } else {
                "administrator email deleted"
            }),
            Err(error) => cli_textout::line(format!("config write error: {error}")),
        },
        Err(error) => cli_textout::line(format!("config serialize error: {error}")),
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
) -> Result<(), String> {
    let mut document = configuration::load(path).unwrap_or_default();
    document.traffic = crate::traffic_config_from(config);
    document.clients = clients.list();
    document.admin.emails = config.admin_emails.clone();
    if let Some(sink) = external.lock().unwrap().as_ref() {
        document.metrics.sql = Some(sink.connection_string().to_string());
    }
    document.smtp = smtp.lock().unwrap().clone();
    document.restapi = restapi.lock().unwrap().clone();
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
}

/// Subcommand: admin smtp enabled | admin smtp disabled — turns SMTP use on or
/// off and persists the setting. Enabling first verifies the server answers.
pub fn set_smtp_enabled(
    path: &std::path::Path,
    smtp: &Arc<Mutex<configuration::SmtpConfig>>,
    enabled: bool,
) {
    let server = smtp.lock().unwrap().server.clone();
    if enabled {
        let Some(server) = server.filter(|server| !server.trim().is_empty()) else {
            cli_textout::line("no SMTP server configured; use: configure smtp <host[:port]>");
            return;
        };
        match crate::smtp::check(&server) {
            Ok(result) => cli_textout::line(format!(
                "SMTP check succeeded: {} ({} ms, {})",
                result.target, result.elapsed_millis, result.greeting
            )),
            Err(error) => {
                cli_textout::line(format!("SMTP check failed: {error}"));
                cli_textout::line("SMTP not enabled");
                return;
            }
        }
    }
    smtp.lock().unwrap().enabled = enabled;
    let mut document = configuration::load(path).unwrap_or_default();
    document.smtp = smtp.lock().unwrap().clone();
    match serde_yaml::to_string(&document).map_err(|error| error.to_string()) {
        Ok(contents) => match std::fs::write(path, contents) {
            Ok(()) => cli_textout::line(if enabled {
                "SMTP enabled"
            } else {
                "SMTP disabled"
            }),
            Err(error) => cli_textout::line(format!("config write error: {error}")),
        },
        Err(error) => cli_textout::line(format!("config serialize error: {error}")),
    }
}

/// Subcommand: admin smtp status — reports the configured server, whether SMTP is
/// enabled, and whether the server currently answers.
pub fn smtp_status(smtp: &Arc<Mutex<configuration::SmtpConfig>>) {
    let settings = smtp.lock().unwrap().clone();
    let state = if settings.enabled {
        "enabled"
    } else {
        "disabled"
    };
    match settings.server.as_deref().filter(|s| !s.trim().is_empty()) {
        None => cli_textout::line(format!("SMTP {state}, no server configured")),
        Some(server) => match crate::smtp::check(server) {
            Ok(result) => cli_textout::line(format!(
                "SMTP {state}, {} reachable ({} ms, {})",
                result.target, result.elapsed_millis, result.greeting
            )),
            Err(error) => cli_textout::line(format!("SMTP {state}, {server} unreachable: {error}")),
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

/// Command: selftest — sends UDP traffic to localhost for 3 seconds and stops automatically.
pub fn run_selftest(
    config: &Arc<Mutex<Config>>,
    stopping: &Arc<AtomicBool>,
    running: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    sql: &Arc<SqlState>,
    external: &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    log_dir: &std::path::Path,
) {
    if running.swap(true, Ordering::Relaxed) {
        cli_textout::line("already running");
        return;
    }
    let run_id = sql.next_run_id(1);
    sql.set_role(core::ROLE_BOTH);
    {
        let mut config = config.lock().unwrap();
        config.packet_type = PacketType::Udp;
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
    cli_textout::line(format!("selftest started run {run_id}"));
    let stop = Arc::clone(stopping);
    let state = Arc::clone(running);
    let sql_state = Arc::clone(sql);
    let state_metrics = Arc::clone(metrics);
    let state_external = Arc::clone(external);
    let state_config = Arc::clone(config);
    let logs = log_dir.to_path_buf();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(SELFTEST_SECONDS));
        stop.store(true, Ordering::Relaxed);
        state.store(false, Ordering::Relaxed);
        cli_textout::raw("\r\n");
        let mut outcome = crate::evaluate_run(
            &state_metrics,
            &state_config.lock().unwrap(),
            Some(Duration::from_secs(SELFTEST_SECONDS)),
        );
        match core::run_debrief(
            DEFAULT_REMOTE,
            run_id,
            PacketType::Udp,
            &state_metrics,
            &sql_state,
            &logs,
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
        sql_state.complete_run(
            run_id,
            outcome.result,
            outcome.sent_bytes,
            outcome.failure_reason.as_deref(),
        );
        crate::record_final_metrics(state_external.lock().unwrap().as_ref(), run_id, &state_metrics);
        crate::write_run_event(&logs, run_id, "Completed");
        cli_textout::raw("\r\n");
        cli_textout::line(format!(
            "selftest completed run {run_id} ({})",
            outcome.result
        ));
    });
}

/// Command: benchmark duration <seconds> — floods the remote server with TCP and reports bandwidth.
pub fn run_benchmark(remote: &str, seconds: u64, sql: &SqlState, log_dir: &std::path::Path) {
    let run_id = sql.next_run_id(1);
    crate::write_run_event(log_dir, run_id, "Starting");
    sql.start_run(run_id);
    let mut stream = match TcpStream::connect(format!("{remote}:9000")) {
        Ok(stream) => stream,
        Err(error) => {
            sql.complete_run(run_id, "error", 0, Some(&error.to_string()));
            crate::write_run_event(log_dir, run_id, "Completed");
            cli_textout::line(format!("benchmark run {run_id} failed: {error}"));
            return;
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
        if bytes > 0 { "ok" } else { "error" },
        bytes,
        if bytes > 0 { None } else { Some("no bytes sent") },
    );
    crate::write_run_event(log_dir, run_id, "Completed");
    cli_textout::line(format!(
        "benchmark run {run_id}: {bytes} bytes in {elapsed_ms} ms ({bytes_per_second} bytes/sec)"
    ));
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
    if let ["type", value] = args {
        let packet_type = PacketType::parse(value).ok_or("type must be tcp or udp")?;
        config.lock().unwrap().packet_type = packet_type;
        return Ok(());
    }
    if let ["bandwidth", "limit", value] = args {
        let value = value
            .parse()
            .map_err(|_| "bandwidth limit must be bytes/sec (0 disables the check)")?;
        config.lock().unwrap().limit_bytes_per_second = value;
        return Ok(());
    }
    Err("use: configure tcp bytes <rate>, udp packetsize <bytes>, type <tcp|udp>, or bandwidth limit <bytes/sec>".into())
}

/// Subcommand: client http check <url>
pub fn client_http_check(log_dir: &std::path::Path, url: &str) {
    let url = normalize_http_target(url);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            cli_textout::line(format!("HTTP client error: {error}"));
            return;
        }
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
                cli_textout::line(format!(
                    "HTTP check succeeded: {url} ({elapsed} ms, {} bytes)",
                    body.len()
                ));
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
            }
            Err(error) => cli_textout::line(format!("HTTP body error: {error}")),
        },
        Err(error) => cli_textout::line(format!("HTTP check failed: {error}")),
    }
}

/// Helper for Command: status — run-scoped totals so counts correlate with the
/// final report instead of the periodically-reset live display counters.
pub fn traffic_status(metrics: &Metrics, running: bool) -> String {
    let values = metrics.run_totals();
    let (lost, order) = metrics.udp_status();
    format!(
        "{} TCP sent {} bytes, UDP sent {} bytes, TCP received {} bytes, UDP received {} bytes, UDP lost {}, out of order {}",
        if running { "running" } else { "stopped" },
        values[1],
        values[3],
        values[5],
        values[7],
        lost,
        order
    )
}

/// Subcommand: monitor history
pub fn show_monitor_history(log_dir: &std::path::Path) {
    for name in ["netmark.log", "alarm.log"] {
        if let Ok(contents) = std::fs::read_to_string(log_dir.join(name)) {
            for line in contents.lines() {
                if name == "alarm.log"
                    || line.contains("Monitor started")
                    || line.contains("Monitor stopped")
                {
                    cli_textout::line(&line);
                }
            }
        }
    }
}

/// Command: help
pub fn print_help(
    stdout_guard: &Arc<Mutex<()>>,
    output: &Arc<Mutex<std::process::ChildStdin>>,
) {
    let mut output = output.lock().unwrap();
    let _ = writeln!(output, "HIDE");
    let _ = output.flush();
    let _guard = stdout_guard.lock().unwrap();
    let rows = vec![
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
            "client <id> http check <url>".into(),
            "load one HTTP or HTTPS page".into(),
        ],
        vec![
            "selftest".into(),
            "send UDP traffic to localhost and stop automatically".into(),
        ],
        vec![
            "benchmark duration <seconds>".into(),
            "flood the remote server with TCP and report bandwidth".into(),
        ],
        vec![
            "configure tcp bytes <rate>".into(),
            "set TCP bytes per second".into(),
        ],
        vec![
            "configure tcp jitter <ms>".into(),
            "set TCP send jitter".into(),
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
            "configure bandwidth limit <bytes/sec>".into(),
            "minimum acceptable throughput; 0 disables the check".into(),
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
            "configure metrics <connection>".into(),
            "override the configured external SQL target".into(),
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
        vec!["status".into(), "show current counters".into()],
        vec![
            "clean".into(),
            "delete data while preserving the run ID counter".into(),
        ],
        vec!["show run <id>".into(), "show stored run metrics".into()],
        vec!["list".into(), "list all run IDs and results".into()],
        vec!["help".into(), "show this help".into()],
        vec!["exit".into(), "stop workers and exit".into()],
    ];
    cli_textout::table(&rows, &[34, 64]);
}
