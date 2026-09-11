//! netmark as a library. The `netmark` binary is a thin front end over this
//! crate; external Rust code can depend on it directly and drive runs through
//! [`sdk::TestRunner`].

pub mod cli;
pub mod cli_textout;
pub mod configuration;
pub mod core;
pub mod metrics;
pub mod monitor;
pub mod rawip;
pub mod restapi;
pub mod sdk;
pub mod sctp;
pub mod session;
pub mod smtp;
pub mod webrtc;

#[cfg(test)]
mod tests;

use crate::core::{Config, Metrics, PacketType, SqlState, StartGate};
use crate::metrics::ExternalSqlMetrics;
use std::fs::{OpenOptions, create_dir_all};
use std::io::Write;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

pub fn config_from_file(file_config: &configuration::FileConfig) -> Config {
    let mut config = config_from_traffic(&file_config.traffic);
    config.admin_emails = file_config.admin.emails.clone();
    config.webrtc = webrtc_settings(&file_config.webrtc);
    config
}

pub fn webrtc_settings(config: &configuration::WebRtcConfig) -> webrtc::Settings {
    webrtc::Settings {
        enabled: config.enabled,
        channels: config.channels.max(1),
        label: config.label.clone(),
        ordered: config.ordered,
    }
}

pub fn webrtc_config_from(settings: &webrtc::Settings) -> configuration::WebRtcConfig {
    configuration::WebRtcConfig {
        enabled: settings.enabled,
        channels: settings.channels,
        label: settings.label.clone(),
        ordered: settings.ordered,
    }
}

pub fn config_from_traffic(traffic: &configuration::TrafficConfig) -> Config {
    Config {
        udp_rate: traffic.udp_rate,
        packet_type: PacketType::parse(&traffic.packet_type).unwrap_or(PacketType::Tcp),
        tcp_bytes_per_second: traffic.tcp_bytes_per_second,
        tcp_window_size: traffic.tcp_window_size,
        udp_packet_size: traffic.udp_packet_size,
        client_runtime: traffic.client_runtime,
        server_runtime: traffic.server_runtime,
        jitter_millis: traffic.jitter_millis,
        client_jitter_millis: traffic.client_jitter_millis,
        server_jitter_millis: traffic.server_jitter_millis,
        max_tcp_jitter_millis: traffic.max_tcp_jitter_millis,
        max_udp_jitter_millis: traffic.max_udp_jitter_millis,
        limit_bytes_per_second: traffic.limit,
        limits: limit_set_from(&traffic.limits),
        protocols: core::ProtocolSwitches {
            tcp: traffic.protocols.tcp,
            sctp: traffic.protocols.sctp,
            udp: traffic.protocols.udp,
            ip: traffic.protocols.ip,
        },
        webrtc: webrtc::Settings::default(),
        admin_emails: Vec::new(),
    }
}

/// Per-client view of the traffic settings, applying that client's runtime,
/// jitter and WebRTC overrides on top of the profile-wide values.
pub fn client_config(
    traffic: &configuration::TrafficConfig,
    client: &configuration::ClientConfig,
    webrtc: &webrtc::Settings,
) -> Config {
    let mut config = config_from_traffic(traffic);
    if let Some(runtime) = client.runtime {
        config.client_runtime = runtime;
    }
    if let Some(jitter) = client.jitter_millis {
        config.client_jitter_millis = jitter;
    }
    config.webrtc = webrtc.clone();
    config.webrtc.enabled = client.webrtc.unwrap_or(webrtc.enabled);
    config
}

pub fn traffic_config_from(config: &Config) -> configuration::TrafficConfig {
    configuration::TrafficConfig {
        udp_rate: config.udp_rate,
        packet_type: config.packet_type.as_str().to_string(),
        tcp_bytes_per_second: config.tcp_bytes_per_second,
        tcp_window_size: config.tcp_window_size,
        udp_packet_size: config.udp_packet_size,
        client_runtime: config.client_runtime,
        server_runtime: config.server_runtime,
        jitter_millis: config.jitter_millis,
        client_jitter_millis: config.client_jitter_millis,
        server_jitter_millis: config.server_jitter_millis,
        max_tcp_jitter_millis: config.max_tcp_jitter_millis,
        max_udp_jitter_millis: config.max_udp_jitter_millis,
        limit: config.limit_bytes_per_second,
        limits: limits_config_from(&config.limits),
        protocols: configuration::ProtocolsConfig {
            tcp: config.protocols.tcp,
            sctp: config.protocols.sctp,
            udp: config.protocols.udp,
            ip: config.protocols.ip,
        },
    }
}

fn limits_from(limits: &configuration::ProtocolLimitsConfig) -> core::Limits {
    core::Limits {
        min_sent_bytes: limits.min_sent_bytes,
        min_received_bytes: limits.min_received_bytes,
        min_sent_bytes_per_second: limits.min_sent_bytes_per_second,
        min_received_bytes_per_second: limits.min_received_bytes_per_second,
        max_jitter_millis: limits.max_jitter_millis,
        max_lost_packets: limits.max_lost_packets,
        max_out_of_order_packets: limits.max_out_of_order_packets,
    }
}

fn protocol_limits_config_from(limits: &core::Limits) -> configuration::ProtocolLimitsConfig {
    configuration::ProtocolLimitsConfig {
        min_sent_bytes: limits.min_sent_bytes,
        min_received_bytes: limits.min_received_bytes,
        min_sent_bytes_per_second: limits.min_sent_bytes_per_second,
        min_received_bytes_per_second: limits.min_received_bytes_per_second,
        max_jitter_millis: limits.max_jitter_millis,
        max_lost_packets: limits.max_lost_packets,
        max_out_of_order_packets: limits.max_out_of_order_packets,
    }
}

pub fn limit_set_from(limits: &configuration::LimitsConfig) -> core::LimitSet {
    core::LimitSet {
        tcp: limits_from(&limits.tcp),
        sctp: limits_from(&limits.sctp),
        udp: limits_from(&limits.udp),
        ip: limits_from(&limits.ip),
    }
}

pub fn limits_config_from(limits: &core::LimitSet) -> configuration::LimitsConfig {
    configuration::LimitsConfig {
        tcp: protocol_limits_config_from(&limits.tcp),
        sctp: protocol_limits_config_from(&limits.sctp),
        udp: protocol_limits_config_from(&limits.udp),
        ip: protocol_limits_config_from(&limits.ip),
    }
}

/// The end state of a run as stored in local SQLite: bytes moved in each
/// direction and the bandwidth that implies, plus whether the run stayed within
/// the configured jitter and throughput limits (millisecond-level metrics never
/// touch local SQLite; they only go to the configured external SQL).
pub struct RunOutcome {
    /// The transport the run used.
    pub protocol: &'static str,
    /// Every counter the run collected, stored with the run.
    pub detail: core::RunDetail,
    pub sent_bytes: u64,
    pub received_bytes: u64,
    pub sent_bytes_per_second: u64,
    pub received_bytes_per_second: u64,
    pub result: &'static str,
    pub failure_reason: Option<String>,
    pub tcp_mss: u64,
    pub tcp_mtu: u64,
    pub tcp_window_size: u64,
}

impl RunOutcome {
    /// Adds a reason discovered after the traffic evaluation, such as a failed debrief.
    pub fn add_failure(&mut self, reason: String) {
        self.result = "fail";
        self.failure_reason = Some(match self.failure_reason.take() {
            Some(existing) => format!("{existing}; {reason}"),
            None => reason,
        });
    }
    pub fn summary(&self) -> core::RunSummary<'_> {
        core::RunSummary {
            result: self.result,
            sent_bytes: self.sent_bytes,
            received_bytes: self.received_bytes,
            sent_bytes_per_second: self.sent_bytes_per_second,
            received_bytes_per_second: self.received_bytes_per_second,
            failure_reason: self.failure_reason.as_deref(),
            protocol: self.protocol,
            detail: self.detail,
        }
    }
    /// The one line every surface prints for a finished run.
    pub fn report_line(&self, run_id: u64) -> String {
        format!(
            "run {run_id} result={} sent_bytes={} received_bytes={} up={} bytes/sec down={} bytes/sec tcp_mss={} tcp_mtu={} tcp_window_size={}{}",
            self.result,
            self.sent_bytes,
            self.received_bytes,
            self.sent_bytes_per_second,
            self.received_bytes_per_second,
            self.tcp_mss,
            self.tcp_mtu,
            self.tcp_window_size,
            self.failure_reason
                .as_deref()
                .map(|reason| format!(" reason=\"{reason}\""))
                .unwrap_or_default()
        )
    }
}

pub fn evaluate_run(metrics: &Metrics, config: &Config, elapsed: Option<Duration>) -> RunOutcome {
    let sent_bytes = metrics.run_sent_total();
    let received_bytes = metrics.run_received_total();
    let elapsed = elapsed.unwrap_or_default();
    let mut reasons = Vec::new();
    if sent_bytes == 0 {
        reasons.push("no bytes were sent".to_string());
    }
    if metrics.tcp_jitter_millis() > config.max_tcp_jitter_millis {
        reasons.push(format!(
            "TCP jitter {}ms exceeded limit {}ms",
            metrics.tcp_jitter_millis(),
            config.max_tcp_jitter_millis
        ));
    }
    if metrics.udp_jitter_millis() > config.max_udp_jitter_millis {
        reasons.push(format!(
            "UDP jitter {}ms exceeded limit {}ms",
            metrics.udp_jitter_millis(),
            config.max_udp_jitter_millis
        ));
    }
    let sent_bytes_per_second = core::bandwidth(sent_bytes, elapsed);
    let received_bytes_per_second = core::bandwidth(received_bytes, elapsed);
    if config.limit_bytes_per_second > 0 {
        // Only check a side's throughput if that role actually transferred bytes
        // this run, so an idle client or server doesn't produce a false failure.
        if sent_bytes > 0 && sent_bytes_per_second < config.limit_bytes_per_second {
            reasons.push(format!(
                "client throughput {sent_bytes_per_second} bytes/sec below limit {}",
                config.limit_bytes_per_second
            ));
        }
        if received_bytes > 0 && received_bytes_per_second < config.limit_bytes_per_second {
            reasons.push(format!(
                "server throughput {received_bytes_per_second} bytes/sec below limit {}",
                config.limit_bytes_per_second
            ));
        }
    }
    check_limits(
        metrics,
        config,
        sent_bytes,
        received_bytes,
        sent_bytes_per_second,
        received_bytes_per_second,
        &mut reasons,
    );
    RunOutcome {
        protocol: config.packet_type.as_str(),
        detail: core::RunDetail::from_metrics(metrics),
        sent_bytes,
        received_bytes,
        sent_bytes_per_second,
        received_bytes_per_second,
        result: if reasons.is_empty() { "ok" } else { "fail" },
        failure_reason: if reasons.is_empty() {
            None
        } else {
            Some(reasons.join("; "))
        },
        tcp_mss: metrics.tcp_transport().0,
        tcp_mtu: metrics.tcp_transport().1,
        tcp_window_size: metrics.tcp_transport().2,
    }
}

/// Applies the per-transport limits set with `configure limits` to a finished
/// run, adding one reason per limit that was not met.
fn check_limits(
    metrics: &Metrics,
    config: &Config,
    sent_bytes: u64,
    received_bytes: u64,
    sent_bytes_per_second: u64,
    received_bytes_per_second: u64,
    reasons: &mut Vec<String>,
) {
    let packet_type = config.packet_type;
    let limits = config.limits.get(packet_type);
    let jitter = match packet_type {
        core::PacketType::Udp => metrics.udp_jitter_millis(),
        core::PacketType::Ip => 0,
        core::PacketType::Tcp | core::PacketType::Sctp => metrics.tcp_jitter_millis(),
    };
    let (lost, out_of_order) = metrics.udp_status();
    let protocol = packet_type.as_str();
    let mut below = |name: &str, measured: u64, limit: u64| {
        if limit > 0 && measured < limit {
            reasons.push(format!("{protocol} {name} {measured} below limit {limit}"));
        }
    };
    below("sent bytes", sent_bytes, limits.min_sent_bytes);
    below("received bytes", received_bytes, limits.min_received_bytes);
    below(
        "sent bytes/sec",
        sent_bytes_per_second,
        limits.min_sent_bytes_per_second,
    );
    below(
        "received bytes/sec",
        received_bytes_per_second,
        limits.min_received_bytes_per_second,
    );
    let mut above = |name: &str, measured: u64, limit: u64| {
        if limit > 0 && measured > limit {
            reasons.push(format!("{protocol} {name} {measured} above limit {limit}"));
        }
    };
    above("jitter ms", jitter, limits.max_jitter_millis);
    above("lost packets", lost, limits.max_lost_packets);
    above(
        "out-of-order packets",
        out_of_order,
        limits.max_out_of_order_packets,
    );
}

/// Writes the single final metrics report for a run to the external database, if
/// configured; this replaces any per-second writes, so every run yields exactly
/// one external row (still tagged with run-id and timestamp).
pub fn record_final_metrics(
    sink: Option<&Arc<ExternalSqlMetrics>>,
    run_id: u64,
    metrics: &Metrics,
    outcome: &RunOutcome,
) {
    if let Some(sink) = sink {
        let (lost, out_of_order) = metrics.udp_status();
        let _ = sink.write(
            &core::timestamp(),
            run_id,
            &metrics.run_totals(),
            lost,
            out_of_order,
            metrics.jitter_millis(),
            outcome.sent_bytes_per_second,
            outcome.received_bytes_per_second,
        );
    }
}

/// Appends one line to netmark.log.
pub fn write_log_line(log_dir: &std::path::Path, line: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("netmark.log"))
    {
        let _ = writeln!(file, "{} {line}", core::timestamp());
    }
}

/// Blocks until an RFC 3339 instant, so machines handed the same profile begin
/// sending together. An instant already past starts immediately.
pub fn wait_until(start_at: Option<&str>) -> Result<(), String> {
    let Some(start_at) = start_at else {
        return Ok(());
    };
    let target = chrono::DateTime::parse_from_rfc3339(start_at)
        .map_err(|error| format!("start_at must be an RFC 3339 timestamp: {error}"))?;
    let wait = target.timestamp_millis() - chrono::Utc::now().timestamp_millis();
    if wait > 0 {
        thread::sleep(Duration::from_millis(wait as u64));
    }
    Ok(())
}

pub fn write_run_event(log_dir: &std::path::Path, run_id: u64, event: &str) {
    if run_id == 0 {
        return;
    }
    for name in ["cli.log", "client.log", "server.log", "netmark.log"] {
        let path = log_dir.join(name);
        let marker = format!("{} run {}", event, run_id);
        let already_logged = std::fs::read_to_string(&path)
            .map(|contents| contents.lines().any(|line| line.ends_with(&marker)))
            .unwrap_or(false);
        if !already_logged
            && let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path)
        {
            let _ = writeln!(file, "{} {}", core::timestamp(), marker);
        }
    }
}

/// Default log directory: `log/` next to the running executable.
pub fn default_log_dir() -> std::path::PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("log")
}

/// What one executed profile produced.
pub(crate) struct ProfileRun {
    pub run_id: u64,
    pub metrics: Arc<Metrics>,
    pub elapsed: Duration,
    pub outcome: RunOutcome,
    pub debrief: Option<core::Debrief>,
}

/// Executes one profile, driving both roles and returning the raw outcome plus
/// the metrics collected and the end-of-run debrief. Shared by auto mode and by
/// [`sdk::TestRunner`].
pub(crate) fn execute_profile(
    profile: &configuration::TestProfile,
    log_dir: &std::path::Path,
    before: impl FnOnce(u64, &Arc<Metrics>) -> Result<(), String>,
) -> Result<ProfileRun, String> {
    create_dir_all(log_dir).map_err(|error| error.to_string())?;
    for name in ["client.log", "server.log", "netmark.log"] {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join(name))
            .map_err(|error| error.to_string())?;
    }
    let sql = Arc::new(SqlState::new());
    sql.enable().map_err(|error| error.to_string())?;
    let clients: Vec<_> = profile.enabled_clients().cloned().collect();
    sql.set_role(core::role_name(!clients.is_empty(), profile.server.enabled));
    let external = profile
        .metrics
        .sql
        .as_deref()
        .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
        .map(Arc::new);
    let webrtc = webrtc_settings(&profile.webrtc);
    let mut base_config = config_from_traffic(&profile.traffic);
    base_config.webrtc = webrtc.clone();
    let config = Arc::new(Mutex::new(base_config));
    let packet_type = config.lock().unwrap().packet_type;
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let run_id = sql.next_run_id(1);
    metrics.reset_run();
    before(run_id, &metrics)?;
    let gate = Arc::new(StartGate::new());
    write_run_event(log_dir, run_id, "Starting");
    if profile.server.enabled {
        core::spawn_server(
            Arc::clone(&config),
            Arc::clone(&gate),
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            log_dir.to_path_buf(),
        );
        core::spawn_debrief_responder(
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            Arc::clone(&sql),
            log_dir.to_path_buf(),
        );
    }
    for client in &clients {
        core::spawn_client(
            Arc::new(Mutex::new(client_config(&profile.traffic, client, &webrtc))),
            Arc::clone(&gate),
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            client.remote.clone(),
            log_dir.to_path_buf(),
            client.id,
        );
    }
    sql.start_run(run_id);
    wait_until(profile.start_at.as_deref())?;
    let started = Instant::now();
    gate.start();
    thread::sleep(Duration::from_secs(profile.duration_seconds.max(1)));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(250));
    let elapsed = started.elapsed();
    write_run_event(log_dir, run_id, "Completed");
    let mut outcome = evaluate_run(&metrics, &config.lock().unwrap(), Some(elapsed));
    // Counters are per instance, not per client, so one debrief covers every
    // client this instance drove; it goes to the lowest-numbered client's remote.
    let debrief = clients.iter().min_by_key(|client| client.id).map(|client| {
        core::run_debrief(
            &client.remote,
            run_id,
            packet_type,
            &metrics,
            &sql,
            log_dir,
        )
    });
    let debrief = match debrief {
        Some(Ok(debrief)) => {
            if let Some(reason) = debrief.mismatch_reason() {
                outcome.add_failure(format!("debrief mismatch: {reason}"));
            }
            Some(debrief)
        }
        Some(Err(error)) => {
            outcome.add_failure(error);
            None
        }
        None => None,
    };
    sql.complete_run(run_id, &outcome.summary());
    write_log_line(log_dir, &outcome.report_line(run_id));
    record_final_metrics(external.as_ref(), run_id, &metrics, &outcome);
    Ok(ProfileRun {
        run_id,
        metrics,
        elapsed,
        outcome,
        debrief,
    })
}

/// Auto mode: runs one non-interactive test from a YAML test profile and exits.
/// The CLI never starts; only the final run outcome is printed to stdout.
pub fn run_auto_mode(profile_path: &std::path::Path) -> bool {
    match sdk::TestRunner::from_profile_file(profile_path).and_then(|runner| runner.verbose().run())
    {
        Ok(report) => report.passed,
        Err(error) => {
            eprintln!("{error}");
            false
        }
    }
}
