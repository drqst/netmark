//! netmark as a library. The `netmark` binary is a thin front end over this
//! crate; external Rust code can depend on it directly and drive runs through
//! [`sdk::TestRunner`].

pub mod cli;
pub mod cli_textout;
pub mod configuration;
pub mod core;
pub mod metrics;
pub mod monitor;
pub mod sdk;
pub mod smtp;

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
    config
}

pub fn config_from_traffic(traffic: &configuration::TrafficConfig) -> Config {
    Config {
        rate: traffic.rate,
        packet_type: PacketType::parse(&traffic.packet_type).unwrap_or(PacketType::Tcp),
        tcp_bytes_per_second: traffic.tcp_bytes_per_second,
        udp_packet_size: traffic.udp_packet_size,
        client_runtime: traffic.client_runtime,
        server_runtime: traffic.server_runtime,
        jitter_millis: traffic.jitter_millis,
        client_jitter_millis: traffic.client_jitter_millis,
        server_jitter_millis: traffic.server_jitter_millis,
        max_tcp_jitter_millis: traffic.max_tcp_jitter_millis,
        max_udp_jitter_millis: traffic.max_udp_jitter_millis,
        limit_bytes_per_second: traffic.limit,
        admin_emails: Vec::new(),
    }
}

pub fn traffic_config_from(config: &Config) -> configuration::TrafficConfig {
    configuration::TrafficConfig {
        rate: config.rate,
        packet_type: config.packet_type.as_str().to_string(),
        tcp_bytes_per_second: config.tcp_bytes_per_second,
        udp_packet_size: config.udp_packet_size,
        client_runtime: config.client_runtime,
        server_runtime: config.server_runtime,
        jitter_millis: config.jitter_millis,
        client_jitter_millis: config.client_jitter_millis,
        server_jitter_millis: config.server_jitter_millis,
        max_tcp_jitter_millis: config.max_tcp_jitter_millis,
        max_udp_jitter_millis: config.max_udp_jitter_millis,
        limit: config.limit_bytes_per_second,
    }
}

/// The end state of a run as stored in local SQLite: bytes sent, plus whether it
/// stayed within the configured jitter and throughput limits (millisecond-level
/// metrics never touch local SQLite; they only go to the configured external SQL).
pub struct RunOutcome {
    pub sent_bytes: u64,
    pub result: &'static str,
    pub failure_reason: Option<String>,
}

pub fn evaluate_run(metrics: &Metrics, config: &Config, elapsed: Option<Duration>) -> RunOutcome {
    let (sent_tcp, sent_udp) = metrics.run_sent_bytes();
    let sent_bytes = sent_tcp + sent_udp;
    let (received_tcp, received_udp) = metrics.run_received_bytes();
    let received_bytes = received_tcp + received_udp;
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
    if config.limit_bytes_per_second > 0 {
        let elapsed_secs = elapsed.unwrap_or_default().as_secs_f64().max(1.0);
        // Only check a side's throughput if that role actually transferred bytes
        // this run, so an idle client or server doesn't produce a false failure.
        if sent_bytes > 0 {
            let bytes_per_second = (sent_bytes as f64 / elapsed_secs) as u64;
            if bytes_per_second < config.limit_bytes_per_second {
                reasons.push(format!(
                    "client throughput {bytes_per_second} bytes/sec below limit {}",
                    config.limit_bytes_per_second
                ));
            }
        }
        if received_bytes > 0 {
            let bytes_per_second = (received_bytes as f64 / elapsed_secs) as u64;
            if bytes_per_second < config.limit_bytes_per_second {
                reasons.push(format!(
                    "server throughput {bytes_per_second} bytes/sec below limit {}",
                    config.limit_bytes_per_second
                ));
            }
        }
    }
    RunOutcome {
        sent_bytes,
        result: if reasons.is_empty() { "ok" } else { "fail" },
        failure_reason: if reasons.is_empty() {
            None
        } else {
            Some(reasons.join("; "))
        },
    }
}

/// Writes the single final metrics report for a run to the external database, if
/// configured; this replaces any per-second writes, so every run yields exactly
/// one external row (still tagged with run-id and timestamp).
pub fn record_final_metrics(
    sink: Option<&Arc<ExternalSqlMetrics>>,
    run_id: u64,
    metrics: &Metrics,
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
        );
    }
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
        if !already_logged {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
                let _ = writeln!(file, "{} {}", core::timestamp(), marker);
            }
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

/// Executes one profile, driving both roles and returning the raw outcome plus
/// the metrics collected. Shared by auto mode and by [`sdk::TestRunner`].
pub(crate) fn execute_profile(
    profile: &configuration::TestProfile,
    log_dir: &std::path::Path,
    before: impl FnOnce(u64, &Arc<Metrics>) -> Result<(), String>,
) -> Result<(u64, Arc<Metrics>, Duration, RunOutcome), String> {
    create_dir_all(log_dir).map_err(|error| error.to_string())?;
    for name in ["client.log", "server.log", "netmark.log"] {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join(name))
            .map_err(|error| error.to_string())?;
    }
    let sql = SqlState::new();
    sql.enable().map_err(|error| error.to_string())?;
    let external = profile
        .metrics
        .sql
        .as_deref()
        .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
        .map(Arc::new);
    let config = Arc::new(Mutex::new(config_from_traffic(&profile.traffic)));
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
    }
    if profile.client.enabled {
        core::spawn_client(
            Arc::clone(&config),
            Arc::clone(&gate),
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            profile.client.remote.clone(),
            log_dir.to_path_buf(),
        );
    }
    sql.start_run(run_id);
    let started = Instant::now();
    gate.start();
    thread::sleep(Duration::from_secs(profile.duration_seconds.max(1)));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(250));
    let elapsed = started.elapsed();
    write_run_event(log_dir, run_id, "Completed");
    let outcome = evaluate_run(&metrics, &config.lock().unwrap(), Some(elapsed));
    sql.complete_run(
        run_id,
        outcome.result,
        outcome.sent_bytes,
        outcome.failure_reason.as_deref(),
    );
    record_final_metrics(external.as_ref(), run_id, &metrics);
    Ok((run_id, metrics, elapsed, outcome))
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
