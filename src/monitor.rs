use crate::core::SqlState;
use crate::metrics::ExternalSqlMetrics;
use reqwest::blocking::Client;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

/// The external metrics database as the CLI holds it: it can be connected and
/// disconnected while the monitor is running.
pub type ExternalSql = Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>;

/// The kind of check the monitor performs. HTTP stays the default; ICMP uses
/// the system `ping` binary so no special capabilities are required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonitorKind {
    Http,
    Icmp,
}

pub struct MonitorState {
    target: Mutex<Option<String>>,
    kind: Mutex<MonitorKind>,
    interval_seconds: AtomicU64,
    ping_enabled: AtomicBool,
    running: AtomicBool,
    monitor_id: AtomicU64,
    next_monitor_id: AtomicU64,
    calls: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
}

impl MonitorState {
    pub fn new() -> Self {
        Self {
            target: Mutex::new(None),
            kind: Mutex::new(MonitorKind::Http),
            interval_seconds: AtomicU64::new(30),
            ping_enabled: AtomicBool::new(false),
            running: AtomicBool::new(false),
            monitor_id: AtomicU64::new(0),
            next_monitor_id: AtomicU64::new(1),
            calls: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }
    pub fn set_target(&self, target: String) {
        *self.target.lock().unwrap() = Some(target);
    }
    pub fn set_kind(&self, kind: MonitorKind) {
        *self.kind.lock().unwrap() = kind;
    }
    pub fn set_interval(&self, seconds: u64) {
        self.interval_seconds.store(seconds.max(1), Ordering::Relaxed);
    }
    pub fn set_ping_enabled(&self, enabled: bool) {
        self.ping_enabled.store(enabled, Ordering::Relaxed);
    }
    pub fn apply_config(&self, config: &crate::configuration::MonitorConfig) {
        if let Some(target) = config.ping.target.clone() {
            *self.target.lock().unwrap() = Some(target);
            *self.kind.lock().unwrap() = MonitorKind::Icmp;
        } else if let Some(target) = config.http.target.clone() {
            *self.target.lock().unwrap() = Some(target);
            *self.kind.lock().unwrap() = MonitorKind::Http;
        }
        self.interval_seconds.store(
            config.ping.interval_seconds.max(1),
            Ordering::Relaxed,
        );
        self.ping_enabled
            .store(config.ping.enabled, Ordering::Relaxed);
    }
    /// Subcommand: monitor start. Only the start status is kept locally; the
    /// checks themselves go to the external database.
    pub fn start(&self, log_dir: &Path, sql: &SqlState) -> Option<u64> {
        if self.running.swap(true, Ordering::Relaxed) {
            None
        } else {
            let id = self.next_monitor_id.fetch_add(1, Ordering::Relaxed);
            self.monitor_id.store(id, Ordering::Relaxed);
            let timestamp = crate::core::timestamp();
            append(
                log_dir,
                "monitor.log",
                &format!("{timestamp} monitor-id={id} Started"),
            );
            sql.record_monitor_status(&timestamp, id, "started");
            Some(id)
        }
    }
    /// Subcommand: monitor stop.
    pub fn stop(&self, log_dir: &Path, sql: &SqlState) {
        if self.running.swap(false, Ordering::Relaxed) {
            let id = self.monitor_id.load(Ordering::Relaxed);
            let timestamp = crate::core::timestamp();
            append(
                log_dir,
                "monitor.log",
                &format!("{timestamp} monitor-id={id} Stopped"),
            );
            sql.record_monitor_status(&timestamp, id, "stopped");
        }
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    pub fn status(&self) -> (bool, u64, u64, u64, u64) {
        (
            self.is_running(),
            self.monitor_id.load(Ordering::Relaxed),
            self.calls.load(Ordering::Relaxed),
            self.successes.load(Ordering::Relaxed),
            self.failures.load(Ordering::Relaxed),
        )
    }
    pub fn config_snapshot(&self) -> crate::configuration::MonitorConfig {
        let target = self.target.lock().unwrap().clone();
        let kind = self.kind.lock().unwrap().clone();
        let interval_seconds = self.interval_seconds.load(Ordering::Relaxed);
        let enabled = self.ping_enabled.load(Ordering::Relaxed);
        let mut config = crate::configuration::MonitorConfig::default();
        match kind {
            MonitorKind::Http => config.http.target = target,
            MonitorKind::Icmp => {
                config.ping.target = target;
                config.ping.interval_seconds = interval_seconds;
                config.ping.enabled = enabled;
            }
        }
        config
    }
    /// The checking loop. Every check, successful or not, is written to the
    /// external database; local SQLite only holds the start/stop status.
    pub fn spawn_worker(self: Arc<Self>, log_dir: PathBuf, external: ExternalSql) {
        thread::spawn(move || {
            let client = match Client::builder().timeout(Duration::from_secs(10)).build() {
                Ok(client) => client,
                Err(_) => return,
            };
            while !std::thread::panicking() {
                let interval = self.interval_seconds.load(Ordering::Relaxed);
                if self.running.load(Ordering::Relaxed)
                    && let Some(target) = self.target.lock().unwrap().clone()
                {
                    let kind = self.kind.lock().unwrap().clone();
                    let do_check = match kind {
                        MonitorKind::Http => true,
                        MonitorKind::Icmp => self.ping_enabled.load(Ordering::Relaxed),
                    };
                    if do_check {
                        let call_id = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                        let timestamp = crate::core::timestamp();
                        let started = Instant::now();
                        let (result, latency_millis) = match kind {
                            MonitorKind::Http => {
                                let result = client
                                    .get(&target)
                                    .send()
                                    .and_then(|response| response.error_for_status())
                                    .and_then(|response| response.bytes().map(|_| ()));
                                let latency = started.elapsed().as_millis() as u64;
                                (result.map_err(|e| e.to_string()), latency)
                            }
                            MonitorKind::Icmp => icmp_check(&target, &started),
                        };
                        let (word, detail) = match result {
                            Ok(_) => {
                                self.successes.fetch_add(1, Ordering::Relaxed);
                                ("OK", String::new())
                            }
                            Err(error) => {
                                self.failures.fetch_add(1, Ordering::Relaxed);
                                ("Failed", error)
                            }
                        };
                        append(
                            &log_dir,
                            "monitor.log",
                            &format!(
                                "{timestamp} monitor-id={} call-id={} {word}",
                                self.monitor_id.load(Ordering::Relaxed),
                                call_id
                            ),
                        );
                        if !detail.is_empty() {
                            append(
                                &log_dir,
                                "alarm.log",
                                &format!(
                                    "{timestamp} monitor-id={} call-id={} {target} {detail}",
                                    self.monitor_id.load(Ordering::Relaxed),
                                    call_id
                                ),
                            );
                        }
                        if let Some(sink) = external.lock().unwrap().clone()
                            && let Err(error) = sink.write_monitor(
                                &timestamp,
                                self.monitor_id.load(Ordering::Relaxed),
                                call_id,
                                &target,
                                word,
                                latency_millis,
                                &detail,
                            )
                        {
                            append(
                                &log_dir,
                                "alarm.log",
                                &format!("{timestamp} monitor data not stored: {error}"),
                            );
                        }
                    }
                }
                thread::sleep(Duration::from_secs(interval));
            }
        });
    }
}

/// Run one ICMP echo request via the system `ping` binary. Using `ping` avoids
/// needing `CAP_NET_RAW` in containers and keeps the monitor self-contained.
fn icmp_check(target: &str, started: &Instant) -> (Result<(), String>, u64) {
    let output = Command::new("ping")
        .args(["-c", "1", "-W", "5", target])
        .output();
    let latency = started.elapsed().as_millis() as u64;
    match output {
        Ok(output) if output.status.success() => (Ok(()), latency),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if stderr.is_empty() { stdout } else { stderr };
            (
                Err(format!("ping failed: {}", detail.trim())),
                latency,
            )
        }
        Err(error) => (Err(format!("cannot run ping: {error}")), latency),
    }
}

impl Default for MonitorState {
    fn default() -> Self {
        Self::new()
    }
}

fn append(log_dir: &Path, name: &str, line: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join(name))
    {
        use std::io::Write;
        let _ = writeln!(file, "{line}");
    }
}
