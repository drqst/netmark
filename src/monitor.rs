use crate::core::SqlState;
use crate::metrics::ExternalSqlMetrics;
use reqwest::blocking::Client;
use std::fs::OpenOptions;
use std::net::IpAddr;
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

#[derive(Clone, Copy)]
pub enum MonitorProtocol {
    Http,
    Icmp,
}

impl MonitorProtocol {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "http" => Some(Self::Http),
            "icmp" => Some(Self::Icmp),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Icmp => "icmp",
        }
    }
}

pub struct MonitorState {
    target: Mutex<Option<(MonitorProtocol, String)>>,
    running: AtomicBool,
    monitor_id: AtomicU64,
    next_monitor_id: AtomicU64,
    calls: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    interval_seconds: AtomicU64,
}

impl MonitorState {
    pub fn new() -> Self {
        Self {
            target: Mutex::new(None),
            running: AtomicBool::new(false),
            monitor_id: AtomicU64::new(0),
            next_monitor_id: AtomicU64::new(1),
            calls: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            interval_seconds: AtomicU64::new(30),
        }
    }
    pub fn set_target(&self, protocol: MonitorProtocol, target: String) {
        *self.target.lock().unwrap() = Some((protocol, target));
    }
    /// Subcommand: monitor start. Only the start status is kept locally; the
    /// checks themselves go to the external database.
    pub fn start(&self, log_dir: &Path, sql: &SqlState) -> Option<u64> {
        self.start_with_interval(log_dir, sql, 30)
    }
    /// Subcommand: monitor start <seconds>.
    pub fn start_with_interval(
        &self,
        log_dir: &Path,
        sql: &SqlState,
        interval_seconds: u64,
    ) -> Option<u64> {
        self.interval_seconds
            .store(interval_seconds, Ordering::Relaxed);
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
    /// The checking loop. Every check, successful or not, is written to the
    /// external database; local SQLite only holds the start/stop status.
    pub fn spawn_worker(self: Arc<Self>, log_dir: PathBuf, external: ExternalSql) {
        thread::spawn(move || {
            let client = match Client::builder().timeout(Duration::from_secs(10)).build() {
                Ok(client) => client,
                Err(_) => return,
            };
            while !std::thread::panicking() {
                if self.running.load(Ordering::Relaxed)
                    && let Some((protocol, target)) = self.target.lock().unwrap().clone()
                {
                        let call_id = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                        let timestamp = crate::core::timestamp();
                        let started = Instant::now();
                        let result: Result<(), String> = match protocol {
                            MonitorProtocol::Http => client
                                .get(&target)
                                .send()
                                .and_then(|response| response.error_for_status())
                                .and_then(|response| response.bytes().map(|_| ()))
                                .map_err(|error| error.to_string()),
                            MonitorProtocol::Icmp => Command::new("ping")
                                .args(["-c", "1", "-W", "10", &target])
                                .output()
                                .map_err(|error| error.to_string())
                                .and_then(|output| {
                                    if output.status.success() {
                                        Ok(())
                                    } else {
                                        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
                                    }
                                }),
                        };
                        let latency_millis = started.elapsed().as_millis() as u64;
                        let (word, detail) = match result {
                            Ok(_) => {
                                self.successes.fetch_add(1, Ordering::Relaxed);
                                ("OK", String::new())
                            }
                            Err(error) => {
                                self.failures.fetch_add(1, Ordering::Relaxed);
                                ("Failed", error.to_string())
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
                                protocol.as_str(),
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
                thread::sleep(Duration::from_secs(
                    self.interval_seconds.load(Ordering::Relaxed),
                ));
            }
        });
    }
}

pub fn valid_icmp_target(value: &str) -> bool {
    value.parse::<IpAddr>().is_ok()
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
