use crate::core::SqlState;
use reqwest::blocking::Client;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::Duration;

pub struct MonitorState {
    target: Mutex<Option<String>>,
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
    pub fn start(&self, log_dir: &PathBuf) -> Option<u64> {
        if self.running.swap(true, Ordering::Relaxed) {
            None
        } else {
            let id = self.next_monitor_id.fetch_add(1, Ordering::Relaxed);
            self.monitor_id.store(id, Ordering::Relaxed);
            append(
                log_dir,
                "monitor.log",
                &format!("{} monitor-id={} Started", crate::core::timestamp(), id),
            );
            Some(id)
        }
    }
    pub fn stop(&self, log_dir: &PathBuf) {
        if self.running.swap(false, Ordering::Relaxed) {
            append(
                log_dir,
                "monitor.log",
                &format!(
                    "{} monitor-id={} Stopped",
                    crate::core::timestamp(),
                    self.monitor_id.load(Ordering::Relaxed)
                ),
            );
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
    pub fn spawn_worker(self: Arc<Self>, log_dir: PathBuf, sql: Arc<SqlState>) {
        thread::spawn(move || {
            let client = match Client::builder().timeout(Duration::from_secs(10)).build() {
                Ok(client) => client,
                Err(_) => return,
            };
            while !std::thread::panicking() {
                if self.running.load(Ordering::Relaxed) {
                    if let Some(target) = self.target.lock().unwrap().clone() {
                        let call_id = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                        let timestamp = crate::core::timestamp();
                        let result = client
                            .get(&target)
                            .send()
                            .and_then(|response| response.error_for_status())
                            .and_then(|response| response.bytes().map(|_| ()));
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
                            sql.record_alarm(&timestamp, &target, &detail);
                        }
                    }
                }
                thread::sleep(Duration::from_secs(30));
            }
        });
    }
}

fn append(log_dir: &PathBuf, name: &str, line: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join(name))
    {
        use std::io::Write;
        let _ = writeln!(file, "{line}");
    }
}
