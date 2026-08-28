use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64 as RandomState;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_REMOTE: &str = "127.0.0.1";
const PORT: u16 = 9000;
pub(crate) const PACKET_SIZE: usize = 1024;
/// UDP packet header: 8-byte sequence number + 8-byte send timestamp (ms).
const UDP_HEADER_LEN: usize = 16;
/// TCP is a byte stream, so a timestamp is embedded every `TCP_TIMESTAMP_CHUNK` bytes.
const TCP_TIMESTAMP_CHUNK: usize = 100;
/// TCP frame header: 8-byte send timestamp (ms) + 2-byte chunk length.
const TCP_HEADER_LEN: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketType {
    Tcp,
    Udp,
}
impl PacketType {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Some(Self::Tcp),
            "udp" => Some(Self::Udp),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub rate: u64,
    pub packet_type: PacketType,
    pub tcp_bytes_per_second: u64,
    pub udp_packet_size: usize,
    pub client_runtime: u64,
    pub server_runtime: u64,
    pub jitter_millis: u64,
    pub client_jitter_millis: u64,
    pub server_jitter_millis: u64,
    pub max_tcp_jitter_millis: u64,
    pub max_udp_jitter_millis: u64,
    /// Minimum acceptable throughput in bytes/sec for a run to be considered a pass; 0 disables the check.
    pub limit_bytes_per_second: u64,
    pub admin_emails: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            rate: 100,
            packet_type: PacketType::Tcp,
            tcp_bytes_per_second: 1024,
            udp_packet_size: 1024,
            client_runtime: 0,
            server_runtime: 0,
            jitter_millis: 0,
            client_jitter_millis: 0,
            server_jitter_millis: 0,
            max_tcp_jitter_millis: 1000,
            max_udp_jitter_millis: 1000,
            limit_bytes_per_second: 0,
            admin_emails: Vec::new(),
        }
    }
}

pub struct StartGate {
    started: Mutex<bool>,
    wake: Condvar,
}
impl StartGate {
    pub fn new() -> Self {
        Self {
            started: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
    pub fn start(&self) {
        *self.started.lock().unwrap() = true;
        self.wake.notify_all();
    }
    fn wait(&self) {
        let mut started = self.started.lock().unwrap();
        while !*started {
            started = self.wake.wait(started).unwrap();
        }
    }
}

pub struct Metrics {
    sent_tcp_packets: AtomicU64,
    sent_tcp_bytes: AtomicU64,
    sent_udp_packets: AtomicU64,
    sent_udp_bytes: AtomicU64,
    received_tcp_packets: AtomicU64,
    received_tcp_bytes: AtomicU64,
    received_udp_packets: AtomicU64,
    received_udp_bytes: AtomicU64,
    lost_udp_packets: AtomicU64,
    out_of_order_udp_packets: AtomicU64,
    jitter_millis: AtomicU64,
    tcp_jitter_millis: AtomicU64,
    udp_jitter_millis: AtomicU64,
    run_sent_tcp_packets: AtomicU64,
    run_sent_tcp_bytes: AtomicU64,
    run_sent_udp_packets: AtomicU64,
    run_sent_udp_bytes: AtomicU64,
    run_received_tcp_packets: AtomicU64,
    run_received_tcp_bytes: AtomicU64,
    run_received_udp_packets: AtomicU64,
    run_received_udp_bytes: AtomicU64,
    last_udp_timestamps: Mutex<Option<(i64, i64)>>,
    last_tcp_timestamps: Mutex<Option<(i64, i64)>>,
}
impl Metrics {
    pub fn new() -> Self {
        Self {
            sent_tcp_packets: AtomicU64::new(0),
            sent_tcp_bytes: AtomicU64::new(0),
            sent_udp_packets: AtomicU64::new(0),
            sent_udp_bytes: AtomicU64::new(0),
            received_tcp_packets: AtomicU64::new(0),
            received_tcp_bytes: AtomicU64::new(0),
            received_udp_packets: AtomicU64::new(0),
            received_udp_bytes: AtomicU64::new(0),
            lost_udp_packets: AtomicU64::new(0),
            out_of_order_udp_packets: AtomicU64::new(0),
            jitter_millis: AtomicU64::new(0),
            tcp_jitter_millis: AtomicU64::new(0),
            udp_jitter_millis: AtomicU64::new(0),
            run_sent_tcp_packets: AtomicU64::new(0),
            run_sent_tcp_bytes: AtomicU64::new(0),
            run_sent_udp_packets: AtomicU64::new(0),
            run_sent_udp_bytes: AtomicU64::new(0),
            run_received_tcp_packets: AtomicU64::new(0),
            run_received_tcp_bytes: AtomicU64::new(0),
            run_received_udp_packets: AtomicU64::new(0),
            run_received_udp_bytes: AtomicU64::new(0),
            last_udp_timestamps: Mutex::new(None),
            last_tcp_timestamps: Mutex::new(None),
        }
    }
    fn add(&self, sent: bool, protocol: PacketType, bytes: usize) {
        let (packets, total, run_packets, run_total) = match (sent, protocol) {
            (true, PacketType::Tcp) => (
                &self.sent_tcp_packets,
                &self.sent_tcp_bytes,
                &self.run_sent_tcp_packets,
                &self.run_sent_tcp_bytes,
            ),
            (true, PacketType::Udp) => (
                &self.sent_udp_packets,
                &self.sent_udp_bytes,
                &self.run_sent_udp_packets,
                &self.run_sent_udp_bytes,
            ),
            (false, PacketType::Tcp) => (
                &self.received_tcp_packets,
                &self.received_tcp_bytes,
                &self.run_received_tcp_packets,
                &self.run_received_tcp_bytes,
            ),
            (false, PacketType::Udp) => (
                &self.received_udp_packets,
                &self.received_udp_bytes,
                &self.run_received_udp_packets,
                &self.run_received_udp_bytes,
            ),
        };
        packets.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(bytes as u64, Ordering::Relaxed);
        run_packets.fetch_add(1, Ordering::Relaxed);
        run_total.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    /// Total bytes sent (TCP, UDP) since the last `reset_run()`, unaffected by the
    /// periodic `snapshot()` used for live reporting.
    pub fn run_sent_bytes(&self) -> (u64, u64) {
        (
            self.run_sent_tcp_bytes.load(Ordering::Relaxed),
            self.run_sent_udp_bytes.load(Ordering::Relaxed),
        )
    }
    /// Total bytes received (TCP, UDP) since the last `reset_run()`, mirroring
    /// `run_sent_bytes` so the server side's throughput can be checked too.
    pub fn run_received_bytes(&self) -> (u64, u64) {
        (
            self.run_received_tcp_bytes.load(Ordering::Relaxed),
            self.run_received_udp_bytes.load(Ordering::Relaxed),
        )
    }
    /// Run-scoped counters in the same `[sent_tcp_packets, sent_tcp_bytes, ...]`
    /// layout as `snapshot()`, for the single final report written to the
    /// external metrics database at the end of a run.
    pub fn run_totals(&self) -> [u64; 8] {
        [
            self.run_sent_tcp_packets.load(Ordering::Relaxed),
            self.run_sent_tcp_bytes.load(Ordering::Relaxed),
            self.run_sent_udp_packets.load(Ordering::Relaxed),
            self.run_sent_udp_bytes.load(Ordering::Relaxed),
            self.run_received_tcp_packets.load(Ordering::Relaxed),
            self.run_received_tcp_bytes.load(Ordering::Relaxed),
            self.run_received_udp_packets.load(Ordering::Relaxed),
            self.run_received_udp_bytes.load(Ordering::Relaxed),
        ]
    }
    /// Clears the per-run counters (sent/received bytes, jitter, UDP loss) so the
    /// next run's final ok/fail evaluation and metrics report reflect only that run.
    pub fn reset_run(&self) {
        self.run_sent_tcp_packets.store(0, Ordering::Relaxed);
        self.run_sent_tcp_bytes.store(0, Ordering::Relaxed);
        self.run_sent_udp_packets.store(0, Ordering::Relaxed);
        self.run_sent_udp_bytes.store(0, Ordering::Relaxed);
        self.run_received_tcp_packets.store(0, Ordering::Relaxed);
        self.run_received_tcp_bytes.store(0, Ordering::Relaxed);
        self.run_received_udp_packets.store(0, Ordering::Relaxed);
        self.run_received_udp_bytes.store(0, Ordering::Relaxed);
        self.jitter_millis.store(0, Ordering::Relaxed);
        self.tcp_jitter_millis.store(0, Ordering::Relaxed);
        self.udp_jitter_millis.store(0, Ordering::Relaxed);
        self.lost_udp_packets.store(0, Ordering::Relaxed);
        self.out_of_order_udp_packets.store(0, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> [u64; 8] {
        [
            self.sent_tcp_packets.swap(0, Ordering::Relaxed),
            self.sent_tcp_bytes.swap(0, Ordering::Relaxed),
            self.sent_udp_packets.swap(0, Ordering::Relaxed),
            self.sent_udp_bytes.swap(0, Ordering::Relaxed),
            self.received_tcp_packets.swap(0, Ordering::Relaxed),
            self.received_tcp_bytes.swap(0, Ordering::Relaxed),
            self.received_udp_packets.swap(0, Ordering::Relaxed),
            self.received_udp_bytes.swap(0, Ordering::Relaxed),
        ]
    }
    pub fn udp_status(&self) -> (u64, u64) {
        (
            self.lost_udp_packets.load(Ordering::Relaxed),
            self.out_of_order_udp_packets.load(Ordering::Relaxed),
        )
    }
    fn udp_sequence(&self, expected: &mut Option<u64>, sequence: u64) {
        if let Some(previous) = *expected {
            if sequence > previous + 1 {
                self.lost_udp_packets
                    .fetch_add(sequence - previous - 1, Ordering::Relaxed);
            }
            if sequence <= previous {
                self.out_of_order_udp_packets
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        *expected = Some((*expected).map_or(sequence, |previous| previous.max(sequence)));
    }
    #[cfg(test)]
    pub(crate) fn test_udp_sequence(&self, expected: &mut Option<u64>, sequence: u64) {
        self.udp_sequence(expected, sequence);
    }
    fn record_jitter(&self, protocol: PacketType, jitter: Duration) {
        let value = jitter.as_millis() as u64;
        self.jitter_millis.fetch_max(value, Ordering::Relaxed);
        match protocol {
            PacketType::Tcp => self.tcp_jitter_millis.fetch_max(value, Ordering::Relaxed),
            PacketType::Udp => self.udp_jitter_millis.fetch_max(value, Ordering::Relaxed),
        };
    }
    #[cfg(test)]
    pub(crate) fn test_jitter(&self, protocol: PacketType, millis: u64) {
        self.record_jitter(protocol, Duration::from_millis(millis));
    }
    /// Computes jitter (RFC 3550 style delay variation) from a packet's send timestamp,
    /// so the receiving side can detect jitter without relying on local send intervals.
    fn timestamp_jitter(
        &self,
        protocol: PacketType,
        last: &Mutex<Option<(i64, i64)>>,
        send_timestamp_ms: i64,
    ) {
        let arrival_ms = Utc::now().timestamp_millis();
        let mut last = last.lock().unwrap();
        if let Some((last_send_ms, last_arrival_ms)) = *last {
            let delay_variation =
                (arrival_ms - last_arrival_ms) - (send_timestamp_ms - last_send_ms);
            self.record_jitter(protocol, Duration::from_millis(delay_variation.unsigned_abs()));
        }
        *last = Some((send_timestamp_ms, arrival_ms));
    }
    fn udp_timestamp_jitter(&self, send_timestamp_ms: i64) {
        self.timestamp_jitter(PacketType::Udp, &self.last_udp_timestamps, send_timestamp_ms);
    }
    fn tcp_timestamp_jitter(&self, send_timestamp_ms: i64) {
        self.timestamp_jitter(PacketType::Tcp, &self.last_tcp_timestamps, send_timestamp_ms);
    }
    pub fn jitter_millis(&self) -> u64 {
        self.jitter_millis.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(crate) fn protocol_jitter_millis(&self) -> (u64, u64) {
        (
            self.tcp_jitter_millis.load(Ordering::Relaxed),
            self.udp_jitter_millis.load(Ordering::Relaxed),
        )
    }
    pub fn tcp_jitter_millis(&self) -> u64 {
        self.tcp_jitter_millis.load(Ordering::Relaxed)
    }
    pub fn udp_jitter_millis(&self) -> u64 {
        self.udp_jitter_millis.load(Ordering::Relaxed)
    }
}

pub struct SqlState {
    enabled: AtomicBool,
    connection: Mutex<Option<Connection>>,
    run_id: AtomicU64,
}
impl SqlState {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            connection: Mutex::new(None),
            run_id: AtomicU64::new(0),
        }
    }
    pub fn enable(&self) -> Result<(), String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS run_counter (id INTEGER PRIMARY KEY CHECK (id = 1), next_id INTEGER NOT NULL); INSERT OR IGNORE INTO run_counter (id, next_id) VALUES (1, 1);").map_err(|e| e.to_string())?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, started_utc TEXT NOT NULL); CREATE TABLE IF NOT EXISTS alarms (timestamp_utc TEXT NOT NULL, target TEXT NOT NULL, error TEXT NOT NULL);").map_err(|e| e.to_string())?;
        let _ = connection.execute("UPDATE run_counter SET next_id = MAX(next_id, COALESCE((SELECT MAX(id) + 1 FROM runs), 1)) WHERE id = 1", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN completed_utc TEXT", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN result TEXT NOT NULL DEFAULT 'running'", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN sent_bytes INTEGER DEFAULT 0", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN failure_reason TEXT", []);
        let _ = connection.execute(
            "UPDATE runs SET completed_utc = COALESCE(completed_utc, ?1), result = 'aborted' WHERE result = 'running'",
            params![timestamp()],
        );
        *self.connection.lock().unwrap() = Some(connection);
        self.enabled.store(true, Ordering::Relaxed);
        Ok(())
    }
    pub fn next_run_id(&self, fallback: u64) -> u64 {
        let connection = self.connection.lock().unwrap();
        connection.as_ref().and_then(|c| c.query_row("UPDATE run_counter SET next_id = next_id + 1 WHERE id = 1 RETURNING next_id - 1", [], |row| row.get(0)).ok()).unwrap_or(fallback)
    }
    pub fn start_run(&self, id: u64) {
        self.run_id.store(id, Ordering::Relaxed);
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let _ = c.execute(
                "INSERT INTO runs (id, started_utc, result) VALUES (?1, ?2, 'running')",
                params![id, timestamp()],
            );
        }
    }
    /// Records the end state of a run: bytes sent, whether it stayed within its
    /// configured limits, and why not, if it didn't. No millisecond-level metrics
    /// are ever stored here; those only go to the configured external SQL database.
    pub fn complete_run(&self, id: u64, result: &str, sent_bytes: u64, failure_reason: Option<&str>) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let _ = c.execute(
                "UPDATE runs SET completed_utc = ?1, result = ?2, sent_bytes = ?3, failure_reason = ?4 WHERE id = ?5 AND result = 'running'",
                params![timestamp(), result, sent_bytes, failure_reason, id],
            );
        }
    }
    pub fn clean(&self) -> Result<(), String> {
        let connection = self.connection.lock().unwrap();
        connection
            .as_ref()
            .ok_or_else(|| "local SQL is disabled".to_string())?
            .execute_batch("DELETE FROM alarms; DELETE FROM runs;")
            .map_err(|error| error.to_string())
    }
    pub fn list_runs(&self) -> Result<(), String> {
        let c = Connection::open(database_path()).map_err(|e| e.to_string())?;
        let mut q = c
            .prepare("SELECT started_utc, id, result, sent_bytes, failure_reason FROM runs ORDER BY id")
            .map_err(|e| e.to_string())?;
        let rows = q
            .query_map([], |row| {
                Ok(format!(
                    "{} {} {} sent_bytes={}{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<u64>>(3)?.unwrap_or(0),
                    row.get::<_, Option<String>>(4)?
                        .map(|reason| format!(" ({reason})"))
                        .unwrap_or_default()
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            crate::cli_textout::line(row.map_err(|e| e.to_string())?);
        }
        Ok(())
    }
    /// Reads a single run's final summary from local SQLite only; per-second
    /// metrics never live here, so this is just the started/completed/result row.
    pub fn show_run(&self, id: u64) -> Result<Option<String>, String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        connection
            .query_row(
                "SELECT started_utc, completed_utc, result, sent_bytes, failure_reason FROM runs WHERE id = ?1",
                params![id],
                |row| {
                    Ok(format!(
                        "started {} completed {} result {} sent_bytes={}{}",
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?
                            .unwrap_or_else(|| "n/a".to_string()),
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<u64>>(3)?.unwrap_or(0),
                        row.get::<_, Option<String>>(4)?
                            .map(|reason| format!(" ({reason})"))
                            .unwrap_or_default()
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())
    }
    pub fn record_alarm(&self, timestamp_utc: &str, target: &str, error: &str) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let mut connection = self.connection.lock().unwrap();
        if connection.is_none() {
            if let Ok(value) = Connection::open(database_path()) {
                let _ = value.execute_batch("CREATE TABLE IF NOT EXISTS alarms (timestamp_utc TEXT NOT NULL, target TEXT NOT NULL, error TEXT NOT NULL)");
                *connection = Some(value);
            }
        }
        if let Some(connection) = connection.as_ref() {
            let _ = connection.execute(
                "INSERT INTO alarms VALUES (?1, ?2, ?3)",
                params![timestamp_utc, target, error],
            );
        }
    }
}

pub fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn database_path() -> PathBuf {
    let base = std::env::current_exe()
        .ok()
        .and_then(|path| {
            let parent = path.parent()?;
            if parent.file_name().is_some_and(|name| name == "deps") {
                parent.parent().map(PathBuf::from)
            } else {
                Some(parent.to_path_buf())
            }
        })
        .unwrap_or_else(|| std::env::current_dir().unwrap())
        .join("log");
    let _ = std::fs::create_dir_all(&base);
    base.join("netmark.sqlite")
}
pub fn spawn_server(
    config: Arc<Mutex<Config>>,
    gate: Arc<StartGate>,
    stopping: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    log_dir: PathBuf,
) {
    let http_stopping = Arc::clone(&stopping);
    thread::spawn(move || run_http_server(&http_stopping));
    thread::spawn(move || {
        let config = config.lock().unwrap().clone();
        let log = File::options()
            .create(true)
            .append(true)
            .open(log_dir.join("server.log"))
            .unwrap();
        match config.packet_type {
            PacketType::Tcp => tcp_server(&gate, &stopping, &metrics, log, config.server_runtime),
            PacketType::Udp => udp_server(&gate, &stopping, &metrics, log, config.server_runtime),
        }
    });
}

fn run_http_server(stopping: &AtomicBool) {
    let listener = match TcpListener::bind("0.0.0.0:8080") {
        Ok(listener) => listener,
        Err(_) => return,
    };
    listener.set_nonblocking(true).ok();
    while !stopping.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request);
                let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: 24\r\nConnection: close\r\n\r\n<html>Hello World</html>";
                let _ = stream.write_all(response);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}
pub fn spawn_client(
    config: Arc<Mutex<Config>>,
    gate: Arc<StartGate>,
    stopping: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    remote: String,
    log_dir: PathBuf,
) {
    thread::spawn(move || {
        gate.wait();
        let config = config.lock().unwrap().clone();
        let log = File::options()
            .create(true)
            .append(true)
            .open(log_dir.join("client.log"))
            .unwrap();
        match config.packet_type {
            PacketType::Tcp => tcp_client(
                config.tcp_bytes_per_second,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.client_jitter_millis,
            ),
            PacketType::Udp => udp_client(
                config.rate,
                config.udp_packet_size,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.server_jitter_millis,
            ),
        }
    });
}

fn address(remote: &str) -> String {
    format!("{remote}:{PORT}")
}
fn expired(started: Instant, runtime: u64) -> bool {
    runtime != 0 && started.elapsed() >= Duration::from_secs(runtime)
}
fn tcp_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
) {
    let listener = match TcpListener::bind(address("0.0.0.0")) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("server error: {e}");
            return;
        }
    };
    listener.set_nonblocking(true).ok();
    gate.wait();
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_nonblocking(true).ok();
                let mut buffer = [0; PACKET_SIZE];
                let mut frame_reader = TcpFrameReader::new();
                while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            metrics.add(false, PacketType::Tcp, n);
                            frame_reader.feed(&buffer[..n], metrics);
                            writeln!(log, "{} TCP {n} bytes", timestamp()).ok();
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10))
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}
fn udp_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
) {
    let socket = match UdpSocket::bind(address("0.0.0.0")) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("server error: {e}");
            return;
        }
    };
    socket.set_nonblocking(true).ok();
    gate.wait();
    let started = Instant::now();
    let mut buffer = [0; PACKET_SIZE];
    let mut expected_sequence = None;
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match socket.recv_from(&mut buffer) {
            Ok((n, _)) => {
                metrics.add(false, PacketType::Udp, n);
                if n >= UDP_HEADER_LEN {
                    metrics.udp_sequence(
                        &mut expected_sequence,
                        u64::from_be_bytes(buffer[..8].try_into().unwrap()),
                    );
                    metrics.udp_timestamp_jitter(i64::from_be_bytes(
                        buffer[8..16].try_into().unwrap(),
                    ));
                }
                writeln!(log, "{} UDP {n} bytes", timestamp()).ok();
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}
fn tcp_client(
    bytes_per_second: u64,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
) {
    let started = Instant::now();
    let mut stream = loop {
        if stopping.load(Ordering::Relaxed) || expired(started, runtime) {
            return;
        }
        match TcpStream::connect(address(remote)) {
            Ok(v) => break v,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    };
    send_tcp_packets(
        bytes_per_second,
        stopping,
        metrics,
        &mut log,
        runtime,
        jitter_millis,
        |p| stream.write_all(p),
    );
}
fn udp_client(
    rate: u64,
    packet_size: usize,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
) {
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(v) => v,
        Err(_) => return,
    };
    if socket.connect(address(remote)).is_err() {
        return;
    }
    send_udp_packets(
        rate,
        packet_size,
        stopping,
        metrics,
        &mut log,
        runtime,
        jitter_millis,
        |p| socket.send(p).map(|_| ()),
    );
}
pub(crate) fn send_tcp_packets<F>(
    bytes_per_second: u64,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    log: &mut File,
    runtime: u64,
    jitter_millis: u64,
    mut send: F,
) where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let packet_size = bytes_per_second.min(PACKET_SIZE as u64).max(1) as usize;
    let interval = Duration::from_secs_f64(packet_size as f64 / bytes_per_second.max(1) as f64);
    let started = Instant::now();
    let mut previous_send = started;
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        let packet = build_tcp_frame(packet_size);
        if send(&packet).is_err() {
            return;
        }
        metrics.add(true, PacketType::Tcp, packet.len());
        let now = Instant::now();
        metrics.record_jitter(
            PacketType::Tcp,
            now.duration_since(previous_send).abs_diff(interval),
        );
        previous_send = now;
        writeln!(log, "{} TCP {} bytes", timestamp(), packet.len()).ok();
        thread::sleep(jittered_delay(interval, jitter_millis));
    }
}
fn send_udp_packets<F>(
    rate: u64,
    packet_size: usize,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    log: &mut File,
    runtime: u64,
    jitter_millis: u64,
    mut send: F,
) where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let packet_size = packet_size.max(UDP_HEADER_LEN);
    let interval = Duration::from_secs_f64(1.0 / rate.max(1) as f64);
    let mut sequence = 0u64;
    let started = Instant::now();
    let mut previous_send = started;
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        let mut packet = vec![0u8; packet_size];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&Utc::now().timestamp_millis().to_be_bytes());
        if send(&packet).is_err() {
            return;
        }
        metrics.add(true, PacketType::Udp, packet.len());
        let now = Instant::now();
        metrics.record_jitter(
            PacketType::Udp,
            now.duration_since(previous_send).abs_diff(interval),
        );
        previous_send = now;
        writeln!(log, "{} UDP {} bytes", timestamp(), packet.len()).ok();
        sequence += 1;
        thread::sleep(jittered_delay(interval, jitter_millis));
    }
}

/// Builds a TCP payload of `payload_len` bytes with a send timestamp embedded
/// every `TCP_TIMESTAMP_CHUNK` bytes, so the receiver can measure jitter on a stream.
fn build_tcp_frame(payload_len: usize) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(payload_len + payload_len.div_ceil(TCP_TIMESTAMP_CHUNK) * TCP_HEADER_LEN);
    let mut remaining = payload_len;
    while remaining > 0 {
        let chunk = remaining.min(TCP_TIMESTAMP_CHUNK);
        buffer.extend_from_slice(&Utc::now().timestamp_millis().to_be_bytes());
        buffer.extend_from_slice(&(chunk as u16).to_be_bytes());
        buffer.extend(std::iter::repeat_n(0u8, chunk));
        remaining -= chunk;
    }
    buffer
}

enum TcpFrameState {
    Header(Vec<u8>),
    Payload { remaining: usize },
}

/// Parses the timestamp-framed TCP byte stream produced by `build_tcp_frame`,
/// tolerating frames split arbitrarily across reads.
struct TcpFrameReader {
    state: TcpFrameState,
}
impl TcpFrameReader {
    fn new() -> Self {
        Self {
            state: TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN)),
        }
    }
    fn feed(&mut self, data: &[u8], metrics: &Metrics) {
        let mut offset = 0;
        while offset < data.len() {
            match &mut self.state {
                TcpFrameState::Header(buffer) => {
                    let need = TCP_HEADER_LEN - buffer.len();
                    let take = need.min(data.len() - offset);
                    buffer.extend_from_slice(&data[offset..offset + take]);
                    offset += take;
                    if buffer.len() == TCP_HEADER_LEN {
                        let timestamp_ms = i64::from_be_bytes(buffer[..8].try_into().unwrap());
                        let length = u16::from_be_bytes(buffer[8..10].try_into().unwrap()) as usize;
                        metrics.tcp_timestamp_jitter(timestamp_ms);
                        self.state = if length == 0 {
                            TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN))
                        } else {
                            TcpFrameState::Payload { remaining: length }
                        };
                    }
                }
                TcpFrameState::Payload { remaining } => {
                    let take = (*remaining).min(data.len() - offset);
                    offset += take;
                    *remaining -= take;
                    if *remaining == 0 {
                        self.state = TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN));
                    }
                }
            }
        }
    }
}

fn jittered_delay(base: Duration, jitter_millis: u64) -> Duration {
    if jitter_millis == 0 {
        return base;
    }
    static RANDOM_STATE: RandomState = RandomState::new(0x9e3779b97f4a7c15);
    let range = jitter_millis.saturating_mul(2).saturating_add(1);
    let state = RANDOM_STATE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.wrapping_mul(6364136223846793005).wrapping_add(1))
        })
        .unwrap_or(0);
    let offset = (state % range) as i64 - jitter_millis as i64;
    if offset.is_negative() {
        base.saturating_sub(Duration::from_millis(offset.unsigned_abs()))
    } else {
        base.saturating_add(Duration::from_millis(offset as u64))
    }
}
