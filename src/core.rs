use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, params};
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_REMOTE: &str = "127.0.0.1";
const PORT: u16 = 9000;
const PACKET_SIZE: usize = 1024;

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
}

#[derive(Clone, Copy)]
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
    last_udp_arrival: Mutex<Option<(Instant, Duration)>>,
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
            last_udp_arrival: Mutex::new(None),
        }
    }
    fn add(&self, sent: bool, protocol: PacketType, bytes: usize) {
        let (packets, total) = match (sent, protocol) {
            (true, PacketType::Tcp) => (&self.sent_tcp_packets, &self.sent_tcp_bytes),
            (true, PacketType::Udp) => (&self.sent_udp_packets, &self.sent_udp_bytes),
            (false, PacketType::Tcp) => (&self.received_tcp_packets, &self.received_tcp_bytes),
            (false, PacketType::Udp) => (&self.received_udp_packets, &self.received_udp_bytes),
        };
        packets.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(bytes as u64, Ordering::Relaxed);
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
    pub fn current(&self) -> [u64; 8] {
        [
            self.sent_tcp_packets.load(Ordering::Relaxed),
            self.sent_tcp_bytes.load(Ordering::Relaxed),
            self.sent_udp_packets.load(Ordering::Relaxed),
            self.sent_udp_bytes.load(Ordering::Relaxed),
            self.received_tcp_packets.load(Ordering::Relaxed),
            self.received_tcp_bytes.load(Ordering::Relaxed),
            self.received_udp_packets.load(Ordering::Relaxed),
            self.received_udp_bytes.load(Ordering::Relaxed),
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
    fn udp_arrival(&self) {
        let now = Instant::now();
        let mut last = self.last_udp_arrival.lock().unwrap();
        if let Some((previous, interval)) = *last {
            let current = now.duration_since(previous);
            self.jitter_millis.fetch_max(
                current.abs_diff(interval).as_millis() as u64,
                Ordering::Relaxed,
            );
            *last = Some((now, current));
        } else {
            *last = Some((now, Duration::ZERO));
        }
    }
    pub fn jitter_millis(&self) -> u64 {
        self.jitter_millis.load(Ordering::Relaxed)
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
        let _ = connection.execute("UPDATE run_counter SET next_id = MAX(next_id, COALESCE((SELECT MAX(id) + 1 FROM runs), 1)) WHERE id = 1", []);
        let _ = connection.execute("UPDATE run_counter SET next_id = MAX(next_id, COALESCE((SELECT MAX(id) + 1 FROM runs), 1)) WHERE id = 1", []);
        connection.execute_batch("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, started_utc TEXT NOT NULL); CREATE TABLE IF NOT EXISTS metrics (run_id INTEGER NOT NULL, timestamp_utc TEXT NOT NULL, sent_tcp_packets INTEGER, sent_tcp_bytes INTEGER, sent_udp_packets INTEGER, sent_udp_bytes INTEGER, received_tcp_packets INTEGER, received_tcp_bytes INTEGER, received_udp_packets INTEGER, received_udp_bytes INTEGER, lost_udp_packets INTEGER DEFAULT 0, out_of_order_udp_packets INTEGER DEFAULT 0, jitter_millis INTEGER DEFAULT 0); CREATE TABLE IF NOT EXISTS alarms (timestamp_utc TEXT NOT NULL, target TEXT NOT NULL, error TEXT NOT NULL);").map_err(|e| e.to_string())?;
        for column in [
            "lost_udp_packets",
            "out_of_order_udp_packets",
            "jitter_millis",
        ] {
            let _ = connection.execute(
                &format!("ALTER TABLE metrics ADD COLUMN {column} INTEGER DEFAULT 0"),
                [],
            );
        }
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN completed_utc TEXT", []);
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN result TEXT NOT NULL DEFAULT 'running'",
            [],
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
    pub fn complete_run(&self, id: u64, result: &str) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let _ = c.execute(
                "UPDATE runs SET completed_utc = ?1, result = ?2 WHERE id = ?3",
                params![timestamp(), result, id],
            );
        }
    }
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }
    pub fn clean(&self) -> Result<(), String> {
        let connection = self.connection.lock().unwrap();
        connection
            .as_ref()
            .ok_or_else(|| "local SQL is disabled".to_string())?
            .execute_batch("DELETE FROM metrics; DELETE FROM alarms; DELETE FROM runs;")
            .map_err(|error| error.to_string())
    }
    pub fn list_runs(&self) -> Result<(), String> {
        let c = Connection::open(database_path()).map_err(|e| e.to_string())?;
        let mut q = c
            .prepare("SELECT started_utc, id, result FROM runs ORDER BY id")
            .map_err(|e| e.to_string())?;
        let rows = q
            .query_map([], |row| {
                Ok(format!(
                    "{} {} {}",
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            crate::cli_textout::line(row.map_err(|e| e.to_string())?);
        }
        Ok(())
    }
    pub fn current_run_id(&self) -> u64 {
        self.run_id.load(Ordering::Relaxed)
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
    pub fn write_snapshot(&self, values: &[u64; 8], lost: u64, out_of_order: u64, jitter: u64) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let _ = c.execute(
                "INSERT INTO metrics (run_id, timestamp_utc, sent_tcp_packets, sent_tcp_bytes, sent_udp_packets, sent_udp_bytes, received_tcp_packets, received_tcp_bytes, received_udp_packets, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    self.run_id.load(Ordering::Relaxed),
                    timestamp(),
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    values[7], lost, out_of_order, jitter
                ],
            );
        }
    }
    pub fn show_run(&self, id: u64) -> Result<(), String> {
        let c = Connection::open(database_path()).map_err(|e| e.to_string())?;
        for column in [
            "lost_udp_packets",
            "out_of_order_udp_packets",
            "jitter_millis",
        ] {
            let _ = c.execute(
                &format!("ALTER TABLE metrics ADD COLUMN {column} INTEGER DEFAULT 0"),
                [],
            );
        }
        let mut q = c.prepare("SELECT timestamp_utc, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis FROM metrics WHERE run_id=?1 ORDER BY timestamp_utc").map_err(|e| e.to_string())?;
        let rows = q
            .query_map(params![id], |r| {
                Ok(format!(
                    "{} sent TCP={} UDP={}, received TCP={} UDP={}, lost UDP={}, out-of-order UDP={}, jitter={} ms{}",
                    r.get::<_, String>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, u64>(2)?,
                    r.get::<_, u64>(3)?,
                    r.get::<_, u64>(4)?,
                    r.get::<_, u64>(5)?,
                    r.get::<_, u64>(6)?,
                    r.get::<_, u64>(7)?,
                    if r.get::<_, u64>(7)? > 10 { " WARNING" } else { "" }
                ))
            })
            .map_err(|e| e.to_string())?;
        crate::cli_textout::line(format!("Run {id}:"));
        for row in rows {
            crate::cli_textout::line(row.map_err(|e| e.to_string())?);
        }
        Ok(())
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
        let config = *config.lock().unwrap();
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
        let config = *config.lock().unwrap();
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
                while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            metrics.add(false, PacketType::Tcp, n);
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
                metrics.udp_arrival();
                if n >= 8 {
                    metrics.udp_sequence(
                        &mut expected_sequence,
                        u64::from_be_bytes(buffer[..8].try_into().unwrap()),
                    );
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
fn send_tcp_packets<F>(
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
    let packet = vec![0u8; packet_size];
    let interval = Duration::from_secs_f64(packet_size as f64 / bytes_per_second.max(1) as f64);
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        if send(&packet).is_err() {
            return;
        }
        metrics.add(true, PacketType::Tcp, packet.len());
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
    let packet_size = packet_size.max(8);
    let interval = Duration::from_secs_f64(1.0 / rate.max(1) as f64);
    let mut sequence = 0u64;
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        let mut packet = vec![0u8; packet_size];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        if send(&packet).is_err() {
            return;
        }
        metrics.add(true, PacketType::Udp, packet.len());
        writeln!(log, "{} UDP {} bytes", timestamp(), packet.len()).ok();
        sequence += 1;
        thread::sleep(jittered_delay(interval, jitter_millis));
    }
}

fn jittered_delay(base: Duration, jitter_millis: u64) -> Duration {
    if jitter_millis == 0 {
        return base;
    }
    let range = jitter_millis.saturating_mul(2).saturating_add(1);
    let offset =
        (Instant::now().elapsed().subsec_nanos() as u64 % range) as i64 - jitter_millis as i64;
    if offset.is_negative() {
        base.saturating_sub(Duration::from_millis(offset.unsigned_abs()))
    } else {
        base.saturating_add(Duration::from_millis(offset as u64))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_defaults_are_local() {
        assert_eq!(DEFAULT_REMOTE, "127.0.0.1");
    }

    #[test]
    fn persisted_run_ids_are_incremental() {
        let sql = SqlState::new();
        sql.enable().unwrap();
        let first = sql.next_run_id(1);
        sql.start_run(first);
        let second = sql.next_run_id(first + 1);
        assert_eq!(second, first + 1);
    }

    #[test]
    fn timestamps_are_explicit_utc() {
        let value = timestamp();
        assert!(value.ends_with('Z'));
        assert!(chrono::DateTime::parse_from_rfc3339(&value).is_ok());
    }

    #[test]
    fn ten_second_udp_client_server_logs_match() {
        let config = Arc::new(Mutex::new(Config {
            rate: 10,
            packet_type: PacketType::Udp,
            tcp_bytes_per_second: 1024,
            udp_packet_size: 1024,
            client_runtime: 10,
            server_runtime: 10,
            jitter_millis: 0,
            client_jitter_millis: 0,
            server_jitter_millis: 0,
        }));
        let gate = Arc::new(StartGate::new());
        let stopping = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Metrics::new());
        let test_dir = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("log");
        std::fs::create_dir_all(&test_dir).unwrap();
        let database_path = database_path();
        let sql = SqlState::new();
        sql.enable().unwrap();
        let run_id = sql.next_run_id(1);
        File::create(test_dir.join("client.log")).unwrap();
        File::create(test_dir.join("server.log")).unwrap();
        File::create(test_dir.join("netmark.log")).unwrap();
        File::create(test_dir.join("cli.log")).unwrap();
        for file_name in ["cli.log", "client.log", "server.log", "netmark.log"] {
            let mut file = File::options()
                .append(true)
                .open(test_dir.join(file_name))
                .unwrap();
            writeln!(file, "{} Starting run {}", timestamp(), run_id).unwrap();
        }
        spawn_server(
            Arc::clone(&config),
            Arc::clone(&gate),
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            test_dir.clone(),
        );
        spawn_client(
            Arc::clone(&config),
            Arc::clone(&gate),
            Arc::clone(&stopping),
            Arc::clone(&metrics),
            DEFAULT_REMOTE.to_string(),
            test_dir.clone(),
        );
        thread::sleep(Duration::from_millis(100));
        gate.start();
        thread::sleep(Duration::from_secs(11));
        stopping.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(250));
        let sent = metrics.current();
        sql.start_run(run_id);
        sql.write_snapshot(&sent, 0, 0, 0);
        let client_log_path = test_dir.join("client.log");
        let server_log_path = test_dir.join("server.log");
        let client_log = std::fs::read_to_string(client_log_path).unwrap();
        let server_log = std::fs::read_to_string(server_log_path).unwrap();
        let client_bytes = client_log
            .lines()
            .filter_map(|line| line.split_whitespace().nth(2)?.parse::<u64>().ok())
            .sum::<u64>();
        let server_bytes = server_log
            .lines()
            .filter_map(|line| line.split_whitespace().nth(2)?.parse::<u64>().ok())
            .sum::<u64>();
        let database_bytes: u64 = Connection::open(database_path)
            .unwrap()
            .query_row(
                "SELECT COALESCE(SUM(sent_udp_bytes), 0) FROM metrics WHERE run_id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .unwrap();
        let result = sent[3] > 0
            && client_bytes > 0
            && server_bytes > 0
            && sent[3] == client_bytes
            && client_bytes == server_bytes
            && database_bytes == sent[3];
        writeln!(
            File::options()
                .append(true)
                .open(test_dir.join("netmark.log"))
                .unwrap(),
            "{} Run-Id {} result {}",
            timestamp(),
            run_id,
            if result { "PASS" } else { "FAIL" }
        )
        .unwrap();
        for file_name in ["cli.log", "client.log", "server.log", "netmark.log"] {
            let mut file = File::options()
                .append(true)
                .open(test_dir.join(file_name))
                .unwrap();
            writeln!(file, "{} Completed run {}", timestamp(), run_id).unwrap();
        }
        assert!(
            std::fs::read_to_string(test_dir.join("netmark.log"))
                .unwrap()
                .contains("result PASS")
        );
        let lifecycle = std::fs::read_to_string(test_dir.join("netmark.log")).unwrap();
        assert_eq!(
            lifecycle.matches(&format!("Starting run {run_id}")).count(),
            1
        );
        assert_eq!(
            lifecycle
                .matches(&format!("Completed run {run_id}"))
                .count(),
            1
        );
        assert!(!lifecycle.contains("run 0"));
        for file_name in ["cli.log", "client.log", "server.log", "netmark.log"] {
            let contents = std::fs::read_to_string(test_dir.join(file_name)).unwrap();
            assert!(
                contents.contains(&format!("Starting run {run_id}")),
                "{file_name} is missing start marker"
            );
            assert!(
                contents.contains(&format!("Completed run {run_id}")),
                "{file_name} is missing completion marker"
            );
        }
        assert!(
            result,
            "run {run_id} did not produce matching nonzero logs and SQLite data"
        );
    }

    #[test]
    fn ten_second_tcp_loopback_logs_match() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Metrics::new());
        let test_dir =
            std::env::temp_dir().join(format!("netmark-tcp-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).unwrap();
        let client_log_path = test_dir.join("client.log");
        let sender_stopping = Arc::clone(&stopping);
        let sender_metrics = Arc::clone(&metrics);
        let sender = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            let mut log = File::create(client_log_path).unwrap();
            send_tcp_packets(
                1024,
                &sender_stopping,
                &sender_metrics,
                &mut log,
                10,
                0,
                |packet| stream.write_all(packet),
            );
        });
        let (mut stream, _) = listener.accept().unwrap();
        let mut server_log = File::create(test_dir.join("server.log")).unwrap();
        let mut buffer = [0; PACKET_SIZE];
        while let Ok(bytes) = stream.read(&mut buffer) {
            if bytes == 0 {
                break;
            }
            writeln!(server_log, "{} TCP {bytes} bytes", timestamp()).unwrap();
        }
        sender.join().unwrap();
        server_log.flush().unwrap();
        let client_bytes = std::fs::read_to_string(test_dir.join("client.log"))
            .unwrap()
            .lines()
            .filter_map(|line| line.split_whitespace().nth(2)?.parse::<u64>().ok())
            .sum::<u64>();
        let server_bytes = std::fs::read_to_string(test_dir.join("server.log"))
            .unwrap()
            .lines()
            .filter_map(|line| line.split_whitespace().nth(2)?.parse::<u64>().ok())
            .sum::<u64>();
        let sent_bytes = metrics.current()[1];
        assert!(sent_bytes > 0, "client sent zero TCP bytes");
        assert_eq!(sent_bytes, client_bytes);
        assert_eq!(client_bytes, server_bytes);
    }
}
