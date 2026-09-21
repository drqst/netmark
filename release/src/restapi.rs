//! A small REST API over [`crate::sdk`], so netmark can be driven from anything
//! that speaks HTTP. The contract is documented in `doc/openapi.yaml`, which is
//! compiled in and served from `/api/v1/openapi.yaml`.
//!
//! The API is unauthenticated and can start traffic runs, so it binds to the
//! loopback interface unless it is explicitly given another address.

use crate::cli::Clients;
use crate::configuration::{RestApiConfig, TestProfile};
use crate::core::{Metrics, PacketType, SqlState};
use crate::sdk::TestRunner;
use crate::session::Session;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// The OpenAPI contract lives in the delivered `doc/` folder and is embedded here
/// so the running service can never disagree with the shipped documentation.
pub const OPENAPI: &str = include_str!("../doc/openapi.yaml");

/// The web interface: a single self-contained page with a web CLI that drives
/// [`RestApi::cli_command`]. Embedded so the binary stays self-sufficient.
pub const WEB_UI: &str = include_str!("webcli.html");

const MAX_BODY: u64 = 64 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_LINE: u64 = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RestApi {
    running: Arc<AtomicBool>,
    address: Mutex<String>,
    /// One run at a time: the traffic ports cannot be shared between runs.
    busy: Arc<AtomicBool>,
    clients: Arc<Clients>,
    log_dir: PathBuf,
    live: Arc<LiveStatus>,
    /// When attached, `POST /api/v1/cli` runs every command through the same
    /// dispatcher as the interactive shell CLI. Absent for the read-only,
    /// state-free command set the tests rely on.
    session: Mutex<Option<Arc<Session>>>,
}

/// What this instance is doing right now. The CLI and the SDK keep it up to
/// date, and the web interface polls it so the status section on the page
/// follows the run without a reload.
#[derive(Default)]
pub struct LiveStatus {
    inner: Mutex<LiveStatusInner>,
}

#[derive(Default)]
struct LiveStatusInner {
    running: bool,
    run_id: u64,
    packet_type: String,
    server_enabled: bool,
    enabled_clients: usize,
    elapsed: Duration,
    metrics: Option<Arc<Metrics>>,
}

impl LiveStatus {
    /// Hands the live counters to the status page; they are read at request time
    /// so the numbers are current without any extra bookkeeping.
    pub fn attach_metrics(&self, metrics: Arc<Metrics>) {
        self.inner.lock().unwrap().metrics = Some(metrics);
    }

    pub fn update(
        &self,
        running: bool,
        run_id: u64,
        packet_type: PacketType,
        server_enabled: bool,
        enabled_clients: usize,
        elapsed: Option<Duration>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.running = running;
        inner.run_id = run_id;
        inner.packet_type = packet_type.as_str().to_string();
        inner.server_enabled = server_enabled;
        inner.enabled_clients = enabled_clients;
        inner.elapsed = elapsed.unwrap_or_default();
    }

    /// One sentence describing the current activity, shared by the web page, the
    /// web CLI and `netmarkctl`.
    pub fn activity(&self) -> String {
        let inner = self.inner.lock().unwrap();
        if !inner.running {
            return "idle - no run in progress".to_string();
        }
        let transport = if inner.packet_type.is_empty() {
            "tcp".to_string()
        } else {
            inner.packet_type.to_uppercase()
        };
        format!(
            "running {transport} run {} for {} s with {} client(s), server {}",
            inner.run_id,
            inner.elapsed.as_secs(),
            inner.enabled_clients,
            if inner.server_enabled {
                "enabled"
            } else {
                "disabled"
            }
        )
    }

    fn json(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let (sent, received) = inner
            .metrics
            .as_ref()
            .map(|metrics| (metrics.run_sent_total(), metrics.run_received_total()))
            .unwrap_or((0, 0));
        json!({
            "running": inner.running,
            "run_id": inner.run_id,
            "elapsed_seconds": inner.elapsed.as_secs(),
            "packet_type": if inner.packet_type.is_empty() { "tcp" } else { inner.packet_type.as_str() },
            "server_enabled": inner.server_enabled,
            "clients_enabled": inner.enabled_clients,
            "sent_bytes": sent,
            "received_bytes": received,
            "sent_bytes_per_second": crate::core::bandwidth(sent, inner.elapsed),
            "received_bytes_per_second": crate::core::bandwidth(received, inner.elapsed),
        })
    }
}

impl RestApi {
    pub fn new(clients: Arc<Clients>, log_dir: PathBuf) -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            address: Mutex::new(String::new()),
            busy: Arc::new(AtomicBool::new(false)),
            clients,
            log_dir,
            live: Arc::new(LiveStatus::default()),
            session: Mutex::new(None),
        }
    }

    /// Attaches a [`Session`] so the web CLI runs the full shell command set;
    /// without it, only the read-only commands are served.
    pub fn attach_session(&self, session: Arc<Session>) {
        *self.session.lock().unwrap() = Some(session);
    }

    /// The live status this instance publishes; the CLI updates it as runs start
    /// and stop.
    pub fn live(&self) -> &Arc<LiveStatus> {
        &self.live
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// The address the web server is listening on, empty when it is not running.
    pub fn address(&self) -> String {
        if self.is_running() {
            self.address.lock().unwrap().clone()
        } else {
            String::new()
        }
    }

    pub fn status(&self) -> String {
        if self.is_running() {
            format!("enabled on http://{}", self.address.lock().unwrap())
        } else {
            "disabled".to_string()
        }
    }

    /// Subcommand: restapi enable. Binds up front so a bad address is reported to
    /// the operator instead of failing silently in a worker thread.
    pub fn enable(self: &Arc<Self>, address: &str) -> Result<(), String> {
        if self.running.load(Ordering::Relaxed) {
            return Err(format!(
                "REST API already running on {}",
                self.address.lock().unwrap()
            ));
        }
        let listener = TcpListener::bind(address)
            .map_err(|error| format!("cannot bind REST API to {address}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| error.to_string())?;
        *self.address.lock().unwrap() = address.to_string();
        self.running.store(true, Ordering::Relaxed);
        let api = Arc::clone(self);
        thread::spawn(move || {
            while api.running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let api = Arc::clone(&api);
                        thread::spawn(move || api.serve(stream));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(())
    }

    /// Subcommand: restapi disable.
    pub fn disable(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn serve(&self, stream: TcpStream) {
        stream.set_nonblocking(false).ok();
        stream.set_read_timeout(Some(IO_TIMEOUT)).ok();
        stream.set_write_timeout(Some(IO_TIMEOUT)).ok();
        let (status, content_type, payload) = match Request::read(&stream) {
            Ok(request) => {
                let (status, body) = self.route(&request);
                match (request.path.as_str(), status, body.is_string()) {
                    // The OpenAPI document is served as YAML, not wrapped in JSON.
                    ("/api/v1/openapi.yaml", 200, true) => (
                        status,
                        "application/yaml",
                        body.as_str().unwrap_or_default().to_string(),
                    ),
                    ("/" | "/index.html", 200, true) => (
                        status,
                        "text/html; charset=utf-8",
                        body.as_str().unwrap_or_default().to_string(),
                    ),
                    _ => (status, "application/json", body.to_string()),
                }
            }
            Err(error) => (400, "application/json", json!({ "error": error }).to_string()),
        };
        let mut writer = &stream;
        let _ = write!(
            writer,
            "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            reason(status),
            payload.len()
        );
        let _ = writer.flush();
    }

    fn route(&self, request: &Request) -> (u16, Value) {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/api/v1/health") => (
                200,
                json!({
                    "status": "ok",
                    "version": env!("CARGO_PKG_VERSION"),
                    "running": self.busy.load(Ordering::Relaxed),
                }),
            ),
            ("GET", "/api/v1/openapi.yaml") => (200, Value::String(OPENAPI.to_string())),
            ("GET", "/api/v1/status") => (200, self.status_json()),
            ("GET", "/" | "/index.html") => (200, Value::String(WEB_UI.to_string())),
            ("POST", "/api/v1/cli") => self.cli_command(&request.body),
            ("GET", "/api/v1/clients") => (
                200,
                json!({
                    "clients": self
                        .clients
                        .list()
                        .iter()
                        .map(|client| serde_json::to_value(client).unwrap_or(Value::Null))
                        .collect::<Vec<_>>()
                }),
            ),
            ("GET", "/api/v1/profile") => (
                200,
                serde_json::to_value(TestProfile::default()).unwrap_or(Value::Null),
            ),
            ("GET", "/api/v1/runs") => match SqlState::new().run_list() {
                Ok(runs) => (200, json!({ "runs": runs })),
                Err(error) => (500, json!({ "error": error })),
            },
            ("POST", "/api/v1/runs") => self.start_run(&request.body),
            ("GET", path) if path.starts_with("/api/v1/runs/") => {
                match path.trim_start_matches("/api/v1/runs/").parse::<u64>() {
                    Ok(id) => self.show_run(id),
                    Err(_) => (400, json!({ "error": "run id must be a number" })),
                }
            }
            ("GET", _) | ("POST", _) => (404, json!({ "error": "no such resource" })),
            _ => (405, json!({ "error": "method not allowed" })),
        }
    }

    /// The live status the web page polls, and the source of the `status` text
    /// in the web CLI, so the browser and the terminal never disagree.
    fn status_json(&self) -> Value {
        let mut status = self.live.json();
        let busy = self.busy.load(Ordering::Relaxed);
        if busy && let Some(object) = status.as_object_mut() {
            object.insert("running".to_string(), Value::Bool(true));
        }
        if let Some(object) = status.as_object_mut() {
            object.insert(
                "version".to_string(),
                Value::String(env!("CARGO_PKG_VERSION").to_string()),
            );
            object.insert("web_server".to_string(), Value::String(self.status()));
            object.insert(
                "web_address".to_string(),
                Value::String(self.address.lock().unwrap().clone()),
            );
            object.insert("sctp".to_string(), Value::String(sctp_status()));
            object.insert("activity".to_string(), Value::String(self.activity()));
        }
        status
    }

    fn activity(&self) -> String {
        if self.busy.load(Ordering::Relaxed) {
            "running a test profile posted to /api/v1/runs".to_string()
        } else {
            self.live.activity()
        }
    }

    fn status_text(&self) -> String {
        let status = self.status_json();
        let field = |name: &str| status.get(name).cloned().unwrap_or(Value::Null);
        format!(
            concat!(
                "netmark {}\n",
                "activity:   {}\n",
                "transport:  {}\n",
                "sctp:       {}\n",
                "run:        {}\n",
                "traffic:    {} bytes sent, {} bytes received\n",
                "bandwidth:  {} bytes/sec up, {} bytes/sec down\n",
                "server:     {}\n",
                "clients:    {} enabled\n",
                "web server: {}"
            ),
            env!("CARGO_PKG_VERSION"),
            field("activity").as_str().unwrap_or_default(),
            field("packet_type").as_str().unwrap_or_default(),
            field("sctp").as_str().unwrap_or_default(),
            match field("run_id").as_u64().unwrap_or(0) {
                0 => "none yet".to_string(),
                id => format!("{id} ({} s elapsed)", field("elapsed_seconds").as_u64().unwrap_or(0)),
            },
            field("sent_bytes").as_u64().unwrap_or(0),
            field("received_bytes").as_u64().unwrap_or(0),
            field("sent_bytes_per_second").as_u64().unwrap_or(0),
            field("received_bytes_per_second").as_u64().unwrap_or(0),
            if field("server_enabled").as_bool().unwrap_or(false) {
                "enabled"
            } else {
                "disabled"
            },
            field("clients_enabled").as_u64().unwrap_or(0),
            field("web_server").as_str().unwrap_or_default(),
        )
    }

    fn show_run(&self, id: u64) -> (u16, Value) {
        let sql = SqlState::new();
        match sql.show_run(id) {
            Ok(Some(summary)) => (
                200,
                json!({
                    "run_id": id,
                    "summary": summary,
                    // Everything else the run collected, as label/value pairs.
                    "detail": sql
                        .run_detail_rows(id)
                        .ok()
                        .flatten()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|row| json!({ "label": row[0], "value": row[1] }))
                        .collect::<Vec<_>>(),
                    "debriefs": sql.debriefs(id).unwrap_or_default(),
                }),
            ),
            Ok(None) => (404, json!({ "error": format!("no local record for run {id}") })),
            Err(error) => (500, json!({ "error": error })),
        }
    }

    /// The web CLI: with a [`Session`] attached it runs the full shell command
    /// set; otherwise it serves a small read-only command set over the same
    /// state the REST API exposes. The `clients`, `profile` and `show <id>`
    /// helpers work either way so `netmarkctl` keeps functioning.
    fn cli_command(&self, body: &str) -> (u16, Value) {
        let request: Value = match serde_json::from_str(body) {
            Ok(value) => value,
            Err(error) => return (400, json!({ "error": format!("invalid JSON body: {error}") })),
        };
        let Some(command) = request.get("command").and_then(Value::as_str) else {
            return (400, json!({ "error": "body must be {\"command\": \"...\"}" }));
        };
        let tokens = command.split_whitespace().collect::<Vec<_>>();
        // Compatibility helpers the local CLI has no direct equivalent for.
        match tokens.as_slice() {
            ["clients"] => return (200, json!({ "output": self.clients_text() })),
            ["profile"] => {
                return (
                    200,
                    json!({ "output": serde_json::to_string_pretty(&TestProfile::default())
                        .unwrap_or_else(|error| error.to_string()) }),
                );
            }
            ["show", id] => return self.web_show(id),
            _ => {}
        }
        // With a session attached, every other command runs through the same
        // dispatcher as the interactive shell CLI.
        let session = self.session.lock().unwrap().clone();
        if let Some(session) = session {
            return (200, json!({ "output": session.execute(command) }));
        }
        let output = match tokens.as_slice() {
            [] | ["help"] => concat!(
                "Commands:\n",
                "  help          this text\n",
                "  status        service version, web server port and what is running now\n",
                "  list          runs recorded in the local database\n",
                "  show <id>     summary and debriefs for one run\n",
                "  clients       configured clients\n",
                "  configure sctp  detailed SCTP help and kernel support\n",
                "  profile       the default test profile as JSON\n",
                "\n",
                "Start runs by POSTing a profile (JSON or YAML) to /api/v1/runs,\n",
                "for example with netmarkctl: netmarkctl run profiles/udp-10kbps.yaml"
            )
            .to_string(),
            ["status"] => self.status_text(),
            ["configure", "sctp"] => crate::cli::sctp_help_rows()
                .iter()
                .map(|row| format!("{:<16}{}", row[0], row[1]))
                .collect::<Vec<_>>()
                .join("\n"),
            ["list"] => match SqlState::new().run_list() {
                Ok(runs) if runs.is_empty() => "no runs recorded".to_string(),
                Ok(runs) => runs.join("\n"),
                Err(error) => return (500, json!({ "error": error })),
            },
            _ => {
                return (
                    400,
                    json!({ "error": format!("unknown command: {command}. Type 'help'.") }),
                );
            }
        };
        (200, json!({ "output": output }))
    }

    /// The configured clients as text, shared by both web CLI command sets.
    fn clients_text(&self) -> String {
        let clients = self.clients.list();
        if clients.is_empty() {
            "no clients configured".to_string()
        } else {
            clients
                .iter()
                .map(|client| {
                    format!(
                        "client {} -> {} ({})",
                        client.id,
                        client.remote,
                        if client.enabled { "enabled" } else { "disabled" }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    }

    /// The web CLI `show <id>` helper: one run's summary and debriefs.
    fn web_show(&self, id: &str) -> (u16, Value) {
        match id.parse::<u64>() {
            Ok(id) => {
                let sql = SqlState::new();
                match sql.show_run(id) {
                    Ok(Some(summary)) => {
                        let mut lines = vec![summary];
                        lines.extend(sql.debriefs(id).unwrap_or_default());
                        (200, json!({ "output": lines.join("\n") }))
                    }
                    Ok(None) => (200, json!({ "output": format!("no local record for run {id}") })),
                    Err(error) => (500, json!({ "error": error })),
                }
            }
            Err(_) => (400, json!({ "error": "run id must be a number" })),
        }
    }

    fn start_run(&self, body: &str) -> (u16, Value) {
        // Accept both JSON and YAML so profile files can be posted unchanged.
        let profile: TestProfile = match serde_json::from_str(body)
            .map_err(|error| error.to_string())
            .or_else(|_| serde_yaml::from_str(body).map_err(|error| error.to_string()))
        {
            Ok(profile) => profile,
            Err(error) => {
                return (
                    400,
                    json!({ "error": format!("invalid test profile: {error}") }),
                );
            }
        };
        if self.busy.swap(true, Ordering::SeqCst) {
            return (409, json!({ "error": "a run is already in progress" }));
        }
        let packet_type =
            PacketType::parse(&profile.traffic.packet_type).unwrap_or(PacketType::Tcp);
        self.live.update(
            true,
            0,
            packet_type,
            profile.server.enabled,
            profile.enabled_clients().count(),
            None,
        );
        let started = std::time::Instant::now();
        let result = TestRunner::new(profile)
            .log_dir(self.log_dir.clone())
            .run();
        self.busy.store(false, Ordering::SeqCst);
        let run_id = result.as_ref().map(|report| report.run_id).unwrap_or(0);
        self.live.update(
            false,
            run_id,
            packet_type,
            false,
            0,
            Some(started.elapsed()),
        );
        match result {
            Ok(report) => (200, report_json(&report)),
            Err(error) => (400, json!({ "error": error })),
        }
    }
}

/// SCTP needs kernel support, so the status surfaces whether a socket can be
/// opened at all rather than waiting for a run to fail.
pub fn sctp_status() -> String {
    match crate::sctp::availability() {
        Ok(()) => "available".to_string(),
        Err(error) => format!("unavailable: {error}"),
    }
}

fn report_json(report: &crate::sdk::RunReport) -> Value {
    json!({
        "run_id": report.run_id,
        "packet_type": report.packet_type,
        "passed": report.passed,
        "result": report.result,
        "failure_reason": report.failure_reason,
        "elapsed_millis": report.elapsed.as_millis() as u64,
        "sent_tcp_bytes": report.sent_tcp_bytes,
        "sent_udp_bytes": report.sent_udp_bytes,
        "sent_ip_bytes": report.sent_ip_bytes,
        "received_tcp_bytes": report.received_tcp_bytes,
        "received_udp_bytes": report.received_udp_bytes,
        "received_ip_bytes": report.received_ip_bytes,
        "tcp_mss": report.tcp_mss,
        "tcp_mtu": report.tcp_mtu,
        "tcp_window_size": report.tcp_window_size,
        "sent_bytes": report.sent_bytes(),
        "received_bytes": report.received_bytes(),
        "sent_bytes_per_second": report.sent_bytes_per_second,
        "received_bytes_per_second": report.received_bytes_per_second,
        "throughput_bytes_per_second": report.throughput_bytes_per_second(),
        "lost_udp_packets": report.lost_udp_packets,
        "out_of_order_udp_packets": report.out_of_order_udp_packets,
        "tcp_jitter_millis": report.tcp_jitter_millis,
        "udp_jitter_millis": report.udp_jitter_millis,
        "webrtc_sent_messages": report.webrtc_sent_messages,
        "webrtc_received_messages": report.webrtc_received_messages,
        "webrtc_invalid_frames": report.webrtc_invalid_frames,
        "debrief": report.debrief.as_ref().map(|debrief| json!({
            "run_id": debrief.run_id,
            "role": debrief.role,
            "protocol": debrief.protocol,
            "sent_packets": debrief.sent_packets,
            "sent_bytes": debrief.sent_bytes,
            "received_packets": debrief.received_packets,
            "received_bytes": debrief.received_bytes,
            "lost_packets": debrief.lost_packets,
            "out_of_order_packets": debrief.out_of_order_packets,
            "matched": debrief.matched(),
            "mismatch_reason": debrief.mismatch_reason(),
        })),
    })
}

struct Request {
    method: String,
    path: String,
    body: String,
}

impl Request {
    fn read(stream: &TcpStream) -> Result<Self, String> {
        let mut reader = BufReader::new(stream);
        let request_line = read_line(&mut reader)?;
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let target = parts.next().unwrap_or_default();
        let path = target.split('?').next().unwrap_or_default().to_string();
        if method.is_empty() || path.is_empty() {
            return Err("malformed request line".to_string());
        }
        let mut content_length = 0u64;
        for _ in 0..MAX_HEADERS {
            let header = read_line(&mut reader)?;
            if header.trim().is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.trim().eq_ignore_ascii_case("content-length")
            {
                content_length = value
                    .trim()
                    .parse()
                    .map_err(|_| "invalid Content-Length".to_string())?;
            }
        }
        if content_length > MAX_BODY {
            return Err(format!("request body larger than {MAX_BODY} bytes"));
        }
        let mut body = String::new();
        reader
            .take(content_length)
            .read_to_string(&mut body)
            .map_err(|error| error.to_string())?;
        Ok(Self { method, path, body })
    }
}

fn read_line(reader: &mut BufReader<&TcpStream>) -> Result<String, String> {
    let mut line = String::new();
    let mut limited = reader.take(MAX_LINE);
    limited
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    Ok(line)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        _ => "Internal Server Error",
    }
}

/// Starts the REST API if netmark.config asks for it.
pub fn start_if_enabled(api: &Arc<RestApi>, config: &RestApiConfig) -> Option<String> {
    if !config.enabled {
        return None;
    }
    api.enable(&config.address).err()
}
