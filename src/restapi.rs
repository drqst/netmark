//! A small REST API over [`crate::sdk`], so netmark can be driven from anything
//! that speaks HTTP. The contract is documented in `doc/openapi.yaml`, which is
//! compiled in and served from `/api/v1/openapi.yaml`.
//!
//! The API is unauthenticated and can start traffic runs, so it binds to the
//! loopback interface unless it is explicitly given another address.

use crate::cli::Clients;
use crate::configuration::{RestApiConfig, TestProfile};
use crate::core::SqlState;
use crate::sdk::TestRunner;
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
}

impl RestApi {
    pub fn new(clients: Arc<Clients>, log_dir: PathBuf) -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            address: Mutex::new(String::new()),
            busy: Arc::new(AtomicBool::new(false)),
            clients,
            log_dir,
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
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
        let (status, body) = match Request::read(&stream) {
            Ok(request) => self.route(&request),
            Err(error) => (400, json!({ "error": error })),
        };
        let payload = if status == 200 && body.is_string() {
            // The OpenAPI document is served as YAML, not wrapped in JSON.
            body.as_str().unwrap_or_default().to_string()
        } else {
            body.to_string()
        };
        let content_type = if status == 200 && body.is_string() {
            "application/yaml"
        } else {
            "application/json"
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

    fn show_run(&self, id: u64) -> (u16, Value) {
        let sql = SqlState::new();
        match sql.show_run(id) {
            Ok(Some(summary)) => (
                200,
                json!({
                    "run_id": id,
                    "summary": summary,
                    "debriefs": sql.debriefs(id).unwrap_or_default(),
                }),
            ),
            Ok(None) => (404, json!({ "error": format!("no local record for run {id}") })),
            Err(error) => (500, json!({ "error": error })),
        }
    }

    fn start_run(&self, body: &str) -> (u16, Value) {
        let profile: TestProfile = match serde_json::from_str(body) {
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
        let result = TestRunner::new(profile)
            .log_dir(self.log_dir.clone())
            .run();
        self.busy.store(false, Ordering::SeqCst);
        match result {
            Ok(report) => (200, report_json(&report)),
            Err(error) => (400, json!({ "error": error })),
        }
    }
}

fn report_json(report: &crate::sdk::RunReport) -> Value {
    json!({
        "run_id": report.run_id,
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
