use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FileConfig {
    #[serde(default)]
    pub traffic: TrafficConfig,
    #[serde(default)]
    pub clients: Vec<ClientConfig>,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub smtp: SmtpConfig,
    #[serde(default)]
    pub restapi: RestApiConfig,
    #[serde(default)]
    pub webrtc: WebRtcConfig,
}

/// The WebRTC data-channel layer that rides on top of the selected transport.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct WebRtcConfig {
    pub enabled: bool,
    pub channels: u16,
    pub label: String,
    pub ordered: bool,
}
impl Default for WebRtcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channels: 1,
            label: "netmark".to_string(),
            ordered: true,
        }
    }
}

/// Mirrors `core::Config` so the same settings apply whether they come from the
/// CLI `configure` command or from netmark.config.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct TrafficConfig {
    /// UDP packets per second; ignored for TCP and SCTP, which are paced by `tcp_bytes_per_second`.
    pub udp_rate: u64,
    /// `tcp`, `sctp`, `udp` or `ip`.
    pub packet_type: String,
    pub tcp_bytes_per_second: u64,
    /// Requested TCP send/receive window in bytes; 0 uses the operating-system default.
    pub tcp_window_size: u32,
    pub udp_packet_size: usize,
    pub client_runtime: u64,
    pub server_runtime: u64,
    pub jitter_millis: u64,
    pub client_jitter_millis: u64,
    pub server_jitter_millis: u64,
    pub max_tcp_jitter_millis: u64,
    pub max_udp_jitter_millis: u64,
    /// Minimum acceptable throughput in bytes/sec for a run to pass; 0 disables the check.
    pub limit: u64,
}

impl Default for TrafficConfig {
    fn default() -> Self {
        Self {
            udp_rate: 100,
            packet_type: "tcp".to_string(),
            tcp_bytes_per_second: 1024,
            tcp_window_size: 0,
            udp_packet_size: 1024,
            client_runtime: 0,
            server_runtime: 0,
            jitter_millis: 0,
            client_jitter_millis: 0,
            server_jitter_millis: 0,
            max_tcp_jitter_millis: 1000,
            max_udp_jitter_millis: 1000,
            limit: 0,
        }
    }
}

/// External metrics database. Nothing is written anywhere off this host until a
/// connection string is set, so this is empty by default.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MetricsConfig {
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct SmtpConfig {
    pub server: Option<String>,
    /// When false netmark never contacts the SMTP server, even if one is configured.
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AdminConfig {
    #[serde(default)]
    pub emails: Vec<String>,
}

pub const DEFAULT_RESTAPI_ADDRESS: &str = "127.0.0.1:8081";

/// Where the REST API listens. It is unauthenticated and can start traffic runs,
/// so it stays on the loopback interface unless deliberately moved.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RestApiConfig {
    pub enabled: bool,
    pub address: String,
}
impl Default for RestApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            address: DEFAULT_RESTAPI_ADDRESS.to_string(),
        }
    }
}

pub fn path_near_executable() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()?
        .parent()
        .map(|path| path.join("netmark.config"))
}

pub fn load(path: &Path) -> Result<FileConfig, String> {
    let contents = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    serde_yaml::from_str(&contents).map_err(|error| error.to_string())
}

/// A fully self-contained, non-interactive run: which roles to enable, the
/// traffic/limits to use, where to send metrics, how long to run, and which
/// Rust hooks registered through the SDK to invoke.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct TestProfile {
    pub server: RoleConfig,
    pub clients: Vec<ClientConfig>,
    pub traffic: TrafficConfig,
    pub webrtc: WebRtcConfig,
    pub metrics: MetricsConfig,
    pub duration_seconds: u64,
    /// RFC 3339 UTC instant to begin sending at. Every machine given the same
    /// value starts together, to the accuracy of their clocks.
    pub start_at: Option<String>,
    pub hooks: HooksConfig,
}
impl Default for TestProfile {
    fn default() -> Self {
        Self {
            server: RoleConfig::default(),
            clients: vec![ClientConfig::new(0)],
            traffic: TrafficConfig::default(),
            webrtc: WebRtcConfig::default(),
            metrics: MetricsConfig::default(),
            duration_seconds: 3,
            start_at: None,
            hooks: HooksConfig::default(),
        }
    }
}
impl TestProfile {
    pub fn client(&self, id: u64) -> Option<&ClientConfig> {
        self.clients.iter().find(|client| client.id == id)
    }
    pub fn enabled_clients(&self) -> impl Iterator<Item = &ClientConfig> {
        self.clients.iter().filter(|client| client.enabled)
    }
}

/// Names of Rust callbacks registered on `sdk::TestRunner`, run before traffic
/// starts and after it stops. An `after` hook that returns an error fails the run.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct HooksConfig {
    pub before: Vec<String>,
    pub after: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct RoleConfig {
    pub enabled: bool,
}

/// One sending client. A netmark instance can drive several at once, each with
/// its own id, destination, and optional runtime, jitter and WebRTC overrides.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClientConfig {
    pub id: u64,
    pub enabled: bool,
    pub remote: String,
    /// Overrides `traffic.client_runtime` when set.
    pub runtime: Option<u64>,
    /// Overrides `traffic.client_jitter_millis` when set.
    pub jitter_millis: Option<u64>,
    /// Overrides `webrtc.enabled` for this client alone; `None` follows it.
    pub webrtc: Option<bool>,
}
impl Default for ClientConfig {
    fn default() -> Self {
        Self::new(0)
    }
}
impl ClientConfig {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            enabled: false,
            remote: "127.0.0.1".to_string(),
            runtime: None,
            jitter_millis: None,
            webrtc: None,
        }
    }
    pub fn summary(&self) -> String {
        format!(
            "client {} {} remote={}{}{} webrtc={}",
            self.id,
            if self.enabled { "enabled" } else { "disabled" },
            self.remote,
            self.runtime
                .map(|value| format!(" runtime={value}"))
                .unwrap_or_default(),
            self.jitter_millis
                .map(|value| format!(" jitter_millis={value}"))
                .unwrap_or_default(),
            match self.webrtc {
                Some(true) => "on",
                Some(false) => "off",
                None => "follow",
            }
        )
    }
}

pub fn load_test_profile(path: &Path) -> Result<TestProfile, String> {
    let contents = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    serde_yaml::from_str(&contents).map_err(|error| error.to_string())
}
