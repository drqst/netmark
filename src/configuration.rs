use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FileConfig {
    #[serde(default)]
    pub traffic: TrafficConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub smtp: SmtpConfig,
}

/// Mirrors `core::Config` so the same settings apply whether they come from the
/// CLI `configure` command or from netmark.config.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct TrafficConfig {
    pub rate: u64,
    pub packet_type: String,
    pub tcp_bytes_per_second: u64,
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
            rate: 100,
            packet_type: "tcp".to_string(),
            tcp_bytes_per_second: 1024,
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
    pub client: ClientRoleConfig,
    pub traffic: TrafficConfig,
    pub metrics: MetricsConfig,
    pub duration_seconds: u64,
    pub hooks: HooksConfig,
}
impl Default for TestProfile {
    fn default() -> Self {
        Self {
            server: RoleConfig::default(),
            client: ClientRoleConfig::default(),
            traffic: TrafficConfig::default(),
            metrics: MetricsConfig::default(),
            duration_seconds: 3,
            hooks: HooksConfig::default(),
        }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClientRoleConfig {
    pub enabled: bool,
    pub remote: String,
}
impl Default for ClientRoleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            remote: "127.0.0.1".to_string(),
        }
    }
}

pub fn load_test_profile(path: &Path) -> Result<TestProfile, String> {
    let contents = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    serde_yaml::from_str(&contents).map_err(|error| error.to_string())
}

