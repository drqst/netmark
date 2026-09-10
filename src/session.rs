//! The command surface shared by the interactive shell CLI and the web CLI.
//!
//! A [`Session`] owns exactly the state the interactive loop in `main.rs` used
//! to keep for itself, and [`Session::execute`] runs one command line and
//! returns its text output. Both surfaces route commands through it so they can
//! never disagree on what a command does or how its reply reads: the terminal
//! prints the returned lines with [`crate::cli_textout::line`], while the REST
//! API returns them as the body of `POST /api/v1/cli`.

use crate::cli::{
    self, CLIENT_USAGE, CONFIGURE_USAGE, Clients, StatusContext, client_command,
    configure, help_for, list_clients, run_benchmark, run_selftest, save_configuration,
    reset_configuration, sctp_help_rows, set_runtime, set_smtp_enabled, show_monitor_history,
    smtp_status, status_rows, update_admin_email, webrtc_command,
};
use crate::configuration::{FileConfig, RestApiConfig, SmtpConfig};
use crate::core::{self, Config, DEFAULT_REMOTE, Metrics, SqlState, StartGate};
use crate::metrics::ExternalSqlMetrics;
use crate::monitor::MonitorState;
use crate::restapi::{LiveStatus, RestApi};
use crate::{cli_textout, webrtc};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

/// All the shared state a running netmark instance drives, and the one command
/// dispatcher over it.
pub struct Session {
    config: Arc<Mutex<Config>>,
    clients: Arc<Clients>,
    metrics: Arc<Metrics>,
    sql: Arc<SqlState>,
    stopping: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    external: Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    smtp: Arc<Mutex<SmtpConfig>>,
    webrtc: Arc<Mutex<webrtc::Settings>>,
    restapi_config: Arc<Mutex<RestApiConfig>>,
    monitor: Arc<MonitorState>,
    log_dir: PathBuf,
    config_path: PathBuf,
    server_enabled: AtomicBool,
    run_id: AtomicU64,
    run_started: Mutex<Option<Instant>>,
    /// The live status published to the web page; shared with the REST API so
    /// the browser follows runs the same way the terminal does.
    live: Arc<LiveStatus>,
    /// The REST API this session belongs to, used by the `restapi` command and
    /// the REST API row of `status`; a weak handle so the two do not keep each
    /// other alive in a cycle.
    restapi: Weak<RestApi>,
}

impl Session {
    /// Builds a session from a loaded configuration file, wiring the shared live
    /// status and the REST API handle the `restapi` command needs.
    pub fn from_config(
        file_config: &FileConfig,
        log_dir: PathBuf,
        config_path: PathBuf,
        clients: Arc<Clients>,
        live: Arc<LiveStatus>,
        restapi: Weak<RestApi>,
    ) -> Self {
        let config = Arc::new(Mutex::new(crate::config_from_file(file_config)));
        let metrics = Arc::new(Metrics::new());
        let external = Arc::new(Mutex::new(
            file_config
                .metrics
                .sql
                .as_deref()
                .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
                .map(Arc::new),
        ));
        live.attach_metrics(Arc::clone(&metrics));
        let sql = Arc::new(SqlState::new());
        sql.enable()
            .expect("cannot initialize local SQLite database");
        Session {
            config,
            clients,
            metrics,
            sql,
            stopping: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            external,
            smtp: Arc::new(Mutex::new(file_config.smtp.clone())),
            webrtc: Arc::new(Mutex::new(crate::webrtc_settings(&file_config.webrtc))),
            restapi_config: Arc::new(Mutex::new(file_config.restapi.clone())),
            monitor: Arc::new(MonitorState::new()),
            log_dir,
            config_path,
            server_enabled: AtomicBool::new(false),
            run_id: AtomicU64::new(0),
            run_started: Mutex::new(None),
            live,
            restapi,
        }
    }

    pub fn config(&self) -> &Arc<Mutex<Config>> {
        &self.config
    }
    pub fn clients(&self) -> &Arc<Clients> {
        &self.clients
    }
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
    pub fn sql(&self) -> &Arc<SqlState> {
        &self.sql
    }
    pub fn stopping(&self) -> &Arc<AtomicBool> {
        &self.stopping
    }
    pub fn running(&self) -> &Arc<AtomicBool> {
        &self.running
    }
    pub fn external(&self) -> &Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>> {
        &self.external
    }
    pub fn monitor(&self) -> &Arc<MonitorState> {
        &self.monitor
    }
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    pub fn server_enabled(&self) -> bool {
        self.server_enabled.load(Ordering::Relaxed)
    }
    pub fn run_id(&self) -> u64 {
        self.run_id.load(Ordering::Relaxed)
    }
    /// When the current run began, so the caller can report elapsed time and
    /// finish an in-flight run on shutdown.
    pub fn run_started(&self) -> Option<Instant> {
        *self.run_started.lock().unwrap()
    }

    /// Publishes the current run state to the live status the web page polls, so
    /// the browser follows the run without a reload. Called after every command
    /// and, in interactive mode, on every idle poll tick.
    pub fn publish_live(&self) {
        self.live.update(
            self.is_running(),
            self.run_id(),
            self.config.lock().unwrap().packet_type,
            self.server_enabled(),
            self.clients.enabled().len(),
            self.run_started().map(|started| started.elapsed()),
        );
    }

    /// Runs one command line and returns its text output, without a trailing
    /// newline. The wording matches the interactive shell CLI exactly, so both
    /// surfaces read the same.
    pub fn execute(&self, command: &str) -> String {
        let output = self.dispatch(command);
        self.publish_live();
        output
    }

    fn dispatch(&self, command: &str) -> String {
        match command.split_whitespace().collect::<Vec<_>>().as_slice() {
            // Command: client
            ["client"] => CLIENT_USAGE.to_string(),
            ["client", "list"] => list_clients(&self.clients),
            ["client", "add"] => {
                let id = self.clients.add();
                format!("client {id} added")
            }
            ["client", "delete", id] => match id.parse::<u64>() {
                Ok(id) if self.clients.remove(id) => format!("client {id} deleted"),
                Ok(id) => format!("no client {id}"),
                Err(_) => "client id must be a number".to_string(),
            },
            ["client", id, rest @ ..] if id.parse::<u64>().is_ok() => {
                client_command(&self.clients, id.parse().unwrap(), rest, &self.log_dir)
            }
            ["client", rest @ ..] => client_command(&self.clients, 0, rest, &self.log_dir),
            // Command: restapi
            ["restapi", "enable"] | ["restapi", "enable", _] => self.enable_restapi(command),
            ["restapi", "disable"] => {
                if let Some(api) = self.restapi.upgrade() {
                    api.disable();
                }
                self.restapi_config.lock().unwrap().enabled = false;
                "REST API disabled".to_string()
            }
            ["restapi", "status"] => match self.restapi.upgrade() {
                Some(api) => api.status(),
                None => "disabled".to_string(),
            },
            ["restapi", ..] => "restapi: enable [<address>] | disable | status".to_string(),
            // Command: webrtc
            ["webrtc", rest @ ..] => match webrtc_command(&self.webrtc, rest) {
                Ok(summary) => summary,
                Err(error) => error,
            },
            // Command: admin
            ["admin"] => ADMIN_USAGE.to_string(),
            ["admin", "add", "email", address] => {
                update_admin_email(&self.config_path, &self.config, address, true)
            }
            ["admin", "delete", "email", address] => {
                update_admin_email(&self.config_path, &self.config, address, false)
            }
            ["admin", "smtp", "enabled"] => set_smtp_enabled(&self.config_path, &self.smtp, true),
            ["admin", "smtp", "disabled"] => set_smtp_enabled(&self.config_path, &self.smtp, false),
            ["admin", "smtp", "status"] | ["admin", "smtp", "check"] => smtp_status(&self.smtp),
            ["admin", "smtp", ..] => "admin smtp: enabled | disabled | status".to_string(),
            ["admin", ..] => ADMIN_USAGE.to_string(),
            // Command: server
            ["server"] => "server: enable | disable | runtime <seconds>".to_string(),
            ["server", "enable"] => {
                self.server_enabled.store(true, Ordering::Relaxed);
                "server enabled".to_string()
            }
            ["server", "disable"] => {
                self.server_enabled.store(false, Ordering::Relaxed);
                self.stopping.store(true, Ordering::Relaxed);
                "Server stopped".to_string()
            }
            ["server", "runtime", seconds] => set_runtime(&self.config, false, seconds),
            ["server", ..] => "server: enable | disable | runtime <seconds>".to_string(),
            // Command: configure
            ["configure", "metrics", connection] => match ExternalSqlMetrics::connect(connection) {
                Ok(sink) => {
                    *self.external.lock().unwrap() = Some(Arc::new(sink));
                    "external SQL metrics enabled".to_string()
                }
                Err(error) => format!("metrics error: {error}"),
            },
            ["configure", "save"] => {
                let snapshot = self.config.lock().unwrap().clone();
                match save_configuration(
                    &self.config_path,
                    &snapshot,
                    &self.clients,
                    &self.external,
                    &self.smtp,
                    &self.restapi_config,
                    &self.webrtc,
                ) {
                    Ok(()) => "configuration saved to netmark.config".to_string(),
                    Err(error) => format!("config save error: {error}"),
                }
            }
            ["configure", "reset"] => {
                reset_configuration(
                    &self.config_path,
                    &self.config,
                    &self.clients,
                    &self.external,
                    &self.smtp,
                    &self.restapi_config,
                    &self.webrtc,
                );
                "configuration reset to defaults".to_string()
            }
            ["configure", "smtp", value] => {
                self.smtp.lock().unwrap().server = Some(value.to_string());
                format!("SMTP server set to {value}")
            }
            ["configure", "tcp", "maxjitter", value] => match value.parse::<u64>() {
                Ok(value) => {
                    self.config.lock().unwrap().max_tcp_jitter_millis = value;
                    format!("TCP maximum jitter set to {value} ms")
                }
                Err(_) => "TCP maxjitter must be milliseconds".to_string(),
            },
            ["configure", "udp", "max", "jitter", value] => match value.parse::<u64>() {
                Ok(value) => {
                    self.config.lock().unwrap().max_udp_jitter_millis = value;
                    format!("UDP maximum jitter set to {value} ms")
                }
                Err(_) => "UDP max jitter must be milliseconds".to_string(),
            },
            ["configure", "limits", rest @ ..] => {
                match cli::configure_limits(&self.config, rest) {
                    Ok(output) | Err(output) => output,
                }
            }
            ["configure"] => CONFIGURE_USAGE.to_string(),
            ["configure", rest @ ..] => match configure(&self.config, rest) {
                Ok(()) => "configuration updated".to_string(),
                Err(error) => error,
            },
            // Command: metrics
            ["metrics"] => "metrics: enable | disable | status".to_string(),
            ["metrics", "status"] => {
                let status = self
                    .external
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|sink| sink.status())
                    .unwrap_or_else(|| "not connected".to_string());
                format!("metrics SQL: {status}")
            }
            ["metrics", "enable"] => match cli::load_default_metrics_sink() {
                Some(sink) => {
                    *self.external.lock().unwrap() = Some(sink);
                    "external SQL metrics enabled".to_string()
                }
                None => "external SQL metrics could not connect".to_string(),
            },
            ["metrics", "disable"] => {
                *self.external.lock().unwrap() = None;
                "external SQL metrics disabled".to_string()
            }
            ["metrics", ..] => "metrics: enable | disable | status".to_string(),
            // Command: monitor
            ["monitor"] => "monitor: IP <url> | start | stop | history".to_string(),
            ["monitor", "IP", target] | ["monitor", "ip", target] => {
                let target = cli::normalize_http_target(target);
                self.monitor.set_target(target.clone());
                format!("monitor target set to {target}")
            }
            ["monitor", "start"] => match self.monitor.start(&self.log_dir, &self.sql) {
                Some(id) => format!("monitor started {id}"),
                None => "monitor already running".to_string(),
            },
            ["monitor", "stop"] => {
                self.monitor.stop(&self.log_dir, &self.sql);
                "monitor stopped".to_string()
            }
            ["monitor", "history"] => show_monitor_history(&self.log_dir),
            ["monitor", ..] => "monitor: IP <url> | start | stop | history".to_string(),
            // Command: selftest
            ["selftest"] => run_selftest(
                &self.config,
                &self.stopping,
                &self.running,
                &self.metrics,
                &self.sql,
                &self.external,
                &self.log_dir,
            ),
            // Command: benchmark
            ["benchmark"] => "benchmark: duration <seconds>".to_string(),
            ["benchmark", "duration", seconds] => match seconds.parse::<u64>() {
                Ok(seconds) if seconds > 0 => run_benchmark(
                    &self
                        .clients
                        .get(0)
                        .map(|client| client.remote)
                        .unwrap_or_else(|| DEFAULT_REMOTE.to_string()),
                    seconds,
                    &self.sql,
                    &self.log_dir,
                ),
                _ => "benchmark duration must be a positive number of seconds".to_string(),
            },
            ["benchmark", ..] => "benchmark: duration <seconds>".to_string(),
            // Command: clean — the interactive loop confirms first; every other
            // surface cleans immediately.
            ["clean"] => match self.sql.clean() {
                Ok(()) => "local SQLite data cleaned; run ID counter preserved".to_string(),
                Err(error) => format!("clean failed: {error}"),
            },
            // Command: start
            ["start"] => self.start_run(),
            // Command: stop
            ["stop"] => self.stop_run(),
            // Command: status
            ["status"] => self.status_text(),
            // Command: show run <id>
            ["show", "run", value] => match value.parse::<u64>() {
                Ok(id) => match self.sql.show_run(id) {
                    Ok(Some(row)) => {
                        let mut lines = vec![format!("Run {id}:"), row];
                        lines.extend(self.sql.debriefs(id).unwrap_or_default());
                        lines.join("\n")
                    }
                    Ok(None) => format!("no local record for run {id}"),
                    Err(error) => format!("sql error: {error}"),
                },
                Err(_) => String::new(),
            },
            // Command: list
            ["list"] => match self.sql.run_list() {
                Ok(rows) => rows.join("\n"),
                Err(error) => format!("sql error: {error}"),
            },
            // Command: sctp
            ["sctp"] => cli_textout::table_lines(&sctp_help_rows(), &[16, 84]).join("\n"),
            ["sctp", "status"] => format!("SCTP: {}", crate::restapi::sctp_status()),
            ["sctp", ..] => "sctp: (no argument for the help page) | status".to_string(),
            // Command: help — help alone lists everything; help <command...> shows
            // that command's rows through the shared help_for.
            ["help", topic @ ..] => match help_for(topic) {
                Ok(rows) => cli_textout::table_lines(&rows, &[36, 64]).join("\n"),
                Err(message) => message,
            },
            [] => String::new(),
            _ => "unknown command; type 'help' for commands".to_string(),
        }
    }

    /// Subcommand: restapi enable. Reuses the configured address when none is
    /// given, and persists the new address so `configure save` keeps it.
    fn enable_restapi(&self, command: &str) -> String {
        let address = match command.split_whitespace().nth(2) {
            Some(address) => address.to_string(),
            None => self.restapi_config.lock().unwrap().address.clone(),
        };
        let Some(api) = self.restapi.upgrade() else {
            return "REST API not available".to_string();
        };
        match api.enable(&address) {
            Ok(()) => {
                let mut settings = self.restapi_config.lock().unwrap();
                settings.enabled = true;
                settings.address = address.clone();
                format!("REST API enabled on http://{address}")
            }
            Err(error) => error,
        }
    }

    /// Command: start — spawns the enabled server and clients and begins a run.
    fn start_run(&self) -> String {
        if self.is_running() {
            return "a run is already active; stop it before starting another".to_string();
        }
        let enabled = self.clients.enabled();
        let run_id = self.sql.next_run_id(self.run_id() + 1);
        self.run_id.store(run_id, Ordering::Relaxed);
        self.sql
            .set_role(core::role_name(!enabled.is_empty(), self.server_enabled()));
        self.stopping.store(false, Ordering::Relaxed);
        self.metrics.snapshot();
        self.metrics.reset_run();
        *self.run_started.lock().unwrap() = Some(Instant::now());
        let gate = Arc::new(StartGate::new());
        self.config.lock().unwrap().webrtc = self.webrtc.lock().unwrap().clone();
        crate::write_run_event(&self.log_dir, run_id, "Starting");
        if self.server_enabled() {
            core::spawn_server(
                Arc::clone(&self.config),
                Arc::clone(&gate),
                Arc::clone(&self.stopping),
                Arc::clone(&self.metrics),
                self.log_dir.clone(),
            );
            core::spawn_debrief_responder(
                Arc::clone(&self.stopping),
                Arc::clone(&self.metrics),
                Arc::clone(&self.sql),
                self.log_dir.clone(),
            );
        }
        for client in &enabled {
            let traffic = crate::traffic_config_from(&self.config.lock().unwrap());
            let per_client = crate::client_config(&traffic, client, &self.webrtc.lock().unwrap());
            core::spawn_client(
                Arc::new(Mutex::new(per_client)),
                Arc::clone(&gate),
                Arc::clone(&self.stopping),
                Arc::clone(&self.metrics),
                client.remote.clone(),
                self.log_dir.clone(),
                client.id,
            );
        }
        self.sql.start_run(run_id);
        self.running.store(true, Ordering::Relaxed);
        gate.start();
        format!("started run {run_id} with {} client(s)", enabled.len())
    }

    /// Command: stop — ends the run, debriefs the server and reports the outcome.
    fn stop_run(&self) -> String {
        if !self.is_running() {
            return "no run is active".to_string();
        }
        self.stopping.store(true, Ordering::Relaxed);
        self.running.store(false, Ordering::Relaxed);
        let run_id = self.run_id();
        crate::write_run_event(&self.log_dir, run_id, "Completed");
        let elapsed = self
            .run_started
            .lock()
            .unwrap()
            .take()
            .map(|started| started.elapsed());
        let mut outcome = crate::evaluate_run(&self.metrics, &self.config.lock().unwrap(), elapsed);
        let mut lines = Vec::new();
        // Counters are per instance, so one debrief covers every client; it goes
        // to the lowest-numbered client's remote.
        if let Some(client) = self
            .clients
            .enabled()
            .into_iter()
            .min_by_key(|client| client.id)
        {
            let packet_type = self.config.lock().unwrap().packet_type;
            match core::run_debrief(
                &client.remote,
                run_id,
                packet_type,
                &self.metrics,
                &self.sql,
                &self.log_dir,
            ) {
                Ok(debrief) => {
                    lines.push(debrief.summary());
                    if let Some(reason) = debrief.mismatch_reason() {
                        outcome.add_failure(format!("debrief mismatch: {reason}"));
                    }
                }
                Err(error) => {
                    lines.push(error.clone());
                    outcome.add_failure(error);
                }
            }
        }
        self.sql.complete_run(run_id, &outcome.summary());
        crate::record_final_metrics(
            self.external.lock().unwrap().as_ref(),
            run_id,
            &self.metrics,
            &outcome,
        );
        crate::write_log_line(&self.log_dir, &outcome.report_line(run_id));
        // The transport is named on stop so an SCTP run is not mistaken for the
        // TCP counters it shares.
        lines.push(format!(
            "stopped transport={} {}",
            self.config.lock().unwrap().packet_type.as_str(),
            outcome.report_line(run_id)
        ));
        lines.join("\n")
    }

    /// Command: status — the aligned status table as text lines.
    pub fn status_text(&self) -> String {
        let (restapi_status, web_server) = match self.restapi.upgrade() {
            Some(api) => (
                api.status(),
                cli::web_server_status(&api.address(), &self.restapi_config.lock().unwrap().address),
            ),
            None => (
                "disabled".to_string(),
                cli::web_server_status("", &self.restapi_config.lock().unwrap().address),
            ),
        };
        let context = StatusContext {
            running: self.is_running(),
            elapsed: self.run_started().map(|started| started.elapsed()),
            run_id: self.run_id(),
            server_enabled: self.server_enabled(),
            clients: &self.clients,
            webrtc: &self.webrtc.lock().unwrap(),
            packet_type: self.config.lock().unwrap().packet_type,
            monitor: self.monitor.status(),
            metrics_sql: self
                .external
                .lock()
                .unwrap()
                .as_ref()
                .map(|sink| sink.status())
                .unwrap_or_else(|| "not connected".to_string()),
            restapi: restapi_status,
            web_server,
            smtp: self.smtp.lock().unwrap().enabled,
        };
        cli_textout::table_lines(&status_rows(&self.metrics, &context), &[16, 80]).join("\n")
    }
}

const ADMIN_USAGE: &str =
    "admin: add email <address> | delete email <address> | smtp enabled | smtp disabled | smtp status";
