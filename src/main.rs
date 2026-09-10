use netmark::cli::*;
use netmark::core::{Config, DEFAULT_REMOTE, Metrics, SqlState, StartGate};
use netmark::metrics::ExternalSqlMetrics;
use netmark::{
    cli_textout, config_from_file, configuration, core, evaluate_run, monitor,
    record_final_metrics, run_auto_mode, write_run_event,
};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::fs::{OpenOptions, create_dir_all};
use std::io::{self, BufRead, Write};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use std::time::Instant;

type ReportLoopArgs = (
    Arc<Metrics>,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
    Arc<Mutex<ChildStdin>>,
    Arc<Mutex<Config>>,
    std::path::PathBuf,
);

fn main() {
    if std::env::args().any(|arg| arg == "--output") {
        output_process();
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("--serve") {
        serve_mode(std::env::args().nth(2));
        return;
    }
    if let Some(profile_path) = std::env::args().nth(1) {
        let success = run_auto_mode(std::path::Path::new(&profile_path));
        std::process::exit(if success { 0 } else { 1 });
    }
    let log_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("log");
    create_dir_all(&log_dir).expect("cannot create log directory");
    for name in [
        "cli.log",
        "client.log",
        "server.log",
        "netmark.log",
        "alarm.log",
    ] {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join(name))
            .expect("cannot open log file");
    }
    let config_path = configuration::path_near_executable()
        .unwrap_or_else(|| std::path::PathBuf::from("netmark.config"));
    let file_config = configuration::load(&config_path).unwrap_or_default();
    let config = Arc::new(Mutex::new(config_from_file(&file_config)));
    let stopping = Arc::new(AtomicBool::new(false));
    let running = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let sql = Arc::new(SqlState::new());
    sql.enable()
        .expect("cannot initialize local SQLite database");
    let external = Arc::new(Mutex::new(
        file_config
            .metrics
            .sql
            .as_deref()
            .and_then(|connection| ExternalSqlMetrics::connect(connection).ok())
            .map(Arc::new),
    ));
    let smtp = Arc::new(Mutex::new(file_config.smtp.clone()));
    let restapi_config = Arc::new(Mutex::new(file_config.restapi.clone()));
    let webrtc = Arc::new(Mutex::new(netmark::webrtc_settings(&file_config.webrtc)));
    let clients = Arc::new(Clients::new(file_config.clients.clone()));
    let restapi = Arc::new(netmark::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    if let Some(error) = netmark::restapi::start_if_enabled(&restapi, &file_config.restapi) {
        eprintln!("REST API not started: {error}");
    }
    restapi.live().attach_metrics(Arc::clone(&metrics));
    println!(
        "netmark web server {}",
        web_server_status(&restapi.address(), &file_config.restapi.address)
    );
    let monitor = Arc::new(monitor::MonitorState::new());
    monitor
        .clone()
        .spawn_worker(log_dir.clone(), Arc::clone(&external));
    let (output_stdin, stdout_guard) = start_output_process();
    let output = Arc::new(Mutex::new(output_stdin));
    {
        let args = (
            Arc::clone(&metrics),
            Arc::clone(&stopping),
            Arc::clone(&running),
            Arc::clone(&output),
            Arc::clone(&config),
            log_dir.clone(),
        );
        thread::spawn(move || report_loop(args));
    }
    let mut cli_log = OpenOptions::new()
        .append(true)
        .open(log_dir.join("cli.log"))
        .unwrap();
    let mut input = String::new();
    let mut history = History::new();
    let mut server_enabled = false;
    let mut run_id = 0u64;
    let mut run_started: Option<Instant> = None;
    let mut cli_mode = true;
    let mut clean_confirmation = false;
    enable_raw_mode().expect("cannot enable terminal input");
    print_prompt(server_enabled, clients.any_enabled(), false);
    loop {
        // Publishing on every poll tick keeps the web status section live while
        // the operator is idle at the prompt.
        restapi.live().update(
            running.load(Ordering::Relaxed),
            run_id,
            config.lock().unwrap().packet_type,
            server_enabled,
            clients.enabled().len(),
            run_started.map(|started| started.elapsed()),
        );
        if !event::poll(Duration::from_millis(100)).unwrap() {
            continue;
        }
        match event::read().unwrap() {
            Event::Key(KeyEvent {
                code: KeyCode::Char(choice),
                modifiers,
                ..
            }) if clean_confirmation && !modifiers.contains(KeyModifiers::CONTROL) => {
                match choice.to_ascii_lowercase() {
                    'y' => {
                        clean_confirmation = false;
                        match sql.clean() {
                            Ok(()) => cli_textout::line(
                                "local SQLite data cleaned; run ID counter preserved",
                            ),
                            Err(error) => cli_textout::line(format!("clean failed: {error}")),
                        }
                    }
                    'n' => {
                        clean_confirmation = false;
                        cli_textout::line("clean cancelled");
                    }
                    _ => cli_textout::line("Please answer Y or N."),
                }
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers,
                ..
            }) if modifiers.contains(KeyModifiers::CONTROL) => {
                cli_textout::line("");
                running.store(false, Ordering::Relaxed);
                monitor.stop(&log_dir, &sql);
                disable_raw_mode().ok();
                clear_input_line();
                cli_textout::line("exiting");
                break;
            }
            Event::Key(KeyEvent {
                code: KeyCode::Esc, ..
            }) if running.load(Ordering::Relaxed) => {
                stopping.store(true, Ordering::Relaxed);
                running.store(false, Ordering::Relaxed);
                cli_mode = true;
                cli_textout::line("stopped");
                print_prompt(server_enabled, clients.any_enabled(), false);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Tab, ..
            }) => {
                cli_mode = !cli_mode;
                let mut out = output.lock().unwrap();
                let _ = writeln!(out, "{}", if cli_mode { "HIDE" } else { "SHOW" });
                let _ = out.flush();
                cli_textout::raw("\r\x1b[2K\r\n");
                if cli_mode {
                    print_prompt(
                        server_enabled,
                        clients.any_enabled(),
                        running.load(Ordering::Relaxed),
                    );
                }
                io::stdout().flush().unwrap();
            }
            Event::Key(KeyEvent {
                code: KeyCode::Up, ..
            }) if cli_mode && !clean_confirmation => {
                history.step_back(&mut input);
                redraw_input(server_enabled, clients.any_enabled(), &running, &input);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Down,
                ..
            }) if cli_mode && !clean_confirmation => {
                history.step_forward(&mut input);
                redraw_input(server_enabled, clients.any_enabled(), &running, &input);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Enter,
                ..
            }) if cli_mode => {
                let line = input.trim().to_string();
                input.clear();
                history.push(&line);
                cli_textout::raw("\r\n");
                writeln!(cli_log, "{} {}", core::timestamp(), line).unwrap();
                cli_log.flush().unwrap();
                // Commands and subcommands are annotated below to distinguish
                // user-facing verbs from plain control flow; their implementations
                // live in cli.rs, tagged the same way.
                match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                    // Command: client — every subcommand that configures or controls
                    // a client takes its id; the bare form is shorthand for client 0.
                    ["client"] => cli_textout::line(CLIENT_USAGE),
                    ["client", "list"] => list_clients(&clients),
                    ["client", "add"] => {
                        let id = clients.add();
                        cli_textout::line(format!("client {id} added"));
                    }
                    ["client", "delete", id] => match id.parse::<u64>() {
                        Ok(id) if clients.remove(id) => {
                            cli_textout::line(format!("client {id} deleted"))
                        }
                        Ok(id) => cli_textout::line(format!("no client {id}")),
                        Err(_) => cli_textout::line("client id must be a number"),
                    },
                    ["client", id, rest @ ..] if id.parse::<u64>().is_ok() => {
                        client_command(&clients, id.parse().unwrap(), rest, &log_dir)
                    }
                    ["client", rest @ ..] => client_command(&clients, 0, rest, &log_dir),
                    // Command: restapi — Subcommands: enable, disable, status
                    ["restapi"] => {
                        cli_textout::line("restapi: enable [<address>] | disable | status")
                    }
                    ["restapi", "enable"] | ["restapi", "enable", _] => {
                        let address = match line.split_whitespace().nth(2) {
                            Some(address) => address.to_string(),
                            None => restapi_config.lock().unwrap().address.clone(),
                        };
                        match restapi.enable(&address) {
                            Ok(()) => {
                                let mut settings = restapi_config.lock().unwrap();
                                settings.enabled = true;
                                settings.address = address.clone();
                                cli_textout::line(format!("REST API enabled on http://{address}"));
                            }
                            Err(error) => cli_textout::line(error),
                        }
                    }
                    ["restapi", "disable"] => {
                        restapi.disable();
                        restapi_config.lock().unwrap().enabled = false;
                        cli_textout::line("REST API disabled");
                    }
                    ["restapi", "status"] => cli_textout::line(restapi.status()),
                    ["restapi", ..] => {
                        cli_textout::line("restapi: enable [<address>] | disable | status")
                    }
                    // Command: webrtc — Subcommands: enable, disable, channels, label, ordered, status
                    ["webrtc", rest @ ..] => match webrtc_command(&webrtc, rest) {
                        Ok(summary) => cli_textout::line(summary),
                        Err(error) => cli_textout::line(error),
                    },
                    // Command: admin — Subcommands: add email, delete email, smtp
                    ["admin"] => cli_textout::line(
                        "admin: add email <address> | delete email <address> | smtp enabled | smtp disabled | smtp status",
                    ),
                    ["admin", "add", "email", address] => {
                        update_admin_email(&config_path, &config, address, true)
                    }
                    ["admin", "delete", "email", address] => {
                        update_admin_email(&config_path, &config, address, false)
                    }
                    ["admin", "smtp", "enabled"] => {
                        set_smtp_enabled(&config_path, &smtp, true)
                    }
                    ["admin", "smtp", "disabled"] => {
                        set_smtp_enabled(&config_path, &smtp, false)
                    }
                    ["admin", "smtp", "status"] | ["admin", "smtp", "check"] => {
                        smtp_status(&smtp)
                    }
                    ["admin", "smtp", ..] => {
                        cli_textout::line("admin smtp: enabled | disabled | status")
                    }
                    ["admin", ..] => cli_textout::line(
                        "admin: add email <address> | delete email <address> | smtp enabled | smtp disabled | smtp status",
                    ),
                    // Command: server — Subcommands: enable, disable, runtime
                    ["server"] => cli_textout::line("server: enable | disable | runtime <seconds>"),
                    ["server", "enable"] => {
                        server_enabled = true;
                        cli_textout::line("server enabled");
                    }
                    ["server", "disable"] => {
                        server_enabled = false;
                        stopping.store(true, Ordering::Relaxed);
                        cli_textout::line("Server stopped");
                    }
                    ["server", "runtime", seconds] => set_runtime(&config, false, seconds),
                    ["server", ..] => {
                        cli_textout::line("server: enable | disable | runtime <seconds>")
                    }
                    // Command: configure — Subcommands: metrics, save, reset, smtp, tcp/udp
                    // maxjitter, and everything handled by cli::configure()
                    ["configure", "metrics", connection] => {
                        match ExternalSqlMetrics::connect(connection) {
                            Ok(sink) => {
                                *external.lock().unwrap() = Some(Arc::new(sink));
                                cli_textout::line("external SQL metrics enabled");
                            }
                            Err(error) => cli_textout::line(format!("metrics error: {error}")),
                        }
                    }
                    ["configure", "save"] => {
                        let snapshot = config.lock().unwrap().clone();
                        match save_configuration(
                            &config_path,
                            &snapshot,
                            &clients,
                            &external,
                            &smtp,
                            &restapi_config,
                            &webrtc,
                        ) {
                            Ok(()) => cli_textout::line("configuration saved to netmark.config"),
                            Err(error) => cli_textout::line(format!("config save error: {error}")),
                        }
                    }
                    ["configure", "reset"] => {
                        reset_configuration(
                            &config_path,
                            &config,
                            &clients,
                            &external,
                            &smtp,
                            &restapi_config,
                            &webrtc,
                        );
                        cli_textout::line("configuration reset to defaults");
                    }
                    ["configure", "smtp", value] => {
                        smtp.lock().unwrap().server = Some(value.to_string());
                        cli_textout::line(format!("SMTP server set to {value}"));
                    }
                    ["configure", "tcp", "maxjitter", value] => match value.parse::<u64>() {
                        Ok(value) => {
                            config.lock().unwrap().max_tcp_jitter_millis = value;
                            cli_textout::line(format!("TCP maximum jitter set to {value} ms"));
                        }
                        Err(_) => cli_textout::line("TCP maxjitter must be milliseconds"),
                    },
                    ["configure", "udp", "max", "jitter", value] => match value.parse::<u64>() {
                        Ok(value) => {
                            config.lock().unwrap().max_udp_jitter_millis = value;
                            cli_textout::line(format!("UDP maximum jitter set to {value} ms"));
                        }
                        Err(_) => cli_textout::line("UDP max jitter must be milliseconds"),
                    },
                    ["configure"] => cli_textout::line(CONFIGURE_USAGE),
                    ["configure", rest @ ..] => match configure(&config, rest) {
                        Ok(()) => cli_textout::line("configuration updated"),
                        Err(error) => cli_textout::line(error),
                    },
                    // Command: metrics — Subcommands: status, enable, disable
                    ["metrics"] => cli_textout::line("metrics: enable | disable | status"),
                    ["metrics", "status"] => {
                        let status = external
                            .lock()
                            .unwrap()
                            .as_ref()
                            .map(|sink| sink.status())
                            .unwrap_or_else(|| "not connected".to_string());
                        cli_textout::line(format!("metrics SQL: {status}"));
                    }
                    ["metrics", "enable"] => match load_default_metrics_sink() {
                        Some(sink) => {
                            *external.lock().unwrap() = Some(sink);
                            cli_textout::line("external SQL metrics enabled");
                        }
                        None => cli_textout::line("external SQL metrics could not connect"),
                    },
                    ["metrics", "disable"] => {
                        *external.lock().unwrap() = None;
                        cli_textout::line("external SQL metrics disabled");
                    }
                    ["metrics", ..] => cli_textout::line("metrics: enable | disable | status"),
                    // Command: monitor — Subcommand: IP <url>
                    ["monitor"] => cli_textout::line("monitor: IP <url> | start | stop | history"),
                    ["monitor", "IP", target] | ["monitor", "ip", target] => {
                        let target = normalize_http_target(target);
                        monitor.set_target(target.clone());
                        cli_textout::line(format!("monitor target set to {target}"));
                    }
                    // Command: selftest
                    ["selftest"] => run_selftest(
                        &config, &stopping, &running, &metrics, &sql, &external, &log_dir,
                    ),
                    // Command: benchmark — Subcommand: duration <seconds>
                    ["benchmark"] => cli_textout::line("benchmark: duration <seconds>"),
                    ["benchmark", "duration", seconds] => match seconds.parse::<u64>() {
                        Ok(seconds) if seconds > 0 => run_benchmark(
                            &clients
                                .get(0)
                                .map(|client| client.remote)
                                .unwrap_or_else(|| DEFAULT_REMOTE.to_string()),
                            seconds,
                            &sql,
                            &log_dir,
                        ),
                        _ => cli_textout::line(
                            "benchmark duration must be a positive number of seconds",
                        ),
                    },
                    ["benchmark", ..] => cli_textout::line("benchmark: duration <seconds>"),
                    // Command: clean
                    ["clean"] => {
                        cli_textout::line(
                            "Are you sure? This will delete all data from runs on this instance of netmark.",
                        );
                        cli_textout::line("Confirm with Y or N.");
                        clean_confirmation = true;
                    }
                    // Subcommand: monitor start
                    ["monitor", "start"] => {
                        if let Some(id) = monitor.start(&log_dir, &sql) {
                            cli_textout::line(format!("monitor started {id}"));
                        } else {
                            cli_textout::line("monitor already running");
                        }
                    }
                    ["monitor", "stop"] => {
                        monitor.stop(&log_dir, &sql);
                        cli_textout::line("monitor stopped");
                    }
                    // Subcommand: monitor history
                    ["monitor", "history"] => show_monitor_history(&log_dir),
                    ["monitor", ..] => {
                        cli_textout::line("monitor: IP <url> | start | stop | history")
                    }
                    // Command: start
                    ["start"] => {
                        if running.load(Ordering::Relaxed) {
                            cli_textout::line("a run is already active; stop it before starting another");
                            continue;
                        }
                        let enabled = clients.enabled();
                        run_id = sql.next_run_id(run_id + 1);
                        sql.set_role(core::role_name(!enabled.is_empty(), server_enabled));
                        stopping.store(false, Ordering::Relaxed);
                        metrics.snapshot();
                        metrics.reset_run();
                        run_started = Some(Instant::now());
                        let gate = Arc::new(StartGate::new());
                        config.lock().unwrap().webrtc = webrtc.lock().unwrap().clone();
                        write_run_event(&log_dir, run_id, "Starting");
                        if server_enabled {
                            core::spawn_server(
                                Arc::clone(&config),
                                Arc::clone(&gate),
                                Arc::clone(&stopping),
                                Arc::clone(&metrics),
                                log_dir.clone(),
                            );
                            core::spawn_debrief_responder(
                                Arc::clone(&stopping),
                                Arc::clone(&metrics),
                                Arc::clone(&sql),
                                log_dir.clone(),
                            );
                        }
                        for client in &enabled {
                            let traffic = netmark::traffic_config_from(&config.lock().unwrap());
                            let per_client = netmark::client_config(
                                &traffic,
                                client,
                                &webrtc.lock().unwrap(),
                            );
                            core::spawn_client(
                                Arc::new(Mutex::new(per_client)),
                                Arc::clone(&gate),
                                Arc::clone(&stopping),
                                Arc::clone(&metrics),
                                client.remote.clone(),
                                log_dir.clone(),
                                client.id,
                            );
                        }
                        sql.start_run(run_id);
                        running.store(true, Ordering::Relaxed);
                        gate.start();
                        cli_mode = false;
                        cli_textout::line(format!(
                            "started run {run_id} with {} client(s)",
                            enabled.len()
                        ));
                    }
                    // Command: stop
                    ["stop"] => {
                        if !running.load(Ordering::Relaxed) {
                            cli_textout::line("no run is active");
                            continue;
                        }
                        stopping.store(true, Ordering::Relaxed);
                        running.store(false, Ordering::Relaxed);
                        cli_mode = true;
                        write_run_event(&log_dir, run_id, "Completed");
                        let elapsed = run_started.take().map(|started| started.elapsed());
                        let mut outcome = evaluate_run(&metrics, &config.lock().unwrap(), elapsed);
                        // Counters are per instance, so one debrief covers every
                        // client; it goes to the lowest-numbered client's remote.
                        if let Some(client) =
                            clients.enabled().into_iter().min_by_key(|client| client.id)
                        {
                            let packet_type = config.lock().unwrap().packet_type;
                            match core::run_debrief(
                                &client.remote,
                                run_id,
                                packet_type,
                                &metrics,
                                &sql,
                                &log_dir,
                            ) {
                                Ok(debrief) => {
                                    cli_textout::line(debrief.summary());
                                    if let Some(reason) = debrief.mismatch_reason() {
                                        outcome.add_failure(format!("debrief mismatch: {reason}"));
                                    }
                                }
                                Err(error) => {
                                    cli_textout::line(&error);
                                    outcome.add_failure(error);
                                }
                            }
                        }
                        sql.complete_run(run_id, &outcome.summary());
                        record_final_metrics(
                            external.lock().unwrap().as_ref(),
                            run_id,
                            &metrics,
                            &outcome,
                        );
                        netmark::write_log_line(&log_dir, &outcome.report_line(run_id));
                        // The transport is named on stop so an SCTP run is not
                        // mistaken for the TCP counters it shares.
                        cli_textout::line(format!(
                            "stopped transport={} {}",
                            config.lock().unwrap().packet_type.as_str(),
                            outcome.report_line(run_id)
                        ));
                    }
                    // Command: status
                    ["status"] => print_status(
                        &metrics,
                        &StatusContext {
                            running: running.load(Ordering::Relaxed),
                            elapsed: run_started.map(|started| started.elapsed()),
                            run_id,
                            server_enabled,
                            clients: &clients,
                            webrtc: &webrtc.lock().unwrap(),
                            packet_type: config.lock().unwrap().packet_type,
                            monitor: monitor.status(),
                            metrics_sql: external
                                .lock()
                                .unwrap()
                                .as_ref()
                                .map(|sink| sink.status())
                                .unwrap_or_else(|| "not connected".to_string()),
                            restapi: restapi.status(),
                            web_server: web_server_status(
                                &restapi.address(),
                                &restapi_config.lock().unwrap().address,
                            ),
                            smtp: smtp.lock().unwrap().enabled,
                        },
                    ),
                    // Command: show run <id>
                    ["show", "run", value] => {
                        if let Ok(id) = value.parse::<u64>() {
                            match sql.show_run(id) {
                                Ok(Some(row)) => {
                                    cli_textout::line(format!("Run {id}:"));
                                    cli_textout::line(row);
                                    for line in sql.debriefs(id).unwrap_or_default() {
                                        cli_textout::line(line);
                                    }
                                }
                                Ok(None) => {
                                    cli_textout::line(format!("no local record for run {id}"))
                                }
                                Err(error) => cli_textout::line(format!("sql error: {error}")),
                            }
                        }
                    }
                    // Command: list
                    ["list"] => match sql.list_runs() {
                        Ok(()) => {}
                        Err(error) => cli_textout::line(format!("sql error: {error}")),
                    },
                    // Command: sctp — the detailed SCTP help page
                    ["sctp"] => print_sctp_help(&stdout_guard, &output),
                    ["sctp", "status"] => {
                        cli_textout::line(format!("SCTP: {}", netmark::restapi::sctp_status()))
                    }
                    ["sctp", ..] => cli_textout::line("sctp: (no argument for the help page) | status"),
                    // Command: help
                    ["help"] => print_help(&stdout_guard, &output),
                    // Command: quit | exit
                    ["quit"] | ["exit"] => break,
                    [] => {}
                    _ => cli_textout::line("unknown command; type 'help' for commands"),
                }
                if cli_mode && !clean_confirmation {
                    redraw_prompt(
                        server_enabled,
                        clients.any_enabled(),
                        running.load(Ordering::Relaxed),
                    );
                }
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char(character),
                modifiers,
                ..
            }) if cli_mode && !modifiers.contains(KeyModifiers::CONTROL) => {
                input.push(character);
                cli_textout::raw(character.to_string());
            }
            Event::Key(KeyEvent {
                code: KeyCode::Backspace,
                ..
            }) if cli_mode && input.pop().is_some() => cli_textout::raw("\x08 \x08"),
            _ => {}
        }
    }
    stopping.store(true, Ordering::Relaxed);
    running.store(false, Ordering::Relaxed);
    monitor.stop(&log_dir, &sql);
    disable_raw_mode().ok();
    if let Some(started) = run_started {
        write_run_event(&log_dir, run_id, "Completed");
        let mut outcome = evaluate_run(&metrics, &config.lock().unwrap(), Some(started.elapsed()));
        outcome.result = "aborted";
        sql.complete_run(run_id, &outcome.summary());
        record_final_metrics(external.lock().unwrap().as_ref(), run_id, &metrics, &outcome);
        netmark::write_log_line(&log_dir, &outcome.report_line(run_id));
    }
    clear_input_line();
    cli_textout::raw("\r\n");
}

/// Headless server mode for containers and services: no interactive terminal,
/// just the REST API and the web interface, blocking until the process is
/// stopped. `netmark --serve [address]`; without an address the configured
/// (loopback by default) REST API address is used.
fn serve_mode(address: Option<String>) {
    let log_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("log");
    create_dir_all(&log_dir).expect("cannot create log directory");
    let config_path = configuration::path_near_executable()
        .unwrap_or_else(|| std::path::PathBuf::from("netmark.config"));
    let file_config = configuration::load(&config_path).unwrap_or_default();
    let sql = SqlState::new();
    sql.enable()
        .expect("cannot initialize local SQLite database");
    let clients = Arc::new(Clients::new(file_config.clients.clone()));
    let restapi = Arc::new(netmark::restapi::RestApi::new(clients, log_dir));
    let address = address.unwrap_or_else(|| file_config.restapi.address.clone());
    if let Err(error) = restapi.enable(&address) {
        eprintln!("cannot start server: {error}");
        std::process::exit(1);
    }
    println!("netmark serving the web interface and REST API on http://{address}");
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn redraw_input(server: bool, client: bool, running: &AtomicBool, input: &str) {
    redraw_prompt(server, client, running.load(Ordering::Relaxed));
    cli_textout::raw(input);
    io::stdout().flush().ok();
}

fn prompt(server: bool, client: bool, running: bool) -> String {
    match (server, client, running) {
        (true, true, true) => "Client Running | Server Running >".into(),
        (true, true, false) => "Client | Server >".into(),
        (true, false, true) => "Server Running >".into(),
        (true, false, false) => "Server >".into(),
        (false, true, true) => "Client Running >".into(),
        (false, true, false) => "Client >".into(),
        _ => ">".into(),
    }
}
fn print_prompt(server: bool, client: bool, running: bool) {
    cli_textout::raw(format!("{} ", prompt(server, client, running)));
}
fn redraw_prompt(server: bool, client: bool, running: bool) {
    cli_textout::raw("\r\x1b[2K");
    print_prompt(server, client, running);
}
fn clear_input_line() {
    cli_textout::raw("\r\x1b[2K");
}
fn start_output_process() -> (ChildStdin, Arc<Mutex<()>>) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--output")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("cannot start output process");
    let mut child_stdout = child.stdout.take().unwrap();
    let child_stdin = child.stdin.take().unwrap();
    let stdout_guard = Arc::new(Mutex::new(()));
    let reader_guard = Arc::clone(&stdout_guard);
    thread::spawn(move || {
        let mut buffer = String::new();
        let mut reader = std::io::BufReader::new(&mut child_stdout);
        while reader.read_line(&mut buffer).unwrap_or(0) > 0 {
            let _guard = reader_guard.lock().unwrap();
            cli_textout::raw(&buffer);
            buffer.clear();
        }
        let _ = child.wait();
    });
    (child_stdin, stdout_guard)
}
fn output_process() {
    let mut visible = true;
    for line in io::stdin().lock().lines().map_while(Result::ok) {
        if line == "SHOW" {
            visible = true;
        } else if line == "HIDE" {
            visible = false;
        } else if visible {
            cli_textout::line(line);
        }
    }
}
fn report_loop(
    (metrics, stopping, running, output, config, log_dir): ReportLoopArgs,
) {
    loop {
        thread::sleep(Duration::from_secs(1));
        if !running.load(Ordering::Relaxed) {
            continue;
        }
        let values = metrics.snapshot();
        let (lost, order) = metrics.udp_status();
        let jitter = metrics.jitter_millis();
        let config_snapshot = config.lock().unwrap().clone();
        if (metrics.tcp_jitter_millis() > config_snapshot.max_tcp_jitter_millis
            || metrics.udp_jitter_millis() > config_snapshot.max_udp_jitter_millis)
            && let Ok(mut log) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_dir.join("netmark.log"))
        {
            let _ = writeln!(
                log,
                "{} ERROR jitter exceeded TCP={}ms UDP={}ms",
                core::timestamp(),
                metrics.tcp_jitter_millis(),
                metrics.udp_jitter_millis()
            );
        }
        // The loop ticks once per second, so these totals are already bytes/sec.
        let line = format!(
            "up {} bytes/sec, down {} bytes/sec | Client sent: TCP {} bytes, UDP/IP {} bytes | Server received: TCP {} bytes, UDP/IP {} bytes (lost {}, out-of-order {}, jitter {} ms)",
            values[1] + values[3],
            values[5] + values[7],
            values[1],
            values[3],
            values[5],
            values[7],
            lost,
            order,
            jitter
        );
        let mut output = output.lock().unwrap();
        if writeln!(output, "{line}").is_err() || output.flush().is_err() {
            break;
        }
        if stopping.load(Ordering::Relaxed) && !running.load(Ordering::Relaxed) {
            break;
        }
    }
}
