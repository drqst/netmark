use netmark::cli::*;
use netmark::core::{Config, Metrics};
use netmark::session::Session;
use netmark::{
    cli_textout, configuration, core, evaluate_run, record_final_metrics, run_auto_mode,
    write_run_event,
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
    let clients = Arc::new(Clients::new(file_config.clients.clone()));
    let restapi = Arc::new(netmark::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    if let Some(error) = netmark::restapi::start_if_enabled(&restapi, &file_config.restapi) {
        eprintln!("REST API not started: {error}");
    }
    // The session owns the shared state and the one command dispatcher; the
    // interactive loop below only draws the terminal and hands lines to it.
    let session = Arc::new(Session::from_config(
        &file_config,
        log_dir.clone(),
        config_path.clone(),
        Arc::clone(&clients),
        Arc::clone(restapi.live()),
        Arc::downgrade(&restapi),
    ));
    let config = Arc::clone(session.config());
    let metrics = Arc::clone(session.metrics());
    let sql = Arc::clone(session.sql());
    let stopping = Arc::clone(session.stopping());
    let running = Arc::clone(session.running());
    let external = Arc::clone(session.external());
    let monitor = Arc::clone(session.monitor());
    println!(
        "netmark web server {}",
        web_server_status(&restapi.address(), &file_config.restapi.address)
    );
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
    let mut cli_mode = true;
    let mut clean_confirmation = false;
    enable_raw_mode().expect("cannot enable terminal input");
    print_prompt(session.server_enabled(), clients.any_enabled(), false);
    loop {
        // Publishing on every poll tick keeps the web status section live while
        // the operator is idle at the prompt.
        session.publish_live();
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
                print_prompt(session.server_enabled(), clients.any_enabled(), false);
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
                        session.server_enabled(),
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
                redraw_input(session.server_enabled(), clients.any_enabled(), &running, &input);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Down,
                ..
            }) if cli_mode && !clean_confirmation => {
                history.step_forward(&mut input);
                redraw_input(session.server_enabled(), clients.any_enabled(), &running, &input);
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
                // Every command is dispatched by the shared Session, so the shell
                // CLI and the web CLI can never disagree. Only terminal-only
                // behaviour stays here: the clean Y/N confirmation, the help and
                // SCTP pages that use the HIDE/SHOW output child, and quit/exit.
                match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                    ["clean"] => {
                        cli_textout::line(
                            "Are you sure? This will delete all data from runs on this instance of netmark.",
                        );
                        cli_textout::line("Confirm with Y or N.");
                        clean_confirmation = true;
                    }
                    // The detailed SCTP page and help hide the live display while
                    // they print, then restore it, so the tables never interleave.
                    ["sctp"] => print_sctp_help(&stdout_guard, &output),
                    ["help", topic @ ..] => print_help(&stdout_guard, &output, topic),
                    ["quit"] | ["exit"] => break,
                    [] => {}
                    tokens => {
                        let was_running = running.load(Ordering::Relaxed);
                        for reply in session.execute(&line).lines() {
                            cli_textout::line(reply);
                        }
                        // A started run switches the terminal to the live display;
                        // stopping it switches back to the prompt.
                        match tokens.first() {
                            Some(&"start")
                                if running.load(Ordering::Relaxed) && !was_running =>
                            {
                                cli_mode = false;
                            }
                            Some(&"stop")
                                if was_running && !running.load(Ordering::Relaxed) =>
                            {
                                cli_mode = true;
                            }
                            _ => {}
                        }
                    }
                }
                if cli_mode && !clean_confirmation {
                    redraw_prompt(
                        session.server_enabled(),
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
    if let Some(started) = session.run_started() {
        let run_id = session.run_id();
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
    let clients = Arc::new(Clients::new(file_config.clients.clone()));
    let restapi = Arc::new(netmark::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    // The headless server shares the same command dispatcher as the interactive
    // CLI, so the web CLI can drive runs while the process blocks below.
    let session = Arc::new(Session::from_config(
        &file_config,
        log_dir.clone(),
        config_path.clone(),
        Arc::clone(&clients),
        Arc::clone(restapi.live()),
        Arc::downgrade(&restapi),
    ));
    restapi.attach_session(Arc::clone(&session));
    let address = address.unwrap_or_else(|| file_config.restapi.address.clone());
    if let Err(error) = restapi.enable(&address) {
        eprintln!("cannot start server: {error}");
        std::process::exit(1);
    }
    println!("netmark serving the web interface and REST API on http://{address}");
    loop {
        // Refreshing the live status keeps the web status panel following runs
        // that the web CLI starts and stops.
        session.publish_live();
        thread::sleep(Duration::from_millis(200));
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
