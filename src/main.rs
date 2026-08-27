mod cli_textout;
mod core;
mod metrics;

use core::{Config, DEFAULT_REMOTE, Metrics, PacketType, SqlState, StartGate};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use metrics::ExternalSqlMetrics;
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

fn main() {
    if std::env::args().any(|arg| arg == "--output") {
        output_process();
        return;
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
    let config = Arc::new(Mutex::new(Config {
        rate: 100,
        packet_type: PacketType::Tcp,
        tcp_bytes_per_second: 1024,
        udp_packet_size: 1024,
        client_runtime: 0,
        server_runtime: 0,
    }));
    let stopping = Arc::new(AtomicBool::new(false));
    let running = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let sql = Arc::new(SqlState::new());
    sql.enable()
        .expect("cannot initialize local SQLite database");
    let external = Arc::new(Mutex::new(None::<Arc<ExternalSqlMetrics>>));
    let monitor_target = Arc::new(Mutex::new(None::<String>));
    let monitor_running = Arc::new(AtomicBool::new(false));
    let (output_stdin, stdout_guard) = start_output_process();
    let output = Arc::new(Mutex::new(output_stdin));
    {
        let args = (
            Arc::clone(&metrics),
            Arc::clone(&stopping),
            Arc::clone(&running),
            Arc::clone(&sql),
            Arc::clone(&output),
            Arc::clone(&external),
        );
        thread::spawn(move || report_loop(args));
    }
    {
        let args = (
            log_dir.clone(),
            Arc::clone(&monitor_target),
            Arc::clone(&monitor_running),
            Arc::clone(&sql),
        );
        thread::spawn(move || monitor_loop(args));
    }
    let mut cli_log = OpenOptions::new()
        .append(true)
        .open(log_dir.join("cli.log"))
        .unwrap();
    let mut input = String::new();
    let mut server_enabled = false;
    let mut client_enabled = false;
    let mut remote = DEFAULT_REMOTE.to_string();
    let mut run_id = 0u64;
    let mut cli_mode = true;
    enable_raw_mode().expect("cannot enable terminal input");
    print_prompt(server_enabled, client_enabled, false);
    loop {
        if !event::poll(Duration::from_millis(100)).unwrap() {
            continue;
        }
        match event::read().unwrap() {
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers,
                ..
            }) if modifiers.contains(KeyModifiers::CONTROL) => {
                stopping.store(true, Ordering::Relaxed);
                running.store(false, Ordering::Relaxed);
                monitor_running.store(false, Ordering::Relaxed);
                disable_raw_mode().ok();
                clear_input_line();
                println!("\nexiting\n");
                break;
            }
            Event::Key(KeyEvent {
                code: KeyCode::Esc, ..
            }) if running.load(Ordering::Relaxed) => {
                stopping.store(true, Ordering::Relaxed);
                running.store(false, Ordering::Relaxed);
                cli_mode = true;
                println!("\nstopped");
                print_prompt(server_enabled, client_enabled, false);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Tab, ..
            }) => {
                cli_mode = !cli_mode;
                let mut out = output.lock().unwrap();
                let _ = writeln!(out, "{}", if cli_mode { "HIDE" } else { "SHOW" });
                let _ = out.flush();
                print!("\r\x1b[2K\r\n");
                if cli_mode {
                    print_prompt(
                        server_enabled,
                        client_enabled,
                        running.load(Ordering::Relaxed),
                    );
                }
                io::stdout().flush().unwrap();
            }
            Event::Key(KeyEvent {
                code: KeyCode::Enter,
                ..
            }) if cli_mode => {
                let line = input.trim().to_string();
                input.clear();
                print!("\r\n");
                writeln!(cli_log, "{} {}", core::timestamp(), line).unwrap();
                cli_log.flush().unwrap();
                match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                    ["client"] => {
                        println!("client: enable | disable | remote <ip> | runtime <seconds>")
                    }
                    ["client", "enable"] => {
                        client_enabled = true;
                        println!("client enabled");
                    }
                    ["client", "disable"] => {
                        client_enabled = false;
                        stopping.store(true, Ordering::Relaxed);
                        println!("Client stopped");
                    }
                    ["client", "remote", host] => {
                        remote = (*host).into();
                        println!("client remote set to {remote}");
                    }
                    ["client", "http", "check", url] => client_http_check(&log_dir, url),
                    ["client", "runtime", seconds] => set_runtime(&config, true, seconds),
                    ["server"] => println!("server: enable | disable | runtime <seconds>"),
                    ["server", "enable"] => {
                        server_enabled = true;
                        println!("server enabled");
                    }
                    ["server", "disable"] => {
                        server_enabled = false;
                        stopping.store(true, Ordering::Relaxed);
                        println!("Server stopped");
                    }
                    ["server", "runtime", seconds] => set_runtime(&config, false, seconds),
                    ["configure", rest @ ..] => match configure(&config, rest) {
                        Ok(()) => println!("configuration updated"),
                        Err(error) => eprintln!("configure error: {error}"),
                    },
                    ["metrics", "add", "sql", connection] => {
                        match ExternalSqlMetrics::connect(connection) {
                            Ok(sink) => {
                                *external.lock().unwrap() = Some(Arc::new(sink));
                                println!("external SQL metrics enabled");
                            }
                            Err(error) => eprintln!("metrics error: {error}"),
                        }
                    }
                    ["monitor", "IP", target] | ["monitor", "ip", target] => {
                        let target = normalize_http_target(target);
                        *monitor_target.lock().unwrap() = Some(target.clone());
                        println!("monitor target set to {target}");
                    }
                    ["selftest"] => {
                        run_selftest(&config, &stopping, &running, &metrics, &sql, &log_dir)
                    }
                    ["monitor", "start"] => monitor_running.store(true, Ordering::Relaxed),
                    ["monitor", "stop"] => monitor_running.store(false, Ordering::Relaxed),
                    ["monitor", "history"] => show_monitor_history(&log_dir),
                    ["start"] => {
                        run_id = sql.next_run_id(run_id + 1);
                        stopping.store(false, Ordering::Relaxed);
                        metrics.snapshot();
                        let gate = Arc::new(StartGate::new());
                        write_run_event(&log_dir, run_id, "Starting");
                        if server_enabled {
                            core::spawn_server(
                                Arc::clone(&config),
                                Arc::clone(&gate),
                                Arc::clone(&stopping),
                                Arc::clone(&metrics),
                                log_dir.clone(),
                            );
                        }
                        if client_enabled {
                            core::spawn_client(
                                Arc::clone(&config),
                                Arc::clone(&gate),
                                Arc::clone(&stopping),
                                Arc::clone(&metrics),
                                remote.clone(),
                                log_dir.clone(),
                            );
                        }
                        sql.start_run(run_id);
                        running.store(true, Ordering::Relaxed);
                        gate.start();
                        cli_mode = false;
                        println!("started run {run_id}");
                    }
                    ["stop"] => {
                        stopping.store(true, Ordering::Relaxed);
                        running.store(false, Ordering::Relaxed);
                        cli_mode = true;
                        write_run_event(&log_dir, run_id, "Completed");
                        sql.complete_run(run_id, "ok");
                        println!("stopped");
                    }
                    ["status"] => println!(
                        "{}",
                        traffic_status(&metrics, running.load(Ordering::Relaxed))
                    ),
                    ["sql", "enable"] => match sql.enable() {
                        Ok(()) => println!("sql enabled"),
                        Err(error) => eprintln!("sql error: {error}"),
                    },
                    ["show", "run", value] => {
                        if let Ok(id) = value.parse() {
                            let _ = sql.show_run(id);
                        }
                    }
                    ["list"] => match sql.list_runs() {
                        Ok(()) => {}
                        Err(error) => eprintln!("sql error: {error}"),
                    },
                    ["help"] => print_help(&stdout_guard, &output),
                    ["quit"] | ["exit"] => break,
                    [] => {}
                    _ => eprintln!("unknown command; type 'help' for commands"),
                }
                if cli_mode {
                    redraw_prompt(
                        server_enabled,
                        client_enabled,
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
                print!("{character}");
                io::stdout().flush().unwrap();
            }
            Event::Key(KeyEvent {
                code: KeyCode::Backspace,
                ..
            }) if cli_mode => {
                if input.pop().is_some() {
                    print!("\x08 \x08");
                    io::stdout().flush().unwrap();
                }
            }
            _ => {}
        }
    }
    stopping.store(true, Ordering::Relaxed);
    running.store(false, Ordering::Relaxed);
    monitor_running.store(false, Ordering::Relaxed);
    disable_raw_mode().ok();
    write_run_event(&log_dir, run_id, "Completed");
    sql.complete_run(run_id, "aborted");
    clear_input_line();
    print!("\r\n");
}

fn set_runtime(config: &Arc<Mutex<Config>>, client: bool, value: &str) {
    match value.parse::<u64>() {
        Ok(value) => {
            if client {
                config.lock().unwrap().client_runtime = value;
            } else {
                config.lock().unwrap().server_runtime = value;
            }
            println!("runtime set to {value} seconds");
        }
        Err(_) => eprintln!("runtime must be a non-negative integer"),
    }
}
fn normalize_http_target(target: &str) -> String {
    if target.starts_with("http://") || target.starts_with("https://") {
        target.to_string()
    } else {
        format!("http://{target}")
    }
}
fn run_selftest(
    config: &Arc<Mutex<Config>>,
    stopping: &Arc<AtomicBool>,
    running: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    sql: &Arc<SqlState>,
    log_dir: &std::path::Path,
) {
    if running.swap(true, Ordering::Relaxed) {
        println!("already running");
        return;
    }
    let run_id = sql.next_run_id(1);
    {
        let mut config = config.lock().unwrap();
        config.packet_type = PacketType::Udp;
        config.rate = 1;
        config.udp_packet_size = 1024;
        config.client_runtime = 1;
        config.server_runtime = 1;
    }
    stopping.store(false, Ordering::Relaxed);
    metrics.snapshot();
    let gate = Arc::new(StartGate::new());
    write_run_event(log_dir, run_id, "Starting");
    core::spawn_server(
        Arc::clone(config),
        Arc::clone(&gate),
        Arc::clone(stopping),
        Arc::clone(metrics),
        log_dir.to_path_buf(),
    );
    core::spawn_client(
        Arc::clone(config),
        Arc::clone(&gate),
        Arc::clone(stopping),
        Arc::clone(metrics),
        DEFAULT_REMOTE.to_string(),
        log_dir.to_path_buf(),
    );
    sql.start_run(run_id);
    gate.start();
    println!("selftest started run {run_id}");
    let stop = Arc::clone(stopping);
    let state = Arc::clone(running);
    let sql_state = Arc::clone(sql);
    let logs = log_dir.to_path_buf();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
        state.store(false, Ordering::Relaxed);
        sql_state.complete_run(run_id, "ok");
        write_run_event(&logs, run_id, "Completed");
        println!("selftest completed run {run_id}");
    });
}
fn configure(config: &Arc<Mutex<Config>>, args: &[&str]) -> Result<(), String> {
    if let ["tcp", "bytes", value] = args {
        let value = value
            .parse()
            .map_err(|_| "TCP bytes/sec must be positive")?;
        if value == 0 {
            return Err("TCP bytes/sec must be positive".into());
        }
        config.lock().unwrap().tcp_bytes_per_second = value;
        return Ok(());
    }
    if let ["udp", "packetsize", value] = args {
        let value = value
            .parse()
            .map_err(|_| "UDP packet size must be at least 8")?;
        if value < 8 {
            return Err("UDP packet size must be at least 8".into());
        }
        config.lock().unwrap().udp_packet_size = value;
        return Ok(());
    }
    if let ["type", value] = args {
        let packet_type = PacketType::parse(value).ok_or("type must be tcp or udp")?;
        config.lock().unwrap().packet_type = packet_type;
        return Ok(());
    }
    Err("use: configure tcp bytes <rate>, udp packetsize <bytes>, or type <tcp|udp>".into())
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
    print!("{} ", prompt(server, client, running));
    io::stdout().flush().unwrap();
}
fn redraw_prompt(server: bool, client: bool, running: bool) {
    print!("\r\x1b[2K");
    print_prompt(server, client, running);
}
fn clear_input_line() {
    print!("\r\x1b[2K");
    io::stdout().flush().unwrap();
}
fn client_http_check(log_dir: &std::path::Path, url: &str) {
    let url = normalize_http_target(url);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("HTTP client error: {error}");
            return;
        }
    };
    let started = Instant::now();
    match client
        .get(&url)
        .send()
        .and_then(|response| response.error_for_status())
    {
        Ok(response) => match response.bytes() {
            Ok(body) => {
                let elapsed = started.elapsed().as_millis();
                println!(
                    "HTTP check succeeded: {url} ({elapsed} ms, {} bytes)",
                    body.len()
                );
                if let Ok(mut log) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_dir.join("client.log"))
                {
                    let _ = writeln!(
                        log,
                        "{} HTTP {} tcp_connection_ms={} bytes={}",
                        core::timestamp(),
                        url,
                        elapsed,
                        body.len()
                    );
                }
            }
            Err(error) => eprintln!("HTTP body error: {error}"),
        },
        Err(error) => eprintln!("HTTP check failed: {error}"),
    }
}
fn write_run_event(log_dir: &std::path::Path, run_id: u64, event: &str) {
    if run_id == 0 {
        return;
    }
    for name in ["cli.log", "client.log", "server.log", "netmark.log"] {
        let path = log_dir.join(name);
        let marker = format!("{} run {}", event, run_id);
        let already_logged = std::fs::read_to_string(&path)
            .map(|contents| contents.lines().any(|line| line.ends_with(&marker)))
            .unwrap_or(false);
        if !already_logged {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
                let _ = writeln!(file, "{} {}", core::timestamp(), marker);
            }
        }
    }
}
fn traffic_status(metrics: &Metrics, running: bool) -> String {
    let values = metrics.current();
    let (lost, order) = metrics.udp_status();
    format!(
        "{} TCP sent {} bytes, UDP sent {} bytes, TCP received {} bytes, UDP received {} bytes, UDP lost {}, out of order {}",
        if running { "running" } else { "stopped" },
        values[1],
        values[3],
        values[5],
        values[7],
        lost,
        order
    )
}
fn start_output_process() -> (ChildStdin, Arc<Mutex<()>>) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--output")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("cannot start output process");
    let mut child_stdout = child.stdout.take().unwrap();
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
    });
    (child.stdin.take().unwrap(), stdout_guard)
}
fn output_process() {
    let mut visible = true;
    for line in io::stdin().lock().lines().flatten() {
        if line == "SHOW" {
            visible = true;
        } else if line == "HIDE" {
            visible = false;
        } else if visible {
            cli_textout::line(line);
        }
    }
}
fn show_monitor_history(log_dir: &std::path::Path) {
    for name in ["netmark.log", "alarm.log"] {
        if let Ok(contents) = std::fs::read_to_string(log_dir.join(name)) {
            for line in contents.lines() {
                if name == "alarm.log"
                    || line.contains("Monitor started")
                    || line.contains("Monitor stopped")
                {
                    cli_textout::line(&line);
                }
            }
        }
    }
}
fn monitor_loop(
    (log_dir, target, running, sql): (
        std::path::PathBuf,
        Arc<Mutex<Option<String>>>,
        Arc<AtomicBool>,
        Arc<SqlState>,
    ),
) {
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(value) => value,
        Err(_) => return,
    };
    while !std::thread::panicking() {
        if running.load(Ordering::Relaxed) {
            if let Some(target) = target.lock().unwrap().clone() {
                if let Err(error) = client
                    .get(&target)
                    .send()
                    .and_then(|response| response.error_for_status())
                    .and_then(|response| response.bytes().map(|_| ()))
                {
                    let time = core::timestamp();
                    if let Ok(mut alarm) = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(log_dir.join("alarm.log"))
                    {
                        let _ = writeln!(alarm, "{} {} {}", time, target, error);
                    }
                    sql.record_alarm(&time, &target, &error.to_string());
                }
            }
        }
        thread::sleep(Duration::from_secs(30));
    }
}
fn report_loop(
    (metrics, stopping, running, sql, output, external): (
        Arc<Metrics>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        Arc<SqlState>,
        Arc<Mutex<ChildStdin>>,
        Arc<Mutex<Option<Arc<ExternalSqlMetrics>>>>,
    ),
) {
    loop {
        thread::sleep(Duration::from_secs(1));
        if !running.load(Ordering::Relaxed) {
            continue;
        }
        let values = metrics.snapshot();
        let (lost, order) = metrics.udp_status();
        let jitter = metrics.jitter_millis();
        sql.write_snapshot(&values, lost, order, jitter);
        if let Some(sink) = external.lock().unwrap().as_ref() {
            let _ = sink.write(
                &core::timestamp(),
                sql.current_run_id(),
                &values,
                lost,
                order,
                jitter,
            );
        }
        let line = format!(
            "Server: TCP {} bytes, UDP {} bytes (lost {}, out-of-order {}, jitter {} ms) Received | Client: TCP {} bytes, UDP {} bytes Sent",
            values[5], values[7], lost, order, jitter, values[1], values[3]
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
fn print_help(stdout_guard: &Arc<Mutex<()>>, output: &Arc<Mutex<ChildStdin>>) {
    let mut output = output.lock().unwrap();
    let _ = writeln!(output, "HIDE");
    let _ = output.flush();
    let _guard = stdout_guard.lock().unwrap();
    cli_textout::lines([
        "server - receiving side of traffic.",
        "  enable - enable server traffic.",
        "  disable - stop and disable server traffic.",
        "  runtime <seconds> - limit server runtime; zero is unlimited.",
        "client - sending side of traffic.",
        "  enable - enable client traffic.",
        "  disable - stop and disable client traffic.",
        "  remote <ip> - set the client destination.",
        "  runtime <seconds> - limit client runtime; zero is unlimited.",
        "  http check <url> - load one HTTP or HTTPS page and log timing.",
        "selftest - send UDP traffic to localhost and stop automatically.",
        "configure - change traffic settings.",
        "  tcp bytes <rate> - set TCP bytes per second.",
        "  udp packetsize <bytes> - set UDP packet size.",
        "metrics add sql <connection> - publish one-second metrics externally.",
        "monitor - check an HTTP or HTTPS endpoint.",
        "  IP <url> - set monitor target.",
        "  start - start checks every 30 seconds.",
        "  stop - stop monitor checks.",
        "  history - show monitor events and alarms.",
        "start - start a traffic run.",
        "stop - stop the traffic run.",
        "status - show current counters.",
        "sql enable - enable local SQLite snapshots.",
        "show run <id> - show stored run metrics.",
        "list - list all run IDs and results.",
        "help - show this help.",
        "exit - stop workers and exit.",
    ]);
}
