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
        jitter_millis: 0,
        client_jitter_millis: 0,
        server_jitter_millis: 0,
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
    let mut clean_confirmation = false;
    enable_raw_mode().expect("cannot enable terminal input");
    print_prompt(server_enabled, client_enabled, false);
    loop {
        if !event::poll(Duration::from_millis(100)).unwrap() {
            continue;
        }
        match event::read().unwrap() {
            Event::Key(KeyEvent {
                code: KeyCode::Char(choice),
                ..
            }) if clean_confirmation => {
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
                print_prompt(
                    server_enabled,
                    client_enabled,
                    running.load(Ordering::Relaxed),
                );
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers,
                ..
            }) if modifiers.contains(KeyModifiers::CONTROL) => {
                cli_textout::line("");
                running.store(false, Ordering::Relaxed);
                monitor_running.store(false, Ordering::Relaxed);
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
                print_prompt(server_enabled, client_enabled, false);
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
                cli_textout::raw("\r\n");
                writeln!(cli_log, "{} {}", core::timestamp(), line).unwrap();
                cli_log.flush().unwrap();
                match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                    ["client"] => cli_textout::line(
                        "client: enable | disable | remote <ip> | runtime <seconds>",
                    ),
                    ["client", "enable"] => {
                        client_enabled = true;
                        cli_textout::line("client enabled");
                    }
                    ["client", "disable"] => {
                        client_enabled = false;
                        stopping.store(true, Ordering::Relaxed);
                        cli_textout::line("Client stopped");
                    }
                    ["client", "remote", host] => {
                        remote = (*host).into();
                        cli_textout::line(format!("client remote set to {remote}"));
                    }
                    ["client", "http", "check", url] => client_http_check(&log_dir, url),
                    ["client", "runtime", seconds] => set_runtime(&config, true, seconds),
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
                    ["configure", rest @ ..] => match configure(&config, rest) {
                        Ok(()) => cli_textout::line("configuration updated"),
                        Err(error) => cli_textout::line(format!("configure error: {error}")),
                    },
                    ["metrics", "add", "sql", connection] => {
                        match ExternalSqlMetrics::connect(connection) {
                            Ok(sink) => {
                                *external.lock().unwrap() = Some(Arc::new(sink));
                                cli_textout::line("external SQL metrics enabled");
                            }
                            Err(error) => cli_textout::line(format!("metrics error: {error}")),
                        }
                    }
                    ["monitor", "IP", target] | ["monitor", "ip", target] => {
                        let target = normalize_http_target(target);
                        *monitor_target.lock().unwrap() = Some(target.clone());
                        cli_textout::line(format!("monitor target set to {target}"));
                    }
                    ["selftest"] => {
                        run_selftest(&config, &stopping, &running, &metrics, &sql, &log_dir)
                    }
                    ["clean"] => {
                        cli_textout::line(
                            "Are you sure? This will delete all data from runs on this instance of netmark.",
                        );
                        cli_textout::line("Confirm with Y or N.");
                        clean_confirmation = true;
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
                        cli_textout::line(format!("started run {run_id}"));
                    }
                    ["stop"] => {
                        stopping.store(true, Ordering::Relaxed);
                        running.store(false, Ordering::Relaxed);
                        cli_mode = true;
                        write_run_event(&log_dir, run_id, "Completed");
                        sql.complete_run(run_id, "ok");
                        cli_textout::line("stopped");
                    }
                    ["status"] => {
                        cli_textout::line(traffic_status(&metrics, running.load(Ordering::Relaxed)))
                    }
                    ["sql", "enable"] => match sql.enable() {
                        Ok(()) => cli_textout::line("sql enabled"),
                        Err(error) => cli_textout::line(format!("sql error: {error}")),
                    },
                    ["sql", "disable"] => {
                        sql.disable();
                        cli_textout::line("local SQL disabled");
                    }
                    ["show", "run", value] => {
                        if let Ok(id) = value.parse() {
                            let _ = sql.show_run(id);
                        }
                    }
                    ["list"] => match sql.list_runs() {
                        Ok(()) => {}
                        Err(error) => cli_textout::line(format!("sql error: {error}")),
                    },
                    ["help"] => print_help(&stdout_guard, &output),
                    ["quit"] | ["exit"] => break,
                    [] => {}
                    _ => cli_textout::line("unknown command; type 'help' for commands"),
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
                cli_textout::raw(character.to_string());
            }
            Event::Key(KeyEvent {
                code: KeyCode::Backspace,
                ..
            }) if cli_mode => {
                if input.pop().is_some() {
                    cli_textout::raw("\x08 \x08");
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
    cli_textout::raw("\r\n");
}

fn set_runtime(config: &Arc<Mutex<Config>>, client: bool, value: &str) {
    match value.parse::<u64>() {
        Ok(value) => {
            if client {
                config.lock().unwrap().client_runtime = value;
            } else {
                config.lock().unwrap().server_runtime = value;
            }
            cli_textout::line(format!("runtime set to {value} seconds"));
        }
        Err(_) => cli_textout::line("runtime must be a non-negative integer"),
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
        cli_textout::line("already running");
        return;
    }
    let run_id = sql.next_run_id(1);
    {
        let mut config = config.lock().unwrap();
        config.packet_type = PacketType::Udp;
        config.rate = 1;
        config.udp_packet_size = 1024;
        config.client_runtime = 10;
        config.server_runtime = 10;
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
    cli_textout::line(format!("selftest started run {run_id}"));
    let stop = Arc::clone(stopping);
    let state = Arc::clone(running);
    let sql_state = Arc::clone(sql);
    let logs = log_dir.to_path_buf();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(11));
        stop.store(true, Ordering::Relaxed);
        state.store(false, Ordering::Relaxed);
        sql_state.complete_run(run_id, "ok");
        write_run_event(&logs, run_id, "Completed");
        cli_textout::line(format!("selftest completed run {run_id}"));
    });
}
fn configure(config: &Arc<Mutex<Config>>, args: &[&str]) -> Result<(), String> {
    if let ["jitter", value] = args {
        let value = value.parse().map_err(|_| "jitter must be milliseconds")?;
        config.lock().unwrap().jitter_millis = value;
        return Ok(());
    }
    if let [protocol, "jitter", value] = args {
        let value = value.parse().map_err(|_| "jitter must be milliseconds")?;
        match *protocol {
            "tcp" => config.lock().unwrap().client_jitter_millis = value,
            "udp" => config.lock().unwrap().client_jitter_millis = value,
            _ => return Err("use configure tcp jitter <ms> or configure udp jitter <ms>".into()),
        }
        return Ok(());
    }
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
    cli_textout::raw(format!("{} ", prompt(server, client, running)));
}
fn redraw_prompt(server: bool, client: bool, running: bool) {
    cli_textout::raw("\r\x1b[2K");
    print_prompt(server, client, running);
}
fn clear_input_line() {
    cli_textout::raw("\r\x1b[2K");
}
fn client_http_check(log_dir: &std::path::Path, url: &str) {
    let url = normalize_http_target(url);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            cli_textout::line(format!("HTTP client error: {error}"));
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
                cli_textout::line(format!(
                    "HTTP check succeeded: {url} ({elapsed} ms, {} bytes)",
                    body.len()
                ));
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
            Err(error) => cli_textout::line(format!("HTTP body error: {error}")),
        },
        Err(error) => cli_textout::line(format!("HTTP check failed: {error}")),
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
    let rows = vec![
        vec!["server".into(), "receiving side of traffic".into()],
        vec!["enable".into(), "enable server traffic".into()],
        vec!["disable".into(), "disable server traffic".into()],
        vec![
            "runtime <seconds>".into(),
            "limit server runtime; zero is unlimited".into(),
        ],
        vec!["client".into(), "sending side of traffic".into()],
        vec!["enable".into(), "enable client traffic".into()],
        vec!["disable".into(), "disable client traffic".into()],
        vec!["remote <ip>".into(), "set client destination".into()],
        vec![
            "runtime <seconds>".into(),
            "limit client runtime; zero is unlimited".into(),
        ],
        vec![
            "http check <url>".into(),
            "load one HTTP or HTTPS page".into(),
        ],
        vec![
            "selftest".into(),
            "send UDP traffic to localhost and stop automatically".into(),
        ],
        vec![
            "configure tcp bytes <rate>".into(),
            "set TCP bytes per second".into(),
        ],
        vec![
            "configure tcp jitter <ms>".into(),
            "set TCP send jitter".into(),
        ],
        vec![
            "configure udp packetsize <bytes>".into(),
            "set UDP packet size".into(),
        ],
        vec![
            "configure udp jitter <ms>".into(),
            "set UDP send jitter".into(),
        ],
        vec![
            "metrics add sql <connection>".into(),
            "publish one-second metrics externally".into(),
        ],
        vec![
            "monitor IP <url>".into(),
            "set HTTP or HTTPS monitor target".into(),
        ],
        vec![
            "monitor start | stop".into(),
            "start or stop 30-second checks".into(),
        ],
        vec![
            "monitor history".into(),
            "show monitor events and alarms".into(),
        ],
        vec!["start | stop".into(), "start or stop a traffic run".into()],
        vec!["status".into(), "show current counters".into()],
        vec![
            "sql enable | disable".into(),
            "control local SQLite snapshots".into(),
        ],
        vec![
            "clean".into(),
            "delete data while preserving the run ID counter".into(),
        ],
        vec!["show run <id>".into(), "show stored run metrics".into()],
        vec!["list".into(), "list all run IDs and results".into()],
        vec!["help".into(), "show this help".into()],
        vec!["exit".into(), "stop workers and exit".into()],
    ];
    cli_textout::table(&rows, &[34, 64]);
}
