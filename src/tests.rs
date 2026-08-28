use crate::cli::Clients;
use crate::core::{
    Config, DEFAULT_REMOTE, Debrief, Metrics, PACKET_SIZE, PacketType, SqlState, StartGate,
    database_path, send_tcp_packets, spawn_client, spawn_server, timestamp,
};
use crate::metrics::ExternalSqlMetrics;
use rusqlite::{Connection, params};
use std::collections::hash_map::RandomState;
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

static TIMED_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Quasi-random u64 without pulling in an extra crate dependency; each `RandomState`
/// is seeded from OS randomness, so an empty hasher's finish() value varies per call.
fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

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
fn udp_sequence_detects_loss_and_out_of_order_packets() {
    let metrics = Metrics::new();
    let mut expected = None;
    metrics.test_udp_sequence(&mut expected, 0);
    metrics.test_udp_sequence(&mut expected, 2);
    metrics.test_udp_sequence(&mut expected, 1);
    assert_eq!(metrics.udp_status(), (1, 1));
}

#[test]
fn jitter_threshold_detects_problem_and_clean_traffic_stays_clean() {
    let clean = Metrics::new();
    clean.test_jitter(PacketType::Tcp, 999);
    clean.test_jitter(PacketType::Udp, 999);
    assert_eq!(clean.protocol_jitter_millis(), (999, 999));
    assert!(clean.tcp_jitter_millis() <= 1000);
    assert!(clean.udp_jitter_millis() <= 1000);

    let problem = Metrics::new();
    problem.test_jitter(PacketType::Tcp, 1001);
    problem.test_jitter(PacketType::Udp, 1001);
    assert!(problem.tcp_jitter_millis() > 1000);
    assert!(problem.udp_jitter_millis() > 1000);
}

#[test]
fn run_that_sends_zero_bytes_is_a_failure() {
    let metrics = Metrics::new();
    let outcome = crate::evaluate_run(&metrics, &Config::default(), Some(Duration::from_secs(1)));
    assert_eq!(outcome.result, "fail");
    assert_eq!(outcome.sent_bytes, 0);
    assert!(outcome.failure_reason.unwrap().contains("no bytes"));
}

#[test]
fn completion_does_not_overwrite_finished_result() {
    let sql = SqlState::new();
    sql.enable().unwrap();
    let id = sql.next_run_id(1);
    sql.start_run(id);
    sql.complete_run(id, "ok", 0, None);
    sql.complete_run(id, "aborted", 0, None);
    let result: String = Connection::open(database_path())
        .unwrap()
        .query_row(
            "SELECT result FROM runs WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result, "ok");
}

#[test]
fn timestamps_are_explicit_utc() {
    let value = timestamp();
    assert!(value.ends_with('Z'));
    assert!(chrono::DateTime::parse_from_rfc3339(&value).is_ok());
}

/// The same schema backs both the CLI-modified config and netmark.config,
/// so it must survive a YAML round trip unchanged.
#[test]
fn traffic_config_round_trips_through_yaml() {
    let mut file_config = crate::configuration::FileConfig::default();
    file_config.traffic.udp_rate = 42;
    file_config.traffic.packet_type = "udp".to_string();
    file_config.traffic.max_tcp_jitter_millis = 250;
    file_config.admin.emails.push("ops@example.com".to_string());
    let yaml = serde_yaml::to_string(&file_config).unwrap();
    let parsed: crate::configuration::FileConfig = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(parsed.traffic.udp_rate, 42);
    assert_eq!(parsed.traffic.packet_type, "udp");
    assert_eq!(parsed.traffic.max_tcp_jitter_millis, 250);
    assert_eq!(parsed.admin.emails, vec!["ops@example.com".to_string()]);
}

/// Writes random values into the configured external SQL metrics database and reads
/// them back; a missing row (no data) is treated as a failure of either the write or read.
#[test]
fn metrics_database_random_data_round_trip() {
    let path = std::env::temp_dir().join(format!(
        "netmark-metrics-round-trip-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let sink = ExternalSqlMetrics::connect(&format!("sqlite://{}", path.display())).unwrap();
    let run_id = random_u64() % 1_000_000 + 1;
    let values: [u64; 8] = std::array::from_fn(|_| random_u64() % 1_000_000 + 1);
    let lost = random_u64() % 1000 + 1;
    let out_of_order = random_u64() % 1000 + 1;
    let jitter = random_u64() % 1000 + 1;
    sink.write(&timestamp(), run_id, &values, lost, out_of_order, jitter)
        .unwrap();
    drop(sink);
    let row: (u64, u64, u64, u64, u64, u64, u64, u64) = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT run_id, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis FROM netmark_metrics WHERE run_id = ?1",
            params![run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .expect("no data found in netmark_metrics; write or read failed");
    assert_eq!(
        row,
        (
            run_id,
            values[1],
            values[3],
            values[5],
            values[7],
            lost,
            out_of_order,
            jitter
        )
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sqlite_metrics_can_be_read_back() {
    let path = std::env::temp_dir().join(format!(
        "netmark-external-metrics-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let sink = ExternalSqlMetrics::connect(&format!("sqlite://{}", path.display())).unwrap();
    sink.write(
        "2026-08-28T00:00:00.000Z",
        7,
        &[1, 1024, 2, 2048, 3, 3072, 4, 4096],
        5,
        6,
        7,
    )
    .unwrap();
    drop(sink);
    let connection = Connection::open(&path).unwrap();
    let row: (u64, u64, u64, u64, u64, u64, u64, u64) = connection.query_row("SELECT run_id, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis FROM netmark_metrics", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?))).unwrap();
    assert_eq!(row, (7, 1024, 2048, 3072, 4096, 5, 6, 7));
    let _ = std::fs::remove_file(path);
}

#[test]
fn three_second_udp_client_server_logs_match() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let started = Instant::now();
    let config = Arc::new(Mutex::new(Config {
        udp_rate: 10,
        packet_type: PacketType::Udp,
        tcp_bytes_per_second: 1024,
        udp_packet_size: 1024,
        client_runtime: 3,
        server_runtime: 3,
        jitter_millis: 0,
        client_jitter_millis: 0,
        server_jitter_millis: 0,
        max_tcp_jitter_millis: 1000,
        max_udp_jitter_millis: 1000,
        limit_bytes_per_second: 0,
        admin_emails: Vec::new(),
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
        0,
    );
    thread::sleep(Duration::from_millis(100));
    gate.start();
    thread::sleep(Duration::from_secs(3));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(250));
    let sent = metrics.run_totals();
    sql.start_run(run_id);
    let metrics_db_path = std::env::temp_dir().join(format!(
        "netmark-udp-test-metrics-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&metrics_db_path);
    let external = ExternalSqlMetrics::connect(&format!("sqlite://{}", metrics_db_path.display()))
        .unwrap();
    external
        .write(&timestamp(), run_id, &sent, 0, 0, 0)
        .unwrap();
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
    let database_bytes: u64 = Connection::open(&metrics_db_path)
        .unwrap()
        .query_row(
            "SELECT COALESCE(SUM(sent_udp_bytes), 0) FROM netmark_metrics WHERE run_id = ?1",
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
    assert!(started.elapsed() >= Duration::from_secs(3));
}

#[test]
fn three_second_tcp_loopback_logs_match() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let started = Instant::now();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let test_dir = std::env::temp_dir().join(format!("netmark-tcp-test-{}", std::process::id()));
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
            3,
            0,
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
    let sent_bytes = metrics.run_totals()[1];
    assert!(sent_bytes > 0, "client sent zero TCP bytes");
    assert_eq!(sent_bytes, client_bytes);
    assert_eq!(client_bytes, server_bytes);
    assert!(started.elapsed() >= Duration::from_secs(3));
}

/// Runs the sample auto-mode test profiles shipped in `profiles/` end to end:
/// a 10kB/s UDP test and a 10kB/s TCP test that each run for 3 seconds.
#[test]
fn sample_test_profiles_pass() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for profile in ["profiles/udp-10kbps.yaml", "profiles/tcp-10kbps-5s.yaml"] {
        let path = manifest_dir.join(profile);
        assert!(
            crate::run_auto_mode(&path),
            "profile {profile} did not pass"
        );
    }
}

/// Sends real UDP datagrams to a running server with timestamps that jump wildly
/// relative to their real arrival spacing (forcing jitter) and with sequence 1
/// arriving after sequence 2 (forcing an out-of-order detection).
#[test]fn udp_end_to_end_detects_jitter_and_out_of_order_packets() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let gate = Arc::new(StartGate::new());
    let config = Arc::new(Mutex::new(Config {
        packet_type: PacketType::Udp,
        server_runtime: 5,
        ..Config::default()
    }));
    let test_dir = std::env::temp_dir().join(format!(
        "netmark-udp-jitter-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    spawn_server(
        Arc::clone(&config),
        Arc::clone(&gate),
        Arc::clone(&stopping),
        Arc::clone(&metrics),
        test_dir,
    );
    thread::sleep(Duration::from_millis(100));
    gate.start();
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect(format!("127.0.0.1:{}", 9000)).unwrap();
    let send = |sequence: u64, send_timestamp_ms: i64| {
        let mut packet = [0u8; 32];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&send_timestamp_ms.to_be_bytes());
        socket.send(&packet).unwrap();
        thread::sleep(Duration::from_millis(20));
    };
    send(0, 0);
    send(2, 50);
    send(1, 5_000);
    thread::sleep(Duration::from_millis(200));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));
    let (_, out_of_order) = metrics.udp_status();
    assert!(
        out_of_order > 0,
        "expected an out-of-order packet to be detected"
    );
    assert!(
        metrics.udp_jitter_millis() > 0,
        "expected jitter to be detected"
    );
}

/// Deliberately sends UDP packets out of order (0, 2, 1). This is the intentional
/// failure case: the test only passes if the reordering is caught, so a nonzero
/// out-of-order count is the successful outcome, not a bug.
#[test]
fn udp_intentional_reordering_is_flagged_as_failure() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let gate = Arc::new(StartGate::new());
    let config = Arc::new(Mutex::new(Config {
        packet_type: PacketType::Udp,
        server_runtime: 5,
        ..Config::default()
    }));
    let test_dir = std::env::temp_dir().join(format!(
        "netmark-udp-reorder-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    spawn_server(
        Arc::clone(&config),
        Arc::clone(&gate),
        Arc::clone(&stopping),
        Arc::clone(&metrics),
        test_dir,
    );
    thread::sleep(Duration::from_millis(100));
    gate.start();
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect(format!("127.0.0.1:{}", 9000)).unwrap();
    let send = |sequence: u64| {
        let mut packet = [0u8; 32];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&(sequence as i64).to_be_bytes());
        socket.send(&packet).unwrap();
        thread::sleep(Duration::from_millis(20));
    };
    send(0);
    send(2);
    send(1); // intentionally out of order
    thread::sleep(Duration::from_millis(200));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));
    let (_, out_of_order) = metrics.udp_status();
    assert!(
        out_of_order > 0,
        "intentionally reordered packets should have been detected as out of order"
    );
}

/// Sends real TCP traffic at a known rate, then checks `evaluate_run`'s bandwidth
/// verdict against configured limits both below and above the achieved throughput.
#[test]
fn bandwidth_is_checked_against_configured_limit() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let test_dir = std::env::temp_dir().join(format!(
        "netmark-bandwidth-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    let bytes_per_second = 65_536u64;
    let runtime_secs = 2u64;
    let sender_stopping = Arc::clone(&stopping);
    let sender_metrics = Arc::clone(&metrics);
    let client_log_path = test_dir.join("client.log");
    let sender = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        let mut log = File::create(client_log_path).unwrap();
        send_tcp_packets(
            bytes_per_second,
            &sender_stopping,
            &sender_metrics,
            &mut log,
            runtime_secs,
            0,
            0,
            |packet| stream.write_all(packet),
        );
    });
    let (mut stream, _) = listener.accept().unwrap();
    let mut buffer = [0; PACKET_SIZE];
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(runtime_secs + 2) {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    sender.join().unwrap();
    stopping.store(true, Ordering::Relaxed);
    let elapsed = Some(Duration::from_secs(runtime_secs));

    let within_limit = Config {
        limit_bytes_per_second: bytes_per_second / 2,
        ..Config::default()
    };
    let outcome = crate::evaluate_run(&metrics, &within_limit, elapsed);
    assert_eq!(
        outcome.result, "ok",
        "throughput should be within limit: {:?}",
        outcome.failure_reason
    );

    let above_limit = Config {
        limit_bytes_per_second: bytes_per_second * 100,
        ..Config::default()
    };
    let outcome = crate::evaluate_run(&metrics, &above_limit, elapsed);
    assert_eq!(outcome.result, "fail");
    assert!(outcome.failure_reason.unwrap().contains("throughput"));
}

/// A profile naming a hook that nobody registered must fail loudly instead of
/// silently skipping the Rust code it asked for.
#[test]
fn unregistered_profile_hook_is_rejected() {
    let mut profile = crate::configuration::TestProfile::default();
    profile.hooks.before.push("missing".to_string());
    let error = crate::sdk::TestRunner::new(profile).run().unwrap_err();
    assert!(error.contains("missing"), "{error}");
}

/// Rust code attached to a profile by name must run, see the report, and be able
/// to fail a run that netmark itself considered fine.
#[test]
fn profile_hooks_run_and_can_fail_a_run() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-sdk-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    let mut profile = crate::configuration::TestProfile::default();
    profile.server.enabled = true;
    profile.clients[0].enabled = true;
    profile.traffic.packet_type = "udp".to_string();
    profile.traffic.udp_rate = 10;
    profile.traffic.client_runtime = 3;
    profile.traffic.server_runtime = 3;
    profile.duration_seconds = 3;
    profile.hooks.before.push("count_start".to_string());
    profile.hooks.after.push("reject".to_string());

    let started_calls = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&started_calls);
    let report = crate::sdk::TestRunner::new(profile)
        .log_dir(&log_dir)
        .hook("count_start", move |context| {
            assert_eq!(context.phase, crate::sdk::Phase::Before);
            flag.store(true, Ordering::Relaxed);
            Ok(())
        })
        .hook("reject", |context| {
            assert!(context.report.unwrap().sent_bytes() > 0);
            Err("rejected by test".to_string())
        })
        .run()
        .unwrap();

    assert!(started_calls.load(Ordering::Relaxed), "before hook did not run");
    assert!(!report.passed);
    assert!(
        report
            .failure_reason
            .as_deref()
            .unwrap_or_default()
            .contains("rejected by test"),
        "{:?}",
        report.failure_reason
    );
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// The SMTP check must reject an unconfigured server and accept one that answers
/// with a real SMTP greeting.
#[test]
fn smtp_check_detects_reachable_and_unreachable_servers() {
    assert!(crate::smtp::check("").is_err());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"220 test.example ESMTP\r\n").unwrap();
        let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
        assert!(line.starts_with("EHLO"));
        stream.write_all(b"250-test.example\r\n250 OK\r\n").unwrap();
    });
    let result = crate::smtp::check(&address.to_string()).unwrap();
    assert!(result.greeting.starts_with("220"));
    server.join().unwrap();

    // Nothing is listening once the accepted connection and listener are gone.
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = closed.local_addr().unwrap();
    drop(closed);
    assert!(crate::smtp::check(&dead.to_string()).is_err());
}

/// A debrief only matches when packets and bytes correlate exactly and nothing
/// was lost or reordered.
#[test]
fn debrief_reports_every_kind_of_mismatch() {
    let clean = Debrief {
        run_id: 1,
        role: "client",
        protocol: "udp".to_string(),
        sent_packets: 10,
        sent_bytes: 1024,
        received_packets: 10,
        received_bytes: 1024,
        lost_packets: 0,
        out_of_order_packets: 0,
    };
    assert!(clean.matched());
    assert!(clean.summary().contains("result=match"));

    for (broken, expected) in [
        (
            Debrief {
                received_packets: 9,
                ..clean.clone()
            },
            "packets sent but",
        ),
        (
            Debrief {
                received_bytes: 1000,
                ..clean.clone()
            },
            "bytes sent but",
        ),
        (
            Debrief {
                lost_packets: 1,
                ..clean.clone()
            },
            "packets lost",
        ),
        (
            Debrief {
                out_of_order_packets: 1,
                ..clean.clone()
            },
            "out of order",
        ),
    ] {
        assert!(!broken.matched());
        assert!(
            broken.mismatch_reason().unwrap().contains(expected),
            "{}",
            broken.summary()
        );
    }
}

/// A real run must end with client and server agreeing on packets and bytes, and
/// both sides must keep their own record in netmark.log and local SQLite.
#[test]
fn client_and_server_debrief_matches_and_is_persisted() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-debrief-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    let mut profile = crate::configuration::TestProfile::default();
    profile.server.enabled = true;
    profile.clients[0].enabled = true;
    profile.traffic.packet_type = "udp".to_string();
    profile.traffic.udp_rate = 20;
    profile.traffic.client_runtime = 3;
    profile.traffic.server_runtime = 3;
    profile.duration_seconds = 3;

    let report = crate::sdk::TestRunner::new(profile)
        .log_dir(&log_dir)
        .run()
        .unwrap();

    let debrief = report
        .debrief
        .as_ref()
        .expect("a run with a client must debrief");
    assert!(debrief.matched(), "{}", debrief.summary());
    assert!(debrief.sent_packets > 0);
    assert_eq!(debrief.sent_packets, debrief.received_packets);
    assert_eq!(debrief.sent_bytes, debrief.received_bytes);
    assert_eq!(debrief.sent_bytes, report.sent_bytes());
    assert!(report.passed, "{:?}", report.failure_reason);

    let log = std::fs::read_to_string(log_dir.join("netmark.log")).unwrap();
    for role in ["role=client", "role=server"] {
        assert!(
            log.lines()
                .any(|line| line.contains("debrief") && line.contains(role)),
            "netmark.log is missing the {role} debrief"
        );
    }

    let stored = SqlState::new().debriefs(report.run_id).unwrap();
    assert_eq!(stored.len(), 2, "expected a client and a server row: {stored:?}");
    assert!(stored.iter().all(|row| row.contains("result=match")), "{stored:?}");
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// Client ids start at 0, are handed out without gaps, and every setting is
/// addressed by id.
#[test]
fn clients_are_identified_by_id_starting_at_zero() {
    let clients = Clients::new(Vec::new());
    assert_eq!(clients.list().len(), 1);
    assert_eq!(clients.get(0).unwrap().id, 0);
    assert!(!clients.any_enabled());

    assert_eq!(clients.add(), 1);
    assert_eq!(clients.add(), 2);
    assert!(clients.update(1, |client| {
        client.enabled = true;
        client.remote = "10.0.0.2".to_string();
        client.runtime = Some(7);
    }));
    assert!(!clients.update(9, |client| client.enabled = true));

    assert!(clients.any_enabled());
    assert_eq!(clients.enabled().len(), 1);
    let one = clients.get(1).unwrap();
    assert_eq!(one.remote, "10.0.0.2");
    assert_eq!(one.runtime, Some(7));
    assert!(one.summary().starts_with("client 1 enabled remote=10.0.0.2"));
    assert_eq!(clients.get(0).unwrap().remote, "127.0.0.1");

    assert!(clients.remove(1));
    assert!(!clients.remove(1));
    // The freed id is reused, so ids stay dense from 0.
    assert_eq!(clients.add(), 1);
}

/// A per-client runtime override must win over the profile-wide traffic setting.
#[test]
fn client_overrides_beat_the_profile_wide_traffic_settings() {
    let mut traffic = crate::configuration::TrafficConfig::default();
    traffic.client_runtime = 30;
    traffic.client_jitter_millis = 5;
    let mut client = crate::configuration::ClientConfig::new(3);
    client.runtime = Some(2);
    let config = crate::client_config(&traffic, &client);
    assert_eq!(config.client_runtime, 2);
    assert_eq!(config.client_jitter_millis, 5);
}

/// Every row a host writes must say which side wrote it, so a database copied off
/// a client is never confused with one from a server.
#[test]
fn local_sqlite_records_the_role_that_wrote_each_row() {
    let sql = SqlState::new();
    sql.enable().unwrap();
    assert_eq!(sql.role(), "none");
    sql.set_role(crate::core::role_name(true, true));
    assert_eq!(sql.role(), "client+server");
    let id = sql.next_run_id(1);
    sql.start_run(id);
    sql.complete_run(id, "ok", 1, None);

    let row: String = Connection::open(database_path())
        .unwrap()
        .query_row("SELECT role FROM runs WHERE id = ?1", params![id], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(row, "client+server");
    assert!(sql.show_run(id).unwrap().unwrap().contains("role client+server"));
    assert!(
        sql.run_list()
            .unwrap()
            .iter()
            .any(|line| line.contains(&format!("run {id} role=client+server")))
    );
}

/// Two clients on one instance must both put traffic on the wire and both appear
/// in client.log under their own id.
#[test]
fn two_clients_both_send_and_are_logged_by_id() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-2client-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    let mut profile = crate::configuration::TestProfile::default();
    profile.server.enabled = true;
    profile.traffic.packet_type = "udp".to_string();
    profile.traffic.udp_rate = 20;
    profile.traffic.client_runtime = 3;
    profile.traffic.server_runtime = 3;
    profile.duration_seconds = 3;
    profile.clients = vec![
        crate::configuration::ClientConfig {
            enabled: true,
            ..crate::configuration::ClientConfig::new(0)
        },
        crate::configuration::ClientConfig {
            enabled: true,
            ..crate::configuration::ClientConfig::new(1)
        },
    ];

    let report = crate::sdk::TestRunner::new(profile)
        .log_dir(&log_dir)
        .run()
        .unwrap();

    let log = std::fs::read_to_string(log_dir.join("client.log")).unwrap();
    for id in [0, 1] {
        assert!(
            log.contains(&format!("client={id}")),
            "client.log has no traffic from client {id}"
        );
    }
    let debrief = report.debrief.as_ref().unwrap();
    assert!(debrief.sent_packets > 0);
    assert!(debrief.matched(), "{}", debrief.summary());
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// The REST API must serve its own contract, report the configured clients, and
/// run a profile end to end.
#[test]
fn rest_api_serves_the_documented_endpoints() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-rest-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    std::fs::create_dir_all(&log_dir).unwrap();

    let clients = Arc::new(Clients::new(Vec::new()));
    clients.update(0, |client| client.enabled = true);
    let api = Arc::new(crate::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    // Bind an ephemeral port, then hand that exact address to the API.
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap().to_string();
    drop(probe);
    api.enable(&address).unwrap();
    assert!(api.status().contains(&address));

    let base = format!("http://{address}");
    let http = reqwest::blocking::Client::new();

    let health: serde_json::Value = http
        .get(format!("{base}/api/v1/health"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(health["status"], "ok");

    let contract = http
        .get(format!("{base}/api/v1/openapi.yaml"))
        .send()
        .unwrap()
        .text()
        .unwrap();
    assert!(contract.starts_with("openapi:"));
    assert_eq!(contract, crate::restapi::OPENAPI);

    let listed: serde_json::Value = http
        .get(format!("{base}/api/v1/clients"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(listed["clients"][0]["id"], 0);
    assert_eq!(listed["clients"][0]["enabled"], true);

    assert_eq!(
        http.get(format!("{base}/api/v1/nope"))
            .send()
            .unwrap()
            .status(),
        404
    );

    let mut profile = crate::configuration::TestProfile::default();
    profile.server.enabled = true;
    profile.clients[0].enabled = true;
    profile.traffic.packet_type = "udp".to_string();
    profile.traffic.udp_rate = 20;
    profile.traffic.client_runtime = 3;
    profile.traffic.server_runtime = 3;
    profile.duration_seconds = 3;
    let report: serde_json::Value = http
        .post(format!("{base}/api/v1/runs"))
        .json(&profile)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(report["passed"], true, "{report}");
    assert_eq!(report["debrief"]["matched"], true, "{report}");

    let run_id = report["run_id"].as_u64().unwrap();
    let stored: serde_json::Value = http
        .get(format!("{base}/api/v1/runs/{run_id}"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(stored["summary"].as_str().unwrap().contains("role client"));
    assert_eq!(stored["debriefs"].as_array().unwrap().len(), 2);

    api.disable();
    assert_eq!(api.status(), "disabled");
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// A profile that names a hook cannot be run over HTTP, because HTTP callers have
/// no way to register Rust code.
#[test]
fn rest_api_rejects_a_profile_with_unregistered_hooks() {
    let log_dir = std::env::temp_dir().join(format!("netmark-rest-hook-{}", std::process::id()));
    let api = Arc::new(crate::restapi::RestApi::new(
        Arc::new(Clients::new(Vec::new())),
        log_dir,
    ));
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap().to_string();
    drop(probe);
    api.enable(&address).unwrap();

    let mut profile = crate::configuration::TestProfile::default();
    profile.hooks.before.push("nothing-registered".to_string());
    let response = reqwest::blocking::Client::new()
        .post(format!("http://{address}/api/v1/runs"))
        .json(&profile)
        .send()
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: serde_json::Value = response.json().unwrap();
    assert!(
        body["error"].as_str().unwrap().contains("nothing-registered"),
        "{body}"
    );
    api.disable();
}
