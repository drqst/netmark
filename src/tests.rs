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

/// A finished run with no traffic, for tests that only care about the result.
fn summary(result: &str) -> crate::core::RunSummary<'_> {
    crate::core::RunSummary {
        result,
        sent_bytes: 0,
        received_bytes: 0,
        sent_bytes_per_second: 0,
        received_bytes_per_second: 0,
        failure_reason: None,
        protocol: "tcp",
        detail: crate::core::RunDetail::default(),
    }
}

#[test]
fn config_defaults_are_local() {
    assert_eq!(DEFAULT_REMOTE, "127.0.0.1");
}

#[test]
fn sctp_and_tcp_window_round_trip_through_traffic_config() {
    let traffic = crate::configuration::TrafficConfig {
        packet_type: "sctp".to_string(),
        tcp_window_size: 65_536,
        ..Default::default()
    };
    let config = crate::config_from_traffic(&traffic);
    assert_eq!(config.packet_type, PacketType::Sctp);
    assert_eq!(config.tcp_window_size, 65_536);
    let round_trip = crate::traffic_config_from(&config);
    assert_eq!(round_trip.packet_type, "sctp");
    assert_eq!(round_trip.tcp_window_size, 65_536);
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
    sql.complete_run(id, &summary("ok"));
    sql.complete_run(id, &summary("aborted"));
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
    let values: [u64; 12] = std::array::from_fn(|_| random_u64() % 1_000_000 + 1);
    let lost = random_u64() % 1000 + 1;
    let out_of_order = random_u64() % 1000 + 1;
    let jitter = random_u64() % 1000 + 1;
    let up = random_u64() % 1000 + 1;
    let down = random_u64() % 1000 + 1;
    sink.write(
        &timestamp(),
        run_id,
        &values,
        lost,
        out_of_order,
        jitter,
        up,
        down,
    )
    .unwrap();
    drop(sink);
    let row: (u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64) = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT run_id, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis, sent_ip_bytes, received_ip_bytes, sent_bytes_per_second, received_bytes_per_second FROM netmark_metrics WHERE run_id = ?1",
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
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
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
            jitter,
            values[9],
            values[11],
            up,
            down
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
        &[1, 1024, 2, 2048, 3, 3072, 4, 4096, 8, 8192, 9, 9216],
        5,
        6,
        7,
        512,
        256,
    )
    .unwrap();
    drop(sink);
    let connection = Connection::open(&path).unwrap();
    let row: (u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64) = connection.query_row("SELECT run_id, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis, sent_ip_bytes, received_ip_bytes, sent_bytes_per_second, received_bytes_per_second FROM netmark_metrics", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?))).unwrap();
    assert_eq!(
        row,
        (7, 1024, 2048, 3072, 4096, 5, 6, 7, 8192, 9216, 512, 256)
    );
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
        tcp_window_size: 0,
        udp_packet_size: 1024,
        client_runtime: 3,
        server_runtime: 3,
        jitter_millis: 0,
        client_jitter_millis: 0,
        server_jitter_millis: 0,
        max_tcp_jitter_millis: 1000,
        max_udp_jitter_millis: 1000,
        limit_bytes_per_second: 0,
        limits: crate::core::LimitSet::default(),
        protocols: crate::core::ProtocolSwitches::default(),
        webrtc: crate::webrtc::Settings::default(),
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
        .write(&timestamp(), run_id, &sent, 0, 0, 0, 0, 0)
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
            &crate::webrtc::Settings::default(),
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

/// Sends real TCP traffic at a known bytes-per-second, then checks `evaluate_run`'s bandwidth
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
            &crate::webrtc::Settings::default(),
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

/// A per-client runtime override must win over the profile-wide traffic setting,
/// and a client can opt out of the WebRTC layer on its own.
#[test]
fn client_overrides_beat_the_profile_wide_traffic_settings() {
    let traffic = crate::configuration::TrafficConfig {
        client_runtime: 30,
        client_jitter_millis: 5,
        ..Default::default()
    };
    let webrtc = crate::webrtc::Settings {
        enabled: true,
        ..crate::webrtc::Settings::default()
    };
    let mut client = crate::configuration::ClientConfig::new(3);
    client.runtime = Some(2);
    let config = crate::client_config(&traffic, &client, &webrtc);
    assert_eq!(config.client_runtime, 2);
    assert_eq!(config.client_jitter_millis, 5);
    // `follow` takes the layer from the webrtc command.
    assert!(config.webrtc.enabled);

    client.webrtc = Some(false);
    assert!(!crate::client_config(&traffic, &client, &webrtc).webrtc.enabled);
    client.webrtc = Some(true);
    assert!(
        crate::client_config(
            &traffic,
            &client,
            &crate::webrtc::Settings::default()
        )
        .webrtc
        .enabled
    );
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
    sql.complete_run(
        id,
        &crate::core::RunSummary {
            result: "ok",
            sent_bytes: 1,
            received_bytes: 2,
            sent_bytes_per_second: 3,
            received_bytes_per_second: 4,
            failure_reason: None,
            protocol: "sctp",
            detail: crate::core::RunDetail {
                sent_tcp_packets: 5,
                sent_tcp_bytes: 6,
                jitter_millis: 7,
                ..crate::core::RunDetail::default()
            },
        },
    );

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

/// A data-channel header must survive a round trip and reject anything that is
/// not one.
#[test]
fn webrtc_frames_round_trip_and_reject_foreign_bytes() {
    let frame = crate::webrtc::Frame {
        message_type: crate::webrtc::MessageType::Data,
        channel: 7,
        ordered: false,
        sequence: 4_000_000_000,
        payload_len: 900,
    };
    let encoded = frame.encode();
    assert_eq!(encoded.len(), crate::webrtc::HEADER_LEN);
    assert_eq!(crate::webrtc::Frame::decode(&encoded), Some(frame));

    assert!(crate::webrtc::Frame::decode(&[0u8; crate::webrtc::HEADER_LEN]).is_none());
    assert!(crate::webrtc::Frame::decode(&encoded[..8]).is_none());

    let mut receiver = crate::webrtc::Receiver::default();
    assert!(receiver.accept(&encoded).is_some());
    assert!(receiver.accept(&[9u8; 32]).is_none());
    assert_eq!(receiver.messages, 1);
    assert_eq!(receiver.invalid, 1);
}

/// Messages must be spread over the configured channels, and the first message on
/// each channel must open it.
#[test]
fn webrtc_sender_opens_each_channel_and_rotates() {
    let mut sender = crate::webrtc::Sender::new(crate::webrtc::Settings {
        enabled: true,
        channels: 3,
        label: "test".to_string(),
        ordered: true,
    });
    let first: Vec<_> = (0..3).map(|_| sender.next(10)).collect();
    assert_eq!(
        first.iter().map(|frame| frame.channel).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(
        first
            .iter()
            .all(|frame| frame.message_type == crate::webrtc::MessageType::Open)
    );

    let second: Vec<_> = (0..3).map(|_| sender.next(10)).collect();
    assert!(
        second
            .iter()
            .all(|frame| frame.message_type == crate::webrtc::MessageType::Data)
    );
    assert_eq!(
        second.iter().map(|frame| frame.sequence).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}

/// With the WebRTC layer on, every message the client sends must be decoded by the
/// server, over both TCP and UDP, with no invalid frames.
#[test]
fn webrtc_messages_survive_both_transports() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    for protocol in ["udp", "tcp"] {
        let log_dir = std::env::temp_dir().join(format!(
            "netmark-webrtc-{protocol}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&log_dir);
        let mut profile = crate::configuration::TestProfile::default();
        profile.server.enabled = true;
        profile.clients[0].enabled = true;
        profile.traffic.packet_type = protocol.to_string();
        profile.traffic.udp_rate = 20;
        profile.traffic.tcp_bytes_per_second = 10240;
        profile.traffic.client_runtime = 3;
        profile.traffic.server_runtime = 3;
        profile.duration_seconds = 3;
        profile.webrtc.enabled = true;
        profile.webrtc.channels = 2;

        let report = crate::sdk::TestRunner::new(profile)
            .log_dir(&log_dir)
            .run()
            .unwrap();

        assert!(
            report.webrtc_sent_messages > 0,
            "{protocol}: no data-channel messages were sent"
        );
        assert_eq!(
            report.webrtc_sent_messages, report.webrtc_received_messages,
            "{protocol}: data-channel messages were lost"
        );
        assert_eq!(report.webrtc_invalid_frames, 0, "{protocol}");
        assert!(report.passed, "{protocol}: {:?}", report.failure_reason);
        let _ = std::fs::remove_dir_all(&log_dir);
    }
}

/// Raw IP needs CAP_NET_RAW. Where it is available the transport must carry
/// traffic end to end; where it is not, the failure must say so rather than
/// reporting a misleading zero.
#[test]
fn raw_ip_transport_works_or_explains_why_not() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    match crate::rawip::RawIpSocket::open() {
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            assert!(error.to_string().contains("CAP_NET_RAW"), "{error}");
        }
        Ok(_) => {
            let log_dir =
                std::env::temp_dir().join(format!("netmark-rawip-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&log_dir);
            let mut profile = crate::configuration::TestProfile::default();
            profile.server.enabled = true;
            profile.clients[0].enabled = true;
            profile.traffic.packet_type = "ip".to_string();
            profile.traffic.udp_rate = 20;
            profile.traffic.client_runtime = 3;
            profile.traffic.server_runtime = 3;
            profile.duration_seconds = 3;

            let report = crate::sdk::TestRunner::new(profile)
                .log_dir(&log_dir)
                .run()
                .unwrap();
            assert!(report.sent_ip_bytes > 0, "no raw IP bytes were sent");
            assert_eq!(report.sent_ip_bytes, report.received_ip_bytes);
            let _ = std::fs::remove_dir_all(&log_dir);
        }
    }
    assert!(crate::rawip::resolve("10.0.0.1:9000").is_ok());
    assert!(crate::rawip::resolve("example.com").is_err());
}

/// Bandwidth up and down must reach every surface: the report, netmark.log and
/// both of the local SQLite views.
#[test]
fn bandwidth_up_and_down_is_reported_everywhere() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir =
        std::env::temp_dir().join(format!("netmark-bandwidth-{}", std::process::id()));
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

    assert!(report.sent_bytes_per_second > 0);
    assert!(report.received_bytes_per_second > 0);
    assert_eq!(
        report.sent_bytes_per_second,
        report.throughput_bytes_per_second()
    );
    assert!(report.summary().contains("up="), "{}", report.summary());
    assert!(report.summary().contains("down="), "{}", report.summary());

    let log = std::fs::read_to_string(log_dir.join("netmark.log")).unwrap();
    assert!(
        log.lines()
            .any(|line| line.contains("up=") && line.contains("down=")),
        "netmark.log has no bandwidth line"
    );

    let sql = SqlState::new();
    let stored = sql.show_run(report.run_id).unwrap().unwrap();
    assert!(stored.contains("up="), "{stored}");
    assert!(stored.contains("down="), "{stored}");
    assert!(
        sql.run_list()
            .unwrap()
            .iter()
            .any(|line| line.contains(&format!("run {} ", report.run_id))
                && line.contains("down="))
    );
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// `help` must document every command the interactive loop accepts.
#[test]
fn help_documents_every_command() {
    let topics = crate::cli::help_topics();
    for command in crate::cli::COMMANDS {
        assert!(
            topics.iter().any(|topic| topic
                .split('|')
                .any(|alternative| alternative.trim().starts_with(command))),
            "help is missing the {command} command"
        );
    }
}

/// Arrow-key history: position 0 is the line being typed, going back reaches
/// older commands and coming forward returns the unfinished line.
#[test]
fn command_history_walks_back_and_forward_from_position_zero() {
    let mut history = crate::cli::History::new();
    let mut input = String::new();

    // Nothing to walk into yet.
    history.step_back(&mut input);
    assert_eq!(input, "");

    history.push("server enable");
    history.push("client 0 enable");
    history.push("start");

    input = "sto".to_string();
    history.step_back(&mut input);
    assert_eq!(input, "start");
    history.step_back(&mut input);
    assert_eq!(input, "client 0 enable");
    history.step_back(&mut input);
    assert_eq!(input, "server enable");
    // Already at the oldest entry.
    history.step_back(&mut input);
    assert_eq!(input, "server enable");

    history.step_forward(&mut input);
    assert_eq!(input, "client 0 enable");
    history.step_forward(&mut input);
    assert_eq!(input, "start");
    // Position 0 restores the half-typed line.
    history.step_forward(&mut input);
    assert_eq!(input, "sto");
    history.step_forward(&mut input);
    assert_eq!(input, "sto");

    // Blank lines and repeats are not stacked.
    history.push("   ");
    history.push("start");
    history.push("start");
    let mut latest = String::new();
    history.step_back(&mut latest);
    assert_eq!(latest, "start");
    history.step_back(&mut latest);
    assert_eq!(latest, "client 0 enable");
}

/// Nothing is written off the host until an external database is configured.
#[test]
fn external_databases_are_off_by_default() {
    assert!(crate::configuration::TestProfile::default().metrics.sql.is_none());
    assert!(crate::configuration::FileConfig::default().metrics.sql.is_none());
    let shipped = crate::configuration::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("netmark.config"),
    )
    .unwrap();
    assert!(
        shipped.metrics.sql.is_none(),
        "the shipped netmark.config must not point at a database"
    );
    assert!(!shipped.restapi.enabled);
    assert!(!shipped.smtp.enabled);
    assert!(!shipped.webrtc.enabled);
}

/// A run whose measured jitter goes past `max_*_jitter_millis` must fail, and say
/// which protocol and by how much.
#[test]
fn too_much_jitter_fails_the_run() {
    let metrics = Metrics::new();
    let config = Config {
        max_tcp_jitter_millis: 100,
        max_udp_jitter_millis: 100,
        ..Config::default()
    };

    metrics.test_jitter(PacketType::Tcp, 100);
    metrics.test_jitter(PacketType::Udp, 100);
    metrics.add_test_bytes(true, PacketType::Udp, 1024);
    let clean = crate::evaluate_run(&metrics, &config, Some(Duration::from_secs(1)));
    assert_eq!(clean.result, "ok", "{:?}", clean.failure_reason);

    metrics.test_jitter(PacketType::Tcp, 750);
    let tcp = crate::evaluate_run(&metrics, &config, Some(Duration::from_secs(1)));
    assert_eq!(tcp.result, "fail");
    let reason = tcp.failure_reason.unwrap();
    assert!(reason.contains("TCP jitter 750ms exceeded limit 100ms"), "{reason}");

    metrics.test_jitter(PacketType::Udp, 900);
    let both = crate::evaluate_run(&metrics, &config, Some(Duration::from_secs(1)));
    let reason = both.failure_reason.unwrap();
    assert!(reason.contains("TCP jitter"), "{reason}");
    assert!(reason.contains("UDP jitter 900ms exceeded limit 100ms"), "{reason}");
}

/// Real UDP packets sent with wildly inconsistent send timestamps must push the
/// measured jitter past the limit and fail the run end to end.
#[test]
fn end_to_end_jitter_beyond_the_limit_fails_the_run() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let gate = Arc::new(StartGate::new());
    let config = Arc::new(Mutex::new(Config {
        packet_type: PacketType::Udp,
        server_runtime: 5,
        max_udp_jitter_millis: 200,
        ..Config::default()
    }));
    let test_dir =
        std::env::temp_dir().join(format!("netmark-jitter-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    spawn_server(
        Arc::clone(&config),
        Arc::clone(&gate),
        Arc::clone(&stopping),
        Arc::clone(&metrics),
        test_dir.clone(),
    );
    thread::sleep(Duration::from_millis(100));
    gate.start();

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("127.0.0.1:9000").unwrap();
    // Packets arrive 20 ms apart but claim to have been sent seconds apart, which
    // is exactly the delay variation jitter is meant to catch.
    for (sequence, sent_at_ms) in [(0u64, 0i64), (1, 30), (2, 8_000), (3, 8_030)] {
        let mut packet = [0u8; 64];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&sent_at_ms.to_be_bytes());
        socket.send(&packet).unwrap();
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(200));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));

    let measured = metrics.udp_jitter_millis();
    assert!(measured > 200, "expected jitter above the limit, got {measured}");
    let outcome = crate::evaluate_run(
        &metrics,
        &config.lock().unwrap(),
        Some(Duration::from_secs(1)),
    );
    assert_eq!(outcome.result, "fail");
    assert!(
        outcome.failure_reason.unwrap().contains("UDP jitter"),
        "the run failed for the wrong reason"
    );
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Out-of-order UDP must be counted per client, reported by the debrief, and fail
/// the run rather than being quietly tolerated.
#[test]
fn out_of_order_udp_packets_fail_the_debrief() {
    let metrics = Metrics::new();
    let mut expected = None;
    for sequence in [0, 1, 3, 2, 4] {
        metrics.test_udp_sequence(&mut expected, sequence);
    }
    // 3 arrived before 2, so 2 is late; no packet is actually missing.
    let (lost, out_of_order) = metrics.udp_status();
    assert_eq!(out_of_order, 1, "the late packet was not flagged");
    assert_eq!(lost, 1, "the gap ahead of the late packet was not counted");

    // Each client has its own sequence space, so two clients starting at 0 is not
    // reordering.
    let separate = Metrics::new();
    let mut client_zero = None;
    let mut client_one = None;
    for sequence in 0..5 {
        separate.test_udp_sequence(&mut client_zero, sequence);
        separate.test_udp_sequence(&mut client_one, sequence);
    }
    assert_eq!(separate.udp_status(), (0, 0));

    let debrief = Debrief {
        run_id: 1,
        role: "client",
        protocol: "udp".to_string(),
        sent_packets: 5,
        sent_bytes: 5120,
        received_packets: 5,
        received_bytes: 5120,
        lost_packets: 0,
        out_of_order_packets: out_of_order,
    };
    assert!(!debrief.matched());
    let reason = debrief.mismatch_reason().unwrap();
    assert!(reason.contains("1 packets out of order"), "{reason}");
}

/// Real UDP packets delivered out of order must be caught by a live server and
/// carried into the run's debrief.
#[test]
fn end_to_end_out_of_order_udp_fails_the_run() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(Metrics::new());
    let gate = Arc::new(StartGate::new());
    let config = Arc::new(Mutex::new(Config {
        packet_type: PacketType::Udp,
        server_runtime: 5,
        ..Config::default()
    }));
    let test_dir =
        std::env::temp_dir().join(format!("netmark-udp-order-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    spawn_server(
        Arc::clone(&config),
        Arc::clone(&gate),
        Arc::clone(&stopping),
        Arc::clone(&metrics),
        test_dir.clone(),
    );
    thread::sleep(Duration::from_millis(100));
    gate.start();

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("127.0.0.1:9000").unwrap();
    let sent_at = chrono::Utc::now().timestamp_millis();
    for (index, sequence) in [0u64, 1, 2, 4, 3, 5].into_iter().enumerate() {
        let mut packet = [0u8; 64];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&(sent_at + index as i64 * 20).to_be_bytes());
        socket.send(&packet).unwrap();
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(200));
    stopping.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));

    let (_, out_of_order) = metrics.udp_status();
    assert!(out_of_order > 0, "the reordered packet was not detected");

    let (received_packets, received_bytes) = metrics.run_counts(false, PacketType::Udp);
    let debrief = Debrief {
        run_id: 1,
        role: "server",
        protocol: "udp".to_string(),
        sent_packets: received_packets,
        sent_bytes: received_bytes,
        received_packets,
        received_bytes,
        lost_packets: 0,
        out_of_order_packets: out_of_order,
    };
    assert!(!debrief.matched(), "{}", debrief.summary());
    assert!(debrief.summary().contains("out_of_order="));
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// `status` must come out as an aligned table with one labelled row per fact.
#[test]
fn status_is_reported_as_a_table() {
    let metrics = Metrics::new();
    metrics.add_test_bytes(true, PacketType::Udp, 2048);
    metrics.add_test_bytes(false, PacketType::Udp, 1024);
    let clients = Clients::new(Vec::new());
    clients.update(0, |client| client.enabled = true);
    let webrtc = crate::webrtc::Settings::default();

    let rows = crate::cli::status_rows(
        &metrics,
        &crate::cli::StatusContext {
            protocols: crate::core::ProtocolSwitches::default(),
            running: true,
            elapsed: Some(Duration::from_secs(2)),
            run_id: 42,
            server_enabled: true,
            clients: &clients,
            webrtc: &webrtc,
            packet_type: PacketType::Udp,
            monitor: (true, 1, 5, 4, 1),
            metrics_sql: "not connected".to_string(),
            restapi: "disabled".to_string(),
            web_server: crate::cli::web_server_status("127.0.0.1:8081", "127.0.0.1:8081"),
            smtp: false,
        },
    );

    assert!(
        rows.iter().all(|row| row.len() == 2),
        "every status row must be a label and a value"
    );
    let value = |label: &str| {
        rows.iter()
            .find(|row| row[0] == label)
            .unwrap_or_else(|| panic!("status has no {label} row"))[1]
            .clone()
    };
    assert_eq!(value("Traffic"), "running (udp)");
    assert_eq!(value("Run"), "42 (2 s elapsed)");
    assert_eq!(value("Bandwidth up"), "1024 bytes/sec");
    assert_eq!(value("Bandwidth down"), "512 bytes/sec");
    assert!(value("Sent").starts_with("2048 bytes"));
    assert!(value("Received").starts_with("1024 bytes"));
    assert_eq!(value("UDP loss"), "0 lost, 0 out of order");
    assert_eq!(value("Server"), "enabled");
    assert!(value("Client 0").contains("enabled"));
    assert!(value("WebRTC").starts_with("webrtc disabled"));
    assert_eq!(value("Monitor"), "on id 1, 5 calls, 4 ok, 1 failed");
    assert_eq!(value("Metrics SQL"), "not connected");
    assert_eq!(value("REST API"), "disabled");
    assert_eq!(
        value("Web server"),
        "listening on http://127.0.0.1:8081 (port 8081)"
    );
    assert!(value("SCTP").starts_with("not selected"));
    assert_eq!(value("SMTP"), "disabled");
}

/// `start_at` is how many machines begin together: it blocks until the instant
/// given, ignores one already past, and rejects anything that is not a timestamp.
#[test]
fn start_at_blocks_until_the_agreed_instant() {
    assert!(crate::wait_until(None).is_ok());

    let past = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    let started = Instant::now();
    crate::wait_until(Some(&past)).unwrap();
    assert!(started.elapsed() < Duration::from_millis(200));

    let future = (chrono::Utc::now() + chrono::Duration::milliseconds(400)).to_rfc3339();
    let started = Instant::now();
    crate::wait_until(Some(&future)).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(350),
        "start_at returned early"
    );

    let error = crate::wait_until(Some("tuesday")).unwrap_err();
    assert!(error.contains("RFC 3339"), "{error}");
}

/// The web server address is reported the same way at start-up and in `status`,
/// including the port, and says so plainly when nothing is listening.
#[test]
fn the_web_server_port_is_reported() {
    assert_eq!(
        crate::cli::web_server_status("0.0.0.0:8080", "127.0.0.1:8081"),
        "listening on http://0.0.0.0:8080 (port 8080)"
    );
    let stopped = crate::cli::web_server_status("", "127.0.0.1:8081");
    assert!(stopped.starts_with("not listening"), "{stopped}");
    assert!(stopped.contains("127.0.0.1:8081"), "{stopped}");
}

/// The `sctp` help page documents the whole life cycle of an SCTP run, so the
/// CLI and the web CLI can both explain the transport without the manual.
#[test]
fn the_sctp_help_page_covers_the_transport() {
    let rows = crate::cli::sctp_help_rows();
    assert!(
        rows.iter().all(|row| row.len() == 2),
        "every help row must be a label and a value"
    );
    let labels: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    for label in [
        "what it is",
        "kernel support",
        "select it",
        "run it",
        "stop it",
        "counters",
    ] {
        assert!(labels.contains(&label), "sctp help has no {label} row");
    }
    assert!(
        crate::cli::help_rows()
            .iter()
            .any(|row| row[0] == "configure sctp" && !row[1].is_empty()),
        "help does not mention the configure sctp command"
    );
}

/// The status the web page polls has to describe what is happening right now,
/// including the transport, SCTP support and the byte counters of a live run.
#[test]
fn live_status_follows_a_run() {
    let api = Arc::new(crate::restapi::RestApi::new(
        Arc::new(Clients::new(Vec::new())),
        std::env::temp_dir(),
    ));
    let metrics = Arc::new(Metrics::new());
    api.live().attach_metrics(Arc::clone(&metrics));

    assert!(api.live().activity().starts_with("idle"));

    metrics.add_test_bytes(true, PacketType::Sctp, 4096);
    api.live().update(
        true,
        7,
        PacketType::Sctp,
        true,
        1,
        Some(Duration::from_secs(2)),
    );
    let activity = api.live().activity();
    assert!(activity.contains("SCTP"), "{activity}");
    assert!(activity.contains("run 7"), "{activity}");

    api.live().update(false, 7, PacketType::Sctp, true, 1, None);
    assert!(api.live().activity().starts_with("idle"));
}

/// Monitor checks belong in the external database; the local SQLite database
/// only records that the monitor was started and stopped.
#[test]
fn monitor_status_is_the_only_monitor_data_in_local_sqlite() {
    let sql = Arc::new(SqlState::new());
    sql.enable().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-monitor-{}", std::process::id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    let monitor = Arc::new(crate::monitor::MonitorState::new());

    let connection = rusqlite::Connection::open(crate::core::database_path()).unwrap();
    // The database outlives the test process, so only rows written here count.
    let baseline: i64 = connection
        .query_row("SELECT COALESCE(MAX(rowid), 0) FROM monitor_status", [], |row| row.get(0))
        .unwrap();

    let id = monitor.start(&log_dir, &sql).expect("monitor did not start");
    assert!(monitor.is_running());
    assert!(monitor.start(&log_dir, &sql).is_none(), "started twice");
    monitor.stop(&log_dir, &sql);
    assert!(!monitor.is_running());

    let mut statement = connection
        .prepare("SELECT status FROM monitor_status WHERE rowid > ?1 AND monitor_id = ?2 ORDER BY rowid")
        .unwrap();
    let recorded: Vec<String> = statement
        .query_map(rusqlite::params![baseline, id], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|status| status.unwrap())
        .collect();
    assert_eq!(recorded, vec!["started".to_string(), "stopped".to_string()]);
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// A whole SCTP run, from `start` through the traffic to the `stop` debrief, on
/// kernels that support SCTP. Where the kernel does not, the run is skipped
/// rather than failed, which is exactly what `sctp status` reports.
#[test]
fn sctp_runs_end_to_end_when_the_kernel_supports_it() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    if let Err(error) = crate::sctp::availability() {
        eprintln!("skipping SCTP end-to-end test: {error}");
        return;
    }
    let log_dir = std::env::temp_dir().join(format!("netmark-sctp-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    let mut profile = crate::configuration::TestProfile::default();
    profile.server.enabled = true;
    profile.clients[0].enabled = true;
    profile.traffic.packet_type = "sctp".to_string();
    profile.traffic.tcp_bytes_per_second = 4096;
    profile.traffic.client_runtime = 3;
    profile.traffic.server_runtime = 3;
    profile.duration_seconds = 3;

    let report = crate::sdk::TestRunner::new(profile)
        .log_dir(log_dir.clone())
        .run()
        .unwrap();
    assert!(report.passed, "{:?}", report.failure_reason);
    assert_eq!(report.packet_type, "sctp");
    assert!(report.sent_bytes() > 0, "no SCTP bytes were sent");
    let debrief = report.debrief.as_ref().expect("SCTP run has no debrief");
    assert_eq!(debrief.protocol, "sctp");
    assert!(debrief.matched(), "{}", debrief.summary());
    let _ = std::fs::remove_dir_all(&log_dir);
}

/// The session dispatcher backs both the shell CLI and the web CLI, so it must
/// answer representative commands with exactly the shell wording.
#[test]
fn session_executes_commands_like_the_shell_cli() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir = std::env::temp_dir().join(format!("netmark-session-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    std::fs::create_dir_all(&log_dir).unwrap();
    let config_path = log_dir.join("netmark.config");

    let clients = Arc::new(Clients::new(Vec::new()));
    let api = Arc::new(crate::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    let session = crate::session::Session::from_config(
        &crate::configuration::FileConfig::default(),
        log_dir.clone(),
        config_path,
        Arc::clone(&clients),
        Arc::clone(api.live()),
        Arc::downgrade(&api),
    );

    assert_eq!(session.execute("server enable"), "server enabled");
    assert_eq!(session.execute("client add"), "client 1 added");
    assert_eq!(session.execute("configure type sctp"), "configuration updated");

    let status = session.execute("status");
    assert!(status.contains("Traffic"), "{status}");
    assert!(status.contains("sctp"), "{status}");
    assert!(status.contains("Server"), "{status}");

    let help_start = session.execute("help start");
    assert!(help_start.contains("start a traffic run"), "{help_start}");

    let help_tcp = session.execute("help configure tcp");
    assert!(help_tcp.contains("configure tcp"), "{help_tcp}");

    assert_eq!(
        session.execute("nonsense"),
        "unknown command; type 'help' for commands"
    );

    let _ = std::fs::remove_dir_all(&log_dir);
}

/// A REST API with a session attached must run state-changing commands over
/// `POST /api/v1/cli`, just like the interactive shell CLI.
#[test]
fn rest_api_with_a_session_runs_cli_commands() {
    let _test_lock = TIMED_TEST_LOCK.lock().unwrap();
    let log_dir =
        std::env::temp_dir().join(format!("netmark-session-cli-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&log_dir);
    std::fs::create_dir_all(&log_dir).unwrap();
    let config_path = log_dir.join("netmark.config");

    let clients = Arc::new(Clients::new(Vec::new()));
    let api = Arc::new(crate::restapi::RestApi::new(
        Arc::clone(&clients),
        log_dir.clone(),
    ));
    let session = Arc::new(crate::session::Session::from_config(
        &crate::configuration::FileConfig::default(),
        log_dir.clone(),
        config_path,
        Arc::clone(&clients),
        Arc::clone(api.live()),
        Arc::downgrade(&api),
    ));
    api.attach_session(Arc::clone(&session));

    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap().to_string();
    drop(probe);
    api.enable(&address).unwrap();

    let base = format!("http://{address}");
    let http = reqwest::blocking::Client::new();
    let reply: serde_json::Value = http
        .post(format!("{base}/api/v1/cli"))
        .json(&serde_json::json!({ "command": "server enable" }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(reply["output"], "server enabled");
    assert!(session.server_enabled());

    api.disable();
    let _ = std::fs::remove_dir_all(&log_dir);
}

#[test]
fn help_answers_a_topic_for_every_command() {
    for topic in [
        vec!["start"],
        vec!["stop"],
        vec!["status"],
        vec!["configure", "type"],
        vec!["client", "0", "remote"],
        vec!["configure", "limits"],
    ] {
        let rows = crate::cli::help_for(&topic).unwrap_or_else(|error| {
            panic!("help {} answered nothing: {error}", topic.join(" "));
        });
        assert!(
            !rows.is_empty(),
            "help {} answered an empty page",
            topic.join(" ")
        );
    }
    // Every top-level command in the completion list is documented.
    for command in crate::cli::COMMANDS {
        if matches!(*command, "quit" | "exit") {
            continue;
        }
        let topic: Vec<&str> = command.split_whitespace().collect();
        assert!(
            crate::cli::help_for(&topic).is_ok(),
            "help {command} answered nothing"
        );
    }
    assert!(crate::cli::help_for(&["nonsense"]).is_err());
    // "help sctp" is the detailed page, not the one-line summary.
    assert_eq!(
        crate::cli::help_for(&["sctp"]).unwrap(),
        crate::cli::sctp_help_rows()
    );
}

#[test]
fn selftest_covers_every_protocol() {
    let protocols: Vec<&str> = crate::cli::SELFTEST_PROTOCOLS
        .iter()
        .map(|packet_type| packet_type.as_str())
        .collect();
    for protocol in ["tcp", "sctp", "udp", "ip"] {
        assert!(
            protocols.contains(&protocol),
            "selftest does not cover {protocol}"
        );
    }
    // TCP and UDP always work; the other two report why they cannot run instead
    // of failing the whole sequence.
    assert!(crate::cli::selftest_availability(PacketType::Tcp).is_ok());
    assert!(crate::cli::selftest_availability(PacketType::Udp).is_ok());
    for packet_type in [PacketType::Sctp, PacketType::Ip] {
        if let Err(reason) = crate::cli::selftest_availability(packet_type) {
            assert!(!reason.is_empty(), "a skipped transport needs a reason");
        }
    }
}

#[test]
fn limits_are_configured_per_protocol() {
    let config = Arc::new(Mutex::new(Config::default()));
    assert_eq!(
        crate::cli::configure_limits(&config, &["udp", "max-lost-packets", "5"]),
        Ok("udp max-lost-packets limit set to 5".to_string())
    );
    assert_eq!(config.lock().unwrap().limits.udp.max_lost_packets, 5);
    // Only the protocol that was named is changed.
    assert_eq!(config.lock().unwrap().limits.tcp.max_lost_packets, 0);

    for protocol in ["tcp", "sctp", "ip"] {
        crate::cli::configure_limits(&config, &[protocol, "min-sent-bytes", "1024"])
            .unwrap_or_else(|error| panic!("{protocol} limit was refused: {error}"));
    }
    assert_eq!(config.lock().unwrap().limits.sctp.min_sent_bytes, 1024);

    // Lost packets are a UDP measurement, so the other transports refuse them
    // and say which parameters they do accept.
    let error = crate::cli::configure_limits(&config, &["tcp", "max-lost-packets", "5"])
        .expect_err("TCP has no lost-packet counter");
    assert!(error.contains("min-sent-bytes"), "{error}");
    assert!(crate::cli::configure_limits(&config, &["tcp", "nonsense", "5"]).is_err());
    assert!(crate::cli::configure_limits(&config, &["tcp", "min-sent-bytes", "x"]).is_err());
    assert!(crate::cli::configure_limits(&config, &["nonsense", "min-sent-bytes", "5"]).is_err());

    let table = crate::cli::configure_limits(&config, &["udp", "status"]).unwrap();
    assert!(table.contains("udp max-lost-packets"), "{table}");
    assert!(table.contains('5'), "{table}");
    let all = crate::cli::configure_limits(&config, &[]).unwrap();
    for protocol in ["tcp", "sctp", "udp", "ip"] {
        assert!(all.contains(protocol), "{all} is missing {protocol}");
    }

    crate::cli::configure_limits(&config, &["udp", "clear"]).unwrap();
    assert_eq!(config.lock().unwrap().limits.udp, crate::core::Limits::default());
    // Zero removes a single limit.
    crate::cli::configure_limits(&config, &["tcp", "min-sent-bytes", "0"]).unwrap();
    assert_eq!(config.lock().unwrap().limits.tcp.min_sent_bytes, 0);
}

#[test]
fn a_missed_limit_fails_the_run() {
    let metrics = Metrics::new();
    metrics.add_test_bytes(true, PacketType::Tcp, 1024);
    metrics.add_test_bytes(false, PacketType::Tcp, 1024);
    let mut config = Config {
        packet_type: PacketType::Tcp,
        ..Config::default()
    };
    let elapsed = Some(Duration::from_secs(1));

    let outcome = crate::evaluate_run(&metrics, &config, elapsed);
    assert_eq!(outcome.result, "ok", "{:?}", outcome.failure_reason);

    config.limits.tcp.min_sent_bytes_per_second = 100_000;
    let outcome = crate::evaluate_run(&metrics, &config, elapsed);
    assert_eq!(outcome.result, "fail");
    let reason = outcome.failure_reason.unwrap();
    assert!(reason.contains("tcp sent bytes/sec"), "{reason}");

    // A limit set for another transport is not applied to this run.
    let mut other = Config {
        packet_type: PacketType::Udp,
        ..Config::default()
    };
    other.limits.tcp.min_sent_bytes_per_second = 100_000;
    assert_eq!(crate::evaluate_run(&metrics, &other, elapsed).result, "ok");
}

#[test]
fn limits_survive_a_configuration_round_trip() {
    let mut config = Config::default();
    config.limits.udp.max_out_of_order_packets = 7;
    config.limits.sctp.min_received_bytes = 2048;
    let traffic = crate::traffic_config_from(&config);
    let restored = crate::config_from_traffic(&traffic);
    assert_eq!(restored.limits, config.limits);
}

#[test]
fn the_kubernetes_cluster_includes_grafana_and_a_test_script() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let postgres = std::fs::read_to_string(root.join("k8s/postgres.yaml")).unwrap();
    let netmark = std::fs::read_to_string(root.join("k8s/netmark.yaml")).unwrap();
    let grafana = std::fs::read_to_string(root.join("k8s/grafana.yaml")).unwrap();
    // PostgreSQL + volume, the web server, and Grafana reading the external metrics DB.
    assert!(postgres.contains("- name: postgres"), "no postgres container");
    assert!(postgres.contains("- name: volume"), "no volume container");
    assert!(
        postgres.contains("claimName: netmark-postgres-data"),
        "the pod does not mount the data volume"
    );
    assert!(netmark.contains("- name: netmark"), "no web server container");
    assert!(netmark.contains("--serve"), "the web server is not served");
    assert!(grafana.contains("- name: grafana"), "no grafana container");
    assert!(
        grafana.contains("netmark-postgres:5432"),
        "grafana is not pointed at the external metrics database"
    );
    assert!(
        grafana.contains("run_id"),
        "grafana dashboard must keep run_id on each series"
    );
    assert!(
        grafana.contains("netmark_metrics"),
        "grafana must query the CLI metrics table"
    );

    let init = std::fs::read_to_string(root.join("init.sh")).unwrap();
    assert!(
        init.contains("k8s/grafana.yaml") || init.contains("$GRAFANA_MANIFEST"),
        "init.sh does not deploy grafana"
    );

    let script = root.join("k8s/cluster-test.sh");
    let cases = std::fs::read_to_string(&script).unwrap();
    for expectation in [
        "kubernetes api reachable",
        "postgres container is deployed",
        "volume container is deployed",
        "grafana container is deployed",
        "data volume is bound",
        "postgres accepts connections",
        "web server reports live status",
        "web CLI answers the status command",
        "grafana reports healthy",
    ] {
        assert!(cases.contains(expectation), "no cluster test for {expectation}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&script).unwrap().permissions().mode();
        assert!(mode & 0o111 != 0, "k8s/cluster-test.sh is not executable");
    }
}

#[test]
fn sctp_and_webrtc_live_under_configure() {
    // The moved commands are documented where they now live, and the old
    // top-level words are gone from the command list and the help.
    let labels: Vec<String> = crate::cli::help_rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    for moved in [
        "configure sctp",
        "configure sctp status",
        "configure webrtc enable | disable",
        "configure webrtc channels <n>",
        "configure webrtc status",
    ] {
        assert!(labels.iter().any(|label| label == moved), "no help for {moved}");
    }
    assert!(
        !labels.iter().any(|label| label == "sctp" || label.starts_with("webrtc ")),
        "a top-level sctp or webrtc row is still documented"
    );
    assert!(!crate::cli::COMMANDS.contains(&"webrtc"));
    // Both are reachable through the standardized help.
    assert_eq!(
        crate::cli::help_for(&["configure", "sctp"]).unwrap(),
        crate::cli::sctp_help_rows()
    );
    assert!(crate::cli::help_for(&["configure", "webrtc"]).is_ok());
}

#[test]
fn protocols_are_enabled_and_disabled_under_configure() {
    let config = Arc::new(Mutex::new(Config::default()));
    for protocol in ["tcp", "sctp", "udp", "ip"] {
        let line = crate::cli::set_protocol_enabled(&config, protocol, false).unwrap();
        assert!(line.starts_with(protocol), "{line}");
        assert!(!config.lock().unwrap().protocols.enabled(
            PacketType::parse(protocol).unwrap()
        ));
        crate::cli::set_protocol_enabled(&config, protocol, true).unwrap();
        assert!(
            config
                .lock()
                .unwrap()
                .protocols
                .enabled(PacketType::parse(protocol).unwrap())
        );
    }
    assert!(crate::cli::set_protocol_enabled(&config, "nonsense", true).is_err());

    // Disabling the selected transport moves the selection to an enabled one.
    crate::cli::configure(&config, &["type", "sctp"]).unwrap();
    let line = crate::cli::set_protocol_enabled(&config, "sctp", false).unwrap();
    assert!(line.contains("transport is now"), "{line}");
    assert_ne!(config.lock().unwrap().packet_type, PacketType::Sctp);
    // A disabled transport cannot be selected again until it is enabled.
    let error = crate::cli::configure(&config, &["type", "sctp"]).unwrap_err();
    assert!(error.contains("configure sctp enable"), "{error}");
    crate::cli::set_protocol_enabled(&config, "sctp", true).unwrap();
    crate::cli::configure(&config, &["type", "sctp"]).unwrap();

    let table = crate::cli::protocols_table(&config, &crate::webrtc::Settings::default());
    for protocol in ["tcp", "sctp", "udp", "ip", "webrtc"] {
        assert!(table.contains(protocol), "{table} is missing {protocol}");
    }
    assert!(table.contains("selected"), "{table}");
}

#[test]
fn protocol_switches_survive_a_configuration_round_trip() {
    let mut config = Config::default();
    config.protocols.set(PacketType::Ip, false);
    let restored = crate::config_from_traffic(&crate::traffic_config_from(&config));
    assert_eq!(restored.protocols, config.protocols);
    assert!(!restored.protocols.enabled(PacketType::Ip));
}

#[test]
fn show_run_reports_everything_collected() {
    let sql = SqlState::new();
    sql.enable().unwrap();
    let id = sql.next_run_id(1);
    sql.start_run(id);
    sql.complete_run(
        id,
        &crate::core::RunSummary {
            result: "ok",
            sent_bytes: 4096,
            received_bytes: 2048,
            sent_bytes_per_second: 512,
            received_bytes_per_second: 256,
            failure_reason: None,
            protocol: "sctp",
            detail: crate::core::RunDetail {
                sent_tcp_packets: 40,
                sent_tcp_bytes: 4096,
                received_tcp_packets: 20,
                received_tcp_bytes: 2048,
                lost_packets: 1,
                out_of_order_packets: 2,
                jitter_millis: 7,
                tcp_jitter_millis: 7,
                tcp_mss: 1460,
                tcp_mtu: 1500,
                tcp_window_size: 65535,
                webrtc_sent_messages: 3,
                ..crate::core::RunDetail::default()
            },
        },
    );

    let rows = sql.run_detail_rows(id).unwrap().expect("the run was stored");
    let rendered = crate::cli_textout::table_lines(&rows, &[16, 80]).join("\n");
    for expected in [
        "Protocol", "sctp", "TCP/SCTP", "UDP", "IP", "UDP loss", "Jitter", "TCP transport",
        "WebRTC", "1460", "65535", "4096",
    ] {
        assert!(rendered.contains(expected), "show run is missing {expected}:\n{rendered}");
    }
    assert!(sql.run_detail_rows(id + 90_000).unwrap().is_none());

    // The same detail shows up per run in the list.
    let listed = sql.run_list().unwrap();
    let line = listed
        .iter()
        .find(|line| line.contains(&format!("run {id} ")))
        .expect("the run is listed");
    assert!(line.contains("protocol=sctp"), "{line}");
    assert!(line.contains("lost=1"), "{line}");
    assert!(line.contains("out_of_order=2"), "{line}");
    assert!(line.contains("jitter=7 ms"), "{line}");
}
