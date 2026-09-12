use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64 as RandomState;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_REMOTE: &str = "127.0.0.1";

/// Values stored in the `role` column of every local SQLite table, so a database
/// pulled off any host says plainly which side wrote each row.
pub const ROLE_CLIENT: &str = "client";
pub const ROLE_SERVER: &str = "server";
pub const ROLE_BOTH: &str = "client+server";
pub const ROLE_NONE: &str = "none";

/// Names the role of an instance from which sides are enabled.
pub fn role_name(client: bool, server: bool) -> &'static str {
    match (client, server) {
        (true, true) => ROLE_BOTH,
        (true, false) => ROLE_CLIENT,
        (false, true) => ROLE_SERVER,
        (false, false) => ROLE_NONE,
    }
}
const PORT: u16 = 9000;
pub(crate) const PACKET_SIZE: usize = 1024;
/// UDP packet header: 8-byte sequence number, 8-byte send timestamp (ms) and
/// 8-byte client id. Sequence numbers are per client, so several clients can send
/// to one server without looking like reordering.
const UDP_HEADER_LEN: usize = 24;
/// TCP is a byte stream, so a timestamp is embedded every `TCP_TIMESTAMP_CHUNK` bytes.
const TCP_TIMESTAMP_CHUNK: usize = 100;
/// TCP frame header: 8-byte send timestamp (ms) + 2-byte chunk length.
const TCP_HEADER_LEN: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketType {
    Tcp,
    Sctp,
    Udp,
    /// Payload carried directly in IPv4 packets, with no transport header.
    Ip,
}
impl PacketType {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Some(Self::Tcp),
            "sctp" => Some(Self::Sctp),
            "udp" => Some(Self::Udp),
            "ip" | "rawip" => Some(Self::Ip),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Sctp => "sctp",
            Self::Udp => "udp",
            Self::Ip => "ip",
        }
    }
}

/// Which transports may be used. `configure <protocol> disable` turns one off,
/// and a disabled transport cannot be selected, started or selftested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolSwitches {
    pub tcp: bool,
    pub sctp: bool,
    pub udp: bool,
    pub ip: bool,
}

impl Default for ProtocolSwitches {
    fn default() -> Self {
        Self {
            tcp: true,
            sctp: false,
            udp: true,
            ip: false,
        }
    }
}

impl ProtocolSwitches {
    pub fn enabled(&self, packet_type: PacketType) -> bool {
        match packet_type {
            PacketType::Tcp => self.tcp,
            PacketType::Sctp => self.sctp,
            PacketType::Udp => self.udp,
            PacketType::Ip => self.ip,
        }
    }
    pub fn set(&mut self, packet_type: PacketType, enabled: bool) {
        match packet_type {
            PacketType::Tcp => self.tcp = enabled,
            PacketType::Sctp => self.sctp = enabled,
            PacketType::Udp => self.udp = enabled,
            PacketType::Ip => self.ip = enabled,
        }
    }
}

/// Limits that decide whether a run fails, kept per transport because each one
/// is measured differently. A zero value means the limit is not checked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    pub min_sent_bytes: u64,
    pub min_received_bytes: u64,
    pub min_sent_bytes_per_second: u64,
    pub min_received_bytes_per_second: u64,
    pub max_jitter_millis: u64,
    pub max_lost_packets: u64,
    pub max_out_of_order_packets: u64,
}

/// The parameters `configure limits <protocol> <parameter> <value>` accepts, in
/// the order they are listed and checked.
pub const LIMIT_PARAMETERS: [&str; 7] = [
    "min-sent-bytes",
    "min-received-bytes",
    "min-sent-bytes-per-second",
    "min-received-bytes-per-second",
    "max-jitter-millis",
    "max-lost-packets",
    "max-out-of-order-packets",
];

impl Limits {
    /// Whether a parameter can be measured for a transport; UDP is the only one
    /// that counts lost and out-of-order packets, and raw IP has no jitter clock.
    pub fn supports(packet_type: PacketType, parameter: &str) -> bool {
        match parameter {
            "max-lost-packets" | "max-out-of-order-packets" => packet_type == PacketType::Udp,
            "max-jitter-millis" => packet_type != PacketType::Ip,
            _ => LIMIT_PARAMETERS.contains(&parameter),
        }
    }
    pub fn get(&self, parameter: &str) -> Option<u64> {
        Some(match parameter {
            "min-sent-bytes" => self.min_sent_bytes,
            "min-received-bytes" => self.min_received_bytes,
            "min-sent-bytes-per-second" => self.min_sent_bytes_per_second,
            "min-received-bytes-per-second" => self.min_received_bytes_per_second,
            "max-jitter-millis" => self.max_jitter_millis,
            "max-lost-packets" => self.max_lost_packets,
            "max-out-of-order-packets" => self.max_out_of_order_packets,
            _ => return None,
        })
    }
    pub fn set(&mut self, parameter: &str, value: u64) -> bool {
        match parameter {
            "min-sent-bytes" => self.min_sent_bytes = value,
            "min-received-bytes" => self.min_received_bytes = value,
            "min-sent-bytes-per-second" => self.min_sent_bytes_per_second = value,
            "min-received-bytes-per-second" => self.min_received_bytes_per_second = value,
            "max-jitter-millis" => self.max_jitter_millis = value,
            "max-lost-packets" => self.max_lost_packets = value,
            "max-out-of-order-packets" => self.max_out_of_order_packets = value,
            _ => return false,
        }
        true
    }
}

/// One set of limits per transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LimitSet {
    pub tcp: Limits,
    pub sctp: Limits,
    pub udp: Limits,
    pub ip: Limits,
}

impl LimitSet {
    pub fn get(&self, packet_type: PacketType) -> &Limits {
        match packet_type {
            PacketType::Tcp => &self.tcp,
            PacketType::Sctp => &self.sctp,
            PacketType::Udp => &self.udp,
            PacketType::Ip => &self.ip,
        }
    }
    pub fn get_mut(&mut self, packet_type: PacketType) -> &mut Limits {
        match packet_type {
            PacketType::Tcp => &mut self.tcp,
            PacketType::Sctp => &mut self.sctp,
            PacketType::Udp => &mut self.udp,
            PacketType::Ip => &mut self.ip,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    /// UDP packets per second; ignored for TCP, which is paced by `tcp_bytes_per_second`.
    pub udp_rate: u64,
    pub packet_type: PacketType,
    pub tcp_bytes_per_second: u64,
    /// Requested TCP socket window in bytes; 0 leaves the OS default unchanged.
    pub tcp_window_size: u32,
    pub udp_packet_size: usize,
    pub client_runtime: u64,
    pub server_runtime: u64,
    pub jitter_millis: u64,
    pub client_jitter_millis: u64,
    pub server_jitter_millis: u64,
    pub max_tcp_jitter_millis: u64,
    pub max_udp_jitter_millis: u64,
    /// Minimum acceptable throughput in bytes/sec for a run to be considered a pass; 0 disables the check.
    pub limit_bytes_per_second: u64,
    /// Per-transport limits set with `configure limits`; a failed limit fails the run.
    pub limits: LimitSet,
    /// Which transports `configure <protocol> enable|disable` allows.
    pub protocols: ProtocolSwitches,
    pub webrtc: crate::webrtc::Settings,
    pub admin_emails: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            udp_rate: 100,
            packet_type: PacketType::Tcp,
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
            limit_bytes_per_second: 0,
            limits: LimitSet::default(),
            protocols: ProtocolSwitches::default(),
            webrtc: crate::webrtc::Settings::default(),
            admin_emails: Vec::new(),
        }
    }
}

pub struct StartGate {
    started: Mutex<bool>,
    wake: Condvar,
}
impl StartGate {
    pub fn new() -> Self {
        Self {
            started: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
    pub fn start(&self) {
        *self.started.lock().unwrap() = true;
        self.wake.notify_all();
    }
    fn wait(&self) {
        let mut started = self.started.lock().unwrap();
        while !*started {
            started = self.wake.wait(started).unwrap();
        }
    }
}

impl Default for StartGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Metrics {
    sent_tcp_packets: AtomicU64,
    sent_tcp_bytes: AtomicU64,
    sent_udp_packets: AtomicU64,
    sent_udp_bytes: AtomicU64,
    received_tcp_packets: AtomicU64,
    received_tcp_bytes: AtomicU64,
    received_udp_packets: AtomicU64,
    received_udp_bytes: AtomicU64,
    lost_udp_packets: AtomicU64,
    out_of_order_udp_packets: AtomicU64,
    jitter_millis: AtomicU64,
    tcp_jitter_millis: AtomicU64,
    udp_jitter_millis: AtomicU64,
    run_sent_tcp_packets: AtomicU64,
    run_sent_tcp_bytes: AtomicU64,
    run_sent_udp_packets: AtomicU64,
    run_sent_udp_bytes: AtomicU64,
    run_received_tcp_packets: AtomicU64,
    run_received_tcp_bytes: AtomicU64,
    run_received_udp_packets: AtomicU64,
    run_received_udp_bytes: AtomicU64,
    sent_ip_packets: AtomicU64,
    sent_ip_bytes: AtomicU64,
    received_ip_packets: AtomicU64,
    received_ip_bytes: AtomicU64,
    run_sent_ip_packets: AtomicU64,
    run_sent_ip_bytes: AtomicU64,
    run_received_ip_packets: AtomicU64,
    run_received_ip_bytes: AtomicU64,
    webrtc_sent_messages: AtomicU64,
    webrtc_received_messages: AtomicU64,
    webrtc_invalid_frames: AtomicU64,
    last_udp_timestamps: Mutex<Option<(i64, i64)>>,
    last_tcp_timestamps: Mutex<Option<(i64, i64)>>,
    tcp_mss: AtomicU64,
    tcp_mtu: AtomicU64,
    tcp_window_size: AtomicU64,
}
impl Metrics {
    pub fn new() -> Self {
        Self {
            sent_tcp_packets: AtomicU64::new(0),
            sent_tcp_bytes: AtomicU64::new(0),
            sent_udp_packets: AtomicU64::new(0),
            sent_udp_bytes: AtomicU64::new(0),
            received_tcp_packets: AtomicU64::new(0),
            received_tcp_bytes: AtomicU64::new(0),
            received_udp_packets: AtomicU64::new(0),
            received_udp_bytes: AtomicU64::new(0),
            lost_udp_packets: AtomicU64::new(0),
            out_of_order_udp_packets: AtomicU64::new(0),
            jitter_millis: AtomicU64::new(0),
            tcp_jitter_millis: AtomicU64::new(0),
            udp_jitter_millis: AtomicU64::new(0),
            run_sent_tcp_packets: AtomicU64::new(0),
            run_sent_tcp_bytes: AtomicU64::new(0),
            run_sent_udp_packets: AtomicU64::new(0),
            run_sent_udp_bytes: AtomicU64::new(0),
            run_received_tcp_packets: AtomicU64::new(0),
            run_received_tcp_bytes: AtomicU64::new(0),
            run_received_udp_packets: AtomicU64::new(0),
            run_received_udp_bytes: AtomicU64::new(0),
            sent_ip_packets: AtomicU64::new(0),
            sent_ip_bytes: AtomicU64::new(0),
            received_ip_packets: AtomicU64::new(0),
            received_ip_bytes: AtomicU64::new(0),
            run_sent_ip_packets: AtomicU64::new(0),
            run_sent_ip_bytes: AtomicU64::new(0),
            run_received_ip_packets: AtomicU64::new(0),
            run_received_ip_bytes: AtomicU64::new(0),
            webrtc_sent_messages: AtomicU64::new(0),
            webrtc_received_messages: AtomicU64::new(0),
            webrtc_invalid_frames: AtomicU64::new(0),
            last_udp_timestamps: Mutex::new(None),
            last_tcp_timestamps: Mutex::new(None),
            tcp_mss: AtomicU64::new(0),
            tcp_mtu: AtomicU64::new(0),
            tcp_window_size: AtomicU64::new(0),
        }
    }
    fn add(&self, sent: bool, protocol: PacketType, bytes: usize) {
        self.add_counts(sent, protocol, 1, bytes as u64);
    }
    /// `packets` and `bytes` are added separately because a TCP read returns an
    /// arbitrary slice of the stream, so bytes are counted per read while packets
    /// are counted per complete timestamped frame.
    fn add_counts(&self, sent: bool, protocol: PacketType, packets: u64, bytes: u64) {
        let (total_packets, total, run_packets, run_total) = match (sent, protocol) {
            (true, PacketType::Tcp | PacketType::Sctp) => (
                &self.sent_tcp_packets,
                &self.sent_tcp_bytes,
                &self.run_sent_tcp_packets,
                &self.run_sent_tcp_bytes,
            ),
            (true, PacketType::Udp) => (
                &self.sent_udp_packets,
                &self.sent_udp_bytes,
                &self.run_sent_udp_packets,
                &self.run_sent_udp_bytes,
            ),
            (false, PacketType::Tcp | PacketType::Sctp) => (
                &self.received_tcp_packets,
                &self.received_tcp_bytes,
                &self.run_received_tcp_packets,
                &self.run_received_tcp_bytes,
            ),
            (false, PacketType::Udp) => (
                &self.received_udp_packets,
                &self.received_udp_bytes,
                &self.run_received_udp_packets,
                &self.run_received_udp_bytes,
            ),
            (true, PacketType::Ip) => (
                &self.sent_ip_packets,
                &self.sent_ip_bytes,
                &self.run_sent_ip_packets,
                &self.run_sent_ip_bytes,
            ),
            (false, PacketType::Ip) => (
                &self.received_ip_packets,
                &self.received_ip_bytes,
                &self.run_received_ip_packets,
                &self.run_received_ip_bytes,
            ),
        };
        total_packets.fetch_add(packets, Ordering::Relaxed);
        total.fetch_add(bytes, Ordering::Relaxed);
        run_packets.fetch_add(packets, Ordering::Relaxed);
        run_total.fetch_add(bytes, Ordering::Relaxed);
    }
    /// Total bytes sent (TCP, UDP) since the last `reset_run()`, unaffected by the
    /// periodic `snapshot()` used for live reporting.
    pub fn run_sent_bytes(&self) -> (u64, u64) {
        (
            self.run_sent_tcp_bytes.load(Ordering::Relaxed),
            self.run_sent_udp_bytes.load(Ordering::Relaxed),
        )
    }
    /// Total bytes received (TCP, UDP) since the last `reset_run()`, mirroring
    /// `run_sent_bytes` so the server side's throughput can be checked too.
    pub fn run_received_bytes(&self) -> (u64, u64) {
        (
            self.run_received_tcp_bytes.load(Ordering::Relaxed),
            self.run_received_udp_bytes.load(Ordering::Relaxed),
        )
    }
    /// Bytes sent across every transport this run, which is what the reported
    /// upload bandwidth is computed from.
    pub fn run_sent_total(&self) -> u64 {
        let (tcp, udp) = self.run_sent_bytes();
        tcp + udp + self.run_sent_ip_bytes.load(Ordering::Relaxed)
    }
    /// Bytes received across every transport this run, for download bandwidth.
    pub fn run_received_total(&self) -> u64 {
        let (tcp, udp) = self.run_received_bytes();
        tcp + udp + self.run_received_ip_bytes.load(Ordering::Relaxed)
    }
    /// Run-scoped counters in the same `[sent_tcp_packets, sent_tcp_bytes, ...]`
    /// layout as `snapshot()`, with the raw IP counters appended, for the single
    /// final report written to the external metrics database at the end of a run.
    pub fn run_totals(&self) -> [u64; 12] {
        [
            self.run_sent_tcp_packets.load(Ordering::Relaxed),
            self.run_sent_tcp_bytes.load(Ordering::Relaxed),
            self.run_sent_udp_packets.load(Ordering::Relaxed),
            self.run_sent_udp_bytes.load(Ordering::Relaxed),
            self.run_received_tcp_packets.load(Ordering::Relaxed),
            self.run_received_tcp_bytes.load(Ordering::Relaxed),
            self.run_received_udp_packets.load(Ordering::Relaxed),
            self.run_received_udp_bytes.load(Ordering::Relaxed),
            self.run_sent_ip_packets.load(Ordering::Relaxed),
            self.run_sent_ip_bytes.load(Ordering::Relaxed),
            self.run_received_ip_packets.load(Ordering::Relaxed),
            self.run_received_ip_bytes.load(Ordering::Relaxed),
        ]
    }
    /// Run-scoped (packets, bytes) for one direction and protocol, as used by the
    /// end-of-run debrief between client and server.
    pub fn run_counts(&self, sent: bool, protocol: PacketType) -> (u64, u64) {
        let totals = self.run_totals();
        match (sent, protocol) {
            (true, PacketType::Tcp | PacketType::Sctp) => (totals[0], totals[1]),
            (true, PacketType::Udp) => (totals[2], totals[3]),
            (false, PacketType::Tcp | PacketType::Sctp) => (totals[4], totals[5]),
            (false, PacketType::Udp) => (totals[6], totals[7]),
            (true, PacketType::Ip) => (totals[8], totals[9]),
            (false, PacketType::Ip) => (totals[10], totals[11]),
        }
    }
    /// WebRTC data-channel messages sent, received, and frames that failed to parse.
    pub fn webrtc_counts(&self) -> (u64, u64, u64) {
        (
            self.webrtc_sent_messages.load(Ordering::Relaxed),
            self.webrtc_received_messages.load(Ordering::Relaxed),
            self.webrtc_invalid_frames.load(Ordering::Relaxed),
        )
    }
    pub(crate) fn record_webrtc_sent(&self) {
        self.webrtc_sent_messages.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_webrtc_received(&self, valid: bool) {
        if valid {
            self.webrtc_received_messages
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.webrtc_invalid_frames.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// Clears the per-run counters (sent/received bytes, jitter, UDP loss) so the
    /// next run's final ok/fail evaluation and metrics report reflect only that run.
    pub fn reset_run(&self) {
        self.run_sent_tcp_packets.store(0, Ordering::Relaxed);
        self.run_sent_tcp_bytes.store(0, Ordering::Relaxed);
        self.run_sent_udp_packets.store(0, Ordering::Relaxed);
        self.run_sent_udp_bytes.store(0, Ordering::Relaxed);
        self.run_received_tcp_packets.store(0, Ordering::Relaxed);
        self.run_received_tcp_bytes.store(0, Ordering::Relaxed);
        self.run_received_udp_packets.store(0, Ordering::Relaxed);
        self.run_received_udp_bytes.store(0, Ordering::Relaxed);
        self.run_sent_ip_packets.store(0, Ordering::Relaxed);
        self.run_sent_ip_bytes.store(0, Ordering::Relaxed);
        self.run_received_ip_packets.store(0, Ordering::Relaxed);
        self.run_received_ip_bytes.store(0, Ordering::Relaxed);
        self.webrtc_sent_messages.store(0, Ordering::Relaxed);
        self.webrtc_received_messages.store(0, Ordering::Relaxed);
        self.webrtc_invalid_frames.store(0, Ordering::Relaxed);
        self.jitter_millis.store(0, Ordering::Relaxed);
        self.tcp_jitter_millis.store(0, Ordering::Relaxed);
        self.udp_jitter_millis.store(0, Ordering::Relaxed);
        self.lost_udp_packets.store(0, Ordering::Relaxed);
        self.out_of_order_udp_packets.store(0, Ordering::Relaxed);
        self.tcp_mss.store(0, Ordering::Relaxed);
        self.tcp_mtu.store(0, Ordering::Relaxed);
        self.tcp_window_size.store(0, Ordering::Relaxed);
    }
    /// Live counters since the last call, used for the once-per-second bandwidth
    /// display; raw IP is folded into the UDP slots because both are datagrams.
    pub fn snapshot(&self) -> [u64; 8] {
        [
            self.sent_tcp_packets.swap(0, Ordering::Relaxed),
            self.sent_tcp_bytes.swap(0, Ordering::Relaxed),
            self.sent_udp_packets.swap(0, Ordering::Relaxed)
                + self.sent_ip_packets.swap(0, Ordering::Relaxed),
            self.sent_udp_bytes.swap(0, Ordering::Relaxed)
                + self.sent_ip_bytes.swap(0, Ordering::Relaxed),
            self.received_tcp_packets.swap(0, Ordering::Relaxed),
            self.received_tcp_bytes.swap(0, Ordering::Relaxed),
            self.received_udp_packets.swap(0, Ordering::Relaxed)
                + self.received_ip_packets.swap(0, Ordering::Relaxed),
            self.received_udp_bytes.swap(0, Ordering::Relaxed)
                + self.received_ip_bytes.swap(0, Ordering::Relaxed),
        ]
    }
    /// TCP transport values observed on the connected client socket. Zero means
    /// the active transport is not TCP or the platform could not report it.
    pub fn tcp_transport(&self) -> (u64, u64, u64) {
        (
            self.tcp_mss.load(Ordering::Relaxed),
            self.tcp_mtu.load(Ordering::Relaxed),
            self.tcp_window_size.load(Ordering::Relaxed),
        )
    }
    fn record_tcp_transport(&self, mss: u64, mtu: u64, window_size: u64) {
        self.tcp_mss.store(mss, Ordering::Relaxed);
        self.tcp_mtu.store(mtu, Ordering::Relaxed);
        self.tcp_window_size.store(window_size, Ordering::Relaxed);
    }
    pub fn udp_status(&self) -> (u64, u64) {
        (
            self.lost_udp_packets.load(Ordering::Relaxed),
            self.out_of_order_udp_packets.load(Ordering::Relaxed),
        )
    }
    fn udp_sequence(&self, expected: &mut Option<u64>, sequence: u64) {
        if let Some(previous) = *expected {
            if sequence > previous + 1 {
                self.lost_udp_packets
                    .fetch_add(sequence - previous - 1, Ordering::Relaxed);
            }
            if sequence <= previous {
                self.out_of_order_udp_packets
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        *expected = Some((*expected).map_or(sequence, |previous| previous.max(sequence)));
    }
    #[cfg(test)]
    pub(crate) fn test_udp_sequence(&self, expected: &mut Option<u64>, sequence: u64) {
        self.udp_sequence(expected, sequence);
    }
    #[cfg(test)]
    pub(crate) fn add_test_bytes(&self, sent: bool, protocol: PacketType, bytes: u64) {
        self.add_counts(sent, protocol, 1, bytes);
    }
    fn record_jitter(&self, protocol: PacketType, jitter: Duration) {
        let value = jitter.as_millis() as u64;
        self.jitter_millis.fetch_max(value, Ordering::Relaxed);
        match protocol {
            PacketType::Tcp | PacketType::Sctp => {
                self.tcp_jitter_millis.fetch_max(value, Ordering::Relaxed)
            }
            // Raw IP is a datagram transport, so it shares the UDP jitter budget.
            PacketType::Udp | PacketType::Ip => {
                self.udp_jitter_millis.fetch_max(value, Ordering::Relaxed)
            }
        };
    }
    #[cfg(test)]
    pub(crate) fn test_jitter(&self, protocol: PacketType, millis: u64) {
        self.record_jitter(protocol, Duration::from_millis(millis));
    }
    /// Computes jitter (RFC 3550 style delay variation) from a packet's send timestamp,
    /// so the receiving side can detect jitter without relying on local send intervals.
    fn timestamp_jitter(
        &self,
        protocol: PacketType,
        last: &Mutex<Option<(i64, i64)>>,
        send_timestamp_ms: i64,
    ) {
        let arrival_ms = Utc::now().timestamp_millis();
        let mut last = last.lock().unwrap();
        if let Some((last_send_ms, last_arrival_ms)) = *last {
            let delay_variation =
                (arrival_ms - last_arrival_ms) - (send_timestamp_ms - last_send_ms);
            self.record_jitter(protocol, Duration::from_millis(delay_variation.unsigned_abs()));
        }
        *last = Some((send_timestamp_ms, arrival_ms));
    }
    fn udp_timestamp_jitter(&self, send_timestamp_ms: i64) {
        self.timestamp_jitter(PacketType::Udp, &self.last_udp_timestamps, send_timestamp_ms);
    }
    fn tcp_timestamp_jitter(&self, send_timestamp_ms: i64) {
        self.timestamp_jitter(PacketType::Tcp, &self.last_tcp_timestamps, send_timestamp_ms);
    }
    pub fn jitter_millis(&self) -> u64 {
        self.jitter_millis.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(crate) fn protocol_jitter_millis(&self) -> (u64, u64) {
        (
            self.tcp_jitter_millis.load(Ordering::Relaxed),
            self.udp_jitter_millis.load(Ordering::Relaxed),
        )
    }
    pub fn tcp_jitter_millis(&self) -> u64 {
        self.tcp_jitter_millis.load(Ordering::Relaxed)
    }
    pub fn udp_jitter_millis(&self) -> u64 {
        self.udp_jitter_millis.load(Ordering::Relaxed)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SqlState {
    enabled: AtomicBool,
    connection: Mutex<Option<Connection>>,
    run_id: AtomicU64,
    role: Mutex<String>,
}

/// What is stored about a finished run, including the bandwidth it achieved in
/// each direction.
pub struct RunSummary<'a> {
    pub result: &'a str,
    pub sent_bytes: u64,
    pub received_bytes: u64,
    pub sent_bytes_per_second: u64,
    pub received_bytes_per_second: u64,
    pub failure_reason: Option<&'a str>,
    /// The transport the run used, so `list` and `show run` can name it.
    pub protocol: &'a str,
    /// Everything else the run collected, stored so `show run` can show it all.
    pub detail: RunDetail,
}

/// Every per-protocol counter a run collected, kept with the run so `show run`
/// can report the whole picture instead of just the totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunDetail {
    pub sent_tcp_packets: u64,
    pub sent_tcp_bytes: u64,
    pub received_tcp_packets: u64,
    pub received_tcp_bytes: u64,
    pub sent_udp_packets: u64,
    pub sent_udp_bytes: u64,
    pub received_udp_packets: u64,
    pub received_udp_bytes: u64,
    pub sent_ip_packets: u64,
    pub sent_ip_bytes: u64,
    pub received_ip_packets: u64,
    pub received_ip_bytes: u64,
    pub lost_packets: u64,
    pub out_of_order_packets: u64,
    pub jitter_millis: u64,
    pub tcp_jitter_millis: u64,
    pub udp_jitter_millis: u64,
    pub tcp_mss: u64,
    pub tcp_mtu: u64,
    pub tcp_window_size: u64,
    pub webrtc_sent_messages: u64,
    pub webrtc_received_messages: u64,
    pub webrtc_invalid_frames: u64,
}

impl RunDetail {
    /// Snapshots the run-scoped counters of a finished run.
    pub fn from_metrics(metrics: &Metrics) -> Self {
        let totals = metrics.run_totals();
        let (lost, out_of_order) = metrics.udp_status();
        let (mss, mtu, window) = metrics.tcp_transport();
        let (webrtc_sent, webrtc_received, webrtc_invalid) = metrics.webrtc_counts();
        Self {
            sent_tcp_packets: totals[0],
            sent_tcp_bytes: totals[1],
            sent_udp_packets: totals[2],
            sent_udp_bytes: totals[3],
            received_tcp_packets: totals[4],
            received_tcp_bytes: totals[5],
            received_udp_packets: totals[6],
            received_udp_bytes: totals[7],
            sent_ip_packets: totals[8],
            sent_ip_bytes: totals[9],
            received_ip_packets: totals[10],
            received_ip_bytes: totals[11],
            lost_packets: lost,
            out_of_order_packets: out_of_order,
            jitter_millis: metrics.jitter_millis(),
            tcp_jitter_millis: metrics.tcp_jitter_millis(),
            udp_jitter_millis: metrics.udp_jitter_millis(),
            tcp_mss: mss,
            tcp_mtu: mtu,
            tcp_window_size: window,
            webrtc_sent_messages: webrtc_sent,
            webrtc_received_messages: webrtc_received,
            webrtc_invalid_frames: webrtc_invalid,
        }
    }
}

/// Bytes per second over `elapsed`, floored at one second so a very short run
/// cannot report an inflated figure.
pub fn bandwidth(bytes: u64, elapsed: Duration) -> u64 {
    (bytes as f64 / elapsed.as_secs_f64().max(1.0)) as u64
}
impl SqlState {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            connection: Mutex::new(None),
            run_id: AtomicU64::new(0),
            role: Mutex::new(ROLE_NONE.to_string()),
        }
    }
    /// Stamps every row this instance writes with what it was doing: "client",
    /// "server", "client+server" or "none".
    pub fn set_role(&self, role: &str) {
        *self.role.lock().unwrap() = role.to_string();
    }
    pub fn role(&self) -> String {
        self.role.lock().unwrap().clone()
    }
    pub fn enable(&self) -> Result<(), String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS run_counter (id INTEGER PRIMARY KEY CHECK (id = 1), next_id INTEGER NOT NULL); INSERT OR IGNORE INTO run_counter (id, next_id) VALUES (1, 1);").map_err(|e| e.to_string())?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, started_utc TEXT NOT NULL); CREATE TABLE IF NOT EXISTS alarms (timestamp_utc TEXT NOT NULL, target TEXT NOT NULL, error TEXT NOT NULL);").map_err(|e| e.to_string())?;
        connection.execute_batch(MONITOR_STATUS_TABLE).map_err(|e| e.to_string())?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS debriefs (run_id INTEGER NOT NULL, timestamp_utc TEXT NOT NULL, role TEXT NOT NULL, protocol TEXT NOT NULL, sent_packets INTEGER NOT NULL, sent_bytes INTEGER NOT NULL, received_packets INTEGER NOT NULL, received_bytes INTEGER NOT NULL, lost_packets INTEGER NOT NULL, out_of_order_packets INTEGER NOT NULL, matched INTEGER NOT NULL, mismatch_reason TEXT, PRIMARY KEY (run_id, role));").map_err(|e| e.to_string())?;
        let _ = connection.execute("UPDATE run_counter SET next_id = MAX(next_id, COALESCE((SELECT MAX(id) + 1 FROM runs), 1)) WHERE id = 1", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN completed_utc TEXT", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN result TEXT NOT NULL DEFAULT 'running'", []);
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN sent_bytes INTEGER DEFAULT 0", []);
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN received_bytes INTEGER DEFAULT 0",
            [],
        );
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN sent_bytes_per_second INTEGER DEFAULT 0",
            [],
        );
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN received_bytes_per_second INTEGER DEFAULT 0",
            [],
        );
        let _ = connection.execute("ALTER TABLE runs ADD COLUMN failure_reason TEXT", []);
        // Everything a run collected, so `show run` can report the whole picture.
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN protocol TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );
        for column in RUN_DETAIL_COLUMNS {
            let _ = connection.execute(
                &format!("ALTER TABLE runs ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"),
                [],
            );
        }
        let _ = connection.execute(
            "ALTER TABLE runs ADD COLUMN role TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );
        let _ = connection.execute(
            "ALTER TABLE alarms ADD COLUMN role TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );
        let _ = connection.execute(
            "UPDATE runs SET completed_utc = COALESCE(completed_utc, ?1), result = 'aborted' WHERE result = 'running'",
            params![timestamp()],
        );
        *self.connection.lock().unwrap() = Some(connection);
        self.enabled.store(true, Ordering::Relaxed);
        Ok(())
    }
    pub fn next_run_id(&self, fallback: u64) -> u64 {
        let connection = self.connection.lock().unwrap();
        connection.as_ref().and_then(|c| c.query_row("UPDATE run_counter SET next_id = next_id + 1 WHERE id = 1 RETURNING next_id - 1", [], |row| row.get(0)).ok()).unwrap_or(fallback)
    }
    pub fn start_run(&self, id: u64) {
        self.run_id.store(id, Ordering::Relaxed);
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let role = self.role();
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let _ = c.execute(
                "INSERT INTO runs (id, started_utc, result, role) VALUES (?1, ?2, 'running', ?3)",
                params![id, timestamp(), role],
            );
        }
    }
    /// Records the end state of a run: bytes moved in each direction, the resulting
    /// upload and download bandwidth, whether it stayed within its configured
    /// limits, and why not, if it didn't. No millisecond-level metrics are ever
    /// stored here; those only go to the configured external SQL database.
    pub fn complete_run(&self, id: u64, summary: &RunSummary<'_>) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(c) = self.connection.lock().unwrap().as_ref() {
            let assignments = RUN_DETAIL_COLUMNS
                .iter()
                .enumerate()
                .map(|(index, column)| format!("{column} = ?{}", index + 9))
                .collect::<Vec<_>>()
                .join(", ");
            let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![
                Box::new(timestamp()),
                Box::new(summary.result.to_string()),
                Box::new(summary.sent_bytes),
                Box::new(summary.received_bytes),
                Box::new(summary.sent_bytes_per_second),
                Box::new(summary.received_bytes_per_second),
                Box::new(summary.failure_reason.map(str::to_string)),
                Box::new(summary.protocol.to_string()),
            ];
            values.extend(
                run_detail_values(&summary.detail)
                    .into_iter()
                    .map(|value| Box::new(value) as Box<dyn rusqlite::ToSql>),
            );
            values.push(Box::new(id));
            let last = values.len();
            let _ = c.execute(
                &format!(
                    "UPDATE runs SET completed_utc = ?1, result = ?2, sent_bytes = ?3, received_bytes = ?4, sent_bytes_per_second = ?5, received_bytes_per_second = ?6, failure_reason = ?7, protocol = ?8, {assignments} WHERE id = ?{last} AND result = 'running'"
                ),
                rusqlite::params_from_iter(values.iter().map(|value| value.as_ref())),
            );
        }
    }
    pub fn clean(&self) -> Result<(), String> {
        let connection = self.connection.lock().unwrap();
        connection
            .as_ref()
            .ok_or_else(|| "local SQL is disabled".to_string())?
            .execute_batch(
                "DELETE FROM alarms; DELETE FROM monitor_status; DELETE FROM debriefs; DELETE FROM runs;",
            )
            .map_err(|error| error.to_string())
    }
    /// Stores this host's side of an end-of-run debrief. The client and the server
    /// each call this against their own local database.
    pub fn record_debrief(&self, debrief: &Debrief) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        if let Some(connection) = self.connection.lock().unwrap().as_ref() {
            let _ = connection.execute(
                "INSERT OR REPLACE INTO debriefs (run_id, timestamp_utc, role, protocol, sent_packets, sent_bytes, received_packets, received_bytes, lost_packets, out_of_order_packets, matched, mismatch_reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    debrief.run_id,
                    timestamp(),
                    debrief.role,
                    debrief.protocol,
                    debrief.sent_packets,
                    debrief.sent_bytes,
                    debrief.received_packets,
                    debrief.received_bytes,
                    debrief.lost_packets,
                    debrief.out_of_order_packets,
                    debrief.matched() as i64,
                    debrief.mismatch_reason(),
                ],
            );
        }
    }
    /// Reads back the debriefs stored for a run, one line per role.
    pub fn debriefs(&self, run_id: u64) -> Result<Vec<String>, String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        let mut query = connection
            .prepare("SELECT timestamp_utc, role, protocol, sent_packets, received_packets, sent_bytes, received_bytes, lost_packets, out_of_order_packets, matched, mismatch_reason FROM debriefs WHERE run_id = ?1 ORDER BY role")
            .map_err(|e| e.to_string())?;
        let rows = query
            .query_map(params![run_id], |row| {
                Ok(format!(
                    "{} debrief run={run_id} role={} protocol={} sent_packets={} received_packets={} sent_bytes={} received_bytes={} lost={} out_of_order={} result={}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, u64>(6)?,
                    row.get::<_, u64>(7)?,
                    row.get::<_, u64>(8)?,
                    if row.get::<_, i64>(9)? == 1 {
                        "match".to_string()
                    } else {
                        format!(
                            "mismatch ({})",
                            row.get::<_, Option<String>>(10)?.unwrap_or_default()
                        )
                    }
                ))
            })
            .map_err(|e| e.to_string())?;
        rows.map(|row| row.map_err(|e| e.to_string())).collect()
    }
    pub fn list_runs(&self) -> Result<(), String> {
        for row in self.run_list()? {
            crate::cli_textout::line(row);
        }
        Ok(())
    }
    /// One summary line per run, newest last, including the role that wrote it and
    /// the bandwidth measured in each direction.
    pub fn run_list(&self) -> Result<Vec<String>, String> {
        let c = Connection::open(database_path()).map_err(|e| e.to_string())?;
        let columns = RUN_DETAIL_COLUMNS.join(", ");
        let mut q = c
            .prepare(&format!("SELECT started_utc, id, role, result, sent_bytes, received_bytes, sent_bytes_per_second, received_bytes_per_second, failure_reason, protocol, {columns} FROM runs ORDER BY id"))
            .map_err(|e| e.to_string())?;
        let rows = q
            .query_map([], |row| {
                let text = |index: usize| -> rusqlite::Result<String> {
                    Ok(row
                        .get::<_, Option<String>>(index)?
                        .unwrap_or_else(|| "n/a".to_string()))
                };
                let number =
                    |index: usize| -> rusqlite::Result<u64> {
                        Ok(row.get::<_, Option<u64>>(index)?.unwrap_or(0))
                    };
                let detail = |name: &str| -> u64 {
                    let index = RUN_DETAIL_COLUMNS
                        .iter()
                        .position(|column| *column == name)
                        .expect("every detail column is listed");
                    number(10 + index).unwrap_or(0)
                };
                Ok(format!(
                    "{} run {} role={} protocol={} {} sent_bytes={} received_bytes={} up={} bytes/sec down={} bytes/sec tcp/sctp={}p/{}B sent {}p/{}B received udp={}p/{}B sent {}p/{}B received ip={}p/{}B sent {}p/{}B received lost={} out_of_order={} jitter={} ms tcp_jitter={} ms udp_jitter={} ms mss={} mtu={} window={} webrtc={}/{}/{}{}",
                    text(0)?,
                    number(1)?,
                    text(2)?,
                    text(9)?,
                    text(3)?,
                    number(4)?,
                    number(5)?,
                    number(6)?,
                    number(7)?,
                    detail("sent_tcp_packets"),
                    detail("sent_tcp_bytes"),
                    detail("received_tcp_packets"),
                    detail("received_tcp_bytes"),
                    detail("sent_udp_packets"),
                    detail("sent_udp_bytes"),
                    detail("received_udp_packets"),
                    detail("received_udp_bytes"),
                    detail("sent_ip_packets"),
                    detail("sent_ip_bytes"),
                    detail("received_ip_packets"),
                    detail("received_ip_bytes"),
                    detail("lost_packets"),
                    detail("out_of_order_packets"),
                    detail("jitter_millis"),
                    detail("tcp_jitter_millis"),
                    detail("udp_jitter_millis"),
                    detail("tcp_mss"),
                    detail("tcp_mtu"),
                    detail("tcp_window_size"),
                    detail("webrtc_sent_messages"),
                    detail("webrtc_received_messages"),
                    detail("webrtc_invalid_frames"),
                    row.get::<_, Option<String>>(8)?
                        .map(|reason| format!(" ({reason})"))
                        .unwrap_or_default()
                ))
            })
            .map_err(|e| e.to_string())?;
        rows.map(|row| row.map_err(|e| e.to_string())).collect()
    }
    /// Reads a single run's final summary from local SQLite only; per-second
    /// metrics never live here, so this is just the started/completed/result row.
    pub fn show_run(&self, id: u64) -> Result<Option<String>, String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        connection
            .query_row(
                "SELECT started_utc, completed_utc, role, result, sent_bytes, received_bytes, sent_bytes_per_second, received_bytes_per_second, failure_reason FROM runs WHERE id = ?1",
                params![id],
                |row| {
                    Ok(format!(
                        "started {} completed {} role {} result {} sent_bytes={} received_bytes={} up={} bytes/sec down={} bytes/sec{}",
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?
                            .unwrap_or_else(|| "n/a".to_string()),
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<u64>>(4)?.unwrap_or(0),
                        row.get::<_, Option<u64>>(5)?.unwrap_or(0),
                        row.get::<_, Option<u64>>(6)?.unwrap_or(0),
                        row.get::<_, Option<u64>>(7)?.unwrap_or(0),
                        row.get::<_, Option<String>>(8)?
                            .map(|reason| format!(" ({reason})"))
                            .unwrap_or_default()
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())
    }
    /// Everything stored for one run, as label/value rows: when it ran, its
    /// role, transport and result, the totals, the per-protocol packet and byte
    /// counters, loss, jitter, the TCP transport figures and the WebRTC counts.
    pub fn run_detail_rows(&self, id: u64) -> Result<Option<Vec<Vec<String>>>, String> {
        let connection = Connection::open(database_path()).map_err(|e| e.to_string())?;
        let columns = RUN_DETAIL_COLUMNS.join(", ");
        connection
            .query_row(
                &format!(
                    "SELECT started_utc, completed_utc, role, protocol, result, failure_reason, sent_bytes, received_bytes, sent_bytes_per_second, received_bytes_per_second, {columns} FROM runs WHERE id = ?1"
                ),
                params![id],
                |row| {
                    let text = |index: usize| -> rusqlite::Result<String> {
                        Ok(row
                            .get::<_, Option<String>>(index)?
                            .unwrap_or_else(|| "n/a".to_string()))
                    };
                    let number =
                        |index: usize| -> rusqlite::Result<u64> {
                            Ok(row.get::<_, Option<u64>>(index)?.unwrap_or(0))
                        };
                    // The detail columns start after the ten named ones.
                    let detail = |name: &str| -> rusqlite::Result<u64> {
                        let index = RUN_DETAIL_COLUMNS
                            .iter()
                            .position(|column| *column == name)
                            .expect("every detail column is listed");
                        number(10 + index)
                    };
                    let mut rows = vec![
                        vec!["Run".to_string(), id.to_string()],
                        vec!["Started".to_string(), text(0)?],
                        vec!["Completed".to_string(), text(1)?],
                        vec!["Role".to_string(), text(2)?],
                        vec!["Protocol".to_string(), text(3)?],
                        vec!["Result".to_string(), text(4)?],
                        vec!["Failure reason".to_string(), text(5)?],
                        vec!["Sent".to_string(), format!("{} bytes", number(6)?)],
                        vec!["Received".to_string(), format!("{} bytes", number(7)?)],
                        vec![
                            "Bandwidth up".to_string(),
                            format!("{} bytes/sec", number(8)?),
                        ],
                        vec![
                            "Bandwidth down".to_string(),
                            format!("{} bytes/sec", number(9)?),
                        ],
                    ];
                    for (label, sent_packets, sent_bytes, received_packets, received_bytes) in [
                        (
                            "TCP/SCTP",
                            "sent_tcp_packets",
                            "sent_tcp_bytes",
                            "received_tcp_packets",
                            "received_tcp_bytes",
                        ),
                        (
                            "UDP",
                            "sent_udp_packets",
                            "sent_udp_bytes",
                            "received_udp_packets",
                            "received_udp_bytes",
                        ),
                        (
                            "IP",
                            "sent_ip_packets",
                            "sent_ip_bytes",
                            "received_ip_packets",
                            "received_ip_bytes",
                        ),
                    ] {
                        rows.push(vec![
                            label.to_string(),
                            format!(
                                "sent {} packets / {} bytes, received {} packets / {} bytes",
                                detail(sent_packets)?,
                                detail(sent_bytes)?,
                                detail(received_packets)?,
                                detail(received_bytes)?
                            ),
                        ]);
                    }
                    rows.push(vec![
                        "UDP loss".to_string(),
                        format!(
                            "{} lost, {} out of order",
                            detail("lost_packets")?,
                            detail("out_of_order_packets")?
                        ),
                    ]);
                    rows.push(vec![
                        "Jitter".to_string(),
                        format!(
                            "peak {} ms (TCP/SCTP {} ms, UDP {} ms)",
                            detail("jitter_millis")?,
                            detail("tcp_jitter_millis")?,
                            detail("udp_jitter_millis")?
                        ),
                    ]);
                    rows.push(vec![
                        "TCP transport".to_string(),
                        format!(
                            "mss {}, mtu {}, window {}",
                            detail("tcp_mss")?,
                            detail("tcp_mtu")?,
                            detail("tcp_window_size")?
                        ),
                    ]);
                    rows.push(vec![
                        "WebRTC".to_string(),
                        format!(
                            "{} messages sent, {} received, {} invalid frames",
                            detail("webrtc_sent_messages")?,
                            detail("webrtc_received_messages")?,
                            detail("webrtc_invalid_frames")?
                        ),
                    ]);
                    Ok(rows)
                },
            )
            .optional()
            .map_err(|e| e.to_string())
    }
    /// Local SQLite keeps only that the monitor was started or stopped; every
    /// check it performs is written to the external (PostgreSQL) database.
    pub fn record_monitor_status(&self, timestamp_utc: &str, monitor_id: u64, status: &str) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let role = self.role();
        let mut connection = self.connection.lock().unwrap();
        if connection.is_none()
            && let Ok(value) = Connection::open(database_path())
        {
            *connection = Some(value);
        }
        if let Some(connection) = connection.as_ref() {
            let _ = connection.execute_batch(MONITOR_STATUS_TABLE);
            let _ = connection.execute(
                "INSERT INTO monitor_status (timestamp_utc, monitor_id, status, role) VALUES (?1, ?2, ?3, ?4)",
                params![timestamp_utc, monitor_id, status, role],
            );
        }
    }
}

/// The detail columns of the `runs` table, in the order `RunDetail` reports them.
pub const RUN_DETAIL_COLUMNS: [&str; 23] = [
    "sent_tcp_packets",
    "sent_tcp_bytes",
    "received_tcp_packets",
    "received_tcp_bytes",
    "sent_udp_packets",
    "sent_udp_bytes",
    "received_udp_packets",
    "received_udp_bytes",
    "sent_ip_packets",
    "sent_ip_bytes",
    "received_ip_packets",
    "received_ip_bytes",
    "lost_packets",
    "out_of_order_packets",
    "jitter_millis",
    "tcp_jitter_millis",
    "udp_jitter_millis",
    "tcp_mss",
    "tcp_mtu",
    "tcp_window_size",
    "webrtc_sent_messages",
    "webrtc_received_messages",
    "webrtc_invalid_frames",
];

fn run_detail_values(detail: &RunDetail) -> [u64; 23] {
    [
        detail.sent_tcp_packets,
        detail.sent_tcp_bytes,
        detail.received_tcp_packets,
        detail.received_tcp_bytes,
        detail.sent_udp_packets,
        detail.sent_udp_bytes,
        detail.received_udp_packets,
        detail.received_udp_bytes,
        detail.sent_ip_packets,
        detail.sent_ip_bytes,
        detail.received_ip_packets,
        detail.received_ip_bytes,
        detail.lost_packets,
        detail.out_of_order_packets,
        detail.jitter_millis,
        detail.tcp_jitter_millis,
        detail.udp_jitter_millis,
        detail.tcp_mss,
        detail.tcp_mtu,
        detail.tcp_window_size,
        detail.webrtc_sent_messages,
        detail.webrtc_received_messages,
        detail.webrtc_invalid_frames,
    ]
}

const MONITOR_STATUS_TABLE: &str = "CREATE TABLE IF NOT EXISTS monitor_status (timestamp_utc TEXT NOT NULL, monitor_id INTEGER NOT NULL, status TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'unknown')";

pub fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn database_path() -> PathBuf {
    let base = std::env::current_exe()
        .ok()
        .and_then(|path| {
            let parent = path.parent()?;
            if parent.file_name().is_some_and(|name| name == "deps") {
                parent.parent().map(PathBuf::from)
            } else {
                Some(parent.to_path_buf())
            }
        })
        .unwrap_or_else(|| std::env::current_dir().unwrap())
        .join("log");
    let _ = std::fs::create_dir_all(&base);
    base.join("netmark.sqlite")
}

impl Default for SqlState {
    fn default() -> Self {
        Self::new()
    }
}

/// End-of-run reconciliation between the sending and the receiving side: what the
/// client says it put on the wire against what the server says it took off it.
#[derive(Debug, Clone)]
pub struct Debrief {
    pub run_id: u64,
    /// Which side wrote this record: "client" or "server".
    pub role: &'static str,
    pub protocol: String,
    pub sent_packets: u64,
    pub sent_bytes: u64,
    pub received_packets: u64,
    pub received_bytes: u64,
    pub lost_packets: u64,
    pub out_of_order_packets: u64,
}

impl Debrief {
    pub fn matched(&self) -> bool {
        self.mismatch_reason().is_none()
    }
    pub fn mismatch_reason(&self) -> Option<String> {
        let mut reasons = Vec::new();
        if self.sent_packets != self.received_packets {
            reasons.push(format!(
                "{} packets sent but {} received",
                self.sent_packets, self.received_packets
            ));
        }
        if self.sent_bytes != self.received_bytes {
            reasons.push(format!(
                "{} bytes sent but {} received",
                self.sent_bytes, self.received_bytes
            ));
        }
        if self.lost_packets > 0 {
            reasons.push(format!("{} packets lost", self.lost_packets));
        }
        if self.out_of_order_packets > 0 {
            reasons.push(format!(
                "{} packets out of order",
                self.out_of_order_packets
            ));
        }
        (!reasons.is_empty()).then(|| reasons.join("; "))
    }
    pub fn summary(&self) -> String {
        format!(
            "debrief run={} role={} protocol={} sent_packets={} received_packets={} sent_bytes={} received_bytes={} lost={} out_of_order={} result={}",
            self.run_id,
            self.role,
            self.protocol,
            self.sent_packets,
            self.received_packets,
            self.sent_bytes,
            self.received_bytes,
            self.lost_packets,
            self.out_of_order_packets,
            match self.mismatch_reason() {
                None => "match".to_string(),
                Some(reason) => format!("mismatch ({reason})"),
            }
        )
    }
}

pub const DEBRIEF_PORT: u16 = PORT + 1;
const DEBRIEF_TAG: &str = "NETMARK-DEBRIEF/1";
const DEBRIEF_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the responder keeps serving after the traffic run has been stopped.
const DEBRIEF_GRACE: Duration = Duration::from_secs(10);
/// Hard cap on a debrief line read off the network.
const DEBRIEF_MAX_LINE: u64 = 512;

fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .filter_map(|pair| pair.split_once('='))
        .find(|(name, _)| *name == key)
        .map(|(_, value)| value)
}

fn number(line: &str, key: &str) -> u64 {
    field(line, key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn read_debrief_line(stream: &TcpStream) -> io::Result<String> {
    let mut line = String::new();
    io::BufReader::new(stream.take(DEBRIEF_MAX_LINE)).read_line(&mut line)?;
    if !line.starts_with(DEBRIEF_TAG) {
        return Err(io::Error::other("not a netmark debrief message"));
    }
    Ok(line)
}

/// Writes the debrief to netmark.log and to this host's local SQLite database, so
/// client and server each keep their own record of the same run.
pub fn report_debrief(debrief: &Debrief, sql: &SqlState, log_dir: &std::path::Path) {
    if let Ok(mut log) = File::options()
        .create(true)
        .append(true)
        .open(log_dir.join("netmark.log"))
    {
        let _ = writeln!(log, "{} {}", timestamp(), debrief.summary());
    }
    sql.record_debrief(debrief);
}

/// Server side: answers one debrief request with what this host received, and
/// records its own copy of the reconciliation before replying.
pub fn spawn_debrief_responder(
    stopping: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    sql: Arc<SqlState>,
    log_dir: PathBuf,
) {
    thread::spawn(move || {
        let listener = match TcpListener::bind(address_port("0.0.0.0", DEBRIEF_PORT)) {
            Ok(listener) => listener,
            Err(_) => return,
        };
        listener.set_nonblocking(true).ok();
        let mut stopped_at: Option<Instant> = None;
        loop {
            if stopping.load(Ordering::Relaxed)
                && stopped_at.get_or_insert_with(Instant::now).elapsed() >= DEBRIEF_GRACE
            {
                return;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    if serve_debrief(stream, &metrics, &sql, &log_dir).is_ok() {
                        return;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20))
                }
                Err(_) => return,
            }
        }
    });
}

fn serve_debrief(
    stream: TcpStream,
    metrics: &Metrics,
    sql: &SqlState,
    log_dir: &std::path::Path,
) -> io::Result<()> {
    stream.set_read_timeout(Some(DEBRIEF_TIMEOUT)).ok();
    stream.set_write_timeout(Some(DEBRIEF_TIMEOUT)).ok();
    stream.set_nonblocking(false).ok();
    let request = read_debrief_line(&stream)?;
    let run_id = number(&request, "run");
    let protocol = field(&request, "protocol").unwrap_or("tcp").to_string();
    let packet_type = PacketType::parse(&protocol).unwrap_or(PacketType::Tcp);
    let (received_packets, received_bytes) = metrics.run_counts(false, packet_type);
    let (lost, out_of_order) = match packet_type {
        PacketType::Udp | PacketType::Ip => metrics.udp_status(),
        PacketType::Tcp | PacketType::Sctp => (0, 0),
    };
    let debrief = Debrief {
        run_id,
        role: "server",
        protocol: protocol.clone(),
        sent_packets: number(&request, "sent_packets"),
        sent_bytes: number(&request, "sent_bytes"),
        received_packets,
        received_bytes,
        lost_packets: lost,
        out_of_order_packets: out_of_order,
    };
    report_debrief(&debrief, sql, log_dir);
    let mut writer = &stream;
    writeln!(
        writer,
        "{DEBRIEF_TAG} run={run_id} protocol={protocol} received_packets={received_packets} received_bytes={received_bytes} lost={lost} out_of_order={out_of_order}"
    )?;
    writer.flush()
}

/// Client side: tells the server what it sent, asks what arrived, and records the
/// reconciliation locally. Returns the debrief so it can be folded into the run report.
pub fn run_debrief(
    remote: &str,
    run_id: u64,
    packet_type: PacketType,
    metrics: &Metrics,
    sql: &SqlState,
    log_dir: &std::path::Path,
) -> Result<Debrief, String> {
    let (sent_packets, sent_bytes) = metrics.run_counts(true, packet_type);
    let protocol = packet_type.as_str();
    let stream = TcpStream::connect(address_port(remote, DEBRIEF_PORT))
        .map_err(|error| format!("debrief connect failed: {error}"))?;
    stream.set_read_timeout(Some(DEBRIEF_TIMEOUT)).ok();
    stream.set_write_timeout(Some(DEBRIEF_TIMEOUT)).ok();
    let mut writer = &stream;
    writeln!(
        writer,
        "{DEBRIEF_TAG} run={run_id} protocol={protocol} sent_packets={sent_packets} sent_bytes={sent_bytes}"
    )
    .and_then(|()| writer.flush())
    .map_err(|error| format!("debrief send failed: {error}"))?;
    let response = read_debrief_line(&stream).map_err(|error| format!("debrief reply failed: {error}"))?;
    let debrief = Debrief {
        run_id,
        role: "client",
        protocol: protocol.to_string(),
        sent_packets,
        sent_bytes,
        received_packets: number(&response, "received_packets"),
        received_bytes: number(&response, "received_bytes"),
        lost_packets: number(&response, "lost"),
        out_of_order_packets: number(&response, "out_of_order"),
    };
    report_debrief(&debrief, sql, log_dir);
    Ok(debrief)
}

pub fn spawn_server(
    config: Arc<Mutex<Config>>,
    gate: Arc<StartGate>,
    stopping: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    log_dir: PathBuf,
) {
    let http_stopping = Arc::clone(&stopping);
    thread::spawn(move || run_http_server(&http_stopping));
    thread::spawn(move || {
        let config = config.lock().unwrap().clone();
        let log = File::options()
            .create(true)
            .append(true)
            .open(log_dir.join("server.log"))
            .unwrap();
        match config.packet_type {
            PacketType::Tcp => tcp_server(
                &gate,
                &stopping,
                &metrics,
                log,
                config.server_runtime,
                config.tcp_window_size,
                &config.webrtc,
            ),
            PacketType::Sctp => sctp_server(
                &gate,
                &stopping,
                &metrics,
                log,
                config.server_runtime,
                &config.webrtc,
            ),
            PacketType::Udp => udp_server(
                &gate,
                &stopping,
                &metrics,
                log,
                config.server_runtime,
                &config.webrtc,
            ),
            PacketType::Ip => ip_server(
                &gate,
                &stopping,
                &metrics,
                log,
                config.server_runtime,
                &config.webrtc,
            ),
        }
    });
}

fn run_http_server(stopping: &AtomicBool) {
    let listener = match TcpListener::bind("0.0.0.0:8080") {
        Ok(listener) => listener,
        Err(_) => return,
    };
    listener.set_nonblocking(true).ok();
    while !stopping.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request);
                let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: 24\r\nConnection: close\r\n\r\n<html>Hello World</html>";
                let _ = stream.write_all(response);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}
pub fn spawn_client(
    config: Arc<Mutex<Config>>,
    gate: Arc<StartGate>,
    stopping: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    remote: String,
    log_dir: PathBuf,
    client_id: u64,
) {
    thread::spawn(move || {
        gate.wait();
        let config = config.lock().unwrap().clone();
        let log = File::options()
            .create(true)
            .append(true)
            .open(log_dir.join("client.log"))
            .unwrap();
        match config.packet_type {
            PacketType::Tcp => tcp_client(
                config.tcp_bytes_per_second,
                config.tcp_window_size,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.client_jitter_millis,
                client_id,
                &config.webrtc,
            ),
            PacketType::Sctp => sctp_client(
                config.tcp_bytes_per_second,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.client_jitter_millis,
                client_id,
                &config.webrtc,
            ),
            PacketType::Udp => udp_client(
                config.udp_rate,
                config.udp_packet_size,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.server_jitter_millis,
                client_id,
                &config.webrtc,
            ),
            PacketType::Ip => ip_client(
                config.udp_rate,
                config.udp_packet_size,
                &stopping,
                &metrics,
                log,
                &remote,
                config.client_runtime,
                config.server_jitter_millis,
                client_id,
                &config.webrtc,
            ),
        }
    });
}

fn address(remote: &str) -> String {
    format!("{remote}:{PORT}")
}
fn address_port(remote: &str, port: u16) -> String {
    format!("{remote}:{port}")
}
fn expired(started: Instant, runtime: u64) -> bool {
    runtime != 0 && started.elapsed() >= Duration::from_secs(runtime)
}
fn tcp_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
    window_size: u32,
    webrtc: &crate::webrtc::Settings,
) {
    let listener = match TcpListener::bind(address("0.0.0.0")) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("server error: {e}");
            return;
        }
    };
    listener.set_nonblocking(true).ok();
    gate.wait();
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                configure_tcp_socket(&stream, window_size);
                stream.set_nonblocking(true).ok();
                let mut buffer = [0; PACKET_SIZE];
                let mut frame_reader = TcpFrameReader::new(webrtc.enabled, PacketType::Tcp);
                while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            metrics.add_counts(false, PacketType::Tcp, 0, n as u64);
                            frame_reader.feed(&buffer[..n], metrics);
                            writeln!(log, "{} TCP {n} bytes", timestamp()).ok();
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10))
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}

fn sctp_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let listener = match crate::sctp::SctpListener::bind(PORT) {
        Ok(listener) => listener,
        Err(error) => {
            writeln!(log, "{} SCTP server unavailable: {error}", timestamp()).ok();
            eprintln!("server error: SCTP unavailable: {error}");
            return;
        }
    };
    listener.set_nonblocking(true).ok();
    gate.wait();
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match listener.accept() {
            Ok(mut stream) => {
                stream.set_nonblocking(true).ok();
                let mut buffer = [0; PACKET_SIZE];
                let mut frame_reader = TcpFrameReader::new(webrtc.enabled, PacketType::Sctp);
                while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            metrics.add_counts(false, PacketType::Sctp, 0, n as u64);
                            frame_reader.feed(&buffer[..n], metrics);
                            writeln!(log, "{} SCTP {n} bytes", timestamp()).ok();
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(10)),
            Err(_) => break,
        }
    }
}
fn udp_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let socket = match UdpSocket::bind(address("0.0.0.0")) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("server error: {e}");
            return;
        }
    };
    socket.set_nonblocking(true).ok();
    gate.wait();
    let started = Instant::now();
    let mut buffer = [0; PACKET_SIZE];
    let mut expected_sequences: HashMap<u64, Option<u64>> = HashMap::new();
    let mut channels = crate::webrtc::Receiver::default();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match socket.recv_from(&mut buffer) {
            Ok((n, _)) => {
                receive_datagram(
                    &buffer[..n],
                    PacketType::Udp,
                    metrics,
                    &mut expected_sequences,
                    &mut channels,
                    webrtc,
                    &mut log,
                );
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(_) => break,
        }
    }
}

/// The raw IP server. Unlike TCP and UDP there is no port to bind: every IPv4
/// packet carrying netmark's protocol number arrives here.
fn ip_server(
    gate: &StartGate,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    runtime: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let socket = match crate::rawip::RawIpSocket::open() {
        Ok(socket) => socket,
        Err(error) => {
            writeln!(log, "{} IP server unavailable: {error}", timestamp()).ok();
            eprintln!("server error: {error}");
            return;
        }
    };
    gate.wait();
    let started = Instant::now();
    let mut buffer = [0; PACKET_SIZE];
    let mut expected_sequences: HashMap<u64, Option<u64>> = HashMap::new();
    let mut channels = crate::webrtc::Receiver::default();
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        match socket.recv(&mut buffer) {
            Ok(n) => {
                receive_datagram(
                    &buffer[..n],
                    PacketType::Ip,
                    metrics,
                    &mut expected_sequences,
                    &mut channels,
                    webrtc,
                    &mut log,
                );
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1))
            }
            Err(_) => break,
        }
    }
}

/// Shared receive accounting for the two datagram transports.
fn receive_datagram(
    packet: &[u8],
    protocol: PacketType,
    metrics: &Arc<Metrics>,
    expected_sequences: &mut HashMap<u64, Option<u64>>,
    channels: &mut crate::webrtc::Receiver,
    webrtc: &crate::webrtc::Settings,
    log: &mut File,
) {
    let n = packet.len();
    metrics.add(false, protocol, n);
    if n >= UDP_HEADER_LEN {
        let client_id = u64::from_be_bytes(packet[16..24].try_into().unwrap());
        metrics.udp_sequence(
            expected_sequences.entry(client_id).or_default(),
            u64::from_be_bytes(packet[..8].try_into().unwrap()),
        );
        metrics.udp_timestamp_jitter(i64::from_be_bytes(packet[8..16].try_into().unwrap()));
        if webrtc.enabled {
            let accepted = channels.accept(&packet[UDP_HEADER_LEN..]).is_some();
            metrics.record_webrtc_received(accepted);
        }
    }
    writeln!(log, "{} {} {n} bytes", timestamp(), protocol.as_str().to_uppercase()).ok();
}

fn configure_tcp_socket(stream: &TcpStream, window_size: u32) {
    if window_size == 0 {
        return;
    }
    let value = window_size.min(i32::MAX as u32) as libc::c_int;
    // SAFETY: the stream's descriptor and `value` pointer are valid.
    unsafe {
        let _ = libc::setsockopt(
            stream.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF,
            (&raw const value).cast(), std::mem::size_of_val(&value) as libc::socklen_t,
        );
        let _ = libc::setsockopt(
            stream.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF,
            (&raw const value).cast(), std::mem::size_of_val(&value) as libc::socklen_t,
        );
    }
}

fn socket_option(stream: &TcpStream, level: libc::c_int, option: libc::c_int) -> u64 {
    let mut value: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    // SAFETY: the stream descriptor, output pointer and output length are valid.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(), level, option, (&raw mut value).cast(), &raw mut length,
        )
    };
    if result == 0 && value > 0 {
        value as u64
    } else {
        0
    }
}

#[allow(clippy::too_many_arguments)]
fn tcp_client(
    bytes_per_second: u64,
    window_size: u32,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let started = Instant::now();
    let mut stream = loop {
        if stopping.load(Ordering::Relaxed) || expired(started, runtime) {
            return;
        }
        match TcpStream::connect(address(remote)) {
            Ok(v) => break v,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    };
    configure_tcp_socket(&stream, window_size);
    metrics.record_tcp_transport(
        socket_option(&stream, libc::IPPROTO_TCP, libc::TCP_MAXSEG),
        socket_option(&stream, libc::IPPROTO_IP, libc::IP_MTU),
        socket_option(&stream, libc::SOL_SOCKET, libc::SO_SNDBUF),
    );
    send_stream_packets(
        bytes_per_second,
        PacketType::Tcp,
        stopping,
        metrics,
        &mut log,
        runtime,
        jitter_millis,
        client_id,
        webrtc,
        |p| stream.write_all(p),
    );
}

#[allow(clippy::too_many_arguments)]
fn sctp_client(
    bytes_per_second: u64,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let destination = match crate::sctp::resolve(remote) {
        Ok(destination) => destination,
        Err(error) => { writeln!(log, "{} SCTP client unavailable: {error}", timestamp()).ok(); return; }
    };
    let started = Instant::now();
    let mut stream = loop {
        if stopping.load(Ordering::Relaxed) || expired(started, runtime) { return; }
        match crate::sctp::SctpStream::connect(destination, PORT) {
            Ok(stream) => break stream,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    };
    send_stream_packets(bytes_per_second, PacketType::Sctp, stopping, metrics, &mut log, runtime, jitter_millis, client_id, webrtc, |packet| stream.write_all(packet));
    // `stop` ends the association explicitly so the receiving side sees the end
    // of the run at once instead of waiting for its own runtime to expire.
    match stream.shutdown() {
        Ok(()) => { writeln!(log, "{} SCTP association closed", timestamp()).ok(); }
        Err(error) => { writeln!(log, "{} SCTP shutdown failed: {error}", timestamp()).ok(); }
    }
}
#[allow(clippy::too_many_arguments)]
fn udp_client(
    udp_rate: u64,
    packet_size: usize,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(v) => v,
        Err(_) => return,
    };
    if socket.connect(address(remote)).is_err() {
        return;
    }
    send_datagrams(
        udp_rate,
        packet_size,
        PacketType::Udp,
        stopping,
        metrics,
        &mut log,
        runtime,
        jitter_millis,
        client_id,
        webrtc,
        |p| socket.send(p).map(|_| ()),
    );
}
#[allow(clippy::too_many_arguments)]
fn ip_client(
    udp_rate: u64,
    packet_size: usize,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    mut log: File,
    remote: &str,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
) {
    let destination = match crate::rawip::resolve(remote) {
        Ok(destination) => destination,
        Err(error) => {
            writeln!(log, "{} IP client unavailable: {error}", timestamp()).ok();
            eprintln!("client error: {error}");
            return;
        }
    };
    let socket = match crate::rawip::RawIpSocket::open() {
        Ok(socket) => socket,
        Err(error) => {
            writeln!(log, "{} IP client unavailable: {error}", timestamp()).ok();
            eprintln!("client error: {error}");
            return;
        }
    };
    send_datagrams(
        udp_rate,
        packet_size,
        PacketType::Ip,
        stopping,
        metrics,
        &mut log,
        runtime,
        jitter_millis,
        client_id,
        webrtc,
        |p| socket.send_to(p, destination).map(|_| ()),
    );
}
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn send_tcp_packets<F>(
    bytes_per_second: u64,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    log: &mut File,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
    send: F,
) where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    send_stream_packets(bytes_per_second, PacketType::Tcp, stopping, metrics, log, runtime, jitter_millis, client_id, webrtc, send);
}

#[allow(clippy::too_many_arguments)]
fn send_stream_packets<F>(
    bytes_per_second: u64,
    protocol: PacketType,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    log: &mut File,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
    mut send: F,
) where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let packet_size = bytes_per_second.min(PACKET_SIZE as u64).max(1) as usize;
    let interval = Duration::from_secs_f64(packet_size as f64 / bytes_per_second.max(1) as f64);
    let started = Instant::now();
    let mut previous_send = started;
    let mut channels = crate::webrtc::Sender::new(webrtc.clone());
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        let (packet, frames, messages) = build_tcp_frame(packet_size, webrtc, &mut channels);
        if send(&packet).is_err() {
            return;
        }
        metrics.add_counts(true, protocol, frames, packet.len() as u64);
        for _ in 0..messages {
            metrics.record_webrtc_sent();
        }
        let now = Instant::now();
        metrics.record_jitter(
            protocol,
            now.duration_since(previous_send).abs_diff(interval),
        );
        previous_send = now;
        writeln!(log, "{} {} {} bytes client={client_id}", timestamp(), protocol.as_str().to_uppercase(), packet.len()).ok();
        thread::sleep(jittered_delay(interval, jitter_millis));
    }
}
/// Sends the datagram transports (UDP and raw IP), which share a packet layout.
#[allow(clippy::too_many_arguments)]
fn send_datagrams<F>(
    udp_rate: u64,
    packet_size: usize,
    protocol: PacketType,
    stopping: &AtomicBool,
    metrics: &Arc<Metrics>,
    log: &mut File,
    runtime: u64,
    jitter_millis: u64,
    client_id: u64,
    webrtc: &crate::webrtc::Settings,
    mut send: F,
) where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let minimum = if webrtc.enabled {
        UDP_HEADER_LEN + crate::webrtc::HEADER_LEN
    } else {
        UDP_HEADER_LEN
    };
    let packet_size = packet_size.max(minimum);
    let interval = Duration::from_secs_f64(1.0 / udp_rate.max(1) as f64);
    let mut sequence = 0u64;
    let started = Instant::now();
    let mut previous_send = started;
    let mut channels = crate::webrtc::Sender::new(webrtc.clone());
    while !stopping.load(Ordering::Relaxed) && !expired(started, runtime) {
        let mut packet = vec![0u8; packet_size];
        packet[..8].copy_from_slice(&sequence.to_be_bytes());
        packet[8..16].copy_from_slice(&Utc::now().timestamp_millis().to_be_bytes());
        packet[16..24].copy_from_slice(&client_id.to_be_bytes());
        if webrtc.enabled {
            let payload_len = (packet_size - minimum) as u16;
            let header = channels.next(payload_len).encode();
            packet[UDP_HEADER_LEN..minimum].copy_from_slice(&header);
        }
        if send(&packet).is_err() {
            return;
        }
        metrics.add(true, protocol, packet.len());
        if webrtc.enabled {
            metrics.record_webrtc_sent();
        }
        let now = Instant::now();
        metrics.record_jitter(
            protocol,
            now.duration_since(previous_send).abs_diff(interval),
        );
        previous_send = now;
        writeln!(
            log,
            "{} {} {} bytes client={client_id}",
            timestamp(),
            protocol.as_str().to_uppercase(),
            packet.len()
        )
        .ok();
        sequence += 1;
        thread::sleep(jittered_delay(interval, jitter_millis));
    }
}

/// Builds a TCP payload of `payload_len` bytes with a send timestamp embedded
/// every `TCP_TIMESTAMP_CHUNK` bytes, so the receiver can measure jitter on a
/// stream. When the WebRTC layer is on, each chunk also opens with a data-channel
/// header. Returns the buffer, the number of frames and the number of
/// data-channel messages it contains.
fn build_tcp_frame(
    payload_len: usize,
    webrtc: &crate::webrtc::Settings,
    channels: &mut crate::webrtc::Sender,
) -> (Vec<u8>, u64, u64) {
    let mut buffer = Vec::with_capacity(payload_len + payload_len.div_ceil(TCP_TIMESTAMP_CHUNK) * TCP_HEADER_LEN);
    let mut remaining = payload_len;
    let mut frames = 0;
    let mut messages = 0;
    while remaining > 0 {
        let chunk = remaining.min(TCP_TIMESTAMP_CHUNK);
        buffer.extend_from_slice(&Utc::now().timestamp_millis().to_be_bytes());
        buffer.extend_from_slice(&(chunk as u16).to_be_bytes());
        let carries_channel = webrtc.enabled && chunk >= crate::webrtc::HEADER_LEN;
        if carries_channel {
            let payload = (chunk - crate::webrtc::HEADER_LEN) as u16;
            buffer.extend_from_slice(&channels.next(payload).encode());
            buffer.extend(std::iter::repeat_n(0u8, chunk - crate::webrtc::HEADER_LEN));
            messages += 1;
        } else {
            buffer.extend(std::iter::repeat_n(0u8, chunk));
        }
        remaining -= chunk;
        frames += 1;
    }
    (buffer, frames, messages)
}

enum TcpFrameState {
    Header(Vec<u8>),
    Payload { remaining: usize, channel: Vec<u8> },
}

/// Parses the timestamp-framed TCP byte stream produced by `build_tcp_frame`,
/// tolerating frames split arbitrarily across reads.
struct TcpFrameReader {
    state: TcpFrameState,
    webrtc: bool,
    protocol: PacketType,
    channels: crate::webrtc::Receiver,
}
impl TcpFrameReader {
    fn new(webrtc: bool, protocol: PacketType) -> Self {
        Self {
            state: TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN)),
            webrtc,
            protocol,
            channels: crate::webrtc::Receiver::default(),
        }
    }
    fn feed(&mut self, data: &[u8], metrics: &Metrics) {
        let mut offset = 0;
        while offset < data.len() {
            match &mut self.state {
                TcpFrameState::Header(buffer) => {
                    let need = TCP_HEADER_LEN - buffer.len();
                    let take = need.min(data.len() - offset);
                    buffer.extend_from_slice(&data[offset..offset + take]);
                    offset += take;
                    if buffer.len() == TCP_HEADER_LEN {
                        let timestamp_ms = i64::from_be_bytes(buffer[..8].try_into().unwrap());
                        let length = u16::from_be_bytes(buffer[8..10].try_into().unwrap()) as usize;
                        metrics.tcp_timestamp_jitter(timestamp_ms);
                        metrics.add_counts(false, self.protocol, 1, 0);
                        self.state = if length == 0 {
                            TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN))
                        } else {
                            TcpFrameState::Payload {
                                remaining: length,
                                channel: Vec::new(),
                            }
                        };
                    }
                }
                TcpFrameState::Payload { remaining, channel } => {
                    let take = (*remaining).min(data.len() - offset);
                    // Only the head of each payload is buffered, just enough to
                    // read the data-channel header out of the stream.
                    if self.webrtc && channel.len() < crate::webrtc::HEADER_LEN {
                        let wanted = (crate::webrtc::HEADER_LEN - channel.len()).min(take);
                        channel.extend_from_slice(&data[offset..offset + wanted]);
                        if channel.len() == crate::webrtc::HEADER_LEN {
                            metrics.record_webrtc_received(self.channels.accept(channel).is_some());
                        }
                    }
                    offset += take;
                    *remaining -= take;
                    if *remaining == 0 {
                        self.state = TcpFrameState::Header(Vec::with_capacity(TCP_HEADER_LEN));
                    }
                }
            }
        }
    }
}

fn jittered_delay(base: Duration, jitter_millis: u64) -> Duration {
    if jitter_millis == 0 {
        return base;
    }
    static RANDOM_STATE: RandomState = RandomState::new(0x9e3779b97f4a7c15);
    let range = jitter_millis.saturating_mul(2).saturating_add(1);
    let state = RANDOM_STATE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.wrapping_mul(6364136223846793005).wrapping_add(1))
        })
        .unwrap_or(0);
    let offset = (state % range) as i64 - jitter_millis as i64;
    if offset.is_negative() {
        base.saturating_sub(Duration::from_millis(offset.unsigned_abs()))
    } else {
        base.saturating_add(Duration::from_millis(offset as u64))
    }
}
