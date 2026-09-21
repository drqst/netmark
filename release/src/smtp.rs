//! Connectivity check against the configured SMTP server. netmark never sends
//! mail from here; it only verifies that a server is reachable and speaking SMTP.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

pub const DEFAULT_PORT: u16 = 25;
const TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of one SMTP reachability check.
#[derive(Debug, Clone)]
pub struct SmtpCheck {
    pub target: String,
    pub greeting: String,
    pub elapsed_millis: u128,
}

/// Splits `host`, `host:port` or `[v6]:port` into an address usable for connecting.
fn resolve(server: &str) -> Result<(String, std::net::SocketAddr), String> {
    let server = server.trim();
    if server.is_empty() {
        return Err("no SMTP server configured".to_string());
    }
    let target = if server.rsplit(':').next().is_some_and(|port| {
        !port.is_empty() && port.chars().all(|character| character.is_ascii_digit())
    }) {
        server.to_string()
    } else {
        format!("{server}:{DEFAULT_PORT}")
    };
    let address = target
        .to_socket_addrs()
        .map_err(|error| format!("cannot resolve {target}: {error}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {target}"))?;
    Ok((target, address))
}

fn read_reply(reader: &mut BufReader<&TcpStream>) -> Result<String, String> {
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|error| format!("read error: {error}"))?;
        if read == 0 {
            return Err("server closed the connection".to_string());
        }
        let trimmed = line.trim_end().to_string();
        // Multi-line replies use "250-" for continuations and "250 " for the last line.
        if trimmed.len() < 4 || trimmed.as_bytes()[3] != b'-' {
            return Ok(trimmed);
        }
    }
}

/// Connects to `server`, reads the banner and completes an EHLO/QUIT exchange.
pub fn check(server: &str) -> Result<SmtpCheck, String> {
    let (target, address) = resolve(server)?;
    let started = Instant::now();
    let stream = TcpStream::connect_timeout(&address, TIMEOUT)
        .map_err(|error| format!("cannot connect to {target}: {error}"))?;
    stream.set_read_timeout(Some(TIMEOUT)).ok();
    stream.set_write_timeout(Some(TIMEOUT)).ok();
    let mut reader = BufReader::new(&stream);
    let greeting = read_reply(&mut reader)?;
    if !greeting.starts_with("220") {
        return Err(format!("{target} did not send an SMTP greeting: {greeting}"));
    }
    let mut writer = &stream;
    writer
        .write_all(b"EHLO netmark\r\n")
        .map_err(|error| format!("write error: {error}"))?;
    let reply = read_reply(&mut reader)?;
    if !reply.starts_with("250") {
        return Err(format!("{target} rejected EHLO: {reply}"));
    }
    let _ = writer.write_all(b"QUIT\r\n");
    Ok(SmtpCheck {
        target,
        greeting,
        elapsed_millis: started.elapsed().as_millis(),
    })
}
