//! A raw IPv4 transport that sits underneath TCP and UDP: netmark's payload is
//! carried directly in IP packets with no transport header at all.
//!
//! It uses IP protocol number 253, reserved for experimentation by RFC 3692.
//! Opening a raw socket needs `CAP_NET_RAW`, so this transport is only available
//! to root or to a binary granted that capability; the error says so plainly.

use std::io;
use std::mem;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

/// RFC 3692 experimentation protocol number.
pub const PROTOCOL: i32 = 253;

/// Maximum IPv4 header length, which the kernel prepends to received packets.
const MAX_IP_HEADER: usize = 60;

pub struct RawIpSocket {
    fd: OwnedFd,
}

impl RawIpSocket {
    /// Opens a non-blocking raw IPv4 socket for [`PROTOCOL`].
    pub fn open() -> io::Result<Self> {
        // SAFETY: plain syscall with constant arguments; the returned descriptor
        // is immediately handed to OwnedFd, which closes it.
        let fd: RawFd = unsafe {
            libc::socket(
                libc::AF_INET,
                libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                PROTOCOL,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            return Err(if error.raw_os_error() == Some(libc::EPERM) {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "raw IP needs CAP_NET_RAW; run netmark as root or grant the capability",
                )
            } else {
                error
            });
        }
        // SAFETY: fd is a fresh, valid, owned descriptor.
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    pub fn send_to(&self, payload: &[u8], destination: Ipv4Addr) -> io::Result<usize> {
        let address = socket_address(destination);
        // SAFETY: `address` is a fully initialised sockaddr_in and the length
        // matches its type; `payload` is a valid slice for its own length.
        let sent = unsafe {
            libc::sendto(
                self.fd.as_raw_fd(),
                payload.as_ptr().cast(),
                payload.len(),
                0,
                (&raw const address).cast(),
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(sent as usize)
    }

    /// Receives one packet and strips the IPv4 header the kernel prepends,
    /// returning the number of payload bytes written into `payload`.
    pub fn recv(&self, payload: &mut [u8]) -> io::Result<usize> {
        let mut buffer = vec![0u8; payload.len() + MAX_IP_HEADER];
        // SAFETY: `buffer` is a valid, owned allocation of the length passed in.
        let read = unsafe {
            libc::recv(
                self.fd.as_raw_fd(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
            )
        };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        let read = read as usize;
        let header_len = header_length(&buffer[..read]).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated IPv4 header")
        })?;
        let body = &buffer[header_len..read];
        let copied = body.len().min(payload.len());
        payload[..copied].copy_from_slice(&body[..copied]);
        Ok(copied)
    }
}

impl AsRawFd for RawIpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl IntoRawFd for RawIpSocket {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}

/// IPv4 header length in bytes, from the IHL field.
fn header_length(packet: &[u8]) -> Option<usize> {
    let length = usize::from(packet.first()? & 0x0f) * 4;
    (length >= 20 && length <= packet.len()).then_some(length)
}

fn socket_address(destination: Ipv4Addr) -> libc::sockaddr_in {
    // SAFETY: sockaddr_in is plain old data, so an all-zero value is valid.
    let mut address: libc::sockaddr_in = unsafe { mem::zeroed() };
    address.sin_family = libc::AF_INET as libc::sa_family_t;
    address.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes(destination.octets()),
    };
    address
}

/// Resolves the client's `remote` setting to an IPv4 address; raw IP has no ports.
pub fn resolve(remote: &str) -> Result<Ipv4Addr, String> {
    let host = remote.split(':').next().unwrap_or(remote);
    host.parse::<Ipv4Addr>()
        .map_err(|_| format!("raw IP needs an IPv4 address, got \"{remote}\""))
}
