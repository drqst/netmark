//! Minimal SCTP one-to-one socket support built on the platform socket API.
//!
//! SCTP must be enabled in the host kernel. When it is unavailable, opening a
//! socket returns the kernel error instead of silently falling back to TCP.

use std::io;
use std::mem;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// IANA-assigned SCTP IP protocol number.
const IPPROTO_SCTP: i32 = 132;

pub struct SctpListener(OwnedFd);
pub struct SctpStream(OwnedFd);

impl SctpListener {
    pub fn bind(port: u16) -> io::Result<Self> {
        let fd = socket()?;
        let address = sockaddr(Ipv4Addr::UNSPECIFIED, port);
        // SAFETY: fd and sockaddr are valid for the supplied size.
        let result = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&raw const address).cast(),
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is a valid SCTP socket.
        if unsafe { libc::listen(fd.as_raw_fd(), 128) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(fd))
    }

    pub fn set_nonblocking(&self, enabled: bool) -> io::Result<()> {
        set_nonblocking(self.0.as_raw_fd(), enabled)
    }

    pub fn accept(&self) -> io::Result<SctpStream> {
        // SAFETY: null peer-address pointers are explicitly supported by accept.
        let fd = unsafe { libc::accept4(self.0.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_CLOEXEC) };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: accept4 returned a fresh owned descriptor.
            Ok(SctpStream(unsafe { OwnedFd::from_raw_fd(fd) }))
        }
    }
}

impl SctpStream {
    pub fn connect(destination: Ipv4Addr, port: u16) -> io::Result<Self> {
        let fd = socket()?;
        let address = sockaddr(destination, port);
        // SAFETY: fd and sockaddr are valid for the supplied size.
        let result = unsafe {
            libc::connect(
                fd.as_raw_fd(),
                (&raw const address).cast(),
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(fd))
        }
    }

    pub fn set_nonblocking(&self, enabled: bool) -> io::Result<()> {
        set_nonblocking(self.0.as_raw_fd(), enabled)
    }

    /// Ends the association in both directions, so `stop` closes the run down
    /// cleanly instead of leaving the peer waiting for more data.
    pub fn shutdown(&self) -> io::Result<()> {
        // SAFETY: fd is a valid connected SCTP socket.
        if unsafe { libc::shutdown(self.0.as_raw_fd(), libc::SHUT_RDWR) } < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl io::Read for SctpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        // SAFETY: the mutable slice is valid for its length.
        let read = unsafe { libc::recv(self.0.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        if read < 0 { Err(io::Error::last_os_error()) } else { Ok(read as usize) }
    }
}

impl io::Write for SctpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        // SAFETY: the slice is valid for its length.
        let written = unsafe { libc::send(self.0.as_raw_fd(), buffer.as_ptr().cast(), buffer.len(), 0) };
        if written < 0 { Err(io::Error::last_os_error()) } else { Ok(written as usize) }
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// Whether the host kernel can open an SCTP socket, so `status` can say why an
/// SCTP run would fail before one is started.
pub fn availability() -> Result<(), String> {
    socket().map(|_| ()).map_err(|error| error.to_string())
}

fn socket() -> io::Result<OwnedFd> {
    // SAFETY: syscall arguments are constants; ownership transfers immediately.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, IPPROTO_SCTP) };
    if fd < 0 { Err(io::Error::last_os_error()) } else { Ok(unsafe { OwnedFd::from_raw_fd(fd) }) }
}

fn set_nonblocking(fd: RawFd, enabled: bool) -> io::Result<()> {
    // SAFETY: fcntl is called with a valid descriptor and flags argument.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 { return Err(io::Error::last_os_error()); }
    let flags = if enabled { flags | libc::O_NONBLOCK } else { flags & !libc::O_NONBLOCK };
    // SAFETY: fcntl is called with a valid descriptor and flags argument.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags) } < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

fn sockaddr(address: Ipv4Addr, port: u16) -> libc::sockaddr_in {
    // SAFETY: sockaddr_in is plain old data, so zero initialization is valid.
    let mut result: libc::sockaddr_in = unsafe { mem::zeroed() };
    result.sin_family = libc::AF_INET as libc::sa_family_t;
    result.sin_port = port.to_be();
    result.sin_addr = libc::in_addr { s_addr: u32::from_ne_bytes(address.octets()) };
    result
}

pub fn resolve(remote: &str) -> Result<Ipv4Addr, String> {
    let host = remote.split(':').next().unwrap_or(remote);
    host.parse::<Ipv4Addr>().map_err(|_| format!("SCTP needs an IPv4 address, got \"{remote}\""))
}
