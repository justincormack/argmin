// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Portable absolute-deadline I/O for connected sockets.
//!
//! `SO_RCVTIMEO` and `SO_SNDTIMEO` are not a portable way to enforce an
//! end-to-end deadline. In particular, some Darwin AF_UNIX sockets reject
//! timeout updates with `EINVAL`, and resetting a relative socket timeout for
//! every syscall permits a trickling peer to extend an operation indefinitely.
//! This module waits for readiness with `poll(2)` against one absolute
//! deadline before every read and write. A `Read` or `Write` call then performs
//! at most the work made ready by that wait; callers such as `read_exact` and
//! `write_all` re-enter the adapter for every partial syscall. Flush is tried
//! first because it is a no-op for ordinary sockets, and waits for writable
//! readiness only when a layered transport reports `WouldBlock`.

use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

/// A connected stream that can wait for read and write readiness until an
/// absolute deadline.
///
/// Implementations must not convert the deadline into a fresh per-operation
/// budget. This trait is public so process-level transports can use the same
/// deadline semantics and test doubles can deterministically model trickled
/// I/O.
pub trait DeadlineTransport: Read + Write {
    /// Configures the underlying stream so one syscall cannot block after the
    /// readiness wait. Implementations must preserve this mode for the
    /// lifetime of the adapter.
    fn prepare_deadline_io(&self) -> io::Result<()>;

    fn wait_readable_until(
        &self,
        deadline: Instant,
        timeout_message: &'static str,
    ) -> io::Result<()>;

    fn wait_writable_until(
        &self,
        deadline: Instant,
        timeout_message: &'static str,
    ) -> io::Result<()>;
}

impl<T: DeadlineTransport + ?Sized> DeadlineTransport for &mut T {
    fn prepare_deadline_io(&self) -> io::Result<()> {
        (**self).prepare_deadline_io()
    }

    fn wait_readable_until(
        &self,
        deadline: Instant,
        timeout_message: &'static str,
    ) -> io::Result<()> {
        (**self).wait_readable_until(deadline, timeout_message)
    }

    fn wait_writable_until(
        &self,
        deadline: Instant,
        timeout_message: &'static str,
    ) -> io::Result<()> {
        (**self).wait_writable_until(deadline, timeout_message)
    }
}

fn timeout_error(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::TimedOut, message)
}

fn remaining_poll_timeout_ms(deadline: Instant, timeout_message: &'static str) -> io::Result<i32> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(timeout_error(timeout_message));
    }
    let timeout_ms = remaining
        .as_nanos()
        .div_ceil(Duration::from_millis(1).as_nanos())
        .min(i32::MAX as u128);
    Ok(i32::try_from(timeout_ms).expect("poll timeout was clamped to i32::MAX"))
}

fn wait_for_fd_until(
    fd: i32,
    events: i16,
    deadline: Instant,
    timeout_message: &'static str,
) -> io::Result<()> {
    loop {
        let timeout_ms = remaining_poll_timeout_ms(deadline, timeout_message)?;
        let mut poll_fd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for the duration of
        // the call, and poll does not retain the pointer.
        let result = unsafe { libc::poll(&raw mut poll_fd, 1, timeout_ms) };
        if result == 0 {
            return Err(timeout_error(timeout_message));
        }
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if Instant::now() >= deadline {
            return Err(timeout_error(timeout_message));
        }
        return Ok(());
    }
}

macro_rules! impl_deadline_transport_for_socket {
    ($socket:ty) => {
        impl DeadlineTransport for $socket {
            fn prepare_deadline_io(&self) -> io::Result<()> {
                self.set_nonblocking(true)
            }

            fn wait_readable_until(
                &self,
                deadline: Instant,
                timeout_message: &'static str,
            ) -> io::Result<()> {
                wait_for_fd_until(self.as_raw_fd(), libc::POLLIN, deadline, timeout_message)
            }

            fn wait_writable_until(
                &self,
                deadline: Instant,
                timeout_message: &'static str,
            ) -> io::Result<()> {
                wait_for_fd_until(self.as_raw_fd(), libc::POLLOUT, deadline, timeout_message)
            }
        }
    };
}

impl_deadline_transport_for_socket!(UnixStream);
impl_deadline_transport_for_socket!(TcpStream);

/// A stream adapter that applies one absolute deadline to all framed I/O.
pub struct DeadlineStream<Stream> {
    stream: Stream,
    deadline: Instant,
    timeout_message: &'static str,
}

impl<Stream> DeadlineStream<Stream> {
    pub fn new(
        stream: Stream,
        deadline: Instant,
        timeout_message: &'static str,
    ) -> io::Result<DeadlineStream<Stream>>
    where
        Stream: DeadlineTransport,
    {
        stream.prepare_deadline_io()?;
        Ok(Self {
            stream,
            deadline,
            timeout_message,
        })
    }

    pub fn get_ref(&self) -> &Stream {
        &self.stream
    }

    pub fn get_mut(&mut self) -> &mut Stream {
        &mut self.stream
    }

    /// Starts a new explicitly bounded protocol phase.
    ///
    /// This must not be used to extend one read or write phase after partial
    /// progress. It exists for protocols whose bounded application operation
    /// sits between independently bounded request and response I/O phases.
    pub fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = deadline;
    }

    pub fn into_inner(self) -> Stream {
        self.stream
    }
}

impl<Stream: DeadlineTransport> Read for DeadlineStream<Stream> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            self.stream
                .wait_readable_until(self.deadline, self.timeout_message)?;
            match self.stream.read(buffer) {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                result => return result,
            }
        }
    }
}

impl<Stream: DeadlineTransport> Write for DeadlineStream<Stream> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            self.stream
                .wait_writable_until(self.deadline, self.timeout_message)?;
            match self.stream.write(buffer) {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            if Instant::now() >= self.deadline {
                return Err(timeout_error(self.timeout_message));
            }
            match self.stream.flush() {
                Err(error) if error.kind() == ErrorKind::WouldBlock => self
                    .stream
                    .wait_writable_until(self.deadline, self.timeout_message)?,
                result => return result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct FlushTransport {
        waits: Cell<usize>,
        flush_calls: Cell<usize>,
        would_block_once: Cell<bool>,
    }

    impl FlushTransport {
        fn new(would_block_once: bool) -> Self {
            Self {
                waits: Cell::new(0),
                flush_calls: Cell::new(0),
                would_block_once: Cell::new(would_block_once),
            }
        }
    }

    impl Read for FlushTransport {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for FlushTransport {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls.set(self.flush_calls.get() + 1);
            if self.would_block_once.replace(false) {
                Err(io::Error::from(ErrorKind::WouldBlock))
            } else {
                Ok(())
            }
        }
    }

    impl DeadlineTransport for FlushTransport {
        fn prepare_deadline_io(&self) -> io::Result<()> {
            Ok(())
        }

        fn wait_readable_until(
            &self,
            _deadline: Instant,
            _timeout_message: &'static str,
        ) -> io::Result<()> {
            Ok(())
        }

        fn wait_writable_until(
            &self,
            _deadline: Instant,
            _timeout_message: &'static str,
        ) -> io::Result<()> {
            self.waits.set(self.waits.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn deadline_stream_flushes_before_waiting_for_writable_readiness() {
        let transport = FlushTransport::new(false);
        let mut stream = DeadlineStream::new(
            transport,
            Instant::now() + Duration::from_secs(1),
            "test deadline expired",
        )
        .unwrap();

        stream.flush().unwrap();

        assert_eq!(stream.get_ref().flush_calls.get(), 1);
        assert_eq!(stream.get_ref().waits.get(), 0);
    }

    #[test]
    fn deadline_stream_waits_after_flush_reports_would_block() {
        let transport = FlushTransport::new(true);
        let mut stream = DeadlineStream::new(
            transport,
            Instant::now() + Duration::from_secs(1),
            "test deadline expired",
        )
        .unwrap();

        stream.flush().unwrap();

        assert_eq!(stream.get_ref().flush_calls.get(), 2);
        assert_eq!(stream.get_ref().waits.get(), 1);
    }

    #[test]
    fn deadline_stream_does_not_flush_after_deadline() {
        let transport = FlushTransport::new(false);
        let mut stream = DeadlineStream::new(
            transport,
            Instant::now() - Duration::from_millis(1),
            "test deadline expired",
        )
        .unwrap();

        let error = stream.flush().unwrap_err();

        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert_eq!(stream.get_ref().flush_calls.get(), 0);
        assert_eq!(stream.get_ref().waits.get(), 0);
    }

    #[test]
    fn unix_deadline_stream_does_not_modify_socket_timeouts() {
        let (mut sender, receiver) = UnixStream::pair().unwrap();
        sender.write_all(b"x").unwrap();
        assert_eq!(receiver.read_timeout().unwrap(), None);
        assert_eq!(receiver.write_timeout().unwrap(), None);

        let mut stream = DeadlineStream::new(
            receiver,
            Instant::now() + Duration::from_secs(1),
            "test deadline expired",
        )
        .unwrap();
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).unwrap();

        assert_eq!(byte, *b"x");
        assert_eq!(stream.get_ref().read_timeout().unwrap(), None);
        assert_eq!(stream.get_ref().write_timeout().unwrap(), None);
    }

    #[test]
    fn unix_deadline_stream_rejects_expired_io() {
        let (_sender, receiver) = UnixStream::pair().unwrap();
        let mut stream = DeadlineStream::new(
            receiver,
            Instant::now() - Duration::from_millis(1),
            "test deadline expired",
        )
        .unwrap();
        let error = stream.read(&mut [0_u8; 1]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert_eq!(error.to_string(), "test deadline expired");
    }

    #[test]
    fn unix_deadline_stream_can_begin_a_distinct_response_phase() {
        let (mut sender, receiver) = UnixStream::pair().unwrap();
        let mut stream = DeadlineStream::new(
            receiver,
            Instant::now() - Duration::from_millis(1),
            "test deadline expired",
        )
        .unwrap();
        stream.set_deadline(Instant::now() + Duration::from_secs(1));
        sender.write_all(b"x").unwrap();

        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).unwrap();
        assert_eq!(byte, *b"x");
    }
}
