use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::pki_types::ServerName;

use crate::deadline_io::DeadlineStream;

pub const STORAGE_RPC_TLS_ALPN: &[u8] = b"argmin-storage-rpc/1";

pub trait StorageRpcStream: Read + Write + Send {
    fn set_operation_deadline(&mut self, deadline: Instant) -> io::Result<()>;
    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()>;
}

pub type BoxStorageRpcStream = Box<dyn StorageRpcStream>;

#[derive(Clone)]
pub enum StorageRpcClientEndpoint {
    Unix {
        socket_path: PathBuf,
    },
    Tcp {
        advertised_endpoint: String,
        addresses: Vec<SocketAddr>,
        server_name: String,
        tls_client_config: Arc<rustls::ClientConfig>,
    },
}

pub(crate) enum StorageRpcEndpointAuthorityIdentity<'a> {
    Unix(&'a Path),
    Tcp(&'a str),
}

impl StorageRpcClientEndpoint {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        Self::Unix {
            socket_path: socket_path.into(),
        }
    }

    pub fn tcp(
        advertised_endpoint: impl Into<String>,
        addresses: Vec<SocketAddr>,
        server_name: impl Into<String>,
        tls_client_config: Arc<rustls::ClientConfig>,
    ) -> io::Result<Self> {
        let advertised_endpoint = advertised_endpoint.into();
        if advertised_endpoint.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage RPC TCP advertised endpoint must not be empty",
            ));
        }
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage RPC TCP endpoint must have at least one resolved address",
            ));
        }
        let server_name = server_name.into();
        ServerName::try_from(server_name.clone()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage RPC TCP endpoint has an invalid TLS server name",
            )
        })?;
        if tls_client_config.alpn_protocols != [STORAGE_RPC_TLS_ALPN] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage RPC TLS client must offer only argmin-storage-rpc/1 ALPN",
            ));
        }
        Ok(Self::Tcp {
            advertised_endpoint,
            addresses,
            server_name,
            tls_client_config,
        })
    }

    #[must_use]
    pub fn advertised_endpoint(&self) -> String {
        match self {
            Self::Unix { socket_path } => socket_path.to_string_lossy().into_owned(),
            Self::Tcp {
                advertised_endpoint,
                ..
            } => advertised_endpoint.clone(),
        }
    }

    pub(crate) fn authority_identity(&self) -> StorageRpcEndpointAuthorityIdentity<'_> {
        match self {
            Self::Unix { socket_path } => {
                StorageRpcEndpointAuthorityIdentity::Unix(socket_path.as_path())
            }
            Self::Tcp {
                advertised_endpoint,
                ..
            } => StorageRpcEndpointAuthorityIdentity::Tcp(advertised_endpoint),
        }
    }

    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&Path> {
        match self {
            Self::Unix { socket_path } => Some(socket_path),
            Self::Tcp { .. } => None,
        }
    }

    pub(crate) fn connect(&self, deadline: Instant) -> io::Result<BoxStorageRpcStream> {
        match self {
            Self::Unix { socket_path } => {
                let stream = UnixStream::connect(socket_path)?;
                let stream = DeadlineStream::new(
                    stream,
                    deadline,
                    "storage RPC absolute operation deadline expired",
                )?;
                Ok(Box::new(stream))
            }
            Self::Tcp {
                addresses,
                server_name,
                tls_client_config,
                ..
            } => connect_tls_tcp(addresses, server_name, tls_client_config, deadline),
        }
    }
}

impl fmt::Debug for StorageRpcClientEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix { socket_path } => f
                .debug_struct("StorageRpcClientEndpoint::Unix")
                .field("socket_path", socket_path)
                .finish(),
            Self::Tcp {
                advertised_endpoint,
                addresses,
                server_name,
                ..
            } => f
                .debug_struct("StorageRpcClientEndpoint::Tcp")
                .field("advertised_endpoint", advertised_endpoint)
                .field("addresses", addresses)
                .field("server_name", server_name)
                .field("tls", &true)
                .finish(),
        }
    }
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "storage RPC absolute operation deadline expired",
        ));
    }
    Ok(remaining)
}

impl StorageRpcStream for DeadlineStream<UnixStream> {
    fn set_operation_deadline(&mut self, deadline: Instant) -> io::Result<()> {
        self.set_deadline(deadline);
        Ok(())
    }

    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.get_ref().shutdown(how)
    }
}

struct DeadlineTlsTcpStream {
    stream: rustls::StreamOwned<rustls::ClientConnection, DeadlineStream<TcpStream>>,
}

struct DeadlineServerTlsTcpStream {
    stream: rustls::StreamOwned<rustls::ServerConnection, DeadlineStream<TcpStream>>,
}

impl Read for DeadlineServerTlsTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for DeadlineServerTlsTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl StorageRpcStream for DeadlineServerTlsTcpStream {
    fn set_operation_deadline(&mut self, deadline: Instant) -> io::Result<()> {
        self.stream.sock.set_deadline(deadline);
        Ok(())
    }

    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.stream.sock.get_ref().shutdown(how)
    }
}

impl Read for DeadlineTlsTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for DeadlineTlsTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl StorageRpcStream for DeadlineTlsTcpStream {
    fn set_operation_deadline(&mut self, deadline: Instant) -> io::Result<()> {
        self.stream.sock.set_deadline(deadline);
        Ok(())
    }

    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.stream.sock.get_ref().shutdown(how)
    }
}

fn connect_tls_tcp(
    addresses: &[SocketAddr],
    server_name: &str,
    tls_client_config: &Arc<rustls::ClientConfig>,
    deadline: Instant,
) -> io::Result<BoxStorageRpcStream> {
    let mut last_error = None;
    let mut tcp_stream = None;
    for address in addresses {
        match TcpStream::connect_timeout(address, remaining(deadline)?) {
            Ok(stream) => {
                tcp_stream = Some(stream);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    let tcp_stream = tcp_stream.ok_or_else(|| {
        last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "storage RPC TCP endpoint has no usable address",
            )
        })
    })?;
    tcp_stream.set_nodelay(true)?;
    let server_name = ServerName::try_from(server_name.to_string()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "storage RPC TCP endpoint has an invalid TLS server name",
        )
    })?;
    let connection = rustls::ClientConnection::new(Arc::clone(tls_client_config), server_name)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let socket = DeadlineStream::new(
        tcp_stream,
        deadline,
        "storage RPC absolute operation deadline expired",
    )?;
    let mut stream = DeadlineTlsTcpStream {
        stream: rustls::StreamOwned::new(connection, socket),
    };
    while stream.stream.conn.is_handshaking() {
        stream.stream.conn.complete_io(&mut stream.stream.sock)?;
    }
    if stream.stream.conn.alpn_protocol() != Some(STORAGE_RPC_TLS_ALPN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage RPC TLS peer did not negotiate required argmin-storage-rpc/1 ALPN",
        ));
    }
    Ok(Box::new(stream))
}

pub(crate) fn accepted_unix_stream(
    stream: UnixStream,
    deadline: Instant,
) -> io::Result<BoxStorageRpcStream> {
    let stream = DeadlineStream::new(
        stream,
        deadline,
        "storage RPC absolute operation deadline expired",
    )?;
    Ok(Box::new(stream))
}

pub(crate) fn accepted_tls_tcp_stream(
    stream: TcpStream,
    tls_server_config: Arc<rustls::ServerConfig>,
    deadline: Instant,
) -> io::Result<BoxStorageRpcStream> {
    if tls_server_config.alpn_protocols != [STORAGE_RPC_TLS_ALPN] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "storage RPC TLS server must offer only argmin-storage-rpc/1 ALPN",
        ));
    }
    stream.set_nodelay(true)?;
    let connection = rustls::ServerConnection::new(tls_server_config)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let socket = DeadlineStream::new(
        stream,
        deadline,
        "storage RPC absolute operation deadline expired",
    )?;
    let mut stream = DeadlineServerTlsTcpStream {
        stream: rustls::StreamOwned::new(connection, socket),
    };
    while stream.stream.conn.is_handshaking() {
        stream.stream.conn.complete_io(&mut stream.stream.sock)?;
    }
    if stream.stream.conn.alpn_protocol() != Some(STORAGE_RPC_TLS_ALPN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage RPC TLS client did not negotiate required argmin-storage-rpc/1 ALPN",
        ));
    }
    Ok(Box::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    fn client_config(alpn: Vec<Vec<u8>>) -> Arc<rustls::ClientConfig> {
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        config.alpn_protocols = alpn;
        Arc::new(config)
    }

    #[test]
    fn tcp_endpoint_requires_exact_storage_rpc_alpn() {
        let error = StorageRpcClientEndpoint::tcp(
            "tcp://localhost:7701",
            vec!["127.0.0.1:7701".parse().unwrap()],
            "localhost",
            client_config(Vec::new()),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("argmin-storage-rpc/1 ALPN"));
    }

    #[test]
    fn tcp_socket_deadline_is_not_extended_by_trickled_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let writer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            for byte in [1_u8, 2, 3] {
                if stream.write_all(&[byte]).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(40));
            }
        });
        let stream = TcpStream::connect(address).unwrap();
        let started = Instant::now();
        let mut stream = DeadlineStream::new(
            stream,
            started + Duration::from_millis(65),
            "test storage RPC deadline expired",
        )
        .unwrap();
        let mut bytes = [0_u8; 3];

        let error = stream.read_exact(&mut bytes).unwrap_err();

        assert!(matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ));
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "trickled bytes extended the absolute storage RPC deadline"
        );
        writer.join().unwrap();
    }

    #[test]
    fn unix_operation_deadlines_do_not_install_socket_timeouts() {
        let (server, _peer) = UnixStream::pair().unwrap();
        let observer = server.try_clone().unwrap();
        let mut stream =
            accepted_unix_stream(server, Instant::now() + Duration::from_secs(1)).unwrap();

        assert_eq!(observer.read_timeout().unwrap(), None);
        assert_eq!(observer.write_timeout().unwrap(), None);

        stream
            .set_operation_deadline(Instant::now() + Duration::from_secs(2))
            .unwrap();

        assert_eq!(observer.read_timeout().unwrap(), None);
        assert_eq!(observer.write_timeout().unwrap(), None);
    }
}
