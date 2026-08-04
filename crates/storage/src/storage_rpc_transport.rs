use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use rustls::pki_types::ServerName;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::deadline_io::DeadlineStream;

pub(crate) const STORAGE_RPC_TLS_ALPN: &[u8] = b"argmin-storage-rpc/1";
const STORAGE_RPC_CLIENT_POOL_MAX_CONNECTIONS_PER_ENDPOINT: usize = 8;

pub trait StorageRpcStream: Read + Write + Send {
    fn set_operation_deadline(&mut self, deadline: Instant) -> io::Result<()>;
    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()>;
}

pub type BoxStorageRpcStream = Box<dyn StorageRpcStream>;

#[derive(Clone)]
pub struct StorageRpcClientEndpoint {
    inner: StorageRpcClientEndpointInner,
}

#[derive(Clone)]
enum StorageRpcClientEndpointInner {
    Unix {
        socket_path: PathBuf,
        request_pool: Arc<StorageRpcClientConnectionPool>,
    },
    #[cfg(test)]
    TestUnpooledUnix { socket_path: PathBuf },
    Tcp {
        advertised_endpoint: String,
        addresses: Vec<SocketAddr>,
        server_name: String,
        tls_client_config: Arc<rustls::ClientConfig>,
        request_pool: Arc<StorageRpcClientConnectionPool>,
    },
}

struct StorageRpcClientConnectionPool {
    state: Mutex<StorageRpcClientConnectionPoolState>,
    available: Condvar,
}

#[derive(Default)]
struct StorageRpcClientConnectionPoolState {
    open: usize,
    idle: Vec<IdleStorageRpcConnection>,
}

struct IdleStorageRpcConnection {
    stream: BoxStorageRpcStream,
    idle_since: Instant,
}

pub(crate) struct StorageRpcRequestConnection {
    stream: Option<BoxStorageRpcStream>,
    pool: Option<Arc<StorageRpcClientConnectionPool>>,
    reusable: bool,
}

pub(crate) enum StorageRpcEndpointAuthorityIdentity<'a> {
    Unix(&'a Path),
    Tcp(&'a str),
}

impl StorageRpcClientEndpoint {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            inner: StorageRpcClientEndpointInner::Unix {
                socket_path: socket_path.into(),
                request_pool: Arc::new(StorageRpcClientConnectionPool::new()),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn unpooled_unix_for_test(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            inner: StorageRpcClientEndpointInner::TestUnpooledUnix {
                socket_path: socket_path.into(),
            },
        }
    }

    pub fn tls_tcp(
        advertised_endpoint: impl Into<String>,
        addresses: Vec<SocketAddr>,
        server_name: impl Into<String>,
        trust_roots: Arc<rustls::RootCertStore>,
    ) -> io::Result<Self> {
        let tls_client_config = storage_rpc_tls_client_config(trust_roots)?;
        Self::tcp_with_config(
            advertised_endpoint,
            addresses,
            server_name,
            tls_client_config,
        )
    }

    pub(crate) fn tcp_with_config(
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
        Ok(Self {
            inner: StorageRpcClientEndpointInner::Tcp {
                advertised_endpoint,
                addresses,
                server_name,
                tls_client_config,
                request_pool: Arc::new(StorageRpcClientConnectionPool::new()),
            },
        })
    }

    #[must_use]
    pub fn advertised_endpoint(&self) -> String {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix { socket_path, .. } => {
                socket_path.to_string_lossy().into_owned()
            }
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { socket_path } => {
                socket_path.to_string_lossy().into_owned()
            }
            StorageRpcClientEndpointInner::Tcp {
                advertised_endpoint,
                ..
            } => advertised_endpoint.clone(),
        }
    }

    pub(crate) fn authority_identity(&self) -> StorageRpcEndpointAuthorityIdentity<'_> {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix { socket_path, .. } => {
                StorageRpcEndpointAuthorityIdentity::Unix(socket_path.as_path())
            }
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { socket_path } => {
                StorageRpcEndpointAuthorityIdentity::Unix(socket_path.as_path())
            }
            StorageRpcClientEndpointInner::Tcp {
                advertised_endpoint,
                ..
            } => StorageRpcEndpointAuthorityIdentity::Tcp(advertised_endpoint),
        }
    }

    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&Path> {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix { socket_path, .. } => Some(socket_path),
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { socket_path } => Some(socket_path),
            StorageRpcClientEndpointInner::Tcp { .. } => None,
        }
    }

    #[must_use]
    pub fn is_tls_tcp(&self) -> bool {
        matches!(&self.inner, StorageRpcClientEndpointInner::Tcp { .. })
    }

    pub(crate) fn connect(&self, deadline: Instant) -> io::Result<BoxStorageRpcStream> {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix { socket_path, .. } => {
                let stream = UnixStream::connect(socket_path)?;
                let stream = DeadlineStream::new(
                    stream,
                    deadline,
                    "storage RPC absolute operation deadline expired",
                )?;
                Ok(Box::new(stream))
            }
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { socket_path } => {
                let stream = UnixStream::connect(socket_path)?;
                let stream = DeadlineStream::new(
                    stream,
                    deadline,
                    "storage RPC absolute operation deadline expired",
                )?;
                Ok(Box::new(stream))
            }
            StorageRpcClientEndpointInner::Tcp {
                addresses,
                server_name,
                tls_client_config,
                ..
            } => connect_tls_tcp(addresses, server_name, tls_client_config, deadline),
        }
    }

    pub(crate) fn connect_request(
        &self,
        deadline: Instant,
        io_timeout: Duration,
        max_connections: usize,
    ) -> io::Result<StorageRpcRequestConnection> {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix {
                socket_path,
                request_pool,
            } => request_pool.checkout(deadline, io_timeout, max_connections, || {
                let stream = UnixStream::connect(socket_path)?;
                let stream = DeadlineStream::new(
                    stream,
                    deadline,
                    "storage RPC absolute operation deadline expired",
                )?;
                Ok(Box::new(stream))
            }),
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { .. } => {
                self.connect(deadline)
                    .map(|stream| StorageRpcRequestConnection {
                        stream: Some(stream),
                        pool: None,
                        reusable: false,
                    })
            }
            StorageRpcClientEndpointInner::Tcp {
                addresses,
                server_name,
                tls_client_config,
                request_pool,
                ..
            } => request_pool.checkout(deadline, io_timeout, max_connections, || {
                connect_tls_tcp(addresses, server_name, tls_client_config, deadline)
            }),
        }
    }
}

pub(crate) fn storage_rpc_tls_client_config(
    trust_roots: Arc<rustls::RootCertStore>,
) -> io::Result<Arc<rustls::ClientConfig>> {
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "failed to select the storage RPC TLS protocol profile",
        )
    })?
    .with_root_certificates((*trust_roots).clone())
    .with_no_client_auth();
    config.alpn_protocols = vec![STORAGE_RPC_TLS_ALPN.to_vec()];
    Ok(Arc::new(config))
}

#[derive(Clone)]
struct StorageRpcSingleCertificateResolver {
    certified_key: Arc<CertifiedKey>,
}

impl fmt::Debug for StorageRpcSingleCertificateResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRpcSingleCertificateResolver")
            .field("certificate_count", &self.certified_key.cert.len())
            .field("private_key", &"<redacted>")
            .finish()
    }
}

impl ResolvesServerCert for StorageRpcSingleCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }
}

pub(crate) fn storage_rpc_tls_server_config(
    certified_key: Arc<CertifiedKey>,
) -> io::Result<Arc<rustls::ServerConfig>> {
    let resolver = StorageRpcSingleCertificateResolver { certified_key };
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "failed to select the storage RPC TLS protocol profile",
        )
    })?
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(resolver));
    config.alpn_protocols = vec![STORAGE_RPC_TLS_ALPN.to_vec()];
    Ok(Arc::new(config))
}

pub(crate) fn validate_storage_rpc_tls_server_config(
    config: &rustls::ServerConfig,
) -> io::Result<()> {
    if config.alpn_protocols != [STORAGE_RPC_TLS_ALPN] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "storage RPC TLS server must offer only argmin-storage-rpc/1 ALPN",
        ));
    }
    Ok(())
}

impl StorageRpcClientConnectionPool {
    fn new() -> Self {
        Self {
            state: Mutex::new(StorageRpcClientConnectionPoolState::default()),
            available: Condvar::new(),
        }
    }

    fn checkout(
        self: &Arc<Self>,
        deadline: Instant,
        io_timeout: Duration,
        configured_max_connections: usize,
        connect: impl FnOnce() -> io::Result<BoxStorageRpcStream>,
    ) -> io::Result<StorageRpcRequestConnection> {
        let max_connections = configured_max_connections
            .clamp(1, STORAGE_RPC_CLIENT_POOL_MAX_CONNECTIONS_PER_ENDPOINT);
        let max_idle_age = io_timeout / 2;
        let mut connect = Some(connect);

        loop {
            remaining(deadline)?;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let now = Instant::now();
            let idle_before = state.idle.len();
            state
                .idle
                .retain(|idle| now.duration_since(idle.idle_since) < max_idle_age);
            let expired = idle_before - state.idle.len();
            state.open = state
                .open
                .checked_sub(expired)
                .expect("storage RPC pool idle connection count exceeds open count");

            if let Some(mut idle) = state.idle.pop() {
                drop(state);
                if idle.stream.set_operation_deadline(deadline).is_ok() {
                    return Ok(StorageRpcRequestConnection {
                        stream: Some(idle.stream),
                        pool: Some(Arc::clone(self)),
                        reusable: false,
                    });
                }
                self.remove_open_connection();
                continue;
            }

            if state.open < max_connections {
                state.open += 1;
                drop(state);
                let result = connect
                    .take()
                    .expect("storage RPC pool starts at most one new connection")(
                );
                return match result {
                    Ok(stream) => Ok(StorageRpcRequestConnection {
                        stream: Some(stream),
                        pool: Some(Arc::clone(self)),
                        reusable: false,
                    }),
                    Err(error) => {
                        self.remove_open_connection();
                        Err(error)
                    }
                };
            }

            let wait = remaining(deadline)?;
            let (next_state, timeout) = self
                .available
                .wait_timeout(state, wait)
                .unwrap_or_else(|error| error.into_inner());
            drop(next_state);
            if timeout.timed_out() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "storage RPC connection pool deadline expired",
                ));
            }
        }
    }

    fn remove_open_connection(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.open = state
            .open
            .checked_sub(1)
            .expect("storage RPC pool connection release without checkout");
        self.available.notify_one();
    }

    fn return_connection(&self, stream: BoxStorageRpcStream) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.idle.push(IdleStorageRpcConnection {
            stream,
            idle_since: Instant::now(),
        });
        self.available.notify_one();
    }
}

impl StorageRpcRequestConnection {
    pub(crate) fn stream_mut(&mut self) -> &mut BoxStorageRpcStream {
        self.stream
            .as_mut()
            .expect("storage RPC request connection retains its stream until drop")
    }

    pub(crate) fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for StorageRpcRequestConnection {
    fn drop(&mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        let Some(pool) = self.pool.as_ref() else {
            return;
        };
        if self.reusable {
            pool.return_connection(stream);
        } else {
            pool.remove_open_connection();
        }
    }
}

impl fmt::Debug for StorageRpcClientEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            StorageRpcClientEndpointInner::Unix { socket_path, .. } => f
                .debug_struct("StorageRpcClientEndpoint::Unix")
                .field("socket_path", socket_path)
                .finish(),
            #[cfg(test)]
            StorageRpcClientEndpointInner::TestUnpooledUnix { socket_path } => f
                .debug_struct("StorageRpcClientEndpoint::TestUnpooledUnix")
                .field("socket_path", socket_path)
                .finish(),
            StorageRpcClientEndpointInner::Tcp {
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
    validate_storage_rpc_tls_server_config(&tls_server_config)?;
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
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::net::TcpListener;
    use std::thread;

    fn test_certificates() -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(include_bytes!("../../s3-tests/testdata/localhost-cert.pem"))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn test_private_key() -> PrivateKeyDer<'static> {
        PrivateKeyDer::from_pem_slice(include_bytes!("../../s3-tests/testdata/localhost-key.pem"))
            .unwrap()
    }

    fn test_certified_key() -> Arc<CertifiedKey> {
        Arc::new(
            CertifiedKey::from_der(
                test_certificates(),
                test_private_key(),
                &rustls::crypto::ring::default_provider(),
            )
            .unwrap(),
        )
    }

    fn test_trust_roots() -> Arc<rustls::RootCertStore> {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                CertificateDer::pem_slice_iter(include_bytes!(
                    "../../s3-tests/testdata/ca-cert.pem"
                ))
                .next()
                .unwrap()
                .unwrap(),
            )
            .unwrap();
        Arc::new(roots)
    }

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
        let error = StorageRpcClientEndpoint::tcp_with_config(
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
    fn tls_tcp_endpoint_constructs_the_storage_owned_profile() {
        let endpoint = StorageRpcClientEndpoint::tls_tcp(
            "tcp://localhost:7701",
            vec!["127.0.0.1:7701".parse().unwrap()],
            "localhost",
            Arc::new(rustls::RootCertStore::empty()),
        )
        .unwrap();

        let StorageRpcClientEndpointInner::Tcp {
            tls_client_config, ..
        } = &endpoint.inner
        else {
            panic!("TLS constructor returned a Unix endpoint");
        };
        assert_eq!(tls_client_config.alpn_protocols, [STORAGE_RPC_TLS_ALPN]);
        assert!(endpoint.is_tls_tcp());
    }

    #[test]
    fn storage_owned_client_profile_rejects_tls_1_2_only_server() {
        let mut server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS12])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(test_certificates(), test_private_key())
        .unwrap();
        server_config.alpn_protocols = vec![STORAGE_RPC_TLS_ALPN.to_vec()];
        let server_config = Arc::new(server_config);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            accepted_tls_tcp_stream(
                stream,
                server_config,
                Instant::now() + Duration::from_secs(1),
            )
            .err()
            .expect("TLS 1.3 storage client must not negotiate with a TLS 1.2-only server")
        });
        let endpoint = StorageRpcClientEndpoint::tls_tcp(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            test_trust_roots(),
        )
        .unwrap();

        let client_error = endpoint
            .connect(Instant::now() + Duration::from_secs(1))
            .err()
            .expect("TLS 1.3 storage client must reject a TLS 1.2-only server");
        let server_error = server.join().unwrap();

        assert_ne!(client_error.kind(), io::ErrorKind::TimedOut);
        assert_ne!(server_error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn storage_owned_server_profile_rejects_tls_1_2_only_client() {
        let server_config = storage_rpc_tls_server_config(test_certified_key()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            accepted_tls_tcp_stream(
                stream,
                server_config,
                Instant::now() + Duration::from_secs(1),
            )
            .err()
            .expect("TLS 1.3 storage server must not negotiate with a TLS 1.2-only client")
        });
        let mut client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS12])
        .unwrap()
        .with_root_certificates((*test_trust_roots()).clone())
        .with_no_client_auth();
        client_config.alpn_protocols = vec![STORAGE_RPC_TLS_ALPN.to_vec()];
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            Arc::new(client_config),
        )
        .unwrap();

        let client_error = endpoint
            .connect(Instant::now() + Duration::from_secs(1))
            .err()
            .expect("TLS 1.2-only client must reject the TLS 1.3 storage server");
        let server_error = server.join().unwrap();

        assert_ne!(client_error.kind(), io::ErrorKind::TimedOut);
        assert_ne!(server_error.kind(), io::ErrorKind::TimedOut);
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

    #[test]
    fn request_pool_reuses_only_successfully_completed_connections() {
        let pool = Arc::new(StorageRpcClientConnectionPool::new());
        let deadline = Instant::now() + Duration::from_secs(1);
        let (first_stream, _first_peer) = UnixStream::pair().unwrap();
        let mut first = pool
            .checkout(deadline, Duration::from_secs(1), 1, || {
                accepted_unix_stream(first_stream, deadline)
            })
            .unwrap();

        first.mark_reusable();
        drop(first);
        assert_eq!(pool.state.lock().unwrap().idle.len(), 1);

        let reused = pool
            .checkout(deadline, Duration::from_secs(1), 1, || {
                panic!("a completed request should reuse its pooled connection")
            })
            .unwrap();
        drop(reused);
        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.open, 0);
            assert!(state.idle.is_empty());
        }

        let (replacement_stream, _replacement_peer) = UnixStream::pair().unwrap();
        let replacement = pool
            .checkout(deadline, Duration::from_secs(1), 1, || {
                accepted_unix_stream(replacement_stream, deadline)
            })
            .unwrap();
        assert_eq!(pool.state.lock().unwrap().open, 1);
        drop(replacement);
        assert_eq!(pool.state.lock().unwrap().open, 0);
    }
}
