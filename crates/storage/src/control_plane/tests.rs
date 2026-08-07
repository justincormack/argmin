use super::*;
use crate::metadata_command::{
    CreateBucketCommand, DeleteFinalizedBucketCommand, MarkBucketDeletingCommand,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
};
use crate::pg_store::PgStore;
use crate::traits::PgMetadataStore;
use crate::types::PlacedSegmentShardRepairWorkItem;
use proptest::prelude::*;
use std::cell::Cell;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

fn reseal_crc64_suffix(bytes: &mut [u8]) {
    let checksum_offset = bytes.len() - std::mem::size_of::<u64>();
    let checksum = checksum::crc64::checksum(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}

#[test]
fn control_plane_rpc_preserves_openraft_operation_error_kind() {
    for kind in [
        ControlPlaneRaftOperationErrorKind::ForwardToLeader,
        ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
        ControlPlaneRaftOperationErrorKind::Fatal,
        ControlPlaneRaftOperationErrorKind::Rejected,
    ] {
        let payload =
            encode_control_plane_rpc_response(Err(ControlPlaneError::OpenRaftOperation {
                kind,
                message: "test OpenRaft failure".to_owned(),
            }))
            .unwrap();
        let error = decode_control_plane_rpc_response(payload).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::OpenRaftOperation {
                kind: actual_kind,
                message,
            } if actual_kind == kind && message == "test OpenRaft failure"
        ));
    }
}

fn control_plane_test_tls_certified_key() -> Arc<CertifiedKey> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certificates = CertificateDer::pem_slice_iter(include_bytes!(
        "../../../s3-tests/testdata/localhost-cert.pem"
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
        "../../../s3-tests/testdata/localhost-key.pem"
    ))
    .unwrap();
    let provider = tls_provider::build_provider();
    Arc::new(CertifiedKey::from_der(certificates, private_key, &provider).unwrap())
}

fn control_plane_test_tls_server_config() -> Arc<rustls::ServerConfig> {
    let certified_key = control_plane_test_tls_certified_key();
    let resolver = ControlPlaneRpcTlsCertificateResolver { certified_key };
    let mut server_config =
        rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));
    server_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
    Arc::new(server_config)
}

fn control_plane_test_tls_client_config(
    negotiate_control_plane_alpn: bool,
) -> Arc<rustls::ClientConfig> {
    let mut config =
        rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(control_plane_test_tls_roots())
            .with_no_client_auth();
    if negotiate_control_plane_alpn {
        config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
    }
    Arc::new(config)
}

fn control_plane_test_tls_roots() -> rustls::RootCertStore {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::CertificateDer;

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(
            CertificateDer::pem_slice_iter(include_bytes!(
                "../../../s3-tests/testdata/ca-cert.pem"
            ))
            .next()
            .unwrap()
            .unwrap(),
        )
        .unwrap();
    roots
}

fn control_plane_test_tls_endpoint(address: std::net::SocketAddr) -> ControlPlaneRpcClientEndpoint {
    ControlPlaneRpcClientEndpoint::tls_tcp(
        format!("tcp://{address}"),
        address.ip().to_string(),
        address.port(),
        "localhost",
        Duration::from_secs(1),
        control_plane_test_tls_roots(),
    )
    .unwrap()
}

fn test_control_plane_server_authority(
    name: &str,
) -> (
    test_util::TempDir,
    Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
) {
    let directory = test_util::tempdir();
    let state_path = directory.path().join(format!("{name}.state"));
    let authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(state_path)).unwrap();
    (directory, Arc::new(Mutex::new(authority)))
}

fn wait_for_control_plane_server_workers_to_finish(policy: &ControlPlaneRpcServerPolicy) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while policy.active_workers() != 0 {
        assert!(
            Instant::now() < deadline,
            "control-plane server worker did not terminate"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn control_plane_rpc_server_policy_rejects_zero_resource_limits() {
    assert!(matches!(
        ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 0, 1),
        Err(ControlPlaneRpcServerConfigError::ZeroWorkerLimit)
    ));
    assert!(matches!(
        ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 1, 0),
        Err(ControlPlaneRpcServerConfigError::ZeroPreAuthByteBudget)
    ));
}

#[test]
fn control_plane_rpc_server_policies_have_independent_worker_reservations() {
    let ordinary = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();
    let recovery = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::AuthorityClockRecovery,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();

    assert!(reserve_control_plane_rpc_worker(
        &ordinary.resources.active_workers,
        ordinary.resources.worker_limit
    ));
    assert!(!reserve_control_plane_rpc_worker(
        &ordinary.resources.active_workers,
        ordinary.resources.worker_limit
    ));
    assert!(reserve_control_plane_rpc_worker(
        &recovery.resources.active_workers,
        recovery.resources.worker_limit
    ));
    assert_eq!(ordinary.active_workers(), 1);
    assert_eq!(recovery.active_workers(), 1);
}

#[test]
fn control_plane_rpc_server_pre_auth_budget_rejects_before_payload_read() {
    let frame =
        encode_control_plane_rpc_frame(ControlPlaneRpcKind::RuntimeMapStatus, &[1]).unwrap();
    let frame_bytes = frame.len();
    let budget = Arc::new(ControlPlaneRpcPreAuthByteBudget::new(frame_bytes));
    let held = budget.reserve(frame_bytes).unwrap();
    let mut framed_header = std::io::Cursor::new(&frame[..control_plane_rpc_frame_overhead()]);

    let error = read_control_plane_request_with_reservation(&mut framed_header, |frame_bytes| {
        budget.reserve(frame_bytes)
    })
    .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("pre-authentication frame budget exhausted")
    ));
    assert_eq!(
        framed_header.position(),
        u64::try_from(control_plane_rpc_frame_overhead()).unwrap()
    );
    assert_eq!(budget.reserved_bytes.load(Ordering::Acquire), frame_bytes);
    drop(held);
    assert_eq!(budget.reserved_bytes.load(Ordering::Acquire), 0);
}

#[test]
fn control_plane_rpc_server_response_errors_use_bounded_categories() {
    let classify = |kind| {
        control_plane_rpc_response_write_error_kind(&ControlPlaneError::io(
            "write test response",
            std::io::Error::from(kind),
        ))
    };

    assert_eq!(
        classify(ErrorKind::BrokenPipe),
        observability::ControlPlaneRpcResponseWriteErrorKind::BrokenPipe
    );
    assert_eq!(
        classify(ErrorKind::ConnectionReset),
        observability::ControlPlaneRpcResponseWriteErrorKind::ConnectionReset
    );
    assert_eq!(
        classify(ErrorKind::WouldBlock),
        observability::ControlPlaneRpcResponseWriteErrorKind::Timeout
    );
    assert_eq!(
        classify(ErrorKind::PermissionDenied),
        observability::ControlPlaneRpcResponseWriteErrorKind::Other
    );
}

#[test]
fn control_plane_rpc_server_requires_exactly_one_response_publication() {
    struct SkipPublication;
    impl ControlPlaneRpcResponsePublication for SkipPublication {
        fn publish(
            &self,
            _publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
        ) -> Result<(), ControlPlaneError> {
            Ok(())
        }
    }

    struct DuplicatePublication;
    impl ControlPlaneRpcResponsePublication for DuplicatePublication {
        fn publish(
            &self,
            publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
        ) -> Result<(), ControlPlaneError> {
            publish()?;
            publish()
        }
    }

    let calls = Cell::new(0);
    let mut publish = || {
        calls.set(calls.get() + 1);
        Ok(())
    };
    let error =
        publish_control_plane_rpc_response(Some(&SkipPublication), &mut publish).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("without publishing")
    ));
    assert_eq!(calls.get(), 0);

    let error =
        publish_control_plane_rpc_response(Some(&DuplicatePublication), &mut publish).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("more than once")
    ));
    assert_eq!(calls.get(), 1);

    publish_control_plane_rpc_response(None, &mut publish).unwrap();
    assert_eq!(calls.get(), 2);
}

#[test]
fn control_plane_tls_server_listener_owns_protocol_profile() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        listener,
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let ControlPlaneRpcServerListenerKind::TlsTcp {
        tls_server_config, ..
    } = &listener.kind
    else {
        panic!("TLS/TCP constructor returned a Unix listener")
    };

    assert_eq!(
        tls_server_config.alpn_protocols,
        [CONTROL_PLANE_RPC_TLS_ALPN]
    );
}

#[test]
fn control_plane_tls_server_bounds_stalled_handshake() {
    let raw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = raw_listener.local_addr().unwrap();
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        raw_listener,
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_millis(100),
    )
    .unwrap();
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();
    let (_authority_directory, authority) =
        test_control_plane_server_authority("stalled-tls-handshake");
    let stalled_client = TcpStream::connect(address).unwrap();
    let started_at = Instant::now();
    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert!(started_at.elapsed() < Duration::from_secs(1));
    drop(stalled_client);
}

#[test]
fn control_plane_unix_server_bounds_trickled_request_absolutely() {
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_millis(100),
    )
    .unwrap();
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();
    let (_authority_directory, authority) =
        test_control_plane_server_authority("trickled-unix-request");
    let writer = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        let mut written = 0;
        for byte in CONTROL_PLANE_RPC_MAGIC {
            if stream.write_all(std::slice::from_ref(byte)).is_err() {
                break;
            }
            written += 1;
            std::thread::sleep(Duration::from_millis(30));
        }
        written
    });
    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);
    let written = writer.join().unwrap();

    assert!(written < CONTROL_PLANE_RPC_MAGIC.len());
}

#[test]
fn control_plane_tls_server_rejects_missing_protocol_profile() {
    use rustls::pki_types::ServerName;

    let raw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = raw_listener.local_addr().unwrap();
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        raw_listener,
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();
    let (_authority_directory, authority) =
        test_control_plane_server_authority("tls-alpn-required");
    let (handshake_tx, handshake_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let client = std::thread::spawn(move || {
        let stream = TcpStream::connect(address).unwrap();
        let connection = rustls::ClientConnection::new(
            control_plane_test_tls_client_config(false),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock).unwrap();
        }
        handshake_tx
            .send(stream.conn.alpn_protocol().map(<[u8]>::to_vec))
            .unwrap();
        release_rx.recv().unwrap();
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    assert_eq!(
        handshake_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        None
    );
    wait_for_control_plane_server_workers_to_finish(&policy);
    release_tx.send(()).unwrap();
    client.join().unwrap();
}

#[test]
fn control_plane_tls_server_requires_auth_before_authority_confirmation() {
    use rustls::pki_types::ServerName;

    let raw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = raw_listener.local_addr().unwrap();
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        raw_listener,
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let verifier = Arc::new(frontend_auth_verifier("auth-cluster", "frontend-1"));
    let verifier_for_assert = Arc::clone(&verifier);
    let confirmation_calls = Arc::new(AtomicUsize::new(0));
    let confirmation_calls_for_policy = Arc::clone(&confirmation_calls);
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_auth_verifier(verifier)
    .with_authority_confirmation(Arc::new(move || {
        confirmation_calls_for_policy.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }));
    let (_authority_directory, authority) =
        test_control_plane_server_authority("tls-auth-required");
    let client = std::thread::spawn(move || {
        let stream = TcpStream::connect(address).unwrap();
        let connection = rustls::ClientConnection::new(
            control_plane_test_tls_client_config(true),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        write_control_plane_rpc_frame(&mut stream, ControlPlaneRpcKind::RuntimeMapStatus, &[])
            .unwrap();
        stream.flush().unwrap();
        read_control_plane_rpc_frame(&mut stream).unwrap_err()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let _error = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert_eq!(confirmation_calls.load(Ordering::Acquire), 0);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
}

#[test]
fn control_plane_unix_server_facade_dispatches_logical_client_request() {
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap();
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            directory.path().join("control.state"),
        ))
        .unwrap(),
    ));
    let client = std::thread::spawn(move || {
        UnixControlPlaneClient::new(socket_path)
            .runtime_map_status_with_check_applied_timeout()
            .unwrap()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let status = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert_eq!(status.pg_routes(), 0);
}

#[test]
fn control_plane_unix_server_requires_auth_before_authority_confirmation() {
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let verifier = Arc::new(frontend_auth_verifier("auth-cluster", "frontend-1"));
    let verifier_for_assert = Arc::clone(&verifier);
    let confirmation_calls = Arc::new(AtomicUsize::new(0));
    let confirmation_calls_for_policy = Arc::clone(&confirmation_calls);
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_auth_verifier(verifier)
    .with_authority_confirmation(Arc::new(move || {
        confirmation_calls_for_policy.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }));
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            directory.path().join("control.state"),
        ))
        .unwrap(),
    ));
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        write_control_plane_rpc_frame(&mut stream, ControlPlaneRpcKind::RuntimeMapStatus, &[])
            .unwrap();
        stream.flush().unwrap();
        read_control_plane_rpc_frame(&mut stream).unwrap_err()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let _error = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert_eq!(confirmation_calls.load(Ordering::Acquire), 0);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
}

#[test]
fn control_plane_server_response_gets_a_fresh_bounded_io_phase() {
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let io_timeout = Duration::from_millis(100);
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        io_timeout,
    )
    .unwrap();
    let confirmation_calls = Arc::new(AtomicUsize::new(0));
    let confirmation_calls_for_policy = Arc::clone(&confirmation_calls);
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_authority_confirmation(Arc::new(move || {
        confirmation_calls_for_policy.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(io_timeout + Duration::from_millis(50));
        Ok(())
    }));
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            directory.path().join("control.state"),
        ))
        .unwrap(),
    ));
    let client = std::thread::spawn(move || {
        UnixControlPlaneClient::new(socket_path)
            .runtime_map_status_with_check_applied_timeout()
            .unwrap()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let status = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert_eq!(status.pg_routes(), 0);
    assert_eq!(confirmation_calls.load(Ordering::Acquire), 1);
}

#[test]
fn control_plane_server_response_deadline_starts_after_publication() {
    struct SlowPublication;

    impl ControlPlaneRpcResponsePublication for SlowPublication {
        fn publish(
            &self,
            publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
        ) -> Result<(), ControlPlaneError> {
            std::thread::sleep(Duration::from_millis(100));
            publish()
        }
    }

    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_millis(50),
    )
    .unwrap();
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_response_publication(Arc::new(SlowPublication));
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            directory.path().join("control.state"),
        ))
        .unwrap(),
    ));
    let client = std::thread::spawn(move || {
        UnixControlPlaneClient::new(socket_path)
            .runtime_map_status_with_check_applied_timeout()
            .unwrap()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let status = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert_eq!(status.pg_routes(), 0);
}

#[test]
fn control_plane_tls_recovery_server_dispatches_authenticated_status() {
    let directory = test_util::tempdir();
    let state_path = directory.path().join("control.state");
    let raw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = raw_listener.local_addr().unwrap();
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        raw_listener,
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(2),
    )
    .unwrap();
    let admin_credential =
        admin_auth_config_credential_with("tcp-admin", "tcp-admin", 1, "tcp-admin-secret");
    let verifier = Arc::new(
        ControlPlaneUnixAuthVerifier::new_empty("tcp-cluster")
            .unwrap()
            .with_admin_credentials(vec![admin_credential.clone()])
            .unwrap(),
    );
    let now_ms = crate::clock::current_time_millis();
    let authority_clock = Arc::new(Mutex::new(
        ControlPlaneAuthorityClock::new(None, now_ms, crate::clock::clock_health_time_millis())
            .unwrap(),
    ));
    let checkpoint_target = Arc::new(ControlPlaneAuthorityClockCheckpointTarget::new(
        &state_path,
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("tcp-cluster", 1),
    ));
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::AuthorityClockRecovery,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_auth_verifier(verifier)
    .with_authority_clock(authority_clock, checkpoint_target, false);
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap(),
    ));
    let client = std::thread::spawn(move || {
        let endpoint = control_plane_test_tls_endpoint(address);
        let client = UnixControlPlaneClient::with_endpoints([endpoint]).unwrap();
        let client = AuthenticatedUnixControlPlaneClient::new(
            client,
            admin_credential.scoped_for_cluster("tcp-cluster").unwrap(),
        );
        client.authority_clock_status(now_ms).unwrap()
    });

    listener
        .accept_one(
            &move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            &policy,
        )
        .unwrap();
    let status = client.join().unwrap();
    wait_for_control_plane_server_workers_to_finish(&policy);

    assert!(status.established());
}

#[test]
fn invalid_control_plane_auth_precedes_authority_confirmation() {
    let verifier = Arc::new(storage_node_auth_verifier(
        "auth-cluster",
        vec![storage_node_auth_node_credential(1)],
    ));
    let confirmation_calls = Arc::new(AtomicUsize::new(0));
    let confirmation_calls_for_policy = Arc::clone(&confirmation_calls);
    let policy = ControlPlaneRpcServerPolicy::new(
        ControlPlaneRpcServerRole::Ordinary,
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    )
    .unwrap()
    .with_auth_verifier(verifier)
    .with_authority_confirmation(Arc::new(move || {
        confirmation_calls_for_policy.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }));
    let request = signed_storage_node_heartbeat_request(
        &storage_node_auth_credential("wrong-cluster", 1, 1),
        &NodeHeartbeat {
            node_id: NodeId::new(1),
            node_incarnation: 1,
            endpoint: "/tmp/node-1.sock".to_owned(),
            observed_epoch: ClusterEpoch::INITIAL,
            requested_lease_duration_ms: 100,
            cluster_map_history_route_references: Default::default(),
            pg_observations: Vec::new(),
        },
        Some(2_000),
        Some(3_000),
    );

    assert!(matches!(
        authenticate_and_admit_control_plane_rpc(request, &policy, false, 2_500),
        Err(ControlPlaneRpcAdmissionFailure::Unauthenticated(_))
    ));
    assert_eq!(confirmation_calls.load(Ordering::Acquire), 0);
}

#[test]
fn control_plane_tls_tcp_endpoint_owns_protocol_profile() {
    let endpoint = ControlPlaneRpcClientEndpoint::tls_tcp(
        "tcp://localhost:7700",
        "127.0.0.1",
        7700,
        "localhost",
        Duration::from_secs(1),
        rustls::RootCertStore::empty(),
    )
    .unwrap();
    let ControlPlaneRpcClientEndpointKind::TlsTcp {
        tls_client_config, ..
    } = endpoint.0
    else {
        panic!("TLS/TCP constructor returned a Unix endpoint")
    };
    assert_eq!(
        tls_client_config.alpn_protocols,
        [CONTROL_PLANE_RPC_TLS_ALPN]
    );

    assert_eq!(
        ControlPlaneRpcClientEndpoint::tls_tcp(
            "",
            "127.0.0.1",
            7700,
            "localhost",
            Duration::from_secs(1),
            rustls::RootCertStore::empty(),
        )
        .unwrap_err(),
        ControlPlaneRpcClientEndpointError::EmptyAdvertisedEndpoint
    );
    assert_eq!(
        ControlPlaneRpcClientEndpoint::tls_tcp(
            "tcp://localhost:7700",
            "",
            7700,
            "localhost",
            Duration::from_secs(1),
            rustls::RootCertStore::empty(),
        )
        .unwrap_err(),
        ControlPlaneRpcClientEndpointError::EmptyHost
    );
    assert_eq!(
        ControlPlaneRpcClientEndpoint::tls_tcp(
            "tcp://localhost:7700",
            "127.0.0.1",
            7700,
            "not a valid server name",
            Duration::from_secs(1),
            rustls::RootCertStore::empty(),
        )
        .unwrap_err(),
        ControlPlaneRpcClientEndpointError::InvalidServerName
    );
}

#[test]
fn control_plane_tls_tcp_endpoint_exchanges_typed_frame() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server_config = control_plane_test_tls_server_config();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let connection = rustls::ServerConnection::new(server_config).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::RuntimeMapStatus);
        assert_eq!(payload, b"request");
        write_control_plane_rpc_frame(&mut stream, kind, b"response").unwrap();
    });
    let endpoint = control_plane_test_tls_endpoint(address);
    let client = UnixControlPlaneClient::with_endpoints([endpoint]).unwrap();

    let response = client
        .send_request_raw_response_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            b"request",
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();

    assert_eq!(response, b"response");
    server.join().unwrap();
}

#[test]
fn control_plane_tls_endpoint_fails_over_after_handshake_failure() {
    let handshake_failure_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let handshake_failure_address = handshake_failure_listener.local_addr().unwrap();
    let healthy_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let healthy_address = healthy_listener.local_addr().unwrap();
    let handshake_failure_server = std::thread::spawn(move || {
        let (stream, _) = handshake_failure_listener.accept().unwrap();
        drop(stream);
    });
    let server_config = control_plane_test_tls_server_config();
    let healthy_server = std::thread::spawn(move || {
        let (stream, _) = healthy_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let connection = rustls::ServerConnection::new(server_config).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &payload).unwrap();
    });
    let client = UnixControlPlaneClient::with_endpoints([
        control_plane_test_tls_endpoint(handshake_failure_address),
        control_plane_test_tls_endpoint(healthy_address),
    ])
    .unwrap();

    let response = client
        .send_request_raw_response_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            b"request",
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();

    assert_eq!(response, b"request");
    handshake_failure_server.join().unwrap();
    healthy_server.join().unwrap();
}

#[test]
fn control_plane_tls_endpoint_does_not_fail_over_after_request_publication() {
    use std::io::Read as _;

    let ambiguous_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ambiguous_address = ambiguous_listener.local_addr().unwrap();
    let unvisited_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unvisited_address = unvisited_listener.local_addr().unwrap();
    let server_config = control_plane_test_tls_server_config();
    let ambiguous_server = std::thread::spawn(move || {
        let (stream, _) = ambiguous_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let connection = rustls::ServerConnection::new(server_config).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let mut first_request_byte = [0];
        stream.read_exact(&mut first_request_byte).unwrap();
        first_request_byte[0]
    });
    let client = UnixControlPlaneClient::with_endpoints([
        control_plane_test_tls_endpoint(ambiguous_address),
        control_plane_test_tls_endpoint(unvisited_address),
    ])
    .unwrap();

    let error = client
        .send_request_raw_response_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            b"request",
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap_err();

    assert!(matches!(error, ControlPlaneError::Io { .. }));
    assert_eq!(ambiguous_server.join().unwrap(), CONTROL_PLANE_RPC_MAGIC[0]);
    unvisited_listener.set_nonblocking(true).unwrap();
    assert!(
        matches!(unvisited_listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock)
    );
}

#[test]
fn configured_control_plane_client_fails_over_only_before_request_starts() {
    let tmp = test_util::tempdir();
    let dead_socket = tmp.path().join("dead.sock");
    let healthy_socket = tmp.path().join("healthy.sock");
    let healthy_listener = std::os::unix::net::UnixListener::bind(&healthy_socket).unwrap();
    let healthy_server = std::thread::spawn(move || {
        let (mut stream, _) = healthy_listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &payload).unwrap();
    });
    let client = UnixControlPlaneClient::with_endpoints([
        ControlPlaneRpcClientEndpoint::unix(dead_socket),
        ControlPlaneRpcClientEndpoint::unix(healthy_socket),
    ])
    .unwrap();

    let response = client
        .send_request_raw_response_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            b"request",
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(response, b"request");
    healthy_server.join().unwrap();

    let ambiguous_socket = tmp.path().join("ambiguous.sock");
    let unvisited_socket = tmp.path().join("unvisited.sock");
    let ambiguous_listener = std::os::unix::net::UnixListener::bind(&ambiguous_socket).unwrap();
    let unvisited_listener = std::os::unix::net::UnixListener::bind(&unvisited_socket).unwrap();
    let ambiguous_server = std::thread::spawn(move || {
        let (mut stream, _) = ambiguous_listener.accept().unwrap();
        let _request = read_control_plane_rpc_frame(&mut stream).unwrap();
    });
    let client = UnixControlPlaneClient::with_endpoints([
        ControlPlaneRpcClientEndpoint::unix(ambiguous_socket),
        ControlPlaneRpcClientEndpoint::unix(unvisited_socket),
    ])
    .unwrap();

    let error = client
        .send_request_raw_response_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            b"request",
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic: source }
            if source.kind() == ErrorKind::UnexpectedEof
    ));
    ambiguous_server.join().unwrap();
    unvisited_listener.set_nonblocking(true).unwrap();
    assert!(
        matches!(unvisited_listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock)
    );
}

#[test]
fn authenticated_endpoint_failover_ignores_concurrent_shared_hint_changes() {
    let tmp = test_util::tempdir();
    let follower_socket = tmp.path().join("follower.sock");
    let leader_socket = tmp.path().join("leader.sock");
    let concurrent_hint_socket = tmp.path().join("concurrent-hint.sock");
    let follower_listener = std::os::unix::net::UnixListener::bind(&follower_socket).unwrap();
    let leader_listener = std::os::unix::net::UnixListener::bind(&leader_socket).unwrap();
    let concurrent_hint_listener =
        std::os::unix::net::UnixListener::bind(&concurrent_hint_socket).unwrap();
    let follower = std::thread::spawn(move || {
        let (mut stream, _) = follower_listener.accept().unwrap();
        let (kind, _) = read_control_plane_rpc_frame(&mut stream).unwrap();
        let response =
            encode_control_plane_rpc_response(Err(ControlPlaneError::AuthorityNotServing)).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });
    let leader = std::thread::spawn(move || {
        let (mut stream, _) = leader_listener.accept().unwrap();
        let (kind, _) = read_control_plane_rpc_frame(&mut stream).unwrap();
        let response = encode_control_plane_rpc_response(Ok(b"leader".to_vec())).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });
    let inner = UnixControlPlaneClient::with_endpoints([
        ControlPlaneRpcClientEndpoint::unix(follower_socket),
        ControlPlaneRpcClientEndpoint::unix(leader_socket),
        ControlPlaneRpcClientEndpoint::unix(concurrent_hint_socket),
    ])
    .unwrap();
    let concurrent_client = inner.clone();
    let client = AuthenticatedUnixControlPlaneClient::new(
        inner,
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let changed_hint = AtomicBool::new(false);

    let response = client
        .send_verified_request_with_endpoint_failover_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            Instant::now() + Duration::from_secs(1),
            || Ok(Vec::new()),
            |response| {
                if !changed_hint.swap(true, Ordering::AcqRel) {
                    // A concurrent request may update the cache after this request has
                    // already selected its endpoint pass.
                    concurrent_client.prefer_endpoint_index(2);
                }
                Ok(response.to_vec())
            },
        )
        .unwrap();

    assert_eq!(response, b"leader");
    follower.join().unwrap();
    leader.join().unwrap();
    concurrent_hint_listener.set_nonblocking(true).unwrap();
    assert!(
        matches!(concurrent_hint_listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock)
    );
    assert_eq!(client.inner().preferred_endpoint_index(), 1);
}

fn assert_snapshot_invariant_error(
    error: ControlPlaneError,
    expected_context: &'static str,
    expected_message: &str,
) {
    let ControlPlaneError::SnapshotInvariantViolation { context, message } = error else {
        panic!("unexpected control-plane error: {error}");
    };
    assert_eq!(context, expected_context);
    assert!(
        message.contains(expected_message),
        "invariant message {message:?} did not contain {expected_message:?}"
    );
}

#[derive(Debug)]
struct FailingStore {
    snapshot: ClusterControlSnapshot,
    fail_saves: Cell<bool>,
}

impl FailingStore {
    fn new(snapshot: ClusterControlSnapshot) -> Self {
        Self {
            snapshot,
            fail_saves: Cell::new(false),
        }
    }

    fn fail_saves(&self) {
        self.fail_saves.set(true);
    }
}

impl ControlPlaneStore for FailingStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        Ok(Some(self.snapshot.clone()))
    }

    fn checkpoint(
        &self,
        _previous_snapshot: Option<&ClusterControlSnapshot>,
        _next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        if self.fail_saves.get() {
            Err(ControlPlaneError::io(
                "test save failure",
                std::io::Error::other("injected save failure"),
            ))
        } else {
            Ok(())
        }
    }
}

fn canonical_snapshot_with_node() -> ClusterControlSnapshot {
    let mut snapshot = ClusterControlSnapshot::empty();
    snapshot.max_committed_timestamp_ms = Some(123);
    let mut node = NodeControlRecord::new(NodeId::new(1), NodeMembershipState::Active);
    node.observed_availability = NodeAvailabilityState::Healthy;
    node.node_incarnation = 11;
    node.endpoint = "node-1.sock".to_owned();
    node.last_observed_epoch = Some(ClusterEpoch::INITIAL);
    node.last_heartbeat_ms = Some(100);
    node.lease_deadline_ms = Some(200);
    snapshot.nodes.insert(node.node_id, node);
    snapshot
}

#[test]
fn volatile_lease_promotion_makes_acting_set_fence_durable() {
    let authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let pg_id = PgId::new(9);
    let proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 2,
        state_digest: 3,
    };
    let mut durable = canonical_snapshot_with_node();
    durable.lease_grant_horizon = Some(CommittedLeaseGrantHorizon::from_parts(authority, 900));
    durable.nodes.insert(
        NodeId::new(2),
        NodeControlRecord::new(NodeId::new(2), NodeMembershipState::Active),
    );
    durable
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .pg_observations
        .insert(
            pg_id,
            NodePgObservationRecord {
                pg_id,
                state: PgState::Active,
                observed_epoch: durable.cluster_epoch,
                observed_at_ms: 100,
                metadata_proof: proof,
                pending_metadata_command: None,
            },
        );
    durable.pgs.insert(
        pg_id,
        PgControlRecord {
            state: PgState::Active,
            active_primary: Some(NodeId::new(1)),
            active_metadata_proof: Some(proof),
            active_metadata_proof_epoch: Some(durable.cluster_epoch),
            ..PgControlRecord::new(pg_id, vec![NodeId::new(1)])
        },
    );
    validate_control_plane_snapshot("promotion test durable snapshot", &durable).unwrap();

    let mut live = durable.clone();
    live.nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .lease_deadline_ms = Some(700);
    let promotion = live
        .promote_volatile_heartbeat_leases_command(&durable)
        .unwrap()
        .expect("newer live deadline should require promotion");
    assert_eq!(
        promotion,
        ControlPlaneCommand::PromoteNodeHeartbeatLeases {
            authority,
            promoted: vec![PromotedNodeHeartbeatLease {
                node_id: NodeId::new(1),
                node_incarnation: 11,
                lease_deadline_ms: 700,
            }],
        }
    );

    let wrong_incarnation = durable
        .apply_control_plane_command(ControlPlaneCommand::PromoteNodeHeartbeatLeases {
            authority,
            promoted: vec![PromotedNodeHeartbeatLease {
                node_id: NodeId::new(1),
                node_incarnation: 12,
                lease_deadline_ms: 700,
            }],
        })
        .unwrap_err();
    assert!(matches!(
        wrong_incarnation,
        ControlPlaneError::NodeIncarnationMismatch {
            node_id: 1,
            sender_incarnation: 12,
            current_incarnation: 11,
        }
    ));
    assert_eq!(
        durable.node(NodeId::new(1)).unwrap().lease_deadline_ms(),
        Some(200)
    );

    let promoted = durable
        .apply_control_plane_command(promotion)
        .unwrap()
        .into_snapshot();
    let transitioned = promoted
        .apply_control_plane_command(ControlPlaneCommand::SetPgActingSet {
            pg_id,
            acting_set: vec![NodeId::new(1), NodeId::new(2)],
        })
        .unwrap()
        .into_snapshot();
    assert_eq!(
        transitioned
            .pg(pg_id)
            .unwrap()
            .previous_primary_lease_deadline_ms(),
        Some(700)
    );
}

#[test]
fn stale_heartbeat_after_expiry_requires_a_durable_base_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();

    let mut expired = authority.snapshot().clone();
    let horizon_authority = LeaseHorizonAuthorityBinding::new(3, Some(9));
    expired.lease_grant_horizon = Some(CommittedLeaseGrantHorizon::from_parts(
        horizon_authority,
        3_000,
    ));
    let current_epoch = expired.cluster_epoch();
    let stale_epoch = ClusterEpoch::new(current_epoch.get() - 1).unwrap();
    let record = expired.nodes.get_mut(&NodeId::new(1)).unwrap();
    record.observed_availability = NodeAvailabilityState::Unavailable;
    record.lease_deadline_ms = None;
    let heartbeat = heartbeat_from_snapshot(&expired, 1, stale_epoch, 2_000);
    let command = ControlPlaneCommand::RecordNodeHeartbeat {
        heartbeat,
        heartbeat_at_ms: 2_000,
        lease_deadline_ms: 2_100,
        lease_horizon_authority: Some(horizon_authority),
    };

    assert!(expired
        .apply_control_plane_command(command.clone())
        .unwrap()
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .is_some());
    assert!(expired
        .apply_covered_volatile_heartbeat(command)
        .unwrap()
        .is_none());
}

#[test]
fn parse_snapshot_round_trips_canonical_snapshot_bytes() {
    let snapshot = canonical_snapshot_with_node();
    let contents = format_snapshot(&snapshot);

    let parsed = parse_snapshot(&contents).unwrap();

    assert_eq!(parsed, snapshot);
    assert_eq!(format_snapshot(&parsed), contents);
}

#[test]
fn parse_snapshot_rejects_noncanonical_equivalent_bytes() {
    let canonical = format_snapshot(&canonical_snapshot_with_node());
    let without_trailing_newline = canonical.trim_end_matches('\n').to_owned();
    let zero_padded_epoch = canonical.replace("cluster_epoch=1\n", "cluster_epoch=01\n");
    let uppercase_hex = canonical.replace("6e6f64652d312e736f636b", "6E6F64652D312E736F636B");
    let duplicate_cluster_epoch =
        canonical.replace("cluster_epoch=1\n", "cluster_epoch=1\ncluster_epoch=1\n");
    let reordered_max_timestamp = canonical.replace(
        "cluster_epoch=1\ninitial_topology=-\nmax_committed_timestamp_ms=123\n",
        "max_committed_timestamp_ms=123\ncluster_epoch=1\ninitial_topology=-\n",
    );

    for (label, contents) in [
        ("missing trailing newline", without_trailing_newline),
        ("zero-padded integer", zero_padded_epoch),
        ("uppercase hex", uppercase_hex),
        ("duplicate top-level key", duplicate_cluster_epoch),
        ("reordered top-level key", reordered_max_timestamp),
    ] {
        assert!(
            matches!(
                parse_snapshot(&contents),
                Err(ControlPlaneError::Parse { message, .. })
                    if message == "control-plane state must use canonical snapshot encoding"
            ),
            "{label} should be rejected as non-canonical"
        );
    }
}

#[test]
fn parse_snapshot_rejects_serving_state_for_out_node() {
    let canonical = format_snapshot(&canonical_snapshot_with_node());
    let invalid = canonical.replace("node=1,active,1,healthy", "node=1,out,1,healthy");

    assert!(matches!(
        parse_snapshot(&invalid),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("node 1 with membership Out has administrative availability true")
    ));
}

fn heartbeat(node_id: u32, observed_epoch: ClusterEpoch, _now_ms: u64) -> NodeHeartbeat {
    NodeHeartbeat {
        node_id: NodeId::new(node_id),
        node_incarnation: 10 + u64::from(node_id),
        endpoint: format!("node-{node_id}.sock"),
        observed_epoch,
        requested_lease_duration_ms: 100,
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    }
}

struct TestControlPlaneAuth {
    cluster_id: String,
}

impl TestControlPlaneAuth {
    fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
        }
    }

    fn storage_node_config(&self, node_id: u32) -> ControlPlaneStorageNodeAuthCredential {
        self.storage_node_config_with(
            node_id,
            &format!("storage-node-{node_id}"),
            1,
            &format!("storage-node-{node_id}-secret"),
        )
    }

    fn storage_node_config_with(
        &self,
        node_id: u32,
        credential_id: &str,
        credential_version: u64,
        secret: &str,
    ) -> ControlPlaneStorageNodeAuthCredential {
        ControlPlaneStorageNodeAuthCredential::new(ControlPlaneStorageNodeAuthCredentialInput {
            node_id: NodeId::new(node_id),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: secret.as_bytes().to_vec(),
        })
        .expect("test storage-node node-scoped auth credential should build")
    }

    fn storage_node_credential(
        &self,
        node_id: u32,
        incarnation: u64,
    ) -> ControlPlaneScopedCredential {
        self.storage_node_config(node_id)
            .scoped_for_cluster_and_incarnation(&self.cluster_id, incarnation)
            .expect("test storage-node auth credential should build")
    }

    fn frontend_config(&self, instance_id: &str) -> ControlPlaneFrontendAuthCredential {
        self.frontend_config_with(
            instance_id,
            &format!("{instance_id}-credential"),
            1,
            &format!("{instance_id}-secret"),
        )
    }

    fn frontend_config_with(
        &self,
        instance_id: &str,
        credential_id: &str,
        credential_version: u64,
        secret: &str,
    ) -> ControlPlaneFrontendAuthCredential {
        ControlPlaneFrontendAuthCredential::new(ControlPlaneFrontendAuthCredentialInput {
            instance_id: instance_id.to_owned(),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: secret.as_bytes().to_vec(),
        })
        .expect("test frontend auth credential should build")
    }

    fn frontend_credential(&self, instance_id: &str) -> ControlPlaneScopedCredential {
        self.frontend_config(instance_id)
            .scoped_for_cluster(&self.cluster_id)
            .expect("test frontend scoped credential should build")
    }

    fn admin_config(&self, instance_id: &str) -> ControlPlaneAdminAuthCredential {
        self.admin_config_with(
            instance_id,
            &format!("{instance_id}-credential"),
            1,
            &format!("{instance_id}-secret"),
        )
    }

    fn admin_config_with(
        &self,
        instance_id: &str,
        credential_id: &str,
        credential_version: u64,
        secret: &str,
    ) -> ControlPlaneAdminAuthCredential {
        ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
            instance_id: instance_id.to_owned(),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: secret.as_bytes().to_vec(),
        })
        .expect("test admin auth credential should build")
    }

    fn admin_credential(&self, instance_id: &str) -> ControlPlaneScopedCredential {
        self.admin_config(instance_id)
            .scoped_for_cluster(&self.cluster_id)
            .expect("test admin scoped credential should build")
    }

    fn storage_node_verifier(
        &self,
        credentials: Vec<ControlPlaneStorageNodeAuthCredential>,
    ) -> ControlPlaneUnixAuthVerifier {
        ControlPlaneUnixAuthVerifier::new(&self.cluster_id, credentials)
            .expect("test storage-node auth verifier should build")
    }

    fn frontend_verifier(&self, instance_id: &str) -> ControlPlaneUnixAuthVerifier {
        self.storage_node_verifier(vec![self.storage_node_config(1)])
            .with_frontend_credentials(vec![self.frontend_config(instance_id)])
            .expect("test frontend auth verifier should build")
    }

    fn admin_verifier(&self, instance_id: &str) -> ControlPlaneUnixAuthVerifier {
        self.storage_node_verifier(vec![self.storage_node_config(1)])
            .with_admin_credentials(vec![self.admin_config(instance_id)])
            .expect("test admin auth verifier should build")
    }
}

fn test_auth(cluster_id: &str) -> TestControlPlaneAuth {
    TestControlPlaneAuth::new(cluster_id)
}

fn storage_node_auth_credential(
    cluster_id: &str,
    node_id: u32,
    incarnation: u64,
) -> ControlPlaneScopedCredential {
    test_auth(cluster_id).storage_node_credential(node_id, incarnation)
}

fn storage_node_auth_node_credential(node_id: u32) -> ControlPlaneStorageNodeAuthCredential {
    test_auth("auth-cluster").storage_node_config(node_id)
}

fn storage_node_auth_node_credential_with(
    node_id: u32,
    credential_id: &str,
    credential_version: u64,
    secret: &str,
) -> ControlPlaneStorageNodeAuthCredential {
    test_auth("auth-cluster").storage_node_config_with(
        node_id,
        credential_id,
        credential_version,
        secret,
    )
}

fn frontend_auth_config_credential(instance_id: &str) -> ControlPlaneFrontendAuthCredential {
    test_auth("auth-cluster").frontend_config(instance_id)
}

fn frontend_auth_config_credential_with(
    instance_id: &str,
    credential_id: &str,
    credential_version: u64,
    secret: &str,
) -> ControlPlaneFrontendAuthCredential {
    test_auth("auth-cluster").frontend_config_with(
        instance_id,
        credential_id,
        credential_version,
        secret,
    )
}

fn frontend_auth_credential(cluster_id: &str, instance_id: &str) -> ControlPlaneScopedCredential {
    test_auth(cluster_id).frontend_credential(instance_id)
}

fn admin_auth_config_credential_with(
    instance_id: &str,
    credential_id: &str,
    credential_version: u64,
    secret: &str,
) -> ControlPlaneAdminAuthCredential {
    test_auth("auth-cluster").admin_config_with(
        instance_id,
        credential_id,
        credential_version,
        secret,
    )
}

fn admin_auth_credential(cluster_id: &str, instance_id: &str) -> ControlPlaneScopedCredential {
    test_auth(cluster_id).admin_credential(instance_id)
}

fn storage_node_auth_verifier(
    cluster_id: &str,
    credentials: Vec<ControlPlaneStorageNodeAuthCredential>,
) -> ControlPlaneUnixAuthVerifier {
    test_auth(cluster_id).storage_node_verifier(credentials)
}

fn frontend_auth_verifier(cluster_id: &str, instance_id: &str) -> ControlPlaneUnixAuthVerifier {
    test_auth(cluster_id).frontend_verifier(instance_id)
}

fn admin_auth_verifier(cluster_id: &str, instance_id: &str) -> ControlPlaneUnixAuthVerifier {
    test_auth(cluster_id).admin_verifier(instance_id)
}

fn scripted_authenticated_authority_clock_response(
    request: ControlPlaneRpcRequest,
    verifier: &ControlPlaneUnixAuthVerifier,
    authority_now_ms: u64,
    response: Result<ControlPlaneAuthorityClockStatus, ControlPlaneError>,
) -> ControlPlaneRpcResponse {
    let request = verify_control_plane_unix_request(request, Some(verifier), authority_now_ms)
        .expect("scripted authority-clock request should authenticate");
    let VerifiedControlPlaneRpcRequest {
        kind,
        response_auth,
        ..
    } = request;
    let response = response.map(|status| {
        let mut payload = Vec::new();
        write_authority_clock_status(&mut payload, status);
        payload
    });
    build_control_plane_verified_response(kind, response, response_auth, authority_now_ms)
        .expect("scripted authority-clock response should encode")
}

fn signed_frontend_runtime_map_request(
    kind: ControlPlaneRpcKind,
    signer: &ControlPlaneScopedCredential,
    payload: Vec<u8>,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> ControlPlaneRpcRequest {
    let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
    let envelope = signer
        .sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Service(
                crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
            ),
            operation: ControlPlaneAuthOperation::FrontendRuntimeMapRead,
            issued_at_ms,
            expires_at_ms,
            sequence: None,
            nonce: Vec::new(),
            payload,
        })
        .expect("test frontend runtime-map read envelope should sign");
    ControlPlaneRpcRequest {
        kind,
        payload: envelope.encode_frame().unwrap(),
    }
}

fn signed_runtime_map_response_payload(
    kind: ControlPlaneRpcKind,
    frontend_signer: &ControlPlaneScopedCredential,
    payload: Vec<u8>,
    issued_at_ms: u64,
) -> Vec<u8> {
    let payload = encode_control_plane_rpc_response(Ok(payload))
        .expect("test runtime-map response should encode");
    sign_control_plane_response_payload(
        kind,
        &frontend_signer
            .runtime_map_response_credential_for_frontend()
            .expect("test frontend credential should derive runtime-map response credential"),
        frontend_signer.principal().clone(),
        ControlPlaneAuthOperation::RuntimeMapResponse,
        issued_at_ms,
        payload,
    )
    .expect("test runtime-map response envelope should sign")
}

fn signed_admin_control_plane_request(
    kind: ControlPlaneRpcKind,
    signer: &ControlPlaneScopedCredential,
    payload: Vec<u8>,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> ControlPlaneRpcRequest {
    let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
    let envelope = signer
        .sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Service(
                crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
            ),
            operation: ControlPlaneAuthOperation::AdminControlPlaneCommand,
            issued_at_ms,
            expires_at_ms,
            sequence: None,
            nonce: Vec::new(),
            payload,
        })
        .expect("test admin control-plane command envelope should sign");
    ControlPlaneRpcRequest {
        kind,
        payload: envelope.encode_frame().unwrap(),
    }
}

fn signed_storage_node_heartbeat_request(
    signer: &ControlPlaneScopedCredential,
    heartbeat: &NodeHeartbeat,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> ControlPlaneRpcRequest {
    signed_storage_node_heartbeat_request_with_embedded_kind(
        signer,
        heartbeat,
        ControlPlaneRpcKind::RefreshNodeHeartbeat,
        issued_at_ms,
        expires_at_ms,
    )
}

fn signed_storage_node_heartbeat_request_with_embedded_kind(
    signer: &ControlPlaneScopedCredential,
    heartbeat: &NodeHeartbeat,
    embedded_kind: ControlPlaneRpcKind,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> ControlPlaneRpcRequest {
    let payload = write_node_heartbeat_payload(heartbeat).unwrap();
    let payload = write_authenticated_control_plane_rpc_payload(embedded_kind, &payload);
    let envelope = signer
        .sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Service(
                crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
            ),
            operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
            issued_at_ms,
            expires_at_ms,
            sequence: None,
            nonce: Vec::new(),
            payload,
        })
        .expect("test storage-node heartbeat envelope should sign");
    ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload: envelope.encode_frame().unwrap(),
    }
}

fn heartbeat_until_serving(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    node_id: u32,
    now_ms: u64,
) -> HeartbeatLease {
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |timestamp_ms| timestamp_ms.max(now_ms));
    let first = authority
        .heartbeat(
            heartbeat(node_id, authority.snapshot().cluster_epoch(), now_ms),
            now_ms,
        )
        .unwrap();
    if first.serving() {
        first
    } else {
        authority
            .heartbeat(
                heartbeat_from_record(authority, node_id, first.cluster_epoch(), now_ms + 1),
                now_ms + 1,
            )
            .unwrap()
    }
}

fn heartbeat_until_serving_with_endpoint(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    node_id: u32,
    now_ms: u64,
    endpoint: String,
) -> HeartbeatLease {
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |timestamp_ms| timestamp_ms.max(now_ms));
    let mut heartbeat = heartbeat(node_id, authority.snapshot().cluster_epoch(), now_ms);
    heartbeat.endpoint = endpoint;
    let first = authority.heartbeat(heartbeat, now_ms).unwrap();
    if first.serving() {
        first
    } else {
        authority
            .heartbeat(
                heartbeat_from_record(authority, node_id, first.cluster_epoch(), now_ms + 1),
                now_ms + 1,
            )
            .unwrap()
    }
}

fn heartbeat_from_record<S: ControlPlaneStore>(
    authority: &SingleAuthorityControlPlane<S>,
    node_id: u32,
    observed_epoch: ClusterEpoch,
    _now_ms: u64,
) -> NodeHeartbeat {
    heartbeat_from_snapshot(authority.snapshot(), node_id, observed_epoch, _now_ms)
}

fn heartbeat_from_snapshot(
    snapshot: &ClusterControlSnapshot,
    node_id: u32,
    observed_epoch: ClusterEpoch,
    _now_ms: u64,
) -> NodeHeartbeat {
    let record = snapshot.node(NodeId::new(node_id)).unwrap();
    let mut heartbeat = heartbeat(node_id, observed_epoch, _now_ms);
    heartbeat.node_incarnation = record.node_incarnation();
    heartbeat.endpoint = record.endpoint().to_owned();
    heartbeat
}

fn history_route_references(
    references: impl IntoIterator<Item = PgClusterMapHistoryRouteReference>,
) -> PgClusterMapHistoryRouteReferences {
    PgClusterMapHistoryRouteReferences::try_from_iter(references).unwrap()
}

fn heartbeat_with_pg_observation<S: ControlPlaneStore>(
    authority: &mut SingleAuthorityControlPlane<S>,
    node_id: u32,
    pg_id: u32,
    state: PgState,
    now_ms: u64,
) -> HeartbeatLease {
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |timestamp_ms| timestamp_ms.max(now_ms));
    let mut heartbeat = heartbeat_from_record(
        authority,
        node_id,
        authority.snapshot().cluster_epoch(),
        now_ms,
    );
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(pg_id),
        state,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, now_ms).unwrap()
}

fn heartbeat_with_pg_proof<S: ControlPlaneStore>(
    authority: &mut SingleAuthorityControlPlane<S>,
    node_id: u32,
    pg_id: u32,
    state: PgState,
    metadata_proof: PgMetadataProof,
    has_pending_metadata_command: bool,
    now_ms: u64,
) -> HeartbeatLease {
    heartbeat_with_pg_proof_and_lease_duration(
        authority,
        node_id,
        pg_id,
        state,
        metadata_proof,
        has_pending_metadata_command,
        (now_ms, 100),
    )
}

fn heartbeat_with_pg_proof_and_lease_duration<S: ControlPlaneStore>(
    authority: &mut SingleAuthorityControlPlane<S>,
    node_id: u32,
    pg_id: u32,
    state: PgState,
    metadata_proof: PgMetadataProof,
    has_pending_metadata_command: bool,
    timing: (u64, u64),
) -> HeartbeatLease {
    let (now_ms, requested_lease_duration_ms) = timing;
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |timestamp_ms| timestamp_ms.max(now_ms));
    let mut heartbeat = heartbeat_from_record(
        authority,
        node_id,
        authority.snapshot().cluster_epoch(),
        now_ms,
    );
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(pg_id),
        state,
        metadata_proof,
        pending_metadata_command: has_pending_metadata_command.then_some(
            test_pending_metadata_command(authority.snapshot().cluster_epoch()),
        ),
    }];
    heartbeat.requested_lease_duration_ms = requested_lease_duration_ms;
    authority.heartbeat(heartbeat, now_ms).unwrap()
}

fn node_incarnation<S: ControlPlaneStore>(
    authority: &SingleAuthorityControlPlane<S>,
    node_id: u32,
) -> u64 {
    authority
        .snapshot()
        .node(NodeId::new(node_id))
        .unwrap()
        .node_incarnation()
}

fn persist_manually_modified_test_snapshot<S: ControlPlaneStore>(
    authority: &mut SingleAuthorityControlPlane<S>,
) {
    let previous_snapshot = authority.durable_snapshot.clone();
    authority.durable_snapshot = authority.snapshot.clone();
    authority
        .store
        .checkpoint_manually_modified_snapshot_for_test(
            &previous_snapshot,
            &authority.durable_snapshot,
        )
        .unwrap();
}

fn open_independent_file_store_restart(
    source: &FileControlPlaneStore,
    destination_path: PathBuf,
) -> SingleAuthorityControlPlane<FileControlPlaneStore> {
    std::fs::copy(source.path(), &destination_path).unwrap();
    std::fs::copy(
        single_authority_identity_path(source.path()),
        single_authority_identity_path(&destination_path),
    )
    .unwrap();
    std::fs::copy(
        single_authority_initialized_path(source.path()),
        single_authority_initialized_path(&destination_path),
    )
    .unwrap();
    if source.journal_path().exists() {
        std::fs::copy(
            source.journal_path(),
            single_authority_journal_path(&destination_path),
        )
        .unwrap();
    }
    SingleAuthorityControlPlane::open(FileControlPlaneStore::new(destination_path)).unwrap()
}

fn bucket_name(name: &str) -> crate::BucketName {
    crate::BucketName::try_from(name).expect("test bucket names must be valid")
}

fn logged_create_bucket_command(
    pg_id: PgId,
    log_index: u64,
    bucket: &crate::BucketName,
) -> MetadataCommandEnvelope {
    let owner = crate::OwnerIdentity::from_principal("owner");
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: &owner.principal,
        owner_canonical_id: &owner.canonical_id,
        acl_grants: &crate::AclGrants::default(),
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&config, 123, 1).unwrap(),
        ),
    )
}

fn logged_mark_bucket_deleting_command(
    store: &PgStore,
    log_index: u64,
    bucket: &crate::BucketName,
) -> MetadataCommandEnvelope {
    let current = store.head_bucket_record_raw(bucket).unwrap();
    let deleting_generation = store.next_bucket_execution_generation_candidate().unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(store.pg_id()),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(deleting_generation),
        )),
    )
}

fn logged_delete_finalized_bucket_command(
    store: &PgStore,
    log_index: u64,
    bucket: &crate::BucketName,
) -> MetadataCommandEnvelope {
    let deleting = store.head_bucket_record_raw(bucket).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(store.pg_id()),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            deleting.bucket_execution_generation,
            deleting.bucket_incarnation_generation,
        )),
    )
}

fn pg_metadata_proof_from_store(store: &PgStore) -> PgMetadataProof {
    let state = store.metadata_command_replica_state().unwrap();
    PgMetadataProof {
        applied_log_index: state.applied_log_index,
        applied_log_hash: state.applied_log_hash,
        state_digest: state.state_digest,
    }
}

fn placed_segment_shard_repair_work_item_for_runtime_refresh(
    seed: u8,
) -> PlacedSegmentShardRepairWorkItem {
    let mut segment_okh = [0; 16];
    segment_okh[0] = seed;
    PlacedSegmentShardRepairWorkItem {
        request: crate::SegmentStoredBytesRequest {
            data_pg_id: 31,
            segment_okh,
            segment_vid: crate::GenerationId::new(u64::from(seed) + 1).unwrap(),
            stored_size: usize::from(seed) + 1024,
            segment_crc64: u64::from(seed),
            ec: crate::EcShape { k: 1, m: 0 },
        },
        shard_index: crate::ShardIndex::new(0),
    }
}

fn metadata_command_for_runtime_refresh(
    seed: u8,
) -> crate::metadata_command::MetadataCommandEnvelope {
    crate::metadata_command::MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(31),
            crate::metadata_command::MetadataCommandLogIndex::new(u64::from(seed) + 1).unwrap(),
        ),
        crate::metadata_command::MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            crate::metadata_command::AdvanceMultipartCompletionBarrierCommand {
                bucket: crate::BucketName::try_from(format!("runtime-refresh-command-{seed}"))
                    .unwrap(),
                barrier_sequence: u64::from(seed),
            },
        ),
    )
}

#[derive(Debug, Clone, Copy)]
enum ControlPlaneHeartbeatModelOp {
    CurrentHeartbeat {
        node_slot: u8,
        observation_kind: u8,
        floor_kind: u8,
    },
    StaleHeartbeat {
        node_slot: u8,
        bump_incarnation: bool,
        change_endpoint: bool,
        include_observation: bool,
    },
    FutureHeartbeat {
        node_slot: u8,
        future_delta: u8,
        bump_incarnation: bool,
        change_endpoint: bool,
        include_observation: bool,
    },
    SetActingSet {
        shape: u8,
    },
    CompleteReadyPeerings,
    ExpireLeases {
        advance_ms: u16,
    },
    RestartAuthority,
}

#[derive(Debug, Clone, Copy)]
enum ControlPlaneHeartbeatCommandBoundaryOp {
    Current {
        node_slot: u8,
        observation_kind: u8,
        floor_kind: u8,
        bump_incarnation: bool,
        change_endpoint: bool,
    },
    Stale {
        node_slot: u8,
        stale_delta: u8,
        bump_incarnation: bool,
        change_endpoint: bool,
        include_observation: bool,
    },
    Future {
        node_slot: u8,
        future_delta: u8,
        bump_incarnation: bool,
        change_endpoint: bool,
        include_observation: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingCommandLifecycleOp {
    InstallPending,
    ConvergePending,
    Heartbeat,
    CompleteReadyPeerings,
    Restart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingCommandSlotState {
    NotInstalled,
    Pending,
    Converged,
}

#[derive(Debug, Clone, Copy)]
enum CrossPgActingSetClientAction {
    UnrelatedChurn { shape: u8 },
    Restart,
    InstallPendingRecovery,
    RecoverTarget,
    ConflictingTargetChange,
}

#[derive(Debug, Clone, Copy)]
struct CrossPgActingSetClientStep {
    action: CrossPgActingSetClientAction,
    lose_next_applied_response: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrossPgActingSetClientPhase {
    ActiveReady,
    ActiveNotReady,
    PeeringPending,
    PeeringRecovering,
    Conflict,
    Desired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingCommandLifecycleModel {
    slot: PendingCommandSlotState,
    pg_state: PgState,
    observed_pending: bool,
    peering_ready: bool,
    route_protected: bool,
}

impl PendingCommandLifecycleModel {
    fn active() -> Self {
        Self {
            slot: PendingCommandSlotState::NotInstalled,
            pg_state: PgState::Active,
            observed_pending: false,
            peering_ready: false,
            route_protected: false,
        }
    }

    fn apply(&mut self, op: PendingCommandLifecycleOp) {
        match op {
            PendingCommandLifecycleOp::InstallPending => {
                if self.slot == PendingCommandSlotState::NotInstalled {
                    self.slot = PendingCommandSlotState::Pending;
                }
            }
            PendingCommandLifecycleOp::ConvergePending => {
                if self.slot == PendingCommandSlotState::Pending {
                    self.slot = PendingCommandSlotState::Converged;
                }
            }
            PendingCommandLifecycleOp::Heartbeat => {
                self.route_protected = self.slot == PendingCommandSlotState::Pending;
                match self.pg_state {
                    PgState::Active => {
                        if self.slot == PendingCommandSlotState::Pending {
                            self.pg_state = PgState::Peering;
                            self.observed_pending = true;
                            self.peering_ready = false;
                        }
                    }
                    PgState::Peering => {
                        self.observed_pending = self.slot == PendingCommandSlotState::Pending;
                        self.peering_ready = !self.observed_pending;
                    }
                    state => {
                        panic!("pending-command model reached unexpected PG state {state:?}")
                    }
                }
            }
            PendingCommandLifecycleOp::CompleteReadyPeerings => {
                if self.pg_state == PgState::Peering && self.peering_ready {
                    self.pg_state = PgState::Active;
                    self.peering_ready = false;
                }
            }
            PendingCommandLifecycleOp::Restart => {
                self.pg_state = PgState::Peering;
                self.observed_pending = false;
                self.peering_ready = false;
            }
        }
    }
}

fn control_plane_heartbeat_model_op_strategy() -> impl Strategy<Value = ControlPlaneHeartbeatModelOp>
{
    prop_oneof![
        8 => (0_u8..3, 0_u8..6, 0_u8..4).prop_map(
            |(node_slot, observation_kind, floor_kind)| {
                ControlPlaneHeartbeatModelOp::CurrentHeartbeat {
                    node_slot,
                    observation_kind,
                    floor_kind,
                }
            }
        ),
        3 => (0_u8..3, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(node_slot, bump_incarnation, change_endpoint, include_observation)| {
                ControlPlaneHeartbeatModelOp::StaleHeartbeat {
                    node_slot,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                }
            }
        ),
        2 => (0_u8..3, 1_u8..16, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(
                node_slot,
                future_delta,
                bump_incarnation,
                change_endpoint,
                include_observation,
            )| {
                ControlPlaneHeartbeatModelOp::FutureHeartbeat {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                }
            }
        ),
        3 => (0_u8..7).prop_map(|shape| ControlPlaneHeartbeatModelOp::SetActingSet { shape }),
        2 => Just(ControlPlaneHeartbeatModelOp::CompleteReadyPeerings),
        2 => (1_u16..250).prop_map(
            |advance_ms| ControlPlaneHeartbeatModelOp::ExpireLeases { advance_ms }
        ),
        2 => Just(ControlPlaneHeartbeatModelOp::RestartAuthority),
    ]
}

fn cross_pg_acting_set_client_step_strategy() -> impl Strategy<Value = CrossPgActingSetClientStep> {
    (
        prop_oneof![
            any::<u8>()
                .prop_map(|shape| { CrossPgActingSetClientAction::UnrelatedChurn { shape } }),
            Just(CrossPgActingSetClientAction::Restart),
            Just(CrossPgActingSetClientAction::InstallPendingRecovery),
            Just(CrossPgActingSetClientAction::RecoverTarget),
            Just(CrossPgActingSetClientAction::ConflictingTargetChange),
        ],
        any::<bool>(),
    )
        .prop_map(
            |(action, lose_next_applied_response)| CrossPgActingSetClientStep {
                action,
                lose_next_applied_response,
            },
        )
}

fn control_plane_heartbeat_command_boundary_op_strategy(
) -> impl Strategy<Value = ControlPlaneHeartbeatCommandBoundaryOp> {
    prop_oneof![
        8 => (
            0_u8..3,
            0_u8..6,
            0_u8..4,
            any::<bool>(),
            any::<bool>(),
        ).prop_map(
            |(
                node_slot,
                observation_kind,
                floor_kind,
                bump_incarnation,
                change_endpoint,
            )| {
                ControlPlaneHeartbeatCommandBoundaryOp::Current {
                    node_slot,
                    observation_kind,
                    floor_kind,
                    bump_incarnation,
                    change_endpoint,
                }
            }
        ),
        3 => (0_u8..3, 1_u8..8, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(
                node_slot,
                stale_delta,
                bump_incarnation,
                change_endpoint,
                include_observation,
            )| {
                ControlPlaneHeartbeatCommandBoundaryOp::Stale {
                    node_slot,
                    stale_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                }
            }
        ),
        2 => (0_u8..3, 1_u8..16, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(
                node_slot,
                future_delta,
                bump_incarnation,
                change_endpoint,
                include_observation,
            )| {
                ControlPlaneHeartbeatCommandBoundaryOp::Future {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                }
            }
        ),
    ]
}

fn heartbeat_model_node_id(node_slot: u8) -> u32 {
    1 + u32::from(node_slot % 3)
}

fn reopen_file_authority(
    store: &FileControlPlaneStore,
) -> SingleAuthorityControlPlane<FileControlPlaneStore> {
    SingleAuthorityControlPlane::open(store.clone()).expect("test control-plane reopen")
}

fn assert_persisted_snapshot_matches_authority(
    authority: &SingleAuthorityControlPlane<FileControlPlaneStore>,
    store: &FileControlPlaneStore,
) {
    assert_eq!(
        store.load().unwrap().unwrap(),
        *authority.snapshot(),
        "persisted control-plane snapshot must match live authority"
    );
}

fn heartbeat_model_pg_id() -> PgId {
    PgId::new(41)
}

fn heartbeat_model_proof(seed: u8) -> PgMetadataProof {
    PgMetadataProof {
        applied_log_index: u64::from(seed) + 1,
        applied_log_hash: 0x1000 + u64::from(seed),
        state_digest: 0x2000 + u64::from(seed),
    }
}

fn test_pending_metadata_command(cluster_epoch: ClusterEpoch) -> PendingMetadataCommandObservation {
    PendingMetadataCommandObservation::new(cluster_epoch, NonZeroU64::MIN, 0xfeed)
}

#[test]
fn pending_recovery_listing_preserves_valid_task_after_other_pg_validation_failure() {
    let valid_pg = PgId::new(1);
    let invalid_pg = PgId::new(2);
    let historical_epoch = ClusterEpoch::INITIAL;
    let current_epoch = ClusterEpoch::new(2).unwrap();
    let primary = NodeId::new(1);
    let invalid_reporter = NodeId::new(2);

    let mut historical = ClusterControlSnapshot::empty();
    for node_id in [primary, invalid_reporter] {
        historical.nodes.insert(
            node_id,
            NodeControlRecord::new(node_id, NodeMembershipState::Active),
        );
    }
    for pg_id in [valid_pg, invalid_pg] {
        historical.pgs.insert(
            pg_id,
            PgControlRecord {
                state: PgState::Active,
                active_primary: Some(primary),
                active_metadata_proof: Some(PgMetadataProof::empty()),
                active_metadata_proof_epoch: Some(historical_epoch),
                ..PgControlRecord::new(pg_id, vec![primary, invalid_reporter])
            },
        );
    }

    let mut snapshot = historical.clone();
    snapshot.cluster_epoch = current_epoch;
    snapshot.history = vec![ClusterMapHistoryRecord::from_snapshot(&historical)];
    for pg in snapshot.pgs.values_mut() {
        pg.state = PgState::Peering;
        pg.active_primary = None;
        pg.active_metadata_proof = None;
        pg.active_metadata_proof_epoch = None;
    }
    let valid_pending =
        PendingMetadataCommandObservation::new(historical_epoch, NonZeroU64::MIN, 0x1111);
    let invalid_pending =
        PendingMetadataCommandObservation::new(historical_epoch, NonZeroU64::MIN, 0x2222);
    snapshot
        .nodes
        .get_mut(&primary)
        .unwrap()
        .pg_observations
        .insert(
            valid_pg,
            NodePgObservationRecord {
                pg_id: valid_pg,
                state: PgState::Peering,
                observed_epoch: current_epoch,
                observed_at_ms: 10,
                metadata_proof: PgMetadataProof::empty(),
                pending_metadata_command: Some(valid_pending),
            },
        );
    snapshot
        .nodes
        .get_mut(&invalid_reporter)
        .unwrap()
        .pg_observations
        .insert(
            invalid_pg,
            NodePgObservationRecord {
                pg_id: invalid_pg,
                state: PgState::Peering,
                observed_epoch: current_epoch,
                observed_at_ms: 10,
                metadata_proof: PgMetadataProof::empty(),
                pending_metadata_command: Some(invalid_pending),
            },
        );

    let listing = snapshot.pending_metadata_command_recoveries();

    assert_eq!(
        listing.tasks(),
        &[PendingMetadataCommandRecoveryTask::new(
            valid_pg,
            PendingMetadataCommandRecovery::new(primary, valid_pending),
        )]
    );
    assert_eq!(listing.failures().len(), 1);
    assert_eq!(listing.failures()[0].pg_id(), invalid_pg);
    assert_eq!(
        listing.failures()[0].kind(),
        PendingMetadataCommandRecoveryDiscoveryFailureKind::ReporterNotHistoricalPrimary
    );
}

fn heartbeat_model_acting_set(shape: u8) -> Vec<NodeId> {
    match shape % 7 {
        0 => vec![NodeId::new(1)],
        1 => vec![NodeId::new(2)],
        2 => vec![NodeId::new(3)],
        3 => vec![NodeId::new(1), NodeId::new(2)],
        4 => vec![NodeId::new(2), NodeId::new(3)],
        5 => vec![NodeId::new(1), NodeId::new(3)],
        _ => vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    }
}

fn heartbeat_model_observation(
    snapshot: &ClusterControlSnapshot,
    node_id: u32,
    observation_kind: u8,
) -> Vec<NodePgHeartbeatObservation> {
    let pg_id = heartbeat_model_pg_id();
    let Some(pg) = snapshot.pg(pg_id) else {
        return Vec::new();
    };
    if !pg.acting_set().contains(&NodeId::new(node_id)) {
        return match observation_kind % 6 {
            0 => Vec::new(),
            _ => vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: heartbeat_model_proof(observation_kind),
                pending_metadata_command: None,
            }],
        };
    }
    match observation_kind % 6 {
        0 => Vec::new(),
        1 => vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Peering,
            metadata_proof: heartbeat_model_proof(1),
            pending_metadata_command: None,
        }],
        2 => vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Peering,
            metadata_proof: heartbeat_model_proof(2),
            pending_metadata_command: Some(test_pending_metadata_command(snapshot.cluster_epoch())),
        }],
        3 => vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Active,
            metadata_proof: pg
                .active_metadata_proof()
                .unwrap_or_else(|| heartbeat_model_proof(3)),
            pending_metadata_command: None,
        }],
        4 => vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Active,
            metadata_proof: heartbeat_model_proof(observation_kind),
            pending_metadata_command: None,
        }],
        _ => vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Active,
            metadata_proof: pg
                .active_metadata_proof()
                .unwrap_or_else(|| heartbeat_model_proof(5)),
            pending_metadata_command: Some(test_pending_metadata_command(snapshot.cluster_epoch())),
        }],
    }
}

fn heartbeat_model_history_references(
    snapshot: &ClusterControlSnapshot,
    floor_kind: u8,
) -> PgClusterMapHistoryRouteReferences {
    let current_pg_id = snapshot.pgs().next().map(PgControlRecord::pg_id);
    match floor_kind % 4 {
        0 => PgClusterMapHistoryRouteReferences::default(),
        1 => current_pg_id.map_or_else(PgClusterMapHistoryRouteReferences::default, |pg_id| {
            history_route_references([PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                snapshot.cluster_epoch(),
                pg_id,
            )])
        }),
        2 => snapshot
            .cluster_map_history()
            .first()
            .and_then(|history| {
                history.pgs().first().map(|pg| {
                    history_route_references([PgClusterMapHistoryRouteReference::new(
                        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                        history.cluster_epoch(),
                        pg.pg_id(),
                    )])
                })
            })
            .unwrap_or_default(),
        _ => current_pg_id.map_or_else(PgClusterMapHistoryRouteReferences::default, |pg_id| {
            history_route_references([PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
                ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap(),
                pg_id,
            )])
        }),
    }
}

fn assert_control_plane_heartbeat_model_invariants(
    authority: &SingleAuthorityControlPlane<FileControlPlaneStore>,
    store: &FileControlPlaneStore,
    now_ms: u64,
) -> Result<(), TestCaseError> {
    let snapshot = authority.snapshot();
    if let Err(error) = snapshot.validate_publication_invariants() {
        return Err(TestCaseError::fail(format!(
            "control-plane snapshot invariant failed: {error}"
        )));
    }
    let persisted = store
        .load()
        .expect("test control-plane store load")
        .expect("test control-plane snapshot persisted");
    prop_assert_eq!(&persisted, snapshot);

    let recovery_listing = snapshot.pending_metadata_command_recoveries();
    prop_assert!(
        recovery_listing.failures().is_empty(),
        "accepted heartbeat state must not contain invalid recovery evidence: {:?}",
        recovery_listing.failures()
    );

    for node in snapshot.nodes() {
        if let Some(observed_epoch) = node.last_observed_epoch() {
            prop_assert!(
                observed_epoch <= snapshot.cluster_epoch(),
                "node {} persisted future observed epoch {} above current {}",
                node.node_id().as_u32(),
                observed_epoch,
                snapshot.cluster_epoch()
            );
        }
        for reference in node.cluster_map_history_route_references().iter() {
            prop_assert!(
                reference.cluster_epoch() <= snapshot.cluster_epoch(),
                "node {} persisted future storage history route {} above current {}",
                node.node_id().as_u32(),
                reference.cluster_epoch(),
                snapshot.cluster_epoch()
            );
            prop_assert!(
                reference.cluster_epoch() == snapshot.cluster_epoch()
                    && snapshot.pg(reference.pg_id()).is_some()
                    || snapshot
                        .reconstructed_pg_route_at_epoch(
                            reference.pg_id(),
                            reference.cluster_epoch()
                        )
                        .is_ok(),
                "node {} persisted missing storage history route ({}, PG {})",
                node.node_id().as_u32(),
                reference.cluster_epoch(),
                reference.pg_id().get()
            );
        }
        for observation in node.pg_observations() {
            prop_assert_eq!(
                observation.observed_epoch(),
                snapshot.cluster_epoch(),
                "current node PG observations must be scoped to the current epoch"
            );
            let pg = snapshot
                .pg(observation.pg_id())
                .expect("current node PG observation references a known PG");
            prop_assert!(
                pg.acting_set().contains(&node.node_id()),
                "node {} observed PG {} outside the acting set",
                node.node_id().as_u32(),
                observation.pg_id().get()
            );
            if let Some(pending) = observation.pending_metadata_command() {
                prop_assert_eq!(
                    pg.state(),
                    PgState::Peering,
                    "accepted pending-command evidence must fence the PG in Peering"
                );
                let expected = PendingMetadataCommandRecoveryTask::new(
                    observation.pg_id(),
                    PendingMetadataCommandRecovery::new(node.node_id(), pending),
                );
                prop_assert!(
                    recovery_listing.tasks().contains(&expected),
                    "accepted pending-command evidence must remain discoverable"
                );
                let historical = snapshot
                    .reconstructed_pg_route_at_epoch(observation.pg_id(), pending.cluster_epoch())
                    .expect("accepted recovery evidence retains its historical route");
                prop_assert_eq!(historical.state(), PgState::Active);
                prop_assert_eq!(historical.primary_node_id(), node.node_id());
            }
        }
    }

    for task in recovery_listing.tasks() {
        let pg = snapshot
            .pg(task.pg_id())
            .expect("recovery task references known PG");
        prop_assert_eq!(pg.state(), PgState::Peering);
    }

    for pg in snapshot.pgs() {
        prop_assert!(
            !pg.acting_set().is_empty(),
            "PG {} must not have an empty acting set",
            pg.pg_id().get()
        );
        for node_id in pg.acting_set() {
            prop_assert!(
                snapshot.node(*node_id).is_some(),
                "PG {} references unknown acting-set node {}",
                pg.pg_id().get(),
                node_id.as_u32()
            );
        }
        if pg.state() == PgState::Active {
            let primary = pg
                .active_primary()
                .expect("active PG must record an active primary");
            prop_assert!(
                pg.acting_set().contains(&primary),
                "active PG {} primary {} must be in acting set",
                pg.pg_id().get(),
                primary.as_u32()
            );
            prop_assert!(
                pg.active_metadata_proof().is_some(),
                "active PG {} must record an accepted metadata proof",
                pg.pg_id().get()
            );
            prop_assert!(
                pg.active_metadata_proof_epoch().is_some(),
                "active PG {} must record the proof epoch",
                pg.pg_id().get()
            );
            if let Ok(route) = snapshot.active_pg_route(pg.pg_id(), now_ms) {
                prop_assert_eq!(route.primary_node_id(), primary);
                prop_assert_eq!(route.state(), PgState::Active);
                prop_assert!(route.primary_lease_deadline_ms().is_some());
            }
        }
    }

    Ok(())
}

struct CrossPgActingSetClientAuthority {
    store: FileControlPlaneStore,
    authority: SingleAuthorityControlPlane<FileControlPlaneStore>,
    now_ms: u64,
    active_epoch: ClusterEpoch,
    proof: PgMetadataProof,
    expected_target_acting_set: Vec<NodeId>,
    phase: CrossPgActingSetClientPhase,
}

impl CrossPgActingSetClientAuthority {
    const TARGET_PG_ID: PgId = PgId::new(40);
    const UNRELATED_PG_ID: PgId = PgId::new(41);

    fn new(store: FileControlPlaneStore) -> Self {
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(
                heartbeat_until_serving(&mut authority, node_id, 1_000 + u64::from(node_id))
                    .serving()
            );
        }
        authority
            .set_pg_acting_set(Self::TARGET_PG_ID, Self::initial_acting_set())
            .unwrap();
        let proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            Self::TARGET_PG_ID.get(),
            PgState::Peering,
            proof,
            false,
            2_000,
        );
        authority.complete_ready_pg_peerings(2_001).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        assert!(heartbeat_with_pg_proof(
            &mut authority,
            1,
            Self::TARGET_PG_ID.get(),
            PgState::Active,
            proof,
            false,
            2_002,
        )
        .serving());

        let state = Self {
            store,
            authority,
            now_ms: 2_003,
            active_epoch,
            proof,
            expected_target_acting_set: Self::initial_acting_set(),
            phase: CrossPgActingSetClientPhase::ActiveReady,
        };
        state.assert_invariants();
        state
    }

    fn initial_acting_set() -> Vec<NodeId> {
        vec![NodeId::new(1)]
    }

    fn desired_acting_set() -> Vec<NodeId> {
        vec![NodeId::new(1), NodeId::new(2)]
    }

    fn conflicting_acting_set() -> Vec<NodeId> {
        vec![NodeId::new(1), NodeId::new(3)]
    }

    fn advance(&mut self) {
        self.now_ms += 1;
    }

    fn target_record(&self) -> &PgControlRecord {
        self.authority
            .snapshot()
            .pg(Self::TARGET_PG_ID)
            .expect("scripted target PG exists")
    }

    fn churn_unrelated_pg(&mut self, shape: u8) {
        self.advance();
        let current = self
            .authority
            .snapshot()
            .pg(Self::UNRELATED_PG_ID)
            .map(PgControlRecord::acting_set);
        let choices = [
            vec![NodeId::new(1)],
            vec![NodeId::new(2)],
            vec![NodeId::new(1), NodeId::new(2)],
        ];
        let mut acting_set = choices[usize::from(shape % 3)].clone();
        if current == Some(acting_set.as_slice()) {
            acting_set = choices[usize::from(shape.wrapping_add(1) % 3)].clone();
        }
        self.authority
            .set_pg_acting_set(Self::UNRELATED_PG_ID, acting_set)
            .unwrap();
        self.phase = match self.phase {
            CrossPgActingSetClientPhase::ActiveReady => CrossPgActingSetClientPhase::ActiveNotReady,
            CrossPgActingSetClientPhase::PeeringPending => {
                CrossPgActingSetClientPhase::PeeringRecovering
            }
            phase => phase,
        };
        self.assert_invariants();
    }

    fn refresh_target_node(&mut self, node_id: NodeId, pending: bool) {
        self.advance();
        let pg = self.target_record();
        if !pg.acting_set().contains(&node_id) {
            return;
        }
        let state = pg.state();
        let metadata_proof = pg
            .active_metadata_proof()
            .or_else(|| pg.peering_metadata_proof_floor())
            .unwrap_or(self.proof);
        let mut heartbeat = heartbeat_from_record(
            &self.authority,
            node_id.as_u32(),
            self.authority.snapshot().cluster_epoch(),
            self.now_ms,
        );
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: Self::TARGET_PG_ID,
            state,
            metadata_proof,
            pending_metadata_command: (pending && node_id == NodeId::new(1))
                .then_some(test_pending_metadata_command(self.active_epoch)),
        }];
        self.authority
            .heartbeat(heartbeat, self.now_ms)
            .expect("model-preconditioned heartbeat should apply");
    }

    fn refresh_target_acting_set(&mut self, pending_primary: bool) {
        let acting_set = self.expected_target_acting_set.clone();
        for node_id in acting_set {
            self.refresh_target_node(node_id, pending_primary);
        }
    }

    fn recover_target(&mut self) {
        if matches!(
            self.phase,
            CrossPgActingSetClientPhase::Conflict | CrossPgActingSetClientPhase::Desired
        ) {
            return;
        }
        self.refresh_target_acting_set(false);
        if self.target_record().state() == PgState::Peering {
            self.advance();
            self.authority
                .complete_ready_pg_peerings(self.now_ms)
                .unwrap();
            self.refresh_target_acting_set(false);
        }
        assert_eq!(self.target_record().state(), PgState::Active);
        self.phase = CrossPgActingSetClientPhase::ActiveReady;
        self.assert_invariants();
    }

    fn install_pending_recovery(&mut self) {
        if self.expected_target_acting_set != Self::initial_acting_set()
            || matches!(
                self.phase,
                CrossPgActingSetClientPhase::Conflict | CrossPgActingSetClientPhase::Desired
            )
        {
            return;
        }
        self.refresh_target_node(NodeId::new(1), true);
        if self
            .authority
            .snapshot()
            .pending_metadata_command_recoveries()
            .tasks()
            .iter()
            .any(|task| task.pg_id() == Self::TARGET_PG_ID)
        {
            self.phase = CrossPgActingSetClientPhase::PeeringPending;
        }
        self.assert_invariants();
    }

    fn restart(&mut self) {
        self.advance();
        self.authority = reopen_file_authority(&self.store);
        self.phase = if self.expected_target_acting_set == Self::desired_acting_set() {
            CrossPgActingSetClientPhase::Desired
        } else if self.expected_target_acting_set == Self::conflicting_acting_set() {
            CrossPgActingSetClientPhase::Conflict
        } else {
            CrossPgActingSetClientPhase::PeeringRecovering
        };
        self.assert_invariants();
    }

    fn install_conflicting_target_change(&mut self) {
        if self.expected_target_acting_set != Self::initial_acting_set() {
            return;
        }
        self.recover_target();
        self.advance();
        self.authority
            .set_pg_acting_set(Self::TARGET_PG_ID, Self::conflicting_acting_set())
            .unwrap();
        self.expected_target_acting_set = Self::conflicting_acting_set();
        self.phase = CrossPgActingSetClientPhase::Conflict;
        self.assert_invariants();
    }

    fn apply_step(&mut self, step: CrossPgActingSetClientStep) {
        match step.action {
            CrossPgActingSetClientAction::UnrelatedChurn { shape } => {
                self.churn_unrelated_pg(shape);
            }
            CrossPgActingSetClientAction::Restart => self.restart(),
            CrossPgActingSetClientAction::InstallPendingRecovery => {
                self.install_pending_recovery();
            }
            CrossPgActingSetClientAction::RecoverTarget => self.recover_target(),
            CrossPgActingSetClientAction::ConflictingTargetChange => {
                self.install_conflicting_target_change();
            }
        }
    }

    fn observe_target_command_result(&mut self, before_acting_set: &[NodeId]) -> bool {
        let after = self.target_record().acting_set();
        if after == before_acting_set {
            return false;
        }
        assert_eq!(
            after,
            Self::desired_acting_set(),
            "checked client may only install its requested target acting set"
        );
        self.expected_target_acting_set = Self::desired_acting_set();
        self.phase = CrossPgActingSetClientPhase::Desired;
        self.assert_invariants();
        true
    }

    fn assert_invariants(&self) {
        let snapshot = self.authority.snapshot();
        snapshot.validate_publication_invariants().unwrap();
        assert_eq!(
            self.store.load().unwrap().unwrap(),
            *snapshot,
            "scripted live and durable snapshots must match"
        );
        let target = snapshot.pg(Self::TARGET_PG_ID).unwrap();
        assert_eq!(
            target.acting_set(),
            self.expected_target_acting_set,
            "only an explicit target command may change the target acting set"
        );
        match self.phase {
            CrossPgActingSetClientPhase::ActiveReady
            | CrossPgActingSetClientPhase::ActiveNotReady => {
                assert_eq!(target.state(), PgState::Active);
            }
            CrossPgActingSetClientPhase::PeeringPending => {
                assert_eq!(target.state(), PgState::Peering);
                assert!(snapshot
                    .pending_metadata_command_recoveries()
                    .tasks()
                    .iter()
                    .any(|task| task.pg_id() == Self::TARGET_PG_ID));
            }
            CrossPgActingSetClientPhase::PeeringRecovering => {
                assert_eq!(target.state(), PgState::Peering);
            }
            CrossPgActingSetClientPhase::Conflict => {
                assert_eq!(
                    self.expected_target_acting_set,
                    Self::conflicting_acting_set()
                );
            }
            CrossPgActingSetClientPhase::Desired => {
                assert_eq!(self.expected_target_acting_set, Self::desired_acting_set());
            }
        }
    }
}

#[derive(Debug)]
struct CrossPgActingSetClientReport {
    snapshot: ClusterControlSnapshot,
    retry_submission_phases: Vec<CrossPgActingSetClientPhase>,
    desired_mutations: usize,
    conflict_applied: bool,
    dropped_applied_responses: usize,
    consumed_steps: usize,
}

fn cross_pg_phase_allows_retry_submission(phase: CrossPgActingSetClientPhase) -> bool {
    matches!(
        phase,
        CrossPgActingSetClientPhase::ActiveReady | CrossPgActingSetClientPhase::ActiveNotReady
    )
}

fn run_cross_pg_checked_client_schedule(
    socket_path: &std::path::Path,
    store: FileControlPlaneStore,
    steps: Vec<CrossPgActingSetClientStep>,
    authenticated: bool,
) -> (
    Result<ClusterEpoch, ControlPlaneError>,
    CrossPgActingSetClientReport,
) {
    let listener = std::os::unix::net::UnixListener::bind(socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let finished = Arc::new(AtomicBool::new(false));
    let server_finished = Arc::clone(&finished);
    let verifier = authenticated.then(|| admin_auth_verifier("model-cluster", "model-admin"));
    let server = std::thread::spawn(move || {
        let mut state = CrossPgActingSetClientAuthority::new(store);
        let mut preflight_served = false;
        let mut initial_mutation_served = false;
        let mut next_step = 0_usize;
        let mut lose_next_applied_response = false;
        let mut last_confirmation_phase = None;
        let mut retry_submission_phases = Vec::new();
        let mut desired_mutations = 0_usize;
        let mut conflict_applied = false;
        let mut dropped_applied_responses = 0_usize;

        while !server_finished.load(Ordering::Acquire) {
            let (mut stream, _) = match listener.accept() {
                Ok(accepted) => accepted,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("scripted control-plane accept failed: {error}"),
            };
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let kind = request.kind;

            if kind == ControlPlaneRpcKind::SetPgActingSet && !initial_mutation_served {
                state.churn_unrelated_pg(0);
                initial_mutation_served = true;
            } else if kind == ControlPlaneRpcKind::PgRuntimeMapSnapshot && preflight_served {
                let step = steps
                    .get(next_step)
                    .copied()
                    .unwrap_or(CrossPgActingSetClientStep {
                        action: CrossPgActingSetClientAction::RecoverTarget,
                        lose_next_applied_response: false,
                    });
                if next_step < steps.len() {
                    next_step += 1;
                }
                state.apply_step(step);
                lose_next_applied_response |= step.lose_next_applied_response;
                last_confirmation_phase = Some(state.phase);
            }

            if kind == ControlPlaneRpcKind::SetPgActingSet && initial_mutation_served {
                if let Some(phase) = last_confirmation_phase.take() {
                    assert!(
                        cross_pg_phase_allows_retry_submission(phase),
                        "checked client resubmitted from disallowed phase {phase:?}"
                    );
                    retry_submission_phases.push(phase);
                }
            }

            let before_acting_set = state.target_record().acting_set().to_vec();
            let authority_now_ms = if authenticated {
                ControlPlaneAuthEnvelope::decode_frame(
                    &request.payload,
                    CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
                )
                .unwrap()
                .header()
                .issued_at_ms()
                .unwrap()
            } else {
                state.now_ms
            };
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut state.authority,
                request,
                authority_now_ms,
                verifier.as_ref(),
                || Ok(authority_now_ms),
            )
            .unwrap();
            let desired_applied = if kind == ControlPlaneRpcKind::SetPgActingSet {
                let applied = state.observe_target_command_result(&before_acting_set);
                desired_mutations += usize::from(applied);
                applied
            } else {
                false
            };

            if kind == ControlPlaneRpcKind::PgRuntimeMapSnapshot && !preflight_served {
                preflight_served = true;
            }
            if desired_applied && lose_next_applied_response {
                lose_next_applied_response = false;
                dropped_applied_responses += 1;
                drop(stream);
                continue;
            }
            write_control_plane_unix_response(&mut stream, response).unwrap();
            conflict_applied |= state.phase == CrossPgActingSetClientPhase::Conflict;
        }

        state.assert_invariants();
        CrossPgActingSetClientReport {
            snapshot: state.authority.snapshot().clone(),
            retry_submission_phases,
            desired_mutations,
            conflict_applied,
            dropped_applied_responses,
            consumed_steps: next_step,
        }
    });

    let result = if authenticated {
        let client = AuthenticatedUnixControlPlaneClient::new(
            UnixControlPlaneClient::new(socket_path),
            admin_auth_credential("model-cluster", "model-admin"),
        );
        client.set_pg_acting_set_checked(
            CrossPgActingSetClientAuthority::TARGET_PG_ID,
            CrossPgActingSetClientAuthority::desired_acting_set(),
            crate::clock::current_time_millis(),
        )
    } else {
        UnixControlPlaneClient::new(socket_path).set_pg_acting_set_checked(
            CrossPgActingSetClientAuthority::TARGET_PG_ID,
            CrossPgActingSetClientAuthority::desired_acting_set(),
        )
    };
    finished.store(true, Ordering::Release);
    let report = server.join().unwrap();
    (result, report)
}

#[derive(Clone)]
struct PendingCommandLifecycleCase {
    snapshot: ClusterControlSnapshot,
    model: PendingCommandLifecycleModel,
    pending: PendingMetadataCommandObservation,
    active_epoch: ClusterEpoch,
    proof: PgMetadataProof,
    now_ms: u64,
}

impl PendingCommandLifecycleCase {
    fn new() -> Self {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(heartbeat_model_pg_id(), vec![NodeId::new(1)])
            .unwrap();
        let proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            proof,
            false,
            2_000,
        );
        authority.complete_ready_pg_peerings(2_010).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        let lease = heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Active,
            proof,
            false,
            2_020,
        );
        assert!(lease.serving());

        Self {
            snapshot: authority.snapshot().clone(),
            model: PendingCommandLifecycleModel::active(),
            pending: test_pending_metadata_command(active_epoch),
            active_epoch,
            proof,
            now_ms: 2_020,
        }
    }

    fn apply(&mut self, op: PendingCommandLifecycleOp, trace: &[PendingCommandLifecycleOp]) {
        self.now_ms += 1;
        match op {
            PendingCommandLifecycleOp::InstallPending
            | PendingCommandLifecycleOp::ConvergePending => {}
            PendingCommandLifecycleOp::Heartbeat => {
                let mut heartbeat = heartbeat_from_snapshot(
                    &self.snapshot,
                    1,
                    self.snapshot.cluster_epoch(),
                    self.now_ms,
                );
                heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
                    pg_id: heartbeat_model_pg_id(),
                    state: self.model.pg_state,
                    metadata_proof: self
                        .snapshot
                        .pg(heartbeat_model_pg_id())
                        .and_then(PgControlRecord::active_metadata_proof)
                        .unwrap_or(self.proof),
                    pending_metadata_command: (self.model.slot == PendingCommandSlotState::Pending)
                        .then_some(self.pending),
                }];
                if self.model.slot == PendingCommandSlotState::Pending {
                    heartbeat.cluster_map_history_route_references =
                        history_route_references([PgClusterMapHistoryRouteReference::new(
                            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
                            self.active_epoch,
                            heartbeat_model_pg_id(),
                        )]);
                }
                let lease_deadline_ms = self.now_ms + heartbeat.requested_lease_duration_ms;
                let applied = self
                    .snapshot
                    .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                        heartbeat,
                        heartbeat_at_ms: self.now_ms,
                        lease_deadline_ms,
                        lease_horizon_authority: None,
                    })
                    .unwrap_or_else(|error| {
                        panic!("heartbeat failed for lifecycle trace {trace:?}: {error}")
                    });
                self.snapshot = applied.into_snapshot();
            }
            PendingCommandLifecycleOp::CompleteReadyPeerings => {
                let ready = self
                    .snapshot
                    .ready_pg_peering_completions(self.now_ms)
                    .unwrap_or_else(|error| {
                        panic!("ready-peering scan failed for lifecycle trace {trace:?}: {error}")
                    });
                let expected_ready =
                    self.model.pg_state == PgState::Peering && self.model.peering_ready;
                assert_eq!(
                    ready.len(),
                    usize::from(expected_ready),
                    "ready-peering mismatch for lifecycle trace {trace:?}"
                );
                let applied = self
                    .snapshot
                    .apply_control_plane_command(ControlPlaneCommand::CompleteReadyPgPeerings {
                        ready_at_ms: self.now_ms,
                        ready,
                    })
                    .unwrap_or_else(|error| {
                        panic!("peering completion failed for lifecycle trace {trace:?}: {error}")
                    });
                self.snapshot = applied.into_snapshot();
            }
            PendingCommandLifecycleOp::Restart => {
                let mut restarted = parse_snapshot(&format_snapshot(&self.snapshot))
                    .unwrap_or_else(|error| {
                        panic!("restart failed for lifecycle trace {trace:?}: {error}")
                    });
                let previous = restarted.clone();
                restarted
                    .bump_authority_after_restart()
                    .unwrap_or_else(|error| {
                        panic!("restart bump failed for lifecycle trace {trace:?}: {error}")
                    });
                restarted.record_history_from(&previous);
                self.snapshot = restarted;
            }
        }
        self.model.apply(op);
        self.assert_matches_model(trace);
    }

    fn assert_matches_model(&self, trace: &[PendingCommandLifecycleOp]) {
        self.snapshot
            .validate_publication_invariants()
            .unwrap_or_else(|error| {
                panic!("snapshot invariant failed for lifecycle trace {trace:?}: {error}")
            });
        let pg = self
            .snapshot
            .pg(heartbeat_model_pg_id())
            .expect("model PG exists");
        assert_eq!(
            pg.state(),
            self.model.pg_state,
            "PG state mismatch for lifecycle trace {trace:?}"
        );

        let listing = self.snapshot.pending_metadata_command_recoveries();
        assert!(
            listing.failures().is_empty(),
            "recovery discovery failed for lifecycle trace {trace:?}: {:?}",
            listing.failures()
        );
        let expected_tasks =
            self.model
                .observed_pending
                .then_some(PendingMetadataCommandRecoveryTask::new(
                    heartbeat_model_pg_id(),
                    PendingMetadataCommandRecovery::new(NodeId::new(1), self.pending),
                ));
        assert_eq!(
            listing.tasks(),
            expected_tasks.as_slice(),
            "recovery task mismatch for lifecycle trace {trace:?}"
        );

        let expected_reference = PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
            self.active_epoch,
            heartbeat_model_pg_id(),
        );
        let references = self
            .snapshot
            .node(NodeId::new(1))
            .expect("model node exists")
            .cluster_map_history_route_references();
        assert_eq!(
            references
                .iter()
                .any(|reference| reference == expected_reference),
            self.model.route_protected,
            "pending route protection mismatch for lifecycle trace {trace:?}"
        );

        if self.model.observed_pending || self.model.route_protected {
            let historical = self
                .snapshot
                .reconstructed_pg_route_at_epoch(heartbeat_model_pg_id(), self.active_epoch)
                .expect("pending recovery retains historical Active route");
            assert_eq!(historical.state(), PgState::Active);
            assert_eq!(historical.primary_node_id(), NodeId::new(1));
            assert!(
                self.snapshot
                    .ready_pg_peering_completions(self.now_ms)
                    .unwrap()
                    .is_empty(),
                "pending recovery must block peering completion for trace {trace:?}"
            );
        }
    }
}

fn explore_pending_command_lifecycle_traces(
    case: PendingCommandLifecycleCase,
    trace: &mut Vec<PendingCommandLifecycleOp>,
    remaining: usize,
    visited: &mut usize,
) {
    const OPS: [PendingCommandLifecycleOp; 5] = [
        PendingCommandLifecycleOp::InstallPending,
        PendingCommandLifecycleOp::ConvergePending,
        PendingCommandLifecycleOp::Heartbeat,
        PendingCommandLifecycleOp::CompleteReadyPeerings,
        PendingCommandLifecycleOp::Restart,
    ];

    if remaining == 0 {
        return;
    }
    for op in OPS {
        let mut child = case.clone();
        trace.push(op);
        child.apply(op, trace);
        *visited += 1;
        explore_pending_command_lifecycle_traces(child, trace, remaining - 1, visited);
        trace.pop();
    }
}

#[test]
fn pending_command_heartbeat_lifecycle_model_exhausts_short_interleavings() {
    let initial = PendingCommandLifecycleCase::new();
    initial.assert_matches_model(&[]);
    let mut trace = Vec::new();
    let mut visited = 1;
    explore_pending_command_lifecycle_traces(initial, &mut trace, 6, &mut visited);
    assert_eq!(visited, 19_531);
}

#[test]
fn pending_command_heartbeat_lifecycle_survives_restarts_and_reactivates() {
    let mut case = PendingCommandLifecycleCase::new();
    let mut trace = Vec::new();
    for op in [
        PendingCommandLifecycleOp::InstallPending,
        PendingCommandLifecycleOp::Heartbeat,
        PendingCommandLifecycleOp::Restart,
        PendingCommandLifecycleOp::Heartbeat,
        PendingCommandLifecycleOp::CompleteReadyPeerings,
        PendingCommandLifecycleOp::ConvergePending,
        PendingCommandLifecycleOp::Restart,
        PendingCommandLifecycleOp::Heartbeat,
        PendingCommandLifecycleOp::Restart,
        PendingCommandLifecycleOp::Heartbeat,
        PendingCommandLifecycleOp::CompleteReadyPeerings,
        PendingCommandLifecycleOp::Heartbeat,
    ] {
        trace.push(op);
        case.apply(op, &trace);
    }
    assert_eq!(case.model.pg_state, PgState::Active);
    assert!(!case.model.observed_pending);
    assert!(case
        .snapshot
        .active_pg_route(heartbeat_model_pg_id(), case.now_ms)
        .is_ok());
}

#[test]
fn checked_clients_compose_pending_restart_and_lost_applied_response() {
    let steps = vec![
        CrossPgActingSetClientStep {
            action: CrossPgActingSetClientAction::InstallPendingRecovery,
            lose_next_applied_response: false,
        },
        CrossPgActingSetClientStep {
            action: CrossPgActingSetClientAction::Restart,
            lose_next_applied_response: false,
        },
        CrossPgActingSetClientStep {
            action: CrossPgActingSetClientAction::RecoverTarget,
            lose_next_applied_response: true,
        },
        CrossPgActingSetClientStep {
            action: CrossPgActingSetClientAction::Restart,
            lose_next_applied_response: false,
        },
    ];
    for authenticated in [false, true] {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let (result, report) =
            run_cross_pg_checked_client_schedule(&socket_path, store, steps.clone(), authenticated);
        result.unwrap();
        assert_eq!(report.consumed_steps, steps.len());
        assert_eq!(report.desired_mutations, 1);
        assert_eq!(report.dropped_applied_responses, 1);
        assert!(!report.conflict_applied);
        assert_eq!(
            report
                .snapshot
                .pg(CrossPgActingSetClientAuthority::TARGET_PG_ID)
                .unwrap()
                .acting_set(),
            CrossPgActingSetClientAuthority::desired_acting_set()
        );
        assert!(report
            .retry_submission_phases
            .iter()
            .copied()
            .all(cross_pg_phase_allows_retry_submission));
    }
}

#[test]
fn checked_clients_fail_closed_after_generated_target_conflict() {
    let steps = vec![CrossPgActingSetClientStep {
        action: CrossPgActingSetClientAction::ConflictingTargetChange,
        lose_next_applied_response: false,
    }];
    for authenticated in [false, true] {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let (result, report) =
            run_cross_pg_checked_client_schedule(&socket_path, store, steps.clone(), authenticated);
        assert!(
            matches!(result, Err(ControlPlaneError::RpcUnconfirmed { .. })),
            "unexpected checked-client conflict result: {result:?}"
        );
        assert_eq!(report.consumed_steps, steps.len());
        assert_eq!(report.desired_mutations, 0);
        assert!(report.conflict_applied);
        assert_eq!(
            report
                .snapshot
                .pg(CrossPgActingSetClientAuthority::TARGET_PG_ID)
                .unwrap()
                .acting_set(),
            CrossPgActingSetClientAuthority::conflicting_acting_set()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        max_shrink_iters: 128,
        ..ProptestConfig::default()
    })]

    #[test]
    fn prop_checked_clients_preserve_target_route_across_generated_interleavings(
        steps in proptest::collection::vec(cross_pg_acting_set_client_step_strategy(), 0..8),
        authenticated in any::<bool>(),
    ) {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let (result, report) = run_cross_pg_checked_client_schedule(
            &socket_path,
            store,
            steps.clone(),
            authenticated,
        );
        prop_assert!(report.consumed_steps <= steps.len());
        prop_assert!(report.desired_mutations <= 1);
        prop_assert!(report.dropped_applied_responses <= 1);
        prop_assert!(report
            .retry_submission_phases
            .iter()
            .copied()
            .all(cross_pg_phase_allows_retry_submission));

        let final_acting_set = report
            .snapshot
            .pg(CrossPgActingSetClientAuthority::TARGET_PG_ID)
            .unwrap()
            .acting_set();
        if report.conflict_applied {
            let failed_closed = matches!(
                result,
                Err(ControlPlaneError::RpcUnconfirmed { .. })
            );
            prop_assert!(failed_closed);
            prop_assert_eq!(
                final_acting_set,
                CrossPgActingSetClientAuthority::conflicting_acting_set()
            );
            prop_assert_eq!(report.desired_mutations, 0);
        } else {
            prop_assert!(result.is_ok(), "unexpected checked-client result: {:?}", result);
            prop_assert_eq!(
                final_acting_set,
                CrossPgActingSetClientAuthority::desired_acting_set()
            );
            prop_assert_eq!(report.desired_mutations, 1);
        }
    }
}

#[test]
fn active_metadata_proof_floor_accepts_only_same_or_later_progress() {
    let active_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    assert!(metadata_proof_satisfies_active_floor(
        active_floor,
        active_floor
    ));
    assert!(metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof {
            applied_log_index: 43,
            applied_log_hash: 0xabd,
            state_digest: 0xdf0,
        },
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof {
            applied_log_index: 41,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        },
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabd,
            state_digest: 0xdef,
        },
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdf0,
        },
    ));
}

#[test]
fn active_metadata_observation_rejects_epoch_local_progress_after_activation() {
    let imported_activation_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };

    assert!(metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        imported_activation_floor
    ));
    assert!(metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof {
            applied_log_index: 43,
            applied_log_hash: 0xabd,
            state_digest: 0xdf0,
        },
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof {
            applied_log_index: 1,
            applied_log_hash: 0x123,
            state_digest: 0x456,
        },
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabd,
            state_digest: 0xdf0,
        },
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof {
            applied_log_index: 41,
            applied_log_hash: 0,
            state_digest: 0x456,
        },
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof {
            applied_log_index: 41,
            applied_log_hash: 0x123,
            state_digest: 0xdef,
        },
    ));
}

#[test]
fn imported_transfer_local_progress_floor_requires_new_log_hash_and_digest() {
    let imported_activation_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };

    assert!(
        metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 1,
                applied_log_hash: 0x123,
                state_digest: 0x456,
            },
        )
    );
    assert!(
        metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 42,
                applied_log_hash: 0x123,
                state_digest: 0x456,
            },
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 42,
                applied_log_hash: 0xabc,
                state_digest: 0xdf0,
            },
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 41,
                applied_log_hash: 0,
                state_digest: 0x456,
            },
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 41,
                applied_log_hash: 0x123,
                state_digest: 0xdef,
            },
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof {
                applied_log_index: 42,
                applied_log_hash: 0x123,
                state_digest: 0xdef,
            },
        )
    );
}

#[test]
fn active_primary_observation_floor_scopes_epoch_local_progress() {
    let imported_activation_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let local_epoch_progress = Some(MetadataProofProgressProvenance {
        floor_epoch: ClusterEpoch::new(7).unwrap(),
        kind: MetadataProofProgressKind::LocalEpoch,
    });
    let imported_transfer_progress = Some(MetadataProofProgressProvenance {
        floor_epoch: ClusterEpoch::new(7).unwrap(),
        kind: MetadataProofProgressKind::ImportedTransfer,
    });

    let lower_epoch_local_proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 0x123,
        state_digest: 0x456,
    };
    let same_index_epoch_local_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0x123,
        state_digest: 0x456,
    };
    let same_log_digest_only_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdf0,
    };
    let malformed_epoch_local_proof = PgMetadataProof {
        applied_log_index: 41,
        applied_log_hash: 0,
        state_digest: 0x456,
    };

    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        lower_epoch_local_proof,
        local_epoch_progress,
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        lower_epoch_local_proof,
        imported_transfer_progress,
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        lower_epoch_local_proof,
        imported_transfer_progress,
        ClusterEpoch::new(8).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        lower_epoch_local_proof,
        None,
        ClusterEpoch::new(8).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        same_index_epoch_local_proof,
        local_epoch_progress,
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        same_index_epoch_local_proof,
        local_epoch_progress,
        ClusterEpoch::new(8).unwrap(),
    ));
    assert!(metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        same_index_epoch_local_proof,
        imported_transfer_progress,
        ClusterEpoch::new(8).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        same_log_digest_only_proof,
        imported_transfer_progress,
        ClusterEpoch::new(8).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        same_index_epoch_local_proof,
        imported_transfer_progress,
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        imported_activation_floor,
        malformed_epoch_local_proof,
        imported_transfer_progress,
        ClusterEpoch::new(7).unwrap(),
    ));
}

#[test]
fn active_primary_observation_floor_rejects_same_epoch_digest_only_progress() {
    let active_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let digest_only_progress = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdf0,
    };
    let divergent_log_hash = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabd,
        state_digest: 0xdf0,
    };
    let zero_hash_digest_only_progress = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0,
        state_digest: 0xdf0,
    };

    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        active_floor,
        digest_only_progress,
        Some(MetadataProofProgressProvenance {
            floor_epoch: ClusterEpoch::new(7).unwrap(),
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        active_floor,
        digest_only_progress,
        None,
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        active_floor,
        divergent_log_hash,
        Some(MetadataProofProgressProvenance {
            floor_epoch: ClusterEpoch::new(7).unwrap(),
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        ClusterEpoch::new(7).unwrap(),
    ));
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0,
            state_digest: 0xdef,
        },
        zero_hash_digest_only_progress,
        Some(MetadataProofProgressProvenance {
            floor_epoch: ClusterEpoch::new(7).unwrap(),
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        ClusterEpoch::new(7).unwrap(),
    ));
}

#[test]
fn authoritative_migration_source_does_not_invent_missing_active_proof_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(22);
    let active_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        active_floor,
        false,
        2_020,
    );

    let digest_only_progress = PgMetadataProof {
        applied_log_index: active_floor.applied_log_index,
        applied_log_hash: active_floor.applied_log_hash,
        state_digest: active_floor.state_digest + 1,
    };
    let mut snapshot = authority.snapshot().clone();
    snapshot
        .pgs
        .get_mut(&pg_id)
        .unwrap()
        .active_metadata_proof_epoch = None;
    snapshot
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .pg_observations
        .get_mut(&pg_id)
        .unwrap()
        .metadata_proof = digest_only_progress;
    let record = snapshot.pg(pg_id).unwrap();

    assert!(matches!(
        validate_authoritative_metadata_migration_source(
            &snapshot,
            record,
            &[NodeId::new(1), NodeId::new(3)],
        ),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 22 })
    ));
}

#[test]
fn control_snapshot_invariants_reject_active_pg_with_peering_transfer_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let pg_id = PgId::new(23);
    let proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let mut snapshot = authority.snapshot().clone();
    snapshot
        .pgs
        .get_mut(&pg_id)
        .unwrap()
        .peering_metadata_proof_floor = Some(proof);
    let error = snapshot.validate_publication_invariants().unwrap_err();
    assert!(
        error.contains("carries peering metadata-transfer state"),
        "unexpected invariant error: {error}"
    );
}

#[test]
fn control_snapshot_invariants_reject_transfer_destination_with_source_fence() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(24);
    let proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        proof,
        false,
        2_020,
    );

    let transfer = PgMetadataTransferProof::new(authority.snapshot().cluster_epoch(), proof);
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], transfer)
        .unwrap();
    let mut snapshot = authority.snapshot().clone();
    snapshot
        .pgs
        .get_mut(&pg_id)
        .unwrap()
        .metadata_transfer_fenced = true;
    let error = snapshot.validate_publication_invariants().unwrap_err();
    assert!(
        error.contains("destination metadata transfer state and source transfer fence"),
        "unexpected invariant error: {error}"
    );
}

#[test]
fn control_snapshot_invariants_reject_transfer_proof_below_floor() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(25);
    let source_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        source_proof,
        false,
        2_020,
    );

    let imported_proof = PgMetadataProof {
        applied_log_index: 8,
        applied_log_hash: 9,
        state_digest: 10,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        authority.snapshot().cluster_epoch(),
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], transfer)
        .unwrap();
    let mut snapshot = authority.snapshot().clone();
    let pg = snapshot.pgs.get_mut(&pg_id).unwrap();
    pg.peering_metadata_proof_floor = Some(PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    });
    let error = snapshot.validate_publication_invariants().unwrap_err();
    assert!(
        error.contains("metadata transfer proof is below the proof floor"),
        "unexpected invariant error: {error}"
    );
}

#[test]
fn direct_snapshot_commit_validates_control_plane_invariants() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let persisted_store = store.clone();
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let pg_id = PgId::new(26);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();

    let mut snapshot = authority.snapshot().clone();
    let pg = snapshot.pgs.get_mut(&pg_id).unwrap();
    pg.state = PgState::Active;
    pg.active_primary = Some(NodeId::new(1));
    pg.active_metadata_proof = Some(PgMetadataProof::empty());
    let before = authority.snapshot().clone();
    let error = authority.commit_snapshot(snapshot).unwrap_err();
    assert_snapshot_invariant_error(
        error,
        "attempted to commit invalid control-plane snapshot",
        "has no metadata proof epoch",
    );
    assert_eq!(authority.snapshot(), &before);
    assert_eq!(persisted_store.load().unwrap().as_ref(), Some(&before));
}

#[test]
fn command_apply_rejects_invalid_snapshot_before_publication() {
    let snapshot =
        ClusterControlSnapshot::test_invalid_active_without_metadata_proof_epoch(PgId::new(27));

    let error = snapshot
        .apply_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(1),
            availability: NodeAvailabilityState::Suspect,
        })
        .unwrap_err();

    assert_snapshot_invariant_error(
        error,
        "control-plane command produced invalid snapshot",
        "has no metadata proof epoch",
    );
}

#[test]
fn retained_history_structure_remains_a_debug_audit() {
    let mut snapshot = ClusterControlSnapshot::empty();
    let history = ClusterMapHistoryRecord::from_snapshot(&snapshot);
    snapshot.history.push(history);

    snapshot.validate_publication_invariants().unwrap();
    let error = snapshot.validate_audit_invariants().unwrap_err();
    assert!(
        error.contains("history epoch 1 is not older than current epoch 1"),
        "unexpected audit error: {error}"
    );
}

#[test]
fn retained_history_transfer_chain_remains_a_debug_audit() {
    let pg_id = PgId::new(7);
    let mut snapshot = ClusterControlSnapshot::empty();
    snapshot.cluster_epoch = ClusterEpoch::new(3).unwrap();
    snapshot.history = vec![
        ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::INITIAL,
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            pgs: vec![HistoricalPgRouteRecord {
                pg_id,
                state: PgState::Active,
                acting_set: vec![NodeId::new(2)],
                active_primary: Some(NodeId::new(2)),
                peering_metadata_transfer: None,
                peering_metadata_transfer_source_route_epoch: None,
                peering_metadata_transfer_source_node_id: None,
            }],
            absent_pgs: Vec::new(),
        },
        ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(2).unwrap(),
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            pgs: vec![HistoricalPgRouteRecord {
                pg_id,
                state: PgState::Peering,
                acting_set: vec![NodeId::new(1)],
                active_primary: None,
                peering_metadata_transfer: Some(PgMetadataTransferProof::new(
                    ClusterEpoch::INITIAL,
                    PgMetadataProof::empty(),
                )),
                peering_metadata_transfer_source_route_epoch: Some(ClusterEpoch::INITIAL),
                peering_metadata_transfer_source_node_id: Some(NodeId::new(1)),
            }],
            absent_pgs: Vec::new(),
        },
    ];

    let error = snapshot.validate_audit_invariants().unwrap_err();

    assert!(
        error.contains(
            "metadata transfer source node 1 does not match source route primary 2 at epoch 1"
        ),
        "unexpected audit error: {error}"
    );
}

#[test]
fn open_validates_control_plane_invariants_before_saving() {
    let pg_id = PgId::new(28);
    let mut snapshot = ClusterControlSnapshot::empty();
    snapshot.nodes.insert(
        NodeId::new(1),
        NodeControlRecord::new(NodeId::new(1), NodeMembershipState::Active),
    );
    snapshot.pgs.insert(
        pg_id,
        PgControlRecord {
            pg_id,
            state: PgState::Peering,
            acting_set: vec![NodeId::new(1)],
            peering_metadata_transfer: Some(PgMetadataTransferProof::new(
                ClusterEpoch::new(7).unwrap(),
                PgMetadataProof::empty(),
            )),
            ..PgControlRecord::new(pg_id, vec![NodeId::new(1)])
        },
    );

    let store = FailingStore::new(snapshot);
    let error = match SingleAuthorityControlPlane::open(store) {
        Ok(_) => panic!("invalid control-plane snapshot unexpectedly opened"),
        Err(error) => error,
    };
    assert_snapshot_invariant_error(
        error,
        "attempted to open invalid control-plane snapshot",
        "has a transfer marker without a proof floor",
    );
}

#[test]
fn replicated_state_machine_constructor_validates_control_plane_invariants() {
    let snapshot =
        ClusterControlSnapshot::test_invalid_active_without_metadata_proof_epoch(PgId::new(29));
    let error = crate::control_plane_command::ReplicatedControlPlaneStateMachine::new(
        snapshot,
        crate::control_plane_command::ControlPlaneLogId::new(1, 1),
    )
    .unwrap_err();
    assert_snapshot_invariant_error(
        error,
        "attempted to create replicated state machine from invalid control-plane snapshot",
        "has no metadata proof epoch",
    );
}

#[test]
fn replicated_snapshot_install_validates_control_plane_invariants_before_mutation() {
    let invalid_snapshot =
        ClusterControlSnapshot::test_invalid_active_without_metadata_proof_epoch(PgId::new(30));
    let payload =
        crate::control_plane_command::encode_control_plane_snapshot(&invalid_snapshot).unwrap();
    assert!(matches!(
        crate::control_plane_command::decode_control_plane_snapshot(&payload),
        Err(ControlPlaneError::Parse { line: 0, message })
            if message.contains("has no metadata proof epoch")
    ));
    let mut state_machine =
        crate::control_plane_command::ReplicatedControlPlaneStateMachine::empty();
    let before = state_machine.clone();
    let error = state_machine
        .install_snapshot_artifact(
            crate::control_plane_command::ControlPlaneSnapshotArtifact::new(None, payload),
        )
        .unwrap_err();
    assert_snapshot_invariant_error(
        error,
        "attempted to install invalid replicated control-plane snapshot",
        "has no metadata proof epoch",
    );
    assert_eq!(state_machine, before);
}

#[test]
fn peering_proof_floor_rejects_uncommitted_digest_only_progress() {
    let active_floor = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let digest_only_progress = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdf0,
    };

    assert!(matches!(
        validate_peering_metadata_proof_floor(
            ClusterEpoch::new(7).unwrap(),
            PgId::new(22),
            NodeId::new(1),
            Some(PeeringMetadataProofFloor {
                proof: active_floor,
                epoch: Some(ClusterEpoch::new(7).unwrap()),
                imported: false,
            }),
            None,
            digest_only_progress,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == active_floor && actual == digest_only_progress
    ));
}

#[path = "tests/rpc.rs"]
mod rpc;
use rpc::runtime_map_test_snapshot_with_active_route;

#[test]
fn file_backed_authority_restarts_with_never_reused_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let first_lease = authority
        .heartbeat(
            heartbeat(1, authority.snapshot().cluster_epoch(), 1_000),
            1_000,
        )
        .unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().max_committed_timestamp_ms(),
        Some(1_000)
    );

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > first_lease.authority_incarnation());
    assert!(restarted.snapshot().cluster_epoch() > first_lease.cluster_epoch());
    assert_eq!(
        restarted.snapshot().max_committed_timestamp_ms(),
        Some(1_000)
    );
}

#[test]
fn file_backed_authority_restarts_empty_state_with_new_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > AuthorityIncarnation::INITIAL);
    assert!(restarted.snapshot().cluster_epoch() > ClusterEpoch::INITIAL);
    assert_eq!(restarted.snapshot().nodes().count(), 0);
}

#[test]
fn file_backed_authority_rejects_pre_v7_state() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=2\nauthority_incarnation=1\ncluster_epoch=1\nnode=1,active,1,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_version_twenty_six_state() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=26\nauthority_incarnation=1\ncluster_epoch=1\ninitial_topology=-\n",
    )
    .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(path).load(),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_state_missing_timestamp_high_water() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=27\nauthority_incarnation=1\ncluster_epoch=1\ninitial_topology=-\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing max committed timestamp"
    ));
}

#[test]
fn initial_216_pg_placement_uses_sparse_history_deltas() {
    const PG_COUNT: u32 = 216;

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    for pg_id in 0..PG_COUNT {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
    }

    let snapshot = authority.snapshot();
    assert_eq!(snapshot.pgs().count(), PG_COUNT as usize);
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.pgs().len())
            .sum::<usize>(),
        0,
        "introducing PGs must not copy every previously configured route"
    );
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.absent_pgs.len())
            .sum::<usize>(),
        PG_COUNT as usize
    );
    let runtime_map = snapshot.runtime_map(1_001).unwrap();
    assert!(
        runtime_map.historical_pg_routes().len()
            <= snapshot.cluster_map_history().len() + PG_COUNT as usize
    );
    let persisted = std::fs::read(store.path()).unwrap();
    assert!(
        persisted.len() < 128 * 1_024,
        "216-PG initial placement state unexpectedly grew to {} bytes",
        persisted.len()
    );
    assert_eq!(store.load().unwrap().as_ref(), Some(snapshot));
}

#[test]
fn sparse_runtime_map_round_trip_preserves_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let before_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();
    let after_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();

    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), before_introduction),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    assert_eq!(
        decoded
            .reconstructed_pg_route_at_epoch(PgId::new(2), after_introduction)
            .unwrap()
            .acting_set(),
        &[NodeId::new(1)]
    );
}

#[test]
fn sparse_runtime_map_round_trip_preserves_epoch_before_first_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let no_pg_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    assert!(runtime_map
        .historical_cluster_epochs()
        .contains(&no_pg_epoch));
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
}

#[test]
fn cluster_map_history_is_persisted_across_epoch_changes_and_pruned() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    let persisted = store.load().unwrap().unwrap();
    let initial_history = persisted.cluster_map_at_epoch(initial_epoch).unwrap();
    assert_eq!(
        initial_history.authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(initial_history.nodes().len(), 0);
    assert_eq!(initial_history.pgs().len(), 0);

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let pg_epoch = authority.snapshot().cluster_epoch();
    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let before_restart = restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(3), pg_epoch)
        .unwrap();
    assert_eq!(
        before_restart.acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );

    let mut authority = restarted;
    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT);
    assert!(history.first().unwrap().cluster_epoch() > initial_epoch);
    assert!(history.last().unwrap().cluster_epoch() < authority.snapshot().cluster_epoch());

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(
        persisted.cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    assert!(persisted.cluster_map_at_epoch(initial_epoch).is_none());
}

#[test]
fn cluster_map_history_pruning_preserves_metadata_transfer_route_epochs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let initial_epoch = ClusterEpoch::INITIAL;
    let source_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 12,
        state_digest: 11,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let destination_epoch = authority.snapshot().cluster_epoch();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 2);
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    let protected_source = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap();
    assert!(protected_source.pg(PgId::new(42)).is_some());
    assert!(protected_source.pg(PgId::new(43)).is_none());
    assert_eq!(protected_source.pgs().len(), 1);
    let protected_destination = authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .unwrap();
    assert!(protected_destination.pg(PgId::new(43)).is_none());
    let destination_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(42), destination_epoch)
        .unwrap();
    assert_eq!(
        destination_route.peering_metadata_transfer(),
        Some(transfer)
    );
    assert_eq!(
        destination_route.peering_metadata_transfer_destination_epoch(),
        Some(destination_epoch)
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(initial_epoch)
        .is_none());

    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(source_epoch).is_some());
    assert!(persisted.cluster_map_at_epoch(destination_epoch).is_some());
    assert_eq!(
        persisted
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        13_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            13_001,
        )
        .unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
    let persisted_after_completion = store.load().unwrap().unwrap();
    assert!(persisted_after_completion
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
}

#[test]
fn exact_old_transfer_route_preserves_and_clears_older_source_dependency_atomically() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 42, PgState::Active, proof, false, 11_002);
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let transfer_epoch = authority.snapshot().cluster_epoch();
    let source_record = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap()
        .clone();
    let transfer_record = ClusterMapHistoryRecord::from_snapshot(authority.snapshot());
    assert_eq!(
        transfer_record
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch,
        Some(source_epoch)
    );

    let current_epoch =
        ClusterEpoch::new(transfer_epoch.get() + CLUSTER_MAP_HISTORY_LIMIT as u64 + 2).unwrap();
    let mut history = vec![source_record, transfer_record];
    for raw_epoch in (transfer_epoch.get() + 1)..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: Vec::new(),
            pgs: Vec::new(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(transfer_epoch, PgId::new(42))].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert!(history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
    validate_metadata_transfer_route_references(
        &history,
        current_epoch,
        std::iter::empty::<(PgId, Option<ClusterEpoch>, Option<NodeId>)>(),
    )
    .unwrap();

    prune_cluster_map_history(
        &mut history,
        &ClusterMapHistoryProtection {
            exact_routes: BTreeSet::new(),
        },
        current_epoch,
    );

    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
}

#[test]
fn exact_old_routes_do_not_displace_recent_reverse_deltas() {
    let current_epoch = ClusterEpoch::new(1_000).unwrap();
    let ordinary_floor =
        ClusterEpoch::new(current_epoch.get() - CLUSTER_MAP_HISTORY_LIMIT as u64).unwrap();
    let old_epoch = ClusterEpoch::new(100).unwrap();
    let pg_id = PgId::new(42);
    let old_pg_id = PgId::new(7);
    let route = HistoricalPgRouteRecord {
        pg_id,
        state: PgState::Active,
        acting_set: vec![NodeId::new(1), NodeId::new(2)],
        active_primary: Some(NodeId::new(1)),
        peering_metadata_transfer: None,
        peering_metadata_transfer_source_route_epoch: None,
        peering_metadata_transfer_source_node_id: None,
    };
    let old_route = HistoricalPgRouteRecord {
        pg_id: old_pg_id,
        ..route.clone()
    };
    let mut history = vec![ClusterMapHistoryRecord {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        cluster_epoch: old_epoch,
        nodes: vec![NodeId::new(1), NodeId::new(2)],
        pgs: vec![old_route],
        absent_pgs: Vec::new(),
    }];
    for raw_epoch in ordinary_floor.get()..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            pgs: (raw_epoch == ordinary_floor.get())
                .then(|| route.clone())
                .into_iter()
                .collect(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(old_epoch, old_pg_id)].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 1);
    assert!(history
        .iter()
        .any(|record| { record.cluster_epoch() == ordinary_floor && record.pg(pg_id).is_some() }));
}

#[test]
fn cluster_map_history_pruning_preserves_only_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let protected_record = authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .unwrap();
    assert!(
        protected_record.pgs().is_empty(),
        "unchanged routes should not be copied into an exact epoch marker"
    );
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(1), protected_epoch)
        .is_ok());
    let current_epoch = authority.snapshot().cluster_epoch();
    let advanced_floor = ClusterEpoch::new(current_epoch.get() - 10).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let retained_before_floor_advance = authority.snapshot().cluster_map_history().len();
    assert_eq!(retained_before_floor_advance, CLUSTER_MAP_HISTORY_LIMIT + 1);
    let runtime_map_before_floor_advance = authority.snapshot().runtime_map(10_999).unwrap();
    assert_eq!(
        runtime_map_before_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_before_floor_advance
            .historical_pg_routes()
            .len(),
        2,
        "one exact route and one reconstruction baseline are sufficient"
    );
    let mut advanced_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    advanced_floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            advanced_floor,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(advanced_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        authority.snapshot().cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    let runtime_map_after_floor_advance = authority.snapshot().runtime_map(11_000).unwrap();
    assert_eq!(
        runtime_map_after_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_after_floor_advance.historical_pg_routes().len(),
        2,
        "advancing the exact route must not restore per-epoch route markers"
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        persisted.snapshot().runtime_map(11_000).unwrap().nodes()[0]
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
}

#[test]
fn exact_old_route_retains_later_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_001);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    authority.heartbeat(exact_heartbeat, 1_001).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let introduction_boundary = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(introduction_boundary)
        .is_some_and(|record| record.absent_pgs.contains(&PgId::new(2))));
    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
}

#[test]
fn heartbeat_persists_exact_cluster_map_history_route_references() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    let current_epoch = authority.snapshot().cluster_epoch();
    let references = PgClusterMapHistoryRouteReferences::try_from_iter([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            current_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            current_epoch,
            PgId::new(2),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            current_epoch,
            PgId::new(1),
        ),
    ])
    .unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    heartbeat.cluster_map_history_route_references = references.clone();
    authority.heartbeat(heartbeat, 10_000).unwrap();

    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
    let persisted_text = format_snapshot(&store.load().unwrap().unwrap());
    assert!(persisted_text.contains("node_history_route=1,live,"));
    assert!(persisted_text.contains("node_history_route=1,backfill-desired,"));
    let invalid_kind = persisted_text.replace(
        "node_history_route=1,live,",
        "node_history_route=1,unknown,",
    );
    assert!(matches!(
        parse_snapshot(&invalid_kind),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("invalid node history route reference kind")
    ));
    let live_line = persisted_text
        .lines()
        .find(|line| line.starts_with("node_history_route=1,live,"))
        .unwrap();
    let duplicate = persisted_text.replace(live_line, &format!("{live_line}\n{live_line}"));
    assert!(matches!(
        parse_snapshot(&duplicate),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("duplicate node history route reference")
    ));
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
}

#[test]
fn heartbeat_rejects_future_or_missing_exact_history_route_before_persisting() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let before = authority.snapshot().clone();

    let mut future = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    future.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::new(current_epoch.get() + 1).unwrap(),
                PgId::new(1),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(future, 10_000),
        Err(ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
            route_epoch,
            pg_id: 1,
            validation_epoch,
            ..
        }) if route_epoch.get() == current_epoch.get() + 1
            && validation_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    let mut missing = heartbeat_from_record(&authority, 1, current_epoch, 10_001);
    missing.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::INITIAL,
                PgId::new(99),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(missing, 10_001),
        Err(
            ControlPlaneError::StorageClusterMapHistoryRouteNotRetained {
                route_epoch: ClusterEpoch::INITIAL,
                pg_id: 99,
                ..
            }
        )
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn cluster_map_history_pruning_preserves_reported_pending_command_epoch_exactly() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        1,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        10_020,
    );
    authority.complete_ready_pg_peerings(10_030).unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
            protected_epoch,
            PgId::new(1),
        )]);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        )),
    }];
    authority.heartbeat(heartbeat, 10_100).unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(1))
            .unwrap()
            .pending_metadata_command(),
        Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        ))
    );

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        let now_ms = 20_000 + u64::from(node_id);
        let current_epoch = authority.snapshot().cluster_epoch();
        let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, now_ms);
        heartbeat.cluster_map_history_route_references =
            history_route_references([PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
                protected_epoch,
                PgId::new(1),
            )]);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(1),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: Some(PendingMetadataCommandObservation::new(
                protected_epoch,
                NonZeroU64::MIN,
                0x1234,
            )),
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(protected_epoch).is_some());
}

#[test]
fn cluster_map_history_pruning_preserves_exact_durable_backfill_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
}

#[test]
fn cluster_map_history_pruning_releases_cleared_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());

    let current_epoch = authority.snapshot().cluster_epoch();
    let clear_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    assert!(authority
        .heartbeat(clear_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );

    for node_id in 100..(100 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );
}

#[test]
fn heartbeat_accepts_exact_route_without_unretained_intermediate_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let destination_epoch = next_epoch(source_epoch).unwrap();
    let unretained_intermediate_epoch = next_epoch(destination_epoch).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(unretained_intermediate_epoch)
        .is_none());
    let current_epoch = authority.snapshot().cluster_epoch();
    let heartbeat_at_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap_or(20_000);
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, heartbeat_at_ms);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            source_epoch,
            PgId::new(42),
        )]);
    authority
        .heartbeat(exact_heartbeat, heartbeat_at_ms)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
}

#[test]
fn peering_acting_set_update_preserves_metadata_transfer_source_route_fields() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        transfer.metadata_proof(),
        false,
        11_003,
    );

    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(2), NodeId::new(3)])
        .unwrap();

    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    let pg = persisted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
}

#[test]
fn control_plane_reload_rejects_transfer_marker_without_source_route_history() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    assert!(store
        .load()
        .unwrap()
        .unwrap()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    store
        .checkpoint(Some(authority.snapshot()), authority.snapshot())
        .unwrap();

    let source_history_prefixes = [
        format!("history={},", source_epoch.get()),
        format!("history_node={},", source_epoch.get()),
        format!("history_pg={},", source_epoch.get()),
    ];
    let state = std::fs::read_to_string(&state_path).unwrap();
    let filtered = state
        .lines()
        .filter(|line| {
            !source_history_prefixes
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&state_path, filtered).unwrap();

    let err = store.load().unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::Parse { message, .. }
            if message.contains(
                "references missing metadata transfer source route epoch"
            )
    ));
}

#[test]
fn snapshot_reconstructs_pg_route_at_epoch_without_serving_authority() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let route_epoch = authority.snapshot().cluster_epoch();

    authority
        .set_node_membership(NodeId::new(4), NodeMembershipState::Active)
        .unwrap();
    let snapshot = authority.snapshot();
    let route = snapshot
        .reconstructed_pg_route_at_epoch(PgId::new(3), route_epoch)
        .unwrap();
    assert_eq!(route.cluster_epoch(), route_epoch);
    assert_eq!(route.pg_id(), PgId::new(3));
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    let missing_epoch = ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap();
    assert!(matches!(
        snapshot.reconstructed_pg_route_at_epoch(PgId::new(3), missing_epoch),
        Err(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })
            if cluster_epoch == missing_epoch
    ));
}

#[test]
fn file_backed_authority_rejects_duplicate_pg_acting_set_nodes() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=1\n",
            "pg=7,peering,1:1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set contains duplicate node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_nodes_absent_from_current_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,active,1:99,1,1,2,3,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set references unknown node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,9,10,11,3,9,10,11,-,-,-,2,1,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "metadata transfer source epoch must not be newer than PG record epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_imported_provenance_without_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,1,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);

    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active metadata transfer imported provenance requires an active metadata proof epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=5\nauthority_incarnation=1\ncluster_epoch=2\nhistory=1,1\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_or_future_history_epochs() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=2,1\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history epoch must be older than current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_nodes_absent_from_history_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1:2,-,-,-,-,-,-,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG 7 acting set references unknown node 2"
    ));
}

#[test]
fn file_backed_authority_rejects_reconstructible_observations_in_history() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_node_pg=2,1,7,peering,2,100,0,0,0,-,-,-\n",
            "history_pg=2,7,peering,1,-,-,-,-,-,-,-,-,-,-\n",
            "node=1,active,1,healthy,11,3,100,200,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    let error = FileControlPlaneStore::new(path).load().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Parse { message, .. }
            if message == "unknown control-plane state line"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=4\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1,-,3,9,10,11,9,10,11,2,1\n",
            "node=1,active,1,healthy,11,4,100,200,6e6f64652d312e736f636b\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "PG 7 metadata transfer source epoch is newer than route epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_incomplete_compact_history_routes() {
    let cases = [
        (
            "history_pg=2,7,active,1,-,-,-,-,-,-,-,-,-,-\n",
            "active PG 7 has no primary",
        ),
        (
            "history_pg=2,7,peering,1,-,2,9,10,11,9,10,11,-,-\n",
            "PG 7 has incomplete metadata transfer route state",
        ),
    ];
    for (index, (history_pg, expected)) in cases.into_iter().enumerate() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{index}.state"));
        let contents = format!(
            "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\nhistory=2,1\nhistory_node=2,1\n{history_pg}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_broken_compact_history_transfer_chains() {
    let cases = [
        (
            "self-reference",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,2,1\n",
            ),
            "PG 7 metadata transfer source route epoch is not older than route epoch",
        ),
        (
            "missing-source-epoch",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source route epoch 1",
        ),
        (
            "missing-source-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source PG at epoch 1",
        ),
        (
            "mismatched-source-primary",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_node=1,2\n",
                "history_pg=1,7,active,2,2,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_node=2,2\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 metadata transfer source node 1 does not match source route primary 2 at epoch 1",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let contents = format!(
            "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_invalid_pg_introduction_history() {
    let current_node = "node=1,active,1,suspect,11,-,-,-,2f746d702f6e6f64652d312e736f636b\n";
    let current_pg = "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n";
    let cases = [
        (
            "duplicate",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history repeats a PG introduction boundary",
        ),
        (
            "route-before-introduction",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg=1,7,peering,1,-,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history PG route precedes its introduction boundary",
        ),
        (
            "missing-current-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
            ),
            "history absent PG is missing from current state",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let current_pg = if name == "missing-current-pg" {
            ""
        } else {
            current_pg
        };
        std::fs::write(
            &path,
            format!(
                "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}{current_node}{current_pg}"
            ),
        )
        .unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error for {name}: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_noncanonical_absent_pg_order() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=1,1\n",
            "history_pg_absent=1,8\n",
            "history_pg_absent=1,7\n",
            "node=1,active,1,suspect,11,-,-,-,2f746d702f6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
            "pg=8,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(path).load(),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history absent PG records must be strictly increasing"
    ));
}

#[test]
fn file_backed_authority_rejects_pg_observations_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=5\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,0,0,0\n",
            "pg=7,peering,1,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_outside_acting_set() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node=2,active,1,healthy,12,2,100,200,6e6f64652d322e736f636b\n",
            "node_pg=2,7,peering,2,100,0,0,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation references PG outside node acting set"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_wrong_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "node=1,active,1,healthy,11,3,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,0,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation epoch must match current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_mismatched_proof() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,10,12,-,-,-\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active node PG observation metadata proof is behind or diverges from PG active proof"
    ));
}

#[test]
fn file_backed_authority_accepts_active_pg_observation_after_metadata_progress() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let initial = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let snapshot = parse_snapshot(concat!(
        "version=27\n",
        "authority_incarnation=1\n",
        "cluster_epoch=2\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=100\nlease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
        "node_pg=1,7,active,2,100,10,20,30,-,-,-\n",
        "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
    ))
    .unwrap();
    store
        .checkpoint(Some(initial.snapshot()), &snapshot)
        .unwrap();
    drop(initial);
    let authority = SingleAuthorityControlPlane::open(store).unwrap();
    let history = authority
        .snapshot()
        .cluster_map_at_epoch(ClusterEpoch::new(2).unwrap())
        .unwrap();
    assert!(history.nodes().contains(&NodeId::new(1)));
    let historical_pg = history.pg(PgId::new(7)).unwrap();
    assert_eq!(historical_pg.state(), PgState::Active);
    assert_eq!(historical_pg.active_primary, Some(NodeId::new(1)));
}

#[test]
fn file_backed_authority_replays_journal_without_per_command_checkpoint() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let digest_computations_before = single_authority_snapshot_digest_computations();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    assert_eq!(
        single_authority_snapshot_digest_computations(),
        digest_computations_before,
        "journal suffix commands must not format and digest the full snapshot"
    );
    assert_eq!(
        std::fs::read(store.path()).unwrap(),
        checkpoint_before,
        "ordinary durable commands must not rewrite the full checkpoint"
    );
    let offsets = store.journal.status_offsets().unwrap();
    assert!(offsets.clean_len > offsets.base_offset);
    let replayed = store.load().unwrap().unwrap();
    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );

    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    let compacted = store.journal.status_offsets().unwrap();
    let retained = store
        .journal
        .read_frames_from(compacted.base_offset)
        .unwrap();
    assert_eq!(retained.frames.len(), 1);
    assert!(
        SingleAuthorityJournalRecord::decode(&retained.frames[0])
            .unwrap()
            .command
            .is_none(),
        "checkpoint compaction must retain exactly one checkpoint anchor"
    );
}

#[test]
fn file_backed_authority_recovers_identity_only_initialization() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let initializing_store = FileControlPlaneStore::new(&path);
    initializing_store
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();

    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(authority.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn single_authority_durable_formats_reject_unsupported_versions() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);

    for version in [0, CONTROL_PLANE_STATE_IDENTITY_VERSION + 1] {
        store_single_authority_clock_checkpoint_binding(&path, binding).unwrap();
        let identity_path = single_authority_identity_path(&path);
        let mut bytes = std::fs::read(&identity_path).unwrap();
        bytes[CONTROL_PLANE_STATE_IDENTITY_MAGIC.len()
            ..CONTROL_PLANE_STATE_IDENTITY_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&identity_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_clock_checkpoint_binding(&path),
            Err(ControlPlaneError::AuthorityClockCheckpoint { message })
                if message == format!(
                    "unsupported single-authority durable identity version {version}"
                )
        ));
    }

    for version in [0, SINGLE_AUTHORITY_INITIALIZED_VERSION + 1] {
        store_single_authority_initialized_binding(&path, binding).unwrap();
        let initialized_path = single_authority_initialized_path(&path);
        let mut bytes = std::fs::read(&initialized_path).unwrap();
        bytes[SINGLE_AUTHORITY_INITIALIZED_MAGIC.len()
            ..SINGLE_AUTHORITY_INITIALIZED_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&initialized_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_initialized_binding(&path),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority initialization marker version {version}"
                )
        ));
    }

    let store = FileControlPlaneStore::new(&path);
    for version in [
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION + 1,
    ] {
        let mut header = store.journal.encode_file_header(0);
        let version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
        header[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut header);
        assert!(matches!(
            store.journal.decode_file_header(&header),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal file header version {version}"
                )
        ));
    }

    for version in [
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION + 1,
    ] {
        let mut record = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: 7,
            resulting_chain_digest: 7,
            command: None,
        }
        .encode()
        .unwrap();
        let version_offset = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        record[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut record);
        assert!(matches!(
            SingleAuthorityJournalRecord::decode(&record),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal record version {version}"
                )
        ));
    }
}

#[test]
fn bare_control_plane_state_path_uses_current_directory_for_durability() {
    assert_eq!(
        state_parent(Path::new("control-plane.state")),
        Path::new(".")
    );
    assert_eq!(
        state_parent(Path::new("./control-plane.state")),
        Path::new(".")
    );
}

#[test]
fn control_plane_state_directory_creation_syncs_each_new_component_parent() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut synced_parents = Vec::new();

    create_control_plane_directory_all_durable_with(&second, |parent| {
        synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        synced_parents,
        vec![
            state_parent(tmp.path()).to_path_buf(),
            tmp.path().to_path_buf(),
            first
        ]
    );
    assert!(second.is_dir());
}

#[test]
fn control_plane_state_directory_creation_reconfirms_failed_sync_on_retry() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut sync_attempts = 0;

    let error = create_control_plane_directory_all_durable_with(&second, |_| {
        sync_attempts += 1;
        if sync_attempts == 2 {
            Err(ControlPlaneError::io(
                "injected control-plane state parent sync",
                std::io::Error::other("injected parent sync failure"),
            ))
        } else {
            Ok(())
        }
    })
    .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "injected control-plane state parent sync"
    ));
    assert!(first.is_dir());
    assert!(
        !second.exists(),
        "the next directory component must not be created before its parent link is durable"
    );

    let mut retry_synced_parents = Vec::new();
    create_control_plane_directory_all_durable_with(&second, |parent| {
        retry_synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(retry_synced_parents, vec![tmp.path().to_path_buf(), first]);
    assert!(second.is_dir());
}

#[test]
fn file_backed_authority_checkpoints_at_command_and_byte_bounds() {
    for (name, command_limit, byte_limit, commands_before_checkpoint) in
        [("commands", 2, u64::MAX, 2), ("bytes", u64::MAX, 1, 1)]
    {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::with_checkpoint_limits(
            tmp.path().join(format!("control-plane-{name}.state")),
            command_limit,
            byte_limit,
        );
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let initial_checkpoint = std::fs::read(store.path()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if commands_before_checkpoint == 2 {
            assert_eq!(std::fs::read(store.path()).unwrap(), initial_checkpoint);
            authority
                .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
                .unwrap();
        }
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .expect("checkpoint threshold should be due")
            .persist()
            .unwrap();

        assert_ne!(
            std::fs::read(store.path()).unwrap(),
            initial_checkpoint,
            "{name} threshold must publish a compacted checkpoint"
        );
        let offsets = store.journal.status_offsets().unwrap();
        let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
        assert_eq!(retained.frames.len(), 1);
        assert!(
            SingleAuthorityJournalRecord::decode(&retained.frames[0])
                .unwrap()
                .command
                .is_none(),
            "{name} threshold compaction must retain one checkpoint anchor"
        );
        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert_eq!(
            restarted
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .membership(),
            NodeMembershipState::Active
        );
    }
}

#[test]
fn file_backed_authority_checkpoint_is_due_at_time_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_policy(
        tmp.path().join("control-plane.state"),
        u64::MAX,
        u64::MAX,
        Duration::ZERO,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("time threshold should be due")
        .persist()
        .unwrap();

    assert_ne!(std::fs::read(store.path()).unwrap(), checkpoint_before);
}

#[test]
fn captured_checkpoint_preparation_failure_latches_poison() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");
    let prepared_path = single_authority_snapshot_tmp_path(&path);
    std::fs::create_dir(&prepared_path).unwrap();

    assert!(matches!(
        checkpoint.persist(),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "create control-plane state"
    ));
    let error = authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));
}

#[test]
fn captured_checkpoint_rebases_commands_appended_during_persistence() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    checkpoint.persist().unwrap();

    let offsets = store.journal.status_offsets().unwrap();
    let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
    assert_eq!(retained.frames.len(), 2);
    assert!(SingleAuthorityJournalRecord::decode(&retained.frames[0])
        .unwrap()
        .command
        .is_none());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn checkpoint_failure_latches_poison_before_concurrent_command_can_append() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let previous_snapshot = authority.durable_snapshot.clone();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let mut next_snapshot = previous_snapshot
        .apply_control_plane_command(command.clone())
        .unwrap()
        .into_snapshot();
    next_snapshot.record_history_from(&previous_snapshot);
    drop(authority);

    store.pause_next_checkpoint_after_journal_replacement();
    store.fail_next_checkpoint_after_anchor();
    let checkpoint_worker = std::thread::spawn(move || checkpoint.persist());
    let replacement_reached = store.wait_for_checkpoint_journal_replacement(Duration::from_secs(2));

    store.arm_commit_before_durability_lock_signal();
    let concurrent_store = store.clone();
    let command_worker = std::thread::spawn(move || {
        concurrent_store.commit_command(&previous_snapshot, &command, &next_snapshot)
    });
    let command_reached_durability_lock =
        store.wait_for_commit_before_durability_lock(Duration::from_secs(2));
    store.release_checkpoint_after_journal_replacement();

    let checkpoint_error = checkpoint_worker.join().unwrap().unwrap_err();
    let command_error = command_worker.join().unwrap().unwrap_err();
    assert!(
        replacement_reached,
        "checkpoint should pause after durable journal replacement"
    );
    assert!(
        command_reached_durability_lock,
        "concurrent command should reach the durability lock while checkpoint publication is paused"
    );
    assert!(matches!(
        checkpoint_error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(matches!(
        command_error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert!(restarted.snapshot().node(NodeId::new(1)).is_some());
    assert!(
        restarted.snapshot().node(NodeId::new(2)).is_none(),
        "the waiting command must not append after replacement failure"
    );
}

#[test]
fn captured_checkpoint_preserves_conservative_suffix_age() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let captured_at = Instant::now();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(captured_at)
        .unwrap()
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    checkpoint.persist().unwrap();

    let durability = store.lock_durability().unwrap();
    assert_eq!(durability.commands_since_checkpoint, 1);
    assert_eq!(durability.first_uncheckpointed_at, Some(captured_at));
}

#[test]
fn checkpoint_capture_uses_tracked_offset_without_scanning_journal() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let expected_offset = store.lock_durability().unwrap().journal_clean_offset;
    std::fs::write(store.journal_path(), b"not a valid journal").unwrap();

    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .expect("capture must not read the journal")
        .expect("command threshold should be due");

    assert_eq!(checkpoint.capture.journal_offset, expected_offset);
}

#[test]
fn stale_captured_checkpoint_is_rejected_before_publication() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let stale = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let current = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    current.persist().unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = stale.persist().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint capture is stale")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .expect("stale checkpoint rejection must not poison the store");
}

#[test]
fn captured_checkpoint_is_bound_to_its_store_instance() {
    let tmp = test_util::tempdir();
    let first_store =
        FileControlPlaneStore::with_checkpoint_limits(tmp.path().join("first.state"), 1, u64::MAX);
    let mut first = SingleAuthorityControlPlane::open(first_store).unwrap();
    first
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = first
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let SingleAuthorityDurableCheckpoint {
        capture, snapshot, ..
    } = checkpoint;
    let second_store = FileControlPlaneStore::new(tmp.path().join("second.state"));
    SingleAuthorityControlPlane::open(second_store.clone()).unwrap();
    let checkpoint_before = std::fs::read(second_store.path()).unwrap();
    let journal_before = std::fs::read(second_store.journal_path()).unwrap();

    let error = second_store
        .persist_captured_checkpoint(capture, &snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("belongs to another store instance")
    ));
    assert_eq!(
        std::fs::read(second_store.path()).unwrap(),
        checkpoint_before
    );
    assert_eq!(
        std::fs::read(second_store.journal_path()).unwrap(),
        journal_before
    );
    second_store.ensure_healthy().unwrap();
}

#[test]
fn captured_checkpoint_persists_without_authority_mutex() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(store).unwrap(),
    ));
    let checkpoint = {
        let mut authority = authority.lock().unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .unwrap()
    };
    let authority_guard = authority.lock().unwrap();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        completed_tx.send(checkpoint.persist()).unwrap();
    });

    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint persistence must not wait for the authority mutex")
        .unwrap();
    drop(authority_guard);
    worker.join().unwrap();
}

#[test]
fn file_backed_authority_checkpoint_compaction_reports_physical_io() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let before = observability::control_plane_journal_metrics_snapshot();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();

    let after = observability::control_plane_journal_metrics_snapshot();
    assert!(after.compaction_total > before.compaction_total);
    assert!(after.compaction_us_total >= before.compaction_us_total);
    assert!(after.compaction_lock_wait_us_total >= before.compaction_lock_wait_us_total);
    assert!(after.compaction_bytes_total > before.compaction_bytes_total);
    assert!(after.compaction_bytes_last > 0);
    assert!(after.compaction_file_sync_total > before.compaction_file_sync_total);
    assert!(after.compaction_directory_sync_total > before.compaction_directory_sync_total);
}

#[test]
fn file_backed_authority_recovers_checkpoint_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_checkpoint_after_anchor();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let error = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_anchor_file_sync_before_directory_sync() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let previous = authority.snapshot().clone();
    let applied = previous
        .clone()
        .apply_control_plane_command(ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        })
        .unwrap();
    let mut next = applied.into_snapshot();
    next.record_history_from(&previous);
    store.fail_next_journal_directory_sync();

    let error = store.checkpoint(Some(&previous), &next).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "sync single-authority control-plane journal directory"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_recovers_initial_identity_creation_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_initial_checkpoint_after_identity();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "initialize single-authority control-plane checkpoint"
    ));
    assert!(single_authority_identity_path(&path).exists());
    assert!(!path.exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_initial_prepared_snapshot_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_prepared_snapshot_sync();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "anchor prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    assert!(!store.journal_path().exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_torn_first_journal_creation() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        store.fail_next_checkpoint_after_prepared_snapshot_sync();
        SingleAuthorityControlPlane::open(store.clone()).unwrap_err();
        let prepared = std::fs::read_to_string(single_authority_snapshot_tmp_path(&path)).unwrap();
        let snapshot_digest = checksum::crc64::checksum(prepared.as_bytes());
        let binding = load_single_authority_clock_checkpoint_binding(&path)
            .unwrap()
            .unwrap();
        let anchor = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: snapshot_digest,
            resulting_chain_digest: snapshot_digest,
            command: None,
        }
        .encode()
        .unwrap();
        store.journal.append_frame(&anchor).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        let restarted =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
        assert_eq!(restarted.snapshot().nodes().count(), 0);
        assert!(single_authority_initialized_path(&path).exists());
    }
}

#[test]
fn file_backed_authority_rejects_torn_established_first_journal_record() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        assert!(
            FileControlPlaneStore::new(&path).load().is_err(),
            "established {shape} journal must fail closed"
        );
    }
}

#[test]
fn file_backed_authority_recovers_initial_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_anchor();

    let error = SingleAuthorityControlPlane::open(store).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(!path.exists());
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_restart_bump_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let incarnation_before = authority.snapshot().authority_incarnation();
    drop(authority);

    let failing_store = FileControlPlaneStore::new(&path);
    failing_store.fail_next_checkpoint_after_anchor();
    let error = SingleAuthorityControlPlane::open(failing_store).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert!(
        restarted.snapshot().authority_incarnation() > incarnation_before,
        "restart must recover and advance beyond the prepared incarnation"
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_truncates_torn_journal_tail_after_replay() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let clean_len = store.journal.clean_len().unwrap();
    let physical_len_before = std::fs::metadata(store.journal_path()).unwrap().len();
    std::fs::OpenOptions::new()
        .append(true)
        .open(store.journal_path())
        .unwrap()
        .write_all(&[0, 0])
        .unwrap();

    let replayed = store.load().unwrap().unwrap();

    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );
    assert_eq!(store.journal.clean_len().unwrap(), clean_len);
    assert_eq!(
        std::fs::metadata(store.journal_path()).unwrap().len(),
        physical_len_before
    );
}

#[test]
fn file_backed_authority_rejects_missing_or_empty_journal_after_acknowledged_command() {
    for missing in [true, false] {
        let tmp = test_util::tempdir();
        let store =
            FileControlPlaneStore::new(tmp.path().join(format!("control-plane-{missing}.state")));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if missing {
            std::fs::remove_file(store.journal_path()).unwrap();
        } else {
            std::fs::File::create(store.journal_path()).unwrap();
        }

        let error = FileControlPlaneStore::new(store.path()).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::CommandDecode { ref message }
                    if message.contains("has no identity-bound checkpoint anchor")
            ),
            "unexpected recovery error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_missing_established_checkpoint_and_journal() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert!(single_authority_initialized_path(&path).exists());
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(store.journal_path()).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("without its control-plane checkpoint")
    ));
}

#[test]
fn file_backed_authority_rejects_foreign_identity_journal() {
    let tmp = test_util::tempdir();
    let first = FileControlPlaneStore::new(tmp.path().join("first.state"));
    let second = FileControlPlaneStore::new(tmp.path().join("second.state"));
    let mut first_authority = SingleAuthorityControlPlane::open(first.clone()).unwrap();
    SingleAuthorityControlPlane::open(second.clone()).unwrap();
    first_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    std::fs::copy(first.journal_path(), second.journal_path()).unwrap();

    assert!(matches!(
        second.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("journal identity does not match durable state")
    ));
}

#[test]
fn file_backed_authority_rejects_checksum_valid_discontinuous_command_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let binding = load_single_authority_clock_checkpoint_binding(store.path())
        .unwrap()
        .unwrap();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let encoded_command = encode_control_plane_command(&command).unwrap();
    let published_chain_digest = store
        .lock_durability()
        .unwrap()
        .published_chain_digest
        .unwrap();
    let wrong_previous_chain_digest = published_chain_digest ^ 1;
    let record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: wrong_previous_chain_digest,
        resulting_chain_digest: single_authority_command_chain_digest(
            wrong_previous_chain_digest,
            &encoded_command,
        ),
        command: Some(command),
    };
    store
        .journal
        .append_frame(&record.encode().unwrap())
        .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(store.path()).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_complete_interior_command_omission() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let offsets = store.journal.status_offsets().unwrap();
    let frames = store
        .journal
        .read_frames_from(offsets.base_offset)
        .unwrap()
        .frames;
    assert_eq!(frames.len(), 3);
    std::fs::remove_file(store.journal_path()).unwrap();
    store.journal.append_frame(&frames[0]).unwrap();
    store.journal.append_frame(&frames[2]).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_checkpoint_off_retained_journal_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_checkpoint = std::fs::read(store.path()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();
    std::fs::write(store.path(), initial_checkpoint).unwrap();

    assert!(matches!(
        store.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("has no identity-bound checkpoint anchor for the durable snapshot")
    ));
}

#[test]
fn file_backed_authority_rejects_stale_checkpoint_before_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let stale_snapshot = authority.snapshot().clone();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = store
        .checkpoint(Some(&stale_snapshot), &stale_snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint base does not match")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
}

#[test]
fn file_backed_authority_poisoned_by_ambiguous_journal_append_stops_serving() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_journal_file_sync();

    let error = authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability poisoned after ambiguous journal append")
    ));
    assert!(authority.snapshot().node(NodeId::new(1)).is_none());
    assert!(matches!(
        authority.runtime_map_snapshot(1),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_pending_metadata_command() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,10,11,2,1,1\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "active node PG observation must not have pending metadata command"
    ));
}

#[test]
fn heartbeat_lease_is_issued_after_persisted_epoch_map_tuple() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();
    let membership_epoch = authority.snapshot().cluster_epoch();
    let lease = authority
        .heartbeat(heartbeat(3, membership_epoch, 2_000), 2_000)
        .unwrap();

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(
        persisted.authority_incarnation(),
        lease.authority_incarnation()
    );
    assert_eq!(persisted.cluster_epoch(), lease.cluster_epoch());
    let persisted_node = persisted.node(NodeId::new(3)).unwrap();
    assert_eq!(
        persisted_node.lease_deadline_ms(),
        Some(lease.lease_deadline_ms())
    );
    assert_eq!(
        persisted_node.availability(),
        NodeAvailabilityState::Healthy
    );
    assert_eq!(persisted_node.last_observed_epoch(), Some(membership_epoch));
    assert!(!lease.serving());
    assert_eq!(
        lease.snapshot().cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert!(std::fs::metadata(store_path).unwrap().is_file());
}

#[test]
fn heartbeat_rejects_stale_node_incarnation_and_fences_new_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(4), NodeMembershipState::Active)
        .unwrap();
    let first = authority
        .heartbeat(heartbeat(4, authority.snapshot().cluster_epoch(), 100), 100)
        .unwrap();

    let mut stale = heartbeat(4, first.cluster_epoch(), 200);
    stale.node_incarnation -= 1;
    assert!(matches!(
        authority.heartbeat(stale, 200),
        Err(ControlPlaneError::StaleNodeIncarnation { node_id: 4, .. })
    ));

    let mut restarted_node = heartbeat(4, first.cluster_epoch(), 300);
    restarted_node.node_incarnation += 1;
    let fenced = authority.heartbeat(restarted_node, 300).unwrap();
    assert!(fenced.cluster_epoch() > first.cluster_epoch());
    assert!(!fenced.serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(4))
            .unwrap()
            .node_incarnation(),
        15
    );

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 4, fenced.cluster_epoch(), 400),
            400,
        )
        .unwrap();
    assert!(caught_up.serving());
}

#[test]
fn endpoint_change_bumps_epoch_and_requires_node_to_observe_new_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(5), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 5, 100);
    assert!(serving.serving());

    let mut moved = heartbeat(5, serving.cluster_epoch(), 200);
    moved.endpoint = "node-5-new.sock".to_owned();
    let changed = authority.heartbeat(moved, 200).unwrap();
    assert!(changed.cluster_epoch() > serving.cluster_epoch());
    assert!(!changed.serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(5))
            .unwrap()
            .endpoint(),
        "node-5-new.sock"
    );
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)], 200),
        None
    );

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 5, changed.cluster_epoch(), 300),
            300,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)], 300),
        Some(NodeId::new(5))
    );
}

#[test]
fn endpoint_change_fences_active_pg_until_repeering_completes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(5), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 5, 100).serving());
    authority
        .set_pg_acting_set(PgId::new(39), vec![NodeId::new(5)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Peering, 200);
    authority
        .complete_pg_peering(
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            201,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Active, 202);
    let active_epoch = active.cluster_epoch();
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            active_epoch,
            203,
        )
        .unwrap();

    let mut moved = heartbeat_from_record(&authority, 5, active_epoch, 204);
    moved.endpoint = "node-5-new.sock".to_owned();
    moved.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(39),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let changed = authority.heartbeat(moved, 204).unwrap();
    assert!(!changed.serving());
    assert!(changed.cluster_epoch() > active_epoch);
    let peering_epoch = changed.cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(39)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    let persisted_pg = store
        .load()
        .unwrap()
        .unwrap()
        .pg(PgId::new(39))
        .unwrap()
        .clone();
    assert_eq!(
        persisted_pg.previous_primary_node_id(),
        Some(NodeId::new(5))
    );
    assert_eq!(persisted_pg.previous_primary_node_incarnation(), Some(15));
    assert_eq!(persisted_pg.previous_primary_lease_deadline_ms(), Some(302));
    assert_eq!(
        persisted_pg
            .previous_primary_lease
            .as_ref()
            .map(|previous| previous.endpoint.as_str()),
        Some("node-5.sock")
    );
    assert_eq!(
        persisted_pg
            .previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(true)
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 205),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == peering_epoch
    ));

    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 5, peering_epoch, 206),
            206
        )
        .unwrap()
        .serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            peering_epoch,
            207,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 39,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Peering, 208);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            209,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive {
            pg_id: 39,
            lease_deadline_ms: 302,
            ..
        })
    ));
    heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Peering, 302);
    heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Peering, 1_302);
    authority
        .complete_pg_peering(
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            1_302,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(&mut authority, 5, 39, PgState::Active, 1_303);
    authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(39),
            NodeId::new(5),
            node_incarnation(&authority, 5),
            active_again.cluster_epoch(),
            1_304,
        )
        .unwrap();
}

#[test]
fn pg_acting_set_changes_start_in_peering_and_validate_nodes() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(7), Vec::new()),
        Err(ControlPlaneError::EmptyActingSet { pg_id: 7 })
    ));
    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(7), vec![NodeId::new(99)]),
        Err(ControlPlaneError::UnknownActingSetNode {
            pg_id: 7,
            node_id: 99
        })
    ));
    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(7), vec![NodeId::new(1), NodeId::new(1)]),
        Err(ControlPlaneError::DuplicateActingSetNode {
            pg_id: 7,
            node_id: 1
        })
    ));

    let before = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    assert!(authority.snapshot().cluster_epoch() > before);
    let pg = authority.snapshot().pg(PgId::new(7)).unwrap();
    assert_eq!(pg.pg_id(), PgId::new(7));
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(authority.serving_pg_primary(PgId::new(7), 1), None);

    let persisted = store.load().unwrap().unwrap();
    let persisted_pg = persisted.pg(PgId::new(7)).unwrap();
    assert_eq!(persisted_pg.state(), PgState::Peering);
    assert_eq!(persisted_pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert!(std::fs::metadata(store_path).unwrap().is_file());
}

#[test]
fn bootstrap_initial_cluster_map_persists_nodes_and_pg_routes_atomically() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();

    let snapshot = authority
        .bootstrap_initial_cluster_map(
            vec![
                (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
                (NodeId::new(4), "/tmp/node-4.sock".to_owned()),
            ],
            vec![PgId::new(0), PgId::new(3)],
        )
        .unwrap();

    assert_eq!(
        snapshot
            .nodes()
            .map(NodeControlRecord::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(2), NodeId::new(4)]
    );
    assert_eq!(
        snapshot
            .nodes()
            .map(NodeControlRecord::endpoint)
            .collect::<Vec<_>>(),
        vec!["/tmp/node-2.sock", "/tmp/node-4.sock"]
    );
    for pg_id in [0, 3] {
        let pg = snapshot.pg(PgId::new(pg_id)).unwrap();
        assert_eq!(pg.acting_set(), &[NodeId::new(2), NodeId::new(4)]);
        assert_eq!(pg.state(), PgState::Peering);
    }
    assert_eq!(
        snapshot
            .runtime_map(1_000)
            .unwrap()
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::endpoint)
            .collect::<Vec<_>>(),
        vec!["/tmp/node-2.sock", "/tmp/node-4.sock"]
    );
    assert!(matches!(
        authority.bootstrap_initial_cluster_map(
            vec![(NodeId::new(5), "/tmp/node-5.sock".to_owned())],
            vec![PgId::new(7)]
        ),
        Err(ControlPlaneError::BootstrapRequiresEmptyState)
    ));
}

#[test]
fn single_authority_uncertified_topology_bootstrap_is_owner_validated_and_idempotent() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let topology = crate::derive_uncertified_initial_control_plane_topology(
        &[
            crate::StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
            crate::StaticStorageNodeEndpoint::new(4, "/tmp/node-4.sock"),
        ],
        &[0, 3],
    )
    .unwrap();

    let established_epoch = authority
        .establish_uncertified_initial_control_plane_topology(&topology)
        .unwrap()
        .expect("empty authority should establish the configured topology");
    assert_eq!(
        established_epoch,
        authority.snapshot().cluster_epoch().get()
    );
    assert_eq!(
        authority
            .snapshot()
            .nodes()
            .map(NodeControlRecord::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(2), NodeId::new(4)]
    );
    assert!(store.load().unwrap().unwrap().pg(PgId::new(3)).is_some());

    let crossed = crate::derive_uncertified_initial_control_plane_topology(
        &[crate::StaticStorageNodeEndpoint::new(9, "/tmp/node-9.sock")],
        &[9],
    )
    .unwrap();
    assert_eq!(
        authority
            .establish_uncertified_initial_control_plane_topology(&crossed)
            .unwrap(),
        None
    );
    assert!(authority.snapshot().node(NodeId::new(9)).is_none());
    assert!(authority.snapshot().pg(PgId::new(9)).is_none());
}

#[test]
fn single_authority_uncertified_topology_with_no_nodes_is_a_noop() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let topology = crate::derive_uncertified_initial_control_plane_topology(&[], &[1]).unwrap();

    assert_eq!(
        authority
            .establish_uncertified_initial_control_plane_topology(&topology)
            .unwrap(),
        None
    );
    assert!(authority.snapshot().nodes().next().is_none());
    assert!(authority.snapshot().pgs().next().is_none());
}

#[test]
fn certified_bootstrap_persists_exact_topology_and_pg_placements() {
    let nodes = vec![
        (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
        (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
        (NodeId::new(3), "/tmp/node-3.sock".to_owned()),
    ];
    let pg_acting_sets = vec![
        (PgId::new(0), vec![NodeId::new(1), NodeId::new(2)]),
        (PgId::new(1), vec![NodeId::new(2), NodeId::new(3)]),
    ];
    let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        9,
        [0x3c; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        vec![101, 102, 103],
        &nodes,
        &pg_acting_sets,
    )
    .unwrap();
    let snapshot = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: nodes.clone(),
            pg_acting_sets: pg_acting_sets.clone(),
            topology: topology.clone(),
        })
        .unwrap()
        .into_snapshot();

    assert_eq!(snapshot.initial_topology(), Some(&topology));
    assert_eq!(
        snapshot.pg(PgId::new(0)).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
    assert_eq!(
        snapshot.pg(PgId::new(1)).unwrap().acting_set(),
        &[NodeId::new(2), NodeId::new(3)]
    );
    let encoded = format_snapshot(&snapshot);
    assert_eq!(parse_snapshot(&encoded).unwrap(), snapshot);

    let missing_certificate = encoded
        .lines()
        .filter(|line| !line.starts_with("initial_topology="))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    assert!(matches!(
        parse_snapshot(&missing_certificate),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing initial topology"
    ));

    let uppercase_digest = encoded.replace(
        &format!("initial_topology=9,{},", "3c".repeat(32)),
        &format!("initial_topology=9,{},", "3C".repeat(32)),
    );
    assert!(matches!(
        parse_snapshot(&uppercase_digest),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "control-plane state must use canonical snapshot encoding"
    ));

    let reversed_voters = encoded.replace("101:102:103", "102:101:103");
    assert!(matches!(
        parse_snapshot(&reversed_voters),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("initial topology Raft voters must be strictly increasing")
    ));

    let error = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_owned())],
            pg_acting_sets: vec![(PgId::new(0), vec![NodeId::new(2)])],
            topology: topology.clone(),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::UnknownActingSetNode {
            pg_id: 0,
            node_id: 2
        }
    ));

    let mut altered_nodes = nodes;
    altered_nodes[0].1 = "/tmp/wrong-node-1.sock".to_owned();
    let altered_endpoint_error = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: altered_nodes,
            pg_acting_sets: pg_acting_sets.clone(),
            topology: topology.clone(),
        })
        .unwrap_err();
    assert!(matches!(
        altered_endpoint_error,
        ControlPlaneError::InvalidInitialTopology { message }
            if message.contains("bootstrap-map digest")
    ));

    let mut altered_pg_acting_sets = pg_acting_sets;
    altered_pg_acting_sets[0].1 = vec![NodeId::new(2), NodeId::new(1)];
    let altered_acting_set_error = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: vec![
                (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
                (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
                (NodeId::new(3), "/tmp/node-3.sock".to_owned()),
            ],
            pg_acting_sets: altered_pg_acting_sets,
            topology,
        })
        .unwrap_err();
    assert!(matches!(
        altered_acting_set_error,
        ControlPlaneError::InvalidInitialTopology { message }
            if message.contains("bootstrap-map digest")
    ));
}

#[test]
fn control_plane_command_replay_matches_single_authority_snapshot() {
    let bootstrap_epoch = ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 1).unwrap();
    let first_heartbeat_epoch = ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 2).unwrap();
    let commands = vec![
        ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: vec![
                (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
                (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
            ],
            pg_ids: vec![PgId::new(7)],
        },
        ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 0,
                endpoint: "/tmp/node-1.sock".to_owned(),
                observed_epoch: bootstrap_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            heartbeat_at_ms: 1_000,
            lease_deadline_ms: 1_100,
            lease_horizon_authority: None,
        },
        ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 0,
                endpoint: "/tmp/node-1.sock".to_owned(),
                observed_epoch: first_heartbeat_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_references: Default::default(),
                pg_observations: vec![NodePgHeartbeatObservation {
                    pg_id: PgId::new(7),
                    state: PgState::Peering,
                    metadata_proof: PgMetadataProof::empty(),
                    pending_metadata_command: None,
                }],
            },
            heartbeat_at_ms: 1_100,
            lease_deadline_ms: 1_200,
            lease_horizon_authority: None,
        },
        ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 1_200,
        },
        ControlPlaneCommand::SetPgActingSet {
            pg_id: PgId::new(7),
            acting_set: vec![NodeId::new(1)],
        },
        ControlPlaneCommand::SetPgState {
            pg_id: PgId::new(7),
            state: PgState::Backfilling,
        },
        ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(2),
            membership: NodeMembershipState::Draining,
        },
        ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(2),
            availability: NodeAvailabilityState::Unavailable,
        },
    ];

    let mut replayed = ClusterControlSnapshot::empty();
    for command in commands.clone() {
        let encoded = crate::control_plane_command::encode_control_plane_command(&command).unwrap();
        let decoded = crate::control_plane_command::decode_control_plane_command(&encoded).unwrap();
        let applied = replayed.apply_control_plane_command(decoded).unwrap();
        assert!(applied.changed());
        replayed = applied.into_snapshot();
    }

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for command in commands {
        authority.apply_and_commit_command(command).unwrap();
    }

    assert_eq!(&replayed, authority.snapshot());
    assert_eq!(
        replayed.cluster_epoch(),
        ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 7).unwrap()
    );
    assert_eq!(replayed.cluster_map_history().len(), 7);
}

#[test]
fn lease_grant_horizon_command_replays_and_fences_authority_rebinding() {
    let initial_authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let replacement_authority = LeaseHorizonAuthorityBinding::new(8, Some(12));
    let initial = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: initial_authority,
            authority_now_ms: 10_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap();
    assert!(initial.changed());
    assert_eq!(
        initial.response(),
        &ControlPlaneCommandResponse::EstablishLeaseGrantHorizon
    );
    let initial = initial.into_snapshot();
    assert_eq!(initial.max_committed_timestamp_ms(), Some(10_000));
    let horizon = initial.lease_grant_horizon().unwrap();
    assert_eq!(horizon.authority(), initial_authority);
    assert_eq!(horizon.grant_not_after_ms(), 40_000);

    let replay = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: initial_authority,
            authority_now_ms: 10_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap();
    assert!(!replay.changed());
    assert_eq!(replay.snapshot(), &initial);

    let error = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: replacement_authority,
            authority_now_ms: 40_999,
            horizon_duration_ms: 30_000,
        })
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
            authority_now_ms: 40_999,
            fenced_until_ms: 41_000,
        }
    ));

    let replacement = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: replacement_authority,
            authority_now_ms: 41_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap()
        .into_snapshot();
    let horizon = replacement.lease_grant_horizon().unwrap();
    assert_eq!(horizon.authority(), replacement_authority);
    assert_eq!(horizon.grant_not_after_ms(), 71_000);
    assert_eq!(replacement.max_committed_timestamp_ms(), Some(41_000));
}

#[test]
fn single_authority_heartbeat_establishes_reuses_and_restores_lease_horizon() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let observed_epoch = control_plane.snapshot().cluster_epoch();

    let first = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, observed_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let first_horizon = control_plane.snapshot().lease_grant_horizon().unwrap();
    assert_eq!(
        first_horizon.grant_not_after_ms(),
        1_000 + CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS
    );
    assert!(control_plane
        .snapshot()
        .lease_grant_horizon_covers(authority, first.lease().lease_deadline_ms()));

    let current_epoch = control_plane.snapshot().cluster_epoch();
    let acknowledged = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    assert_eq!(acknowledged.lease().lease_deadline_ms(), 12_100);
    assert_eq!(
        control_plane.snapshot().lease_grant_horizon(),
        Some(first_horizon),
        "a covered heartbeat must not extend the durable horizon"
    );
    let durable_after_epoch_acknowledgement = std::fs::read(&store_path).unwrap();
    let persisted_after_epoch_acknowledgement = store.load().unwrap().unwrap();
    let journal_offset_before_volatile_renewal = store.journal.clean_len().unwrap();

    let renewed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_001),
            12_001,
            authority,
        )
        .unwrap();
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        durable_after_epoch_acknowledgement,
        "an unchanged heartbeat covered by the durable horizon must not rewrite state"
    );
    assert_eq!(
        store.journal.clean_len().unwrap(),
        journal_offset_before_volatile_renewal,
        "an unchanged heartbeat covered by the durable horizon must not append a journal record"
    );
    assert_eq!(
        control_plane
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(renewed.lease().lease_deadline_ms()),
        "the volatile lease renewal must still be visible to live serving checks"
    );
    assert_eq!(
        store.load().unwrap().unwrap().lease_grant_horizon(),
        Some(first_horizon),
        "the heartbeat and horizon must have identical restart state"
    );
    assert_eq!(
        store
            .load()
            .unwrap()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        persisted_after_epoch_acknowledgement
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        "volatile renewal must not alter the restart lease"
    );

    let before_replacement = control_plane.snapshot().clone();
    let replacement_authority = LeaseHorizonAuthorityBinding::new(8, None);
    let error = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_002),
            12_002,
            replacement_authority,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::PreviousLeaseGrantHorizonStillActive { .. }
    ));
    assert_eq!(control_plane.snapshot(), &before_replacement);
}

#[test]
fn single_authority_promotes_volatile_lease_before_unrelated_durable_command() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let initial_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, initial_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let current_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    let renewed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_001),
            12_001,
            authority,
        )
        .unwrap();
    let acknowledged_deadline = renewed.lease().lease_deadline_ms();

    control_plane
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    assert_eq!(
        store
            .load()
            .unwrap()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(acknowledged_deadline),
        "the unrelated command must first promote the acknowledged volatile lease"
    );
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(acknowledged_deadline)
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn single_authority_promotes_volatile_lease_before_semantic_heartbeat() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .bootstrap_initial_cluster_map(
            vec![(NodeId::new(1), "/tmp/node-1.sock".to_owned())],
            vec![PgId::new(7)],
        )
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let initial_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, initial_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let current_epoch = control_plane.snapshot().cluster_epoch();
    let volatile = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    let acknowledged_deadline = volatile.lease().lease_deadline_ms();
    let mut semantic = heartbeat(1, current_epoch, 12_001);
    semantic.requested_lease_duration_ms = 1;
    semantic.endpoint = "/tmp/node-1-moved.sock".to_owned();

    let refreshed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(semantic, 12_001, authority)
        .unwrap();

    assert_eq!(refreshed.lease().lease_deadline_ms(), acknowledged_deadline);
    let durable = store.load().unwrap().unwrap();
    assert_eq!(
        durable.node(NodeId::new(1)).unwrap().lease_deadline_ms(),
        Some(acknowledged_deadline)
    );
    assert_eq!(
        durable.node(NodeId::new(1)).unwrap().endpoint(),
        "/tmp/node-1-moved.sock"
    );
}

#[test]
fn single_authority_exact_heartbeat_retransmission_does_not_append_journal() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    heartbeat_until_serving(&mut control_plane, 1, 1_000);
    let heartbeat_at_ms = 2_000;
    let heartbeat = heartbeat_from_record(
        &control_plane,
        1,
        control_plane.snapshot().cluster_epoch(),
        heartbeat_at_ms,
    );
    let first = control_plane
        .heartbeat(heartbeat.clone(), heartbeat_at_ms)
        .unwrap();
    let snapshot_after_first = control_plane.snapshot().clone();
    let journal_offset_after_first = store.journal.clean_len().unwrap();

    let retry = control_plane.heartbeat(heartbeat, heartbeat_at_ms).unwrap();

    assert_eq!(retry.lease_deadline_ms(), first.lease_deadline_ms());
    assert_eq!(control_plane.snapshot(), &snapshot_after_first);
    assert_eq!(
        store.journal.clean_len().unwrap(),
        journal_offset_after_first,
        "an exact heartbeat retry must not append a non-mutating journal record"
    );
}

#[test]
fn rejected_horizon_enabled_heartbeat_leaves_durable_state_unchanged() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = control_plane.snapshot().clone();
    let future_epoch = ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap();

    let error = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, future_epoch, 1_000),
            1_000,
            LeaseHorizonAuthorityBinding::new(7, None),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::FutureNodeObservedEpoch { .. }
    ));
    assert_eq!(control_plane.snapshot(), &before);
    assert_eq!(store.load().unwrap(), Some(before));
}

#[test]
fn lease_grant_horizon_command_rejects_invalid_duration_and_timestamp() {
    let authority = LeaseHorizonAuthorityBinding::new(1, None);
    let baseline = ClusterControlSnapshot::empty();
    for duration_ms in [0, MAX_LEASE_GRANT_HORIZON_MS + 1] {
        assert!(matches!(
            baseline.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority,
                authority_now_ms: 1_000,
                horizon_duration_ms: duration_ms,
            }),
            Err(ControlPlaneError::InvalidLeaseGrantHorizonDuration { .. })
        ));
    }
    assert!(matches!(
        baseline.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: u64::MAX,
            horizon_duration_ms: 1,
        }),
        Err(ControlPlaneError::LeaseGrantHorizonTimestampOverflow { .. })
    ));

    let established = baseline
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: 1_000,
            horizon_duration_ms: 10_000,
        })
        .unwrap()
        .into_snapshot();
    assert!(matches!(
        established.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: 999,
            horizon_duration_ms: 10_000,
        }),
        Err(ControlPlaneError::CommittedTimestampRegression { .. })
    ));
}

#[test]
fn lease_grant_horizon_round_trips_canonical_snapshot_state() {
    let snapshot = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: LeaseHorizonAuthorityBinding::new(9, Some(13)),
            authority_now_ms: 5_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap()
        .into_snapshot();
    let encoded = format_snapshot(&snapshot);
    assert!(encoded.contains("lease_grant_horizon=9,13,35000\n"));
    assert_eq!(parse_snapshot(&encoded).unwrap(), snapshot);

    for invalid in [
        encoded.replace("9,13,35000", "0,13,35000"),
        encoded.replace("9,13,35000", "9,0,35000"),
        encoded.replace("9,13,35000", "9,13,0"),
        encoded.replace(
            "max_committed_timestamp_ms=5000",
            "max_committed_timestamp_ms=-",
        ),
        encoded.replace("9,13,35000", "9,13,65001"),
    ] {
        assert!(parse_snapshot(&invalid).is_err());
    }
}

#[test]
fn replicated_snapshot_install_rejects_impossible_lease_grant_horizon() {
    for invalid_snapshot in [
        ClusterControlSnapshot::test_invalid_lease_grant_horizon(None, 30_000),
        ClusterControlSnapshot::test_invalid_lease_grant_horizon(
            Some(5_000),
            5_000 + MAX_LEASE_GRANT_HORIZON_MS + 1,
        ),
    ] {
        let payload =
            crate::control_plane_command::encode_control_plane_snapshot(&invalid_snapshot).unwrap();
        let mut state_machine =
            crate::control_plane_command::ReplicatedControlPlaneStateMachine::empty();
        let before = state_machine.clone();

        assert!(state_machine
            .install_snapshot_artifact(
                crate::control_plane_command::ControlPlaneSnapshotArtifact::new(None, payload)
            )
            .is_err());
        assert_eq!(state_machine, before);
    }
}

#[test]
fn single_authority_linearized_command_sink_persists_submitted_command() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(state_path.clone());
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();

    let applied = ControlPlaneLinearizedCommandSink::submit_control_plane_command(
        &mut authority,
        ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        },
    )
    .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::SetNodeMembership
    );
    assert_eq!(applied.snapshot(), authority.snapshot());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert_eq!(store.load().unwrap().unwrap(), *authority.snapshot());
    assert!(std::fs::metadata(state_path).unwrap().is_file());
}

#[test]
fn single_authority_linearized_runtime_map_read_carries_freshness_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let authority = SingleAuthorityControlPlane::open(store).unwrap();

    let runtime_map =
        ControlPlaneLinearizedRuntimeMapSource::linearized_runtime_map_snapshot(&authority, 12_345)
            .unwrap();

    assert_eq!(
        runtime_map.freshness_proof(),
        &RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation: authority.snapshot().authority_incarnation(),
            issued_at_ms: 12_345,
        }
    );
}

#[test]
fn record_node_heartbeat_command_records_current_epoch_pg_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();

    let observed_epoch = authority.snapshot().cluster_epoch();
    let metadata_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    let mut heartbeat = heartbeat_from_record(&authority, 1, observed_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(7),
        state: PgState::Peering,
        metadata_proof,
        pending_metadata_command: None,
    }];
    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::RecordNodeHeartbeat
    );
    let snapshot = applied.snapshot();
    assert_eq!(snapshot.cluster_epoch(), before.cluster_epoch());
    assert_eq!(snapshot.max_committed_timestamp_ms(), Some(2_000));
    let record = snapshot.node(NodeId::new(1)).unwrap();
    assert_eq!(record.last_observed_epoch(), Some(observed_epoch));
    assert_eq!(record.last_heartbeat_ms(), Some(2_000));
    assert_eq!(record.lease_deadline_ms(), Some(2_100));
    let observation = record.pg_observation(PgId::new(7)).unwrap();
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(observation.observed_epoch(), observed_epoch);
    assert_eq!(observation.observed_at_ms(), 2_000);
    assert_eq!(observation.metadata_proof(), metadata_proof);
    assert!(!observation.has_pending_metadata_command());
}

#[test]
fn record_node_heartbeat_command_stale_epoch_clears_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
        .unwrap();

    let current_epoch = authority.snapshot().cluster_epoch();
    let stale_epoch = ClusterEpoch::new(current_epoch.get() - 1).unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, stale_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(8),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap();

    let snapshot = applied.snapshot();
    assert!(applied.changed());
    assert_eq!(snapshot.cluster_epoch(), before.cluster_epoch());
    assert_eq!(snapshot.max_committed_timestamp_ms(), Some(2_000));
    let record = snapshot.node(NodeId::new(1)).unwrap();
    assert_eq!(record.last_observed_epoch(), Some(stale_epoch));
    assert_eq!(record.last_heartbeat_ms(), Some(2_000));
    assert_eq!(record.lease_deadline_ms(), Some(2_100));
    assert!(record.pg_observation(PgId::new(8)).is_none());
}

#[test]
fn record_node_heartbeat_command_rejects_future_history_route_without_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let current_epoch = authority.snapshot().cluster_epoch();
    let future_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 2_000);
    heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            future_epoch,
            PgId::new(1),
        )]);
    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
            node_id: 1,
            route_epoch,
            validation_epoch,
            ..
        } if route_epoch == future_epoch && validation_epoch == current_epoch
    ));
    assert_eq!(
        before
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );
    assert_eq!(before.cluster_epoch(), current_epoch);
}

#[test]
fn record_node_heartbeat_command_validates_committed_lease_deadline() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let current_epoch = authority.snapshot().cluster_epoch();
    let before = authority.snapshot().clone();
    let mut zero_duration = heartbeat_from_record(&authority, 1, current_epoch, 2_000);
    zero_duration.requested_lease_duration_ms = 0;
    assert!(matches!(
        before.apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: zero_duration,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_000,
            lease_horizon_authority: None,
        },),
        Err(ControlPlaneError::InvalidLeaseDuration)
    ));

    let mut overlong_duration = heartbeat_from_record(&authority, 1, current_epoch, 2_001);
    overlong_duration.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
    assert!(matches!(
        before.apply_control_plane_command(
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: overlong_duration,
                heartbeat_at_ms: 2_001,
                lease_deadline_ms: 2_001 + MAX_HEARTBEAT_LEASE_MS + 1,
                lease_horizon_authority: None,
            },
        ),
        Err(ControlPlaneError::LeaseDurationTooLong {
            requested_ms,
            max_ms,
        }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
            && max_ms == MAX_HEARTBEAT_LEASE_MS
    ));

    let heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 2_001);
    assert!(matches!(
        before.apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_001,
            lease_deadline_ms: 2_200,
            lease_horizon_authority: None,
        },),
        Err(ControlPlaneError::LeaseDeadlineMismatch {
            node_id: 1,
            heartbeat_at_ms: 2_001,
            requested_ms: 100,
            expected_deadline_ms: 2_101,
            actual_deadline_ms: 2_200,
        })
    ));
}

#[test]
fn record_node_heartbeat_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), 999);
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 999,
            lease_deadline_ms: 1_099,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 999,
            max_committed_timestamp_ms: 1_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn record_node_heartbeat_command_accepts_elapsed_forward_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let heartbeat_at_ms = 1_001 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1;
    let heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), heartbeat_at_ms);
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + 100,
            lease_horizon_authority: None,
        })
        .unwrap();

    assert_eq!(
        applied.snapshot().max_committed_timestamp_ms(),
        Some(heartbeat_at_ms)
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(heartbeat_at_ms + 100)
    );
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn record_node_heartbeat_command_rejects_lease_deadline_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let mut heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), 1_001);
    heartbeat.requested_lease_duration_ms = 50;
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 1_001,
            lease_deadline_ms: 1_051,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::NodeLeaseDeadlineRegression {
            node_id: 1,
            current_lease_deadline_ms: 1_101,
            requested_lease_deadline_ms: 1_051,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn expire_heartbeat_leases_command_replays_with_committed_expiry_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            9,
            PgState::Peering,
            1_010 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(9),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_020,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_030);

    let before = authority.snapshot().clone();
    let not_yet_expired = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 1_099,
        })
        .unwrap();
    assert!(not_yet_expired.changed());
    assert_eq!(
        not_yet_expired.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: Vec::new(),
            peering_pgs: Vec::new(),
        }
    );
    let mut expected_not_yet_expired = before.clone();
    expected_not_yet_expired.record_committed_timestamp(1_099);
    assert_eq!(not_yet_expired.snapshot(), &expected_not_yet_expired);

    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 1_130,
        })
        .unwrap();
    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: vec![NodeId::new(1), NodeId::new(2)],
            peering_pgs: vec![PgId::new(9)],
        }
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    assert_eq!(
        applied.snapshot().pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        applied.snapshot().cluster_epoch(),
        ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap()
    );
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(1_130));
}

#[test]
fn targeted_heartbeat_expiry_does_not_expire_unlisted_volatile_renewals() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_000).serving());

    let horizon_authority = LeaseHorizonAuthorityBinding::new(7, Some(2));
    let authority_now_ms = authority.snapshot().max_committed_timestamp_ms().unwrap();
    let with_horizon = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: horizon_authority,
            authority_now_ms,
            horizon_duration_ms: CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS,
        })
        .unwrap()
        .into_snapshot();
    let node_1_deadline = with_horizon
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let node_2_deadline = with_horizon
        .node(NodeId::new(2))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expire_at_ms = node_1_deadline.max(node_2_deadline);

    let applied = with_horizon
        .apply_control_plane_command(ControlPlaneCommand::ExpireNodeHeartbeatLeases {
            authority: horizon_authority,
            expire_at_ms,
            expired: vec![ExpiredNodeHeartbeatLease {
                node_id: NodeId::new(1),
                lease_deadline_ms: node_1_deadline,
            }],
        })
        .unwrap();

    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: vec![NodeId::new(1)],
            peering_pgs: Vec::new(),
        }
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .observed_availability(),
        NodeAvailabilityState::Unavailable
    );
    let unlisted = applied.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(
        unlisted.observed_availability(),
        NodeAvailabilityState::Healthy,
        "a durable deadline that looks expired must not override a newer volatile grant"
    );
    assert_eq!(unlisted.lease_deadline_ms(), Some(node_2_deadline));
}

#[test]
fn targeted_heartbeat_expiry_transitions_horizon_after_successor_fence() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let previous_authority = LeaseHorizonAuthorityBinding::new(7, Some(1));
    let successor_authority = LeaseHorizonAuthorityBinding::new(8, Some(2));
    let authority_now_ms = authority.snapshot().max_committed_timestamp_ms().unwrap();
    let baseline = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: previous_authority,
            authority_now_ms,
            horizon_duration_ms: CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS,
        })
        .unwrap()
        .into_snapshot();
    let lease_deadline_ms = baseline
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let successor_fence_ms = baseline.lease_grant_horizon().unwrap().grant_not_after_ms()
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    let command = |expire_at_ms| ControlPlaneCommand::ExpireNodeHeartbeatLeases {
        authority: successor_authority,
        expire_at_ms,
        expired: vec![ExpiredNodeHeartbeatLease {
            node_id: NodeId::new(1),
            lease_deadline_ms,
        }],
    };

    assert!(matches!(
        baseline.apply_control_plane_command(command(successor_fence_ms - 1)),
        Err(ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
            authority_now_ms,
            fenced_until_ms,
        }) if authority_now_ms == successor_fence_ms - 1
            && fenced_until_ms == successor_fence_ms
    ));

    let applied = baseline
        .apply_control_plane_command(command(successor_fence_ms))
        .unwrap();
    assert_eq!(
        applied.snapshot().lease_grant_horizon_authority(),
        Some(successor_authority)
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .observed_availability(),
        NodeAvailabilityState::Unavailable
    );
}

#[test]
fn expire_heartbeat_leases_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 999,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 999,
            max_committed_timestamp_ms: 1_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn expire_heartbeat_leases_command_accepts_elapsed_forward_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority.expire_heartbeat_leases(1_101).unwrap();

    let before = authority.snapshot().clone();
    let expire_at_ms = 1_101 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1;
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms })
        .unwrap();

    assert_eq!(
        applied.snapshot().max_committed_timestamp_ms(),
        Some(expire_at_ms)
    );
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn authority_clock_rejects_restart_discontinuity_before_expiry() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    drop(authority);
    let store = FileControlPlaneStore::new(&state_path);
    let authority = SingleAuthorityControlPlane::open(store).unwrap();
    let before = authority.snapshot().clone();
    let far_future_now_ms = 1_001 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 123;
    let mut clock = ControlPlaneAuthorityClock::new(
        before.max_committed_timestamp_ms(),
        far_future_now_ms,
        Some(20),
    )
    .unwrap();
    assert!(matches!(
        clock.effective_now_ms(far_future_now_ms, Some(20)),
        Err(ControlPlaneError::AuthorityClockNotEstablished {
            blocked_reason: Some(
                ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity
            ),
        })
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn authority_clock_accepts_long_restart_when_wall_and_health_elapsed_match() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        3_601_000,
        Some(3_600_050),
        Some(checkpoint),
    )
    .unwrap();

    assert_eq!(
        clock.effective_now_ms(3_601_001, Some(3_600_051)).unwrap(),
        3_601_001
    );
}

#[test]
fn authority_clock_restart_checkpoint_rejects_elapsed_clock_divergence() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        3_601_000 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1,
        Some(3_600_050),
        Some(checkpoint),
    )
    .unwrap();

    assert!(matches!(
        clock.effective_now_ms(
            3_601_000 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1,
            Some(3_600_050),
        ),
        Err(ControlPlaneError::AuthorityClockNotEstablished {
            blocked_reason: Some(
                ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity
            ),
        })
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_health_regression_and_wrong_high_water() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 500);
    let health_regressed = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_100,
        Some(499),
        Some(checkpoint),
    )
    .unwrap();
    assert!(!health_regressed
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_000),
            None,
            true,
            true,
        ))
        .established());

    let checkpoint_ahead =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_002), 1_002, 500);
    let wrong_high_water = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_001),
        1_102,
        Some(600),
        Some(checkpoint_ahead),
    )
    .unwrap();
    assert!(!wrong_high_water
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_001),
            None,
            true,
            true,
        ))
        .established());
}

#[test]
fn durable_authority_clock_requires_checkpoint_for_restored_timestamp_state() {
    let clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_000,
        Some(500),
        None,
    )
    .unwrap();

    assert!(!clock
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_000),
            None,
            true,
            true,
        ))
        .established());
}

#[test]
fn restarted_authority_clock_cannot_reuse_restored_lease_horizon_generation() {
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();

    clock
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    clock.bind_initial_raft_leadership_term(Some(11));

    let restarted_authority = clock.lease_horizon_authority_binding(Some(11)).unwrap();
    assert_eq!(restarted_authority.clock_generation(), 8);
    assert_ne!(restarted_authority, previous_authority);
}

#[test]
fn checkpoint_proven_single_authority_restart_resumes_lease_horizon_generation() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x41; 32]);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 7, Some(1_000), 1_000, 50);
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_001,
        Some(51),
        Some(checkpoint),
    )
    .unwrap();

    assert!(clock.resume_single_authority_lease_horizon_generation(previous_authority));
    assert_eq!(
        clock.lease_horizon_authority_binding(None).unwrap(),
        previous_authority
    );
    assert!(!clock.resume_single_authority_lease_horizon_generation(previous_authority));
}

#[test]
fn single_authority_horizon_resume_requires_checkpoint_and_no_raft_term() {
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let mut unproven =
        ControlPlaneAuthorityClock::new_with_restart_checkpoint(Some(1_000), 1_001, Some(51), None)
            .unwrap();
    assert!(!unproven.resume_single_authority_lease_horizon_generation(previous_authority));

    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut raft_clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_001,
        Some(51),
        Some(checkpoint),
    )
    .unwrap();
    assert!(
        !raft_clock.resume_single_authority_lease_horizon_generation(
            LeaseHorizonAuthorityBinding::new(7, Some(11))
        )
    );
}

#[test]
fn recovered_clock_checkpoint_cannot_resume_an_older_horizon_generation() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x43; 32]);
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), None, true, true);
    let mut recovered =
        ControlPlaneAuthorityClock::new_with_restart_checkpoint(Some(1_000), 1_000, Some(50), None)
            .unwrap();
    recovered
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    assert_eq!(recovered.status(context).generation(), 8);
    recovered
        .reestablish(8, Some(1_000), None, context, 1_000, Some(50))
        .unwrap();
    let checkpoint = validated_authority_clock_restart_checkpoint(
        binding,
        Some(1_000),
        &mut recovered,
        1_001,
        Some(51),
    )
    .unwrap();
    assert_eq!(checkpoint.authority_generation(), 9);

    let mut restarted = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_002,
        Some(52),
        Some(checkpoint),
    )
    .unwrap();
    assert_eq!(restarted.status(context).generation(), 9);
    assert!(!restarted.resume_single_authority_lease_horizon_generation(previous_authority));
    restarted
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    assert_eq!(
        restarted
            .lease_horizon_authority_binding(None)
            .unwrap()
            .clock_generation(),
        9
    );
}

#[test]
fn file_backed_authority_does_not_replace_blocked_restart_checkpoint() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    crate::clock::with_time_override(1_000, || {
        let store = FileControlPlaneStore::new(&state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    });
    let binding = FileControlPlaneStore::new(&state_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let invalid_checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_001), 1_000, 2_000);
    std::fs::write(
        authority_clock_restart_checkpoint_path(&state_path),
        invalid_checkpoint.encode(),
    )
    .unwrap();

    for _ in 0..2 {
        crate::clock::with_time_override(5_000, || {
            let store = FileControlPlaneStore::new(&state_path);
            let authority = SingleAuthorityControlPlane::open(store).unwrap();
            assert_eq!(
                load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
                Some(invalid_checkpoint)
            );
            let clock = ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
                authority.snapshot().max_committed_timestamp_ms(),
                Some(invalid_checkpoint),
            )
            .unwrap();
            assert!(!clock
                .status(ControlPlaneAuthorityClockContext::new(
                    authority.snapshot().max_committed_timestamp_ms(),
                    None,
                    true,
                    true,
                ))
                .established());
        });
    }
}

#[test]
fn authority_clock_restart_checkpoint_file_round_trips_and_rejects_corruption() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = FileControlPlaneStore::new(&state_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let stored = crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 9, Some(4_999)).unwrap()
    });
    assert_eq!(stored.authority_generation(), 9);
    assert_eq!(
        load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
        Some(stored)
    );

    let checkpoint_path = authority_clock_restart_checkpoint_path(&state_path);
    for version in [
        CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION - 1,
        CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION + 1,
    ] {
        let mut bytes = stored.encode();
        let version_offset = CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len();
        bytes[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&checkpoint_path, bytes).unwrap();
        assert!(matches!(
            load_authority_clock_restart_checkpoint(&state_path, binding),
            Err(ControlPlaneError::AuthorityClockCheckpoint { message })
                if message == format!("unsupported checkpoint version {version}")
        ));
    }

    let zero_generation =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 0, Some(4_999), 5_000, 5_000);
    std::fs::write(&checkpoint_path, zero_generation.encode()).unwrap();
    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("generation must be nonzero")
    ));

    let mut bytes = stored.encode();
    bytes[CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len() + 2] ^= 1;
    std::fs::write(checkpoint_path, bytes).unwrap();
    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("checksum mismatch")
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_wrong_raft_cluster_and_node() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 1);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 1, Some(4_999)).unwrap();
    });

    for wrong_binding in [
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-b", 1),
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 2),
    ] {
        assert!(matches!(
            load_authority_clock_restart_checkpoint(&state_path, wrong_binding),
            Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
                if message.contains("identity does not match")
        ));
    }
}

#[test]
fn authority_clock_restart_checkpoint_rejects_wrong_single_authority_identity() {
    let tmp = test_util::tempdir();
    let first_path = tmp.path().join("first.state");
    let second_path = tmp.path().join("second.state");
    let first_binding = FileControlPlaneStore::new(&first_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let second_binding = FileControlPlaneStore::new(&second_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    assert_ne!(first_binding, second_binding);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&first_path, first_binding, 1, Some(4_999))
            .unwrap();
    });

    assert!(matches!(
        load_authority_clock_restart_checkpoint(&first_path, second_binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("identity does not match")
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_oversized_sparse_file_before_reading() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 1);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 1, Some(4_999)).unwrap();
    });
    std::fs::OpenOptions::new()
        .write(true)
        .open(authority_clock_restart_checkpoint_path(&state_path))
        .unwrap()
        .set_len(1 << 30)
        .unwrap();

    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("does not match required fixed length")
    ));
}

#[test]
fn file_backed_authority_restarts_after_long_elapsed_downtime_without_recovery() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    crate::clock::with_time_override(1_001, || {
        let store = FileControlPlaneStore::new(&state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    });

    let store = FileControlPlaneStore::new(&state_path);
    let binding = store
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let checkpoint = store
        .load_authority_clock_restart_checkpoint(binding)
        .unwrap()
        .expect("file-backed state should have a clock checkpoint");
    crate::clock::with_time_override(3_601_001, || {
        let authority = SingleAuthorityControlPlane::open(store).unwrap();
        let clock = ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
            authority.snapshot().max_committed_timestamp_ms(),
            Some(checkpoint),
        )
        .unwrap();
        assert!(clock
            .status(ControlPlaneAuthorityClockContext::new(
                authority.snapshot().max_committed_timestamp_ms(),
                None,
                true,
                true,
            ))
            .established());
    });
}

#[test]
fn authority_clock_accepts_healthy_elapsed_time_after_idle() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    assert_eq!(
        clock.effective_now_ms(11_000, Some(10_050)).unwrap(),
        11_000
    );
}

#[test]
fn authority_clock_rejects_missing_initial_health_sample() {
    assert!(matches!(
        ControlPlaneAuthorityClock::new(Some(1_000), 1_000, None),
        Err(ControlPlaneError::AuthorityClockSourceUnavailable)
    ));
}

#[test]
fn authority_clock_latches_later_health_source_failure() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    assert!(matches!(
        clock.effective_now_ms(1_100, None),
        Err(ControlPlaneError::AuthorityClockSourceUnavailable)
    ));
    assert!(clock.effective_now_ms(1_101, Some(151)).is_err());
}

#[test]
fn authority_clock_status_observes_new_raft_term_before_serving_request() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), Some(8), true, true);

    let status = clock.observe_status(context, 1_100, Some(150)).unwrap();

    assert!(!status.established());
    assert_eq!(
        status.blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged)
    );
    assert_eq!(status.bound_raft_leadership_term(), Some(7));
    assert_eq!(status.current_raft_leadership_term(), Some(8));
    assert!(status.local_raft_authority_leader());
    assert!(status.local_raft_authority_serving());
}

#[test]
fn authority_clock_status_codec_preserves_blocked_local_raft_leader() {
    let clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    let status = clock.status(ControlPlaneAuthorityClockContext::new(
        Some(1_000),
        Some(8),
        true,
        false,
    ));
    let mut encoded = Vec::new();
    write_authority_clock_status(&mut encoded, status);
    let mut reader = PayloadReader::new(&encoded);
    let decoded = read_authority_clock_status(&mut reader).unwrap();
    reader.finish().unwrap();

    assert!(decoded.local_raft_authority_leader());
    assert!(!decoded.local_raft_authority_serving());
    assert_eq!(decoded.current_raft_leadership_term(), Some(8));
}

#[test]
fn authority_clock_invalidates_new_local_raft_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    clock.validate_raft_leadership_term(7).unwrap();
    assert_eq!(clock.effective_now_ms(1_100, Some(150)).unwrap(), 1_100);

    assert!(matches!(
        clock.validate_raft_leadership_term(8),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: Some(7),
            current_term: 8,
        })
    ));
    assert!(clock.effective_now_ms(1_101, Some(151)).is_err());
}

#[test]
fn authority_clock_reestablishment_is_fenced_by_generation_timestamp_and_term() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    assert!(clock.validate_raft_leadership_term(8).is_err());
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), Some(8), true, true);
    let blocked = clock.status(context);
    assert!(!blocked.established());
    assert_eq!(
        blocked.blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged)
    );

    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(999),
            Some(8),
            context,
            1_100,
            Some(150),
        ),
        Err(ControlPlaneError::AuthorityClockCommittedTimestampMismatch { .. })
    ));
    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(1_000),
            Some(9),
            context,
            1_100,
            Some(150),
        ),
        Err(ControlPlaneError::AuthorityClockRaftTermMismatch { .. })
    ));

    let established = clock
        .reestablish(
            blocked.generation(),
            Some(1_000),
            Some(8),
            context,
            1_100,
            Some(150),
        )
        .unwrap();
    assert!(established.established());
    assert_eq!(established.bound_raft_leadership_term(), Some(8));
    assert_eq!(established.generation(), blocked.generation() + 1);

    assert!(clock.effective_now_ms(1_101, None).is_err());
    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(1_000),
            Some(8),
            context,
            1_102,
            Some(152),
        ),
        Err(ControlPlaneError::AuthorityClockGenerationMismatch { .. })
    ));
}

#[test]
fn authority_clock_reestablishment_rejects_follower_and_wall_behind_high_water() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(2_000), 4_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    assert!(clock.validate_raft_leadership_term(8).is_err());
    let follower_context =
        ControlPlaneAuthorityClockContext::new(Some(2_000), Some(8), false, false);
    let generation = clock.status(follower_context).generation();
    assert!(matches!(
        clock.reestablish(
            generation,
            Some(2_000),
            Some(8),
            follower_context,
            4_000,
            Some(50),
        ),
        Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority)
    ));

    let leader_context = ControlPlaneAuthorityClockContext::new(Some(2_000), Some(8), true, true);
    assert!(matches!(
        clock.reestablish(
            generation,
            Some(2_000),
            Some(8),
            leader_context,
            1_999,
            Some(50),
        ),
        Err(ControlPlaneError::AuthorityClockWallBehindCommittedTimestamp { .. })
    ));
}

#[test]
fn restored_follower_clock_cannot_establish_first_local_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(None);
    clock.observe_committed_timestamp_high_water(Some(1_000));

    assert!(matches!(
        clock.validate_raft_leadership_term(8),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: None,
            current_term: 8,
        })
    ));
}

#[test]
fn follower_role_without_timestamp_high_water_still_requires_reestablishment() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(None);

    assert!(matches!(
        clock.validate_raft_leadership_term(1),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: None,
            current_term: 1,
        })
    ));
}

#[test]
fn fresh_unbound_clock_can_bind_first_local_raft_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();

    clock.validate_raft_leadership_term(1).unwrap();
    assert_eq!(clock.effective_now_ms(1_001, Some(51)).unwrap(), 1_001);
}

#[test]
fn authority_clock_latches_forward_step_without_timestamp_ratchet() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority.expire_heartbeat_leases(1_101).unwrap();

    let before = authority.snapshot().clone();
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_101), 1_101, Some(10)).unwrap();
    let far_future_now_ms = 1_101 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 123;
    assert!(matches!(
        clock.effective_now_ms(far_future_now_ms, Some(11)),
        Err(ControlPlaneError::CommittedTimestampTooFarAhead { .. })
    ));
    for _ in 0..2 {
        assert!(matches!(
            clock.effective_now_ms(far_future_now_ms, Some(11)),
            Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump),
            })
        ));
    }
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn complete_pg_peering_command_replays_with_committed_completion_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 8, PgState::Peering, 1_010);

    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::CompletePgPeering {
            pg_id: PgId::new(8),
            primary: NodeId::new(1),
            node_incarnation: node_incarnation(&authority, 1),
            complete_at_ms: 1_011,
        })
        .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::CompletePgPeering
    );
    let pg = applied.snapshot().pg(PgId::new(8)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(
        applied.snapshot().cluster_epoch(),
        ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap()
    );
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(1_011));
}

#[test]
fn active_pg_primary_comes_from_authoritative_acting_set() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);

    assert!(matches!(
        authority.set_pg_state(PgId::new(8), PgState::Active),
        Err(ControlPlaneError::ActivePgRequiresPeeringComplete { pg_id: 8 })
    ));
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            8,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(8),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_050,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 8,
            node_id: 2
        })
    ));
    assert!(matches!(
        authority.complete_pg_peering(PgId::new(8), NodeId::new(99), 99, 2_050),
        Err(ControlPlaneError::PgPrimaryNotInActingSet {
            pg_id: 8,
            node_id: 99
        })
    ));
    authority
        .complete_pg_peering(
            PgId::new(8),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(8), 2_050), None);
    heartbeat_with_pg_observation(&mut authority, 1, 8, PgState::Active, 3_001);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
            3_002,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(8), 3_002),
        Some(NodeId::new(1))
    );
}

#[test]
fn heartbeat_refresh_completes_ready_peering_for_storage_node_before_frontend_export() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(peering_heartbeat, 2_000)
        .unwrap();

    let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(
        refresh.lease().cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert!(
        !refresh.lease().serving(),
        "peering completion bumps the epoch before the node observes it"
    );
    let storage_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(22))
        .unwrap();
    assert_eq!(storage_route.state(), PgState::Active);
    assert!(matches!(
        authority.snapshot().runtime_map(2_001),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 22, .. })
    ));

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_002);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(active_heartbeat, 2_002)
        .unwrap();
    assert!(refresh.lease().serving());
    let frontend_map = authority.snapshot().runtime_map(2_003).unwrap();
    assert_eq!(
        frontend_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(22))
            .unwrap()
            .state(),
        PgState::Active
    );
}

#[test]
fn storage_node_refresh_hands_active_route_to_primary_after_non_primary_completes_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(77), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };

    for node_id in [1, 2] {
        let now_ms = 2_000 + u64::from(node_id);
        let mut heartbeat = heartbeat_from_record(&authority, node_id, peering_epoch, now_ms);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(77),
            state: PgState::Peering,
            metadata_proof: proof,
            pending_metadata_command: None,
        }];
        if node_id == 1 {
            authority.heartbeat(heartbeat, now_ms).unwrap();
        } else {
            let non_primary_handoff = authority.refresh_node_heartbeat(heartbeat, now_ms).unwrap();
            let route = non_primary_handoff
                .runtime_map()
                .pg_routes()
                .iter()
                .find(|route| route.pg_id() == PgId::new(77))
                .unwrap();
            assert_eq!(route.state(), PgState::Active);
            assert_eq!(route.primary_node_id(), NodeId::new(1));
            assert_eq!(route.primary_lease_deadline_ms(), None);
        }
    }
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(active_epoch > peering_epoch);
    let active_pg = authority.snapshot().pg(PgId::new(77)).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert!(matches!(
        authority.snapshot().runtime_map(2_003),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 77, .. })
    ));

    let stale_primary_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_004);
    let primary_handoff = authority
        .refresh_node_heartbeat(stale_primary_heartbeat, 2_004)
        .unwrap();
    assert!(
        !primary_handoff.lease().serving(),
        "the primary still has to observe the new epoch before serving"
    );
    let route = primary_handoff
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(77))
        .unwrap();
    assert_eq!(route.cluster_epoch(), active_epoch);
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.primary_node_id(), NodeId::new(1));

    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_005);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(77),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    let active_refresh = authority
        .refresh_node_heartbeat(active_heartbeat, 2_005)
        .unwrap();
    assert!(active_refresh.lease().serving());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(77), 2_006),
        Some(NodeId::new(1))
    );
    assert!(authority.snapshot().runtime_map(2_006).is_ok());
}

#[test]
fn storage_node_refresh_recovers_when_another_active_primary_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    for (pg_id, node_id, now_ms) in [(80, 1, 2_000), (81, 2, 2_100)] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(node_id)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, node_id, pg_id, PgState::Peering, now_ms);
        authority
            .complete_pg_peering(
                PgId::new(pg_id),
                NodeId::new(node_id),
                node_incarnation(&authority, node_id),
                now_ms + 1,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, node_id, pg_id, PgState::Active, now_ms + 2);
    }

    let active_epoch = authority.snapshot().cluster_epoch();
    for (pg_id, node_id, now_ms) in [(80, 1, 3_000), (81, 2, 3_001)] {
        let mut heartbeat = heartbeat_from_record(&authority, node_id, active_epoch, now_ms);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(pg_id),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap();
    }

    let mut node_2_heartbeat = heartbeat_from_record(&authority, 2, active_epoch, 3_200);
    node_2_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(81),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let node_2_refresh = authority
        .refresh_node_heartbeat(node_2_heartbeat, 3_200)
        .expect("one expired primary must not prevent another node from refreshing");
    let unavailable_route = node_2_refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(80))
        .unwrap();
    assert_eq!(unavailable_route.state(), PgState::Active);
    assert_eq!(unavailable_route.primary_node_id(), NodeId::new(1));
    assert_eq!(unavailable_route.primary_lease_deadline_ms(), None);

    let mut node_1_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 3_201);
    node_1_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(80),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let node_1_refresh = authority
        .refresh_node_heartbeat(node_1_heartbeat, 3_201)
        .expect("the expired primary must be able to renew after receiving the current map");
    assert!(node_1_refresh.lease().serving());
    assert!(authority.snapshot().runtime_map(3_202).is_ok());
}

#[test]
fn storage_node_refresh_does_not_block_non_actor_on_pending_active_handoff() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(70), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 70, PgState::Peering, 1_010);
    authority
        .complete_pg_peering(
            PgId::new(70),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_011,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 70, PgState::Active, 1_012);
    authority
        .set_pg_acting_set(PgId::new(71), vec![NodeId::new(2)])
        .unwrap();
    let mut next_snapshot = authority.snapshot().clone();
    {
        let pg = next_snapshot.pgs.get_mut(&PgId::new(71)).unwrap();
        pg.state = PgState::Active;
        pg.active_primary = Some(NodeId::new(2));
        pg.active_metadata_proof = Some(PgMetadataProof::empty());
        pg.active_metadata_proof_epoch = Some(authority.snapshot().cluster_epoch());
    }
    next_snapshot.bump_epoch().unwrap();
    authority.commit_snapshot(next_snapshot).unwrap();

    let active_epoch = authority.snapshot().cluster_epoch();
    let pg_71 = authority.snapshot().pg(PgId::new(71)).unwrap();
    assert_eq!(pg_71.state(), PgState::Active);
    assert_eq!(pg_71.active_primary(), Some(NodeId::new(2)));
    assert!(authority.snapshot().runtime_map(1_013).is_err());

    let mut unrelated_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 1_014);
    unrelated_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(70),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(unrelated_heartbeat, 1_014)
        .unwrap();
    let unrelated_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(70))
        .unwrap();
    assert_eq!(unrelated_route.state(), PgState::Active);
    let pending_handoff_route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(71))
        .unwrap();
    assert_eq!(pending_handoff_route.state(), PgState::Active);
    assert_eq!(pending_handoff_route.primary_node_id(), NodeId::new(2));
    assert_eq!(pending_handoff_route.primary_lease_deadline_ms(), None);
}

#[test]
fn active_pg_primary_is_bound_by_peering_completion() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            20,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(20))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(1))
    );

    heartbeat_with_pg_observation(&mut authority, 2, 20, PgState::Active, 3_001);
    assert_eq!(authority.serving_pg_primary(PgId::new(20), 3_001), None);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_002,
        ),
        Err(ControlPlaneError::PgNotPeering {
            pg_id: 20,
            state: PgState::Active,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 20, PgState::Active, 3_003);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(20), 3_003),
        Some(NodeId::new(1))
    );
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_004,
        )
        .unwrap();
}

#[test]
fn active_pg_route_requires_bound_primary_and_active_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(21), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(21),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();

    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(21), 2_002),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 21, .. })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Active, 2_003);
    let route = authority
        .snapshot()
        .active_pg_route(PgId::new(21), 2_004)
        .unwrap();
    assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
    assert_eq!(route.pg_id(), PgId::new(21));
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), &[NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(
        route.primary_lease_deadline_ms(),
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
    );
    assert_eq!(
        authority.snapshot().active_pg_routes(2_004).unwrap(),
        vec![route.clone()]
    );

    let local_route = crate::cluster::LocalPgRoute::from(&route);
    assert_eq!(local_route.cluster_epoch(), route.cluster_epoch());
    assert_eq!(local_route.pg_id(), route.pg_id());
    assert_eq!(local_route.primary_node_id(), route.primary_node_id());
    assert_eq!(local_route.acting_set(), route.acting_set());
    assert_eq!(local_route.state(), route.state());

    let storage_node_route = crate::storage_node_server::StorageNodePgRoute::from(&route);
    assert_eq!(storage_node_route.cluster_epoch, route.cluster_epoch());
    assert_eq!(storage_node_route.pg_id, route.pg_id().get());
    assert_eq!(storage_node_route.primary_node_id, route.primary_node_id());
    assert_eq!(storage_node_route.acting_set, route.acting_set());
    assert_eq!(storage_node_route.state, route.state());
}

#[test]
fn active_primary_service_requires_current_observation_metadata_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let accepted_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(22), 2_030)
        .is_ok());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(22), 2_030),
        Some(NodeId::new(1))
    );
    assert!(authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_030,
        )
        .is_ok());

    let progressed_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xabd,
        state_digest: 0xdf0,
    };
    let mut progressed_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_031);
    progressed_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(progressed_heartbeat, 2_031).unwrap();

    assert_eq!(
        authority.serving_pg_primary(PgId::new(22), 2_031),
        Some(NodeId::new(1))
    );
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(22), 2_031)
        .is_ok());
    assert!(authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_031, NodeId::new(1), ClusterEpoch::INITIAL)
        .is_ok());
    assert!(authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_031,
        )
        .is_ok());

    let mismatched_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xabe,
        state_digest: 0xdf1,
    };
    authority
        .snapshot
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .pg_observations
        .get_mut(&PgId::new(22))
        .unwrap()
        .metadata_proof = mismatched_proof;

    assert_eq!(authority.serving_pg_primary(PgId::new(22), 2_032), None);
    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(22), 2_032),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
    assert!(matches!(
        authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(2_032, NodeId::new(1), ClusterEpoch::INITIAL),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_032,
        ),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == progressed_proof && actual == mismatched_proof
    ));
}

#[test]
fn active_primary_heartbeat_does_not_promote_digest_only_cleanup_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    let accepted_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(22),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    let accepted_proof_epoch = authority
        .snapshot()
        .pg(PgId::new(22))
        .unwrap()
        .active_metadata_proof_epoch();

    let cleanup_proof = PgMetadataProof {
        applied_log_index: accepted_proof.applied_log_index,
        applied_log_hash: accepted_proof.applied_log_hash,
        state_digest: 0xdf0,
    };
    let mut cleanup_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_030);
    cleanup_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Active,
        metadata_proof: cleanup_proof,
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(cleanup_heartbeat, 2_030),
        Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: 22,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == accepted_proof && actual == cleanup_proof
    ));

    let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), accepted_proof_epoch);
    assert!(authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_031, NodeId::new(1), ClusterEpoch::INITIAL)
        .is_ok());
}

#[test]
fn non_primary_active_observation_cannot_satisfy_primary_active_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let accepted_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        let mut peering_heartbeat = heartbeat_from_record(
            &authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            2_000 + u64::from(node_id),
        );
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(23),
            state: PgState::Peering,
            metadata_proof: accepted_proof,
            pending_metadata_command: None,
        }];
        authority
            .heartbeat(peering_heartbeat, 2_000 + u64::from(node_id))
            .unwrap();
    }
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let progressed_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xabd,
        state_digest: 0xdf0,
    };
    let mut non_primary_active =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_020);
    non_primary_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(23),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(non_primary_active, 2_020).unwrap();
    assert_eq!(authority.serving_pg_primary(PgId::new(23), 2_030), None);
    assert!(authority
        .snapshot()
        .active_pg_route(PgId::new(23), 2_030)
        .is_err());
    let non_primary_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_030, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();
    let route = non_primary_runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(23))
        .unwrap();
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.primary_lease_deadline_ms(), None);
}

#[test]
fn non_primary_active_observation_may_lag_active_primary_metadata_proof() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(path.clone());
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(&mut authority, node_id, 24, PgState::Peering, 2_000);
    }
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let accepted_proof = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let stale_proof = PgMetadataProof::empty();
    let mut next_snapshot = authority.snapshot().clone();
    {
        let pg = next_snapshot.pgs.get_mut(&PgId::new(24)).unwrap();
        pg.active_metadata_proof = Some(accepted_proof);
    }
    let active_epoch = next_snapshot.cluster_epoch();
    for node_id in [1, 2] {
        let node = next_snapshot.nodes.get_mut(&NodeId::new(node_id)).unwrap();
        node.last_observed_epoch = Some(active_epoch);
        node.last_heartbeat_ms = Some(2_011);
        node.lease_deadline_ms = Some(3_011);
        node.pg_observations.insert(
            PgId::new(24),
            NodePgObservationRecord {
                pg_id: PgId::new(24),
                state: PgState::Active,
                observed_epoch: active_epoch,
                observed_at_ms: 2_011,
                metadata_proof: if node_id == 1 {
                    accepted_proof
                } else {
                    stale_proof
                },
                pending_metadata_command: None,
            },
        );
    }
    authority.commit_snapshot(next_snapshot).unwrap();

    let mut stale_replica_heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_020);
    stale_replica_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(24),
        state: PgState::Active,
        metadata_proof: stale_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_replica_heartbeat, 2_020).unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(24), 2_021),
        Some(NodeId::new(1))
    );
    assert!(authority.snapshot().runtime_map(2_021).is_ok());
}

#[test]
fn active_pg_route_fails_closed_for_peering_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();

    assert!(matches!(
        authority.snapshot().active_pg_route(PgId::new(22), 1_001),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 22,
            state: PgState::Peering,
            ..
        })
    ));
    assert!(authority
        .snapshot()
        .active_pg_routes(1_001)
        .unwrap()
        .is_empty());
}

#[test]
fn pg_route_exports_peering_pg_for_fail_closed_runtime_install() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let route = authority.snapshot().pg_route(PgId::new(24), 1_001).unwrap();
    assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
    assert_eq!(route.pg_id(), PgId::new(24));
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    assert_eq!(
        authority.snapshot().pg_routes(1_001).unwrap(),
        vec![route.clone()]
    );

    let local_route = crate::cluster::LocalPgRoute::from(&route);
    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2)],
        &[24],
        crate::EcShape { k: 1, m: 1 },
        authority.snapshot().cluster_epoch(),
        vec![local_route],
    )
    .unwrap();
    assert!(matches!(
        local_map.metadata_pg_primary_node(authority.snapshot().cluster_epoch(), PgId::new(24)),
        Err(crate::StoreError::PgNotActive {
            pg_id: 24,
            state: PgState::Peering,
            ..
        })
    ));
}

#[test]
fn pg_route_certifies_only_exact_committed_peering_metadata_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let committed_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            24,
            PgState::Peering,
            committed_proof,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        24,
        PgState::Active,
        committed_proof,
        false,
        2_011,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Active,
        committed_proof,
        false,
        2_012,
    );

    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        committed_proof,
        false,
        2_020,
    );
    let route = authority.snapshot().pg_route(PgId::new(24), 2_021).unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(
        route.metadata_read_route(),
        Some(PgMetadataReadRoute::new(NodeId::new(2), committed_proof))
    );
    let local_route = crate::cluster::LocalPgRoute::from(&route);
    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2)],
        &[24],
        crate::EcShape { k: 1, m: 1 },
        authority.snapshot().cluster_epoch(),
        vec![local_route],
    )
    .unwrap();
    assert_eq!(
        local_map
            .metadata_pg_read_node(authority.snapshot().cluster_epoch(), PgId::new(24))
            .unwrap()
            .node_id(),
        NodeId::new(2)
    );
    assert!(local_map
        .metadata_pg_primary_node(authority.snapshot().cluster_epoch(), PgId::new(24))
        .is_err());

    let ahead_proof = PgMetadataProof {
        applied_log_index: committed_proof.applied_log_index + 1,
        applied_log_hash: committed_proof.applied_log_hash + 1,
        state_digest: committed_proof.state_digest + 1,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        ahead_proof,
        false,
        2_022,
    );
    assert_eq!(
        authority
            .snapshot()
            .pg_route(PgId::new(24), 2_023)
            .unwrap()
            .metadata_read_route(),
        None,
        "uncertified progress beyond the committed floor must not become read authority"
    );

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        24,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        2_024,
    );
    assert_eq!(
        authority
            .snapshot()
            .pg_route(PgId::new(24), 2_025)
            .unwrap()
            .metadata_read_route(),
        None,
        "a replica below the committed floor must not be certified"
    );

    let mut pending_snapshot = authority.snapshot().clone();
    let pending_epoch = pending_snapshot.cluster_epoch();
    let pending_observation = pending_snapshot
        .nodes
        .get_mut(&NodeId::new(2))
        .unwrap()
        .pg_observations
        .get_mut(&PgId::new(24))
        .unwrap();
    pending_observation.metadata_proof = committed_proof;
    pending_observation.pending_metadata_command =
        Some(test_pending_metadata_command(pending_epoch));
    assert_eq!(
        peering_metadata_read_route_for_snapshot(
            &pending_snapshot,
            pending_snapshot.pg(PgId::new(24)).unwrap(),
            2_027,
        ),
        None,
        "a pending command makes the replica's visible state ambiguous"
    );
}

#[test]
fn peering_metadata_read_certification_requires_exact_expected_proof() {
    let floor_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let floor = PeeringMetadataProofFloor {
        proof: floor_proof,
        epoch: Some(ClusterEpoch::new(4).unwrap()),
        imported: false,
    };
    let ahead_proof = PgMetadataProof {
        applied_log_index: 8,
        applied_log_hash: 0x123,
        state_digest: 0x456,
    };
    assert!(peering_metadata_proof_is_read_certified(
        floor,
        None,
        floor_proof
    ));
    assert!(!peering_metadata_proof_is_read_certified(
        floor,
        None,
        ahead_proof
    ));

    let imported_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 0x789,
        state_digest: 0xabc,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        ClusterEpoch::new(3).unwrap(),
        floor_proof,
        imported_proof,
    );
    assert!(peering_metadata_proof_is_read_certified(
        floor,
        Some(transfer),
        imported_proof
    ));
    assert!(!peering_metadata_proof_is_read_certified(
        floor,
        Some(transfer),
        floor_proof
    ));
}

#[test]
fn runtime_map_exports_pg_routes_with_routed_node_endpoints() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    assert_eq!(
        runtime_map.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(1_001 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    let route = &runtime_map.pg_routes()[0];
    assert_eq!(route.pg_id(), PgId::new(25));
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(
        runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(1), NodeId::new(2)]
    );
    assert_eq!(runtime_map.nodes()[0].endpoint(), "node-1.sock");
    assert_eq!(runtime_map.nodes()[1].endpoint(), "node-2.sock");
    assert_eq!(
        runtime_map.nodes()[0].node_incarnation(),
        node_incarnation(&authority, 1)
    );
}

#[test]
fn pg_runtime_map_snapshot_uses_bounded_non_serving_validity() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();

    let runtime_map =
        ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(&authority, PgId::new(25), 1_234)
            .unwrap();

    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(1_234 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
    assert_eq!(runtime_map.pg_routes()[0].primary_lease_deadline_ms(), None);
}

#[test]
fn serving_pg_runtime_map_ignores_unrelated_unserved_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2)])
        .unwrap();
    authority
        .submit_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(2),
                node_incarnation: node_incarnation(&authority, 2),
                endpoint: "node-2.sock".to_owned(),
                observed_epoch: authority.snapshot().cluster_epoch(),
                requested_lease_duration_ms: 1_000,
                cluster_map_history_route_references: Default::default(),
                pg_observations: vec![NodePgHeartbeatObservation {
                    pg_id: PgId::new(26),
                    state: PgState::Peering,
                    metadata_proof: PgMetadataProof::empty(),
                    pending_metadata_command: None,
                }],
            },
            1_030,
        )
        .unwrap();
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            1_050,
        )
        .unwrap();

    assert!(authority.runtime_map_snapshot(1_051).is_err());
    let scoped = authority
        .serving_pg_runtime_map_snapshot(PgId::new(25), 1_051)
        .unwrap();

    assert_eq!(scoped.pg_routes().len(), 1);
    assert_eq!(scoped.pg_routes()[0].pg_id(), PgId::new(25));
    assert_eq!(scoped.pg_routes()[0].state(), PgState::Peering);
    assert!(scoped.freshness_proof().is_serving_authority_read());
    assert_eq!(
        scoped.valid_until_ms(),
        Some(1_051 + MAX_HEARTBEAT_LEASE_MS)
    );
}

#[test]
fn runtime_map_exports_retained_historical_pg_routes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 2_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(2_001).unwrap();
    assert_eq!(
        runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect::<Vec<_>>(),
        vec![NodeId::new(1), NodeId::new(2)]
    );
    let historical = runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(26), source_epoch)
        .unwrap();
    assert_eq!(historical.cluster_epoch(), source_epoch);
    assert_eq!(historical.pg_id(), PgId::new(26));
    assert_eq!(historical.primary_node_id(), NodeId::new(1));
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);
    assert_eq!(historical.primary_lease_deadline_ms(), None);

    let current = runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(26), runtime_map.cluster_epoch())
        .unwrap();
    assert_eq!(current.acting_set(), &[NodeId::new(2)]);
    assert_eq!(current.primary_lease_deadline_ms(), None);
}

#[test]
fn storage_node_refresh_filters_history_but_preserves_transfer_source_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(3)])
        .unwrap();
    let unrelated_history_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();

    let source_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        30,
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(30),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        30,
        PgState::Active,
        source_proof,
        false,
        2_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        PgMetadataProof {
            applied_log_index: 10,
            applied_log_hash: 11,
            state_digest: 12,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(30), vec![NodeId::new(2)], transfer)
        .unwrap();

    let full_runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    assert!(full_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(31), unrelated_history_epoch)
        .is_ok());
    assert!(full_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .is_ok());

    let filtered_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_003, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(filtered_runtime_map.historical_pg_routes().len(), 1);
    let source_route = filtered_runtime_map
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(source_route.primary_node_id(), NodeId::new(1));
    assert!(matches!(
        filtered_runtime_map
            .reconstructed_pg_route_at_epoch(PgId::new(31), unrelated_history_epoch,),
        Err(ControlPlaneError::UnknownClusterMapEpoch { .. })
    ));
}

#[test]
fn storage_node_refresh_filters_unrelated_history_around_exact_old_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(30),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for raw_pg_id in 100..120 {
        authority
            .set_pg_acting_set(PgId::new(raw_pg_id), vec![NodeId::new(2)])
            .unwrap();
    }
    for round in 0..20 {
        let node_id = if round % 2 == 0 {
            NodeId::new(3)
        } else {
            NodeId::new(2)
        };
        for raw_pg_id in 100..120 {
            authority
                .set_pg_acting_set(PgId::new(raw_pg_id), vec![node_id])
                .unwrap();
        }
    }

    let filtered_runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), ClusterEpoch::INITIAL)
        .unwrap();
    assert!(filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .all(|route| route.acting_set().contains(&NodeId::new(1))));
    assert!(filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| route.pg_id() == PgId::new(30) && route.cluster_epoch() == protected_epoch));
    assert!(!filtered_runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| (100..120).contains(&route.pg_id().get())));

    let mut encoded = Vec::new();
    write_runtime_map_snapshot(&mut encoded, &filtered_runtime_map).unwrap();
    assert!(
        encoded.len() < CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN / 8,
        "storage-node refresh retained too much unrelated route history: {} bytes",
        encoded.len()
    );
}

#[test]
fn storage_node_refresh_distributes_remote_exact_route_to_historical_actor() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(2)])
        .unwrap();
    let observed_epoch = authority.snapshot().cluster_epoch();

    let mut metadata_owner_heartbeat = heartbeat_from_record(&authority, 2, observed_epoch, 2_000);
    metadata_owner_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            source_epoch,
            PgId::new(30),
        )]);
    authority
        .heartbeat(metadata_owner_heartbeat, 2_000)
        .unwrap();

    let source_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_001, NodeId::new(1), observed_epoch)
        .unwrap();
    let historical = source_refresh
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);

    let unrelated_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_001, NodeId::new(3), observed_epoch)
        .unwrap();
    assert!(unrelated_refresh
        .historical_pg_routes()
        .iter()
        .all(|route| route.pg_id() != PgId::new(30) || route.cluster_epoch() != source_epoch));
}

#[test]
fn storage_node_refresh_history_uses_observed_epoch_for_running_node() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let protected_references = history_route_references([PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        protected_epoch,
        PgId::new(30),
    )]);
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for node_id in 10..18 {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let current_epoch = authority.snapshot().cluster_epoch();
    let observed_epoch = ClusterEpoch::new(current_epoch.get() - 2).unwrap();

    let bootstrap_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(bootstrap_refresh.historical_pg_routes().len(), 1);
    assert!(bootstrap_refresh
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));

    let running_refresh = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(1), observed_epoch)
        .unwrap();
    assert_eq!(running_refresh.historical_pg_routes().len(), 1);
    assert!(running_refresh
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));
}

#[test]
fn storage_node_restart_refresh_uses_control_plane_last_observed_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let protected_references = history_route_references([PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        protected_epoch,
        PgId::new(30),
    )]);
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_100);
    floor_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(floor_heartbeat, 1_100)
        .unwrap()
        .serving());

    for node_id in 10..18 {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let observed_epoch = authority.snapshot().cluster_epoch();
    let mut observed_heartbeat = heartbeat_from_record(&authority, 1, observed_epoch, 2_000);
    observed_heartbeat.cluster_map_history_route_references = protected_references.clone();
    assert!(authority
        .heartbeat(observed_heartbeat, 2_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .and_then(NodeControlRecord::last_observed_epoch),
        Some(observed_epoch)
    );

    let mut restart_heartbeat = heartbeat_from_record(&authority, 1, ClusterEpoch::INITIAL, 2_100);
    restart_heartbeat.cluster_map_history_route_references = protected_references;
    authority
        .heartbeat(restart_heartbeat.clone(), 2_100)
        .expect("lost restart heartbeat response should still apply");
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .and_then(NodeControlRecord::last_observed_epoch),
        Some(observed_epoch),
        "stale restart heartbeat must not regress stored observed epoch"
    );
    let refresh = authority
        .refresh_node_heartbeat(restart_heartbeat, 2_200)
        .unwrap();
    let (_lease, runtime_map) = refresh.into_parts();
    assert!(runtime_map
        .historical_pg_routes()
        .iter()
        .all(|route| route.cluster_epoch() >= observed_epoch
            || route.cluster_epoch() == protected_epoch));
    assert!(runtime_map
        .historical_pg_routes()
        .iter()
        .any(|route| route.cluster_epoch() == protected_epoch));
}

#[test]
fn storage_node_refresh_includes_refreshing_node_without_assigned_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();

    let runtime_map = authority
        .snapshot()
        .runtime_map_for_storage_node_refresh(2_000, NodeId::new(2), ClusterEpoch::INITIAL)
        .unwrap();

    assert!(runtime_map
        .nodes()
        .iter()
        .any(|node| node.node_id() == NodeId::new(2)));
    assert!(runtime_map
        .pg_routes()
        .iter()
        .all(|route| !route.acting_set().contains(&NodeId::new(2))));
}

#[test]
fn runtime_map_refresh_preserves_historical_pg_routes_for_storage_cluster() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 2_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(2)])
        .unwrap();
    let moved_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    let intervening_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 1 },
    )
    .unwrap();

    let historical = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), source_epoch)
        .unwrap();
    assert_eq!(historical.acting_set(), &[NodeId::new(1)]);
    let moved = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), moved_epoch)
        .unwrap();
    assert_eq!(moved.acting_set(), &[NodeId::new(2)]);
    let intervening = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), intervening_epoch)
        .unwrap();
    assert_eq!(intervening.acting_set(), &[NodeId::new(2)]);
    let current = cluster
        .reconstructed_pg_route_at_epoch(PgId::new(30), runtime_map.cluster_epoch())
        .unwrap();
    assert_eq!(current.acting_set(), &[NodeId::new(2)]);

    for epoch in runtime_map.historical_cluster_epochs() {
        for pg_id in [PgId::new(30), PgId::new(31)] {
            let expected = runtime_map.reconstructed_pg_route_at_epoch(pg_id, *epoch);
            let actual = cluster.reconstructed_pg_route_at_epoch(pg_id, *epoch);
            match expected {
                Ok(expected) => {
                    let actual = actual.expect("local map should retain the runtime-map route");
                    assert!(pg_route_configuration_eq(&actual, &expected));
                }
                Err(ControlPlaneError::UnknownPg { .. }) => assert!(actual.is_err()),
                Err(error) => panic!("runtime map rejected retained epoch {epoch}: {error}"),
            }
        }
    }
}

#[test]
fn runtime_map_valid_until_is_minimum_active_primary_lease_deadline() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(28), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_002);
    heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Peering, 3_000);
    authority
        .complete_pg_peering(
            PgId::new(28),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Active, 3_002);
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 3_003);

    let node_1_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let node_2_deadline = authority
        .snapshot()
        .node(NodeId::new(2))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    assert!(node_2_deadline < node_1_deadline);

    let runtime_map = authority.snapshot().runtime_map(3_004).unwrap();
    assert_eq!(runtime_map.valid_until_ms(), Some(node_2_deadline));
    assert_eq!(
        runtime_map
            .pg_routes()
            .iter()
            .filter(|route| route.state() == PgState::Active)
            .map(PgRouteSnapshot::primary_lease_deadline_ms)
            .collect::<Vec<_>>(),
        vec![Some(node_1_deadline), Some(node_2_deadline)]
    );
}

#[test]
fn runtime_map_content_certificate_reuses_static_content_with_fresh_lease_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(127), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(127),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Active, 2_002);

    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let certificate = RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
        authority.snapshot(),
        &runtime_map,
    );
    let mut renewed_snapshot = authority.snapshot().clone();
    renewed_snapshot
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .lease_deadline_ms = Some(runtime_map.valid_until_ms().unwrap() + 1_000);
    let renewed_runtime_map = renewed_snapshot.runtime_map(2_003).unwrap();
    assert_eq!(
        renewed_runtime_map.content_digest(),
        runtime_map.content_digest()
    );

    let cached_status = ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
        &renewed_snapshot,
        2_003,
        *renewed_runtime_map.freshness_proof(),
        certificate,
    )
    .unwrap()
    .expect("lease-only changes should preserve certificate reuse");
    assert_eq!(
        cached_status,
        ControlPlaneRuntimeMapStatus::from_runtime_map(&renewed_runtime_map)
    );
    assert_eq!(
        cached_status.lease_renewal().unwrap().validity(),
        renewed_runtime_map.validity()
    );
}

#[test]
fn runtime_map_content_certificate_rejects_same_epoch_route_change() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(127), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(127),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 127, PgState::Active, 2_002);

    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let certificate = RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
        authority.snapshot(),
        &runtime_map,
    );
    let mut changed_snapshot = authority.snapshot().clone();
    let changed_pg = changed_snapshot.pgs.get_mut(&PgId::new(127)).unwrap();
    changed_pg.state = PgState::Peering;
    changed_pg.active_primary = None;
    let changed_runtime_map = changed_snapshot.runtime_map(2_003).unwrap();
    assert_eq!(
        changed_runtime_map.cluster_epoch(),
        runtime_map.cluster_epoch()
    );
    assert_eq!(
        changed_runtime_map.pg_routes().len(),
        runtime_map.pg_routes().len()
    );
    assert_ne!(
        changed_runtime_map.content_digest(),
        runtime_map.content_digest()
    );

    assert!(
        ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
            &changed_snapshot,
            2_003,
            *changed_runtime_map.freshness_proof(),
            certificate,
        )
        .unwrap()
        .is_none(),
        "same-epoch route changes must force a full runtime-map refresh"
    );

    *authority.runtime_map_content_certificate.lock().unwrap() = Some(certificate);
    authority.snapshot = changed_snapshot;
    let rebuilt_status = authority.runtime_map_status(2_003).unwrap();
    assert_eq!(
        rebuilt_status
            .lease_renewal()
            .expect("single-authority status should retain a bounded validity proof")
            .content_digest(),
        changed_runtime_map.content_digest(),
        "the status source must rebuild rather than renew stale same-epoch content"
    );
}

#[test]
fn single_authority_runtime_map_content_certificate_invalidates_on_commit() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();

    let initial_status = authority.runtime_map_status(1_000).unwrap();
    let initial_certificate = authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .expect("status should cache the content certificate");
    assert_eq!(initial_status.cluster_epoch(), ClusterEpoch::INITIAL);

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .is_none());
    let updated_status = authority.runtime_map_status(1_001).unwrap();
    let updated_certificate = authority
        .runtime_map_content_certificate
        .lock()
        .unwrap()
        .expect("status should rebuild the invalidated certificate");
    assert_ne!(
        updated_status.cluster_epoch(),
        initial_status.cluster_epoch()
    );
    assert_ne!(updated_certificate, initial_certificate);
}

#[test]
fn runtime_map_builds_frontend_topology_with_validity_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(29), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 1, 29, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(29),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Active, 2_003);
    let runtime_map = authority.snapshot().runtime_map(2_004).unwrap();
    let valid_until_ms = runtime_map
        .valid_until_ms()
        .expect("active runtime map should have a validity deadline");

    let local_map = crate::cluster::LocalClusterMap::open_frontend_topology_only_with_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 1 },
    )
    .unwrap();

    assert_eq!(local_map.epoch(), runtime_map.cluster_epoch());
    assert_eq!(local_map.route_map_valid_until_ms(), Some(valid_until_ms));
    assert!(local_map.is_route_map_valid_at(valid_until_ms - 1));
    assert!(!local_map.is_route_map_valid_at(valid_until_ms));
    let route = local_map.pg_route(PgId::new(29)).unwrap();
    assert_eq!(route.primary_node_id(), NodeId::new(2));
    assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
    assert_eq!(route.state(), PgState::Active);
}

#[test]
fn runtime_map_builds_storage_cluster_with_validity_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);
    let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
    let valid_until_ms = runtime_map.valid_until_ms().unwrap();

    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();

    assert_eq!(cluster.cluster_epoch(), runtime_map.cluster_epoch());
    assert_eq!(cluster.operation_epoch(), runtime_map.cluster_epoch());
    assert_eq!(cluster.route_map_valid_until_ms(), Some(valid_until_ms));
    cluster
        .require_route_map_valid_at(valid_until_ms - 1)
        .unwrap();
    assert!(matches!(
        cluster.require_route_map_valid_at(valid_until_ms),
        Err(crate::StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: expired_at,
            now_ms,
        }) if cluster_epoch == runtime_map.cluster_epoch()
            && expired_at == valid_until_ms
            && now_ms == valid_until_ms
    ));
    let route = cluster.local_pg_route(PgId::new(31)).unwrap();
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.state(), PgState::Active);
}

#[test]
fn storage_cluster_refreshes_from_control_plane_runtime_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    assert_eq!(
        cluster.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        cluster.route_map_valid_until_ms(),
        Some(2_001 + MAX_HEARTBEAT_LEASE_MS)
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(
        refreshed.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        refreshed.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Active
    );
    assert_eq!(
        refreshed.route_map_valid_until_ms(),
        authority
            .snapshot()
            .runtime_map(2_004)
            .unwrap()
            .valid_until_ms()
    );
    assert!(refreshed.route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_refresh_preserves_process_local_reclaim_queue() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("refresh-queue-bucket").unwrap();
    let root = crate::BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 1,
    };
    cluster.enqueue_bucket_delete_finalize(root.clone());
    assert_eq!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(root.clone()))
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    let recreated_root = crate::BucketDeleteFinalizeRoot {
        bucket,
        bucket_incarnation_generation: 2,
    };
    refreshed.enqueue_bucket_delete_finalize(recreated_root.clone());
    cluster.finish_bucket_delete_finalize_work(&root);
    assert_eq!(
        refreshed.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(recreated_root.clone()))
    );
    refreshed.finish_bucket_delete_finalize_work(&recreated_root);
    assert!(refreshed.try_take_reclaim_work().is_none());
}

#[test]
fn storage_cluster_refresh_preserves_process_local_shard_repair_queue() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let repair = placed_segment_shard_repair_work_item_for_runtime_refresh(1);
    assert!(cluster.test_enqueue_placed_segment_shard_repair(repair));

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(
        refreshed.try_take_placed_segment_shard_repair_work(),
        Some(repair)
    );
    assert!(refreshed
        .try_take_placed_segment_shard_repair_work()
        .is_none());
}

#[test]
fn storage_cluster_refresh_preserves_metadata_runtime_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let pg_id = PgId::new(31);
    let lock_ptr = cluster.test_metadata_command_pg_lock_ptr(pg_id);
    let command = metadata_command_for_runtime_refresh(3);
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 0);
    let guard = cluster.test_begin_metadata_command_recovery_leader(pg_id, &command);
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 1);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(refreshed.test_metadata_command_pg_lock_ptr(pg_id), lock_ptr);
    assert_eq!(refreshed.test_metadata_command_recovery_flight_count(), 1);
    drop(guard);
    assert_eq!(refreshed.test_metadata_command_recovery_flight_count(), 0);
}

#[test]
fn storage_cluster_unix_refresh_preserves_process_local_runtime_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let registry_key = cluster.process_local_registry_key();
    let bucket = crate::BucketName::try_from("unix-refresh-queue-bucket").unwrap();
    let root = crate::BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 1,
    };
    cluster.enqueue_bucket_delete_finalize(root.clone());
    let repair = placed_segment_shard_repair_work_item_for_runtime_refresh(2);
    assert!(cluster.test_enqueue_placed_segment_shard_repair(repair));

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = cluster
        .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
            &authority,
            2_004,
            crate::cluster::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        )
        .unwrap();
    assert_eq!(refreshed.process_local_registry_key(), registry_key);
    assert_eq!(
        refreshed.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(root.clone()))
    );
    refreshed.finish_bucket_delete_finalize_work(&root);
    assert_eq!(
        refreshed.try_take_placed_segment_shard_repair_work(),
        Some(repair)
    );
    assert!(refreshed.try_take_reclaim_work().is_none());
    assert!(refreshed
        .try_take_placed_segment_shard_repair_work()
        .is_none());
}

#[test]
fn storage_cluster_route_handle_refresh_installs_current_map() {
    let _clock = crate::clock::test_time_override_guard(1_050);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(31))
            .unwrap()
            .state(),
        PgState::Peering
    );

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

    let refreshed = handle
        .refresh_from_control_plane_runtime_map(&authority, 2_004)
        .unwrap();
    assert_eq!(Arc::as_ptr(&handle.current()), Arc::as_ptr(&refreshed));
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(31))
            .unwrap()
            .state(),
        PgState::Active
    );
    assert!(handle.current().route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_route_handle_rejects_epoch_downgrade() {
    let _clock = crate::clock::test_time_override_guard(2_001);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    let older_map = authority.snapshot().runtime_map(2_001).unwrap();
    let older_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &older_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&older_cluster));

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 2, 3_000).serving());
    let newer_map = authority.snapshot().runtime_map(3_001).unwrap();
    let newer_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &newer_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    assert!(newer_cluster.cluster_epoch() > older_cluster.cluster_epoch());
    handle.install(Arc::clone(&newer_cluster)).unwrap();

    assert!(matches!(
        handle.install(older_cluster),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::EpochDowngrade {
            current,
            candidate,
        }) if current == newer_cluster.cluster_epoch() && candidate == older_map.cluster_epoch()
    ));
    assert_eq!(
        handle.current().cluster_epoch(),
        newer_cluster.cluster_epoch()
    );
}

#[test]
fn storage_cluster_constructor_rejects_same_epoch_unbounded_dynamic_authority() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);

    let current_map = authority.snapshot().runtime_map(2_003).unwrap();
    let current_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &current_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let mut unbounded_map = current_map.clone();
    unbounded_map.validity = RouteMapValidity::Forever;
    assert!(matches!(
        crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &unbounded_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::DynamicRouteAuthorityUnboundedValidity { epoch })
            if epoch == current_map.cluster_epoch()
    ));
    assert!(current_cluster.route_map_valid_until_ms().is_some());
}

#[test]
fn storage_cluster_constructor_rejects_later_epoch_unbounded_dynamic_authority() {
    let current_map = runtime_map_test_snapshot_with_active_route();
    let mut unbounded_map = current_map.clone();
    unbounded_map.cluster_epoch = ClusterEpoch::new(current_map.cluster_epoch().get() + 1)
        .expect("test epoch should not overflow");
    unbounded_map.validity = RouteMapValidity::Forever;
    for route in &mut unbounded_map.pg_routes {
        route.cluster_epoch = unbounded_map.cluster_epoch;
    }
    let expected_epoch = ClusterEpoch::new(current_map.cluster_epoch().get() + 1).unwrap();
    assert!(matches!(
        crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &unbounded_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::DynamicRouteAuthorityUnboundedValidity { epoch })
            if epoch == expected_epoch
    ));
}

#[test]
fn storage_cluster_route_handle_accepts_same_epoch_shorter_bounded_validity() {
    crate::clock::with_time_override(12_000, || {
        let mut current_map = runtime_map_test_snapshot_with_active_route();
        current_map.validity = RouteMapValidity::until_ms(14_000).unwrap();
        current_map.pg_routes[0].primary_lease_deadline_ms = Some(14_000);
        let current_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &current_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(current_cluster);
        let mut shorter_map = current_map.clone();
        shorter_map.validity = RouteMapValidity::until_ms(13_500).unwrap();
        let shorter_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &shorter_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();

        handle.install(shorter_cluster).unwrap();
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(13_500));
    });
}

#[test]
fn storage_cluster_route_handle_extends_all_pinned_same_epoch_validity() {
    let _clock = crate::clock::test_time_override_guard(500);
    let route = PgRouteSnapshot::reconstructed(
        ClusterEpoch::INITIAL,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(1_000).unwrap(),
        )
        .unwrap();
    let pinned_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(local_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    pinned_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(1_000).unwrap());
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned_cluster));
    let candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(2_000).unwrap(),
        )
        .unwrap();
    let candidate_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(candidate_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    candidate_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(2_000).unwrap());

    handle.install(Arc::clone(&candidate_cluster)).unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(2_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(2_000));
    assert_eq!(
        Arc::as_ptr(&handle.current()),
        Arc::as_ptr(&candidate_cluster)
    );

    let second_candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(3_000).unwrap(),
        )
        .unwrap();
    let second_candidate_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(second_candidate_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    second_candidate_cluster
        .test_store_route_map_validity(RouteMapValidity::until_ms(3_000).unwrap());

    handle
        .install(Arc::clone(&second_candidate_cluster))
        .unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(3_000));
    assert_eq!(candidate_cluster.route_map_valid_until_ms(), Some(3_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(3_000));
    assert_eq!(
        Arc::as_ptr(&handle.current()),
        Arc::as_ptr(&second_candidate_cluster)
    );
}

#[test]
fn storage_cluster_route_handle_does_not_extend_pinned_previous_epoch_validity() {
    let _clock = crate::clock::test_time_override_guard(500);
    let initial_route = PgRouteSnapshot::reconstructed(
        ClusterEpoch::INITIAL,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [crate::cluster::LocalPgRoute::from(&initial_route)],
            RouteMapValidity::until_ms(1_000).unwrap(),
        )
        .unwrap();
    let pinned_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(local_map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    pinned_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(1_000).unwrap());
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned_cluster));
    let next_epoch = ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 1).unwrap();
    let next_route = PgRouteSnapshot::reconstructed(
        next_epoch,
        PgId::new(31),
        NodeId::new(1),
        vec![NodeId::new(1)],
        PgState::Active,
    );
    let candidate_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            next_epoch,
            [crate::cluster::LocalPgRoute::from(&next_route)],
            RouteMapValidity::until_ms(3_000).unwrap(),
        )
        .unwrap();
    let candidate_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::new(candidate_map), next_epoch)
            .unwrap();
    candidate_cluster.test_store_route_map_validity(RouteMapValidity::until_ms(3_000).unwrap());

    handle.install(candidate_cluster).unwrap();

    assert_eq!(pinned_cluster.route_map_valid_until_ms(), Some(1_000));
    assert_eq!(handle.current().route_map_valid_until_ms(), Some(3_000));
}

#[test]
fn storage_cluster_route_handle_rejects_static_to_dynamic_same_epoch() {
    let _clock = crate::clock::test_time_override_guard(1_050);
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);
    let active_map = authority.snapshot().runtime_map(2_004).unwrap();
    let active_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &active_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let unbounded_local_map =
        crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            crate::EcShape { k: 1, m: 0 },
            active_map.cluster_epoch(),
            active_map
                .pg_routes()
                .iter()
                .map(crate::cluster::LocalPgRoute::from),
        )
        .unwrap();
    let unbounded_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::new(unbounded_local_map),
        active_map.cluster_epoch(),
    )
    .unwrap();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&unbounded_cluster));
    assert_eq!(
        active_cluster.cluster_epoch(),
        unbounded_cluster.cluster_epoch()
    );
    assert_eq!(unbounded_cluster.route_map_valid_until_ms(), None);
    assert!(active_cluster.route_map_valid_until_ms().is_some());

    assert!(matches!(
        handle.install(active_cluster),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh)
    ));
    assert!(Arc::ptr_eq(&handle.current(), &unbounded_cluster));
    assert_eq!(unbounded_cluster.route_map_valid_until_ms(), None);
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_installs_current_map() {
    let base_now_ms = crate::clock::current_time_millis();
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, base_now_ms).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        31,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        (base_now_ms + 2, 10_000),
    );

    let peering_map = authority.snapshot().runtime_map(base_now_ms + 3).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &peering_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);

    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            base_now_ms + 4,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        31,
        PgState::Active,
        PgMetadataProof::empty(),
        false,
        (base_now_ms + 5, 10_000),
    );
    let expected_runtime_map = authority.snapshot().runtime_map(base_now_ms + 6).unwrap();
    let now = Arc::new(AtomicU64::new(base_now_ms + 6));
    let loop_now = Arc::clone(&now);
    let mut refresh_loop = handle
        .clone()
        .spawn_control_plane_refresh_loop(authority, Duration::from_millis(5), move || {
            loop_now.fetch_add(1, Ordering::SeqCst)
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if refresh_loop.status().successes > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "frontend runtime-map refresh loop did not install a map: {:?}",
            refresh_loop.status()
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    let current = handle.current();
    assert_eq!(
        current.cluster_epoch(),
        expected_runtime_map.cluster_epoch()
    );
    assert_eq!(
        current.local_pg_route(PgId::new(31)).unwrap().state(),
        PgState::Active
    );
    assert_eq!(
        current.route_map_valid_until_ms(),
        expected_runtime_map.valid_until_ms()
    );
    assert_eq!(refresh_loop.status().failures, 0);
    assert_eq!(
        refresh_loop.status().last_success,
        Some(crate::StorageClusterRuntimeMapRefreshLoopSuccess {
            cluster_epoch: expected_runtime_map.cluster_epoch(),
            route_map_validity: expected_runtime_map.validity(),
        })
    );

    refresh_loop.stop();
    let attempts_after_stop = refresh_loop.status().attempts;
    std::thread::sleep(Duration::from_millis(15));
    assert_eq!(refresh_loop.status().attempts, attempts_after_stop);
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_resamples_time_after_discovery() {
    struct TimestampRecordingRuntimeMapSource {
        snapshot: ClusterControlSnapshot,
        discovery_now_ms: Arc<AtomicU64>,
        recovery_now_ms: Arc<Mutex<Vec<u64>>>,
        refresh_now_ms: Arc<AtomicU64>,
    }

    impl ControlPlaneRuntimeMapSource for TimestampRecordingRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            let _ = self.refresh_now_ms.compare_exchange(
                u64::MAX,
                authority_now_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.snapshot.runtime_map(authority_now_ms)
        }

        fn pending_metadata_command_recoveries(
            &self,
            authority_now_ms: u64,
        ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
            let _ = self.discovery_now_ms.compare_exchange(
                u64::MAX,
                authority_now_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            Ok(PendingMetadataCommandRecoveryListing::new(
                [31, 32]
                    .into_iter()
                    .map(|pg_id| {
                        PendingMetadataCommandRecoveryTask::new(
                            PgId::new(pg_id),
                            PendingMetadataCommandRecovery::new(
                                NodeId::new(1),
                                PendingMetadataCommandObservation::new(
                                    ClusterEpoch::INITIAL,
                                    NonZeroU64::MIN,
                                    u64::from(pg_id),
                                ),
                            ),
                        )
                    })
                    .collect(),
                Vec::new(),
            ))
        }

        fn pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.recovery_now_ms
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(authority_now_ms);
            Err(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.snapshot
                .serving_runtime_map_for_pg_with_freshness_proof(
                    pg_id,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                )
        }
    }

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
    let initial_map = authority.snapshot().runtime_map(2_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &initial_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let discovery_now_ms = Arc::new(AtomicU64::new(u64::MAX));
    let recovery_now_ms = Arc::new(Mutex::new(Vec::new()));
    let refresh_now_ms = Arc::new(AtomicU64::new(u64::MAX));
    let source = TimestampRecordingRuntimeMapSource {
        snapshot: authority.snapshot().clone(),
        discovery_now_ms: Arc::clone(&discovery_now_ms),
        recovery_now_ms: Arc::clone(&recovery_now_ms),
        refresh_now_ms: Arc::clone(&refresh_now_ms),
    };
    let clock = Arc::new(AtomicU64::new(2_001));
    let loop_clock = Arc::clone(&clock);
    let mut refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_secs(1), move || {
            loop_clock.fetch_add(6_000, Ordering::SeqCst)
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while refresh_loop.status().successes == 0 {
        assert!(
            Instant::now() < deadline,
            "refresh loop did not complete: {:?}",
            refresh_loop.status()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    refresh_loop.stop();

    assert_eq!(discovery_now_ms.load(Ordering::SeqCst), 2_001);
    assert_eq!(
        *recovery_now_ms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        vec![8_001, 14_001]
    );
    assert_eq!(refresh_now_ms.load(Ordering::SeqCst), 20_001);
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_retains_transient_failure_classification() {
    struct FailOnceRuntimeMapSource {
        snapshot: ClusterControlSnapshot,
        failures_remaining: AtomicU64,
    }

    impl ControlPlaneRuntimeMapSource for FailOnceRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            if self
                .failures_remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ControlPlaneError::io(
                    "sentinel runtime-map read",
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "sentinel-bucket/sentinel-object/sentinel-upload-id",
                    ),
                ));
            }
            self.snapshot.runtime_map(authority_now_ms)
        }

        fn pending_metadata_command_recoveries(
            &self,
            _authority_now_ms: u64,
        ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
            Ok(PendingMetadataCommandRecoveryListing::new(
                Vec::new(),
                Vec::new(),
            ))
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.snapshot
                .serving_runtime_map_for_pg_with_freshness_proof(
                    pg_id,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                )
        }
    }

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 1_001);
    let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);
    let source = FailOnceRuntimeMapSource {
        snapshot: authority.snapshot().clone(),
        failures_remaining: AtomicU64::new(1),
    };
    let mut refresh_loop = handle
        .spawn_control_plane_refresh_loop(source, Duration::from_millis(5), || 1_002)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let status = refresh_loop.status();
        if status.failures == 1 && status.successes > 0 {
            assert_eq!(status.last_error, None);
            assert_eq!(
                status.last_failure,
                Some(crate::StorageClusterRuntimeMapRefreshLoopFailure {
                    attempt: 1,
                    kind: "control_plane_io_timeout",
                })
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "refresh loop did not fail then recover: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    refresh_loop.stop();
}

#[test]
fn storage_cluster_runtime_map_refresh_loop_rejects_zero_interval() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 1_001);
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(cluster);

    assert!(matches!(
        handle.spawn_control_plane_refresh_loop(authority, Duration::ZERO, || 1_000),
        Err(crate::cluster::StorageClusterRuntimeMapRefreshError::RefreshLoopZeroInterval)
    ));
}

#[test]
fn runtime_node_routes_build_unix_storage_client_configs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let node = &runtime_map.nodes()[0];

    let default_config =
        crate::cluster::LocalUnixStorageNodeClientConfig::from_runtime_node_route(node);
    assert_eq!(default_config.node_id(), NodeId::new(1));
    assert_eq!(
        default_config.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(
        default_config.rpc_admission_limit(),
        crate::cluster::LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT
    );

    let configured =
        crate::cluster::LocalUnixStorageNodeClientConfig::with_rpc_admission_settings_from_runtime_node_route(
            node,
            crate::cluster::LocalUnixStorageNodeClientAdmissionSettings {
                rpc_admission_limit: 17,
                rpc_admission_wait_timeout: std::time::Duration::from_millis(200),
                rpc_control_admission_wait_timeout: std::time::Duration::from_millis(300),
            },
        );
    assert_eq!(configured.node_id(), NodeId::new(1));
    assert_eq!(
        configured.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(configured.rpc_admission_limit(), 17);
    assert_eq!(
        configured.rpc_admission_wait_timeout(),
        std::time::Duration::from_millis(200)
    );
    assert_eq!(
        configured.rpc_control_admission_wait_timeout(),
        std::time::Duration::from_millis(300)
    );

    let settings = crate::cluster::LocalUnixStorageNodeClientAdmissionSettings {
        rpc_admission_limit: 23,
        rpc_admission_wait_timeout: std::time::Duration::from_millis(400),
        rpc_control_admission_wait_timeout: std::time::Duration::from_millis(500),
    };
    let [ref refreshed_config] =
        crate::StorageCluster::unix_storage_node_client_configs_from_runtime_map(
            &runtime_map,
            settings,
        )
        .try_into()
        .unwrap();
    assert_eq!(refreshed_config.node_id(), NodeId::new(1));
    assert_eq!(
        refreshed_config.socket_path(),
        Some(std::path::Path::new("node-1.sock"))
    );
    assert_eq!(refreshed_config.rpc_admission_limit(), 23);
    assert_eq!(
        refreshed_config.rpc_admission_wait_timeout(),
        std::time::Duration::from_millis(400)
    );
    assert_eq!(
        refreshed_config.rpc_control_admission_wait_timeout(),
        std::time::Duration::from_millis(500)
    );
}

#[test]
fn runtime_map_installs_unix_storage_clients_from_absolute_endpoints() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let endpoint = tmp
        .path()
        .join("node-1.sock")
        .to_string_lossy()
        .into_owned();
    assert!(heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving());
    authority
        .set_pg_acting_set(PgId::new(32), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    let cluster = crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
        NodeId::new(1),
        &runtime_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();

    assert_eq!(cluster.local_node_count(), 1);
    assert_eq!(
        cluster.local_node_ids().collect::<Vec<_>>(),
        vec![NodeId::new(1)]
    );
    assert_eq!(
        cluster.local_pg_route(PgId::new(32)).unwrap().state(),
        PgState::Peering
    );
}

#[test]
fn runtime_map_builds_storage_node_process_config_for_node_routes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(34), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(35), vec![NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Peering, 1_001);
    heartbeat_with_pg_observation(&mut authority, 2, 34, PgState::Peering, 1_002);
    authority
        .complete_pg_peering(
            PgId::new(34),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_003,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Active, 1_004);
    let runtime_map = authority.snapshot().runtime_map(1_005).unwrap();
    let valid_until_ms = runtime_map.valid_until_ms().unwrap();

    let node_1_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
        NodeId::new(1),
        tmp.path().join("node-1"),
        crate::EcShape { k: 1, m: 0 },
        &runtime_map,
    )
    .unwrap();
    assert_eq!(node_1_config.node_id, NodeId::new(1));
    assert_eq!(node_1_config.cluster_epoch, runtime_map.cluster_epoch());
    assert_eq!(
        node_1_config.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert_eq!(
        node_1_config.socket_path,
        std::path::PathBuf::from("node-1.sock")
    );
    assert_eq!(node_1_config.pg_ids, vec![34, 35]);
    assert_eq!(node_1_config.pg_routes.len(), 2);
    assert_eq!(node_1_config.pg_routes[0].pg_id, 34);
    assert_eq!(node_1_config.pg_routes[0].state, PgState::Active);
    assert_eq!(
        node_1_config.pg_routes[0].acting_set,
        vec![NodeId::new(1), NodeId::new(2)]
    );
    assert_eq!(node_1_config.pg_routes[1].pg_id, 35);
    assert_eq!(node_1_config.pg_routes[1].state, PgState::Peering);
    assert_eq!(node_1_config.pg_routes[1].acting_set, vec![NodeId::new(2)]);

    let node_2_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
        NodeId::new(2),
        tmp.path().join("node-2"),
        crate::EcShape { k: 1, m: 0 },
        &runtime_map,
    )
    .unwrap();
    assert_eq!(node_2_config.pg_ids, vec![34, 35]);
    assert_eq!(
        node_2_config.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert_eq!(
        node_2_config
            .pg_routes
            .iter()
            .map(|route| route.pg_id)
            .collect::<Vec<_>>(),
        vec![34, 35]
    );
}

#[test]
fn runtime_map_storage_node_config_rejects_absent_node() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(36), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    assert!(matches!(
        crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
            NodeId::new(2),
            tmp.path().join("node-2"),
            crate::EcShape { k: 1, m: 0 },
            &runtime_map,
        ),
        Err(crate::storage_node_server::StorageNodeServerError::RuntimeMapNodeNotFound {
            node_id: 2,
            cluster_epoch,
        }) if cluster_epoch == runtime_map.cluster_epoch()
    ));
}

#[test]
fn runtime_map_unix_storage_clients_reject_relative_endpoints() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(33), vec![NodeId::new(1)])
        .unwrap();
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

    assert!(matches!(
        crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
            NodeId::new(1),
            &runtime_map,
            crate::EcShape { k: 1, m: 0 },
        ),
        Err(crate::ClusterBuildError::RemoteStorageNodeClientSocketPathNotAbsolute { path })
            if path.as_path() == std::path::Path::new("node-1.sock")
    ));
}

#[test]
fn runtime_map_requires_endpoints_for_routed_nodes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();

    assert!(matches!(
        authority.snapshot().runtime_map(1_000),
        Err(ControlPlaneError::NodeEndpointMissing { node_id: 1, .. })
    ));
}

#[test]
fn active_pg_route_fails_closed_after_primary_lease_expiry() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
    let lease_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();

    assert!(matches!(
        authority
            .snapshot()
            .active_pg_route(PgId::new(23), lease_deadline),
        Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 23, .. })
    ));
    let reconstructed = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(23), authority.snapshot().cluster_epoch())
        .unwrap();
    assert_eq!(reconstructed.state(), PgState::Active);
    assert_eq!(reconstructed.primary_node_id(), NodeId::new(1));
    assert_eq!(reconstructed.acting_set(), &[NodeId::new(1)]);
    assert_eq!(reconstructed.primary_lease_deadline_ms(), None);
}

#[test]
fn complete_pg_peering_requires_unexpired_primary_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(11), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, 1_050);
    let lease_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(11),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));
    assert_eq!(
        authority.snapshot().pg(PgId::new(11)).unwrap().state(),
        PgState::Peering
    );

    heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, lease_deadline + 1);
    authority
        .complete_pg_peering(
            PgId::new(11),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline + 2,
        )
        .unwrap();
}

#[test]
fn complete_pg_peering_requires_current_peering_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(12), vec![NodeId::new(1)])
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 12,
            node_id: 1,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Active, 2_020);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_030,
        ),
        Err(ControlPlaneError::PgPeeringObservationNotPeering {
            pg_id: 12,
            node_id: 1,
            state: PgState::Active,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Peering, 2_040);
    authority
        .complete_pg_peering(
            PgId::new(12),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
}

#[test]
fn complete_pg_peering_only_activates_from_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_state(PgId::new(15), PgState::Degraded)
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(15),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgNotPeering {
            pg_id: 15,
            state: PgState::Degraded,
            ..
        })
    ));
}

#[test]
fn active_pg_service_requires_primary_active_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_010), None);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020),
            2_020,
        )
        .unwrap();
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_021,
        ),
        Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 16,
            node_id: 1,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_030);
    assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_030), None);
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_040,
        ),
        Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 16,
            node_id: 1,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Active, 2_050);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(16), 2_050),
        Some(NodeId::new(1))
    );
    authority
        .authorize_pg_primary_service(
            PgId::new(16),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            2_060,
        )
        .unwrap();
}

#[test]
fn node_service_authorization_requires_current_epoch_incarnation_and_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let record = authority.snapshot().node(NodeId::new(1)).unwrap();
    let node_incarnation = record.node_incarnation();
    let lease_deadline_ms = record.lease_deadline_ms().unwrap();

    let authorized = authority
        .authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            serving.cluster_epoch(),
            150,
        )
        .unwrap();
    assert_eq!(authorized.node_id(), NodeId::new(1));
    assert_eq!(authorized.node_incarnation(), node_incarnation);
    assert_eq!(authorized.cluster_epoch(), serving.cluster_epoch());
    assert_eq!(
        authorized.authority_incarnation(),
        authority.snapshot().authority_incarnation()
    );
    assert_eq!(authorized.lease_deadline_ms(), lease_deadline_ms);
    authority
        .validate_node_service_authorization(&authorized, 151)
        .unwrap();

    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation + 1,
            serving.cluster_epoch(),
            150,
        ),
        Err(ControlPlaneError::NodeIncarnationMismatch { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            ClusterEpoch::INITIAL,
            150,
        ),
        Err(ControlPlaneError::StaleNodeObservedEpoch { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.authorize_node_service(
            NodeId::new(1),
            node_incarnation,
            serving.cluster_epoch(),
            lease_deadline_ms,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));
    assert!(matches!(
        authority.validate_node_service_authorization(&authorized, lease_deadline_ms),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let mut shorter_lease =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 152);
    shorter_lease.requested_lease_duration_ms = 5;
    let refreshed = authority.heartbeat(shorter_lease, 152).unwrap();
    assert_eq!(
        refreshed.lease_deadline_ms(),
        authorized.lease_deadline_ms()
    );
    assert!(authorized.lease_deadline_ms() > 157);
    authority
        .validate_node_service_authorization(&authorized, 157)
        .unwrap();
}

#[test]
fn node_service_authorization_cannot_validate_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let active_epoch = serving.cluster_epoch();
    let node_incarnation = node_incarnation(&authority, 1);
    let authorization = authority
        .authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 150)
        .unwrap();
    authority
        .validate_node_service_authorization(&authorization, 151)
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(37), vec![NodeId::new(1)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > active_epoch);

    assert!(matches!(
        authority.validate_node_service_authorization(&authorization, 152),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == peering_epoch
    ));
    assert!(matches!(
        authority.authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 153),
        Err(ControlPlaneError::StaleNodeObservedEpoch {
            node_id: 1,
            observed_epoch,
            current_epoch,
        }) if observed_epoch == active_epoch && current_epoch == peering_epoch
    ));

    let current = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, peering_epoch, 154),
            154,
        )
        .unwrap();
    assert!(current.serving());
    authority
        .authorize_node_service(NodeId::new(1), node_incarnation, peering_epoch, 155)
        .unwrap();
}

#[test]
fn node_service_authorization_cannot_validate_after_authority_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 1, 100);
    assert!(serving.serving());
    let active_epoch = serving.cluster_epoch();
    let active_authority_incarnation = serving.authority_incarnation();
    let node_incarnation = node_incarnation(&authority, 1);
    let authorization = authority
        .authorize_node_service(NodeId::new(1), node_incarnation, active_epoch, 150)
        .unwrap();
    authority
        .validate_node_service_authorization(&authorization, 151)
        .unwrap();

    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().cluster_epoch() > active_epoch);
    assert!(restarted.snapshot().authority_incarnation() > active_authority_incarnation);
    assert!(matches!(
        restarted.validate_node_service_authorization(&authorization, 152),
        Err(ControlPlaneError::StaleAuthorityIncarnation {
            authority_incarnation,
            current_authority_incarnation,
        }) if authority_incarnation == active_authority_incarnation
            && current_authority_incarnation == restarted.snapshot().authority_incarnation()
    ));

    let restart_epoch = restarted.snapshot().cluster_epoch();
    let current = restarted
        .heartbeat(
            heartbeat_from_record(&restarted, 1, restart_epoch, 153),
            153,
        )
        .unwrap();
    assert!(current.serving());
    restarted
        .authorize_node_service(NodeId::new(1), node_incarnation, restart_epoch, 154)
        .unwrap();
}

#[test]
fn pg_primary_authorization_fails_closed_for_peering_and_wrong_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(10), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            10,
            PgState::Peering,
            2_000 + u64::from(node_id),
        );
    }
    let node_one_incarnation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .node_incarnation();
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(1),
            node_one_incarnation,
            authority.snapshot().cluster_epoch(),
            2_050,
        ),
        Err(ControlPlaneError::PgNotActive { pg_id: 10, .. })
    ));

    authority
        .complete_pg_peering(
            PgId::new(10),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 10, PgState::Active, 3_001);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
            3_002,
        )
        .unwrap();
    let node_one_incarnation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .node_incarnation();
    let node_two_incarnation = authority
        .snapshot()
        .node(NodeId::new(2))
        .unwrap()
        .node_incarnation();
    let authorized = authority
        .authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(1),
            node_one_incarnation,
            authority.snapshot().cluster_epoch(),
            3_050,
        )
        .unwrap();
    assert_eq!(authorized.pg_id(), PgId::new(10));
    assert_eq!(authorized.primary_node_id(), NodeId::new(1));
    assert_eq!(
        authorized.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(
        authorized.authority_incarnation(),
        authority.snapshot().authority_incarnation()
    );
    assert_eq!(
        authorized.lease_deadline_ms(),
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap()
    );

    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(10),
            NodeId::new(2),
            node_two_incarnation,
            authority.snapshot().cluster_epoch(),
            3_050,
        ),
        Err(ControlPlaneError::NodeNotPgPrimary {
            pg_id: 10,
            node_id: 2,
            primary_node_id: 1,
            ..
        })
    ));
}

#[test]
fn pg_operation_authorization_requires_active_primary_for_all_operation_classes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(14), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(14),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Active, 3_000);

    let operations = [
        PgServiceOperation::MetadataRead,
        PgServiceOperation::MetadataList,
        PgServiceOperation::MetadataWrite,
        PgServiceOperation::PayloadRead,
        PgServiceOperation::PayloadWrite,
    ];
    for operation in operations {
        let authorization = authority
            .authorize_pg_operation(
                operation,
                PgId::new(14),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                3_050,
            )
            .unwrap();
        assert_eq!(authorization.operation(), operation);
        assert_eq!(authorization.pg_id(), PgId::new(14));
        assert_eq!(authorization.primary_node_id(), NodeId::new(1));
        assert_eq!(
            authorization.primary_node_incarnation(),
            node_incarnation(&authority, 1)
        );
        assert_eq!(
            authorization.cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        authority
            .validate_pg_operation_authorization(&authorization, 3_060)
            .unwrap();
        authority
            .validate_pg_operation_authorization_for(&authorization, operation, 3_060)
            .unwrap();
        let wrong_operation = match operation {
            PgServiceOperation::MetadataWrite => PgServiceOperation::MetadataRead,
            _ => PgServiceOperation::MetadataWrite,
        };
        assert!(matches!(
            authority.validate_pg_operation_authorization_for(
                &authorization,
                wrong_operation,
                3_060,
            ),
            Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                expected,
                actual,
            }) if expected == wrong_operation && actual == operation
        ));
    }

    for state in [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ] {
        authority.set_pg_state(PgId::new(14), state).unwrap();
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 4_000),
                4_000,
            )
            .unwrap();
        for operation in operations {
            assert!(matches!(
                authority.authorize_pg_operation(
                    operation,
                    PgId::new(14),
                    NodeId::new(1),
                    node_incarnation(&authority, 1),
                    authority.snapshot().cluster_epoch(),
                    4_050,
                ),
                Err(ControlPlaneError::PgNotActive {
                    pg_id: 14,
                    state: err_state,
                    ..
                }) if err_state == state
            ));
        }
    }
}

#[test]
fn pg_operation_authorization_validation_fences_stale_tokens() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_000);

    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            3_050,
        )
        .unwrap();
    authority
        .validate_pg_operation_authorization(&authorization, 3_060)
        .unwrap();

    let mut shorter_lease =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 3_061);
    shorter_lease.requested_lease_duration_ms = 5;
    shorter_lease.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(17),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refreshed = authority.heartbeat(shorter_lease, 3_061).unwrap();
    assert_eq!(
        refreshed.lease_deadline_ms(),
        authorization.lease_deadline_ms()
    );
    assert!(authorization.lease_deadline_ms() > 3_066);
    authority
        .validate_pg_operation_authorization(&authorization, 3_066)
        .unwrap();

    assert!(matches!(
        authority
            .validate_pg_operation_authorization(&authorization, authorization.lease_deadline_ms()),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_070);
    authority
        .set_pg_state(PgId::new(17), PgState::Peering)
        .unwrap();
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 3_060),
        Err(ControlPlaneError::StaleAuthorizationEpoch { .. })
    ));

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(matches!(
        restarted.validate_pg_operation_authorization(&authorization, 3_060),
        Err(ControlPlaneError::StaleAuthorityIncarnation { .. })
    ));
}

#[test]
fn stale_observed_epoch_heartbeat_returns_map_without_becoming_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(6), NodeMembershipState::Active)
        .unwrap();
    let stale_epoch = ClusterEpoch::INITIAL;
    let lease = authority
        .heartbeat(heartbeat(6, stale_epoch, 100), 100)
        .unwrap();
    assert!(!lease.serving());
    assert_eq!(lease.snapshot().cluster_epoch(), lease.cluster_epoch());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(6)], 100),
        None
    );

    let caught_up = authority
        .heartbeat(heartbeat(6, lease.cluster_epoch(), 200), 200)
        .unwrap();
    assert!(!caught_up.serving());
    let final_lease = authority
        .heartbeat(heartbeat(6, caught_up.cluster_epoch(), 300), 300)
        .unwrap();
    assert!(final_lease.serving());
}

#[test]
fn heartbeat_records_current_epoch_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
        .unwrap();

    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(15),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        },
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();

    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(15))
        .unwrap();
    assert_eq!(observation.pg_id(), PgId::new(15));
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(
        observation.observed_epoch(),
        authority.snapshot().cluster_epoch()
    );
    assert_eq!(observation.observed_at_ms(), 2_000);
    assert_eq!(
        observation.metadata_proof(),
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        }
    );
    let persisted = store.load().unwrap().unwrap();
    let persisted_observation = persisted
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(15))
        .unwrap();
    assert_eq!(persisted_observation.state(), PgState::Peering);
    assert_eq!(
        persisted_observation.metadata_proof(),
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 10,
            state_digest: 11,
        }
    );
}

#[test]
fn complete_pg_peering_requires_matching_metadata_proofs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let matching_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let different_proof = PgMetadataProof {
        applied_log_index: 41,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for (node_id, metadata_proof) in [(1, matching_proof), (2, different_proof)] {
        let mut heartbeat = heartbeat_from_record(
            &authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            2_000 + u64::from(node_id),
        );
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Peering,
            metadata_proof,
            pending_metadata_command: None,
        }];
        authority
            .heartbeat(heartbeat, 2_000 + u64::from(node_id))
            .unwrap();
    }
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 19,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == matching_proof && actual == different_proof
    ));

    let mut heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_060);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Peering,
        metadata_proof: matching_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_060).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(active_pg.active_metadata_proof(), Some(matching_proof));
    let persisted = store.load().unwrap().unwrap();
    let persisted_pg = persisted.pg(PgId::new(19)).unwrap();
    assert_eq!(persisted_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(persisted_pg.active_metadata_proof(), Some(matching_proof));
}

#[test]
fn complete_pg_peering_accepts_converged_later_epoch_log_with_unchanged_state() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(35);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let active_floor = PgMetadataProof {
        applied_log_index: 90,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            active_floor,
            false,
            2_012 + u64::from(node_id),
        );
    }

    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)])
        .unwrap();
    let later_epoch_proof = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 0x123,
        state_digest: active_floor.state_digest,
    };
    for node_id in [1, 2, 3] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            later_epoch_proof,
            false,
            2_020 + u64::from(node_id),
        );
    }
    assert_eq!(
        authority.complete_ready_pg_peerings(2_030).unwrap(),
        vec![pg_id],
        "the converged reset proof must be discovered and completed automatically"
    );

    let active = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(active.state(), PgState::Active);
    assert_eq!(active.active_metadata_proof(), Some(later_epoch_proof));
}

#[test]
fn complete_pg_peering_rejects_same_log_divergent_replica_digest() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(32);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let node_one_pg = PgStore::open(&tmp.path().join("node-1-pg"), pg_id.get()).unwrap();
    let node_two_pg = PgStore::open(&tmp.path().join("node-2-pg"), pg_id.get()).unwrap();
    let bucket = bucket_name("divergent-replica-source");
    let create = logged_create_bucket_command(pg_id, 1, &bucket);
    node_one_pg
        .apply_metadata_command_and_record(1, &create)
        .unwrap();
    node_two_pg
        .apply_metadata_command_and_record(2, &create)
        .unwrap();
    let primary_proof = pg_metadata_proof_from_store(&node_one_pg);
    assert_eq!(primary_proof, pg_metadata_proof_from_store(&node_two_pg));

    node_two_pg
        .put_bucket_versioning(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    node_two_pg.refresh_metadata_command_state_digest().unwrap();
    let divergent_proof = pg_metadata_proof_from_store(&node_two_pg);
    assert_eq!(
        divergent_proof.applied_log_index,
        primary_proof.applied_log_index
    );
    assert_eq!(
        divergent_proof.applied_log_hash,
        primary_proof.applied_log_hash
    );
    assert_ne!(divergent_proof.state_digest, primary_proof.state_digest);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        primary_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        divergent_proof,
        false,
        2_001,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 32,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == primary_proof && actual == divergent_proof
    ));
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );
}

#[test]
fn finalized_bucket_cleanup_proof_progress_requires_logged_command() {
    let tmp = test_util::tempdir();
    let pg_id = PgId::new(33);
    let store = PgStore::open(tmp.path(), pg_id.get()).unwrap();
    let bucket = bucket_name("finalized-cleanup-proof");
    let create = logged_create_bucket_command(pg_id, 1, &bucket);
    store.apply_metadata_command_and_record(1, &create).unwrap();
    let mark = logged_mark_bucket_deleting_command(&store, 2, &bucket);
    store.apply_metadata_command_and_record(1, &mark).unwrap();
    let cleanup_floor = pg_metadata_proof_from_store(&store);

    let digest_only_cleanup = PgMetadataProof {
        applied_log_index: cleanup_floor.applied_log_index,
        applied_log_hash: cleanup_floor.applied_log_hash,
        state_digest: cleanup_floor.state_digest.wrapping_add(1),
    };
    let cleanup_floor_epoch = ClusterEpoch::new(7).unwrap();
    let cleanup_observed_epoch = ClusterEpoch::new(8).unwrap();
    assert!(!metadata_proof_satisfies_active_primary_observation_floor(
        cleanup_floor,
        digest_only_cleanup,
        Some(MetadataProofProgressProvenance {
            floor_epoch: cleanup_floor_epoch,
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        cleanup_observed_epoch,
    ));

    let delete = logged_delete_finalized_bucket_command(&store, 3, &bucket);
    store.apply_metadata_command_and_record(1, &delete).unwrap();
    let logged_cleanup = pg_metadata_proof_from_store(&store);
    assert!(
        logged_cleanup.applied_log_index > cleanup_floor.applied_log_index,
        "finalized cleanup must advance the command-log index"
    );
    assert_ne!(
        logged_cleanup.applied_log_hash, cleanup_floor.applied_log_hash,
        "finalized cleanup must advance the command-log hash"
    );
    assert!(metadata_proof_satisfies_active_primary_observation_floor(
        cleanup_floor,
        logged_cleanup,
        Some(MetadataProofProgressProvenance {
            floor_epoch: cleanup_floor_epoch,
            kind: MetadataProofProgressKind::LocalEpoch,
        }),
        cleanup_observed_epoch,
    ));
    assert!(matches!(
        store.head_bucket_record_raw(&bucket),
        Err(crate::MetadataError::BucketNotFound { .. })
    ));
}

#[test]
fn pg_peering_reconstruction_fails_closed_until_serving_replicas_converge() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let reconstructed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        31,
        PgState::Peering,
        reconstructed_proof,
        false,
        2_001,
    );

    let lagging_proof = PgMetadataProof {
        applied_log_index: 41,
        applied_log_hash: 0xaaa,
        state_digest: 0xddd,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        lagging_proof,
        false,
        2_002,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == lagging_proof
    ));

    let same_index_hash_fork = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabd,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        same_index_hash_fork,
        false,
        2_020,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_030,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == same_index_hash_fork
    ));

    let same_index_state_fork = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdf0,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        same_index_state_fork,
        false,
        2_040,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 31,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == reconstructed_proof && actual == same_index_state_fork
    ));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        31,
        PgState::Peering,
        reconstructed_proof,
        false,
        2_060,
    );
    authority
        .complete_pg_peering(
            PgId::new(31),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(31)).unwrap();
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(active_pg.active_metadata_proof(), Some(reconstructed_proof));
}

#[test]
fn complete_pg_peering_rejects_pending_metadata_command_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(29), vec![NodeId::new(1)])
        .unwrap();

    let proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(&mut authority, 1, 29, PgState::Peering, proof, false, 2_000);
    authority.complete_ready_pg_peerings(2_010).unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_epoch)),
    }];
    authority.heartbeat(heartbeat, 2_020).unwrap();
    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_030);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Peering,
        metadata_proof: proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_epoch)),
    }];
    authority.heartbeat(heartbeat, 2_030).unwrap();

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(29),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_050,
        ),
        Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 29,
            node_id: 1,
            ..
        })
    ));

    let mut heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_060);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(29),
        state: PgState::Peering,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_060).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(29),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_070,
        )
        .unwrap();
}

#[test]
fn active_heartbeat_accepts_metadata_progress_after_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
        .unwrap();

    let accepted_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let peering_epoch = authority.snapshot().cluster_epoch();
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Peering,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch < active_epoch);
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));

    let mut equal_active = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    equal_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: accepted_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(equal_active, 2_020).unwrap();
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(accepted_proof));
    assert_eq!(
        pg.active_metadata_proof_epoch(),
        Some(peering_epoch),
        "equal-proof heartbeat must not restamp proof provenance"
    );
    let pre_progress_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();

    let progressed_proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 0x1234,
        state_digest: 0x5678,
    };
    let mut progressed_active = heartbeat_from_record(&authority, 1, active_epoch, 2_030);
    progressed_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    let lease = authority.heartbeat(progressed_active, 2_030).unwrap();
    assert_eq!(lease.lease_deadline_ms(), 2_130);
    assert!(lease.lease_deadline_ms() > pre_progress_deadline);
    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(19))
        .unwrap();
    assert_eq!(observation.state(), PgState::Active);
    assert_eq!(observation.metadata_proof(), progressed_proof);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(19), 2_031),
        Some(NodeId::new(1))
    );
    let expiry_at_old_deadline = authority
        .expire_heartbeat_leases(pre_progress_deadline + 1)
        .unwrap();
    assert_eq!(expiry_at_old_deadline.expired_nodes(), &[]);
    assert_eq!(expiry_at_old_deadline.peering_pgs(), &[]);
    assert_eq!(
        authority.snapshot().pg(PgId::new(19)).unwrap().state(),
        PgState::Active
    );
    let pg = authority.snapshot().pg(PgId::new(19)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_stamps_active_proofs_with_peering_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    for pg_id in [21, 22] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }

    let peering_epoch = authority.snapshot().cluster_epoch();
    let proof_21 = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let proof_22 = PgMetadataProof {
        applied_log_index: 99,
        applied_log_hash: 0xaabb,
        state_digest: 0xccdd,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![
        NodePgHeartbeatObservation {
            pg_id: PgId::new(21),
            state: PgState::Peering,
            metadata_proof: proof_21,
            pending_metadata_command: None,
        },
        NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Peering,
            metadata_proof: proof_22,
            pending_metadata_command: None,
        },
    ];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let mut completed = authority.complete_ready_pg_peerings(2_010).unwrap();
    completed.sort_by_key(|pg_id| pg_id.get());
    assert_eq!(completed, vec![PgId::new(21), PgId::new(22)]);
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch < active_epoch);
    for (pg_id, proof) in [(21, proof_21), (22, proof_22)] {
        let pg = authority.snapshot().pg(PgId::new(pg_id)).unwrap();
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(pg.active_metadata_proof(), Some(proof));
        assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));
    }

    let progressed_proof = PgMetadataProof {
        applied_log_index: 1,
        applied_log_hash: 0x1234,
        state_digest: 0x5678,
    };
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(21),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_020).unwrap();
    let pg = authority.snapshot().pg(PgId::new(21)).unwrap();
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_unobserved_metadata_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(23),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let forged_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(23),
                    primary: NodeId::new(1),
                    node_incarnation: node_incarnation(&authority, 1),
                    active_metadata_proof: forged_proof,
                    active_metadata_proof_epoch: peering_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
            pg_id: 23,
            node_id: 1,
            expected,
            actual,
            ..
        }) if expected == observed_proof && actual == forged_proof
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_wrong_metadata_proof_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(24),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let forged_epoch = ClusterEpoch::new(peering_epoch.get() + 1).unwrap();
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(24),
                    primary: NodeId::new(1),
                    node_incarnation: node_incarnation(&authority, 1),
                    active_metadata_proof: observed_proof,
                    active_metadata_proof_epoch: forged_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofEpochMismatch {
            pg_id: 24,
            expected,
            actual,
        }) if expected == peering_epoch && actual == forged_epoch
    ));
}

#[test]
fn complete_ready_pg_peerings_command_replays_with_committed_ready_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(25),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let command = ControlPlaneCommand::CompleteReadyPgPeerings {
        ready_at_ms: 2_010,
        ready: vec![ReadyPgPeeringCompletion {
            pg_id: PgId::new(25),
            primary: NodeId::new(1),
            node_incarnation: node_incarnation(&authority, 1),
            active_metadata_proof: observed_proof,
            active_metadata_proof_epoch: peering_epoch,
        }],
    };
    let applied = authority
        .snapshot()
        .apply_control_plane_command(command.clone())
        .unwrap();
    let pg = applied.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(pg.active_metadata_proof(), Some(observed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(2_010));

    authority.snapshot = applied.into_snapshot();
    persist_manually_modified_test_snapshot(&mut authority);
    let active_epoch = authority.snapshot().cluster_epoch();
    let progressed_proof = PgMetadataProof {
        applied_log_index: 43,
        applied_log_hash: 0xbc,
        state_digest: 0xef,
    };
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_010);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(25),
        state: PgState::Active,
        metadata_proof: progressed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(active_heartbeat, 2_010).unwrap();
    let replayed = authority
        .snapshot()
        .apply_control_plane_command(command)
        .unwrap();
    assert!(
        !replayed.changed(),
        "exact active CompleteReadyPgPeerings replay should be a no-op"
    );
    let pg = replayed.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
    assert_eq!(pg.active_metadata_proof(), Some(progressed_proof));
    assert_eq!(pg.active_metadata_proof_epoch(), Some(active_epoch));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_stale_node_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(51), vec![NodeId::new(1)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(51),
        state: PgState::Peering,
        metadata_proof: observed_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();

    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(2_010)
        .unwrap();
    assert_eq!(ready.len(), 1);
    let mut restarted = heartbeat_from_record(&authority, 1, peering_epoch, 2_011);
    restarted.node_incarnation += 1;
    authority.heartbeat(restarted, 2_011).unwrap();

    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_012,
                ready,
            },
        ),
        Err(ControlPlaneError::NodeIncarnationMismatch {
            node_id: 1,
            sender_incarnation,
            current_incarnation,
        }) if sender_incarnation + 1 == current_incarnation
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 2_000).serving());

    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::CompleteReadyPgPeerings {
            ready_at_ms: 1_999,
            ready: Vec::new(),
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 1_999,
            max_committed_timestamp_ms: 2_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn complete_ready_pg_peerings_command_rejects_non_deterministic_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    let peering_epoch = authority.snapshot().cluster_epoch();
    let observed_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    for node_id in [1, 2] {
        let mut peering_heartbeat =
            heartbeat_from_record(&authority, node_id, peering_epoch, 2_000);
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(26),
            state: PgState::Peering,
            metadata_proof: observed_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    }

    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_010,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(26),
                    primary: NodeId::new(2),
                    node_incarnation: node_incarnation(&authority, 2),
                    active_metadata_proof: observed_proof,
                    active_metadata_proof_epoch: peering_epoch,
                }],
            },
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 26,
            node_id: 2
        })
    ));
}

#[test]
fn complete_ready_pg_peerings_command_rejects_duplicate_pg_completion() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();

    let active_proof = authority
        .snapshot()
        .pg(PgId::new(27))
        .unwrap()
        .active_metadata_proof()
        .unwrap();
    authority
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(27),
            vec![NodeId::new(2)],
            PgMetadataTransferProof::new(authority.snapshot().cluster_epoch(), active_proof),
        )
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        27,
        PgState::Peering,
        active_proof,
        false,
        2_020,
    );

    let completion = ReadyPgPeeringCompletion {
        pg_id: PgId::new(27),
        primary: NodeId::new(2),
        node_incarnation: node_incarnation(&authority, 2),
        active_metadata_proof: active_proof,
        active_metadata_proof_epoch: peering_epoch,
    };
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 2_030,
                ready: vec![completion, completion],
            },
        ),
        Err(ControlPlaneError::DuplicateReadyPgPeeringCompletion { pg_id: 27 })
    ));
}

#[test]
fn active_primary_heartbeat_pending_command_fences_pg_for_recovery() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(32), vec![NodeId::new(1)])
        .unwrap();

    let active_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 0xabc,
        state_digest: 0xdef,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        32,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(32),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let pending = test_pending_metadata_command(active_epoch);

    let mut pending_active = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    pending_active.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(32),
        state: PgState::Active,
        metadata_proof: active_proof,
        pending_metadata_command: Some(pending),
    }];
    let before_invalid = authority.snapshot().clone();
    let mut invalid_pending = pending_active.clone();
    invalid_pending.pg_observations[0].pending_metadata_command = Some(
        test_pending_metadata_command(ClusterEpoch::new(active_epoch.get() + 1).unwrap()),
    );
    assert!(matches!(
        authority.refresh_node_heartbeat(invalid_pending, 2_020),
        Err(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })
            if cluster_epoch == ClusterEpoch::new(active_epoch.get() + 1).unwrap()
    ));
    assert_eq!(authority.snapshot(), &before_invalid);

    let refresh = authority
        .refresh_node_heartbeat(pending_active, 2_020)
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > active_epoch);
    assert!(!refresh.lease().serving());
    let route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(32))
        .unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(
        route.pending_metadata_command_recovery(),
        Some(PendingMetadataCommandRecovery::new(NodeId::new(1), pending))
    );
    authority = reopen_file_authority(&store);
    let pg = authority.snapshot().pg(PgId::new(32)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(32))
        .is_none());
    assert!(authority
        .snapshot()
        .pending_metadata_command_recoveries()
        .tasks()
        .is_empty());

    let restart_epoch = authority.snapshot().cluster_epoch();
    let mut reconstructed = heartbeat_from_record(&authority, 1, restart_epoch, 2_030);
    reconstructed.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(32),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: Some(pending),
    }];
    let reconstructed = authority
        .refresh_node_heartbeat(reconstructed, 2_030)
        .unwrap();
    assert_eq!(reconstructed.runtime_map().cluster_epoch(), restart_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pending_metadata_command_recoveries()
            .tasks(),
        &[PendingMetadataCommandRecoveryTask::new(
            PgId::new(32),
            PendingMetadataCommandRecovery::new(NodeId::new(1), pending),
        )]
    );
    let observation = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(32))
        .unwrap();
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(observation.observed_epoch(), restart_epoch);
    assert_eq!(observation.pending_metadata_command(), Some(pending));
    assert_eq!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(32), active_epoch)
            .unwrap()
            .state(),
        PgState::Active
    );
    assert_eq!(store.load().unwrap().unwrap(), *authority.snapshot());
}

#[test]
fn peering_heartbeat_rejects_pending_command_without_historical_active_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(33), vec![NodeId::new(1)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let mut invalid = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    invalid.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(33),
        state: PgState::Peering,
        metadata_proof: heartbeat_model_proof(3),
        pending_metadata_command: Some(test_pending_metadata_command(peering_epoch)),
    }];
    let before = authority.snapshot().clone();

    assert!(matches!(
        authority.refresh_node_heartbeat(invalid, 2_000),
        Err(
            ControlPlaneError::PgPeeringPendingMetadataCommandReporterNotHistoricalPrimary {
                pg_id: 33,
                node_id: 1,
                pending_epoch,
                historical_state: PgState::Peering,
                ..
            }
        ) if pending_epoch == peering_epoch
    ));
    assert_eq!(authority.snapshot(), &before);
    assert_eq!(store.load().unwrap().unwrap(), before);
}

#[test]
fn stale_heartbeat_does_not_mutate_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();

    let mut stale = heartbeat_from_record(
        &authority,
        1,
        ClusterEpoch::new(current_epoch.get() - 1).unwrap(),
        2_000,
    );
    stale.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(16),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let response = authority.heartbeat(stale, 2_000).unwrap();
    assert!(!response.serving());
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(16))
        .is_none());
}

#[test]
fn heartbeat_rejects_invalid_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();

    let mut duplicate =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    duplicate.pg_observations = vec![
        NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        },
        NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        },
    ];
    assert!(matches!(
        authority.heartbeat(duplicate, 2_000),
        Err(ControlPlaneError::DuplicatePgObservation {
            node_id: 1,
            pg_id: 17
        })
    ));

    let mut unknown =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_001);
    unknown.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(99),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(unknown, 2_001),
        Err(ControlPlaneError::UnknownPg { pg_id: 99 })
    ));

    let mut wrong_node =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_002);
    wrong_node.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(17),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    assert!(matches!(
        authority.heartbeat(wrong_node, 2_002),
        Err(ControlPlaneError::PgObservationNotInActingSet {
            node_id: 2,
            pg_id: 17
        })
    ));
}

#[test]
fn epoch_change_drops_reconstructible_pg_observations_from_history() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    let observation_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(18),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_some());

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    let history = authority
        .snapshot()
        .cluster_map_at_epoch(observation_epoch)
        .unwrap();
    assert!(history.nodes().contains(&NodeId::new(1)));
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(18), observation_epoch)
        .is_ok());
}

#[test]
fn restart_epoch_bump_drops_reconstructible_pg_observations_from_history() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    let observation_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
    let metadata_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(18),
        state: PgState::Peering,
        metadata_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(heartbeat, 2_000).unwrap();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_some());

    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert!(restarted.snapshot().cluster_epoch() > observation_epoch);
    assert!(restarted
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    assert!(restarted
        .snapshot()
        .cluster_map_at_epoch(observation_epoch)
        .unwrap()
        .nodes()
        .contains(&NodeId::new(1)));
    let persisted_text = std::fs::read_to_string(store.path()).unwrap();
    assert!(!persisted_text.contains("history_node_pg="));
    assert!(persisted_text
        .lines()
        .any(|line| { line.starts_with("history_node=") && line.split(',').count() == 2 }));
    assert!(persisted_text
        .lines()
        .any(|line| { line.starts_with("history_pg_absent=") && line.split(',').count() == 2 }));

    let persisted = store.load().unwrap().unwrap();
    assert!(persisted
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(PgId::new(18))
        .is_none());
    SingleAuthorityControlPlane::open(store).unwrap();
}

#[test]
fn authority_restart_moves_active_pg_back_to_peering_before_service() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
        .unwrap();
    let active_metadata_proof = PgMetadataProof {
        applied_log_index: 11,
        applied_log_hash: 12,
        state_digest: 13,
    };
    let mut peering_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Peering,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(peering_heartbeat, 2_000).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let mut active_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_002);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Active,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let active = authority.heartbeat(active_heartbeat, 2_002).unwrap();
    let active_epoch = active.cluster_epoch();
    assert_eq!(
        authority.snapshot().pg(PgId::new(26)).unwrap().state(),
        PgState::Active
    );

    let mut restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let restart_epoch = restarted.snapshot().cluster_epoch();
    assert!(restart_epoch > active_epoch);
    let restarted_pg = restarted.snapshot().pg(PgId::new(26)).unwrap();
    assert_eq!(restarted_pg.state(), PgState::Peering);
    assert_eq!(restarted_pg.active_primary(), None);
    assert_eq!(restarted_pg.active_metadata_proof(), None);
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(active_metadata_proof)
    );
    let historical_pg = restarted
        .snapshot()
        .cluster_map_at_epoch(active_epoch)
        .unwrap()
        .pgs()
        .iter()
        .find(|record| record.pg_id() == PgId::new(26))
        .unwrap();
    assert_eq!(historical_pg.state(), PgState::Active);
    assert_eq!(historical_pg.active_primary, Some(NodeId::new(1)));
    let historical_route = restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(26), active_epoch)
        .unwrap();
    assert_eq!(historical_route.cluster_epoch(), active_epoch);
    assert_eq!(historical_route.state(), PgState::Active);
    assert_eq!(historical_route.primary_node_id(), NodeId::new(1));
    assert_eq!(historical_route.acting_set(), &[NodeId::new(1)]);
    assert_eq!(historical_route.primary_lease_deadline_ms(), None);

    let mut stale_active_heartbeat = heartbeat_from_record(&restarted, 1, restart_epoch, 2_003);
    stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Active,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let refresh = restarted
        .refresh_node_heartbeat(stale_active_heartbeat, 2_003)
        .unwrap();
    assert!(refresh.lease().serving());
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Peering
    );
    assert!(matches!(
        restarted.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&restarted, 1),
            restart_epoch,
            2_004,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 26,
            state: PgState::Peering,
            ..
        })
    ));

    let mut current_peering_heartbeat = heartbeat_from_record(&restarted, 1, restart_epoch, 2_005);
    current_peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(26),
        state: PgState::Peering,
        metadata_proof: active_metadata_proof,
        pending_metadata_command: None,
    }];
    let refresh = restarted
        .refresh_node_heartbeat(current_peering_heartbeat, 2_005)
        .unwrap();
    assert!(
        !refresh.lease().serving(),
        "same-process peering completion bumps the epoch before the node observes it"
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Active
    );
    assert_eq!(
        restarted.snapshot().pg(PgId::new(26)).unwrap().state(),
        PgState::Active,
        "the unchanged primary process need not wait out its own old lease"
    );
}

#[test]
fn stale_observed_epoch_heartbeat_updates_liveness_without_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(7), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 7, 100);
    assert!(serving.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 100),
        Some(NodeId::new(7))
    );
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(7)])
        .unwrap();
    let route_epoch = authority.snapshot().cluster_epoch();

    let before = authority.snapshot().node(NodeId::new(7)).unwrap().clone();
    let stale_epoch = ClusterEpoch::new(route_epoch.get() - 1).unwrap();
    let mut stale = heartbeat(7, stale_epoch, 200);
    stale.node_incarnation = before.node_incarnation() + 1;
    stale.endpoint = "stale-node-7.sock".to_owned();
    let stale_response = authority.refresh_node_heartbeat(stale, 200).unwrap();
    let stale_lease = stale_response.lease();
    assert!(!stale_lease.serving());
    assert!(stale_lease.cluster_epoch() > serving.cluster_epoch());
    assert_eq!(
        stale_response.runtime_map().cluster_epoch(),
        stale_lease.cluster_epoch()
    );
    let stale_route = stale_response
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(1))
        .unwrap();
    assert_eq!(stale_route.state(), PgState::Peering);
    assert_eq!(stale_route.primary_node_id(), NodeId::new(7));
    assert_eq!(stale_route.primary_lease_deadline_ms(), None);

    let after = authority.snapshot().node(NodeId::new(7)).unwrap();
    assert_eq!(after.node_incarnation(), before.node_incarnation() + 1);
    assert_eq!(after.endpoint(), "stale-node-7.sock");
    assert_eq!(after.availability(), NodeAvailabilityState::Healthy);
    assert_eq!(after.last_observed_epoch(), Some(stale_epoch));
    assert_eq!(after.last_heartbeat_ms(), Some(200));
    assert_eq!(after.lease_deadline_ms(), Some(300));
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 200),
        None
    );
}

#[test]
fn future_observed_epoch_heartbeat_rejected_without_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();

    let proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    heartbeat_with_pg_proof(&mut authority, 1, 1, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(1),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut active_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_020);
    active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];
    assert!(authority
        .heartbeat(active_heartbeat, 2_020)
        .unwrap()
        .serving());
    assert_eq!(
        authority.serving_pg_primary(PgId::new(1), 2_021),
        Some(NodeId::new(1))
    );

    let before = authority.snapshot().clone();
    let before_node = before.node(NodeId::new(1)).unwrap();
    let future_epoch = ClusterEpoch::new(before.cluster_epoch().get() + 100).unwrap();
    let mut future = heartbeat_from_record(&authority, 1, future_epoch, 2_030);
    future.node_incarnation = before_node.node_incarnation() + 1;
    future.endpoint = "future-node-1.sock".to_owned();
    future.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: proof,
        pending_metadata_command: None,
    }];

    assert!(matches!(
        authority.refresh_node_heartbeat(future, 2_030),
        Err(ControlPlaneError::FutureNodeObservedEpoch {
            node_id: 1,
            observed_epoch,
            current_epoch,
        }) if observed_epoch == future_epoch && current_epoch == before.cluster_epoch()
    ));
    assert_eq!(authority.snapshot(), &before);
    assert_eq!(store.load().unwrap().unwrap(), before);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(1), 2_031),
        Some(NodeId::new(1))
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        max_shrink_iters: 256,
    ..ProptestConfig::default()
    })]

    #[test]
    fn prop_record_node_heartbeat_command_matches_single_authority(
        ops in proptest::collection::vec(
            control_plane_heartbeat_command_boundary_op_strategy(),
            1..48,
        ),
    ) {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000 + u64::from(node_id)).serving());
        }
        authority
            .set_pg_acting_set(heartbeat_model_pg_id(), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        let initial_proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_000,
        );
        heartbeat_with_pg_proof(
            &mut authority,
            2,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_001,
        );
        authority.complete_ready_pg_peerings(2_002).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        let mut active_primary_heartbeat =
            heartbeat_from_record(&authority, 1, active_epoch, 2_003);
        active_primary_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: heartbeat_model_pg_id(),
            state: PgState::Active,
            metadata_proof: initial_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(active_primary_heartbeat, 2_003).unwrap();

        let mut replayed = authority.snapshot().clone();
        let mut now_ms = 3_000_u64;

        for op in ops {
            now_ms = now_ms.saturating_add(10);
            let heartbeat = match op {
                ControlPlaneHeartbeatCommandBoundaryOp::Current {
                    node_slot,
                    observation_kind,
                    floor_kind,
                    bump_incarnation,
                    change_endpoint,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let mut heartbeat = heartbeat_from_snapshot(
                        &replayed,
                        node_id,
                        replayed.cluster_epoch(),
                        now_ms,
                    );
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-current-node-{node_id}-{now_ms}.sock");
                    }
                    heartbeat.pg_observations =
                        heartbeat_model_observation(&replayed, node_id, observation_kind);
                    heartbeat.cluster_map_history_route_references =
                        heartbeat_model_history_references(&replayed, floor_kind);
                    heartbeat
                }
                ControlPlaneHeartbeatCommandBoundaryOp::Stale {
                    node_slot,
                    stale_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let current_raw_epoch = replayed.cluster_epoch().get();
                    let stale_raw_epoch = current_raw_epoch
                        .saturating_sub(u64::from(stale_delta))
                        .max(ClusterEpoch::INITIAL.get());
                    let stale_epoch = ClusterEpoch::new(stale_raw_epoch).unwrap();
                    let mut heartbeat =
                        heartbeat_from_snapshot(&replayed, node_id, stale_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-stale-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&replayed, node_id, 3);
                    }
                    heartbeat
                }
                ControlPlaneHeartbeatCommandBoundaryOp::Future {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let future_epoch = ClusterEpoch::new(
                        replayed.cluster_epoch().get() + u64::from(future_delta),
                    )
                    .unwrap();
                    let mut heartbeat =
                        heartbeat_from_snapshot(&replayed, node_id, future_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint =
                            format!("boundary-future-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&replayed, node_id, 4);
                    }
                    heartbeat
                }
            };

            let lease_deadline_ms = now_ms + heartbeat.requested_lease_duration_ms;
            let before_replayed = replayed.clone();
            let before_authority = authority.snapshot().clone();
            let replay_result = replayed.apply_control_plane_command(
                ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: heartbeat.clone(),
                    heartbeat_at_ms: now_ms,
                    lease_deadline_ms,
                    lease_horizon_authority: None,
                },
            );
            let authority_result = authority.heartbeat(heartbeat, now_ms);

            match (replay_result, authority_result) {
                (Ok(applied), Ok(_lease)) => {
                    replayed = applied.into_snapshot();
                    prop_assert_eq!(
                        &replayed,
                        authority.snapshot(),
                        "direct heartbeat command replay must match single-authority heartbeat"
                    );
                }
                (Err(_), Err(_)) => {
                    prop_assert_eq!(
                        &replayed,
                        &before_replayed,
                        "rejected direct heartbeat command must not mutate replay snapshot"
                    );
                    prop_assert_eq!(
                        authority.snapshot(),
                        &before_authority,
                        "rejected single-authority heartbeat must not mutate snapshot"
                    );
                }
                (Ok(_), Err(error)) => {
                    prop_assert!(
                        false,
                        "direct heartbeat command accepted but single-authority rejected: {error:?}"
                    );
                }
                (Err(error), Ok(_)) => {
                    prop_assert!(
                        false,
                        "single-authority heartbeat accepted but direct command rejected: {error:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn prop_control_plane_epoch_heartbeat_model_preserves_invariants(
        ops in proptest::collection::vec(control_plane_heartbeat_model_op_strategy(), 1..48),
    ) {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000 + u64::from(node_id)).serving());
        }
        authority
            .set_pg_acting_set(heartbeat_model_pg_id(), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        let initial_proof = heartbeat_model_proof(9);
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_000,
        );
        heartbeat_with_pg_proof(
            &mut authority,
            2,
            heartbeat_model_pg_id().get(),
            PgState::Peering,
            initial_proof,
            false,
            2_001,
        );
        authority.complete_ready_pg_peerings(2_002).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        let mut active_primary_heartbeat =
            heartbeat_from_record(&authority, 1, active_epoch, 2_003);
        active_primary_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: heartbeat_model_pg_id(),
            state: PgState::Active,
            metadata_proof: initial_proof,
            pending_metadata_command: None,
        }];
        authority.heartbeat(active_primary_heartbeat, 2_003).unwrap();

        let mut now_ms = 3_000_u64;
        assert_control_plane_heartbeat_model_invariants(&authority, &store, now_ms)?;

        for op in ops {
            now_ms = now_ms.saturating_add(10);
            match op {
                ControlPlaneHeartbeatModelOp::CurrentHeartbeat {
                    node_slot,
                    observation_kind,
                    floor_kind,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let before = authority.snapshot().clone();
                    let mut heartbeat = heartbeat_from_record(
                        &authority,
                        node_id,
                        authority.snapshot().cluster_epoch(),
                        now_ms,
                    );
                    heartbeat.pg_observations = heartbeat_model_observation(
                        authority.snapshot(),
                        node_id,
                        observation_kind,
                    );
                    heartbeat.cluster_map_history_route_references =
                        heartbeat_model_history_references(authority.snapshot(), floor_kind);
                    if authority.heartbeat(heartbeat, now_ms).is_err() {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(
                            store.load().unwrap().unwrap(),
                            before,
                            "rejected current heartbeat must not mutate durable state"
                        );
                    }
                }
                ControlPlaneHeartbeatModelOp::StaleHeartbeat {
                    node_slot,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let current_epoch = authority.snapshot().cluster_epoch();
                    let stale_epoch = ClusterEpoch::new(current_epoch.get().saturating_sub(1))
                        .unwrap_or(ClusterEpoch::INITIAL);
                    let mut heartbeat =
                        heartbeat_from_record(&authority, node_id, stale_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint = format!("stale-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(authority.snapshot(), node_id, 3);
                    }
                    let before = authority.snapshot().clone();
                    let before_epoch = authority.snapshot().cluster_epoch();
                    let Ok(lease) = authority.heartbeat(heartbeat, now_ms) else {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(store.load().unwrap().unwrap(), before);
                        continue;
                    };
                    prop_assert!(!lease.serving());
                    prop_assert!(lease.cluster_epoch() >= before_epoch);
                    prop_assert!(
                        authority
                            .snapshot()
                            .node(NodeId::new(node_id))
                            .unwrap()
                            .pg_observation(heartbeat_model_pg_id())
                            .is_none(),
                        "stale heartbeats must not install current PG observations"
                    );
                }
                ControlPlaneHeartbeatModelOp::FutureHeartbeat {
                    node_slot,
                    future_delta,
                    bump_incarnation,
                    change_endpoint,
                    include_observation,
                } => {
                    let node_id = heartbeat_model_node_id(node_slot);
                    let before = authority.snapshot().clone();
                    let future_epoch = ClusterEpoch::new(
                        before.cluster_epoch().get() + u64::from(future_delta),
                    )
                    .unwrap();
                    let mut heartbeat =
                        heartbeat_from_record(&authority, node_id, future_epoch, now_ms);
                    if bump_incarnation {
                        heartbeat.node_incarnation =
                            heartbeat.node_incarnation.saturating_add(1);
                    }
                    if change_endpoint {
                        heartbeat.endpoint = format!("future-node-{node_id}-{now_ms}.sock");
                    }
                    if include_observation {
                        heartbeat.pg_observations =
                            heartbeat_model_observation(&before, node_id, 4);
                    }
                    let error = authority.refresh_node_heartbeat(heartbeat, now_ms).unwrap_err();
                    let is_fail_closed_error = matches!(
                        error,
                        ControlPlaneError::FutureNodeObservedEpoch { .. }
                            | ControlPlaneError::CommittedTimestampTooFarAhead { .. }
                    );
                    prop_assert!(is_fail_closed_error);
                    prop_assert_eq!(authority.snapshot(), &before);
                    prop_assert_eq!(
                        store.load().unwrap().unwrap(),
                        before,
                        "future heartbeats must not mutate durable state"
                    );
                }
                ControlPlaneHeartbeatModelOp::SetActingSet { shape } => {
                    let before = authority.snapshot().clone();
                    if authority
                        .set_pg_acting_set(
                            heartbeat_model_pg_id(),
                            heartbeat_model_acting_set(shape),
                        )
                        .is_err()
                    {
                        prop_assert_eq!(authority.snapshot(), &before);
                        prop_assert_eq!(
                            store.load().unwrap().unwrap(),
                            before,
                            "rejected acting-set changes must not mutate durable state"
                        );
                    }
                }
                ControlPlaneHeartbeatModelOp::CompleteReadyPeerings => {
                    authority.complete_ready_pg_peerings(now_ms).unwrap();
                }
                ControlPlaneHeartbeatModelOp::ExpireLeases { advance_ms } => {
                    now_ms = now_ms.saturating_add(u64::from(advance_ms));
                    authority.expire_heartbeat_leases(now_ms).unwrap();
                }
                ControlPlaneHeartbeatModelOp::RestartAuthority => {
                    authority = reopen_file_authority(&store);
                }
            }

            assert_control_plane_heartbeat_model_invariants(&authority, &store, now_ms)?;
        }
    }
}

#[test]
fn administrative_unavailable_fence_survives_heartbeat_and_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let healthy_epoch = authority
        .heartbeat(heartbeat(2, authority.snapshot().cluster_epoch(), 100), 100)
        .unwrap()
        .cluster_epoch();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let unavailable = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(unavailable.membership(), NodeMembershipState::Active);
    assert_eq!(
        unavailable.availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(!unavailable.administratively_available());
    assert_eq!(
        unavailable.observed_availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(authority.snapshot().cluster_epoch() > healthy_epoch);

    let unavailable_epoch = authority.snapshot().cluster_epoch();
    let fenced = authority
        .heartbeat(heartbeat(2, unavailable_epoch, 500), 500)
        .unwrap();
    assert_eq!(fenced.cluster_epoch(), unavailable_epoch);
    assert!(!fenced.serving());
    let fenced_node = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!fenced_node.administratively_available());
    assert_eq!(
        fenced_node.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let fenced_lease_deadline_ms = fenced_node.lease_deadline_ms();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let still_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), unavailable_epoch);
    assert_eq!(
        still_fenced.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    assert_eq!(still_fenced.lease_deadline_ms(), fenced_lease_deadline_ms);

    let mut authority = reopen_file_authority(&store);
    let restarted = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!restarted.administratively_available());
    assert_eq!(restarted.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(
        restarted.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let restart_epoch = authority.snapshot().cluster_epoch();
    assert!(!authority
        .heartbeat(heartbeat(2, restart_epoch, 600), 600)
        .unwrap()
        .serving());
    let expiry = authority.expire_heartbeat_leases(700).unwrap();
    assert_eq!(expiry.cluster_epoch(), restart_epoch);
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(2)]);
    assert!(expiry.peering_pgs().is_empty());
    let expired_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!expired_fenced.administratively_available());
    assert_eq!(
        expired_fenced.observed_availability(),
        NodeAvailabilityState::Unavailable
    );

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    let enabled_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority
        .heartbeat(heartbeat(2, enabled_epoch, 700), 700)
        .unwrap();
    assert!(!recovering.serving());
    let serving = authority
        .heartbeat(heartbeat(2, recovering.cluster_epoch(), 800), 800)
        .unwrap();
    assert!(serving.serving());
    let enabled = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(enabled.membership(), NodeMembershipState::Active);
    assert!(enabled.administratively_available());
    assert_eq!(
        enabled.observed_availability(),
        NodeAvailabilityState::Healthy
    );
}

#[test]
fn deterministic_primary_uses_first_healthy_serving_acting_set_member() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Unavailable)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Out)
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    let acting_set = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(7), &acting_set, 2_000),
        Some(NodeId::new(3))
    );
}

#[test]
fn expired_heartbeat_lease_marks_node_unavailable_and_bumps_epoch_once() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let node_one = heartbeat_until_serving(&mut authority, 1, 1_000);
    let node_two = heartbeat_until_serving(&mut authority, 2, 1_000);
    assert!(node_one.serving());
    assert!(node_two.serving());
    let node_one_current = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    assert!(node_one_current.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_002,),
        Some(NodeId::new(1))
    );
    authority
        .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            9,
            PgState::Peering,
            1_003 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(9),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_011);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_051),
            1_051,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(9), 1_051),
        Some(NodeId::new(1))
    );

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(1_152).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(9)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(expiry.snapshot().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        expiry.snapshot().pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_112,),
        None
    );
    assert_eq!(authority.serving_pg_primary(PgId::new(9), 1_112), None);

    let durable_after_expiry = std::fs::read(&store_path).unwrap();
    let repeated = authority.expire_heartbeat_leases(9_999).unwrap();
    assert_eq!(repeated.expired_nodes(), &[]);
    assert_eq!(repeated.peering_pgs(), &[]);
    assert_eq!(repeated.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        durable_after_expiry,
        "an expiry scan with no lease transition must not rewrite state"
    );

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(persisted.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        persisted.pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        persisted.node(NodeId::new(2)).unwrap().availability(),
        NodeAvailabilityState::Unavailable
    );
}

#[test]
fn heartbeat_after_expiry_must_observe_new_epoch_before_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 3, 1_000);
    assert!(serving.serving());

    let expiry = authority.expire_heartbeat_leases(1_101).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(3)]);

    let stale_after_expiry = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, serving.cluster_epoch(), 1_200),
            1_200,
        )
        .unwrap();
    assert!(!stale_after_expiry.serving());
    assert_eq!(stale_after_expiry.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(3))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, stale_after_expiry.cluster_epoch(), 1_300),
            1_300,
        )
        .unwrap();
    assert!(recovered.cluster_epoch() > stale_after_expiry.cluster_epoch());
    assert!(!recovered.serving());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, recovered.cluster_epoch(), 1_400),
            1_400,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(3)], 1_400),
        Some(NodeId::new(3))
    );
}

#[test]
fn recovered_earlier_primary_forces_active_pg_back_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 100).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Peering, 1_000);
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, 1_050);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_060,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Active, 1_070);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_080),
            1_080,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), 1_080),
        Some(NodeId::new(1))
    );

    let node_one_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expiry = authority
        .expire_heartbeat_leases(node_one_deadline)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(13)]);

    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Peering,
        node_one_deadline + 1,
    );
    let successor_fence_ms = node_one_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, successor_fence_ms);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            successor_fence_ms,
        )
        .unwrap();
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Active,
        successor_fence_ms + 1,
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 1),
        Some(NodeId::new(2))
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                successor_fence_ms + 2,
            ),
            successor_fence_ms + 2,
        )
        .unwrap();
    assert!(!recovered.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 2),
        None
    );

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                recovered.cluster_epoch(),
                successor_fence_ms + 3,
            ),
            successor_fence_ms + 3,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            successor_fence_ms + 4,
        ),
        Err(ControlPlaneError::PgNotActive { pg_id: 13, .. })
    ));
}

#[test]
fn stale_runtime_map_fails_closed_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 2_002);

    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let stale_frontend_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &active_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(
        stale_frontend_cluster.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert!(stale_frontend_cluster
        .require_route_map_valid_at(valid_until_ms - 1)
        .is_ok());

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(valid_until_ms).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(17)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        stale_frontend_cluster.require_route_map_valid_at(valid_until_ms),
        Err(crate::StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: expired_at,
            now_ms,
        }) if cluster_epoch == active_map.cluster_epoch()
            && expired_at == valid_until_ms
            && now_ms == valid_until_ms
    ));
    assert_eq!(
        authority
            .snapshot()
            .runtime_map(valid_until_ms)
            .unwrap()
            .pg_routes()[0]
            .state(),
        PgState::Peering
    );
}

#[test]
fn stale_primary_authorization_cannot_validate_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Active, 2_002);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active.cluster_epoch(),
            2_003,
        )
        .unwrap();
    assert!(authority
        .validate_pg_operation_authorization(&authorization, 2_004)
        .is_ok());

    let lease_deadline_ms = authorization.primary().lease_deadline_ms();
    let expiry = authority
        .expire_heartbeat_leases(lease_deadline_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(
        authority.snapshot().pg(PgId::new(18)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, lease_deadline_ms),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active.cluster_epoch()
            && current_epoch == expiry.cluster_epoch()
    ));
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            expiry.cluster_epoch(),
            lease_deadline_ms,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, expiry.cluster_epoch(), lease_deadline_ms + 1),
            lease_deadline_ms + 1,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > expiry.cluster_epoch());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, lease_deadline_ms + 2),
            lease_deadline_ms + 2,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            recovery_epoch,
            lease_deadline_ms + 3,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 18,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Peering,
        lease_deadline_ms + 4,
    );
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline_ms + 5,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Active,
        lease_deadline_ms + 6,
    );
    let fresh_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            lease_deadline_ms + 7,
        )
        .unwrap();
    assert_eq!(
        fresh_authorization.cluster_epoch(),
        active_again.cluster_epoch()
    );
    authority
        .validate_pg_operation_authorization(&fresh_authorization, lease_deadline_ms + 8)
        .unwrap();
}

#[test]
fn storage_node_refresh_after_epoch_transition_cannot_keep_stale_active_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let active_valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(active_map.pg_routes()[0].state(), PgState::Active);

    let expiry = authority
        .expire_heartbeat_leases(active_valid_until_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(19)).unwrap().state(),
        PgState::Peering
    );

    let mut stale_active_heartbeat =
        heartbeat_from_record(&authority, 1, active_epoch, active_valid_until_ms + 1);
    stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(stale_active_heartbeat, active_valid_until_ms + 1)
        .unwrap();

    assert!(!refresh.lease().serving());
    assert_eq!(refresh.lease().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        refresh.runtime_map().cluster_epoch(),
        expiry.cluster_epoch()
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Peering
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].primary_lease_deadline_ms(),
        None
    );
    let record = authority.snapshot().node(NodeId::new(1)).unwrap();
    assert_eq!(record.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(record.last_observed_epoch(), Some(active_epoch));
}

#[test]
fn temporary_availability_loss_reactivates_same_primary_before_old_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_003);
    let active_epoch = active.cluster_epoch();
    let active_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_004,
        )
        .unwrap();

    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Suspect)
        .unwrap();
    let suspect_epoch = authority.snapshot().cluster_epoch();
    assert!(suspect_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&active_authorization, 2_005),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == suspect_epoch
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            suspect_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let node_two = authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, suspect_epoch, 2_007),
            2_007,
        )
        .unwrap();
    assert!(node_two.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            suspect_epoch,
            2_008,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 25,
            state: PgState::Peering,
            ..
        })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, suspect_epoch, 2_009),
            2_009,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > suspect_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(25)).unwrap().state(),
        PgState::Peering
    );

    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, 2_010),
            2_010,
        )
        .unwrap()
        .serving());
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_011);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_012);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert_eq!(
        pg.previous_primary_node_incarnation(),
        Some(node_incarnation(&authority, 1))
    );
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_013);
    assert_eq!(
        authority.complete_ready_pg_peerings(2_013).unwrap(),
        vec![PgId::new(25)],
        "the unchanged primary process must not wait out its own old lease"
    );
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_014);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_015,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn recovering_preferred_replica_does_not_displace_live_previous_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Active, 2_002);

    let recovery = heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_003);
    assert!(
        !recovery.serving(),
        "recovering replica must first observe its availability epoch"
    );
    let recovery_epoch = recovery.cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(26)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_006);

    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_004);
    heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_005);
    assert_eq!(authority.snapshot().cluster_epoch(), recovery_epoch);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(26),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_006,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 26,
            node_id: 2,
        })
    ));
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(2_006)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(
        ready[0].primary,
        NodeId::new(1),
        "the exact previous primary must retain priority while its old lease is live"
    );
    assert_eq!(
        authority.complete_ready_pg_peerings(2_006).unwrap(),
        vec![PgId::new(26)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(26))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(1))
    );
}

#[test]
fn acting_set_reorder_moves_primary_after_previous_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_003);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Active, 2_004);

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    let reordered_epoch = authority.snapshot().cluster_epoch();
    let previous_lease_deadline = authority
        .snapshot()
        .pg(PgId::new(27))
        .unwrap()
        .previous_primary_lease_deadline_ms()
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false)
    );

    drop(authority);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > reordered_epoch);
    let pg = authority.snapshot().pg(PgId::new(27)).unwrap();
    assert_eq!(
        pg.previous_primary_lease_deadline_ms(),
        Some(previous_lease_deadline)
    );
    assert_eq!(
        pg.previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false),
        "acting-set transition provenance must survive authority restart"
    );
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_005);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_006);
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline - 1)
        .unwrap()
        .is_empty());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 27,
            node_id: 1,
        })
    ));
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 27, .. })
    ));

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline)
        .unwrap()
        .is_empty());
    heartbeat_with_pg_observation(
        &mut authority,
        1,
        27,
        PgState::Peering,
        previous_lease_deadline,
    );
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        previous_lease_deadline + 1,
    );
    let successor_fence_ms = previous_lease_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, successor_fence_ms);
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        successor_fence_ms + 1,
    );
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(successor_fence_ms + 1)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].primary, NodeId::new(2));
    assert_eq!(
        authority
            .complete_ready_pg_peerings(successor_fence_ms + 1)
            .unwrap(),
        vec![PgId::new(27)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(2))
    );
}

#[test]
fn acting_set_change_fences_old_primary_token_until_new_peering_completes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 77,
        applied_log_hash: 0xabcddcba,
        state_digest: 0x12344321,
    };
    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Active,
        active_proof,
        false,
        2_004,
    );
    let old_epoch = active.cluster_epoch();
    let old_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            old_epoch,
            2_005,
        )
        .unwrap();
    authority
        .validate_pg_operation_authorization(&old_authorization, 2_006)
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > old_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(20)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(20))
            .unwrap()
            .peering_metadata_proof_floor(),
        Some(active_proof)
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&old_authorization, 2_007),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == old_epoch && current_epoch == peering_epoch
    ));
    for node_id in [1, 2] {
        let now_ms = 2_008 + u64::from(node_id);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, node_id, peering_epoch, now_ms),
                now_ms,
            )
            .unwrap();
    }
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            peering_epoch,
            2_011,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            peering_epoch,
            2_012,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));

    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 2_013);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 2_013).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_014,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive {
            pg_id: 20,
            lease_deadline_ms: 2_103,
            ..
        })
    ));
    let mut ready_but_fenced = heartbeat_from_record(&authority, 2, peering_epoch, 2_015);
    ready_but_fenced.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(ready_but_fenced, 2_015).unwrap();
    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(2_016)
        .unwrap()
        .is_empty());
    let mut fence_bridge = heartbeat_from_record(&authority, 2, peering_epoch, 2_103);
    fence_bridge.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(fence_bridge, 2_103).unwrap();
    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_103);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 3_103).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_103,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 20,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == active_proof && actual == PgMetadataProof::empty()
    ));

    let mut node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_104);
    node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(node_two_peering, 3_104).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_104,
        )
        .unwrap();
    let mut new_active_heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_105);
    new_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Active,
        metadata_proof: active_proof,
        pending_metadata_command: None,
    }];
    let new_active = authority.heartbeat(new_active_heartbeat, 3_105).unwrap();
    let new_epoch = new_active.cluster_epoch();
    assert!(new_epoch > peering_epoch);

    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, new_epoch, 3_106),
            3_106,
        )
        .unwrap();
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            new_epoch,
            3_107,
        ),
        Err(ControlPlaneError::NodeNotPgPrimary {
            pg_id: 20,
            node_id: 1,
            primary_node_id: 2,
            ..
        })
    ));

    let new_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            new_epoch,
            3_108,
        )
        .unwrap();
    assert_eq!(new_authorization.primary_node_id(), NodeId::new(2));
    authority
        .validate_pg_operation_authorization(&new_authorization, 2_109)
        .unwrap();
}

#[test]
fn active_metadata_pg_acting_set_change_requires_authoritative_overlap() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(40), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(40),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(40), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 40 })
    ));
    let pg = authority.snapshot().pg(PgId::new(40)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(pg.active_metadata_proof(), Some(active_proof));
}

#[test]
fn active_metadata_migration_waits_for_source_after_unrelated_epoch_change() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let target_pg_id = PgId::new(40);
    let unrelated_pg_id = PgId::new(41);
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            target_pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(unrelated_pg_id, vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(target_pg_id)
        .is_none());
    let before = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 40,
            cluster_epoch,
        }) if cluster_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_003,
    );

    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let target = authority.snapshot().pg(target_pg_id).unwrap();
    assert_eq!(target.state(), PgState::Peering);
    assert_eq!(target.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(target.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn active_metadata_overlap_migration_does_not_relax_non_primary_imported_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_floor = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            45,
            PgState::Peering,
            imported_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(45),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(45)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        45,
        PgState::Active,
        imported_floor,
        false,
        2_020,
    );
    authority
        .set_pg_acting_set(PgId::new(47), vec![NodeId::new(1)])
        .unwrap();

    let epoch_local_progress = PgMetadataProof {
        applied_log_index: imported_floor.applied_log_index,
        applied_log_hash: imported_floor.applied_log_hash + 1,
        state_digest: imported_floor.state_digest + 1,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        45,
        PgState::Active,
        epoch_local_progress,
        false,
        2_021,
    );
    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(45), vec![NodeId::new(2), NodeId::new(3)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 45 })
    ));
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        45,
        PgState::Active,
        epoch_local_progress,
        false,
        2_022,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
        assert_eq!(pg.active_metadata_proof(), Some(epoch_local_progress));
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(epoch_local_progress)
    );

    let imported_high_index_floor = PgMetadataProof {
        applied_log_index: 20,
        applied_log_hash: 30,
        state_digest: 40,
    };
    let primary_destination_progress = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 31,
        state_digest: 41,
    };
    authority
        .set_pg_acting_set(PgId::new(46), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            46,
            PgState::Peering,
            imported_high_index_floor,
            false,
            3_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(46),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(46)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        46,
        PgState::Active,
        imported_high_index_floor,
        false,
        3_020,
    );
    authority
        .set_pg_acting_set(PgId::new(48), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        46,
        PgState::Active,
        primary_destination_progress,
        false,
        3_021,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
        assert_eq!(
            pg.active_metadata_proof(),
            Some(primary_destination_progress)
        );
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(
            PgId::new(46),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(primary_destination_progress)
    );
}

#[test]
fn imported_active_primary_restart_preserves_epoch_local_peering_floor() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 100,
        state_digest: 200,
    };
    authority
        .set_pg_acting_set(PgId::new(49), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(49)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof {
        applied_log_index: imported_proof.applied_log_index,
        applied_log_hash: imported_proof.applied_log_hash + 1,
        state_digest: imported_proof.state_digest + 1,
    };
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    restarting_primary.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(epoch_local_proof));

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 49, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn imported_active_restart_without_initial_observation_accepts_later_epoch_local_peering_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof {
        applied_log_index: 42,
        applied_log_hash: 100,
        state_digest: 200,
    };
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(50)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof {
        applied_log_index: imported_proof.applied_log_index,
        applied_log_hash: imported_proof.applied_log_hash + 1,
        state_digest: imported_proof.state_digest + 1,
    };
    let active_proof_epoch = authority
        .snapshot()
        .pg(PgId::new(50))
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();
    let active_route_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_route_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(
        pg.peering_metadata_proof_floor_epoch(),
        Some(active_proof_epoch)
    );
    assert!(pg.peering_metadata_proof_floor_imported());

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(50),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 50, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn peering_metadata_pg_acting_set_change_preserves_floor_and_requires_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(41),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(41), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 41 })
    ));
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn pg_transition_graph_survives_file_reopen_and_continues() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(61);
    let initial_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_000, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), None);

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_010, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_011,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        initial_proof,
        false,
        (2_012, 5),
    );

    let expected_overlap_floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch();
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor_epoch(),
        expected_overlap_floor_epoch
    );
    for (node_id, now_ms) in [(1, 2_020), (2, 2_021)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    assert_eq!(
        authority.complete_ready_pg_peerings(2_022).unwrap(),
        vec![pg_id]
    );
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_metadata_proof(), Some(initial_proof));
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    for (node_id, now_ms) in [(1, 2_030), (2, 2_031)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    authority.complete_ready_pg_peerings(2_032).unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    let source_primary = active_pg.active_primary().unwrap();
    let source_proof = active_pg.active_metadata_proof().unwrap();
    let imported_proof = PgMetadataProof {
        applied_log_index: source_proof.applied_log_index + 1,
        applied_log_hash: source_proof.applied_log_hash + 100,
        state_digest: source_proof.state_digest + 100,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(3)], transfer)
        .unwrap();
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.acting_set(), &[NodeId::new(3)]);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_node_id(),
        Some(source_primary)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor_epoch(),
        Some(active_epoch)
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (2_040, 5),
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (3_035, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_035,
        )
        .unwrap();
    let imported_active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(imported_active_pg.state(), PgState::Active);
    assert_eq!(
        imported_active_pg.active_metadata_proof(),
        Some(imported_proof)
    );
    assert!(imported_active_pg.active_metadata_transfer_imported());
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_import_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_import_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_import_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(restarted_import_pg.peering_metadata_proof_floor_imported());
    assert_eq!(restarted_import_pg.peering_metadata_transfer(), None);
    heartbeat_with_pg_proof(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_050,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_051,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn metadata_transfer_allows_explicit_non_overlap_pg_migration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        (2_000, 3),
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        (2_002, 1),
    );
    let active_epoch = authority.snapshot().cluster_epoch();

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(42), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));

    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        PgMetadataProof {
            applied_log_index: active_proof.applied_log_index,
            applied_log_hash: active_proof.applied_log_hash + 1,
            state_digest: active_proof.state_digest,
        },
        PgMetadataProof {
            applied_log_index: active_proof.applied_log_index,
            applied_log_hash: active_proof.applied_log_hash + 10,
            state_digest: active_proof.state_digest,
        },
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let future_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let future_transfer = PgMetadataTransferProof::new(future_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            future_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochInFuture { pg_id: 42, .. })
    ));

    let stale_epoch = ClusterEpoch::new(active_epoch.get() - 1).unwrap();
    let stale_epoch_transfer = PgMetadataTransferProof::new(stale_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_epoch_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochStale { pg_id: 42, .. })
    ));

    let imported_proof = PgMetadataProof {
        applied_log_index: active_proof.applied_log_index,
        applied_log_hash: active_proof.applied_log_hash + 100,
        state_digest: active_proof.state_digest,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let snapshot_without_transfer = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(1)],
            transfer,
            next_epoch(active_epoch).unwrap(),
        ),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));
    assert_eq!(authority.snapshot(), &snapshot_without_transfer);
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
    assert!(!pg.metadata_transfer_fenced());
    let mismatched_same_acting_set = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: imported_proof.applied_log_index,
            applied_log_hash: imported_proof.applied_log_hash + 1,
            state_digest: imported_proof.state_digest,
        },
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            mismatched_same_acting_set,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofMismatch { pg_id: 42, .. })
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    let mismatched_destination_epoch = next_epoch(peering_epoch).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            mismatched_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 42,
            expected_destination_epoch,
            actual_destination_epoch,
        }) if expected_destination_epoch == mismatched_destination_epoch
            && actual_destination_epoch == peering_epoch
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            peering_epoch,
        )
        .unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    let transfer_epoch = authority.snapshot().cluster_epoch();
    assert_eq!(
        authority
            .fence_pg_for_metadata_transfer(PgId::new(42))
            .unwrap()
            .cluster_epoch(),
        transfer_epoch
    );
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert!(!pg.metadata_transfer_fenced());

    let restarted = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-first-restart.state"),
    );
    let restarted_pg = restarted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), Some(transfer));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        3_003,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_003,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            ..
        }) if cluster_epoch == peering_epoch
    ));

    let stale_source_above_imported = PgMetadataProof {
        applied_log_index: imported_proof.applied_log_index + 10,
        applied_log_hash: imported_proof.applied_log_hash + 10,
        state_digest: imported_proof.state_digest + 10,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        stale_source_above_imported,
        false,
        3_004,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_004,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            expected,
            actual,
            ..
        }) if cluster_epoch == peering_epoch
            && expected == imported_proof
            && actual == stale_source_above_imported
    ));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        3_005,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_006,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(2)));
    assert_eq!(active_pg.active_metadata_proof(), Some(imported_proof));
    assert!(active_pg.active_metadata_transfer_imported());
    assert_eq!(active_pg.peering_metadata_proof_floor(), None);
    assert_eq!(active_pg.peering_metadata_transfer(), None);

    let restarted_active = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-active-restart.state"),
    );
    let restarted_active_pg = restarted_active.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(restarted_active_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_active_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_active_pg.peering_metadata_transfer(), None);
    assert!(!restarted_active_pg.metadata_transfer_fenced());
    assert!(!restarted_active_pg.active_metadata_transfer_imported());

    let epoch_local_source_proof = PgMetadataProof {
        applied_log_index: imported_proof.applied_log_index,
        applied_log_hash: imported_proof.applied_log_hash + 200,
        state_digest: imported_proof.state_digest + 1,
    };
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Active,
        imported_proof,
        false,
        2_007,
    );
    let repeated_source_epoch = authority.snapshot().cluster_epoch();
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(fenced_pg.state(), PgState::Peering);
    assert!(fenced_pg.metadata_transfer_fence_source_imported);
    assert_eq!(
        fenced_pg.metadata_transfer_fence_epoch(),
        Some(fenced_epoch)
    );
    assert_eq!(
        fenced_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    let repeated_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        repeated_source_epoch,
        epoch_local_source_proof,
        PgMetadataProof {
            applied_log_index: epoch_local_source_proof.applied_log_index,
            applied_log_hash: epoch_local_source_proof.applied_log_hash + 100,
            state_digest: epoch_local_source_proof.state_digest,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(1)],
            repeated_transfer,
        )
        .unwrap();
    let repeated_transfer_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(repeated_transfer_pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        repeated_transfer_pg.peering_metadata_transfer(),
        Some(repeated_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_retry_without_stored_deadline_uses_max_source_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Active,
        active_proof,
        false,
        2_020,
    );

    authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();
    let record = authority
        .snapshot
        .pgs
        .get_mut(&PgId::new(50))
        .expect("test PG should exist");
    assert!(record.metadata_transfer_fenced);
    record.metadata_transfer_fence_source_lease_deadline_ms = None;
    persist_manually_modified_test_snapshot(&mut authority);

    let retry = authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();

    assert_eq!(retry.source_primary_lease_deadline_ms(), Some(2_120));
}

#[test]
fn fencing_pending_recovery_peering_preserves_imported_source_provenance() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(52);
    let source_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        source_proof,
        false,
        2_002,
    );

    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let imported_proof = PgMetadataProof {
        applied_log_index: 3,
        applied_log_hash: 20,
        state_digest: 21,
    };
    let first_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        authority.snapshot().cluster_epoch(),
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], first_transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        2_010,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_200,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_201,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_transfer_imported());

    let local_progress = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 30,
        state_digest: 31,
    };
    let pending = test_pending_metadata_command(active_epoch);
    let mut pending_heartbeat = heartbeat_from_record(&authority, 2, active_epoch, 3_220);
    pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Active,
        metadata_proof: local_progress,
        pending_metadata_command: Some(pending),
    }];
    authority.heartbeat(pending_heartbeat, 3_220).unwrap();
    let recovery_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(recovering.state(), PgState::Peering);
    assert_eq!(
        recovering.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(recovering.peering_metadata_proof_floor_imported());

    let mut cleared_heartbeat = heartbeat_from_record(&authority, 2, recovery_epoch, 3_221);
    cleared_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Peering,
        metadata_proof: local_progress,
        pending_metadata_command: None,
    }];
    authority.heartbeat(cleared_heartbeat, 3_221).unwrap();
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fenced = authority.snapshot().pg(pg_id).unwrap();
    assert!(fenced.metadata_transfer_fence_source_imported);

    let stale_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let stale_second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof {
            applied_log_index: 2,
            applied_log_hash: stale_destination_epoch.get(),
            state_digest: local_progress.state_digest,
        },
    );
    authority
        .set_pg_acting_set(PgId::new(53), vec![NodeId::new(1)])
        .unwrap();
    let snapshot_after_unrelated_advance = authority.snapshot().clone();
    let actual_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            stale_second_transfer,
            stale_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 52,
            expected_destination_epoch,
            actual_destination_epoch: actual,
        }) if expected_destination_epoch == stale_destination_epoch
            && actual == actual_destination_epoch
    ));
    assert_eq!(authority.snapshot(), &snapshot_after_unrelated_advance);

    let second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof {
            applied_log_index: 2,
            applied_log_hash: actual_destination_epoch.get(),
            state_digest: local_progress.state_digest,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            second_transfer,
            actual_destination_epoch,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().cluster_epoch(),
        actual_destination_epoch
    );
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        transferred.peering_metadata_transfer(),
        Some(second_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_rejects_epoch_local_source_proof_without_imported_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_activation_floor = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    let epoch_local_source_proof = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 12,
        state_digest: 13,
    };
    for (idx, pg_id) in [42, 43].into_iter().enumerate() {
        let base_ms = 1_990 + (idx as u64 * 100);
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Peering,
            imported_activation_floor,
            false,
            base_ms + 10,
        );
        authority
            .complete_pg_peering(
                PgId::new(pg_id),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                base_ms + 20,
            )
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Active,
            imported_activation_floor,
            false,
            base_ms + 30,
        );
    }

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        imported_activation_floor,
        false,
        2_180,
    );
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    assert!(
        !authority
            .snapshot()
            .pg(PgId::new(42))
            .unwrap()
            .metadata_transfer_fence_source_imported
    );
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof {
            applied_log_index: 3,
            applied_log_hash: 14,
            state_digest: 15,
        },
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            fenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    authority
        .set_pg_state(PgId::new(43), PgState::Peering)
        .unwrap();
    let unfenced_epoch = authority.snapshot().cluster_epoch();
    let unfenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        unfenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof {
            applied_log_index: 3,
            applied_log_hash: 16,
            state_digest: 17,
        },
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(43),
            vec![NodeId::new(2)],
            unfenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 43, .. })
    ));
}

#[test]
fn fenced_metadata_transfer_accepts_later_prefence_epoch_local_source_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(42);
    let floor = PgMetadataProof {
        applied_log_index: 3,
        applied_log_hash: 9_745,
        state_digest: 14_796,
    };
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        floor,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_002,
    );
    let floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    assert!(source_epoch > floor_epoch);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_003,
    );
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fence_epoch = authority.snapshot().cluster_epoch();
    assert!(fence_epoch > source_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );

    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    let post_fence_epoch = authority.snapshot().cluster_epoch();
    assert!(post_fence_epoch > fence_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);

    let source_proof = PgMetadataProof {
        applied_log_index: 2,
        applied_log_hash: 71_284,
        state_digest: 19_648,
    };
    let imported_proof = PgMetadataProof {
        applied_log_index: source_proof.applied_log_index,
        applied_log_hash: 82_951,
        state_digest: source_proof.state_digest,
    };
    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        floor_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let at_fence_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fence_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            at_fence_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], transfer)
        .unwrap();
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.state(), PgState::Peering);
    assert_eq!(transferred.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transferred.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    let reopened = reopen_file_authority(&store);
    assert_eq!(
        reopened
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .peering_metadata_transfer(),
        Some(transfer)
    );
}

#[test]
fn fenced_metadata_transfer_accepts_prefence_source_epoch_with_floor_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let source_proof = PgMetadataProof {
        applied_log_index: 3,
        applied_log_hash: 9_474,
        state_digest: 15_725,
    };
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        source_proof,
        false,
        2_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();

    authority
        .fence_pg_for_metadata_transfer(PgId::new(44))
        .unwrap();
    assert!(source_epoch < authority.snapshot().cluster_epoch());
    let imported_proof = PgMetadataProof {
        applied_log_index: 3,
        applied_log_hash: 49_281,
        state_digest: source_proof.state_digest,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(44), vec![NodeId::new(2)], transfer)
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(44)).unwrap();
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
}

#[test]
fn complete_pg_peering_requires_every_acting_node_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_001).serving());
    authority
        .set_pg_acting_set(PgId::new(39), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    assert!(heartbeat_until_serving(&mut authority, 2, 1_999).serving());
    heartbeat_with_pg_observation(&mut authority, 1, 39, PgState::Peering, 2_000);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 39,
            node_id: 2,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 2, 39, PgState::Peering, 2_002);
    authority
        .complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_003,
        )
        .unwrap();
}

#[test]
fn acting_set_change_discards_stale_peering_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(38), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let first_peering_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_observation(&mut authority, 1, 38, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 38, PgState::Peering, 2_001);

    authority
        .set_pg_acting_set(
            PgId::new(38),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let changed_epoch = authority.snapshot().cluster_epoch();
    assert!(changed_epoch > first_peering_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == changed_epoch
    ));
    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, changed_epoch, 2_003),
            2_003,
        )
        .unwrap()
        .serving());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 38,
            cluster_epoch,
            ..
        }) if cluster_epoch == changed_epoch
    ));

    for (node_id, now_ms) in [(1, 2_005), (2, 2_006), (3, 2_007)] {
        heartbeat_with_pg_observation(&mut authority, node_id, 38, PgState::Peering, now_ms);
    }
    authority
        .complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_008,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn membership_change_to_joining_forces_active_pg_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_003,
        )
        .unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Joining)
        .unwrap();
    let joining_epoch = authority.snapshot().cluster_epoch();
    assert!(joining_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(23)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 2_004),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == joining_epoch
    ));

    let joining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, joining_epoch, 2_005),
            2_005,
        )
        .unwrap();
    assert!(!joining_lease.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            joining_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == joining_epoch
    ));
}

#[test]
fn membership_change_to_draining_forces_repeering_before_service() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Draining)
        .unwrap();
    let draining_epoch = authority.snapshot().cluster_epoch();
    assert!(draining_epoch > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(24)).unwrap().state(),
        PgState::Peering
    );
    let draining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, draining_epoch, 2_003),
            2_003,
        )
        .unwrap();
    assert!(
        draining_lease.serving(),
        "draining nodes can still serve after observing the new map"
    );
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            draining_epoch,
            2_004,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 24,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_005);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_006,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_007);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_008,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn failed_expiry_persist_does_not_expose_uncommitted_epoch_or_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(12), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 12, 1_000).serving());
    let committed = authority.snapshot().clone();
    assert_eq!(
        committed.node(NodeId::new(12)).unwrap().availability(),
        NodeAvailabilityState::Healthy
    );

    let failing_store = FailingStore::new(committed.clone());
    let mut restarted = SingleAuthorityControlPlane::open(failing_store).unwrap();
    assert!(restarted
        .heartbeat(
            heartbeat_from_record(&restarted, 12, restarted.snapshot().cluster_epoch(), 1_001,),
            1_001
        )
        .unwrap()
        .serving());
    let visible_before_failure = restarted.snapshot().clone();
    restarted.store.fail_saves();
    assert!(matches!(
        restarted.expire_heartbeat_leases(1_101),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "test save failure"
    ));
    assert_eq!(restarted.snapshot(), &visible_before_failure);
    assert_eq!(
        restarted.deterministic_pg_primary(PgId::new(1), &[NodeId::new(12)], 1_001),
        Some(NodeId::new(12))
    );
}

#[test]
fn heartbeat_rejects_unknown_removed_and_zero_duration_nodes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, ClusterEpoch::INITIAL, 1), 1),
        Err(ControlPlaneError::UnknownNode { node_id: 9 })
    ));

    authority
        .set_node_membership(NodeId::new(9), NodeMembershipState::Removed)
        .unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, authority.snapshot().cluster_epoch(), 2), 2),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 9, .. })
    ));

    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();
    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = 0;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::InvalidLeaseDuration)
    ));
}

#[test]
fn heartbeat_rejects_overlong_lease_duration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();

    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::LeaseDurationTooLong {
            requested_ms,
            max_ms,
        }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
            && max_ms == MAX_HEARTBEAT_LEASE_MS
    ));
}

#[test]
fn primary_selection_requires_unexpired_authority_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(42), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 42, 1_000);
    assert!(serving.serving());
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms() - 1,
        ),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms(),
        ),
        None
    );

    authority
        .set_pg_acting_set(PgId::new(21), vec![NodeId::new(42)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Peering, 1_010);
    authority
        .complete_pg_peering(
            PgId::new(21),
            NodeId::new(42),
            node_incarnation(&authority, 42),
            1_020,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Active, 1_030);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms() - 1),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms()),
        None
    );
}

#[test]
fn removed_nodes_cannot_rejoin_or_be_marked_healthy() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Removed)
        .unwrap();

    assert!(matches!(
        authority.set_node_membership(NodeId::new(11), NodeMembershipState::Active),
        Err(ControlPlaneError::RemovedNodeCannotRejoin { node_id: 11 })
    ));
    assert!(matches!(
        authority.mark_node_availability(NodeId::new(11), NodeAvailabilityState::Healthy),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 11, .. })
    ));
}
