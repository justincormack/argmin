// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    AbortStreamUploadCommand, BucketWriteReservationProof, CreateBucketCommand,
    DeleteFinalizedBucketCommand, MarkBucketDeletingCommand, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
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
fn control_plane_rpc_v14_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 14, 0, 12, 0, 0, 0, 3, 75, 136, 143, 73, 152, 40, 182, 151, 1, 2,
        3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 14"
    ));
}

#[test]
fn control_plane_rpc_v15_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 15, 0, 12, 0, 0, 0, 3, 25, 251, 193, 234, 127, 14, 74, 195, 1, 2,
        3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 15"
    ));
}

#[test]
fn control_plane_rpc_v16_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 16, 0, 12, 0, 0, 0, 3, 168, 219, 98, 75, 82, 215, 243, 245, 1, 2,
        3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 16"
    ));
}

#[test]
fn control_plane_rpc_v17_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 17, 0, 12, 0, 0, 0, 3, 250, 168, 44, 232, 181, 241, 15, 161, 1,
        2, 3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 17"
    ));
}

#[test]
fn control_plane_rpc_v18_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 18, 0, 12, 0, 0, 0, 3, 12, 61, 255, 12, 156, 154, 11, 93, 1, 2,
        3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 18"
    ));
}

#[test]
fn control_plane_rpc_v20_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 20, 0, 12, 0, 0, 0, 3, 213, 207, 126, 151, 150, 219, 145, 206, 1,
        2, 3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 20"
    ));
}

#[test]
fn control_plane_rpc_v21_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 21, 0, 12, 0, 0, 0, 3, 135, 188, 48, 52, 113, 253, 109, 154, 1,
        2, 3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 21"
    ));
}

#[test]
fn control_plane_rpc_v22_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 22, 0, 12, 0, 0, 0, 3, 113, 41, 227, 208, 88, 150, 105, 102, 1,
        2, 3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 22"
    ));
}

#[test]
fn control_plane_rpc_v23_frame_remains_rejected_evidence() {
    const FRAME: &[u8] = &[
        97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97, 110,
        101, 45, 114, 112, 99, 0, 23, 0, 12, 0, 0, 0, 3, 35, 90, 173, 115, 191, 176, 149, 50, 1, 2,
        3,
    ];
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(FRAME)).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "unsupported control-plane RPC version 23"
    ));
}

#[test]
fn control_plane_rpc_v24_frame_encoding_is_exact() {
    let frame =
        encode_control_plane_rpc_frame(ControlPlaneRpcKind::RuntimeMapStatus, &[0x01, 0x02, 0x03])
            .unwrap();

    assert_eq!(
        frame,
        [
            97, 114, 103, 109, 105, 110, 45, 99, 111, 110, 116, 114, 111, 108, 45, 112, 108, 97,
            110, 101, 45, 114, 112, 99, 0, 24, 0, 12, 0, 0, 0, 3, 82, 243, 91, 242, 218, 207, 55,
            131, 1, 2, 3,
        ]
    );
}

#[test]
fn control_plane_rpc_v23_rejects_retired_singular_install_kind() {
    const RETIRED_INSTALL_KIND: u16 = 19;
    let payload = [0x01, 0x02, 0x03];
    let payload_len = u32::try_from(payload.len()).unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(CONTROL_PLANE_RPC_MAGIC);
    write_u16(&mut frame, CONTROL_PLANE_RPC_VERSION);
    write_u16(&mut frame, RETIRED_INSTALL_KIND);
    write_u32(&mut frame, payload_len);
    write_u64(
        &mut frame,
        control_plane_rpc_frame_checksum(
            CONTROL_PLANE_RPC_VERSION,
            RETIRED_INSTALL_KIND,
            payload_len,
            &payload,
        ),
    );
    frame.extend_from_slice(&payload);

    assert!(matches!(
        read_control_plane_rpc_frame(&mut std::io::Cursor::new(frame)),
        Err(ControlPlaneError::RpcProtocol { diagnostic })
            if diagnostic.as_str() == "unknown control-plane RPC kind 19"
    ));
}

#[test]
fn control_plane_rpc_frame_marker_failures_are_typed() {
    for truncated in [
        &[][..],
        &CONTROL_PLANE_RPC_MAGIC[..CONTROL_PLANE_RPC_MAGIC.len() - 1],
        CONTROL_PLANE_RPC_MAGIC,
    ] {
        assert_eq!(
            validate_control_plane_rpc_frame_marker(truncated),
            Err(ControlPlaneRpcFrameFormatError::Truncated)
        );
    }

    let mut unknown_magic = Vec::from(CONTROL_PLANE_RPC_MAGIC);
    unknown_magic[0] ^= 1;
    unknown_magic.extend_from_slice(&CONTROL_PLANE_RPC_VERSION.to_be_bytes());
    assert_eq!(
        validate_control_plane_rpc_frame_marker(&unknown_magic),
        Err(ControlPlaneRpcFrameFormatError::UnknownMagic)
    );

    for version in [
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        CONTROL_PLANE_RPC_VERSION + 1,
    ] {
        let mut unsupported = Vec::from(CONTROL_PLANE_RPC_MAGIC);
        unsupported.extend_from_slice(&version.to_be_bytes());
        assert_eq!(
            validate_control_plane_rpc_frame_marker(&unsupported),
            Err(ControlPlaneRpcFrameFormatError::UnsupportedVersion(version))
        );
    }
}

#[test]
fn control_plane_rpc_server_rejects_other_versions_before_admission_or_dispatch() {
    let directory = test_util::tempdir();
    let socket_path = directory.path().join("control-plane.sock");
    let listener = ControlPlaneRpcServerListener::unix(
        UnixListener::bind(&socket_path).unwrap(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
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
        Ok(())
    }));
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            directory.path().join("control.state"),
        ))
        .unwrap(),
    ));
    authority
        .lock()
        .unwrap()
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.lock().unwrap().snapshot().clone();
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();

    for version in [CONTROL_PLANE_RPC_VERSION - 1, CONTROL_PLANE_RPC_VERSION + 1] {
        let frame = encode_control_plane_rpc_frame_with_version(
            ControlPlaneRpcKind::SetPgActingSet,
            &payload,
            version,
        )
        .unwrap();
        let socket_path = socket_path.clone();
        let client = std::thread::spawn(move || {
            let mut stream = UnixStream::connect(socket_path).unwrap();
            stream.write_all(&frame).unwrap();
        });

        listener
            .accept_one(
                &|| ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
                &policy,
            )
            .unwrap();
        client.join().unwrap();
        wait_for_control_plane_server_workers_to_finish(&policy);

        assert_eq!(confirmation_calls.load(Ordering::Acquire), 0);
        assert_eq!(authority.lock().unwrap().snapshot(), &before);
    }
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
            cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
            cluster_map_history_route_references: Default::default(),
            pg_observations: Vec::new(),
        },
        Some(2_000),
        Some(3_000),
    );

    assert!(matches!(
        authenticate_and_admit_control_plane_rpc(request, &policy, false, true, 2_500),
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
fn control_plane_rpc_current_and_adjacent_alpn_profiles_are_negotiated_exactly() {
    let endpoint = control_plane_test_tls_endpoint("127.0.0.1:1".parse().unwrap());
    let ControlPlaneRpcClientEndpointKind::TlsTcp {
        tls_client_config, ..
    } = endpoint.0
    else {
        panic!("TLS/TCP constructor returned a Unix endpoint")
    };
    let listener = ControlPlaneRpcServerListener::tls_tcp(
        TcpListener::bind("127.0.0.1:0").unwrap(),
        control_plane_test_tls_certified_key(),
        1,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
        Duration::from_secs(1),
    )
    .unwrap();
    let ControlPlaneRpcServerListenerKind::TlsTcp {
        tls_server_config, ..
    } = listener.kind
    else {
        panic!("TLS/TCP constructor returned a Unix listener")
    };

    crate::internal_tls_protocol::assert_current_and_adjacent_profile_negotiation(
        InternalTlsProtocol::ControlPlaneRpc,
        tls_client_config,
        tls_server_config,
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

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "truncated control-plane RPC frame marker"
    ));
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
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "truncated control-plane RPC frame marker"
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
fn runtime_map_current_state_digest_v4_is_stable() {
    let control_snapshot = canonical_snapshot_with_node();
    let runtime_map = rpc::runtime_map_test_snapshot_with_active_route();

    assert_eq!(
        runtime_map_current_state_digest(&control_snapshot, runtime_map.pg_routes()).as_bytes(),
        [
            80, 218, 200, 242, 185, 195, 41, 114, 198, 162, 33, 132, 151, 163, 175, 7, 198, 170,
            85, 0, 57, 226, 178, 255, 0, 199, 69, 98, 246, 50, 229, 152,
        ]
    );
}

#[test]
fn volatile_lease_promotion_makes_acting_set_fence_durable() {
    let authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let pg_id = PgId::new(9);
    let proof = PgMetadataProof::current(1, 2, 3);
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
fn control_plane_state_version_failures_are_typed_before_state_construction() {
    assert_eq!(
        require_current_control_plane_state_version(None),
        Err(ControlPlaneStateVersionError::Missing)
    );
    for version in [
        28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 45,
    ] {
        assert_eq!(
            require_current_control_plane_state_version(Some(version)),
            Err(ControlPlaneStateVersionError::Unsupported(version))
        );
    }
    assert_eq!(
        require_current_control_plane_state_version(Some(44)),
        Ok(44)
    );

    assert!(matches!(
        parse_snapshot("authority_incarnation=1\ncluster_epoch=1\n"),
        Err(ControlPlaneError::Parse { line: 0, message })
            if message == "missing control-plane state version"
    ));
}

#[test]
fn canonical_control_plane_state_v28_text_remains_rejected_evidence() {
    const STATE_V28: &str = concat!(
        "version=28\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
    );
    assert_eq!(
        (
            STATE_V28.len(),
            hex_encode(&checksum::sha256::digest(STATE_V28.as_bytes()))
        ),
        (
            183,
            "b9ab9e86a71477344a48d5923bf40ac1742d5ae85060f225f2ec0a55eca2838e".to_owned()
        )
    );
    assert!(matches!(
        parse_snapshot(STATE_V28),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 28"
    ));
}

#[test]
fn canonical_control_plane_state_v28_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v28_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            3_596,
            "4ae025e955d70386a92c8814ed18855f6c7374f62852342feb6814181edbaba4".to_owned()
        )
    );

    let before = canonical_snapshot_with_node();
    let mut remaining = AGGREGATE;
    let mut count = 0usize;
    while !remaining.is_empty() {
        let (raw_len, tail) = remaining.split_at(8);
        let len = usize::try_from(u64::from_be_bytes(raw_len.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(len);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(snapshot.starts_with("version=28\n"));
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 28"
        ));
        assert_eq!(before, canonical_snapshot_with_node());
        remaining = tail;
        count += 1;
    }
    assert!(
        count > 1,
        "representative v28 aggregate must contain a corpus"
    );
}

#[test]
fn canonical_control_plane_state_v29_text_remains_rejected_evidence() {
    const STATE_V29: &str = concat!(
        "version=29\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert_eq!(
        (
            STATE_V29.len(),
            hex_encode(&checksum::sha256::digest(STATE_V29.as_bytes()))
        ),
        (
            185,
            "14ce3a9b2971dbf7935fcbb77c281ee140d3bb2515be01a4d63feac39bba5b46".to_owned()
        )
    );
    assert!(matches!(
        parse_snapshot(STATE_V29),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 29"
    ));
}

#[test]
fn canonical_control_plane_state_v29_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v29_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            4_685,
            "eb50cb06a9ddec679a03e67fdf4a079395fc886b2ae5a328488f2b90c52efeac".to_owned()
        )
    );

    let before = canonical_snapshot_with_node();
    let mut remaining = AGGREGATE;
    let mut count = 0usize;
    while !remaining.is_empty() {
        let (raw_len, tail) = remaining.split_at(8);
        let len = usize::try_from(u64::from_be_bytes(raw_len.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(len);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(snapshot.starts_with("version=29\n"));
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 29"
        ));
        assert_eq!(before, canonical_snapshot_with_node());
        remaining = tail;
        count += 1;
    }
    assert!(
        count > 1,
        "representative v29 aggregate must contain a corpus"
    );
}

#[test]
fn canonical_control_plane_state_v30_text_remains_rejected_evidence() {
    const V30: &str = concat!(
        "version=30\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V30),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 30"
    ));
}

#[test]
fn canonical_control_plane_state_v31_text_remains_rejected_evidence() {
    const V31: &str = concat!(
        "version=31\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V31),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 31"
    ));
}

#[test]
fn canonical_control_plane_state_v32_text_remains_rejected_evidence() {
    const V32: &str = concat!(
        "version=32\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V32),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 32"
    ));
}

#[test]
fn canonical_control_plane_state_v35_text_remains_rejected_evidence() {
    const V35: &str = concat!(
        "version=35\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V35),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 35"
    ));
}

#[test]
fn canonical_control_plane_state_v36_text_remains_rejected_evidence() {
    const V36: &str = concat!(
        "version=36\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V36),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 36"
    ));
}

#[test]
fn canonical_control_plane_state_v37_text_remains_rejected_evidence() {
    const V37: &str = concat!(
        "version=37\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V37),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 37"
    ));
}

#[test]
fn canonical_control_plane_state_v38_text_remains_rejected_evidence() {
    const V38: &str = concat!(
        "version=38\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V38),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 38"
    ));
}

#[test]
fn canonical_control_plane_state_v39_text_remains_rejected_evidence() {
    const V39: &str = concat!(
        "version=39\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V39),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 39"
    ));
}

#[test]
fn canonical_control_plane_state_v40_text_remains_rejected_evidence() {
    const V40: &str = concat!(
        "version=40\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V40),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 40"
    ));
}

#[test]
fn canonical_control_plane_state_v41_text_remains_rejected_evidence() {
    const V41: &str = concat!(
        "version=41\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V41),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 41"
    ));
}

#[test]
fn canonical_control_plane_state_v42_text_remains_rejected_evidence() {
    const V42: &str = concat!(
        "version=42\n",
        "authority_incarnation=1\n",
        "cluster_epoch=1\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=123\n",
        "lease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
    );
    assert!(matches!(
        parse_snapshot(V42),
        Err(ControlPlaneError::Parse { line: 1, message })
            if message == "unsupported control-plane state version 42"
    ));
}

#[test]
fn canonical_control_plane_state_v43_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v43_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            130_078,
            "9434b2ae07e55307b850ec4c2a98998d33e9994a375552170c3dc6de611bf325".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        assert!(matches!(
            parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 43"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v43 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v44_text_is_exact() {
    assert_eq!(
        format_snapshot(&canonical_snapshot_with_node()),
        concat!(
            "version=44\n",
            "authority_incarnation=1\n",
            "cluster_epoch=1\n",
            "initial_topology=-\n",
            "max_committed_timestamp_ms=123\n",
            "lease_grant_horizon=-\n",
            "node=1,active,1,healthy,11,1,100,200,-,6e6f64652d312e736f636b\n",
        )
    );
}

#[test]
fn canonical_control_plane_state_v30_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v30_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            5_199,
            "4e6e430bb64dad8a2048a09be56988943442f0f7d50d9eec6b286ab76d22938d".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 30"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v30 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v31_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v31_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            11_009,
            "2832390e31e23a98c527fb924163e738f39d89e3cfcd590480527297fafc2db8".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 31"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v31 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v32_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v32_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            11_535,
            "5cd2fa31798c17a4d55e337ee457171c19b432517274e1b2b2a0b1e430ad0999".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 32"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v32 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v33_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v33_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            11_535,
            "d45b799299bbaa0ec21f95aa7f8191c46f8b38682aaa57b57c0baaf625660360".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        let snapshot = std::str::from_utf8(snapshot).unwrap();
        assert!(matches!(
            parse_snapshot(snapshot),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 33"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v33 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v36_representative_aggregate_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("testdata/state_v36_representative.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            38_995,
            "2eedd2196545416805fdd55f11d193d7819340a369edc15187113fb73f64e9a1".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        assert!(matches!(
            parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 36"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > 1, "v36 aggregate must contain a corpus");
}

#[test]
fn canonical_control_plane_state_v44_representative_aggregate_is_stable() {
    let mut snapshots = vec![canonical_snapshot_with_node()];

    let certified_nodes = vec![
        (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
        (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
        (NodeId::new(3), "/tmp/node-3.sock".to_owned()),
    ];
    let certified_pgs = vec![
        (PgId::new(7), vec![NodeId::new(1), NodeId::new(2)]),
        (PgId::new(9), vec![NodeId::new(2), NodeId::new(3)]),
    ];
    let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        9,
        [0x3c; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        vec![101, 102, 103],
        &certified_nodes,
        &certified_pgs,
        test_certified_storage_placement_policy((1..=3).map(NodeId::new), 2, 50),
    )
    .unwrap();
    let certified = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: certified_nodes,
            pg_acting_sets: certified_pgs,
            topology,
        })
        .unwrap()
        .into_snapshot();
    snapshots.push(certified.clone());

    let unavailable_tmp = test_util::tempdir();
    let unavailable_store =
        FileControlPlaneStore::new(unavailable_tmp.path().join("unavailable-transition.state"));
    unavailable_store.checkpoint(None, &certified).unwrap();
    let mut unavailable_authority = SingleAuthorityControlPlane::open(unavailable_store).unwrap();
    for node_id in 1..=3 {
        let now_ms = 6_000 + u64::from(node_id);
        let mut request = heartbeat(
            node_id,
            unavailable_authority.snapshot().cluster_epoch(),
            now_ms,
        );
        request.endpoint = format!("/tmp/node-{node_id}.sock");
        request.requested_lease_duration_ms = 10_000;
        unavailable_authority.heartbeat(request, now_ms).unwrap();
        let mut request = heartbeat_from_record(
            &unavailable_authority,
            node_id,
            unavailable_authority.snapshot().cluster_epoch(),
            now_ms + 1,
        );
        request.requested_lease_duration_ms = 10_000;
        unavailable_authority
            .heartbeat(request, now_ms + 1)
            .unwrap();
    }
    let unavailable_transition_proof = PgMetadataProof::current(31, 32, 33);
    for node_id in 1..=2 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut unavailable_authority,
            node_id,
            7,
            PgState::Peering,
            unavailable_transition_proof,
            false,
            (6_100 + u64::from(node_id), 10_000),
        );
    }
    unavailable_authority
        .complete_pg_peering(
            PgId::new(7),
            NodeId::new(1),
            node_incarnation(&unavailable_authority, 1),
            6_103,
        )
        .unwrap();
    for node_id in 1..=2 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut unavailable_authority,
            node_id,
            7,
            PgState::Active,
            unavailable_transition_proof,
            false,
            (
                6_110 + u64::from(node_id),
                if node_id == 1 { 100 } else { 10_000 },
            ),
        );
    }
    let unavailable_deadline_ms = unavailable_authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    unavailable_authority
        .expire_heartbeat_leases(unavailable_deadline_ms)
        .unwrap();
    for _ in 0..2 {
        for node_id in 2..=3 {
            let now_ms = unavailable_authority
                .snapshot()
                .max_committed_timestamp_ms()
                .unwrap()
                + 1;
            let mut request = heartbeat_from_record(
                &unavailable_authority,
                node_id,
                unavailable_authority.snapshot().cluster_epoch(),
                now_ms,
            );
            request.requested_lease_duration_ms = 10_000;
            unavailable_authority.heartbeat(request, now_ms).unwrap();
        }
    }
    let source_observed_at_ms = unavailable_authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap()
        + 1;
    heartbeat_with_pg_proof_and_lease_duration(
        &mut unavailable_authority,
        2,
        7,
        PgState::Peering,
        unavailable_transition_proof,
        false,
        (source_observed_at_ms, 10_000),
    );
    let begin_at_ms = unavailable_authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 50;
    unavailable_authority
        .begin_unavailable_pg_placement_transition(PgId::new(7), NodeId::new(1), begin_at_ms)
        .unwrap();
    let work = unavailable_authority
        .poll_unavailable_pg_reconciliation(
            &mut UnavailablePgReconciliationCursor::start(),
            begin_at_ms,
        )
        .unwrap()
        .expect("new unavailable transition remains recoverable");
    snapshots.push(unavailable_authority.snapshot().clone());
    let authorization = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: work.mutation_binding().clone(),
        staging_generation: work.transition_epoch().get(),
        artifact_target_epoch: ClusterEpoch::new(
            unavailable_authority.snapshot().cluster_epoch().get() + 1,
        )
        .unwrap(),
        artifact_digest: [0x7a; 32],
        artifact_length: 8_192,
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    unavailable_authority
        .authorize_unavailable_pg_staging_intents_batch(std::slice::from_ref(&authorization))
        .unwrap();
    snapshots.push(unavailable_authority.snapshot().clone());
    let destination_epoch =
        ClusterEpoch::new(unavailable_authority.snapshot().cluster_epoch().get() + 1).unwrap();
    let transfer = PgMetadataTransferProof::new(
        work.mutation_binding().source_epoch(),
        unavailable_transition_proof,
    );
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        work.mutation_binding(),
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
    )
    .unwrap();
    let mut publications = Vec::new();
    for staging_actor_id in work.destination_acting_set().iter().copied() {
        let staging_actor_record = unavailable_authority
            .snapshot()
            .node(staging_actor_id)
            .unwrap();
        let staging_page =
            crate::pg_store::metadata_transfer_staging_publication_evidence_page_for_test(
                crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                    staging_actor_id,
                    staging_actor_record.node_incarnation(),
                    staging_actor_record.endpoint().to_owned(),
                )
                .unwrap(),
                &intent,
                transfer,
                None,
            );
        unavailable_authority
            .apply_metadata_transfer_staging_evidence_page(
                staging_page.operation_payload().to_vec(),
                staging_page.page_digest(),
            )
            .unwrap();
        let staging_actor_record = unavailable_authority
            .snapshot()
            .node(staging_actor_id)
            .unwrap();
        let evidence_key = MetadataTransferStagingEvidenceKey {
            pg_id: work.pg_id(),
            staging_generation: authorization.staging_generation,
            actor_node_id: staging_actor_id,
            actor_node_incarnation: staging_actor_record.node_incarnation(),
            kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            target_epoch: Some(destination_epoch),
        };
        publications.push(UnavailablePgStagingPublicationBinding {
            node_id: staging_actor_id,
            node_incarnation: staging_actor_record.node_incarnation(),
            endpoint: staging_actor_record.endpoint().to_owned(),
            evidence_digest: checksum::sha256::digest(
                &unavailable_authority
                    .snapshot()
                    .metadata_transfer_staging_evidence[&evidence_key],
            ),
        });
    }
    publications.sort_by_key(|publication| publication.node_id);
    snapshots.push(unavailable_authority.snapshot().clone());
    unavailable_authority
        .install_unavailable_pg_placement_transitions_batch(
            &[UnavailablePgTransitionInstallRequest {
                unavailable_transition: work.mutation_binding().clone(),
                transfer,
                expected_destination_epoch: destination_epoch,
                publications,
            }],
            destination_epoch,
        )
        .unwrap();
    let payload_ready_at_ms = unavailable_authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap()
        + 1;
    for node_id in [3, 2] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut unavailable_authority,
            node_id,
            7,
            PgState::Peering,
            unavailable_transition_proof,
            false,
            (payload_ready_at_ms + u64::from(node_id), 10_000),
        );
    }
    let payload_ready_at_ms = unavailable_authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap()
        .max(unavailable_deadline_ms + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1);
    let work = unavailable_authority
        .poll_unavailable_pg_reconciliation(
            &mut UnavailablePgReconciliationCursor::start(),
            payload_ready_at_ms,
        )
        .unwrap()
        .expect("installed unavailable transition remains recoverable");
    unavailable_authority
        .complete_unavailable_pg_placement_transition(&work, payload_ready_at_ms)
        .unwrap();
    snapshots.push(unavailable_authority.snapshot().clone());

    let with_horizon = canonical_snapshot_with_node()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: LeaseHorizonAuthorityBinding::new(7, Some(11)),
            authority_now_ms: 500,
            horizon_duration_ms: 2_000,
        })
        .unwrap()
        .into_snapshot();
    snapshots.push(with_horizon);

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("representative.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(24);
    let source_proof = PgMetadataProof::current(7, 8, 9);
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
    snapshots.push(authority.snapshot().clone());
    let source_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof::current(8, 10, 12);
    authority
        .set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            PgMetadataTransferProof::new_with_imported_metadata_proof(
                source_epoch,
                source_proof,
                imported_proof,
            ),
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        2_030,
    );
    let references = PgClusterMapHistoryRouteReferences::try_from_iter(
        [
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
        ]
        .into_iter()
        .map(|kind| PgClusterMapHistoryRouteReference::new(kind, source_epoch, pg_id)),
    )
    .unwrap();
    let mut referenced_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_040);
    referenced_heartbeat.cluster_map_history_route_references = references.clone();
    authority.heartbeat(referenced_heartbeat, 2_040).unwrap();
    snapshots.push(authority.snapshot().clone());
    let omitted_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_041);
    authority
        .heartbeat(omitted_heartbeat.clone(), 2_041)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .retiring_cluster_map_history_route_references,
        references
    );
    authority.heartbeat(omitted_heartbeat, 2_042).unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .retiring_cluster_map_history_route_references,
        references,
        "retransmitting one completed scan must not advance route retirement"
    );
    snapshots.push(authority.snapshot().clone());
    let fresh_omission =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_043);
    authority.heartbeat(fresh_omission, 2_043).unwrap();
    assert!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .retiring_cluster_map_history_route_references
            .is_empty(),
        "a distinct completed scan may retire the omitted route"
    );

    let store = FileControlPlaneStore::new(tmp.path().join("pending.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 3_000).serving());
    }
    let pg_id = PgId::new(25);
    let proof = PgMetadataProof::current(20, 21, 22);
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
        4_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            4_010,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        proof,
        false,
        4_020,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let mut pending_heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 4_030);
    pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Peering,
        metadata_proof: proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_epoch)),
    }];
    authority.heartbeat(pending_heartbeat, 4_030).unwrap();
    snapshots.push(authority.snapshot().clone());

    let store = FileControlPlaneStore::new(tmp.path().join("active-pending.state"));
    let mut active_pending_authority = SingleAuthorityControlPlane::open(store).unwrap();
    active_pending_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut active_pending_authority, 1, 5_000).serving());
    let active_pending_pg_id = PgId::new(26);
    let active_pending_proof = PgMetadataProof::current(23, 24, 25);
    active_pending_authority
        .set_pg_acting_set(active_pending_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut active_pending_authority,
        1,
        active_pending_pg_id.get(),
        PgState::Peering,
        active_pending_proof,
        false,
        5_010,
    );
    active_pending_authority
        .complete_pg_peering(
            active_pending_pg_id,
            NodeId::new(1),
            node_incarnation(&active_pending_authority, 1),
            5_020,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut active_pending_authority,
        1,
        active_pending_pg_id.get(),
        PgState::Active,
        active_pending_proof,
        false,
        5_030,
    );
    let active_pending_epoch = active_pending_authority.snapshot().cluster_epoch();
    let mut current_pending_heartbeat =
        heartbeat_from_record(&active_pending_authority, 1, active_pending_epoch, 5_040);
    current_pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: active_pending_pg_id,
        state: PgState::Active,
        metadata_proof: active_pending_proof,
        pending_metadata_command: Some(test_pending_metadata_command(active_pending_epoch)),
    }];
    active_pending_authority
        .heartbeat(current_pending_heartbeat, 5_040)
        .unwrap();
    snapshots.push(active_pending_authority.snapshot().clone());

    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.initial_topology.is_some()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.initial_topology.is_none()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.lease_grant_horizon.is_some()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.lease_grant_horizon.is_none()));
    for (label, projection) in [
        (
            "active metadata proof",
            (|record: &PgControlRecord| record.active_metadata_proof.is_some())
                as fn(&PgControlRecord) -> bool,
        ),
        ("metadata transfer", |record: &PgControlRecord| {
            record.peering_metadata_transfer.is_some()
        }),
        ("previous primary lease", |record: &PgControlRecord| {
            record.previous_primary_lease.is_some()
        }),
    ] {
        assert!(
            snapshots
                .iter()
                .flat_map(|snapshot| snapshot.pgs.values())
                .any(projection),
            "representative aggregate omitted present {label}"
        );
        assert!(
            snapshots
                .iter()
                .flat_map(|snapshot| snapshot.pgs.values())
                .any(|record| !projection(record)),
            "representative aggregate omitted absent {label}"
        );
    }
    assert!(snapshots
        .iter()
        .flat_map(|snapshot| snapshot.nodes.values())
        .flat_map(|node| node.pg_observations.values())
        .any(|observation| observation.pending_metadata_command.is_some()));
    assert!(snapshots
        .iter()
        .flat_map(|snapshot| snapshot.nodes.values())
        .flat_map(|node| node.pg_observations.values())
        .any(|observation| observation.pending_metadata_command.is_none()));
    assert!(snapshots.iter().any(|snapshot| {
        snapshot
            .pg(active_pending_pg_id)
            .is_some_and(|pg| pg.state() == PgState::Active)
            && snapshot
                .node(NodeId::new(1))
                .and_then(|node| node.pg_observations.get(&active_pending_pg_id))
                .and_then(|observation| observation.pending_metadata_command.as_ref())
                .is_some_and(|pending| pending.cluster_epoch() == snapshot.cluster_epoch())
    }));
    for kind in [
        PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
        PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
    ] {
        assert!(snapshots
            .iter()
            .flat_map(|snapshot| snapshot.nodes.values())
            .flat_map(|node| node.cluster_map_history_route_references.iter())
            .any(|reference| reference.kind() == kind));
    }

    const HISTORICAL_V34_AGGREGATE: &[u8] =
        include_bytes!("testdata/state_v34_representative.aggregate");
    assert_eq!(
        (
            HISTORICAL_V34_AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(HISTORICAL_V34_AGGREGATE))
        ),
        (
            14_715,
            "e444f6b8b05fe347c6d600484845747e11f18dbd78947974234c7aacf98426a7".to_owned()
        )
    );

    const HISTORICAL_V35_AGGREGATE: &[u8] =
        include_bytes!("testdata/state_v35_representative.aggregate");
    assert_eq!(
        (
            HISTORICAL_V35_AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(HISTORICAL_V35_AGGREGATE))
        ),
        (
            19_248,
            "9af617171917370ee903ac7c9263fb9b78b8368abb7cf0182bbc2ed9158b0fca".to_owned()
        )
    );
    let mut remaining = HISTORICAL_V35_AGGREGATE;
    while !remaining.is_empty() {
        let (length, tail) = remaining.split_at(8);
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(length);
        assert!(matches!(
            parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 35"
        ));
        remaining = tail;
    }

    let (install_source, install_requests) = transitions::staged_two_pg_install_fixture();
    let destination_epoch = install_requests[0].expected_destination_epoch;
    let installed = install_source
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: install_requests,
                expected_destination_epoch: destination_epoch,
            },
        )
        .unwrap()
        .into_snapshot();
    assert!(installed
        .unavailable_pg_placement_transitions
        .values()
        .all(|transition| transition.destination_install.is_some()));
    snapshots.push(installed.clone());

    for (version, aggregate, length, digest) in [
        (
            37,
            include_bytes!("testdata/state_v37_representative.aggregate").as_slice(),
            38_995,
            "5e09f13e0d032acbd63811fd768a3b9d79f9535feaf0370c5dea89bc808b2f51",
        ),
        (
            38,
            include_bytes!("testdata/state_v38_representative.aggregate").as_slice(),
            58_029,
            "ea1d5bea6346cf9cfa05b496a29d6cb61afb5944db23c869e58d8614fe346b51",
        ),
    ] {
        assert_eq!(
            (
                aggregate.len(),
                hex_encode(&checksum::sha256::digest(aggregate))
            ),
            (length, digest.to_owned())
        );
        let mut remaining = aggregate;
        while !remaining.is_empty() {
            let (raw_length, tail) = remaining.split_at(8);
            let snapshot_length =
                usize::try_from(u64::from_be_bytes(raw_length.try_into().unwrap())).unwrap();
            let (snapshot, tail) = tail.split_at(snapshot_length);
            assert!(matches!(
                parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
                Err(ControlPlaneError::Parse { line: 1, message })
                    if message == format!("unsupported control-plane state version {version}")
            ));
            remaining = tail;
        }
    }

    let actor_key = installed
        .metadata_transfer_staging_evidence_pages
        .keys()
        .copied()
        .find(|key| {
            key.2 == 2
                && installed
                    .metadata_transfer_staging_evidence_pages
                    .contains_key(&(key.0, key.1, 1))
        })
        .unwrap();
    let checkpointed = installed
        .apply_control_plane_command(
            ControlPlaneCommand::CheckpointMetadataTransferStagingEvidencePages {
                actor_node_id: actor_key.0,
                actor_node_incarnation: actor_key.1,
                first_generation: 1,
                last_generation: 1,
            },
        )
        .unwrap()
        .into_snapshot();
    snapshots.push(checkpointed);
    let actor_closure = transitions::staging_actor_closure_snapshot_fixture();
    let closure_key = *actor_closure
        .metadata_transfer_staging_actor_closures
        .keys()
        .next()
        .unwrap();
    let retirement = actor_closure
        .retire_metadata_transfer_staging_actor_closure_command(closure_key.0, closure_key.1)
        .unwrap();
    snapshots.push(actor_closure.clone());
    snapshots.push(
        actor_closure
            .apply_control_plane_command(retirement)
            .unwrap()
            .into_snapshot(),
    );
    snapshots.push(transitions::finalized_staging_floor_snapshot_fixture());
    snapshots.push(transitions::collapsed_staging_checkpoint_snapshot_fixture());

    let mut aggregate = Vec::new();
    let mut aggregate_text = String::new();
    for snapshot in snapshots {
        snapshot.validate_current_state_invariants().unwrap();
        let formatted = format_snapshot(&snapshot);
        assert_eq!(parse_snapshot(&formatted).unwrap(), snapshot);
        aggregate.extend_from_slice(&(formatted.len() as u64).to_be_bytes());
        aggregate.extend_from_slice(formatted.as_bytes());
        aggregate_text.push_str(&formatted);
    }
    for required_record in [
        "version=44\n",
        "initial_topology=9,",
        "lease_grant_horizon=7,11,2500\n",
        "history=",
        "history_node=",
        "history_pg=",
        "history_pg_absent=",
        "node=",
        "node_history_route=",
        "node_history_route_retiring=",
        "node_pg=",
        "pg=",
        "unavailable_node=",
        "unavailable_pg_transition=",
        "retained_unavailable_pg_transition=",
        "metadata_transfer_staging_evidence_page=",
        "metadata_transfer_staging_evidence_checkpoint=",
        "metadata_transfer_staging_evidence_checkpoint_anchor=",
        "metadata_transfer_staging_actor_closure=",
        "metadata_transfer_staging_retired_actor_closure=",
        "metadata_transfer_staging_finalized_floor=",
        "metadata_transfer_staging_evidence=",
    ] {
        assert!(
            aggregate_text.contains(required_record),
            "representative aggregate omitted {required_record:?}"
        );
    }
    const HISTORICAL_V41_AGGREGATE: &[u8] =
        include_bytes!("testdata/state_v41_representative.aggregate");
    assert_eq!(
        (
            HISTORICAL_V41_AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(HISTORICAL_V41_AGGREGATE))
        ),
        (
            115_265,
            "30b87b7b582ffc9939653ebbc32958ebbb935e2d87592afaeced237bba463e22".to_owned()
        )
    );
    let mut historical = HISTORICAL_V41_AGGREGATE;
    while !historical.is_empty() {
        let (raw_length, tail) = historical.split_at(8);
        let snapshot_length =
            usize::try_from(u64::from_be_bytes(raw_length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(snapshot_length);
        assert!(matches!(
            parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 41"
        ));
        historical = tail;
    }
    const HISTORICAL_V42_AGGREGATE: &[u8] =
        include_bytes!("testdata/state_v42_representative.aggregate");
    assert_eq!(
        (
            HISTORICAL_V42_AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(HISTORICAL_V42_AGGREGATE))
        ),
        (
            115_667,
            "e821fe62500f41870faad9f6e8a43efde3c38dabe2a79a4a162805ab79c25225".to_owned()
        )
    );
    let mut historical = HISTORICAL_V42_AGGREGATE;
    while !historical.is_empty() {
        let (raw_length, tail) = historical.split_at(8);
        let snapshot_length =
            usize::try_from(u64::from_be_bytes(raw_length.try_into().unwrap())).unwrap();
        let (snapshot, tail) = tail.split_at(snapshot_length);
        assert!(matches!(
            parse_snapshot(std::str::from_utf8(snapshot).unwrap()),
            Err(ControlPlaneError::Parse { line: 1, message })
                if message == "unsupported control-plane state version 42"
        ));
        historical = tail;
    }
    assert_eq!(
        (
            aggregate.len(),
            hex_encode(&checksum::sha256::digest(&aggregate))
        ),
        (
            136_450,
            "751ad15d70cde06ea90038535f6d15ca0b05e5bca01ce79f1083b9bd097cae60".to_owned()
        )
    );
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
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
    signed_admin_control_plane_request_with_embedded_kind(
        kind,
        kind,
        signer,
        payload,
        issued_at_ms,
        expires_at_ms,
    )
}

fn signed_admin_control_plane_request_with_embedded_kind(
    outer_kind: ControlPlaneRpcKind,
    embedded_kind: ControlPlaneRpcKind,
    signer: &ControlPlaneScopedCredential,
    payload: Vec<u8>,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> ControlPlaneRpcRequest {
    let payload = write_authenticated_control_plane_rpc_payload(embedded_kind, &payload);
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
        kind: outer_kind,
        payload: envelope.encode_frame().unwrap(),
    }
}

fn signed_admin_control_plane_request_with_auth_version(
    kind: ControlPlaneRpcKind,
    signer: &ControlPlaneScopedCredential,
    payload: Vec<u8>,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
    auth_version: u16,
) -> ControlPlaneRpcRequest {
    let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
    let payload = signer
        .sign_envelope_frame_with_version_for_test(
            crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Service(
                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                ),
                operation: ControlPlaneAuthOperation::AdminControlPlaneCommand,
                issued_at_ms,
                expires_at_ms,
                sequence: None,
                nonce: Vec::new(),
                payload,
            },
            auth_version,
        )
        .expect("test admin control-plane command envelope should sign");
    ControlPlaneRpcRequest { kind, payload }
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
    heartbeat.cluster_map_history_route_scan_generation = NonZeroU64::new(
        record
            .cluster_map_history_route_scan_generation
            .map_or(1, |generation| generation.get() + 1),
    )
    .unwrap();
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
            CreateBucketCommand::from_config_for_test(&config, 123, 1).unwrap(),
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
    PgMetadataProof::current(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    )
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
    pending_epoch_current: bool,
    pg_state: PgState,
    observed_pending: bool,
    peering_ready: bool,
    route_protected: bool,
}

impl PendingCommandLifecycleModel {
    fn active() -> Self {
        Self {
            slot: PendingCommandSlotState::NotInstalled,
            pending_epoch_current: true,
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
                        self.observed_pending = self.slot == PendingCommandSlotState::Pending;
                        self.peering_ready = false;
                        if self.observed_pending && !self.pending_epoch_current {
                            self.pg_state = PgState::Peering;
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
                self.pending_epoch_current = false;
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
    PgMetadataProof::current(
        u64::from(seed) + 1,
        0x1000 + u64::from(seed),
        0x2000 + u64::from(seed),
    )
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
                let expected = PendingMetadataCommandRecoveryTask::new(
                    observation.pg_id(),
                    PendingMetadataCommandRecovery::new(node.node_id(), pending),
                );
                prop_assert!(
                    recovery_listing.tasks().contains(&expected),
                    "accepted pending-command evidence must remain discoverable"
                );
                match pg.state() {
                    PgState::Active => {
                        prop_assert_eq!(pending.cluster_epoch(), snapshot.cluster_epoch());
                        prop_assert_eq!(pg.active_primary(), Some(node.node_id()));
                    }
                    PgState::Peering => {
                        prop_assert!(pending.cluster_epoch() < snapshot.cluster_epoch());
                        let historical = snapshot
                            .reconstructed_pg_route_at_epoch(
                                observation.pg_id(),
                                pending.cluster_epoch(),
                            )
                            .expect("accepted recovery evidence retains its historical route");
                        prop_assert_eq!(historical.state(), PgState::Active);
                        prop_assert_eq!(historical.primary_node_id(), node.node_id());
                    }
                    state => {
                        return Err(TestCaseError::fail(format!(
                            "accepted pending-command evidence has invalid PG state {state:?}"
                        )));
                    }
                }
            }
        }
    }

    for task in recovery_listing.tasks() {
        let pg = snapshot
            .pg(task.pg_id())
            .expect("recovery task references known PG");
        prop_assert!(matches!(pg.state(), PgState::Active | PgState::Peering));
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
                            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
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
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
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
    let active_floor = PgMetadataProof::current(42, 0xabc, 0xdef);
    assert!(metadata_proof_satisfies_active_floor(
        active_floor,
        active_floor
    ));
    assert!(metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof::current(43, 0xabd, 0xdf0),
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof::current(41, 0xabc, 0xdef),
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof::current(42, 0xabd, 0xdef),
    ));
    assert!(!metadata_proof_satisfies_active_floor(
        active_floor,
        PgMetadataProof::current(42, 0xabc, 0xdf0),
    ));
}

#[test]
fn active_metadata_observation_rejects_epoch_local_progress_after_activation() {
    let imported_activation_floor = PgMetadataProof::current(42, 0xabc, 0xdef);

    assert!(metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        imported_activation_floor
    ));
    assert!(metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof::current(43, 0xabd, 0xdf0),
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof::current(1, 0x123, 0x456),
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof::current(42, 0xabd, 0xdf0),
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof::current(41, 0, 0x456),
    ));
    assert!(!metadata_proof_satisfies_active_observation_floor(
        imported_activation_floor,
        PgMetadataProof::current(41, 0x123, 0xdef),
    ));
}

#[test]
fn imported_transfer_local_progress_floor_requires_new_log_hash_and_digest() {
    let imported_activation_floor = PgMetadataProof::current(42, 0xabc, 0xdef);

    assert!(
        metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(1, 0x123, 0x456),
        )
    );
    assert!(
        metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(42, 0x123, 0x456),
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(42, 0xabc, 0xdf0),
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(41, 0, 0x456),
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(41, 0x123, 0xdef),
        )
    );
    assert!(
        !metadata_proof_satisfies_imported_transfer_local_progress_floor(
            imported_activation_floor,
            PgMetadataProof::current(42, 0x123, 0xdef),
        )
    );
}

#[test]
fn active_primary_observation_floor_scopes_epoch_local_progress() {
    let imported_activation_floor = PgMetadataProof::current(42, 0xabc, 0xdef);
    let local_epoch_progress = Some(MetadataProofProgressProvenance {
        floor_epoch: ClusterEpoch::new(7).unwrap(),
        kind: MetadataProofProgressKind::LocalEpoch,
    });
    let imported_transfer_progress = Some(MetadataProofProgressProvenance {
        floor_epoch: ClusterEpoch::new(7).unwrap(),
        kind: MetadataProofProgressKind::ImportedTransfer,
    });

    let lower_epoch_local_proof = PgMetadataProof::current(1, 0x123, 0x456);
    let same_index_epoch_local_proof = PgMetadataProof::current(42, 0x123, 0x456);
    let same_log_digest_only_proof = PgMetadataProof::current(42, 0xabc, 0xdf0);
    let malformed_epoch_local_proof = PgMetadataProof::current(41, 0, 0x456);

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
    let active_floor = PgMetadataProof::current(42, 0xabc, 0xdef);
    let digest_only_progress = PgMetadataProof::current(42, 0xabc, 0xdf0);
    let divergent_log_hash = PgMetadataProof::current(42, 0xabd, 0xdf0);
    let zero_hash_digest_only_progress = PgMetadataProof::current(42, 0, 0xdf0);

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
        PgMetadataProof::current(42, 0, 0xdef),
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
    let active_floor = PgMetadataProof::current(42, 0xabc, 0xdef);
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

    let digest_only_progress = PgMetadataProof::current(
        active_floor.applied_log_index,
        active_floor.applied_log_hash,
        active_floor.state_digest + 1,
    );
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
    let proof = PgMetadataProof::current(7, 8, 9);
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
    let proof = PgMetadataProof::current(7, 8, 9);
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
    let source_proof = PgMetadataProof::current(7, 8, 9);
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

    let imported_proof = PgMetadataProof::current(8, 9, 10);
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
    pg.peering_metadata_proof_floor = Some(PgMetadataProof::current(9, 10, 11));
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
                peering_metadata_proof_floor: None,
                peering_metadata_proof_floor_epoch: None,
                peering_metadata_proof_floor_imported: false,
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
                peering_metadata_proof_floor: Some(PgMetadataProof::empty()),
                peering_metadata_proof_floor_epoch: Some(ClusterEpoch::INITIAL),
                peering_metadata_proof_floor_imported: true,
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
    let active_floor = PgMetadataProof::current(42, 0xabc, 0xdef);
    let digest_only_progress = PgMetadataProof::current(42, 0xabc, 0xdf0);

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

#[path = "tests/durability.rs"]
mod durability;

#[path = "tests/lease_clock.rs"]
mod lease_clock;

#[path = "tests/routing.rs"]
mod routing;

#[path = "tests/pg_lifecycle.rs"]
mod pg_lifecycle;

#[path = "tests/transitions.rs"]
pub(crate) mod transitions;

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
        test_certified_storage_placement_policy((1..=3).map(NodeId::new), 2, 50),
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
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
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
