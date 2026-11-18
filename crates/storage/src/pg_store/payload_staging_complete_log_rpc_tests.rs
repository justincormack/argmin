// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated Unix reads with explicitly selected complete grammar.
//! Setup stays with PG storage. The reader does not authorize recovery/apply.

use super::super::abandonment::source::PartFinalizationSource;
use super::complete_log_range::{apply, index, ordinary};
use super::*;
use crate::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use crate::node_runtime::clients::{
    LocalUnixStorageNodeClientAdmissionSettings, UnixStorageNodeClient,
};
use crate::node_runtime::pg_store::{
    command_log::CompleteCommandLogEntryKind, CompleteLogPage, PgStore,
};
use crate::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeProcessConfig, StorageNodeServer,
};
use crate::storage_rpc::{self as rpc, StorageRpcErrorCode, StorageRpcMessageKind};
use crate::{PgState, StoreError};
use std::os::unix::{fs::PermissionsExt, net::UnixStream};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandRetainedLogEntries;

#[test]
fn staging_ack_transport_is_gated_and_rejects_unconfirmed_inputs() {
    for selected in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, selected);
        let node = server.test_storage_node();
        let changes = || {
            config
                .pg_routes
                .iter()
                .map(|route| node.get_pg(route.pg_id).unwrap().conn.total_changes())
                .collect::<Vec<_>>()
        };
        let before = changes();
        let descriptor = crate::tests::frozen_hex_fixture(include_str!(
            "testdata/staging_descriptor_v1_intro_direct_put.hex"
        ));
        let request =
            rpc::complete_staging_ack::encode_request(&rpc::complete_staging_ack::Request {
                receiver: rpc::complete_settled_prefix::Request {
                    node: config.node_id,
                    pg: fixture(StagingTarget::DirectPut).subject.object_pg,
                    epoch: config.cluster_epoch,
                    topology: rpc::complete_settled_prefix::topology_digest(&topology()),
                    deadline: rpc::StorageRpcOperationDeadline::from_instant(
                        Instant::now() + Duration::from_secs(30),
                    ),
                },
                provenance: rpc::complete_staging_ack::Provenance::Direct {
                    descriptor: &descriptor,
                    logical_size: 4,
                },
                receipts: vec![rpc::complete_staging_ack::Receipt {
                    shard_index: 0,
                    ack: crate::WriteAck {
                        stored_size: 13,
                        crc64: 0,
                    },
                }],
            })
            .unwrap();
        let _running = Running::start(server, config.socket_path.clone());
        for (canonical, bytes) in [
            (true, request.bytes().to_vec()),
            (false, b"SART\x01\x00".to_vec()),
        ] {
            let reply = client
                .test_staging_ack_request_until(bytes, Instant::now() + Duration::from_secs(30))
                .unwrap()
                .unwrap_err();
            if selected {
                if canonical {
                    assert!(reply.message.contains("expired"), "{}", reply.message);
                } else {
                    assert_eq!(reply.code, StorageRpcErrorCode::PayloadDecode);
                }
            } else {
                assert_eq!(reply.code, StorageRpcErrorCode::UnsupportedOperation);
                assert_eq!(
                    reply.message,
                    "staging shard acknowledgement record is not activated"
                );
            }
            assert_eq!(changes(), before);
        }
    }
}

pub(super) fn budget_hint(
    kind: StorageRpcMessageKind,
    pg: PgId,
    first: u64,
    last: u64,
    covered_end: u64,
) -> Vec<u8> {
    rpc::complete_log_budget::encode_response(
        kind,
        &rpc::StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(4),
            cluster_epoch: ClusterEpoch::new(5).unwrap(),
            pg_id: pg,
            first_log_index: index(first),
            last_log_index: index(last),
        },
        covered_end + 1,
    )
    .unwrap()
}

pub(super) struct Running {
    stop: Arc<AtomicBool>,
    socket: std::path::PathBuf,
    thread: Option<thread::JoinHandle<()>>,
}

impl Running {
    pub(super) fn start(server: StorageNodeServer, socket: std::path::PathBuf) -> Self {
        Self::start_inner(server, socket, None)
    }

    pub(super) fn start_at(
        server: StorageNodeServer,
        socket: std::path::PathBuf,
        now_ms: u64,
    ) -> Self {
        Self::start_inner(server, socket, Some(now_ms))
    }

    fn start_inner(
        server: StorageNodeServer,
        socket: std::path::PathBuf,
        now_ms: Option<u64>,
    ) -> Self {
        let server = Arc::new(server);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_server = Arc::clone(&server);
        let worker_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            // Clock overrides are thread-local and must be installed/dropped
            // by this worker, never transferred from the test thread.
            let _clock = now_ms.map(crate::clock::test_time_override_guard);
            loop {
                let result = worker_server.accept_one();
                if worker_stop.load(Ordering::Acquire) {
                    return;
                }
                result.unwrap();
            }
        });
        Self {
            stop,
            socket,
            thread: Some(thread),
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = UnixStream::connect(&self.socket);
        if let Err(panic) = self.thread.take().unwrap().join() {
            if !thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

pub(super) fn installed(
    dir: &test_util::TempDir,
    state: PgState,
    historical: bool,
    select_complete: bool,
) -> (
    StorageNodeServer,
    UnixStorageNodeClient,
    StorageNodeProcessConfig,
) {
    installed_at_epoch(
        dir,
        state,
        ClusterEpoch::new(if historical { 6 } else { 5 }).unwrap(),
        select_complete,
    )
}

pub(super) fn installed_at_epoch(
    dir: &test_util::TempDir,
    state: PgState,
    epoch: ClusterEpoch,
    select_complete: bool,
) -> (
    StorageNodeServer,
    UnixStorageNodeClient,
    StorageNodeProcessConfig,
) {
    let mut config = capture::config(&fixture(StagingTarget::DirectPut));
    config.cluster_epoch = epoch;
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
        route.state = state;
    }
    installed_config(dir, config, select_complete)
}

pub(super) fn installed_config(
    dir: &test_util::TempDir,
    mut config: StorageNodeProcessConfig,
    select_complete: bool,
) -> (
    StorageNodeServer,
    UnixStorageNodeClient,
    StorageNodeProcessConfig,
) {
    config.data_dir = dir.path().join("node");
    config.socket_path = dir.path().join("socket/node.sock");
    std::fs::create_dir(config.socket_path.parent().unwrap()).unwrap();
    std::fs::set_permissions(
        config.socket_path.parent().unwrap(),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let (prepared, client) = prepared(&config);
    let mut server = prepared.bind().unwrap();
    if select_complete {
        server.select_complete_log_operations_for_test();
    }
    (server, client, config)
}

pub(super) fn prepared(
    config: &StorageNodeProcessConfig,
) -> (PreparedStorageNodeServer, UnixStorageNodeClient) {
    authenticated(PreparedStorageNodeServer::new(config.clone()))
}

pub(super) fn authenticated(
    server: PreparedStorageNodeServer,
) -> (PreparedStorageNodeServer, UnixStorageNodeClient) {
    let (server, client_auth) = authenticated_server(server);
    let config = server.config();
    let client = UnixStorageNodeClient::with_rpc_admission_settings_and_auth(
        config.node_id,
        config.cluster_epoch,
        &config.socket_path,
        LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        Some(client_auth),
    )
    .with_pg_topology(Arc::new(topology()));
    (server, client)
}

pub(super) fn authenticated_server(
    server: PreparedStorageNodeServer,
) -> (
    PreparedStorageNodeServer,
    Arc<crate::StorageRpcClientAuthConfig>,
) {
    let config = server.config();
    let credential = ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
        cluster_id: "complete-log-rpc-test".into(),
        credential_id: "frontend-1".into(),
        credential_version: 1,
        principal: ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".into(),
        },
        secret: b"complete-log-rpc-test-secret".to_vec(),
    })
    .unwrap();
    const TOPOLOGY: &str = "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0";
    let server_auth = crate::StorageRpcServerAuthConfig::new(
        credential.cluster_id(),
        ControlPlaneScopedCredentialStore::new(
            std::iter::once(credential.clone())
                .chain((0..5).map(window_receiver_credential_for))
                .collect(),
        )
        .unwrap(),
        config.cluster_epoch.get(),
        TOPOLOGY,
    )
    .unwrap();
    let client_auth = Arc::new(
        crate::FrontendStorageRpcClientCapability::new(
            credential,
            config.cluster_epoch.get(),
            TOPOLOGY,
        )
        .unwrap()
        .into(),
    );
    (server.with_rpc_auth(server_auth), client_auth)
}

pub(super) fn window_receiver_credential() -> ControlPlaneScopedCredential {
    window_receiver_credential_for(4)
}

fn window_receiver_credential_for(node: u32) -> ControlPlaneScopedCredential {
    ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
        cluster_id: "complete-log-rpc-test".into(),
        credential_id: format!("window-receiver-{node}"),
        credential_version: 1,
        principal: ControlPlaneAuthPrincipal::StorageNodeProcess {
            node_id: NodeId::new(node),
        },
        secret: b"window-receiver-test-secret".to_vec(),
    })
    .unwrap()
}

pub(super) fn window_receiver_auth(
    epoch: ClusterEpoch,
) -> crate::StorageNodeStorageRpcClientCapability {
    window_receiver_auth_for(epoch, 4)
}

pub(super) fn window_receiver_auth_for(
    epoch: ClusterEpoch,
    node: u32,
) -> crate::StorageNodeStorageRpcClientCapability {
    crate::StorageNodeStorageRpcClientCapability::new(
        window_receiver_credential_for(node),
        epoch.get(),
        "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0",
    )
    .unwrap()
}

fn read(
    client: &UnixStorageNodeClient,
    pg: PgId,
    first: u64,
    last: u64,
) -> Result<CompleteLogPage, StoreError> {
    client.complete_metadata_log_page_until(
        pg,
        ClusterEpoch::new(5).unwrap(),
        index(first),
        index(last),
        Instant::now() + Duration::from_secs(30),
    )
}

pub(super) fn seed_context(pg: &PgStore, case: usize) -> ContextualCommand {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let context = prepare_context(pg, case);
    apply(pg, &context);
    context
}

pub(super) fn prepare_context(pg: &PgStore, case: usize) -> ContextualCommand {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let context = match case {
        0 => {
            let owner = fixture(StagingTarget::DirectPut);
            ContextualCommand::decode_field(
                ordinary(owner.subject.id.introduction),
                &[1, 0, 0],
                &topology(),
            )
            .unwrap()
        }
        1 => {
            let mut owner = fresh_capture::template(StagingTarget::DirectPut);
            let command = fresh_capture::candidate(pg, &mut owner);
            ContextualCommand::from_described(candidate::capture(pg, &owner, command, false))
        }
        2 => {
            let owner = part::initialize(pg, 2);
            let source = part::command(pg, &owner);
            let cleanup = abandonment::cleanup(pg, &owner, &source);
            let context = ContextualCommand::abandoned_part(
                PartFinalizationSource::capture_for_test(&source)
                    .unwrap()
                    .bind_cleanup(cleanup, &topology())
                    .unwrap(),
                &topology(),
            )
            .unwrap();
            abandonment::terminal(pg, &source, true);
            pg.conn.execute_batch("COMMIT").unwrap();
            context
        }
        _ => panic!("unknown fixture"),
    };
    complete_application::anchor(pg, context.id());
    context
}

#[test]
fn staging_complete_log_rpc_authenticated_contexts_survive_active_peering_and_historical_reads() {
    for (state, historical) in [
        (PgState::Active, false),
        (PgState::Peering, false),
        (PgState::Peering, true),
    ] {
        for case in 0..3 {
            let dir = test_util::tempdir();
            let context_epoch = if case == 2 { 6 } else { 5 };
            let (server, client, config) = installed_at_epoch(
                &dir,
                state,
                ClusterEpoch::new(context_epoch + u64::from(historical)).unwrap(),
                true,
            );
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let context;
            let changes;
            {
                let pg = node.get_pg(pg_id.get()).unwrap();
                context = seed_context(&pg, case);
                changes = pg.conn.total_changes();
            }
            let _running = Running::start(server, config.socket_path);
            let id = context.id();
            assert_eq!(id.cluster_epoch().get(), context_epoch);
            let page = client
                .complete_metadata_log_page_until(
                    pg_id,
                    id.cluster_epoch(),
                    index(1),
                    id.log_index(),
                    Instant::now() + Duration::from_secs(30),
                )
                .unwrap();
            assert_eq!(page.covered_range(), (index(1), id.log_index()));
            assert_eq!(page.len() as u64, id.log_index().get());
            let CompleteCommandLogEntryKind::Applied(received) =
                page.into_entries().pop().unwrap().into_kind()
            else {
                panic!()
            };
            assert_eq!(
                received.encode_command().unwrap().bytes(),
                context.encode_command().unwrap().bytes()
            );
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                changes
            );
        }
    }
}

#[test]
fn staging_complete_log_rpc_budget_reduces_covered_interval_without_skipping_history() {
    const MIB: usize = 1024 * 1024;
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let node = server.test_storage_node();
    let bucket = fixture(StagingTarget::DirectPut).subject.bucket;
    let pg_id = PgId::new(topology().bucket_pg_for(&bucket));
    let mut previous = 0;
    let changes;
    {
        let pg = node.get_pg(pg_id.get()).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        for position in 1..=17 {
            previous = complete_hash_range::seed_log_row(
                &pg,
                MetadataCommandId::new(config.cluster_epoch, pg_id, index(position)),
                MIB,
                previous,
            );
        }
        pg.conn.execute_batch("COMMIT").unwrap();
        changes = pg.conn.total_changes();
    }
    let responses = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&responses);
    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
        if kind == KIND {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let _running = Running::start(server, config.socket_path);
    let first = read(&client, pg_id, 1, 17).unwrap();
    assert_eq!(first.covered_range(), (index(1), index(16)));
    assert_eq!(first.len(), 16);
    assert_eq!(responses.load(Ordering::SeqCst), 2);
    let second = read(&client, pg_id, 17, 17).unwrap();
    assert_eq!(second.covered_range(), (index(17), index(17)));
    assert_eq!(second.len(), 1);
    let ids: Vec<_> = first
        .entries()
        .iter()
        .chain(second.entries())
        .map(|row| row.id().log_index().get())
        .collect();
    assert_eq!(ids, (1..=17).collect::<Vec<_>>());
    assert_eq!(
        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
        changes
    );
}

#[test]
fn staging_complete_log_rpc_unstructured_errors_never_reduce_the_range() {
    for (code, expected_sends) in [
        (StorageRpcErrorCode::RetainedLogValidationBudgetExceeded, 1),
        (StorageRpcErrorCode::ResourceExhausted, 1),
        (StorageRpcErrorCode::Internal, 1),
        (StorageRpcErrorCode::PayloadDecode, 1),
    ] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, true);
        let responses = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&responses);
        server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind == KIND {
                count.fetch_add(1, Ordering::SeqCst);
                frame.payload =
                    rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                        code,
                        message: "injected peer error".into(),
                    })
                    .unwrap();
            }
        }));
        let _running = Running::start(server, config.socket_path);
        let error = read(&client, PgId::new(0), 1, 31).unwrap_err();
        if code == StorageRpcErrorCode::ResourceExhausted {
            assert!(
                matches!(error, StoreError::StorageRpcResourceExhausted { .. }),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(error, StoreError::StorageRpc { failure, .. } if failure == code),
                "{error:?}"
            );
        }
        assert_eq!(responses.load(Ordering::SeqCst), expected_sends);
    }
}

#[test]
fn staging_complete_log_rpc_capacity_failure_stops_an_already_reduced_range() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let responses = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&responses);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind == KIND {
            let response = count.fetch_add(1, Ordering::SeqCst);
            if response == 0 {
                frame.payload = budget_hint(kind, PgId::new(0), 1, 31, 16);
                return;
            }
            frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ResourceExhausted,
                // Diagnostics alone never permit a retry.
                message: "budget exhausted".into(),
            })
            .unwrap();
        }
    }));
    let _running = Running::start(server, config.socket_path);
    let error = read(&client, PgId::new(0), 1, 31).unwrap_err();
    assert!(
        matches!(error, StoreError::StorageRpcResourceExhausted { .. }),
        "{error:?}"
    );
    assert_eq!(responses.load(Ordering::SeqCst), 2);
}

#[test]
fn staging_complete_log_rpc_binds_hints_before_retry_and_never_retries_twice() {
    for kind in [
        KIND,
        StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
    ] {
        for fault in [
            "node",
            "epoch",
            "pg",
            "first",
            "last",
            "kind",
            "before",
            "same",
            "after",
            "truncated",
            "trailing",
            "second_hint",
            "auth",
        ] {
            let dir = test_util::tempdir();
            let (server, client, config) = installed(&dir, PgState::Active, false, true);
            let responses = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&responses);
            server.set_response_frame_test_hook(Arc::new(move |actual, frame| {
                if actual != kind {
                    return;
                }
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                let (last, end) = if attempt == 0 { (31, 16) } else { (16, 8) };
                let response = budget_hint(kind, PgId::new(0), 7, last, end);
                let mut body = rpc::decode_storage_rpc_response_payload(&response)
                    .unwrap()
                    .unwrap();
                match fault {
                    "kind" => body[4] ^= 1,
                    "node" => body[6] ^= 1,
                    "epoch" => body[10] ^= 1,
                    "pg" => body[18] ^= 1,
                    "first" => body[22] ^= 1,
                    "last" => body[30] ^= 1,
                    "before" => body[38..46].copy_from_slice(&6u64.to_le_bytes()),
                    "same" => body[38..46].copy_from_slice(&31u64.to_le_bytes()),
                    "after" => body[38..46].copy_from_slice(&32u64.to_le_bytes()),
                    "truncated" => {
                        body.pop();
                    }
                    "trailing" => body.push(0),
                    "second_hint" | "auth" => {}
                    _ => unreachable!(),
                }
                frame.payload = rpc::encode_storage_rpc_success_response(&body);
            }));
            if fault == "auth" {
                server.set_response_envelope_test_hook(Arc::new(move |actual, bytes| {
                    if actual == kind {
                        *bytes.last_mut().unwrap() ^= 1;
                    }
                }));
            }
            let _running = Running::start(server, config.socket_path);
            let error = match kind {
                KIND => read(&client, PgId::new(0), 7, 31).map(|_| ()).unwrap_err(),
                _ => client
                    .complete_metadata_log_hash_page_until(
                        PgId::new(0),
                        config.cluster_epoch,
                        index(7),
                        index(31),
                        Instant::now() + Duration::from_secs(30),
                    )
                    .map(|_| ())
                    .unwrap_err(),
            };
            if fault == "auth" {
                assert!(
                    matches!(&error, StoreError::StorageRpc { detail, .. } if detail.as_str().contains("authentication")),
                    "{error:?}"
                );
            } else {
                let expected = if fault == "second_hint" {
                    StorageRpcErrorCode::RetainedLogValidationBudgetExceeded
                } else {
                    StorageRpcErrorCode::PayloadDecode
                };
                assert!(
                    matches!(&error, StoreError::StorageRpc { failure, .. } if *failure == expected),
                    "{fault}: {error:?}"
                );
            }
            assert_eq!(
                responses.load(Ordering::SeqCst),
                if fault == "second_hint" { 2 } else { 1 },
                "{fault}"
            );
        }
    }
}

#[test]
fn staging_complete_log_rpc_rejects_signed_crossed_page_and_bad_response_authentication() {
    for bad_auth in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = seed_context(&node.get_pg(pg_id.get()).unwrap(), 0);
        if bad_auth {
            server.set_response_envelope_test_hook(Arc::new(|kind, bytes| {
                if kind == KIND {
                    *bytes.last_mut().unwrap() ^= 1;
                }
            }));
        } else {
            server.set_response_frame_test_hook(Arc::new(|kind, frame| {
                if kind != KIND {
                    return;
                }
                let body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                    .unwrap()
                    .unwrap();
                let mut rows = rpc::complete_log_range::decode(&body).unwrap();
                rows[0].log_index += 1;
                let mut encoder = rpc::complete_log_range::Encoder::new();
                for row in rows {
                    encoder = encoder.push(row).unwrap();
                }
                frame.payload = rpc::encode_storage_rpc_success_response(&encoder.finish());
            }));
        }
        let _running = Running::start(server, config.socket_path);
        let id = context.id();
        let error = read(&client, pg_id, id.log_index().get(), id.log_index().get()).unwrap_err();
        if !bad_auth {
            assert!(
                matches!(error, StoreError::StorageRpc { failure, .. } if failure == StorageRpcErrorCode::PayloadDecode),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(&error, StoreError::StorageRpc { operation: "read storage RPC response", detail, .. } if detail.as_str().contains("authentication")),
                "{error:?}"
            );
        }
    }
}

#[test]
fn staging_complete_log_rpc_deadline_does_not_restart_on_retry_and_preflight_sends_nothing() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let responses = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&responses);
    let deadline = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let cutoff = Arc::clone(&deadline);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind != KIND {
            return;
        }
        let response = count.fetch_add(1, Ordering::SeqCst);
        if response == 0 {
            frame.payload = budget_hint(kind, PgId::new(0), 1, 31, 16);
            return;
        }
        if response == 1 {
            // The first response makes the client enter its retry. Hold the
            // second beyond the original deadline: rebasing it would permit
            // further retries instead of stopping at these two sends.
            let until = cutoff.lock().unwrap().unwrap();
            thread::sleep(
                until.saturating_duration_since(Instant::now()) + Duration::from_millis(20),
            );
        }
        frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
            code: StorageRpcErrorCode::RetainedLogValidationBudgetExceeded,
            message: "budget exhausted".into(),
        })
        .unwrap();
    }));
    let _running = Running::start(server, config.socket_path);
    for (pg, first, last, epoch, until) in [
        (
            PgId::new(99),
            1,
            1,
            config.cluster_epoch,
            Instant::now() + Duration::from_secs(30),
        ),
        (
            PgId::new(0),
            1,
            32,
            config.cluster_epoch,
            Instant::now() + Duration::from_secs(30),
        ),
        (
            PgId::new(0),
            1,
            1,
            ClusterEpoch::new(6).unwrap(),
            Instant::now() + Duration::from_secs(30),
        ),
        (PgId::new(0), 1, 1, config.cluster_epoch, Instant::now()),
    ] {
        assert!(client
            .complete_metadata_log_page_until(pg, epoch, index(first), index(last), until)
            .is_err());
    }
    assert_eq!(responses.load(Ordering::SeqCst), 0);
    let until = Instant::now() + Duration::from_secs(2);
    *deadline.lock().unwrap() = Some(until);
    assert!(client
        .complete_metadata_log_page_until(
            PgId::new(0),
            config.cluster_epoch,
            index(1),
            index(31),
            until
        )
        .is_err());
    assert_eq!(responses.load(Ordering::SeqCst), 2);
}

#[test]
fn staging_complete_log_rpc_keeps_default_grammar_and_route_rejection_closed() {
    for (select_complete, historical) in [(false, false), (true, true)] {
        let dir = test_util::tempdir();
        let (server, client, config) =
            installed(&dir, PgState::Active, historical, select_complete);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = seed_context(&node.get_pg(pg_id.get()).unwrap(), 0);
        let _running = Running::start(server, config.socket_path);
        let id = context.id();
        let error = read(&client, pg_id, id.log_index().get(), id.log_index().get()).unwrap_err();
        if historical {
            assert!(
                matches!(error, StoreError::StorageRpc { failure, .. } if failure == StorageRpcErrorCode::StaleShardLocation),
                "{error:?}"
            );
        }
        assert_eq!(
            node.get_pg(pg_id.get())
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            id.log_index().get()
        );
    }
}

#[test]
fn staging_complete_log_rpc_does_not_fall_back_to_retired_applied_grammar() {
    for complete_reader in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, complete_reader);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let command = ordinary(MetadataCommandId::new(
            config.cluster_epoch,
            pg_id,
            index(1),
        ));
        node.get_pg(pg_id.get())
            .unwrap()
            .apply_metadata_command_and_record(4, &command)
            .unwrap();
        let _running = Running::start(server, config.socket_path);
        if !complete_reader {
            assert_eq!(
                client
                    .retained_metadata_command_log_entries(
                        pg_id,
                        config.cluster_epoch,
                        index(1),
                        index(1)
                    )
                    .unwrap()
                    .len(),
                1
            );
        }
        // The selected complete decoder must reject V13 whether the current
        // handler returned it, or the complete handler detected it in SQLite.
        let error = read(&client, pg_id, 1, 1).unwrap_err();
        assert!(matches!(error, StoreError::StorageRpc { .. }), "{error:?}");
        assert_eq!(
            node.get_pg(pg_id.get())
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            1
        );
    }
}
