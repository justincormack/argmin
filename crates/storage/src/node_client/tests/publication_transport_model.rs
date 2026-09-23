// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded plain-Unix transport observations backed by the real node server.
//! Socket failures are scheduled by a one-exchange proxy, never sleeps.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use crate::metadata_command::{CreateBucketCommand, ReserveObjectGenerationCommand};
use crate::storage_rpc::{
    decode_metadata_command_request, decode_storage_rpc_response_payload, encode_storage_rpc_frame,
    encode_storage_rpc_frame_with_version_for_test, STORAGE_RPC_FRAME_ENCODING_VERSION,
};
use std::cell::RefCell;
use std::io::Write;
use std::os::unix::net::UnixStream;

const REQUIRED: [&str; 6] = [
    "not connected is definitely not sent",
    "unforwarded request is still ambiguous to client",
    "lost reply preserves actual application",
    "malformed operation reply preserves actual application",
    "owner A survives retry and crossed command",
    "owner B survives retry and crossed command",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Delivery {
    Healthy,
    NotConnected,
    DropBeforeDispatch,
    DropAfterApply,
    Truncated,
    InvalidMagic,
    UnsupportedVersion,
    BadChecksum,
    WrongRequestId,
    WrongKind,
    MalformedResponse,
    MalformedOutcome,
}

impl Delivery {
    fn applied(self) -> bool {
        !matches!(self, Self::NotConnected | Self::DropBeforeDispatch)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    Send(Delivery),
    RetryExact,
    CrossedCommand,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    owner: usize,
    trace: Vec<Step>,
}

impl State {
    fn successors(&self) -> Vec<(Step, Self)> {
        let steps = match self.trace.as_slice() {
            [] => [
                Delivery::Healthy,
                Delivery::NotConnected,
                Delivery::DropBeforeDispatch,
                Delivery::DropAfterApply,
                Delivery::Truncated,
                Delivery::InvalidMagic,
                Delivery::UnsupportedVersion,
                Delivery::BadChecksum,
                Delivery::WrongRequestId,
                Delivery::WrongKind,
                Delivery::MalformedResponse,
                Delivery::MalformedOutcome,
            ]
            .into_iter()
            .map(Step::Send)
            .collect(),
            [Step::Send(_)] => vec![Step::RetryExact],
            [Step::Send(_), Step::RetryExact] => vec![Step::CrossedCommand],
            [Step::Send(_), Step::RetryExact, Step::CrossedCommand] => Vec::new(),
            _ => panic!("invalid transport trace: {:?}", self.trace),
        };
        steps
            .into_iter()
            .map(|step| {
                let mut next = self.clone();
                next.trace.push(step);
                (step, next)
            })
            .collect()
    }

    fn witnesses(&self) -> Vec<&'static str> {
        match self.trace.as_slice() {
            [Step::Send(Delivery::NotConnected)] => vec![REQUIRED[0]],
            [Step::Send(Delivery::DropBeforeDispatch)] => vec![REQUIRED[1]],
            [Step::Send(Delivery::DropAfterApply)] => vec![REQUIRED[2]],
            [Step::Send(Delivery::MalformedOutcome)] => vec![REQUIRED[3]],
            [Step::Send(Delivery::DropAfterApply), Step::RetryExact, Step::CrossedCommand] => {
                vec![REQUIRED[4 + self.owner]]
            }
            _ => Vec::new(),
        }
    }
}

fn diagnostic(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn equal<T: std::fmt::Debug + PartialEq>(
    actual: T,
    expected: T,
    label: &str,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label}: got {actual:?}, expected {expected:?}"))
    }
}

// Wake accept on early client failure/unwind and always join. Read/write timeouts
// are deadlock guards, not model transitions or timing-based assertions.
struct Proxy {
    path: std::path::PathBuf,
    worker: Option<thread::JoinHandle<Result<(), String>>>,
}

impl Proxy {
    fn finish(mut self) -> Result<(), String> {
        let _ = UnixStream::connect(&self.path);
        self.worker
            .take()
            .unwrap()
            .join()
            .map_err(|_| "proxy worker panicked".to_string())?
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = UnixStream::connect(&self.path);
            let _ = worker.join();
        }
    }
}

fn proxy_exchange(
    listener: UnixListener,
    backend: std::path::PathBuf,
    expected: MetadataCommandEnvelope,
    delivery: Delivery,
) -> Result<(), String> {
    let (mut front, _) = listener.accept().map_err(diagnostic)?;
    front
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(diagnostic)?;
    front
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(diagnostic)?;
    let request = read_storage_rpc_frame_from(&mut front).map_err(diagnostic)?;
    equal(
        request.kind,
        StorageRpcMessageKind::MetadataCommandApplyAndRecord,
        "wire operation",
    )?;
    let decoded = decode_metadata_command_request(
        &request.payload,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new(),
    )
    .map_err(diagnostic)?;
    equal(
        decoded.command,
        expected,
        "exact command crosses the transport",
    )?;
    if delivery == Delivery::DropBeforeDispatch {
        return Ok(());
    }
    let mut back = UnixStream::connect(backend).map_err(diagnostic)?;
    back.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(diagnostic)?;
    back.set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(diagnostic)?;
    write_storage_rpc_frame_to(&mut back, &request).map_err(diagnostic)?;
    let mut response = read_storage_rpc_frame_from(&mut back).map_err(diagnostic)?;
    // The production server has finished its operation before response faults.
    // Healthy crossed-command responses are real remote rejections, not forged.
    if delivery != Delivery::Healthy {
        let payload = decode_storage_rpc_response_payload(&response.payload)
            .map_err(diagnostic)?
            .map_err(diagnostic)?;
        let decoded = crate::storage_rpc::decode_metadata_command_state_outcome_response(&payload)
            .map_err(diagnostic)?;
        if !matches!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::State(_)
        ) {
            return Err(format!("server did not apply command: {decoded:?}"));
        }
    }
    match delivery {
        Delivery::DropAfterApply => return Ok(()),
        Delivery::WrongRequestId => response.request_id += 1,
        Delivery::WrongKind => response.kind = StorageRpcMessageKind::Health,
        Delivery::MalformedResponse => response.payload = vec![255],
        Delivery::MalformedOutcome => {
            response.payload = encode_storage_rpc_success_response(&[255])
        }
        _ => {}
    }
    let mut bytes = if delivery == Delivery::UnsupportedVersion {
        encode_storage_rpc_frame_with_version_for_test(
            response.request_id,
            response.kind,
            &response.payload,
            STORAGE_RPC_FRAME_ENCODING_VERSION + 1,
        )
    } else {
        encode_storage_rpc_frame(response.request_id, response.kind, &response.payload)
            .map_err(diagnostic)?
    };
    match delivery {
        Delivery::Truncated => {
            bytes.pop();
        }
        Delivery::InvalidMagic => bytes[4] ^= 1, // after the u32 magic length
        Delivery::BadChecksum => *bytes.last_mut().unwrap() ^= 1,
        _ => {}
    }
    front.write_all(&bytes).map_err(diagnostic)
}

struct Fixture {
    _server: TestStorageNodeServerGuard,
    node: Arc<SharedStorageNode>,
    config: StorageNodeProcessConfig,
    commands: [MetadataCommandEnvelope; 2],
    reservations: [SessionId; 2],
    bucket: BucketName,
    key: ObjectKey,
    directory: test_util::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let directory = test_util::tempdir();
        let config = test_config(&directory);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let node = server.test_storage_node();
        let bucket = crate::tests::bucket_name("transport-model");
        let key = crate::tests::object_key("object");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let grants = crate::AclGrants::default();
        let create = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let seed = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                config.cluster_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create, 123, 1).unwrap(),
            ),
        );
        node.get_pg(0)
            .unwrap()
            .apply_metadata_command_and_record(7, &seed)
            .unwrap();
        let reservations = [
            crate::tests::stream_session_id("wire-owner-a"),
            crate::tests::stream_session_id("wire-owner-b"),
        ];
        let commands = std::array::from_fn(|i| {
            MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    config.cluster_epoch,
                    PgId::new(0),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        bucket.clone(),
                        key.clone(),
                        reservations[i].clone(),
                        GenerationId::new(i as u64 + 1).unwrap(),
                        123,
                    ),
                ),
            )
        });
        Self {
            _server: spawn_test_storage_node_server(server),
            node,
            config,
            commands,
            reservations,
            bucket,
            key,
            directory,
        }
    }

    fn send(
        &self,
        owner: usize,
        delivery: Delivery,
        sequence: usize,
    ) -> Result<Result<MetadataCommandReplicaState, MetadataCommandApplyError>, String> {
        let path = self
            .directory
            .path()
            .join(format!("sock/proxy-{sequence}.sock"));
        let proxy = if delivery == Delivery::NotConnected {
            None
        } else {
            let listener = UnixListener::bind(&path).map_err(diagnostic)?;
            let backend = self.config.socket_path.clone();
            let command = self.commands[owner].clone();
            Some(Proxy {
                path: path.clone(),
                worker: Some(thread::spawn(move || {
                    proxy_exchange(listener, backend, command, delivery)
                })),
            })
        };
        // Fresh client/connection on every exchange; no pool reuse or process
        // restart is claimed. The exact durable command is reused on retry.
        let client =
            UnixStorageNodeClient::new(self.config.node_id, self.config.cluster_epoch, path);
        let result = MetadataCommandNodeClient::apply_metadata_command_and_record_until(
            &client,
            PgId::new(0),
            &self.commands[owner],
            Instant::now() + Duration::from_secs(30),
        );
        drop(client);
        if let Some(proxy) = proxy {
            proxy.finish()?;
        }
        Ok(result)
    }

    fn observe(&self, owner: usize, applied: bool) -> Result<MetadataCommandReplicaState, String> {
        let pg = self.node.get_pg(0).map_err(diagnostic)?;
        let proof = pg.metadata_command_replica_state().map_err(diagnostic)?;
        equal(
            proof.applied_log_index,
            if applied { 2 } else { 1 },
            "durable applied index",
        )?;
        for i in 0..2 {
            let reservation = match pg.get_object_generation_reservation(
                &self.bucket,
                &self.key,
                &self.reservations[i],
            ) {
                Ok(generation) => Some(generation.get()),
                Err(MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                Err(error) => return Err(diagnostic(error)),
            };
            equal(
                reservation,
                (applied && i == owner).then_some(i as u64 + 1),
                "exact reservation owner",
            )?;
        }
        Ok(proof)
    }
}

fn replay(state: &State) -> Result<(), String> {
    let fixture = Fixture::new();
    let mut proof = fixture.observe(state.owner, false)?;
    let mut applied = false;
    for (sequence, step) in state.trace.iter().enumerate() {
        let (owner, delivery) = match *step {
            Step::Send(delivery) => (state.owner, delivery),
            Step::RetryExact => (state.owner, Delivery::Healthy),
            Step::CrossedCommand => (1 - state.owner, Delivery::Healthy),
        };
        let result = fixture.send(owner, delivery, sequence)?;
        match *step {
            Step::Send(Delivery::Healthy) | Step::RetryExact => {
                let returned = result.map_err(diagnostic)?;
                equal(
                    returned,
                    fixture.observe(state.owner, true)?,
                    "acknowledged durable proof",
                )?;
            }
            Step::CrossedCommand => {
                let error = result
                    .err()
                    .ok_or("crossed command unexpectedly succeeded")?;
                equal(
                    error.kind(),
                    MetadataCommandApplyErrorKind::Definitive,
                    "valid remote rejection",
                )?;
                match error.into_source() {
                    BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                        node_id: 7,
                        pg_id: 0,
                        cluster_epoch,
                        log_index: 2,
                    }) if cluster_epoch == fixture.config.cluster_epoch => {}
                    other => return Err(format!("wrong crossed-command rejection: {other:?}")),
                }
            }
            Step::Send(delivery) => {
                let error = result
                    .err()
                    .ok_or("faulted response unexpectedly succeeded")?;
                equal(
                    error.kind(),
                    if delivery == Delivery::NotConnected {
                        MetadataCommandApplyErrorKind::NotSent
                    } else {
                        MetadataCommandApplyErrorKind::MayHaveApplied
                    },
                    "dispatch certainty",
                )?;
                if delivery != Delivery::NotConnected {
                    let expected = match delivery {
                        Delivery::DropBeforeDispatch
                        | Delivery::DropAfterApply
                        | Delivery::Truncated => StorageRpcErrorCode::TransportClosed,
                        _ => StorageRpcErrorCode::PayloadDecode,
                    };
                    match error.into_source() {
                        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                            failure, ..
                        }) => equal(failure, expected, "transport diagnostic category")?,
                        other => return Err(format!("wrong transport error: {other:?}")),
                    }
                }
            }
        }
        let next_applied = applied || delivery.applied();
        let next_proof = fixture.observe(state.owner, next_applied)?;
        if applied || !next_applied {
            equal(
                next_proof,
                proof,
                "retry/rejection preserves complete durable proof",
            )?;
        }
        applied = next_applied;
        proof = next_proof;
    }
    Ok(())
}

#[test]
fn publication_model_exhausts_unix_transport_observations_and_exact_retry() {
    let failure = RefCell::new(None);
    let explored = explore(
        (0..2).map(|owner| State {
            owner,
            trace: Vec::new(),
        }),
        &REQUIRED,
        State::successors,
        |state| match replay(state) {
            Ok(()) => Checks {
                failure: None,
                witnesses: state.witnesses(),
            },
            Err(error) => {
                *failure.borrow_mut() = Some((state.clone(), error));
                Checks {
                    failure: Some("transport uncertainty preserves durable command identity"),
                    witnesses: Vec::new(),
                }
            }
        },
        1_000,
    );
    if let Some(trace) = explored.counterexample() {
        let (state, error) = failure.into_inner().unwrap();
        assert_eq!(trace, state.trace);
        assert_eq!(
            replay(&state).unwrap_err(),
            error,
            "counterexample must replay"
        );
        panic!("transport counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 74);
    assert_eq!(explored.transitions, 72);
    for name in REQUIRED {
        let trace = explored.witness(name).unwrap();
        let state = explored
            .states()
            .find(|state| state.trace == trace && state.witnesses().contains(&name))
            .unwrap();
        replay(state).unwrap();
        eprintln!("{name}: {state:?}");
    }
}
