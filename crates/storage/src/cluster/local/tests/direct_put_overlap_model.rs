// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact per-shard cleanup of a physically mismatched pending direct PUT.
//! The adversarial pending layout is prepared owner-locally, but both layouts
//! have real, valid EC bytes. Rejection and recovery use production operations.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use std::cell::RefCell;

const DATA: &[u8] = b"pending command payload must remain recoverable";
const CALLER_EC: EcShape = EcShape { k: 2, m: 2 };
const COMMAND_EC: EcShape = EcShape { k: 2, m: 1 };
const REQUIRED: [&str; 4] = [
    "partial overlap cleans only the caller-only shard",
    "disjoint staging cleans every caller shard",
    "partial overlap remains recoverable after repeated rejection",
    "disjoint pending payload remains recoverable after repeated rejection",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Layout {
    PartialOverlap,
    Disjoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    Stage,
    Reject,
    Restage,
    Recover,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    layout: Layout,
    command_first: bool,
    trace: Vec<Step>,
}

impl State {
    fn successors(&self) -> Vec<(Step, Self)> {
        let steps: &[Step] = match self.trace.as_slice() {
            [] => &[Step::Stage],
            [Step::Stage] | [Step::Stage, Step::Reject, Step::Restage] => &[Step::Reject],
            [Step::Stage, Step::Reject] => &[Step::Recover, Step::Restage],
            [Step::Stage, Step::Reject, Step::Restage, Step::Reject] => &[Step::Recover],
            [.., Step::Recover] => &[],
            _ => panic!("invalid overlap trace: {:?}", self.trace),
        };
        steps
            .iter()
            .map(|&step| {
                let mut next = self.clone();
                next.trace.push(step);
                (step, next)
            })
            .collect()
    }

    fn witnesses(&self) -> Vec<&'static str> {
        let offset = match self.layout {
            Layout::PartialOverlap => 0,
            Layout::Disjoint => 1,
        };
        match self.trace.as_slice() {
            [Step::Stage, Step::Reject] => vec![REQUIRED[offset]],
            [Step::Stage, Step::Reject, Step::Restage, Step::Reject, Step::Recover] => {
                vec![REQUIRED[2 + offset]]
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

struct Fixture {
    map: Arc<LocalClusterMap>,
    cluster: Arc<StorageCluster>,
    bucket: crate::BucketName,
    key: crate::ObjectKey,
    reservation: crate::SessionId,
    _directory: test_util::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let directory = test_util::tempdir();
        let nodes: Vec<_> = (0..4).map(NodeId::new).collect();
        let map =
            Arc::new(LocalClusterMap::open(directory.path(), &nodes, &[0], CALLER_EC).unwrap());
        let cluster = current_cluster(&map);
        let bucket = crate::tests::bucket_name("overlap-model");
        let key = crate::tests::object_key("object");
        create_test_bucket(&cluster, &bucket);
        Self {
            map,
            cluster,
            bucket,
            key,
            reservation: crate::tests::stream_session_id("overlap"),
            _directory: directory,
        }
    }

    fn prepared(&self) -> crate::PreparedDirectPutObjectCommit {
        crate::PreparedDirectPutObjectCommit {
            versioning: crate::BucketVersioningState::Disabled,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            etag_crc64: checksum::crc64::checksum(DATA),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &self.cluster,
                &self.bucket,
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            ),
        }
    }

    fn write_command_payload(
        &self,
        identity: DirectPayloadTestIdentity,
    ) -> Result<crate::DirectPutWrittenSegment, String> {
        let written_shards = self
            .cluster
            .write_placed_segment_payload_shards(
                self.cluster
                    .validated_data_pg(PgId::new(0))
                    .map_err(diagnostic)?,
                identity.ec,
                &identity.segment_okh,
                identity.segment_vid,
                DATA,
            )
            .map_err(diagnostic)?;
        Ok(crate::DirectPutWrittenSegment {
            data_pg_id: 0,
            ec: identity.ec,
            written_shards,
        })
    }

    fn install_pending(
        &self,
        identity: DirectPayloadTestIdentity,
        written: &crate::DirectPutWrittenSegment,
        prepared: &crate::PreparedDirectPutObjectCommit,
    ) -> Result<MetadataCommandEnvelope, String> {
        let request = direct_put_commit_req_with_bucket_write_proof(
            DirectPutCommitReqFixture {
                bucket: &self.bucket,
                key: &self.key,
                reservation_id: self.reservation.clone(),
                generation_id: identity.segment_vid,
                payload: DATA,
                segment_okh: identity.segment_okh,
                written,
            },
            prepared.bucket_write_reservation.clone(),
        );
        let acks: Vec<_> = written
            .written_shards
            .iter()
            .map(|s| (&s.key, s.ack))
            .collect();
        self.cluster
            .register_payload_shard_acks(0, &acks)
            .map_err(diagnostic)?;
        let primary = self
            .map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(0))
            .map_err(diagnostic)?;
        let pg = primary.storage_node().get_pg(0).map_err(diagnostic)?;
        let command = self
            .cluster
            .prepare_commit_direct_put_object_command(
                PgId::new(0),
                &pg,
                &request,
                crate::VersionId::Null,
                request.bucket_write_reservation.clone(),
            )
            .map_err(diagnostic)?;
        drop(pg);
        insert_pending_metadata_command_for_test(&self.map, PgId::new(0), &self.bucket, &command);
        Ok(command)
    }

    fn files(
        &self,
        identity: DirectPayloadTestIdentity,
    ) -> Result<Vec<(std::path::PathBuf, Vec<u8>)>, String> {
        (0..identity.ec.k + identity.ec.m)
            .map(|index| {
                let path = self
                    .cluster
                    .test_payload_shard_file_path(
                        identity.data_pg_id,
                        identity.ec,
                        &identity.segment_okh,
                        identity.segment_vid,
                        index,
                    )
                    .map_err(diagnostic)?;
                let bytes = std::fs::read(&path).map_err(diagnostic)?;
                Ok((path, bytes))
            })
            .collect()
    }

    fn observe(
        &self,
        state: &State,
        command: &MetadataCommandEnvelope,
        caller_staged: bool,
        recovered: bool,
        command_files: &[(std::path::PathBuf, Vec<u8>)],
        caller_files: &[(std::path::PathBuf, Vec<u8>)],
    ) -> Result<(), String> {
        equal(
            pending_metadata_command_for_test(&self.map, PgId::new(0), &self.bucket).as_ref(),
            (!recovered).then_some(command),
            "exact pending command",
        )?;
        for (index, (path, bytes)) in command_files.iter().enumerate() {
            equal(
                std::fs::read(path).map_err(diagnostic)?,
                bytes.clone(),
                &format!("command shard {index} bytes survive"),
            )?;
        }
        for (index, (path, bytes)) in caller_files.iter().enumerate() {
            // Independent expected partition: not the production key-set helper.
            let shared = state.layout == Layout::PartialOverlap && index < 3;
            let expected = caller_staged || shared;
            equal(
                path.exists(),
                expected,
                &format!("caller shard {index} presence"),
            )?;
            if expected {
                equal(
                    std::fs::read(path).map_err(diagnostic)?,
                    bytes.clone(),
                    "caller bytes unchanged",
                )?;
            }
        }
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            unreachable!()
        };
        let mut proofs = Vec::new();
        for node_id in 0..4 {
            let node = self.map.node(NodeId::new(node_id)).unwrap().storage_node();
            let pg = node.get_pg(0).map_err(diagnostic)?;
            proofs.push(pg.metadata_command_replica_state().map_err(diagnostic)?);
            let reservation = match crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &self.bucket,
                &self.key,
                &self.reservation,
            ) {
                Ok(generation) => Some(generation),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                Err(error) => return Err(diagnostic(error)),
            };
            equal(
                reservation,
                (!recovered).then_some(commit.object.generation_id),
                "command reservation ownership",
            )?;
            let object = crate::PgMetadataStore::get_object_meta(&*pg, &self.bucket, &self.key);
            if recovered {
                let object = object.map_err(diagnostic)?;
                let live = object
                    .as_live()
                    .ok_or("recovery did not publish a live object")?;
                equal(
                    live.generation_id,
                    commit.object.generation_id,
                    "visible generation",
                )?;
                equal(live.size, DATA.len() as u64, "visible size")?;
                equal(
                    live.etag,
                    crate::ObjectEtag::single_part(checksum::crc64::checksum(DATA)),
                    "visible ETag",
                )?;
                equal(
                    crate::PgMetadataStore::get_object_segments(
                        &*pg,
                        &self.bucket,
                        &self.key,
                        crate::VersionId::Null,
                    )
                    .map_err(diagnostic)?,
                    commit.segments.clone(),
                    "recovered exact command layout",
                )?;
            } else if !matches!(object, Err(crate::MetadataError::ObjectNotFound)) {
                return Err(format!("mismatched request published metadata: {object:?}"));
            }
        }
        for proof in &proofs[1..] {
            equal(proof, &proofs[0], "replica proof convergence")?;
        }
        // Even before recovery, prove preserved bytes decode under the command's
        // actual EC shape, not merely that some paths still exist.
        let segment = &commit.segments[0];
        let mut body = Vec::new();
        self.cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: DATA.len(),
                    segment_crc64: segment.segment_crc64,
                    ec: COMMAND_EC,
                },
                &mut body,
            )
            .map_err(diagnostic)?;
        equal(body.as_slice(), DATA, "command payload is recoverable")
    }
}

fn replay(state: &State) -> Result<(), String> {
    if state.trace.is_empty() {
        return Ok(());
    }
    let mut fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admission = handle.admit_current_route().map_err(diagnostic)?;
    let route = admission
        .active_put_object_route(&fixture.bucket, &fixture.key)
        .map_err(diagnostic)?;
    // EC shape can change placement ordering. Select a deterministic fixture
    // with a genuine shared prefix, not merely equal keys on different nodes.
    // This bounded setup search is outside the explored operation schedule.
    let mut selected = None;
    for candidate in 0..128 {
        let reservation = crate::tests::stream_session_id(format!("overlap-{candidate}"));
        let generation = route.reserve_generation(&reservation).map_err(diagnostic)?;
        let okh = crate::direct_put_segment_key_hash(&reservation, 0);
        let mut shared_prefix = true;
        for index in 0..3 {
            let caller = fixture
                .cluster
                .test_payload_shard_file_path(0, CALLER_EC, &okh, generation, index)
                .map_err(diagnostic)?;
            let command = fixture
                .cluster
                .test_payload_shard_file_path(0, COMMAND_EC, &okh, generation, index)
                .map_err(diagnostic)?;
            shared_prefix &= caller == command;
        }
        if shared_prefix {
            fixture.reservation = reservation;
            selected = Some(generation);
            break;
        }
        route.release_generation_reservation(&reservation);
    }
    let generation =
        selected.ok_or("no shared-prefix placement in 128 deterministic candidates")?;
    let caller_okh = crate::direct_put_segment_key_hash(&fixture.reservation, 0);
    let mut command_okh = caller_okh;
    if state.layout == Layout::Disjoint {
        command_okh[0] ^= 1;
    }
    let identity = DirectPayloadTestIdentity {
        data_pg_id: 0,
        ec: COMMAND_EC,
        segment_okh: command_okh,
        segment_vid: generation,
    };
    let prepared = fixture.prepared();
    let mut payload = None;
    let mut command = None;
    let mut command_files = Vec::new();
    let mut caller_files = Vec::new();
    let mut recovered = false;
    for step in &state.trace {
        match step {
            Step::Stage => {
                let mut written = None;
                if state.command_first {
                    written = Some(fixture.write_command_payload(identity)?);
                    command_files = fixture.files(identity)?;
                }
                let staged = route
                    .write_direct_object_payload(
                        &fixture.reservation,
                        generation,
                        DATA.len() as u64,
                        DATA,
                    )
                    .map_err(diagnostic)?;
                caller_files = fixture.files(direct_payload_test_identity(&staged))?;
                if !state.command_first {
                    written = Some(fixture.write_command_payload(identity)?);
                    command_files = fixture.files(identity)?;
                }
                for (index, (caller_path, caller_bytes)) in caller_files.iter().enumerate() {
                    let shared = state.layout == Layout::PartialOverlap && index < 3;
                    equal(
                        command_files.iter().any(|(path, _)| path == caller_path),
                        shared,
                        "fixture has exact intended physical overlap",
                    )?;
                    if shared {
                        equal(
                            caller_bytes,
                            &command_files[index].1,
                            "shared EC bytes are identical",
                        )?;
                    }
                }
                command = Some(fixture.install_pending(
                    identity,
                    written.as_ref().unwrap(),
                    &prepared,
                )?);
                payload = Some(staged);
            }
            Step::Reject => {
                let calls = std::cell::Cell::new(0);
                let error = route
                    .commit_direct_object(payload.take().unwrap(), &prepared, |_| {
                        calls.set(calls.get() + 1);
                        Ok::<(), ()>(())
                    })
                    .err()
                    .ok_or("physically mismatched command accepted")?;
                equal(
                    error.kind(),
                    crate::DirectPutFailureKind::MetadataCommandContention,
                    "mismatch outcome",
                )?;
                equal(
                    error.diagnostic_cause_label(),
                    "store_metadata_command_contention",
                    "mismatch diagnostic",
                )?;
                equal(
                    calls.get(),
                    0,
                    "mismatch must reject before recovery/authorization",
                )?;
            }
            Step::Restage => {
                payload = Some(
                    route
                        .write_direct_object_payload(
                            &fixture.reservation,
                            generation,
                            DATA.len() as u64,
                            DATA,
                        )
                        .map_err(diagnostic)?,
                );
            }
            Step::Recover => {
                equal(
                    fixture
                        .cluster
                        .drain_pending_metadata_command_with_authorized_recovery_route(
                            PgId::new(0),
                            command.as_ref().unwrap(),
                            &fixture.cluster,
                        )
                        .map_err(diagnostic)?,
                    PendingMetadataCommandOutcome::Applied,
                    "exact command recovery",
                )?;
                recovered = true;
            }
        }
        fixture.observe(
            state,
            command.as_ref().unwrap(),
            payload.is_some(),
            recovered,
            &command_files,
            &caller_files,
        )?;
    }
    // Prefix replay ends by destroying the whole isolated fixture, not by
    // scheduling cancellation of a live request sharing pending-command keys.
    if let Some(payload) = payload {
        payload.disarm();
    }
    Ok(())
}

#[test]
fn publication_model_exhausts_direct_payload_overlap_cleanup_schedules() {
    let failure = RefCell::new(None);
    let initials = [Layout::PartialOverlap, Layout::Disjoint]
        .into_iter()
        .flat_map(|layout| {
            [false, true].into_iter().map(move |command_first| State {
                layout,
                command_first,
                trace: Vec::new(),
            })
        });
    let explored = explore(
        initials,
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
                    failure: Some("per-shard cleanup preserves recoverable pending ownership"),
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
        panic!("overlap counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 28);
    assert_eq!(explored.transitions, 24);
    for name in REQUIRED {
        let trace = explored.witness(name).unwrap();
        let state = explored
            .states()
            .find(|s| s.trace == trace && s.witnesses().contains(&name))
            .unwrap();
        replay(state).unwrap();
        eprintln!("{name}: {state:?}");
    }
}
