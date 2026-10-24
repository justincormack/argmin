// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded ownership schedules over real admitted PUT handles and shard files.
//! A matching pending command is installed through production preparation/store
//! APIs; the only injected fault is a failed subsequent abandonment inspection.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use std::cell::RefCell;

#[path = "direct_put_cancellation_model.rs"]
mod cancellation_model;

#[path = "direct_put_cleanup_progress.rs"]
mod cleanup_progress;

const DATA: [&[u8]; 2] = [b"owner A staged body", b"owner B different staged body"];
const REQUIRED: [&str; 6] = [
    "ordinary drop releases only its own staging",
    "explicit discard releases only its own staging",
    "losing conditional commit preserves visible payload",
    "pending inspection failure preserves both staged payloads",
    "owner A recovers before other request cancellation",
    "owner B recovers before other request cancellation",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum FirstOutcome {
    Drop,
    Discard,
    Commit,
    PendingInspectionFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum OtherOutcome {
    Drop,
    CommitIfAbsent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    StageBoth,
    SettleFirst(FirstOutcome),
    Recover,
    SettleOther(OtherOutcome),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    owner: usize,
    trace: Vec<Step>,
}

impl State {
    fn successors(&self) -> Vec<(Step, Self)> {
        let steps = match self.trace.as_slice() {
            [] => vec![Step::StageBoth],
            [Step::StageBoth] => [
                FirstOutcome::Drop,
                FirstOutcome::Discard,
                FirstOutcome::Commit,
                FirstOutcome::PendingInspectionFailure,
            ]
            .into_iter()
            .map(Step::SettleFirst)
            .collect(),
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::PendingInspectionFailure)] => {
                vec![Step::Recover]
            }
            [Step::StageBoth, Step::SettleFirst(_)]
            | [Step::StageBoth, Step::SettleFirst(FirstOutcome::PendingInspectionFailure), Step::Recover] =>
            {
                vec![
                    Step::SettleOther(OtherOutcome::Drop),
                    Step::SettleOther(OtherOutcome::CommitIfAbsent),
                ]
            }
            [Step::StageBoth, Step::SettleFirst(_), Step::SettleOther(_)]
            | [Step::StageBoth, Step::SettleFirst(FirstOutcome::PendingInspectionFailure), Step::Recover, Step::SettleOther(_)] => {
                Vec::new()
            }
            _ => panic!("invalid payload trace: {:?}", self.trace),
        };
        steps
            .into_iter()
            .map(|step| {
                let mut state = self.clone();
                state.trace.push(step);
                (step, state)
            })
            .collect()
    }

    fn witnesses(&self) -> Vec<&'static str> {
        match self.trace.as_slice() {
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::Drop)] => vec![REQUIRED[0]],
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::Discard)] => vec![REQUIRED[1]],
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::Commit), Step::SettleOther(OtherOutcome::CommitIfAbsent)] =>
            {
                vec![REQUIRED[2]]
            }
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::PendingInspectionFailure)] => {
                vec![REQUIRED[3]]
            }
            [Step::StageBoth, Step::SettleFirst(FirstOutcome::PendingInspectionFailure), Step::Recover, Step::SettleOther(OtherOutcome::Drop)] =>
            {
                vec![REQUIRED[4 + self.owner]]
            }
            _ => Vec::new(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ownership {
    Unstaged,
    Caller,
    Pending,
    Visible,
    Released,
    CancelledPendingRelease,
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
    reservations: [crate::SessionId; 2],
    encrypted: [crate::tests::SseS3SegmentTestFixture; 2],
    shard_integrity: RefCell<[Option<crate::types::SegmentShardIntegrityRecord>; 2]>,
    _directory: test_util::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let directory = test_util::tempdir();
        let map = Arc::new(
            LocalClusterMap::open(
                directory.path(),
                &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
                &[0],
                EcShape { k: 2, m: 1 },
            )
            .unwrap(),
        );
        let cluster = current_cluster(&map);
        let bucket = crate::tests::bucket_name("payload-ownership-model");
        let key = crate::tests::object_key("object");
        create_test_bucket(&cluster, &bucket);
        Self {
            map,
            cluster,
            bucket,
            key,
            reservations: [
                crate::tests::stream_session_id("payload-a"),
                crate::tests::stream_session_id("payload-b"),
            ],
            encrypted: [
                crate::tests::sse_s3_segment_test_fixture(DATA[0], 0),
                crate::tests::sse_s3_segment_test_fixture(DATA[1], 0),
            ],
            shard_integrity: RefCell::new([None, None]),
            _directory: directory,
        }
    }

    fn record_payload_integrity(
        &self,
        owner: usize,
        payload: &crate::DirectPutPayloadWrite<'_>,
    ) -> Result<(), String> {
        let encrypted = &self.encrypted[owner];
        let shard_batch = payload
            .written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        self.shard_integrity.borrow_mut()[owner] = Some(
            crate::types::SegmentShardIntegrityWitness::from_ordered_shard_acks(
                payload.written.data_pg_id,
                payload.written.ec,
                &payload.segment_okh,
                payload.segment_vid,
                encrypted.storage_bytes.len() as u64,
                encrypted.storage_crc64,
                &shard_batch,
            )
            .map_err(diagnostic)?
            .durable_record(),
        );
        Ok(())
    }

    fn committed_encryption(
        &self,
        owner: usize,
        generation_id: crate::GenerationId,
    ) -> crate::ObjectEncryption {
        let snapshot = self
            .cluster
            .load_bucket_snapshot(&self.bucket, crate::BucketSnapshotRequest::default())
            .unwrap();
        committed_test_object_encryption_for_incarnation(
            &self.encrypted[owner],
            snapshot
                .bucket_incarnation_read_subject()
                .bucket_incarnation_generation(),
            &self.bucket,
            &self.key,
            generation_id,
        )
    }

    fn prepared(
        &self,
        owner: usize,
        generation_id: crate::GenerationId,
    ) -> crate::PreparedDirectPutObjectCommit {
        let mut prepared = prepared_encrypted_direct_put(
            &self.cluster,
            &self.bucket,
            &self.key,
            crate::BucketVersioningState::Disabled,
            DATA[owner],
            &self.encrypted[owner],
        );
        prepared.encryption = self.committed_encryption(owner, generation_id);
        prepared
    }

    fn install_matching_pending(
        &self,
        owner: usize,
        payload: &crate::DirectPutPayloadWrite<'_>,
        prepared: &crate::PreparedDirectPutObjectCommit,
    ) -> Result<MetadataCommandEnvelope, String> {
        let request = encrypted_direct_put_commit_req_with_bucket_write_proof(
            EncryptedDirectPutCommitReqFixture {
                bucket: &self.bucket,
                key: &self.key,
                reservation_id: self.reservations[owner].clone(),
                generation_id: payload.generation_id,
                versioning: crate::BucketVersioningState::Disabled,
                segment_okh: payload.segment_okh,
                written: &payload.written,
                plaintext: DATA[owner],
                encrypted: &self.encrypted[owner],
            },
            prepared.bucket_write_reservation.clone(),
        );
        let acks: Vec<_> = payload
            .written
            .written_shards
            .iter()
            .map(|shard| (&shard.key, shard.ack))
            .collect();
        self.cluster
            .register_payload_shard_acks(request.data_pg_id, &acks)
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
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err("direct PUT preparation returned the wrong command kind".into());
        };
        let mut commit = commit.as_ref().clone();
        commit.segments[0].shard_integrity = self.shard_integrity.borrow()[owner].clone();
        let command = MetadataCommandEnvelope::new(
            command.id(),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(commit)),
        );
        insert_pending_metadata_command_for_test(&self.map, PgId::new(0), &self.bucket, &command);
        Ok(command)
    }

    fn observe(
        &self,
        ownership: [Ownership; 2],
        identities: &[Option<DirectPayloadTestIdentity>; 2],
        pending: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), String> {
        let shard_integrity = self.shard_integrity.borrow();
        equal(
            pending_metadata_command_for_test(&self.map, PgId::new(0), &self.bucket).as_ref(),
            pending,
            "exact pending envelope",
        )?;
        for (owner, identity) in identities.iter().enumerate() {
            if let Some(identity) = identity {
                let expected = matches!(
                    ownership[owner],
                    Ownership::Caller | Ownership::Pending | Ownership::Visible
                );
                for shard in 0..identity.ec.k + identity.ec.m {
                    equal(
                        self.cluster
                            .test_payload_shard_file_exists(
                                identity.data_pg_id,
                                identity.ec,
                                &identity.segment_okh,
                                identity.segment_vid,
                                shard,
                            )
                            .map_err(diagnostic)?,
                        expected,
                        &format!("owner {owner} physical shard {shard}"),
                    )?;
                }
            }
        }
        let visible = ownership
            .iter()
            .position(|owner| *owner == Ownership::Visible);
        let mut proofs = Vec::new();
        for node_id in 0..3 {
            let node = self.map.node(NodeId::new(node_id)).unwrap().storage_node();
            let pg = node.get_pg(0).map_err(diagnostic)?;
            proofs.push(pg.metadata_command_replica_state().map_err(diagnostic)?);
            for owner in 0..2 {
                let actual = match crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &self.bucket,
                    &self.key,
                    &self.reservations[owner],
                ) {
                    Ok(generation) => Some(generation),
                    Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                    Err(error) => return Err(diagnostic(error)),
                };
                let expected = if matches!(
                    ownership[owner],
                    Ownership::Caller | Ownership::Pending | Ownership::CancelledPendingRelease
                ) {
                    Some(identities[owner].unwrap().segment_vid)
                } else {
                    None
                };
                equal(
                    actual,
                    expected,
                    &format!("node {node_id} owner {owner} reservation"),
                )?;
            }
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &self.bucket, &self.key);
            match visible {
                None => {
                    if !matches!(stored, Err(crate::MetadataError::ObjectNotFound)) {
                        return Err(format!("unexpected visible metadata: {stored:?}"));
                    }
                }
                Some(owner) => {
                    let encrypted = &self.encrypted[owner];
                    let stored = stored.map_err(diagnostic)?;
                    let live = stored.as_live().ok_or("expected live object")?;
                    let identity = identities[owner].unwrap();
                    equal(
                        live.generation_id,
                        identity.segment_vid,
                        "visible owner generation",
                    )?;
                    equal(live.size, DATA[owner].len() as u64, "visible object size")?;
                    equal(
                        live.etag,
                        crate::ObjectEtag::single_part(checksum::crc64::checksum(DATA[owner])),
                        "visible ETag",
                    )?;
                    let segments = crate::PgMetadataStore::get_object_segments(
                        &*pg,
                        &self.bucket,
                        &self.key,
                        crate::VersionId::Null,
                    )
                    .map_err(diagnostic)?;
                    equal(
                        segments,
                        vec![crate::ObjectSegmentRecord {
                            shard_integrity: shard_integrity[owner].clone(),
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                            version_id: crate::VersionId::Null,
                            segment_index: 0,
                            size: DATA[owner].len() as u64,
                            segment_crc64: encrypted.storage_crc64,
                            segment_okh: identity.segment_okh,
                            segment_vid: identity.segment_vid,
                            placement_cluster_epoch: ClusterEpoch::INITIAL,
                            data_pg_id: identity.data_pg_id,
                            ec_k: identity.ec.k,
                            ec_m: identity.ec.m,
                        }],
                        "visible exact physical layout",
                    )?;
                }
            }
        }
        equal(proofs[0], proofs[1], "witness metadata proof")?;
        equal(proofs[0], proofs[2], "trailing metadata proof")?;
        if let Some(owner) = visible {
            let identity = identities[owner].unwrap();
            let encrypted = &self.encrypted[owner];
            let mut bytes = Vec::new();
            self.cluster
                .read_segment_payload_stored_bytes_into(
                    crate::SegmentStoredBytesRequest {
                        data_pg_id: identity.data_pg_id,
                        segment_okh: identity.segment_okh,
                        segment_vid: identity.segment_vid,
                        stored_size: encrypted.storage_bytes.len(),
                        segment_crc64: encrypted.storage_crc64,
                        ec: identity.ec,
                    },
                    &mut bytes,
                )
                .map_err(diagnostic)?;
            equal(
                bytes.as_slice(),
                encrypted.storage_bytes.as_slice(),
                "visible metadata has readable exact payload",
            )?;
        }
        Ok(())
    }

    fn leave_pending_after_inspection_failure(
        &self,
        owner: usize,
        route: &crate::ActivePutObjectRoute<'_>,
        payload: crate::DirectPutPayloadWrite<'_>,
    ) -> Result<MetadataCommandEnvelope, String> {
        let prepared = self.prepared(owner, payload.generation_id);
        self.leave_pending_with_prepared_after_inspection_failure(owner, route, payload, &prepared)
    }

    fn leave_pending_with_prepared_after_inspection_failure(
        &self,
        owner: usize,
        route: &crate::ActivePutObjectRoute<'_>,
        payload: crate::DirectPutPayloadWrite<'_>,
        prepared: &crate::PreparedDirectPutObjectCommit,
    ) -> Result<MetadataCommandEnvelope, String> {
        let command = self.install_matching_pending(owner, &payload, prepared)?;
        let expected_command = command.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);
        let hook = self
            .cluster
            .test_install_before_direct_put_abandoned_log_inspection_hook(Arc::new(
                move |candidate| {
                    assert_eq!(
                        candidate, &expected_command,
                        "inspection changed exact command"
                    );
                    observed_calls.fetch_add(1, Ordering::SeqCst);
                    Err(StoreError::MetadataCommandLogChecksumMismatch {
                        node_id: 0,
                        pg_id: 0,
                        cluster_epoch: candidate.id().cluster_epoch(),
                        log_index: candidate.id().log_index().get(),
                        stored_checksum: 1,
                        computed_checksum: 2,
                    }
                    .into())
                },
            ));
        let result = route.commit_direct_object(payload, prepared, |_| Ok::<(), ()>(()));
        drop(hook);
        let error = result
            .err()
            .ok_or("pending inspection failure returned success")?;
        equal(
            error.diagnostic_cause_label(),
            "store_integrity_failure",
            "inspection error identity",
        )?;
        equal(
            calls.load(Ordering::SeqCst),
            1,
            "deterministic fatal inspection",
        )?;
        Ok(command)
    }
}

fn replay(state: &State) -> Result<(), String> {
    let fixture = Fixture::new();
    let handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&fixture.cluster));
    let admissions = [
        handle.admit_current_route().map_err(diagnostic)?,
        handle.admit_current_route().map_err(diagnostic)?,
    ];
    let routes = [
        admissions[0]
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .map_err(diagnostic)?,
        admissions[1]
            .active_put_object_route(&fixture.bucket, &fixture.key)
            .map_err(diagnostic)?,
    ];
    let mut payloads = [None, None];
    let mut identities = [None, None];
    let mut ownership = [Ownership::Unstaged; 2];
    let mut pending = None;
    fixture.observe(ownership, &identities, pending.as_ref())?;
    for step in &state.trace {
        match *step {
            Step::StageBoth => {
                for owner in 0..2 {
                    let encrypted = &fixture.encrypted[owner];
                    let generation = routes[owner]
                        .test_reserve_generation_with_id(&fixture.reservations[owner])
                        .map_err(diagnostic)?;
                    let generation_id = generation.generation_id();
                    let committed_encryption = fixture.committed_encryption(owner, generation_id);
                    let encryption_reservation = generation
                        .bind_encryption(&encrypted.encryption)
                        .bind_standard_segment(encrypted.logical_size)
                        .bind_committed_encryption(&committed_encryption)
                        .map_err(diagnostic)?;
                    let payload = routes[owner]
                        .write_direct_object_segment_payload(
                            encryption_reservation,
                            &encrypted.storage_bytes,
                        )
                        .map_err(diagnostic)?;
                    fixture.record_payload_integrity(owner, &payload)?;
                    identities[owner] = Some(direct_payload_test_identity(&payload));
                    payloads[owner] = Some(payload);
                    ownership[owner] = Ownership::Caller;
                }
                if identities[0].unwrap().segment_vid == identities[1].unwrap().segment_vid {
                    return Err("competing requests share a generation".into());
                }
            }
            Step::SettleFirst(FirstOutcome::Drop) | Step::SettleOther(OtherOutcome::Drop) => {
                let owner = if matches!(step, Step::SettleFirst(_)) {
                    state.owner
                } else {
                    1 - state.owner
                };
                drop(payloads[owner].take().unwrap());
                ownership[owner] = Ownership::Released;
            }
            Step::SettleFirst(FirstOutcome::Discard) => {
                routes[state.owner]
                    .discard_direct_object_payload(payloads[state.owner].take().unwrap())
                    .map_err(diagnostic)?;
                ownership[state.owner] = Ownership::Released;
            }
            Step::SettleFirst(FirstOutcome::Commit)
            | Step::SettleOther(OtherOutcome::CommitIfAbsent) => {
                let owner = if matches!(step, Step::SettleFirst(_)) {
                    state.owner
                } else {
                    1 - state.owner
                };
                let already_visible = ownership.contains(&Ownership::Visible);
                let prepared =
                    fixture.prepared(owner, payloads[owner].as_ref().unwrap().generation_id);
                let result = routes[owner]
                    .commit_direct_object(payloads[owner].take().unwrap(), &prepared, |snapshot| {
                        if snapshot.existing_etag.is_some() {
                            Err("already visible")
                        } else {
                            Ok(())
                        }
                    })
                    .map_err(diagnostic)?;
                if already_visible {
                    equal(
                        result.err(),
                        Some("already visible"),
                        "losing condition retains callback error",
                    )?;
                    ownership[owner] = Ownership::Released;
                } else {
                    let outcome = result.map_err(diagnostic)?;
                    equal(
                        outcome.live_size,
                        DATA[owner].len() as u64,
                        "committed size",
                    )?;
                    ownership[owner] = Ownership::Visible;
                }
            }
            Step::SettleFirst(FirstOutcome::PendingInspectionFailure) => {
                let owner = state.owner;
                let payload = payloads[owner].take().unwrap();
                let command = fixture.leave_pending_after_inspection_failure(
                    owner,
                    &routes[owner],
                    payload,
                )?;
                ownership[owner] = Ownership::Pending;
                pending = Some(command);
            }
            Step::Recover => {
                let command = pending.as_ref().unwrap();
                equal(
                    fixture
                        .cluster
                        .drain_pending_metadata_command_with_authorized_recovery_route(
                            PgId::new(0),
                            command,
                            &fixture.cluster,
                        )
                        .map_err(diagnostic)?,
                    PendingMetadataCommandOutcome::Applied,
                    "healthy authorized exact-command recovery",
                )?;
                pending = None;
                ownership[state.owner] = Ownership::Visible;
            }
        }
        fixture.observe(ownership, &identities, pending.as_ref())?;
    }
    Ok(())
}

#[test]
fn publication_model_exhausts_direct_payload_ownership_and_cleanup_schedules() {
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
                    failure: Some("cleanup respects caller and durable-command payload ownership"),
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
        panic!("payload ownership counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 30);
    assert_eq!(explored.transitions, 28);
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
