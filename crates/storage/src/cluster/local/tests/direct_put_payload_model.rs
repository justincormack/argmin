// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded ownership schedules over real admitted PUT handles and shard files.
//! A matching pending command is installed through production preparation/store
//! APIs; the only injected fault is a failed subsequent abandonment inspection.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use std::cell::RefCell;

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
            _directory: directory,
        }
    }

    fn prepared(&self, owner: usize) -> crate::PreparedDirectPutObjectCommit {
        crate::PreparedDirectPutObjectCommit {
            versioning: crate::BucketVersioningState::Disabled,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            etag_crc64: checksum::crc64::checksum(DATA[owner]),
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

    fn install_matching_pending(
        &self,
        owner: usize,
        payload: &crate::DirectPutPayloadWrite<'_>,
        prepared: &crate::PreparedDirectPutObjectCommit,
    ) -> Result<MetadataCommandEnvelope, String> {
        let request = direct_put_commit_req_with_bucket_write_proof(
            DirectPutCommitReqFixture {
                bucket: &self.bucket,
                key: &self.key,
                reservation_id: self.reservations[owner].clone(),
                generation_id: payload.generation_id,
                payload: DATA[owner],
                segment_okh: payload.segment_okh,
                written: &payload.written,
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
        insert_pending_metadata_command_for_test(&self.map, PgId::new(0), &self.bucket, &command);
        Ok(command)
    }

    fn observe(
        &self,
        ownership: [Ownership; 2],
        identities: &[Option<DirectPayloadTestIdentity>; 2],
        pending: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), String> {
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
                let expected = if matches!(ownership[owner], Ownership::Caller | Ownership::Pending)
                {
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
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                            version_id: crate::VersionId::Null,
                            segment_index: 0,
                            size: DATA[owner].len() as u64,
                            segment_crc64: checksum::crc64::checksum(DATA[owner]),
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
            let mut bytes = Vec::new();
            self.cluster
                .read_segment_payload_stored_bytes_into(
                    crate::SegmentStoredBytesRequest {
                        data_pg_id: identity.data_pg_id,
                        segment_okh: identity.segment_okh,
                        segment_vid: identity.segment_vid,
                        stored_size: DATA[owner].len(),
                        segment_crc64: checksum::crc64::checksum(DATA[owner]),
                        ec: identity.ec,
                    },
                    &mut bytes,
                )
                .map_err(diagnostic)?;
            equal(
                bytes.as_slice(),
                DATA[owner],
                "visible metadata has readable exact payload",
            )?;
        }
        Ok(())
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
                    let generation = routes[owner]
                        .reserve_generation(&fixture.reservations[owner])
                        .map_err(diagnostic)?;
                    let payload = routes[owner]
                        .write_direct_object_payload(
                            &fixture.reservations[owner],
                            generation,
                            DATA[owner].len() as u64,
                            DATA[owner],
                        )
                        .map_err(diagnostic)?;
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
                let prepared = fixture.prepared(owner);
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
                let prepared = fixture.prepared(owner);
                let payload = payloads[owner].take().unwrap();
                let command = fixture.install_matching_pending(owner, &payload, &prepared)?;
                let expected_command = command.clone();
                let calls = Arc::new(AtomicUsize::new(0));
                let observed_calls = Arc::clone(&calls);
                let hook = fixture
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
                let result =
                    routes[owner].commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()));
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
