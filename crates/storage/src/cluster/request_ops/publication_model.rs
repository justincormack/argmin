// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded replay through the real publisher attempt and pending-command
//! finisher. Hook failures separate pre-application failure from lost replies
//! after durable application; they do not model socket/authentication failures.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use crate::cluster::local::LocalClusterMap;
use crate::metadata_command::{
    CreateBucketCommand, MetadataCommandLogIndex, PendingMetadataCommandInspection,
    ReserveObjectGenerationCommand,
};
use crate::{EcShape, GenerationId, SessionId};
use std::cell::RefCell;

#[path = "publication_retry_model.rs"]
mod retry_model;

// Node identities are fixed independently of the publisher's ordering helper.
const PRIMARY: u32 = 0;
const WITNESS: u32 = 1;
const TRAILING: u32 = 2;
const PG: PgId = PgId::new(0);
const REQUIRED: [&str; 5] = [
    "A retained reservation after lost reply, frontend replacement and recovery",
    "B retained reservation after lost reply, frontend replacement and recovery",
    "witness reply lost before primary application",
    "primary reply lost with durable publication",
    "trailing reply lost with all replicas durable",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Boundary {
    Before(u32),
    After(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Attempt(Option<Boundary>),
    ReplaceFrontend,
    Recover,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    winner: usize,
    // Full trace identity avoids merging opaque cluster/runtime/database state.
    trace: Vec<Action>,
}

impl State {
    fn successors(&self) -> Vec<(Action, Self)> {
        let actions = match self.trace.as_slice() {
            [] => vec![
                Action::Attempt(None),
                Action::Attempt(Some(Boundary::Before(WITNESS))),
                Action::Attempt(Some(Boundary::After(WITNESS))),
                Action::Attempt(Some(Boundary::Before(PRIMARY))),
                Action::Attempt(Some(Boundary::After(PRIMARY))),
                Action::Attempt(Some(Boundary::Before(TRAILING))),
                Action::Attempt(Some(Boundary::After(TRAILING))),
            ],
            [Action::Attempt(_)] => vec![Action::ReplaceFrontend, Action::Recover],
            [Action::Attempt(_), Action::ReplaceFrontend] => vec![Action::Recover],
            [Action::Attempt(_), Action::Recover]
            | [Action::Attempt(_), Action::ReplaceFrontend, Action::Recover] => Vec::new(),
            _ => panic!("invalid model trace: {:?}", self.trace),
        };
        actions
            .into_iter()
            .map(|action| {
                let mut child = self.clone();
                child.trace.push(action);
                (action, child)
            })
            .collect()
    }

    fn witnesses(&self) -> Vec<&'static str> {
        match self.trace.as_slice() {
            [Action::Attempt(Some(Boundary::After(_))), Action::ReplaceFrontend, Action::Recover] =>
            {
                vec![REQUIRED[self.winner]]
            }
            [Action::Attempt(Some(Boundary::After(WITNESS)))] => vec![REQUIRED[2]],
            [Action::Attempt(Some(Boundary::After(PRIMARY)))] => vec![REQUIRED[3]],
            [Action::Attempt(Some(Boundary::After(TRAILING)))] => vec![REQUIRED[4]],
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
    context: &str,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}: got {actual:?}, expected {expected:?}"))
    }
}

// Independent specification of the one-attempt delivery prefix. In particular,
// an After fault has a durable effect even though no acknowledgement arrives.
fn expected_prefix(fault: Option<Boundary>) -> ([bool; 3], bool, Vec<Boundary>) {
    let mut applied = [false; 3];
    let mut marked = false;
    let mut events = Vec::new();
    for replica in [WITNESS, PRIMARY, TRAILING] {
        let before = Boundary::Before(replica);
        events.push(before);
        if fault == Some(before) {
            break;
        }
        marked = true;
        applied[replica as usize] = true;
        let after = Boundary::After(replica);
        events.push(after);
        if fault == Some(after) {
            break;
        }
    }
    (applied, marked, events)
}

fn check_attempt(
    result: Result<(), MetadataCommandApplyAttemptFailure>,
    fault: Option<Boundary>,
) -> Result<(), String> {
    let Some(fault) = fault else {
        return result.map_err(diagnostic);
    };
    let failure = result
        .err()
        .ok_or_else(|| format!("fault {fault:?} incorrectly returned success"))?;
    let (count, progress, ambiguous) = match fault {
        Boundary::Before(WITNESS) => (0, MetadataCommandApplyProgress::Abortable, false),
        Boundary::After(WITNESS) => (0, MetadataCommandApplyProgress::Witnessed, true),
        Boundary::Before(PRIMARY) => (1, MetadataCommandApplyProgress::Witnessed, false),
        Boundary::After(PRIMARY) => (
            1,
            MetadataCommandApplyProgress::PublicationUnconfirmed,
            true,
        ),
        Boundary::Before(TRAILING) => (2, MetadataCommandApplyProgress::Published, false),
        Boundary::After(TRAILING) => (2, MetadataCommandApplyProgress::Published, true),
        Boundary::Before(_) | Boundary::After(_) => unreachable!("fixed three-replica model"),
    };
    equal(
        failure.failure.applied_nodes,
        count,
        "acknowledged application count",
    )?;
    equal(
        failure.failure.progress,
        progress,
        "publication classification",
    )?;
    equal(
        failure.failure.may_have_applied,
        ambiguous,
        "request-publication ambiguity",
    )?;
    equal(
        failure.apply_error_kind,
        ambiguous.then_some(MetadataCommandApplyErrorKind::MayHaveApplied),
        "dispatch classification",
    )?;
    match failure.failure.source {
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::TransportClosed,
            ..
        }) => Ok(()),
        source => Err(format!("unexpected failure source: {source:?}")),
    }
}

struct Fixture {
    map: Arc<LocalClusterMap>,
    cluster: Arc<StorageCluster>,
    command: MetadataCommandEnvelope,
    bucket: crate::BucketName,
    key: crate::ObjectKey,
    reservations: [SessionId; 2],
    // Keep the directory alive until the stores and cluster are closed.
    _directory: test_util::TempDir,
}

impl Fixture {
    fn new(winner: usize) -> Self {
        let directory = test_util::tempdir();
        let map = Arc::new(
            LocalClusterMap::open(
                directory.path(),
                &[
                    NodeId::new(PRIMARY),
                    NodeId::new(WITNESS),
                    NodeId::new(TRAILING),
                ],
                &[0],
                EcShape { k: 2, m: 1 },
            )
            .unwrap(),
        );
        assert_eq!(
            map.metadata_pg_primary_node(ClusterEpoch::INITIAL, PG)
                .unwrap()
                .node_id(),
            NodeId::new(PRIMARY)
        );
        let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let bucket = crate::tests::bucket_name("publisher-model");
        let key = crate::tests::object_key("object");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let grants = crate::AclGrants::default();
        let config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
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
                ClusterEpoch::INITIAL,
                PG,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&config, 123, 1).unwrap(),
            ),
        );
        for replica in 0..3 {
            map.node(NodeId::new(replica))
                .unwrap()
                .test_node()
                .get_pg(0)
                .unwrap()
                .apply_metadata_command_and_record(replica, &seed)
                .unwrap();
        }
        let reservations = [
            crate::tests::stream_session_id("publisher-a"),
            crate::tests::stream_session_id("publisher-b"),
        ];
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PG,
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservations[winner].clone(),
                GenerationId::new(winner as u64 + 1).unwrap(),
                123,
            )),
        );
        map.node(NodeId::new(PRIMARY))
            .unwrap()
            .test_node()
            .get_pg(0)
            .unwrap()
            .try_insert_pending_metadata_command_slot(PRIMARY, &command, Some(&bucket))
            .unwrap();
        Self {
            map,
            cluster,
            command,
            bucket,
            key,
            reservations,
            _directory: directory,
        }
    }

    fn observe(
        &self,
        winner: usize,
        applied: [bool; 3],
        marked: bool,
        cleared: bool,
    ) -> Result<(), String> {
        let mut proofs = Vec::new();
        for replica in 0..3 {
            let pg = self
                .map
                .node(NodeId::new(replica))
                .unwrap()
                .test_node()
                .get_pg(0)
                .map_err(diagnostic)?;
            let expected = if replica == PRIMARY && !cleared {
                PendingMetadataCommandInspection::Present {
                    command: Box::new(self.command.clone()),
                    publication_started: marked,
                }
            } else {
                PendingMetadataCommandInspection::Absent
            };
            equal(
                pg.pending_metadata_command_inspection(replica, ClusterEpoch::INITIAL)
                    .map_err(diagnostic)?,
                expected,
                "durable slot",
            )?;
            let proof = pg.metadata_command_replica_state().map_err(diagnostic)?;
            equal(
                proof.applied_log_index,
                if applied[replica as usize] { 2 } else { 1 },
                "durable application prefix",
            )?;
            proofs.push(proof);
            for request in 0..2 {
                let value = match pg.get_object_generation_reservation(
                    &self.bucket,
                    &self.key,
                    &self.reservations[request],
                ) {
                    Ok(value) => Some(value.get()),
                    Err(MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                    Err(error) => return Err(diagnostic(error)),
                };
                equal(
                    value,
                    (request == winner && applied[replica as usize]).then_some(request as u64 + 1),
                    "reservation identity survives acknowledgement loss and draining",
                )?;
            }
        }
        if applied == [true; 3] {
            equal(proofs[0], proofs[1], "witness convergence proof")?;
            equal(proofs[0], proofs[2], "trailing convergence proof")?;
        }
        Ok(())
    }

    fn attempt(&self, fault: Option<Boundary>) -> Result<(), String> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook = |after: bool| {
            let events = Arc::clone(&events);
            let command = self.command.clone();
            Arc::new(move |node: NodeId, candidate: &MetadataCommandEnvelope| {
                assert_eq!(
                    candidate, &command,
                    "publisher changed exact command identity"
                );
                let boundary = if after {
                    Boundary::After(node.as_u32())
                } else {
                    Boundary::Before(node.as_u32())
                };
                events.lock().unwrap().push(boundary);
                if fault == Some(boundary) {
                    Err(StoreError::StorageRpc {
                        node_id: node.as_u32(),
                        operation: "publication model",
                        failure: StorageRpcErrorCode::TransportClosed,
                        detail: crate::StorageNodeFailureDetail::new(
                            "model-controlled delivery failure",
                        ),
                    })
                } else {
                    Ok(())
                }
            })
        };
        let before = self
            .cluster
            .test_install_before_metadata_command_apply_hook(hook(false));
        let after = self
            .cluster
            .test_install_after_metadata_command_apply_hook(hook(true));
        // A single attempt exposes the boundary without retry sleeps or timing
        // races. This generous deadline is a guard, not a scheduled model event.
        let result = self.cluster.apply_metadata_command_to_acting_set_once(
            NodeId::new(PRIMARY),
            &self.command,
            MetadataCommandExecutionRoute::normal(),
            &self.cluster,
            MetadataCommandApplyAttemptContext {
                progress: MetadataCommandApplyProgress::Abortable,
                deadline: Instant::now() + Duration::from_secs(600),
                provenance: MetadataCommandApplyProgressProvenance::Authoritative,
                publication_start: MetadataCommandPublicationStartPolicy::Required,
            },
        );
        drop(after);
        drop(before);
        equal(
            events.lock().unwrap().clone(),
            expected_prefix(fault).2,
            "actual publisher ordering and failure boundary",
        )?;
        check_attempt(result, fault)
    }
}

fn replay(state: &State) -> Result<(), String> {
    let mut fixture = Fixture::new(state.winner);
    let mut applied = [false; 3];
    let mut marked = false;
    let mut cleared = false;
    fixture.observe(state.winner, applied, marked, cleared)?;
    for action in &state.trace {
        match *action {
            Action::Attempt(fault) => {
                fixture.attempt(fault)?;
                (applied, marked, _) = expected_prefix(fault);
            }
            Action::ReplaceFrontend => {
                fixture.cluster = StorageCluster::from_static_local_map(Arc::clone(&fixture.map))
                    .map_err(diagnostic)?;
            }
            Action::Recover => {
                let mut budget = RequestWorkBudget::new(Duration::from_secs(600), None);
                let outcome = fixture
                    .cluster
                    .finish_pending_metadata_command_to_acting_set_with_work_budget(
                        PG,
                        &fixture.command,
                        false,
                        &mut budget,
                    )
                    .map_err(diagnostic)?;
                equal(
                    outcome,
                    PendingMetadataCommandOutcome::Applied,
                    "exact pending command converges",
                )?;
                applied = [true; 3];
                marked = true;
                cleared = true;
            }
        }
        fixture.observe(state.winner, applied, marked, cleared)?;
    }
    Ok(())
}

#[test]
fn publication_model_exhausts_publisher_acknowledgement_loss_and_recovery() {
    let failure = RefCell::new(None);
    let explored = explore(
        (0..2).map(|winner| State {
            winner,
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
                    failure: Some(
                        "publisher preserves exact durable ownership across lost acknowledgement",
                    ),
                    witnesses: Vec::new(),
                }
            }
        },
        1_000,
    );
    if let Some(trace) = explored.counterexample() {
        let (state, error) = failure.into_inner().unwrap();
        assert_eq!(trace, state.trace);
        assert_eq!(replay(&state).unwrap_err(), error, "failure must replay");
        panic!("publisher counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 58);
    assert_eq!(explored.transitions, 56);
    // The explorer trace does not carry an initial-state identifier. Find the
    // actual winning initial state when replaying each required witness.
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
