// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! First publication-model increment: real PG transactions under a bounded
//! delivery schedule. This does not model the cluster publisher, payload files,
//! transport authentication, torn writes, or recovery authorization yet.

use super::*;
use crate::bounded_explorer::{explore, Checks};
use std::cell::RefCell;
use std::fmt::Debug;

const DEPTH: usize = 8;
const MAX_STATES: usize = 10_000;
const SAFETY: &str = "exact command owns the slot and materialized reservation";
const WITNESSES: [&str; 4] = [
    "request A converges after reopen and exact replay",
    "request B converges after reopen and exact replay",
    "witness applied while primary is unapplied",
    "primary terminal cleanup precedes trailing convergence",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Install(usize),
    MarkPublication,
    Apply(usize),
    ClearTerminal,
    Reopen,
    ReplayWitness,
}

// Independent expected effects; never derive these from returned production
// observations. Full traces remain part of identity: no projection of SQLite
// state is claimed to describe every possible future transition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Oracle {
    winner: Option<usize>,
    published: bool,
    applied: [bool; 3],
    cleared: bool,
    reopened: bool,
    replayed: bool,
}

impl Oracle {
    fn advance(&mut self, action: Action) {
        match action {
            Action::Install(winner) => self.winner = Some(winner),
            Action::MarkPublication => self.published = true,
            Action::Apply(replica) => self.applied[replica] = true,
            Action::ClearTerminal => self.cleared = true,
            Action::Reopen => self.reopened = true,
            Action::ReplayWitness => self.replayed = true,
        }
    }

    fn actions(&self) -> Vec<Action> {
        if self.winner.is_none() {
            return vec![Action::Install(0), Action::Install(1)];
        }
        let mut actions = Vec::new();
        if !self.published {
            actions.push(Action::MarkPublication);
        } else {
            // The scheduler assumes witness-before-primary publication. It
            // does not prove the higher-level publisher enforces that order.
            if !self.applied[1] {
                actions.push(Action::Apply(1));
            } else {
                for replica in [0, 2] {
                    if !self.applied[replica] {
                        actions.push(Action::Apply(replica));
                    }
                }
                if !self.replayed {
                    actions.push(Action::ReplayWitness);
                }
            }
            if self.applied[0] && !self.cleared {
                actions.push(Action::ClearTerminal);
            }
        }
        if !self.reopened {
            actions.push(Action::Reopen);
        }
        actions
    }

    fn witnesses(&self) -> Vec<&'static str> {
        let mut witnesses = Vec::new();
        if self.applied == [true; 3] && self.cleared && self.reopened && self.replayed {
            witnesses.push(WITNESSES[self.winner.unwrap()]);
        }
        if self.applied[1] && !self.applied[0] {
            witnesses.push(WITNESSES[2]);
        }
        if self.cleared && !self.applied[2] {
            witnesses.push(WITNESSES[3]);
        }
        witnesses
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct State {
    trace: Vec<Action>,
    oracle: Oracle,
}

impl State {
    fn successors(&self) -> Vec<(Action, Self)> {
        if self.trace.len() == DEPTH {
            return Vec::new();
        }
        self.oracle
            .actions()
            .into_iter()
            .map(|action| {
                let mut child = self.clone();
                child.trace.push(action);
                child.oracle.advance(action);
                (action, child)
            })
            .collect()
    }
}

fn diagnostic(error: impl Debug) -> String {
    format!("{error:?}")
}

fn equal<T: Debug + PartialEq>(actual: T, expected: T, context: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}: got {actual:?}, expected {expected:?}"))
    }
}

struct Fixture {
    seed: test_util::TempDir,
    bucket: BucketName,
    key: ObjectKey,
    reservations: [SessionId; 2],
    commands: [MetadataCommandEnvelope; 2],
}

impl Fixture {
    fn new() -> Self {
        let seed = test_util::tempdir();
        let bucket = trusted_bucket_name("publication-model");
        let key = trusted_object_key("object");
        let reservations = [
            crate::tests::stream_session_id("publication-a"),
            crate::tests::stream_session_id("publication-b"),
        ];
        let store = PgStore::open(seed.path(), 1).unwrap();
        store
            .apply_metadata_command_and_record(
                0,
                &create_bucket_probe_command(1, 1, bucket.clone(), 1),
            )
            .unwrap();
        // Closing the sole connection checkpoints its WAL. Copy only this
        // closed seed, never a live database or an incomplete WAL prefix.
        drop(store);
        let commands = std::array::from_fn(|request| {
            MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    PgId::new(1),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        bucket.clone(),
                        key.clone(),
                        reservations[request].clone(),
                        GenerationId::new(request as u64 + 1).unwrap(),
                        123,
                    ),
                ),
            )
        });
        assert_eq!(commands[0].id(), commands[1].id());
        assert_ne!(commands[0].command_bytes(), commands[1].command_bytes());
        Self {
            seed,
            bucket,
            key,
            reservations,
            commands,
        }
    }

    fn observe(&self, stores: &[PgStore; 3], oracle: &Oracle) -> Result<(), String> {
        for (replica, store) in stores.iter().enumerate() {
            let inspection = store
                .pending_metadata_command_inspection(replica as u32, ClusterEpoch::INITIAL)
                .map_err(diagnostic)?;
            let expected = if replica == 0 && !oracle.cleared {
                oracle
                    .winner
                    .map_or(PendingMetadataCommandInspection::Absent, |winner| {
                        PendingMetadataCommandInspection::Present {
                            command: Box::new(self.commands[winner].clone()),
                            publication_started: oracle.published,
                        }
                    })
            } else {
                PendingMetadataCommandInspection::Absent
            };
            equal(inspection, expected, "exact durable pending slot")?;
            let state = store.metadata_command_replica_state().map_err(diagnostic)?;
            equal(
                state.applied_log_index,
                if oracle.applied[replica] { 2 } else { 1 },
                "replica applied prefix",
            )?;
            for request in 0..2 {
                let reservation = store.get_object_generation_reservation(
                    &self.bucket,
                    &self.key,
                    &self.reservations[request],
                );
                let value = match reservation {
                    Ok(value) => Some(value.get()),
                    Err(MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                    Err(error) => return Err(diagnostic(error)),
                };
                let expected = (oracle.applied[replica] && oracle.winner == Some(request))
                    .then_some(request as u64 + 1);
                equal(
                    value,
                    expected,
                    "reservation belongs only to winning request",
                )?;
            }
        }
        if oracle.applied == [true; 3] {
            let primary = stores[0]
                .metadata_command_replica_state()
                .map_err(diagnostic)?;
            for store in &stores[1..] {
                equal(
                    store.metadata_command_replica_state().map_err(diagnostic)?,
                    primary,
                    "replicas converge on the same exact proof",
                )?;
            }
        }
        Ok(())
    }

    // Probe rejected competitors at every pending state, including after
    // primary apply and after reopen. Then recheck every modelled observation
    // so a failed call that mutates those fields is not successful rejection.
    fn probe(&self, stores: &[PgStore; 3], oracle: &Oracle) -> Result<(), String> {
        let Some(winner) = oracle.winner else {
            return Ok(());
        };
        let primary = &stores[0];
        let command = &self.commands[winner];
        let competitor = &self.commands[1 - winner];
        for (replica, store) in stores.iter().enumerate() {
            if oracle.applied[replica] {
                let result = store.apply_metadata_command_and_record(replica as u32, competitor);
                equal(
                    matches!(
                        result,
                        Err(BucketSnapshotLoadError::Store(
                            StoreError::MetadataCommandLogConflict { .. }
                        ))
                    ),
                    true,
                    "same-index different-command replay must be rejected",
                )?;
            }
        }
        if oracle.cleared {
            return self.observe(stores, oracle);
        }
        let marking = primary.mark_pending_metadata_command_publication_started(0, competitor);
        let rejected = if oracle.applied[0] {
            matches!(
                marking,
                Err(BucketSnapshotLoadError::Store(
                    StoreError::MetadataCommandLogConflict { .. }
                ))
            )
        } else {
            matches!(
                marking,
                Err(BucketSnapshotLoadError::Store(
                    StoreError::MetadataCommandPendingConflict { .. }
                ))
            )
        };
        equal(
            rejected,
            true,
            "crossed publication marking must be rejected",
        )?;
        let error =
            primary.try_insert_pending_metadata_command_slot(0, competitor, Some(&self.bucket));
        let rejected = if oracle.applied[0] {
            matches!(error, Err(StoreError::MetadataCommandLogConflict { .. }))
        } else {
            matches!(
                error,
                Err(StoreError::MetadataCommandPendingConflict { .. })
            )
        };
        equal(
            rejected,
            true,
            "competing insertion rejected with exact conflict class",
        )?;
        equal(
            primary
                .remove_pending_metadata_command_slot(0, competitor)
                .map_err(diagnostic)?,
            false,
            "crossed cleanup must not remove winning slot",
        )?;
        if !oracle.applied[0] {
            let removal = primary.remove_pending_metadata_command_slot(0, command);
            equal(
                matches!(
                    removal,
                    Err(StoreError::MetadataCommandTerminalEntryPending { .. })
                ),
                true,
                "exact cleanup requires primary terminal evidence",
            )?;
            primary
                .try_insert_pending_metadata_command_slot(0, command, Some(&self.bucket))
                .map_err(diagnostic)?;
        }
        if oracle.published {
            let replacement = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    PgId::new(1),
                    MetadataCommandLogIndex::new(3).unwrap(),
                ),
                command.payload().clone(),
            );
            equal(
                primary
                    .replace_pending_metadata_command_slot_for_reissue(
                        0,
                        command,
                        &replacement,
                        Some(&self.bucket),
                    )
                    .map_err(diagnostic)?,
                false,
                "published slot is irrevocable",
            )?;
        }
        self.observe(stores, oracle)
    }

    fn replay(&self, trace: &[Action]) -> Result<Oracle, String> {
        let directory = test_util::tempdir();
        let paths: [PathBuf; 3] =
            std::array::from_fn(|node| directory.path().join(node.to_string()));
        for path in &paths {
            fs::create_dir_all(path).map_err(diagnostic)?;
            fs::copy(
                self.seed.path().join("metadata.db"),
                path.join("metadata.db"),
            )
            .map_err(diagnostic)?;
        }
        let open = || -> Result<[PgStore; 3], String> {
            Ok([
                PgStore::open(&paths[0], 1).map_err(diagnostic)?,
                PgStore::open(&paths[1], 1).map_err(diagnostic)?,
                PgStore::open(&paths[2], 1).map_err(diagnostic)?,
            ])
        };
        let mut stores = open()?;
        let mut oracle = Oracle::default();
        self.observe(&stores, &oracle)?;
        for &action in trace {
            equal(
                oracle.actions().contains(&action),
                true,
                "enabled semantic action",
            )?;
            let command = &self.commands[oracle.winner.unwrap_or(0)];
            match action {
                Action::Install(request) => stores[0]
                    .try_insert_pending_metadata_command_slot(
                        0,
                        &self.commands[request],
                        Some(&self.bucket),
                    )
                    .map_err(diagnostic)?,
                Action::MarkPublication => stores[0]
                    .mark_pending_metadata_command_publication_started(0, command)
                    .map_err(diagnostic)?,
                Action::Apply(replica) => {
                    stores[replica]
                        .apply_metadata_command_and_record(replica as u32, command)
                        .map_err(diagnostic)?;
                }
                Action::ClearTerminal => equal(
                    stores[0]
                        .remove_pending_metadata_command_slot(0, command)
                        .map_err(diagnostic)?,
                    true,
                    "exact terminal cleanup",
                )?,
                Action::Reopen => {
                    drop(stores);
                    stores = open()?;
                }
                Action::ReplayWitness => {
                    stores[1]
                        .apply_metadata_command_and_record(1, command)
                        .map_err(diagnostic)?;
                }
            }
            oracle.advance(action);
            self.observe(&stores, &oracle)?;
            self.probe(&stores, &oracle)?;
        }
        Ok(oracle)
    }
}

#[test]
fn publication_model_exhausts_slot_reservation_and_replica_schedules() {
    let fixture = Fixture::new();
    let failure = RefCell::new(None);
    let explored = explore(
        [State::default()],
        &WITNESSES,
        State::successors,
        |state| match fixture
            .replay(&state.trace)
            .and_then(|actual| equal(actual, state.oracle.clone(), "replayed oracle"))
        {
            Ok(()) => Checks {
                failure: None,
                witnesses: state.oracle.witnesses(),
            },
            Err(error) => {
                *failure.borrow_mut() = Some(error);
                Checks {
                    failure: Some(SAFETY),
                    witnesses: Vec::new(),
                }
            }
        },
        MAX_STATES,
    );
    if let Some(trace) = explored.counterexample() {
        let replayed = fixture
            .replay(&trace)
            .expect_err("counterexample must replay");
        assert_eq!(Some(&replayed), failure.borrow().as_ref());
        panic!("publication counterexample: {trace:?}: {replayed}");
    }
    explored.assert_complete();
    assert_eq!(
        explored.state_count(),
        493,
        "bounded schedule catalogue changed"
    );
    assert_eq!(explored.transitions, 492);
    for witness in WITNESSES {
        let trace = explored.witness(witness).unwrap();
        assert!(fixture
            .replay(&trace)
            .unwrap()
            .witnesses()
            .contains(&witness));
        eprintln!("{witness}: {trace:?}");
    }
    eprintln!(
        "publication model: depth={DEPTH}, states={}, transitions={}",
        explored.state_count(),
        explored.transitions
    );
}
