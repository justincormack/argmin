// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Cancellation around a retained, authority-backed recovery handoff. Operation
//! boundaries are scheduled, not instruction-level concurrency or process death.

use super::*;

const REQUIRED: [&str; 8] = [
    "drop before recovery preserves command ownership",
    "discard before recovery preserves command ownership",
    "drop after recovery releases caller staging",
    "discard after recovery releases caller staging",
    "early release retry cannot take over authorized recovery",
    "healthy release retry completes deferred cleanup",
    "owner A survives cancellation and completes recovery",
    "owner B survives cancellation and completes recovery",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Cancellation {
    Drop,
    Discard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    StageBoth,
    LeavePending,
    RelinquishRecovery,
    CancelOther(Cancellation),
    RetryRelease,
    Recover,
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
            [Step::StageBoth] => vec![Step::LeavePending],
            [Step::StageBoth, Step::LeavePending] => vec![Step::RelinquishRecovery],
            [Step::StageBoth, Step::LeavePending, Step::RelinquishRecovery] => vec![
                Step::CancelOther(Cancellation::Drop),
                Step::CancelOther(Cancellation::Discard),
                Step::Recover,
            ],
            [.., Step::RelinquishRecovery, Step::Recover] => vec![
                Step::CancelOther(Cancellation::Drop),
                Step::CancelOther(Cancellation::Discard),
            ],
            [.., Step::RelinquishRecovery, Step::CancelOther(_)] => {
                vec![Step::Recover, Step::RetryRelease]
            }
            [.., Step::CancelOther(_), Step::RetryRelease] => vec![Step::Recover],
            [.., Step::CancelOther(_), Step::Recover]
            | [.., Step::CancelOther(_), Step::RetryRelease, Step::Recover] => {
                vec![Step::RetryRelease]
            }
            [.., Step::Recover, Step::RetryRelease | Step::CancelOther(_)] => Vec::new(),
            _ => panic!("invalid cancellation trace: {:?}", self.trace),
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
            [.., Step::RelinquishRecovery, Step::CancelOther(Cancellation::Drop)] => {
                vec![REQUIRED[0]]
            }
            [.., Step::RelinquishRecovery, Step::CancelOther(Cancellation::Discard)] => {
                vec![REQUIRED[1]]
            }
            [.., Step::Recover, Step::CancelOther(Cancellation::Drop)] => vec![REQUIRED[2]],
            [.., Step::Recover, Step::CancelOther(Cancellation::Discard)] => vec![REQUIRED[3]],
            [.., Step::CancelOther(_), Step::RetryRelease] => vec![REQUIRED[4]],
            [.., Step::Recover, Step::RetryRelease] => vec![REQUIRED[5], REQUIRED[6 + self.owner]],
            _ => Vec::new(),
        }
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
    let mut awaiting_recovery = false;
    let other = 1 - state.owner;
    fixture.observe(ownership, &identities, None)?;
    for step in &state.trace {
        match *step {
            Step::StageBoth => {
                for actor in 0..2 {
                    let generation = routes[actor]
                        .reserve_generation(&fixture.reservations[actor])
                        .map_err(diagnostic)?;
                    let payload = routes[actor]
                        .write_direct_object_payload(
                            &fixture.reservations[actor],
                            generation,
                            DATA[actor].len() as u64,
                            DATA[actor],
                        )
                        .map_err(diagnostic)?;
                    identities[actor] = Some(direct_payload_test_identity(&payload));
                    payloads[actor] = Some(payload);
                    ownership[actor] = Ownership::Caller;
                }
                if identities[0].unwrap().segment_vid == identities[1].unwrap().segment_vid {
                    return Err("competing requests share a generation".into());
                }
            }
            Step::LeavePending => {
                pending = Some(fixture.leave_pending_after_inspection_failure(
                    state.owner,
                    &routes[state.owner],
                    payloads[state.owner].take().unwrap(),
                )?);
                ownership[state.owner] = Ownership::Pending;
            }
            Step::RelinquishRecovery => {
                // Real recovery-flight admission and transfer; deliberately no
                // active leader thread or wall-clock waiting in this bound.
                let runtime = fixture.map.runtime_state();
                let MetadataCommandRecoveryAdmission::Leader(guard) =
                    runtime.join_metadata_command_recovery(PgId::new(0), pending.as_ref().unwrap())
                else {
                    return Err("expected initial recovery leadership".into());
                };
                guard.relinquish_for_authorized_recovery();
                awaiting_recovery = true;
            }
            Step::CancelOther(cancellation) => {
                let payload = payloads[other].take().unwrap();
                match cancellation {
                    Cancellation::Drop => drop(payload),
                    Cancellation::Discard => routes[other]
                        .discard_direct_object_payload(payload)
                        .map_err(diagnostic)?,
                }
                ownership[other] = if awaiting_recovery {
                    Ownership::CancelledPendingRelease
                } else {
                    Ownership::Released
                };
            }
            Step::RetryRelease => {
                // This is the same production operation used by RAII cleanup,
                // with its typed result retained instead of best-effort erasure.
                let result = fixture.cluster.release_object_generation_reservation(
                    &fixture.bucket,
                    &fixture.key,
                    &fixture.reservations[other],
                );
                if awaiting_recovery {
                    if !matches!(
                        result,
                        Err(crate::ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery)
                    ) {
                        return Err(format!(
                            "unauthorized cleanup retry must retain handoff: {result:?}"
                        ));
                    }
                } else {
                    result.map_err(diagnostic)?;
                    ownership[other] = Ownership::Released;
                }
            }
            Step::Recover => {
                equal(
                    fixture
                        .cluster
                        .drain_pending_metadata_command_with_authorized_recovery_route(
                            PgId::new(0),
                            pending.as_ref().unwrap(),
                            &fixture.cluster,
                        )
                        .map_err(diagnostic)?,
                    PendingMetadataCommandOutcome::Applied,
                    "authorized takeover applies exact pending command",
                )?;
                pending = None;
                awaiting_recovery = false;
                ownership[state.owner] = Ownership::Visible;
            }
        }
        fixture.observe(ownership, &identities, pending.as_ref())?;
        equal(
            fixture
                .map
                .runtime_state()
                .test_metadata_command_recovery_flight_count(),
            usize::from(awaiting_recovery),
            "retained recovery flight lifetime",
        )?;
    }
    // An unfinished prefix is torn down, not implicitly extended by another
    // cancellation action after the last observation.
    for payload in payloads.into_iter().flatten() {
        payload.disarm();
    }
    Ok(())
}

#[test]
fn publication_model_exhausts_cancellation_around_authorized_payload_recovery() {
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
                    failure: Some("cancellation preserves retained command and cleanup authority"),
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
        panic!("cancellation counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 38);
    assert_eq!(explored.transitions, 36);
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
