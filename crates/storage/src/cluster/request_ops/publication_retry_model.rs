// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Outer publisher policy with model-owned retry waiting and real application,
//! durable observation, progress merging, and pending-command completion.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const REQUIRED_RETRY: [&str; 7] = [
    "healthy retry converges after witness acknowledgement loss",
    "budget end preserves unconfirmed witness publication",
    "later pre-dispatch failure retains earlier publication ambiguity",
    "witnessed definite failure remains irrevocable rather than abortable",
    "primary observation confirms publication despite lost reply",
    "owner A survives ambiguous retry and frontend replacement",
    "owner B survives ambiguous retry and frontend replacement",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WaitSchedule {
    Stop,
    RetryHealthy,
    RetryNotSent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Scenario {
    fault: Option<Boundary>,
    wait: WaitSchedule,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    Publish(Scenario),
    ReplaceFrontend,
    Recover,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RetryState {
    winner: usize,
    trace: Vec<Step>,
}

impl RetryState {
    fn successors(&self) -> Vec<(Step, Self)> {
        let steps = match self.trace.as_slice() {
            [] => [
                None,
                Some(Boundary::Before(WITNESS)),
                Some(Boundary::After(WITNESS)),
                Some(Boundary::Before(PRIMARY)),
                Some(Boundary::After(PRIMARY)),
                Some(Boundary::Before(TRAILING)),
                Some(Boundary::After(TRAILING)),
            ]
            .into_iter()
            .flat_map(|fault| {
                [
                    WaitSchedule::Stop,
                    WaitSchedule::RetryHealthy,
                    WaitSchedule::RetryNotSent,
                ]
                .into_iter()
                .map(move |wait| Step::Publish(Scenario { fault, wait }))
            })
            .collect(),
            [Step::Publish(_)] => vec![Step::ReplaceFrontend, Step::Recover],
            [Step::Publish(_), Step::ReplaceFrontend] => vec![Step::Recover],
            [Step::Publish(_), Step::Recover]
            | [Step::Publish(_), Step::ReplaceFrontend, Step::Recover] => Vec::new(),
            _ => panic!("invalid retry trace: {:?}", self.trace),
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
            [Step::Publish(Scenario {
                fault: Some(Boundary::After(WITNESS)),
                wait: WaitSchedule::RetryHealthy,
            })] => vec![REQUIRED_RETRY[0]],
            [Step::Publish(Scenario {
                fault: Some(Boundary::After(WITNESS)),
                wait: WaitSchedule::Stop,
            })] => vec![REQUIRED_RETRY[1]],
            [Step::Publish(Scenario {
                fault: Some(Boundary::After(WITNESS)),
                wait: WaitSchedule::RetryNotSent,
            })] => vec![REQUIRED_RETRY[2]],
            [Step::Publish(Scenario {
                fault: Some(Boundary::Before(PRIMARY)),
                wait: WaitSchedule::Stop,
            })] => vec![REQUIRED_RETRY[3]],
            [Step::Publish(Scenario {
                fault: Some(Boundary::After(PRIMARY)),
                ..
            })] => vec![REQUIRED_RETRY[4]],
            [Step::Publish(Scenario {
                fault: Some(Boundary::After(WITNESS)),
                wait: WaitSchedule::RetryNotSent,
            }), Step::ReplaceFrontend, Step::Recover] => vec![REQUIRED_RETRY[5 + self.winner]],
            _ => Vec::new(),
        }
    }
}

fn interrupted_before_primary(scenario: Scenario) -> bool {
    matches!(
        scenario.fault,
        Some(Boundary::After(WITNESS) | Boundary::Before(PRIMARY))
    )
}

fn expected_after_publish(scenario: Scenario) -> ([bool; 3], bool) {
    if interrupted_before_primary(scenario) && scenario.wait == WaitSchedule::RetryHealthy {
        ([true; 3], true)
    } else {
        let (applied, marked, _) = expected_prefix(scenario.fault);
        (applied, marked)
    }
}

fn check_outcome(
    result: Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure>,
    scenario: Scenario,
) -> Result<(), String> {
    if scenario.fault.is_none()
        || (interrupted_before_primary(scenario) && scenario.wait == WaitSchedule::RetryHealthy)
    {
        return equal(
            result.map_err(diagnostic)?,
            MetadataCommandApplyOutcome::Converged,
            "confirmed full convergence",
        );
    }
    if matches!(
        scenario.fault,
        Some(Boundary::After(PRIMARY) | Boundary::Before(TRAILING) | Boundary::After(TRAILING))
    ) {
        return equal(
            result.map_err(diagnostic)?,
            MetadataCommandApplyOutcome::PublishedPendingRecovery,
            "confirmed publication hands off remaining work",
        );
    }
    let failure = result
        .err()
        .ok_or_else(|| "unconfirmed command returned success".to_string())?;
    let before_witness = scenario.fault == Some(Boundary::Before(WITNESS));
    let ambiguous = scenario.fault == Some(Boundary::After(WITNESS));
    equal(
        failure.progress,
        if before_witness {
            MetadataCommandApplyProgress::Abortable
        } else {
            MetadataCommandApplyProgress::Witnessed
        },
        "retained publication progress",
    )?;
    equal(
        failure.may_have_applied,
        ambiguous,
        "ambiguity survives later pre-dispatch failure",
    )?;
    equal(
        failure.applied_nodes,
        usize::from(scenario.fault == Some(Boundary::Before(PRIMARY))),
        "maximum acknowledged application count across retries",
    )?;
    match (&failure.source, before_witness, ambiguous) {
        (
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::TransportClosed,
                ..
            }),
            true,
            false,
        ) => Ok(()),
        (
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandOutcomeUnconfirmed {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 2,
            }),
            false,
            true,
        ) => Ok(()),
        (
            BucketSnapshotLoadError::Store(
                StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index: 2,
                },
            ),
            false,
            false,
        ) => Ok(()),
        _ => Err(format!("wrong outer failure classification: {failure:?}")),
    }
}

fn transport_closed(node: u32) -> StoreError {
    StoreError::StorageRpc {
        node_id: node,
        operation: "outer publication model",
        failure: StorageRpcErrorCode::TransportClosed,
        detail: crate::StorageNodeFailureDetail::new("model-controlled delivery failure"),
    }
}

impl Fixture {
    fn publish_with_schedule(&self, scenario: Scenario) -> Result<(), String> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let hook = |after: bool| {
            let events = Arc::clone(&events);
            let attempts = Arc::clone(&attempts);
            let command = self.command.clone();
            Arc::new(move |node: NodeId, candidate: &MetadataCommandEnvelope| {
                assert_eq!(candidate, &command, "retry changed exact command identity");
                let boundary = if after {
                    Boundary::After(node.as_u32())
                } else {
                    Boundary::Before(node.as_u32())
                };
                let attempt = attempts.load(Ordering::SeqCst);
                events.lock().unwrap().push((attempt, boundary));
                if attempt == 1 && scenario.fault == Some(boundary) {
                    Err(transport_closed(node.as_u32()))
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
        let attempt_counter = Arc::clone(&attempts);
        let command = self.command.clone();
        let attempt_hook = self
            .cluster
            .test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
                assert_eq!(candidate, &command);
                let attempt = attempt_counter.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(
                    attempt <= 2,
                    "model allowed at most two publication attempts"
                );
                if attempt == 2 && scenario.wait == WaitSchedule::RetryNotSent {
                    Err(transport_closed(PRIMARY))
                } else {
                    Ok(())
                }
            }));
        let mut waits = 0;
        let mut waits_after_attempts = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(600);
        let result = self
            .cluster
            .apply_metadata_command_to_acting_set_from_origin_with_retry_wait(
                NodeId::new(PRIMARY),
                &self.command,
                MetadataCommandExecutionRoute::normal(),
                &self.cluster,
                MetadataCommandApplyAttemptContext {
                    progress: MetadataCommandApplyProgress::Abortable,
                    deadline,
                    provenance: MetadataCommandApplyProgressProvenance::Authoritative,
                    publication_start: MetadataCommandPublicationStartPolicy::Required,
                },
                |retries, actual_deadline| {
                    assert_eq!(
                        actual_deadline, deadline,
                        "retry must not reset the deadline"
                    );
                    assert_eq!(*retries, waits, "retry counter must be retained");
                    *retries += 1;
                    waits += 1;
                    waits_after_attempts.push(attempts.load(Ordering::SeqCst));
                    scenario.wait != WaitSchedule::Stop && waits == 1
                },
            );
        drop(attempt_hook);
        drop(after);
        drop(before);
        let retryable = interrupted_before_primary(scenario);
        let expected_waits: Vec<usize> = if !retryable {
            vec![]
        } else {
            match scenario.wait {
                WaitSchedule::Stop | WaitSchedule::RetryHealthy => vec![1],
                WaitSchedule::RetryNotSent => vec![1, 2],
            }
        };
        equal(
            waits_after_attempts,
            expected_waits,
            "retry/handoff decision sequence",
        )?;
        equal(
            attempts.load(Ordering::SeqCst),
            if retryable && scenario.wait != WaitSchedule::Stop {
                2
            } else {
                1
            },
            "bounded attempt count",
        )?;
        let mut expected_events: Vec<_> = expected_prefix(scenario.fault)
            .2
            .into_iter()
            .map(|event| (1, event))
            .collect();
        if retryable && scenario.wait == WaitSchedule::RetryHealthy {
            // Witness was already applied: the real retry replays it without
            // calling its before-apply hook, then applies primary and trailing.
            expected_events.extend(
                [
                    Boundary::After(WITNESS),
                    Boundary::Before(PRIMARY),
                    Boundary::After(PRIMARY),
                    Boundary::Before(TRAILING),
                    Boundary::After(TRAILING),
                ]
                .into_iter()
                .map(|event| (2, event)),
            );
        }
        equal(
            events.lock().unwrap().clone(),
            expected_events,
            "actual dispatch sequence across attempts",
        )?;
        check_outcome(result, scenario)
    }
}

fn replay_retry(state: &RetryState) -> Result<(), String> {
    let mut fixture = Fixture::new(state.winner);
    let mut applied = [false; 3];
    let mut marked = false;
    let mut cleared = false;
    fixture.observe(state.winner, applied, marked, cleared)?;
    for step in &state.trace {
        match *step {
            Step::Publish(scenario) => {
                fixture.publish_with_schedule(scenario)?;
                (applied, marked) = expected_after_publish(scenario);
            }
            Step::ReplaceFrontend => {
                fixture.cluster = StorageCluster::from_static_local_map(Arc::clone(&fixture.map))
                    .map_err(diagnostic)?;
            }
            Step::Recover => {
                let mut budget = RequestWorkBudget::new(Duration::from_secs(600), None);
                equal(
                    fixture
                        .cluster
                        .finish_pending_metadata_command_to_acting_set_with_work_budget(
                            PG,
                            &fixture.command,
                            false,
                            &mut budget,
                        )
                        .map_err(diagnostic)?,
                    PendingMetadataCommandOutcome::Applied,
                    "healthy finisher preserves issuing reservation",
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
fn publication_model_exhausts_outer_retry_budget_and_handoff_schedules() {
    let failure = RefCell::new(None);
    let explored = explore(
        (0..2).map(|winner| RetryState {
            winner,
            trace: Vec::new(),
        }),
        &REQUIRED_RETRY,
        RetryState::successors,
        |state| match replay_retry(state) {
            Ok(()) => Checks {
                failure: None,
                witnesses: state.witnesses(),
            },
            Err(error) => {
                *failure.borrow_mut() = Some((state.clone(), error));
                Checks {
                    failure: Some("retry preserves accumulated publication evidence"),
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
            replay_retry(&state).unwrap_err(),
            error,
            "counterexample must replay"
        );
        panic!("outer publication counterexample: {state:?}: {error}");
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 170);
    assert_eq!(explored.transitions, 168);
    for name in REQUIRED_RETRY {
        let trace = explored.witness(name).unwrap();
        let state = explored
            .states()
            .find(|state| state.trace == trace && state.witnesses().contains(&name))
            .unwrap();
        replay_retry(state).unwrap();
        eprintln!("{name}: {state:?}");
    }
}
