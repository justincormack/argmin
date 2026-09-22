// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Replay-based model of the real route handle, not a duplicate renewal predicate.
// A semantic trace is the state identity: no merging of opaque live clusters or
// assumptions that their public observations describe all future behavior.
use super::*;
use crate::bounded_explorer::{Checks, Exploration, explore};

const DEPTH: usize = 6;
const MAX_STATES: usize = 100_000;
const SAFETY: &str = "renewal preserves each generation's issuing authority";
const RENEWED: &str = "current authority renews while old pinned authority expires";
const DELAYED: &str = "old renewal delivered after installation is ignored";
const OLD_RENEWED: &str = "original authority can renew before replacement";
const TIMES: [u64; 4] = [3_999, 4_000, 10_999, 11_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Event {
    Install,
    Send,
    Deliver(usize),
    Lose(usize),
    AdvanceTo(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Message {
    Unsent,
    Queued { issued_at: u64 },
    Consumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Observation {
    now: u64,
    installed: bool,
    messages: [Message; 2],
    // Actual authority deadline, bound monotonic deadline, and admission result.
    leases: [(u64, u64, bool); 2],
    old_renewed: bool,
    new_renewed: bool,
    delayed_ignored: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    trace: Vec<Event>,
    observation: Observation,
}

fn cluster(incarnation: u64, deadline: u64) -> Arc<StorageCluster> {
    let validity = RouteMapValidity::until_ms(deadline).unwrap();
    let mut cluster = active_test_cluster(validity);
    let digest = cluster
        .route_authority
        .dynamic_proof()
        .unwrap()
        .content_digest;
    Arc::get_mut(&mut cluster).unwrap().route_authority =
        StorageClusterRouteAuthority::Dynamic(DynamicRouteAuthorityProof {
            content_digest: digest,
            freshness_proof: RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: crate::control_plane::AuthorityIncarnation::new(incarnation)
                    .unwrap(),
                issued_at_ms: 1_000,
            },
        });
    // Use production conservative binding, not the test deadline setter (which
    // deliberately omits the skew subtraction). Equal wall/monotonic clocks.
    let bound = BoundRouteMapLease::bind(1_000, deadline, 1_000, 1_000, 1_000).unwrap();
    cluster.replace_route_map_lease(validity, Some(bound));
    cluster
}

fn lease(cluster: &StorageCluster) -> (u64, u64, bool) {
    let lease = cluster.local_map.route_map_lease_snapshot();
    let valid = match cluster.require_route_map_valid_now() {
        Ok(()) => true,
        Err(StoreError::RouteMapExpired { .. }) => false,
        Err(error) => panic!("unexpected admission error: {error:?}"),
    };
    (
        lease.validity.valid_until_ms().unwrap(),
        lease.local_valid_until_monotonic_ms.unwrap(),
        valid,
    )
}

fn replay(trace: &[Event]) -> Observation {
    crate::clock::with_time_override(1_000, || {
        let old = cluster(1, 5_000);
        let new = cluster(2, 9_000);
        let digest = old.route_authority.dynamic_proof().unwrap().content_digest;
        assert_eq!(
            digest,
            new.route_authority.dynamic_proof().unwrap().content_digest
        );
        let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&old));
        let mut observation = Observation {
            now: 1_000,
            installed: false,
            messages: [Message::Unsent; 2],
            leases: [lease(&old), lease(&new)],
            old_renewed: false,
            new_renewed: false,
            delayed_ignored: false,
            error: None,
        };
        // Independent oracle: authority 1 owns only old, authority 2 only new.
        // No production selection predicate is used to compute these values.
        let mut expected = [(5_000, 4_000), (9_000, 8_000)];
        for event in trace {
            if let Event::AdvanceTo(now) = event {
                assert!(*now > observation.now);
                observation.now = *now;
            }
            let result = crate::clock::with_time_override(observation.now, || {
                match *event {
                    Event::Install => {
                        assert!(!observation.installed);
                        handle
                            .install(Arc::clone(&new))
                            .map_err(|e| format!("install: {e:?}"))?;
                        observation.installed = true;
                    }
                    Event::Send => {
                        let sender = usize::from(observation.installed);
                        assert_eq!(observation.messages[sender], Message::Unsent);
                        observation.messages[sender] = Message::Queued {
                            issued_at: observation.now,
                        };
                    }
                    Event::Deliver(sender) => {
                        let Message::Queued { issued_at } = observation.messages[sender] else {
                            panic!("delivery requires a queued renewal");
                        };
                        observation.messages[sender] = Message::Consumed;
                        let status = ControlPlaneRuntimeMapStatus::test_with_lease_renewal(
                            ClusterEpoch::INITIAL,
                            digest,
                            RouteMapValidity::until_ms(12_000).unwrap(),
                            RuntimeMapFreshnessProof::SingleAuthority {
                                authority_incarnation:
                                    crate::control_plane::AuthorityIncarnation::new(
                                        u64::try_from(sender).unwrap() + 1,
                                    )
                                    .unwrap(),
                                issued_at_ms: issued_at,
                            },
                        );
                        let applied = handle
                            .renew_from_runtime_map_status(status, observation.now, observation.now)
                            .map_err(|e| format!("renewal: {e:?}"))?
                            .is_some();
                        let expected_applied = sender == usize::from(observation.installed);
                        if applied != expected_applied {
                            return Err(format!(
                                "renewal acceptance: got {applied}, expected {expected_applied}"
                            ));
                        }
                        if expected_applied {
                            // With equal clocks, D - skew is the monotonic bound.
                            // At expiry production binds an immediately expired lease.
                            expected[sender] = (12_000, observation.now.max(11_000));
                            if sender == 0 {
                                observation.old_renewed = true;
                            } else {
                                observation.new_renewed = true;
                            }
                        } else {
                            observation.delayed_ignored = true;
                        }
                    }
                    Event::Lose(sender) => {
                        assert!(matches!(
                            observation.messages[sender],
                            Message::Queued { .. }
                        ));
                        observation.messages[sender] = Message::Consumed;
                    }
                    Event::AdvanceTo(_) => {}
                }
                observation.leases = [lease(&old), lease(&new)];
                for (index, &(deadline, monotonic)) in expected.iter().enumerate() {
                    let actual = observation.leases[index];
                    let expected = (deadline, monotonic, observation.now < monotonic);
                    if actual != expected {
                        return Err(format!(
                            "generation {}: got {actual:?}, expected {expected:?}",
                            index + 1
                        ));
                    }
                }
                let expected_current = if observation.installed { &new } else { &old };
                if !Arc::ptr_eq(&handle.current(), expected_current) {
                    return Err("renewal changed current generation identity".to_string());
                }
                Ok(())
            });
            if let Err(error) = result {
                observation.error = Some(error);
                break;
            }
        }
        observation
    })
}

#[derive(Default)]
struct RenewalModel {
    generated: Mutex<BTreeSet<Vec<Event>>>,
    checked: Mutex<BTreeSet<Vec<Event>>>,
}

impl RenewalModel {
    fn state(&self, trace: Vec<Event>) -> State {
        let observation = replay(&trace);
        let mut generated = self.generated.lock().unwrap();
        generated.insert(trace.clone());
        assert!(
            generated.len() <= MAX_STATES,
            "incomplete search: model capacity exceeded"
        );
        State { trace, observation }
    }
}

impl RenewalModel {
    fn actions(&self, state: &State, actions: &mut Vec<Event>) {
        if state.trace.len() == DEPTH || state.observation.error.is_some() {
            return;
        }
        let observation = &state.observation;
        if !observation.installed && observation.now < 8_000 {
            actions.push(Event::Install);
        }
        if observation.messages[usize::from(observation.installed)] == Message::Unsent {
            actions.push(Event::Send);
        }
        for (sender, message) in observation.messages.iter().enumerate() {
            if matches!(message, Message::Queued { .. }) {
                actions.push(Event::Deliver(sender));
                actions.push(Event::Lose(sender));
            }
        }
        actions.extend(
            TIMES
                .into_iter()
                .filter(|now| *now > observation.now)
                .map(Event::AdvanceTo),
        );
    }

    fn successors(&self, state: &State) -> Vec<(Event, State)> {
        let mut actions = Vec::new();
        self.actions(state, &mut actions);
        actions
            .into_iter()
            .map(|event| {
                let mut trace = state.trace.clone();
                trace.push(event);
                (event, self.state(trace))
            })
            .collect()
    }

    fn checks(&self, state: &State) -> Checks {
        self.checked.lock().unwrap().insert(state.trace.clone());
        let o = &state.observation;
        let mut checks = Checks {
            failure: o.error.as_ref().map(|_| SAFETY),
            witnesses: Vec::new(),
        };
        if o.new_renewed && !o.leases[0].2 && o.leases[1].2 {
            checks.witnesses.push(RENEWED);
        }
        if o.delayed_ignored {
            checks.witnesses.push(DELAYED);
        }
        if o.old_renewed {
            checks.witnesses.push(OLD_RENEWED);
        }
        checks
    }

    fn run(&self) -> Exploration<State, Event> {
        explore(
            [self.state(Vec::new())],
            &[RENEWED, DELAYED, OLD_RENEWED],
            |state| self.successors(state),
            |state| self.checks(state),
            MAX_STATES,
        )
    }
}

#[test]
fn incarnation_renewal_model_exhausts_bounded_schedules() {
    let model = RenewalModel::default();
    let start = Instant::now();
    let explored = model.run();
    if let Some(actions) = explored.counterexample() {
        let observation = replay(&actions);
        assert!(observation.error.is_some(), "failure must replay");
        panic!(
            "historical-renewal counterexample: {actions:?}: {:?}",
            observation.error
        );
    }
    explored.assert_complete();
    assert_eq!(explored.state_count(), 2_146, "schedule catalogue changed");
    assert_eq!(explored.transitions, 2_145);
    assert_eq!(
        *model.generated.lock().unwrap(),
        *model.checked.lock().unwrap()
    );
    assert_eq!(
        model.generated.lock().unwrap().len(),
        explored.state_count()
    );
    assert_eq!(
        explored
            .states()
            .map(|state| state.trace.clone())
            .collect::<BTreeSet<_>>(),
        *model.generated.lock().unwrap()
    );
    for name in [RENEWED, DELAYED, OLD_RENEWED] {
        let trace = explored.witness(name).expect("required witness missing");
        let observation = replay(&trace);
        assert_eq!(observation.error, None);
        assert!(
            model
                .checks(&State {
                    trace: trace.clone(),
                    observation
                })
                .witnesses
                .contains(&name)
        );
        eprintln!("{name}: {trace:?}");
    }
    eprintln!(
        "incarnation renewal: depth={DEPTH}, states={}, transitions={}, elapsed={:?}; complete bounded search, no semantic deduplication",
        explored.state_count(),
        explored.transitions,
        start.elapsed()
    );
}

#[test]
fn incarnation_renewal_discovered_schedule_replays_through_route_handle() {
    // Discovered by the isolated production mutation, not supplied to the search.
    let trace = [
        Event::Send,
        Event::AdvanceTo(3_999),
        Event::Install,
        Event::Deliver(0),
        Event::Send,
        Event::Deliver(1),
    ];
    let result = replay(&trace);
    assert_eq!(result.error, None, "{trace:?}: {result:?}");
    assert_eq!(
        result.leases,
        [(5_000, 4_000, true), (12_000, 11_000, true)]
    );
    let mut expired = trace.to_vec();
    expired.push(Event::AdvanceTo(4_000));
    let result = replay(&expired);
    assert_eq!(result.error, None);
    assert_eq!(
        result.leases,
        [(5_000, 4_000, false), (12_000, 11_000, true)]
    );
}
