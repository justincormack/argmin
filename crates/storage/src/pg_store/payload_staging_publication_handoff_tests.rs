// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming installation-to-publication transfer. All history is produced by
//! actual classified SQL attempts, not fabricated observation values.

use super::*;
use crate::node_runtime::pg_store::payload_staging::submission::{
    ContextualSubmission, InstallationObservation as Installed, SubmissionError,
};
use crate::node_runtime::pg_store::{CompletePublicationSender, PublicationObservation};
use crate::types::AdmittedRouteEffectFence;

pub(super) fn uncertain_install(holder: &mut ContextualSubmission, pg: &PgStore) {
    pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_pending AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;").unwrap();
    let error = complete_pending::install(holder, pg).unwrap_err();
    assert!(
        matches!(error, SubmissionError::Unconfirmed(_)),
        "{error:?}"
    );
    assert!(error
        .to_string()
        .contains("commit pending metadata command slot transaction"));
    assert_eq!(holder.observation().installation, Installed::Unconfirmed);
    assert!(pg.conn.is_autocommit());
    pg.conn
        .execute_batch("DROP TRIGGER fail_pending; DROP TABLE deferred_failure;")
        .unwrap();
}

#[test]
fn staging_complete_publication_handoff_preserves_every_installation_history_without_effects() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for case in 0..7 {
        for outcome in [
            "fresh",
            "rejected",
            "installed",
            "commit-uncertain",
            "unwind",
            "damaged-retry",
        ] {
            let (_dir, pg, context) = complete_pending::setup(case);
            let mut holder = context.into_submission();
            let expected = match outcome {
                "fresh" => Installed::NoAcceptanceObserved,
                "rejected" => {
                    assert!(matches!(
                        holder.install(
                            &pg,
                            0,
                            &topology(),
                            AdmittedRouteEffectFence::unbounded(
                                holder.observation().command_id.cluster_epoch()
                            ),
                            Some(Instant::now())
                        ),
                        Err(SubmissionError::Rejected(_))
                    ));
                    Installed::NoAcceptanceObserved
                }
                "installed" => {
                    complete_pending::install(&mut holder, &pg).unwrap();
                    Installed::Installed
                }
                "commit-uncertain" => {
                    uncertain_install(&mut holder, &pg);
                    Installed::Unconfirmed
                }
                "unwind" => {
                    let before = snapshot(&pg);
                    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        PgStore::test_with_before_pending_slot_effect_hook(
                            |in_transaction| {
                                assert!(in_transaction);
                                panic!("interrupt installation before transfer");
                            },
                            || complete_pending::install(&mut holder, &pg),
                        )
                    }));
                    assert!(caught.is_err());
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    assert_eq!(snapshot(&pg), before);
                    Installed::Unconfirmed
                }
                "damaged-retry" => {
                    complete_pending::install(&mut holder, &pg).unwrap();
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                        .unwrap();
                    assert!(matches!(
                        complete_pending::install(&mut holder, &pg),
                        Err(SubmissionError::Unconfirmed(_))
                    ));
                    Installed::Unconfirmed
                }
                _ => unreachable!(),
            };
            assert_eq!(holder.observation().installation, expected);
            let observation = holder.observation();
            let before = snapshot(&pg);
            let writes = pg.conn.total_changes();
            let sender = holder.into_publication_sender(NodeId::new(4), deadline());
            assert_eq!(sender.installation_observation(), Some(observation));
            assert_eq!(sender.id(), Some(observation.command_id));
            assert_eq!(
                sender.observation(),
                PublicationObservation::NoAttemptObserved
            );
            assert_eq!(snapshot(&pg), before, "{case}: {outcome}");
            assert_eq!(pg.conn.total_changes(), writes);
            drop(sender);
            assert_eq!(snapshot(&pg), before, "drop: {case}: {outcome}");
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_publication_handoff_installed_unix_and_tls_retries_keep_both_histories() {
    for tls in [false, true] {
        sender::remote_history(tls, Some(Installed::Installed));
    }
}

#[test]
fn staging_complete_publication_handoff_uncertain_unix_and_tls_retries_keep_both_histories() {
    for tls in [false, true] {
        sender::remote_history(tls, Some(Installed::Unconfirmed));
    }
}

#[test]
fn staging_complete_publication_handoff_embedded_confirmation_does_not_rewrite_installation() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (map, handle) =
            embedded::embedded(&dir, case, PgState::Active, RouteMapValidity::Forever);
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        let encoded = context.encode_command().unwrap();
        let copy = ContextualCommand::decode_command(
            encoded.bytes(),
            encoded.checksum_crc64(),
            &topology(),
        )
        .unwrap();
        let mut holder = copy.into_submission();
        uncertain_install(&mut holder, &node.get_pg(pg_id.get()).unwrap());
        // Another exact installer/recovery completes the retained command. This
        // does not rewrite the original caller's unconfirmed installation result.
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let installation = holder.observation();
        let mut sender = holder.into_publication_sender(NodeId::new(4), deadline());
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        let result = sender.test_send_embedded_with_hook(&section, deadline(), |stage| {
            if stage == "after-commit" {
                Err(StoreError::OperationDeadlineExceeded {
                    context: "lost embedded publication result",
                })
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(sender.installation_observation(), Some(installation));
        assert_eq!(sender.observation(), PublicationObservation::Unconfirmed);
        sender.send_embedded(&section, deadline()).unwrap();
        assert_eq!(sender.installation_observation(), Some(installation));
        assert_eq!(
            sender.observation(),
            PublicationObservation::MarkerConfirmed
        );
        assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}

#[test]
fn staging_complete_publication_handoff_is_consuming_and_noncloneable() {
    let _: fn(ContextualSubmission, NodeId, Instant) -> CompletePublicationSender =
        ContextualSubmission::into_publication_sender;
    trait NotClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    let _ = <CompletePublicationSender as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::CompletePublicationAttempt<'_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::payload_staging::SubmittedPublicationEvidence as NotClone<_>>::check;
}
