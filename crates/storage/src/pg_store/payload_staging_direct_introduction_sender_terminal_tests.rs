// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Handoff must move exact confirmed custody, never infer it from durable apply.
use super::*;

#[path = "payload_staging_direct_introduction_sender_retirement_tests.rs"]
mod retirement;

macro_rules! reject_handoff {
    ($f:ident, $publication:ident, $diagnostic:expr) => {{
        let before = $f.observe();
        let id = $publication.id();
        let bounds = $publication.test_bounds();
        let published = $publication.publication();
        let applications: Vec<_> = (0..5)
            .map(|node| $publication.application(NodeId::new(node)))
            .collect();
        let failure = $publication
            .into_terminal()
            .err()
            .expect("incomplete or expired custody");
        assert!(
            failure.error().to_string().contains($diagnostic),
            "{}",
            failure.error()
        );
        $publication = failure.into_publication();
        assert_eq!($publication.id(), id);
        assert_eq!($publication.test_bounds(), bounds);
        assert_eq!(
            $publication.test_installation(),
            InstallationObservation::Installed
        );
        assert_eq!($publication.publication(), published);
        for (node, expected) in applications.into_iter().enumerate() {
            assert_eq!($publication.application(NodeId::new(node as u32)), expected);
        }
        assert_eq!($f.observe(), before, "failed transfer has no effects");
    }};
}

#[test]
fn staging_direct_introduction_sender_terminal_requires_every_confirmation_and_preserves_custody() {
    for tls in [false, true] {
        for witness in [0, 3] {
            with_application!(@consuming [(4, tls), (witness, !tls), (2, tls)], |f, session, publication, owner, proof| {
                reject_handoff!(f, publication, "confirmed 0 of 5");
                publication.publish().unwrap();
                for (count, member) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                    reject_handoff!(f, publication, &format!("confirmed {count} of 5"));
                    apply_member!(f, publication, member).unwrap();
                }
                let before = f.observe();
                let bounds = publication.test_bounds();
                let id = publication.id();
                // Handoff neither transports nor rebinds wall time to a new
                // monotonic deadline, even after a backward clock adjustment.
                let terminal = crate::clock::with_time_override(crate::clock::current_time_millis().saturating_sub(1_000), || {
                    publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()))
                });
                assert_eq!(terminal.id(), id);
                assert_eq!(terminal.test_bounds(), bounds);
                assert_eq!(terminal.installation(), Installed::Installed);
                assert_eq!(terminal.test_installation(), InstallationObservation::Installed);
                assert_eq!(terminal.publication(), Published::MarkerConfirmed);
                for node in 0..5 { assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied); }
                terminal.require_valid().unwrap();
                assert_eq!(f.observe(), before);
                // Pending slot and durable owner/resources remain unchanged.
                drop(terminal);
                assert_eq!(f.observe(), before, "Drop must not retire or release");
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_terminal_uncertain_apply_cannot_become_confirmed_by_handoff()
{
    for tls in [false, true] {
        with_application!(@consuming [(4, tls), (0, !tls), (2, tls)], |f, session, publication, owner, proof| {
            publication.publish().unwrap();
            for member in [0, 4, 2] {
                let peer = f.remote(NodeId::new(member)).unwrap();
                peer.server.set_response_frame_test_hook(Arc::new(|kind, frame| {
                    if kind == APPLY {
                        frame.payload = rpc::encode_storage_rpc_success_response(&[]);
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true).unwrap();
                    }
                }));
                assert!(publication.apply_remote(&peer.client).is_err());
                assert_eq!(publication.application(peer.node), Applied::Unconfirmed);
                reject_handoff!(f, publication, "confirmed");
                peer.server.set_response_frame_test_hook(Arc::new(|_, _| {}));
                let before = f.observe();
                publication.apply_remote(&peer.client).unwrap();
                assert_eq!(f.observe(), before, "confirm exact durable apply without mutation");
            }
            apply_member!(f, publication, 3).unwrap();
            assert!(publication.test_apply_embedded_replica(NodeId::new(1), |stage| {
                if stage == "after-commit" { return Err(StoreError::RouteCapabilitySubjectMismatch { operation: "lost final member confirmation" }); }
                Ok(())
            }).is_err());
            // All five replicas are durable, but only four are confirmed. The
            // transfer must not replace observation with storage inspection.
            assert_eq!(publication.application(NodeId::new(1)), Applied::Unconfirmed);
            for node in 0..5 {
                assert_eq!(f.node(NodeId::new(node)).get_pg(publication.id().pg_id().get()).unwrap().metadata_command_replica_state().unwrap().applied_log_index, publication.id().log_index().get());
            }
            reject_handoff!(f, publication, "confirmed 4 of 5");
            let before = f.observe();
            apply_member!(f, publication, 1).unwrap();
            let terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            terminal.require_valid().unwrap();
            assert_eq!(f.observe(), before);
            drop(terminal);
            assert_eq!(f.observe(), before);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_terminal_rejects_expiry_and_reopened_primary_without_custody_loss(
) {
    for tls in [false, true] {
        with_application!(@consuming [(4, tls), (0, !tls), (2, tls)], |f, session, publication, owner, proof| {
            publication.publish().unwrap();
            for member in [0, 4, 2, 3, 1] { apply_member!(f, publication, member).unwrap(); }
            let bounds = publication.test_bounds();
            // Move the owner into the closure and return the SAME failed owner.
            publication = crate::clock::with_time_override(bounds.1, || {
                let before = f.observe();
                let failure = publication.into_terminal().err().expect("expired authority");
                assert!(failure.error().to_string().contains("expired"), "{}", failure.error());
                let publication = failure.into_publication();
                assert_eq!(f.observe(), before);
                publication
            });
            assert_eq!(publication.test_bounds(), bounds);
            for node in 0..5 { assert_eq!(publication.application(NodeId::new(node)), Applied::Applied); }
            let primary = f.remote(NodeId::new(4)).unwrap();
            session.test_close_primary_section();
            let _new = primary.client.open_metadata_command_critical_section_until(publication.id().pg_id(), deadline()).unwrap();
            reject_handoff!(f, publication, "exact open primary session");
            let before = f.observe();
            drop(publication);
            assert_eq!(f.observe(), before);
        });
    }
}

#[test]
fn staging_direct_introduction_sender_terminal_expiry_and_primary_close_do_not_erase_confirmation()
{
    for tls in [false, true] {
        with_application!(@consuming [(4, tls), (2, !tls)], |f, session, publication, owner, proof| {
            publication.publish().unwrap();
            for member in [0, 4, 2, 3, 1] { apply_member!(f, publication, member).unwrap(); }
            let terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            let before = f.observe();
            let bounds = terminal.test_bounds();
            crate::clock::with_time_override(bounds.1, || { assert!(terminal.require_valid().is_err()); });
            terminal.require_valid().unwrap();
            let primary = f.remote(NodeId::new(4)).unwrap();
            session.test_close_primary_section();
            let _new = primary.client.open_metadata_command_critical_section_until(terminal.id().pg_id(), deadline()).unwrap();
            assert!(terminal.require_valid().is_err());
            assert_eq!(terminal.test_bounds(), bounds);
            assert_eq!(terminal.test_installation(), InstallationObservation::Installed);
            assert_eq!(terminal.publication(), Published::MarkerConfirmed);
            for node in 0..5 { assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied); }
            drop(terminal);
            assert_eq!(f.observe(), before);
        });
    }
}
