// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh introduction with an embedded primary and exact Unix/TLS members.
use super::*;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::ApplicationObservation as Applied;
use crate::storage_rpc::{self as wire, StorageRpcMessageKind as Kind};
use fanout_mixed::{Mixed, Remote};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn mixed(
    endpoints: [(u32, bool); 2],
) -> (
    Mixed,
    StagingOwnerRecord,
    BucketWriteReservationProof,
    test_util::TempDir,
) {
    let (f, owner, proof) = inputs();
    let Setup {
        _dir: directory,
        map,
        handle,
        context,
    } = f;
    (
        Mixed::from_settled_fixture(map, handle, context, endpoints),
        owner,
        proof,
        directory,
    )
}

fn count(peer: &Remote, corrupt: Arc<AtomicBool>) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    peer.server
        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind == Kind::MetadataCommandApplyAndRecord {
                observed.fetch_add(1, Ordering::SeqCst);
                if corrupt.load(Ordering::SeqCst) {
                    let mut payload = wire::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                    payload[30] ^= 1;
                    frame.payload = wire::encode_storage_rpc_success_response(&payload);
                }
            }
        }));
    count
}

#[test]
fn staging_direct_introduction_remote_unix_tls_cohorts_reject_substitutes_and_converge() {
    for endpoints in [
        [(0, false), (2, true)],
        [(0, true), (2, false)],
        [(2, false), (3, true)],
    ] {
        let (f, owner, proof, directory) = mixed(endpoints);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut captured = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .unwrap();
        captured.install().unwrap();
        let mut publication = captured
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let before = f.observe();
        let shadows = f.shadows();
        let replies: Vec<_> = f
            .remote
            .iter()
            .map(|peer| count(peer, Arc::new(AtomicBool::new(false))))
            .collect();
        // No server is running: each rejection must occur before transport.
        for peer in &f.remote {
            let substitute = UnixStorageNodeClient::new(
                peer.node,
                f.context.id().cluster_epoch(),
                directory.path().join("absent.sock"),
            );
            assert!(publication
                .apply_remote_replica(&substitute)
                .unwrap_err()
                .to_string()
                .contains("exact admitted peer client"));
        }
        let peer = &f.remote[0];
        assert!(publication.apply_remote_replica(&peer.client).is_err());
        assert_eq!(f.observe(), before);
        publication.publish().unwrap();
        let before = f.observe();
        assert!(publication
            .apply(NodeId::new(4))
            .unwrap_err()
            .to_string()
            .contains("witness confirmation"));
        let trailing = f.remote(NodeId::new(2)).unwrap();
        assert!(publication
            .apply_remote_replica(&trailing.client)
            .unwrap_err()
            .to_string()
            .contains("primary confirmation"));
        assert_eq!(f.observe(), before);
        assert!(replies
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0));
        let _running: Vec<_> = f.remote.iter().map(Remote::run).collect();
        for node in [0, 4, 2, 3, 1] {
            assert!(publication.completion().is_err());
            let member = NodeId::new(node);
            if let Some(peer) = f.remote(member) {
                publication.apply_remote_replica(&peer.client).unwrap();
            } else {
                publication.apply(member).unwrap();
            }
            assert_eq!(publication.application(member), Applied::Applied);
        }
        assert_eq!(
            publication
                .completion()
                .unwrap()
                .confirmed_members()
                .unwrap(),
            5
        );
        let mut expected = None;
        for node in 0..5 {
            let node = f.node(NodeId::new(node));
            let pg = node.get_pg(section.pg_id().get()).unwrap();
            let snapshot = pg.command_build_snapshot().unwrap();
            let record = sql::load(&pg.conn, section.pg_id(), &owner.subject.id.attempt)
                .unwrap()
                .unwrap();
            snapshot.finish().unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&record, expected);
            } else {
                expected = Some(record);
            }
        }
        assert_eq!(
            f.shadows(),
            shadows,
            "remote dispatch used hidden embedded stores"
        );
        let before = f.observe();
        for peer in &f.remote {
            publication.apply_remote_replica(&peer.client).unwrap();
        }
        assert_eq!(
            f.observe(),
            before,
            "exact remote replay mutated durable state"
        );
        assert!(replies
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 2));
        let cutoffs = publication.test_original_deadlines();
        let now = crate::clock::current_time_millis();
        let monotonic = crate::clock::monotonic_time_millis();
        crate::clock::with_time_and_monotonic_override(now + 20_000, monotonic + 20_000, || {
            for peer in &f.remote {
                assert!(publication.apply_remote_replica(&peer.client).is_err());
            }
            assert!(publication.completion().is_err());
        });
        assert_eq!(publication.test_original_deadlines(), cutoffs);
        assert_eq!(publication.publication(), Published::MarkerConfirmed);
        assert_eq!(
            publication.test_installation(),
            InstallationObservation::Installed
        );
        assert!(
            replies
                .iter()
                .all(|count| count.load(Ordering::SeqCst) == 2),
            "expired attempt sent RPC"
        );
        let mut terminal = publication
            .into_terminal()
            .unwrap_or_else(|error| panic!("{}", error.error()));
        let primary_state = {
            let node = f.node(NodeId::new(4));
            let pg = node.get_pg(section.pg_id().get()).unwrap();
            introduction::state(&pg)
        };
        terminal.retire().unwrap();
        assert_eq!(
            terminal.retirement(),
            crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired
        );
        let after = f.observe();
        for (index, (old, new)) in before.iter().zip(&after).enumerate() {
            if index != 4 {
                assert_eq!(old.0, new.0, "retirement changed replica state");
                assert_eq!(old.1, new.1, "retirement mutated a replica");
            }
        }
        {
            let node = f.node(NodeId::new(4));
            let pg = node.get_pg(section.pg_id().get()).unwrap();
            assert_eq!(introduction::state(&pg), primary_state);
            let pending: bool = pg
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM metadata_command_pending_slot)",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(!pending);
        }
        assert_eq!(terminal.test_original_deadlines(), cutoffs);
        assert!(replies
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 2));
        // Exercise the real witness RPC, including an authenticated malformed
        // response. Its failure must return retired custody for a fresh read.
        let inspections = Arc::new(AtomicUsize::new(0));
        let corrupt = Arc::new(AtomicBool::new(true));
        if let Some(witness) = f.remote(NodeId::new(0)) {
            let count = Arc::clone(&inspections);
            let bad = Arc::clone(&corrupt);
            witness
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, bytes| {
                    if kind == Kind::MetadataCommandReplicaState {
                        count.fetch_add(1, Ordering::SeqCst);
                        if bad.load(Ordering::SeqCst) {
                            bytes.payload.truncate(1);
                        }
                    }
                }));
            let failure = terminal
                .into_confirmed_window()
                .err()
                .expect("malformed witness cannot confirm");
            terminal = failure.into_terminal();
            assert_eq!(
                inspections.load(Ordering::SeqCst),
                1,
                "hidden witness retry"
            );
            assert_eq!(
                terminal.retirement(),
                crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired
            );
            assert_eq!(terminal.test_original_deadlines(), cutoffs);
            assert_eq!(f.observe(), after);
            corrupt.store(false, Ordering::SeqCst);
        }
        let window = terminal
            .into_confirmed_window()
            .unwrap_or_else(|error| panic!("{}", error.error()));
        window.require_valid().unwrap();
        assert_eq!(window.observation().witness, Some(NodeId::new(0)));
        assert_eq!(
            window.observation().prefix.applied_log_index,
            window.id().log_index().get()
        );
        assert_eq!(window.test_original_deadlines(), cutoffs);
        assert_eq!(
            inspections.load(Ordering::SeqCst),
            if f.remote(NodeId::new(0)).is_some() {
                2
            } else {
                0
            }
        );
        let lock = f
            .map
            .runtime_state()
            .metadata_command_pg_lock(window.id().pg_id());
        let prepared = window
            .prepare_file_dispatch(
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|error| panic!("{}", error.error()));
        assert_eq!(prepared.test_original_deadlines(), cutoffs);
        assert!(lock.try_lock().is_none());
        let writer = prepared
            .release_primary(section)
            .unwrap_or_else(|error| panic!("{}", error.error()));
        assert!(
            lock.try_lock().is_some(),
            "primary release must precede dispatch-ready return"
        );
        drop(writer);
        assert_eq!(f.observe(), after);
    }
}

#[test]
fn staging_direct_introduction_remote_lost_and_crossed_confirmations_retain_exact_history() {
    for tls in [false, true] {
        for lost_node in [0, 2] {
            let (f, owner, proof, _dir) = mixed([
                (0, if lost_node == 0 { tls } else { !tls }),
                (2, if lost_node == 2 { tls } else { !tls }),
            ]);
            let admission = f.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let mut captured = section
                .prepare_direct_introduction(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    &owner.subject.id.attempt,
                    &proof,
                )
                .unwrap();
            captured.install().unwrap();
            let mut publication = captured
                .into_publication()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish().unwrap();
            let lost = f.remote(NodeId::new(lost_node)).unwrap();
            let corrupt = Arc::new(AtomicBool::new(false));
            let replies = count(lost, Arc::clone(&corrupt));
            let _others: Vec<_> = f
                .remote
                .iter()
                .filter(|peer| peer.node != lost.node)
                .map(Remote::run)
                .collect();
            if lost_node == 2 {
                publication
                    .apply_remote_replica(&f.remote(NodeId::new(0)).unwrap().client)
                    .unwrap();
                for node in [4, 3, 1] {
                    publication.apply(NodeId::new(node)).unwrap();
                }
            }
            let before = f.observe();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    lost.server
                        .test_accept_one_complete_application_fault(false, true)
                        .unwrap()
                });
                assert!(publication.apply_remote_replica(&lost.client).is_err());
                worker.join().unwrap();
            });
            assert_eq!(replies.load(Ordering::SeqCst), 1, "hidden retry");
            assert_eq!(publication.application(lost.node), Applied::Unconfirmed);
            assert_ne!(
                f.observe()[lost_node as usize].0,
                before[lost_node as usize].0,
                "reply loss preceded apply"
            );
            if lost_node == 0 {
                assert!(publication
                    .apply(NodeId::new(4))
                    .unwrap_err()
                    .to_string()
                    .contains("witness confirmation"));
            } else {
                assert!(publication
                    .completion()
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("confirmed 4 of 5"));
            }
            let applied = f.observe();
            let _retry = lost.run();
            corrupt.store(true, Ordering::SeqCst);
            assert!(publication.apply_remote_replica(&lost.client).is_err());
            assert_eq!(publication.application(lost.node), Applied::Unconfirmed);
            assert_eq!(f.observe(), applied);
            assert_eq!(replies.load(Ordering::SeqCst), 2);
            corrupt.store(false, Ordering::SeqCst);
            publication.apply_remote_replica(&lost.client).unwrap();
            assert_eq!(publication.application(lost.node), Applied::Applied);
            assert_eq!(f.observe(), applied);
            assert_eq!(replies.load(Ordering::SeqCst), 3);
            if lost_node == 0 {
                publication.apply(NodeId::new(4)).unwrap();
                publication
                    .apply_remote_replica(&f.remote(NodeId::new(2)).unwrap().client)
                    .unwrap();
                for node in [3, 1] {
                    publication.apply(NodeId::new(node)).unwrap();
                }
            }
            assert_eq!(
                publication
                    .completion()
                    .unwrap()
                    .confirmed_members()
                    .unwrap(),
                5
            );
            assert_eq!(
                publication.test_installation(),
                InstallationObservation::Installed
            );
            let before_drop = f.observe();
            drop(publication);
            assert_eq!(f.observe(), before_drop);
        }
    }
}
