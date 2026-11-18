// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::super::super::abandonment_remote as shared;
use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonmentQuery, CompleteAbandonmentSendError as E, CompleteUnstartedAbandonment,
    UnstartedAbandonmentObservation as O,
};

const KIND: Kind = Kind::MetadataCommandRecordAbandoned;

#[test]
fn staging_replica_abandonment_historical_remote_primary_keeps_report_and_embedded_replica_custody()
{
    use crate::node_runtime::pg_store::ReplicaAbandonmentObservation as R;
    for tls in [false, true] {
        for case in 0..4 {
            let mixed = Mixed::new(case, &[(4, tls), (0, !tls)]);
            let shadows = mixed.shadows();
            with_owner(&mixed, "uncertain", |owner| {
                let primary = mixed.peer(4);
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                let before = mixed.observe();
                owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(
                    owner.replica_observation(NodeId::new(2)),
                    R::TombstoneConfirmed
                );
                let after = mixed.observe();
                assert_ne!(after[2], before[2]);
                for n in [0, 1, 3, 4] {
                    assert_eq!(after[n], before[n]);
                }
                owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(mixed.observe(), after);
            });
            assert_eq!(mixed.shadows(), shadows);
        }
    }
}

pub(super) fn with_owner(
    mixed: &Mixed,
    origin: &str,
    action: impl FnOnce(&mut CompleteUnstartedAbandonment<'_>),
) {
    let f = &mixed.fixture;
    let id = f.context.id();
    let admission = f.handle.admit_current_route().unwrap();
    let node = mixed.node(4);
    let source = shared::sender(
        &node.get_pg(id.pg_id().get()).unwrap(),
        &f.context,
        origin,
        deadline(),
    );
    let mut owner = source
        .into_fanout(
            admission
                .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    action(&mut owner);
}

#[test]
fn staging_abandonment_historical_remote_unix_tls_contexts_restart_and_no_write_confirmation() {
    for tls in [false, true] {
        for case in 0..4 {
            let mixed = Mixed::new(case, &[(4, tls), (0, !tls)]);
            let shadows = mixed.shadows();
            with_owner(&mixed, "uncertain", |owner| {
                let primary = mixed.peer(4);
                let node = mixed.node(4);
                let id = mixed.fixture.context.id();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let retained = (introduction::state(&pg), complete_pending::observe(&pg));
                drop(pg);
                let before = mixed.observe();
                let original = owner.installation_observation();
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                assert_eq!(owner.installation_observation(), original);
                let after = mixed.observe();
                assert_ne!(after[4], before[4]);
                assert_eq!(&after[..4], &before[..4]);
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!(
                    (introduction::state(&pg), complete_pending::observe(&pg)),
                    retained
                );
                assert!(!started(&pg));
            });
            let before = mixed.observe();
            let mixed = lifetime::reopen(mixed);
            for ((after, _), (before, _)) in mixed.observe().iter().zip(&before) {
                assert_eq!(after, before);
            }
            assert_eq!(mixed.shadows(), shadows);
            with_owner(&mixed, "inspected", |owner| {
                let primary = mixed.peer(4);
                let before = mixed.observe();
                let count = primary.count(KIND);
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                assert_eq!(count.load(Ordering::SeqCst), 1);
                assert_eq!(
                    mixed.observe(),
                    before,
                    "reopened exact tombstone confirms without writes"
                );
            });
        }
    }
}

#[test]
fn staging_abandonment_historical_remote_rejects_equal_crc_crossed_report_and_admission() {
    for tls in [false, true] {
        let mixed = Mixed::new(2, &[(4, tls), (0, !tls)]);
        with_owner(&mixed, "installed", |owner| {
            let primary = mixed.peer(4);
            let payload = shared::encode(
                &mixed.fixture.context,
                primary.id,
                crate::clock::current_time_millis() + 60_000,
            );
            let query = CompleteAbandonmentQuery::decode(&payload, &topology()).unwrap();
            let before = mixed.observe();
            primary.server.test_abandonment_crossed_admission(
                &mixed.peer(0).server,
                &query.request().unwrap(),
            );
            assert_eq!(mixed.observe(), before);
            let mut crossed = primary.config.clone();
            let p = crossed.pending_metadata_command_recoveries[0].1.pending();
            let mut sha = p.command_sha256();
            sha[0] ^= 1;
            crossed.pending_metadata_command_recoveries[0].1 =
                crate::control_plane::PendingMetadataCommandRecovery::new(
                    primary.id,
                    PendingMetadataCommandObservation::new(
                        p.cluster_epoch(),
                        NonZeroU64::new(p.log_index()).unwrap(),
                        p.command_checksum(),
                        sha,
                    ),
                );
            primary
                .server
                .install_control_plane_runtime_config(crossed)
                .unwrap();
            let seen = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&seen);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == KIND {
                        let error =
                            crate::storage_rpc::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap_err();
                        assert_eq!(
                            error.code,
                            crate::storage_rpc::StorageRpcErrorCode::StaleShardLocation
                        );
                        assert!(error
                            .message
                            .contains("exact historical pending-command authorization"));
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            {
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                assert!(matches!(
                    owner.record_primary_remote(&primary.client, &session, deadline()),
                    Err(E::Unconfirmed(_))
                ));
            }
            assert_eq!(seen.load(Ordering::SeqCst), 1);
            assert_eq!(owner.observation(), O::Unconfirmed);
            assert_eq!(mixed.observe(), before);
            primary
                .server
                .install_control_plane_runtime_config(primary.config.clone())
                .unwrap();
            let count = primary.count(KIND);
            let _running = primary.run();
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
            assert_eq!(count.load(Ordering::SeqCst), 1);
        });
    }
}

#[test]
fn staging_abandonment_historical_remote_rechecks_server_and_wire_cutoffs_beside_insert_and_commit()
{
    for kind in ["server", "wire"] {
        for boundary in ["before-tombstone", "before-commit"] {
            let mixed = Mixed::new(1, &[(4, false)]);
            let primary = mixed.peer(4);
            let mut config = primary.config.clone();
            let now = crate::clock::current_time_millis();
            let expires = now + if kind == "server" { 3_000 } else { 1_000 };
            if kind == "server" {
                config.route_map_validity = RouteMapValidity::until_ms(expires).unwrap();
                primary
                    .server
                    .install_control_plane_runtime_config(config.clone())
                    .unwrap();
            }
            let payload = shared::encode(
                &mixed.fixture.context,
                primary.id,
                now + if kind == "wire" { 1_000 } else { 60_000 },
            );
            let query = CompleteAbandonmentQuery::decode(&payload, &topology()).unwrap();
            let request = query.request().unwrap();
            let before = mixed.observe();
            let mut reached = false;
            let result = primary
                .server
                .test_with_abandonment_authority(&request, deadline(), |pg, authority| {
                    request.test_abandon_with_hook(pg, authority, |at| {
                        if at == boundary {
                            reached = true;
                            assert!(crate::clock::current_time_millis() < expires);
                            if kind == "server" {
                                config.route_map_validity =
                                    RouteMapValidity::until_ms(now + 60_000).unwrap();
                                primary
                                    .server
                                    .install_control_plane_runtime_config(config.clone())
                                    .unwrap();
                            }
                            while crate::clock::current_time_millis() <= expires {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                        }
                        Ok(())
                    })
                })
                .unwrap();
            assert!(reached);
            assert!(result.is_err(), "{kind}/{boundary}");
            for ((after, changes), (before, old_changes)) in mixed.observe().iter().zip(&before) {
                assert_eq!(after, before);
                if boundary == "before-tombstone" {
                    assert_eq!(changes, old_changes, "no effect before expiry");
                }
            }
        }
    }
}
