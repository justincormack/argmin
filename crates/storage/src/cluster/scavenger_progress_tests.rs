// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::{
    ShardScavengerPayloadReference, ShardScavengerProtectedShardSetReference,
    ShardScavengerReferencePageItem, ShardScavengerStagingProtection,
};
use crate::{EcShape, GenerationId, NodeId, SessionId};

fn pending(index: u64) -> Cursor {
    Cursor::PendingStagingWindow {
        cluster_epoch: ClusterEpoch::INITIAL,
        pg_id: PgId::new(0),
        log_index: index,
        command_checksum: 7,
        protection_checksum: 9,
        topology_digest: [4; 32],
    }
}

fn materialized(index: u32) -> Cursor {
    Cursor::StagingWindow {
        pg_id: PgId::new(0),
        topology_digest: [5; 32],
        attempt: SessionId::try_from("1".repeat(32)).unwrap(),
        introduction_epoch: ClusterEpoch::INITIAL,
        introduction_index: 1,
        window_index: index,
    }
}

fn object() -> Cursor {
    Cursor::ObjectSegment {
        bucket: crate::BucketName::new("progress").unwrap(),
        key: crate::ObjectKey::new("key").unwrap(),
        version_id: 0,
        segment_index: 0,
    }
}

fn page(cursor: Cursor, complete: bool) -> ShardScavengerReferencePage {
    ShardScavengerReferencePage {
        complete,
        items: vec![ShardScavengerReferencePageItem {
            cursor,
            reference: ShardScavengerPayloadReference::StagingProtection(
                ShardScavengerStagingProtection {
                    identity: ShardScavengerProtectedShardSetReference {
                        data_pg_id: 0,
                        okh: [8; 16],
                        generation_id: GenerationId::new(1).unwrap(),
                        ec: EcShape { k: 1, m: 0 },
                    },
                    placement_epoch: ClusterEpoch::INITIAL,
                    file_nodes: vec![NodeId::new(0)],
                    acknowledgement_primary: NodeId::new(0),
                },
            ),
        }],
    }
}

fn step(progress: &mut ReferenceProgress, after: &mut Option<Cursor>, next: Cursor) {
    let updated = progress
        .preview(after.as_ref(), &page(next.clone(), false))
        .unwrap();
    *progress = updated;
    *after = Some(next);
}

#[test]
fn scavenger_progress_rejects_cycles_and_pending_identity_reentry() {
    for (a, b) in [(materialized(0), materialized(1)), (pending(1), pending(2))] {
        let mut progress = ReferenceProgress::default();
        let mut after = None;
        step(&mut progress, &mut after, a.clone());
        step(&mut progress, &mut after, b);
        assert!(progress.preview(after.as_ref(), &page(a, false)).is_err());
        assert!(progress.is_invalid());
        assert!(progress
            .preview(after.as_ref(), &page(materialized(9), true))
            .is_err());
    }
}

#[test]
fn scavenger_progress_permits_pending_restart_prefix_and_materialization() {
    let mut progress = ReferenceProgress::default();
    let mut after = None;
    // Position-only checks below deliberately use cursors directly, independent
    // of the separate payload-kind validation at the store/RPC boundary.
    step(&mut progress, &mut after, object());
    step(&mut progress, &mut after, pending(1));
    step(&mut progress, &mut after, object()); // legitimate replaced-slot rescan
    step(&mut progress, &mut after, pending(2));
    step(&mut progress, &mut after, object()); // pending becomes materialized
    step(&mut progress, &mut after, materialized(0));
    step(&mut progress, &mut after, materialized(1));
    assert_eq!(progress.retired.len(), 2);
    assert!(!progress.is_invalid());
}

#[test]
fn scavenger_progress_bounds_restarts_without_retaining_every_key() {
    let mut progress = ReferenceProgress::default();
    let mut after = None;
    step(&mut progress, &mut after, pending(1));
    for index in 2..=MAX_PENDING_RESTARTS as u64 + 1 {
        step(&mut progress, &mut after, pending(index));
    }
    assert_eq!(progress.retired.len(), MAX_PENDING_RESTARTS);
    assert!(progress
        .preview(after.as_ref(), &page(pending(100), false))
        .unwrap_err()
        .to_string()
        .contains("restart budget exhausted"));
    assert_eq!(progress.retired.len(), MAX_PENDING_RESTARTS);
    assert!(progress.is_invalid());
}

#[test]
fn scavenger_progress_validates_every_item_and_does_not_commit_failed_preview() {
    let mut progress = ReferenceProgress::default();
    let mut bad = page(materialized(0), false);
    bad.items.push(page(materialized(1), false).items.remove(0));
    bad.items.push(page(materialized(0), false).items.remove(0));
    assert!(progress.preview(None, &bad).is_err());
    assert!(progress.retired.is_empty());
    let mut progress = ReferenceProgress::default();
    let tentative = progress
        .preview(Some(&pending(1)), &page(pending(2), false))
        .unwrap();
    assert_eq!(tentative.retired.len(), 1);
    assert!(
        progress.retired.is_empty(),
        "a later expansion error must not retire the source"
    );
}

#[test]
fn scavenger_progress_rejects_same_command_index_cycles_and_crossed_owner() {
    let a = pending(1);
    let mut b = a.clone();
    if let Cursor::PendingStagingWindow {
        protection_checksum,
        ..
    } = &mut b
    {
        *protection_checksum += 1;
    }
    let mut progress = ReferenceProgress::default();
    let mut after = None;
    step(&mut progress, &mut after, a.clone());
    step(&mut progress, &mut after, b);
    assert!(progress.preview(after.as_ref(), &page(a, false)).is_err());
    for field in 0..3 {
        let a = materialized(0);
        let mut b = materialized(1);
        let Cursor::StagingWindow {
            pg_id,
            topology_digest,
            introduction_index,
            ..
        } = &mut b
        else {
            unreachable!()
        };
        match field {
            0 => *pg_id = PgId::new(1),
            1 => topology_digest[0] ^= 1,
            2 => *introduction_index += 1,
            _ => unreachable!(),
        }
        assert!(!advances(&a, &b));
    }
}

#[test]
fn scavenger_progress_pins_native_phase_order_and_pending_restarts() {
    let bucket = crate::BucketName::new("progress").unwrap();
    let key = crate::ObjectKey::new("key").unwrap();
    let cursors = [
        object(),
        Cursor::StreamUploadSegment {
            session_id: SessionId::try_from("a".repeat(32)).unwrap(),
            segment_index: 1,
        },
        Cursor::MultipartPartSegment {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: crate::UploadId::for_test("upload"),
            part_number: 1,
            segment_index: 1,
        },
        Cursor::ObjectReclaimSegment {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id: GenerationId::new(1).unwrap(),
            segment_index: 1,
        },
        Cursor::MultipartReclaimSegment {
            bucket,
            key,
            generation_id: GenerationId::new(1).unwrap(),
            part_number: 1,
            segment_index: 1,
        },
        Cursor::PendingPlacedCommand {
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(0),
            log_index: 1,
            command_checksum: 7,
            reference_index: 0,
        },
        Cursor::PendingReclaimCommand {
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(0),
            log_index: 1,
            command_checksum: 7,
            reference_index: 0,
        },
        pending(1),
        materialized(0),
    ];
    for (index, cursor) in cursors.iter().enumerate() {
        for (next_index, next) in cursors.iter().enumerate() {
            assert_eq!(
                advances(cursor, next),
                next_index > index,
                "{cursor:?} -> {next:?}"
            );
        }
    }
    for a in &cursors[5..=7] {
        let mut b = a.clone();
        match &mut b {
            Cursor::PendingPlacedCommand { log_index, .. }
            | Cursor::PendingReclaimCommand { log_index, .. }
            | Cursor::PendingStagingWindow { log_index, .. } => *log_index += 1,
            _ => unreachable!(),
        }
        let mut progress = ReferenceProgress::default();
        let mut after = None;
        step(&mut progress, &mut after, a.clone());
        step(&mut progress, &mut after, object());
        step(&mut progress, &mut after, b);
        assert!(progress
            .preview(after.as_ref(), &page(a.clone(), false))
            .is_err());
        assert!(progress.is_invalid());
    }
}

#[test]
fn scavenger_progress_authenticated_unix_a_b_a_fails_before_audit_effects() {
    use crate::cluster::{LocalClusterMap, LocalUnixStorageNodeClientConfig};
    use crate::control_plane_auth::{
        ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
        ControlPlaneScopedCredentialStore,
    };
    use crate::storage_rpc::*;
    use crate::storage_rpc_auth::write_storage_rpc_auth_transport_frame;
    use std::{
        os::unix::{
            fs::PermissionsExt,
            net::{UnixListener, UnixStream},
        },
        sync::Arc,
        time::Duration,
    };
    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    for (a, b) in [(materialized(0), materialized(1)), (pending(1), pending(2))] {
        let dir = test_util::tempdir();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("peer.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let credential = ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "progress".to_owned(),
            credential_id: "maintenance".to_owned(),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::LocalMaintenance {
                process_id: "audit".to_owned(),
            },
            secret: b"scavenger-progress-test".to_vec(),
        })
        .unwrap();
        let auth = crate::StorageRpcServerAuthConfig::new(
            credential.cluster_id(),
            ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap(),
            9,
            DIGEST,
        )
        .unwrap();
        let client_auth =
            crate::MaintenanceStorageRpcClientCapability::new(credential, 9, DIGEST).unwrap();
        let responses = [a.clone(), b.clone(), a.clone()];
        let peer = std::thread::spawn(move || {
            let mut after = None;
            for cursor in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let Ok((envelope, _reservation)) = auth.read_request_envelope(&mut stream) else {
                    return;
                };
                let (request, signing) = auth
                    .verify_request(
                        NodeId::new(0),
                        crate::clock::current_time_millis(),
                        &envelope,
                    )
                    .unwrap()
                    .into_frame_and_response_signing_context();
                assert_eq!(
                    request.kind,
                    StorageRpcMessageKind::ShardScavengerReferencePage,
                    "no physical audit RPC may be issued"
                );
                let decoded =
                    decode_shard_scavenger_reference_page_request(&request.payload).unwrap();
                assert_eq!(decoded.after, after);
                let payload =
                    encode_shard_scavenger_reference_page_response(&page(cursor.clone(), false))
                        .unwrap();
                let response = StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                };
                let signed = auth
                    .sign_response(
                        &signing,
                        NodeId::new(0),
                        crate::clock::current_time_millis(),
                        &response,
                    )
                    .unwrap();
                write_storage_rpc_auth_transport_frame(&mut stream, &signed).unwrap();
                after = Some(cursor);
            }
        });
        // Wake/join the peer on early assertion failure too.
        struct JoinPeer(std::path::PathBuf, Option<std::thread::JoinHandle<()>>);
        impl Drop for JoinPeer {
            fn drop(&mut self) {
                let _ = UnixStream::connect(&self.0);
                if let Some(peer) = self.1.take() {
                    let _ = peer.join();
                }
            }
        }
        let mut peer = JoinPeer(socket.clone(), Some(peer));
        let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
            NodeId::new(0),
            [NodeId::new(0)],
            &[0],
            EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
            NodeId::new(0),
            socket,
        )
        .with_maintenance_rpc_auth(client_auth)])
            .unwrap();
        let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
        let mut pass = cluster.begin_shard_scavenger_audit_pass();
        for expected in [a, b.clone()] {
            assert_eq!(
                cluster.audit_next_shard_scavenger_pg(&mut pass).unwrap(),
                Some(vec![])
            );
            assert_eq!(pass.reference_after, Some(expected));
        }
        let before_files = pass.referenced_scans[&0].staging_files.clone();
        let error = cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap_err();
        assert!(
            matches!(
                error,
                StoreError::ShardScavengerScanIncomplete {
                    context: "reference page progress",
                    ..
                }
            ),
            "{error:?}"
        );
        peer.1.take().unwrap().join().unwrap();
        assert_eq!(pass.reference_after, Some(b));
        assert_eq!(pass.next_reference_pg_index, 0);
        assert_eq!(pass.next_pg_index, 0);
        assert_eq!(pass.referenced_scans[&0].staging_files, before_files);
        assert!(pass.current_unreferenced.is_empty());
        assert!(pass.reference_scan_invalid());
        assert!(cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap_err()
            .to_string()
            .contains("requires fresh discovery"));
        crate::maintenance::assert_scavenger_progress_failure_restarts_cleanly(cluster, pass);
    }
}
