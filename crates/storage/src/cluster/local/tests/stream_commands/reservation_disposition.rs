// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_client::{BuildCreateStreamUploadCommandReq, CreateStreamUploadPrecondition};
use crate::node_runtime::payload_placement::PayloadPlacementInput;

fn reservations(
    map: &LocalClusterMap,
    bucket: &BucketName,
) -> Vec<crate::BucketWriteReservationRecord> {
    let pg_id = PgId::new(map.pg_topology.bucket_pg_for(bucket));
    let primary = map.metadata_pg_primary_node(map.epoch, pg_id).unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let mut rows = crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, bucket).unwrap();
    rows.sort_by(|a, b| a.reservation_id.cmp(&b.reservation_id));
    rows
}

#[test]
fn applied_stream_creation_reservation_disposition_matches_after_reopen() {
    for reopen in [false, true] {
        for part in [false, true] {
            let tmp = test_util::tempdir();
            let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
            let pg_ids = [0, 1, 2, 3];
            let ec = EcShape { k: 2, m: 1 };
            let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec).unwrap());
            let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
            let bucket = crate::tests::bucket_name("disposition");
            let key = crate::tests::object_key("key");
            create_test_bucket(&cluster, &bucket);
            // Metadata-only creation/cleanup test: no shard payload is written.
            let encryption = crate::tests::metadata_only_test_encryption();
            let upload = part.then(|| {
                create_encrypted_test_multipart_upload(
                    &cluster,
                    &bucket,
                    &key,
                    &upload_id_from_label("disposition"),
                    encryption.clone(),
                )
            });
            let session_id = crate::tests::stream_session_id("disposition");
            let target = upload
                .as_ref()
                .map_or(crate::StreamUploadTarget::PutObject, |upload| {
                    crate::StreamUploadTarget::UploadPart {
                        upload_id: upload.upload_id.clone(),
                        part_number: 1,
                    }
                });
            let proof = acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                if part {
                    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                } else {
                    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                },
                Some(key.as_str()),
            );
            // Same object, independent responsibility: creation must neither
            // release a future part-finalization proof nor another PUT proof.
            let canary = acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                if part {
                    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND
                } else {
                    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                },
                Some(key.as_str()),
            );
            let before = reservations(&map, &bucket);
            assert_eq!(before.len(), 2);
            assert_ne!(proof.reservation_id, canary.reservation_id);
            let command = {
                let client = cluster
                    .object_mutation_metadata_primary_client(&bucket, &key)
                    .unwrap();
                let route = client
                    .open_stream_upload_creation_metadata_route(
                        cluster.operation_epoch(),
                        cluster.object_metadata_pg(&bucket, &key),
                        &bucket,
                        &key,
                    )
                    .unwrap();
                route
                    .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                        effect_fence: crate::types::AdmittedRouteEffectFence::unbounded(
                            cluster.operation_epoch(),
                        ),
                        placement: PayloadPlacementInput::from_local_map(&map),
                        request: &crate::CreateStreamUploadReq {
                            session_id: session_id.clone(),
                            bucket: bucket.clone(),
                            key: key.clone(),
                            target,
                            encryption,
                        },
                        cleanup_after: Some(crate::clock::current_time_millis() + 60_000),
                        bucket_write_reservation: &proof,
                        precondition: upload.as_ref().map_or(
                            CreateStreamUploadPrecondition::PutObject {
                                expected_current: None,
                            },
                            |upload| CreateStreamUploadPrecondition::UploadPart {
                                expected_upload: upload,
                            },
                        ),
                    })
                    .unwrap()
            };
            insert_pending_metadata_command_for_test(&map, command.id().pg_id(), &bucket, &command);
            for node_id in node_ids {
                map.node(node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(command.id().pg_id().get())
                    .unwrap()
                    .apply_metadata_command_and_record(node_id.as_u32(), &command)
                    .unwrap();
            }
            assert_eq!(
                reservations(&map, &bucket),
                before,
                "apply alone must not release either bucket proof"
            );
            let (map, cluster) = if reopen {
                drop(cluster);
                drop(map);
                let map =
                    Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec).unwrap());
                let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
                (map, cluster)
            } else {
                cluster
                    .release_applied_metadata_command_bucket_write_reservations(&command)
                    .unwrap();
                // Retry the cleanup itself, then let the ordinary exact-command
                // convergence path remove the slot before the subsequent abort.
                cluster
                    .release_applied_metadata_command_bucket_write_reservations(&command)
                    .unwrap();
                let outcome = cluster
                    .drain_pending_metadata_command_with_recovery_gate(
                        command.id().pg_id(),
                        &command,
                    )
                    .unwrap();
                assert!(outcome.is_logically_applied());
                assert!(
                    pending_metadata_command_for_test(&map, command.id().pg_id(), &bucket)
                        .is_none()
                );
                (map, cluster)
            };
            let expected = before
                .iter()
                .filter(|row| !part || row.reservation_id != proof.reservation_id)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                reservations(&map, &bucket),
                expected,
                "reopen={reopen} part={part}"
            );
            for node_id in node_ids {
                let pg = map
                    .node(node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(command.id().pg_id().get())
                    .unwrap();
                let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
                assert_eq!(
                    session.bucket_write_reservation.as_ref(),
                    (!part).then_some(&proof)
                );
            }
            cluster
                .abort_stream_upload_session(&bucket, &key, &session_id)
                .unwrap();
            let only_canary = before
                .into_iter()
                .filter(|row| row.reservation_id == canary.reservation_id)
                .collect::<Vec<_>>();
            assert_eq!(
                reservations(&map, &bucket),
                only_canary,
                "abort must release only the stream's own obligation"
            );
        }
    }
}
