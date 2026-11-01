// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::{
    PutObjectMutationEffectRoute, StreamAppendCommitRequest, StreamAppendStoredSegmentSize,
};
use crate::traits::ShardStore;
use crate::types::AdmittedRouteEffectFence;
use crate::{PgMetadataStore, PgStore};

// Test the complete publication callers, not merely the fenced store adapter.
// The trigger makes any attempted insertion observable even if caller cleanup
// subsequently removes its row. A lost delegation must yield the sentinel DB
// error instead of the required captured-deadline error.
#[test]
fn fresh_direct_put_delegates_ack_deadline_before_insertion() {
    direct_put_ack_deadline(false);
}

#[test]
fn matching_direct_put_restores_acks_after_request_deadline() {
    direct_put_ack_deadline(true);
}

fn direct_put_ack_deadline(matching: bool) {
    let time = crate::clock::test_time_override_guard(1_000);
    let tmp = test_util::tempdir();
    let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &nodes, &[0], EcShape { k: 2, m: 1 }).unwrap());
    let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::BucketName::try_from("ack-deadline").unwrap();
    let key = crate::ObjectKey::try_from("direct").unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = crate::SessionId::try_from("d".repeat(32)).unwrap();
    let generation = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation)
        .unwrap();
    let payload = b"real encrypted Direct PUT payload";
    let okh = crate::direct_put_segment_key_hash(&reservation, 0);
    let written = write_encrypted_direct_put_segment_payload_shards(
        &cluster, &bucket, &key, generation, 0, &okh, payload,
    )
    .unwrap();
    let request = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation,
            generation_id: generation,
            payload,
            segment_okh: okh,
            written: &written,
        },
    );
    let pg_id = cluster.object_metadata_pg(&bucket, &key).pg_id();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    {
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        if matching {
            let command = cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &request,
                    &written.written_shards,
                    crate::VersionId::Null,
                    request.bucket_write_reservation.clone(),
                )
                .unwrap();
            drop(pg);
            insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
        } else {
            pg.test_reject_shard_ack_insertion();
        }
    }
    let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000);
    let control = time.control();
    let ran = Arc::new(AtomicBool::new(false));
    let hook_ran = Arc::clone(&ran);
    let result = PgStore::test_with_before_shard_ack_effect_hook(
        move |in_transaction| {
            assert!(in_transaction);
            control.set(4_500);
            hook_ran.store(true, Ordering::SeqCst);
        },
        || {
            cluster.commit_direct_put_object_from_payload_shards_with_route_validation(
                PutObjectMutationEffectRoute {
                    bucket_pg_id: cluster.bucket_metadata_pg(&bucket),
                    object_pg_id: cluster.object_metadata_pg(&bucket, &key),
                    bucket: &bucket,
                    key: &key,
                    effect_fence: fence,
                },
                &request,
                &written.written_shards,
                || {},
                || fence.require_valid_for(ClusterEpoch::INITIAL),
                |_| Ok::<(), ()>(()),
            )
        },
    );
    assert!(ran.load(Ordering::SeqCst));
    assert!(fence.require_valid_for(ClusterEpoch::INITIAL).is_err());
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    if matching {
        result.unwrap().unwrap();
        for shard in &written.written_shards {
            pg.validate_written_shard_ack(&shard.key, shard.ack)
                .unwrap();
        }
        assert!(pg.get_object_meta(&bucket, &key).is_ok());
    } else {
        assert!(
            matches!(
                result,
                Err(crate::ObjectPgActionError::Store(
                    StoreError::RouteMapExpired { .. }
                ))
            ),
            "{result:?}"
        );
        for shard in &written.written_shards {
            assert!(matches!(
                pg.stat_shard(&shard.key),
                Err(StoreError::NotFound)
            ));
        }
        assert!(matches!(
            pg.get_object_meta(&bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn fresh_stream_append_delegates_ack_deadline_before_insertion() {
    let time = crate::clock::test_time_override_guard(1_000);
    let tmp = test_util::tempdir();
    let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &nodes, &[0], EcShape { k: 2, m: 1 }).unwrap());
    let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::BucketName::try_from("ack-deadline").unwrap();
    let key = crate::ObjectKey::try_from("stream").unwrap();
    create_test_bucket(&cluster, &bucket);
    let session = crate::SessionId::try_from("e".repeat(32)).unwrap();
    let encrypted = crate::tests::sse_s3_segment_test_fixture(b"real encrypted stream payload", 0);
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session,
            encrypted.encryption.clone(),
        )
        .unwrap();
    let (_, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session.clone(),
                segment_index: 0,
                size: encrypted.logical_size,
                segment_crc64: encrypted.storage_crc64,
                payload_crc64: encrypted.payload_crc64,
            },
        )
        .unwrap();
    let shards = cluster
        .write_stream_segment_payload_shards(&segment, &encrypted.storage_bytes)
        .unwrap();
    let batch: Vec<_> = shards.iter().map(|shard| (&shard.key, shard.ack)).collect();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(0))
        .unwrap();
    primary
        .storage_node()
        .get_pg(0)
        .unwrap()
        .test_reject_shard_ack_insertion();
    let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000);
    let control = time.control();
    let ran = Arc::new(AtomicBool::new(false));
    let hook_ran = Arc::clone(&ran);
    let result = PgStore::test_with_before_shard_ack_effect_hook(
        move |in_transaction| {
            assert!(in_transaction);
            control.set(4_500);
            hook_ran.store(true, Ordering::SeqCst);
        },
        || {
            cluster.commit_stream_segment_append_with_route_validation(
                PutObjectMutationEffectRoute {
                    bucket_pg_id: cluster.bucket_metadata_pg(&bucket),
                    object_pg_id: cluster.object_metadata_pg(&bucket, &key),
                    bucket: &bucket,
                    key: &key,
                    effect_fence: fence,
                },
                StreamAppendCommitRequest {
                    bucket: &bucket,
                    key: &key,
                    session_id: &session,
                    segment_index: 0,
                    segment_record: &segment,
                    stored_segment_size: StreamAppendStoredSegmentSize::Exact(
                        encrypted.storage_bytes.len() as u64,
                    ),
                    shard_batch: &batch,
                },
                &crate::StreamUploadTarget::PutObject,
                || fence.require_valid_for(ClusterEpoch::INITIAL),
            )
        },
    );
    assert!(ran.load(Ordering::SeqCst));
    assert!(
        matches!(
            result,
            Err(crate::ObjectPgActionError::Store(
                StoreError::RouteMapExpired { .. }
            ))
        ),
        "{result:?}"
    );
    let pg = primary.storage_node().get_pg(0).unwrap();
    for shard in &shards {
        assert!(matches!(
            pg.stat_shard(&shard.key),
            Err(StoreError::NotFound)
        ));
    }
    assert!(pg.get_stream_upload(&session).is_ok());
}
