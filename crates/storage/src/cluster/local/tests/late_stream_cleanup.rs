// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy)]
enum Target {
    PutObject,
    UploadPart,
}

#[derive(Clone, Copy)]
enum PauseAfter {
    Preparation,
    PublishedAppend,
}

fn shard_state(
    cluster: &StorageCluster,
    segment: &crate::StreamUploadSegmentRecord,
) -> Vec<(bool, bool)> {
    let ec = EcShape {
        k: segment.ec_k,
        m: segment.ec_m,
    };
    (0..ec.k + ec.m)
        .map(|index| {
            let key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), index);
            (
                cluster.test_shard_exists(segment.data_pg_id, &key).unwrap(),
                cluster
                    .test_payload_shard_file_exists(
                        segment.data_pg_id,
                        ec,
                        &segment.segment_okh,
                        segment.segment_vid,
                        index,
                    )
                    .unwrap(),
            )
        })
        .collect()
}

fn assert_abort_fences_delayed_stream_write(target: Target, pause_after: PauseAfter) {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0, 1, 2, 3],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let (bucket, key, _, _) = bucket_key_with_distinct_object_and_data_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
    );
    let cluster = StorageCluster::from_static_local_map(map).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("a4".repeat(16)).unwrap();
    let expected_target = match target {
        Target::PutObject => {
            cluster
                .create_put_object_stream_session_record(
                    &bucket,
                    &key,
                    &session_id,
                    crate::ObjectEncryption::None,
                )
                .unwrap();
            crate::StreamUploadTarget::PutObject
        }
        Target::UploadPart => {
            let (completion, _) =
                seed_streamed_multipart_completion(&cluster, &bucket, &key, "latestreamcleanup");
            let upload = cluster
                .load_in_progress_multipart_upload(&bucket, &key, &completion.upload_id)
                .unwrap();
            cluster
                .create_upload_part_stream_session(
                    &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                    2,
                    &session_id,
                )
                .unwrap();
            crate::StreamUploadTarget::UploadPart {
                upload_id: completion.upload_id,
                part_number: 2,
            }
        }
    };
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let payload = b"delayed stream payload";
    let (actual_target, segment) = cluster
        .prepare_stream_segment_append_with_route_validation(
            crate::cluster::PutObjectMutationEffectRoute {
                bucket_pg_id: cluster.bucket_metadata_pg(&bucket),
                object_pg_id: cluster.object_metadata_pg(&bucket, &key),
                bucket: &bucket,
                key: &key,
                effect_fence: admission.effect_fence(),
            },
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
            },
            || admission.require_valid_now_raw(),
        )
        .unwrap();
    assert_eq!(actual_target, expected_target);
    let absent = vec![(false, false); usize::from(segment.ec_k + segment.ec_m)];
    assert_eq!(shard_state(&cluster, &segment), absent);

    // Use the production placed-write phase with the original, still-live route
    // admission. Splitting the phases models a delayed request whose caller no
    // longer runs append-error compensation; no sleep or background worker races.
    let write = || {
        cluster.write_stream_segment_payload_shards_with_route_validation::<StoreError>(
            &segment,
            payload,
            admission.effect_fence(),
            || admission.require_valid_now_raw(),
            &mut || Ok(()),
        )
    };
    if matches!(pause_after, PauseAfter::PublishedAppend) {
        let written = write().unwrap();
        let batch: Vec<_> = written
            .iter()
            .map(|shard| (&shard.key, shard.ack))
            .collect();
        cluster
            .commit_stream_segment_append(&bucket, &key, &session_id, 0, &segment, &batch)
            .unwrap();
        assert_eq!(
            shard_state(&cluster, &segment),
            vec![(true, true); absent.len()],
            "positive control must persist the exact shard files and acknowledgements"
        );
    }

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    assert_eq!(
        cluster
            .load_stream_upload_session(&bucket, &key, &session_id)
            .unwrap_err()
            .kind(),
        crate::StreamUploadFailureKind::SessionNotFound,
    );
    assert_eq!(
        shard_state(&cluster, &segment),
        absent,
        "abort must finish cleanup first"
    );
    admission.require_valid_now_raw().unwrap();

    // In the published-append case this is a delayed duplicate of a known write,
    // not the separate crash window before a segment manifest is recorded.
    let late = write();
    let after = shard_state(&cluster, &segment);
    assert_eq!(
        after, absent,
        "completed stream cleanup must prevent late physical publication; write={late:?}; \
         shard observations are (durable acknowledgement, placed file)"
    );
    assert!(
        late.is_err(),
        "an aborted session must reject its delayed write"
    );
}

#[test]
#[ignore = "known late-write cleanup gap; see plans/durable-generation-cleanup-ownership.md"]
fn streamed_put_abort_fences_prepared_write() {
    assert_abort_fences_delayed_stream_write(Target::PutObject, PauseAfter::Preparation);
}

#[test]
#[ignore = "known late-write cleanup gap; see plans/durable-generation-cleanup-ownership.md"]
fn upload_part_abort_fences_prepared_write() {
    assert_abort_fences_delayed_stream_write(Target::UploadPart, PauseAfter::Preparation);
}

#[test]
#[ignore = "known late-write cleanup gap; see plans/durable-generation-cleanup-ownership.md"]
fn streamed_put_abort_fences_duplicate_of_published_append() {
    assert_abort_fences_delayed_stream_write(Target::PutObject, PauseAfter::PublishedAppend);
}

#[test]
#[ignore = "known late-write cleanup gap; see plans/durable-generation-cleanup-ownership.md"]
fn upload_part_abort_fences_duplicate_of_published_append() {
    assert_abort_fences_delayed_stream_write(Target::UploadPart, PauseAfter::PublishedAppend);
}
