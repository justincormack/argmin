// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn invalid_extents() -> [(u32, u64); 4] {
    [
        (0, crate::INTERNAL_SEGMENT_SIZE as u64 + 1),
        (0, u64::MAX),
        (crate::payload_limits::MAX_STREAM_SEGMENTS, 0),
        (u32::MAX, 1),
    ]
}

#[test]
fn unix_stream_extent_rejection_precedes_transport() {
    let client = test_unix_storage_node_client();
    let bucket = crate::tests::bucket_name("extent-bucket");
    let key = crate::tests::object_key("key");
    let session_id = crate::tests::stream_session_id("extent");
    let route = client
        .open_stream_upload_session_metadata_route(
            client.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
    let before = rpc_requests_started_for_test(&client);
    for (segment_index, size) in invalid_extents() {
        let error = route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index,
                    size,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                None,
                AdmittedRouteEffectFence::unbounded(client.cluster_epoch),
            )
            .unwrap_err();
        assert!(
            matches!(error, ObjectPgActionError::InvalidRequest { reason }
            if reason.contains("single-write index or logical-size limit"))
        );
        assert_eq!(rpc_requests_started_for_test(&client), before);
    }
}

fn preparation_rejects_invalid_extents(remote: bool) {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    let bucket = crate::tests::bucket_name("extent-bucket");
    let key = crate::tests::object_key("key");
    let upload_id = crate::tests::multipart_upload_id("extent");
    let targets = [
        StreamUploadTarget::PutObject,
        StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
    ];
    let mut sessions = Vec::new();
    {
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::create_multipart_upload(
            &*pg,
            &CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: OwnerIdentity::from_principal("owner"),
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: metadata_only_test_encryption(),
            },
        )
        .unwrap();
        for (index, target) in targets.iter().enumerate() {
            let session_id = crate::tests::stream_session_id(format!("extent-{index}"));
            let generation = pg
                .reserve_object_generation(&bucket, &key, &session_id)
                .unwrap();
            // Metadata-only preparation fixture: no ciphertext is supplied or
            // persisted. The positive control tests allocation, not payload IO.
            pg.create_stream_upload(&CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: target.clone(),
                encryption: metadata_only_test_encryption(),
            })
            .unwrap();
            let session = pg.get_stream_upload(&session_id).unwrap();
            let expected = match target {
                StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
                    session,
                    generation_id: generation,
                },
                StreamUploadTarget::UploadPart { .. } => {
                    crate::ExpectedStreamAppendState::UploadPart {
                        session,
                        upload: Box::new(pg.get_multipart_upload(&upload_id).unwrap()),
                        part_generation_id: generation,
                    }
                }
            };
            sessions.push((session_id, expected));
        }
    }
    let _server = spawn_test_storage_node_server(server);
    let client =
        UnixStorageNodeClient::new(config.node_id, config.cluster_epoch, config.socket_path);
    let local = LocalStorageNodeClient::new(config.node_id, node.clone());
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let fence = AdmittedRouteEffectFence::unbounded(config.cluster_epoch);
    for (session_id, expected) in &sessions {
        let route = local
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                session_id,
            )
            .unwrap();
        let observe = || {
            let pg = node.get_pg(0).unwrap();
            let rows = sessions
                .iter()
                .map(|(id, _)| {
                    (
                        pg.get_stream_upload(id).unwrap(),
                        pg.list_stream_segments(id).unwrap(),
                        pg.get_object_generation_reservation(&bucket, &key, id)
                            .unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            (
                rows,
                pg.metadata_command_replica_state().unwrap(),
                pg.max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap(),
            )
        };
        let before = observe();
        for (segment_index, size) in invalid_extents() {
            let request = PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index,
                size,
                segment_crc64: 0,
                payload_crc64: 0,
            };
            if remote {
                // Bypass the sender's validation to pin installed-server
                // rejection rather than just the no-transport guard above.
                let request = StorageRpcStreamSegmentAppendPrepareRequest {
                    object: client.object_request(pg_id.pg_id(), &bucket, &key),
                    request,
                    expected_append_state: Some(expected.clone()),
                    effect_deadline: None,
                };
                let error = client
                    .rpc_request(
                        StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                        encode_stream_segment_append_prepare_request(&request),
                    )
                    .unwrap_err();
                assert!(
                    matches!(
                        error,
                        StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::PayloadDecode,
                            ..
                        }
                    ),
                    "{error:?}"
                );
            } else {
                let error = route
                    .prepare_segment_append_with_expected_append_state(
                        &request,
                        Some(expected),
                        fence,
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, ObjectPgActionError::InvalidRequest { reason }
                    if reason.contains("single-write index or logical-size limit"))
                );
            }
            assert_eq!(
                observe(),
                before,
                "rejected {segment_index}/{size} must preserve both sessions and the log"
            );
        }
        let request = PrepareStreamUploadSegmentAppendReq {
            session_id: session_id.clone(),
            segment_index: crate::payload_limits::MAX_STREAM_SEGMENTS - 1,
            size: crate::INTERNAL_SEGMENT_SIZE as u64,
            segment_crc64: 0,
            payload_crc64: 0,
        };
        let remote_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                session_id,
            )
            .unwrap();
        let positive = if remote {
            remote_route.as_ref()
        } else {
            route.as_ref()
        };
        let (_, segment) = positive
            .prepare_segment_append_with_expected_append_state(&request, Some(expected), fence)
            .unwrap();
        assert_eq!(segment.size, crate::INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(segment.segment_index, request.segment_index);
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .get_stream_upload(session_id)
                .unwrap()
                .next_segment_vid
                .get(),
            segment.segment_vid.get() + 1
        );
    }
}

#[test]
fn embedded_stream_extent_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_invalid_extents(false);
}

#[test]
fn installed_unix_stream_extent_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_invalid_extents(true);
}
