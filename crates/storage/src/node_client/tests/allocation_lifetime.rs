// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND;
use crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND;

#[path = "allocation_lifetime_publication.rs"]
mod publication;

#[test]
fn embedded_retired_allocation_lifetime_rejects_fresh_work_but_allows_convergence() {
    allocation_lifetime_matrix(false);
}

#[test]
fn installed_unix_retired_allocation_lifetime_rejects_fresh_work_but_allows_convergence() {
    allocation_lifetime_matrix(true);
}

fn assert_lifetime_unavailable(error: ObjectPgActionError, remote: bool) {
    assert!(
        if remote {
            matches!(&error, ObjectPgActionError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::InactivePgRoute, detail, ..
            }) if detail.as_str() == "PG 0 has no live allocation lifetime")
        } else {
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::PgAllocationLifetimeUnavailable {
                    pg_id: 0
                })
            )
        },
        "remote={remote}: {error:?}"
    );
}

fn allocation_lifetime_matrix(remote: bool) {
    for part in [false, true] {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.default_ec_shape = EcShape { k: 1, m: 0 };
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let node = server.test_storage_node();
        let bucket = crate::tests::bucket_name("allocation-lifetime");
        let key = crate::tests::object_key("key");
        let upload_id = crate::tests::multipart_upload_id("lifetime");
        // Metadata-only builder/preparation fixture: no payload is written.
        let upload = {
            let pg = node.get_pg(0).unwrap();
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
            pg.refresh_metadata_command_state_digest().unwrap();
            pg.get_multipart_upload(&upload_id).unwrap()
        };
        let request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("stream"),
            bucket: bucket.clone(),
            key: key.clone(),
            target: if part {
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number: 1,
                }
            } else {
                StreamUploadTarget::PutObject
            },
            encryption: upload.encryption.clone(),
        };
        let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        proof.operation_kind = if part {
            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        } else {
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        }
        .to_string();
        let _server = spawn_test_storage_node_server(server);
        let unix = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let local = LocalStorageNodeClient::new(config.node_id, node.clone());
        let client: &dyn ObjectMutationMetadataNodeClient = if remote { &unix } else { &local };
        let generation_client: &dyn ObjectGenerationMetadataNodeClient =
            if remote { &unix } else { &local };
        let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
        let route = client
            .open_stream_upload_creation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
            .unwrap();
        let generation_route = generation_client
            .open_object_generation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
            .unwrap();
        let placement = PayloadPlacementInput::from_server_config(&config);
        let build = || {
            route.build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                effect_fence: AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
                placement,
                request: &request,
                cleanup_after: Some(10_000),
                precondition: if part {
                    CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    }
                } else {
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                },
                bucket_write_reservation: &proof,
            })
        };
        let candidate = generation_route.next_object_generation_id().unwrap();
        let direct_id = crate::tests::stream_session_id("direct");
        let mut direct_proof = proof.clone();
        direct_proof.operation_kind = PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into();
        let build_direct = || {
            generation_route.build_reserve_put_object_generation_command(
                &direct_id,
                candidate,
                &direct_proof,
                placement,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        };
        let observe = || {
            let pg = node.get_pg(0).unwrap();
            (
                pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap(),
                PgMetadataStore::next_generation_id(&*pg, &bucket, &key).unwrap(),
                pg.list_all_stream_uploads().unwrap(),
                pg.list_stream_segments(&request.session_id).unwrap(),
            )
        };
        let before = observe();
        build_direct().unwrap();
        let command = build().unwrap();
        assert_eq!(observe(), before, "active building is read-only");
        let original = {
            let pg = node.get_pg(0).unwrap();
            let original = pg.test_allocation_lifetime_identity().unwrap();
            // Model failed publication/import: its replacement guard is dropped
            // without completing, leaving unchanged durable rows and old route.
            drop(pg.begin_allocation_lifetime_replacement().unwrap());
            assert_eq!(pg.test_allocation_lifetime_identity(), None);
            original
        };
        assert_lifetime_unavailable(build_direct().unwrap_err(), remote);
        assert_lifetime_unavailable(build().unwrap_err(), remote);
        assert_eq!(observe(), before, "retirement must reject before mutation");
        assert_eq!(
            generation_route.next_object_generation_id().unwrap(),
            candidate
        );
        // An already-issued command must still converge. Replica apply cannot
        // depend on the local primary's volatile allocation lifetime.
        let session = {
            let pg = node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(config.node_id.as_u32(), &command)
                .unwrap();
            pg.apply_metadata_command_and_record(config.node_id.as_u32(), &command)
                .unwrap();
            pg.get_stream_upload(&request.session_id).unwrap()
        };
        let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() else {
            panic!("expected stream creation")
        };
        assert!(route
            .matching_stream_upload_exists(&request, Some(create))
            .unwrap());
        for cross_node in [false, true] {
            let mut crossed = create.clone();
            if cross_node {
                crossed.allocation_issuer.node_id = NodeId::new(config.node_id.as_u32() + 1);
            } else {
                crossed.allocation_issuer.lifetime[0] ^= 1;
            }
            let before = observe();
            let error = route
                .matching_stream_upload_exists(&request, Some(&crossed))
                .unwrap_err();
            assert!(
                if remote {
                    matches!(
                        &error,
                        ObjectPgActionError::Store(StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::Internal,
                            ..
                        })
                    )
                } else {
                    matches!(&error, ObjectPgActionError::Metadata(MetadataError::InvariantViolation {
                    context: "create stream upload generation mismatch", reason,
                }) if reason.contains("allocation issuer"))
                },
                "{error:?}"
            );
            assert_eq!(observe(), before);
        }
        assert_eq!(
            generation_route
                .object_generation_reservation(&request.session_id)
                .unwrap(),
            candidate
        );
        let expected = if part {
            crate::ExpectedStreamAppendState::UploadPart {
                session: session.clone(),
                upload: Box::new(upload.clone()),
                part_generation_id: candidate,
            }
        } else {
            crate::ExpectedStreamAppendState::PutObject {
                session: session.clone(),
                generation_id: candidate,
            }
        };
        let append_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &request.session_id,
            )
            .unwrap();
        let append = || {
            append_route.prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: request.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                placement,
            )
        };
        let applied = observe();
        assert_eq!(append_route.load_session().unwrap(), session);
        assert_lifetime_unavailable(append().unwrap_err(), remote);
        assert_eq!(
            observe(),
            applied,
            "retired preparation advanced the allocator"
        );
        {
            let pg = node.get_pg(0).unwrap();
            pg.begin_allocation_lifetime_replacement()
                .unwrap()
                .complete()
                .unwrap();
            assert_ne!(pg.test_allocation_lifetime_identity().unwrap(), original);
        }
        // A new live lifetime must not adopt the previous issuer's allocator,
        // including when a Unix frame reconstructs the local route afresh.
        assert_lifetime_unavailable(append().unwrap_err(), remote);
        assert_eq!(observe(), applied, "replacement adopted an old stream");
        let reopened_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &request.session_id,
            )
            .unwrap();
        assert_lifetime_unavailable(
            reopened_route
                .prepare_segment_append_with_expected_append_state(
                    &PrepareStreamUploadSegmentAppendReq {
                        session_id: request.session_id.clone(),
                        segment_index: 0,
                        size: 1,
                        segment_crc64: 0,
                        payload_crc64: 0,
                    },
                    Some(&expected),
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                    placement,
                )
                .unwrap_err(),
            remote,
        );
        assert_eq!(observe(), applied);

        let mut fresh_request = request.clone();
        fresh_request.session_id = crate::tests::stream_session_id("fresh-stream");
        let mut fresh_proof = proof.clone();
        fresh_proof.reservation_id = "fresh-stream-proof".into();
        let fresh_command = route
            .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                placement,
                request: &fresh_request,
                cleanup_after: Some(10_000),
                precondition: if part {
                    CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    }
                } else {
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                },
                bucket_write_reservation: &fresh_proof,
            })
            .unwrap();
        let (fresh_session, fresh_generation) = {
            let pg = node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(config.node_id.as_u32(), &fresh_command)
                .unwrap();
            (
                pg.get_stream_upload(&fresh_request.session_id).unwrap(),
                pg.get_object_generation_reservation(&bucket, &key, &fresh_request.session_id)
                    .unwrap(),
            )
        };
        let fresh_expected = if part {
            crate::ExpectedStreamAppendState::UploadPart {
                session: fresh_session,
                upload: Box::new(upload.clone()),
                part_generation_id: fresh_generation,
            }
        } else {
            crate::ExpectedStreamAppendState::PutObject {
                session: fresh_session,
                generation_id: fresh_generation,
            }
        };
        let fresh_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &fresh_request.session_id,
            )
            .unwrap();
        let (_, segment) = fresh_route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: fresh_request.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                Some(&fresh_expected),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                placement,
            )
            .unwrap();
        assert_eq!(segment.segment_vid, GenerationId::MIN);
        assert_eq!(
            fresh_route.load_session().unwrap().next_segment_vid.get(),
            2
        );
        assert!(fresh_route.load_segments().unwrap().is_empty());
    }
}
