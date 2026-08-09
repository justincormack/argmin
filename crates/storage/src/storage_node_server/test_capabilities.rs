// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

    #[test]
    fn active_object_routes_keep_their_captured_deadline_after_validity_extension() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("active-object-route-bucket");
        let key = crate::tests::object_key("active-object-route-key");
        let reservation_id = crate::tests::stream_session_id("active-object");
        let upload_id = crate::tests::multipart_upload_id("active-object-route-upload");
        let reserved_generation = crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            let generation =
                PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                    .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            generation
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcObjectRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "active-object-route-proof".to_string(),
            owner_token: "active-object-route-owner".to_string(),
            cluster_epoch: config.cluster_epoch,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.to_string(),
            created_at: 1_000,
            lease_deadline: 4_000,
            target_context: Some(key.as_str().to_string()),
        };
        let mut metadata_proof = proof.clone();
        metadata_proof.operation_kind = "put-object-metadata".to_string();
        let mut create_multipart_proof = proof.clone();
        create_multipart_proof.operation_kind =
            CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut complete_multipart_proof = proof.clone();
        complete_multipart_proof.operation_kind =
            COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut abort_multipart_proof = proof.clone();
        abort_multipart_proof.operation_kind =
            ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut stream_proof = proof.clone();
        stream_proof.operation_kind =
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut renewed_stream_proof = stream_proof.clone();
        renewed_stream_proof.lease_deadline = 4_500;
        let mut delete_current_proof = proof.clone();
        delete_current_proof.operation_kind = "delete-current-object".to_string();
        let mut delete_specific_proof = proof.clone();
        delete_specific_proof.operation_kind = "delete-object-version".to_string();
        let mut marker_proof = proof.clone();
        marker_proof.operation_kind = "insert-delete-marker".to_string();
        let direct_put_request = crate::CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_reservation_id: reservation_id.clone(),
            versioning: BucketVersioningState::Suspended,
            owner: crate::OwnerIdentity::from_principal("active-object-route-owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: reserved_generation,
            size: 12,
            etag_crc64: 99,
            ec: EcShape { k: 4, m: 2 },
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            segment_index: 0,
            segment_crc64: 99,
            segment_okh: [7; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 0,
            bucket_write_reservation: proof.clone(),
        };
        let serialized_tags =
            "<Tagging><TagSet><Tag><Key>route</Key><Value>active</Value></Tag></TagSet></Tagging>";
        let existing_multipart_request = CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: crate::OwnerIdentity::from_principal("active-object-route-owner"),
            owner: crate::OwnerIdentity::from_principal("active-object-route-owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        let terminal_multipart_part = crate::MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 2,
            generation: 1,
            size: 8,
            payload_crc64: 55,
            etag: vec![5; 8],
            etag_kind: crate::EtagKind::Crc64,
            part_vid: GenerationId::new(20).unwrap(),
            placement_cluster_epoch: config.cluster_epoch,
            ec_k: 4,
            ec_m: 2,
            last_modified: 1_000,
            checksum: None,
        };
        let stream_create_request = CreateStreamUploadReq {
            session_id: reservation_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };
        let expected_stream_command =
            CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                stream_create_request.clone(),
                1_000,
                Some(4_000),
                stream_proof.clone(),
            );
        let new_stream_create_request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("active-new"),
            ..stream_create_request.clone()
        };
        let upload_part_stream_session_id = crate::tests::stream_session_id("active-part");
        let mut upload_part_stream_proof = proof.clone();
        upload_part_stream_proof.operation_kind =
            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut upload_part_finalize_proof = proof.clone();
        upload_part_finalize_proof.operation_kind =
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let upload_part_stream_create_request = CreateStreamUploadReq {
            session_id: upload_part_stream_session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 1,
            },
            encryption: crate::ObjectEncryption::None,
        };
        let expected_upload_part_stream_command =
            CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                upload_part_stream_create_request.clone(),
                1_000,
                upload_part_stream_proof.clone(),
            );
        let new_upload_part_stream_create_request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("active-part-new"),
            ..upload_part_stream_create_request.clone()
        };
        crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            PgMetadataStore::put_object_with_segments(
                &*pg,
                &crate::PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("active-object-route-owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::new(9).unwrap(),
                    size: 0,
                    etag: crate::ObjectEtag::single_part(99),
                    ec: EcShape { k: 4, m: 2 },
                    layout: crate::ObjectLayout::Standard,
                    tags: Some(crate::SerializedTagSet::new(serialized_tags.to_string())),
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                },
                &[],
            )
            .unwrap();
            PgMetadataStore::create_multipart_upload(&*pg, &existing_multipart_request).unwrap();
            PgMetadataStore::upsert_multipart_part(&*pg, &terminal_multipart_part).unwrap();
            pg.apply_metadata_command(&MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    config.cluster_epoch,
                    PgId::new(0),
                    MetadataCommandLogIndex::new(1).unwrap(),
                ),
                MetadataCommandPayload::CreateStreamUpload(Box::new(
                    expected_stream_command.clone(),
                )),
            ))
            .unwrap();
            pg.apply_metadata_command(&MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    config.cluster_epoch,
                    PgId::new(0),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                MetadataCommandPayload::CreateStreamUpload(Box::new(
                    expected_upload_part_stream_command.clone(),
                )),
            ))
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        });

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        match handler.active_object_route(&foreign_permit, &request, "test object route") {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign admission permit created an active object route"),
        }
        let cleanup_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.active_object_route(&cleanup_permit, &request, "test object route") {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires active route admission"));
            }
            Ok(_) => panic!("retained-cleanup permit created an active object route"),
        }
        drop(cleanup_permit);

        let primary_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_primary_object_route(&active_permit, &request, "test primary object route")
                .unwrap()
        });
        let read_route = crate::clock::with_time_override(1_000, || {
            handler
                .metadata_read_object_route(&active_permit, &request, "test object read route")
                .unwrap()
        });
        let acting_set_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_object_route(&active_permit, &request, "test acting-set object route")
                .unwrap()
        });
        let direct_put_snapshot = crate::clock::with_time_override(1_000, || {
            assert_eq!(
                primary_route
                    .generation_reservation(&reservation_id)
                    .unwrap(),
                reserved_generation
            );
            assert_eq!(
                primary_route.next_generation_id().unwrap(),
                GenerationId::new(11).unwrap()
            );
            assert_eq!(
                acting_set_route.next_version_id().unwrap(),
                VersionId::from_u64(1)
            );
            let snapshot = primary_route
                .load_direct_put_commit_snapshot(&reservation_id, reserved_generation)
                .unwrap();
            assert_eq!(
                snapshot.auth_snapshot.existing_etag.as_deref(),
                Some("\"0000000000000063\"")
            );
            assert!(snapshot.current.is_some());
            let command = primary_route
                .build_direct_put_commit_command(&direct_put_request, VersionId::Null, &snapshot)
                .unwrap();
            let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
                panic!("active object route must build a direct PUT command");
            };
            assert!(commit.matches_request(&bucket, &key, &reservation_id, reserved_generation));
            snapshot
        });

        let mut mismatched_direct_put_request = direct_put_request.clone();
        mismatched_direct_put_request.key = crate::tests::object_key("different-request-key");
        let capability_mismatch = crate::clock::with_time_override(1_000, || {
            primary_route.build_direct_put_commit_command(
                &mismatched_direct_put_request,
                VersionId::Null,
                &direct_put_snapshot,
            )
        });
        match capability_mismatch {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("does not match active object route"));
            }
            other => panic!("mismatched request must fail at the route capability: {other:?}"),
        }

        let mut mismatched_proof_request = direct_put_request.clone();
        mismatched_proof_request.bucket_write_reservation.bucket =
            crate::tests::bucket_name("different-proof-bucket");
        let proof_mismatch = crate::clock::with_time_override(1_000, || {
            primary_route.build_direct_put_commit_command(
                &mismatched_proof_request,
                VersionId::Null,
                &direct_put_snapshot,
            )
        });
        match proof_mismatch {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("does not match active object route"));
            }
            other => panic!("mismatched proof must fail at the route capability: {other:?}"),
        }

        let mut mismatched_operation_proof = proof.clone();
        mismatched_operation_proof.operation_kind =
            PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mut mismatched_target_proof = proof.clone();
        mismatched_target_proof.target_context = Some("different-proof-key".to_string());
        let mut mismatched_epoch_proof = proof.clone();
        mismatched_epoch_proof.cluster_epoch = ClusterEpoch::new(2).unwrap();
        for (case, mismatched_proof) in [
            ("operation", mismatched_operation_proof),
            ("target", mismatched_target_proof),
            ("epoch", mismatched_epoch_proof),
        ] {
            let mut mismatched_proof_request = direct_put_request.clone();
            mismatched_proof_request.bucket_write_reservation = mismatched_proof;
            let result = crate::clock::with_time_override(1_000, || {
                primary_route.build_direct_put_commit_command(
                    &mismatched_proof_request,
                    VersionId::Null,
                    &direct_put_snapshot,
                )
            });
            match result {
                Err(StorageNodeObjectRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                    assert!(error.message.contains(
                        "bucket write reservation proof does not match the active object route"
                    ));
                }
                other => panic!(
                    "mismatched direct PUT proof {case} must fail at the route capability: {other:?}"
                ),
            }
        }

        let mut mismatched_object = request.clone();
        mismatched_object.key = crate::tests::object_key("different-routed-key");
        let mismatch_response = crate::clock::with_time_override(1_000, || {
            handler
                .direct_put_commit_command_build_response(
                    &active_permit,
                    StorageRpcDirectPutCommandBuildRequest {
                        object: mismatched_object,
                        request: direct_put_request.clone(),
                        version_id: VersionId::Null,
                        expected_snapshot: direct_put_snapshot.clone(),
                        bucket_write_reservation: proof.clone(),
                    },
                )
                .unwrap()
        });
        let mismatch_error = decode_storage_rpc_response_payload(&mismatch_response)
            .unwrap()
            .unwrap_err();
        assert_eq!(mismatch_error.code, StorageRpcErrorCode::PayloadDecode);
        assert!(mismatch_error.message.contains("key does not match"));

        let read_subject = crate::clock::with_time_override(1_000, || {
            let subject = read_route.load_object_read_auth_subject(None).unwrap();
            let snapshot = read_route
                .load_object_read_snapshot_for_subject(
                    None,
                    &subject.identity,
                    crate::ObjectReadSnapshotMode::StandardSegments,
                )
                .unwrap();
            assert_eq!(snapshot.stored, subject.stored);
            assert!(snapshot.object_segments.is_empty());
            subject
        });

        let stream_append_request = PrepareStreamUploadSegmentAppendReq {
            session_id: reservation_id.clone(),
            segment_index: 0,
            size: 12,
            segment_crc64: 99,
            payload_crc64: 99,
            segment_okh: [7; 16],
        };
        crate::clock::with_time_override(1_000, || {
            assert!(primary_route
                .matching_stream_upload_exists(
                    &stream_create_request,
                    Some(&expected_stream_command),
                )
                .unwrap());
            assert!(primary_route
                .matching_stream_upload_exists(
                    &upload_part_stream_create_request,
                    Some(&expected_upload_part_stream_command),
                )
                .unwrap());
            let session = primary_route
                .load_stream_upload_session(&reservation_id)
                .unwrap();
            assert_eq!(session.session_id, reservation_id);
            assert_eq!(
                session.bucket_write_reservation.as_ref(),
                Some(&stream_proof)
            );
            assert!(primary_route
                .load_stream_upload_segments(&reservation_id)
                .unwrap()
                .is_empty());
            let (target, segment) = primary_route
                .prepare_stream_segment_append(
                    &stream_append_request,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap();
            assert_eq!(target, StreamUploadTarget::PutObject);
            assert_eq!(segment.session_id, reservation_id);
            assert_eq!(segment.placement_cluster_epoch, config.cluster_epoch);
            primary_route
                .update_stream_upload_bucket_write_reservation(
                    &reservation_id,
                    &stream_proof,
                    &renewed_stream_proof,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap();
            assert_eq!(
                primary_route
                    .load_stream_upload_session(&reservation_id)
                    .unwrap()
                    .bucket_write_reservation
                    .as_ref(),
                Some(&renewed_stream_proof)
            );
        });

        let later_stream_proof = BucketWriteReservationProof {
            lease_deadline: renewed_stream_proof.lease_deadline.saturating_add(1_000),
            ..renewed_stream_proof.clone()
        };
        let expired_stream_update = crate::clock::with_time_override(3_500, || {
            primary_route.update_stream_upload_bucket_write_reservation(
                &reservation_id,
                &renewed_stream_proof,
                &later_stream_proof,
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 3_000),
            )
        });
        assert!(matches!(
            expired_stream_update,
            Err(StorageNodeObjectRouteError::Object(
                ObjectPgActionError::Store(StoreError::RouteMapExpired { .. })
            ))
        ));
        crate::clock::with_time_override(1_000, || {
            assert_eq!(
                primary_route
                    .load_stream_upload_session(&reservation_id)
                    .unwrap()
                    .bucket_write_reservation
                    .as_ref(),
                Some(&renewed_stream_proof),
                "expired request effect fence must reject before stream proof mutation"
            );
        });

        let mut mismatched_stream_request = stream_create_request.clone();
        mismatched_stream_request.key = crate::tests::object_key("different-stream-request-key");
        let mismatched_stream_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_stream_upload_exists(&mismatched_stream_request, None)
        });
        match mismatched_stream_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("subject does not match"));
            }
            other => panic!("mismatched stream create request must fail: {other:?}"),
        }
        let mut mismatched_stream_command = expected_stream_command.clone();
        mismatched_stream_command.session.key =
            crate::tests::object_key("different-stream-command-key");
        let mismatched_stream_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_stream_upload_exists(
                &stream_create_request,
                Some(&mismatched_stream_command),
            )
        });
        match mismatched_stream_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error
                    .message
                    .contains("expected command subject does not match"));
            }
            other => panic!("mismatched stream command subject must fail: {other:?}"),
        }
        let mut mismatched_stream_target_command = expected_stream_command.clone();
        mismatched_stream_target_command.session.target = StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        };
        let mismatched_stream_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_stream_upload_exists(
                &stream_create_request,
                Some(&mismatched_stream_target_command),
            )
        });
        match mismatched_stream_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error
                    .message
                    .contains("expected command subject does not match"));
            }
            other => panic!("mismatched stream command target must fail: {other:?}"),
        }
        let mut mismatched_stream_proof_command = expected_stream_command.clone();
        mismatched_stream_proof_command
            .bucket_write_reservation
            .operation_kind = UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mismatched_stream_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_stream_upload_exists(
                &stream_create_request,
                Some(&mismatched_stream_proof_command),
            )
        });
        match mismatched_stream_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("mismatched stream command proof must fail: {other:?}"),
        }
        let mut mismatched_upload_part_stream_proof_command =
            expected_upload_part_stream_command.clone();
        mismatched_upload_part_stream_proof_command
            .bucket_write_reservation
            .operation_kind = PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mismatched_stream_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_stream_upload_exists(
                &upload_part_stream_create_request,
                Some(&mismatched_upload_part_stream_proof_command),
            )
        });
        match mismatched_stream_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("mismatched UploadPart stream command proof must fail: {other:?}"),
        }
        let mut mismatched_renewed_stream_proof = renewed_stream_proof.clone();
        mismatched_renewed_stream_proof.owner_token = "different-owner".to_string();
        let mismatched_stream_update = crate::clock::with_time_override(1_000, || {
            primary_route.update_stream_upload_bucket_write_reservation(
                &reservation_id,
                &renewed_stream_proof,
                &mismatched_renewed_stream_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match mismatched_stream_update {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("same stable identity"));
            }
            other => panic!("mismatched stream reservation renewal must fail: {other:?}"),
        }

        let multipart_upload = crate::clock::with_time_override(1_000, || {
            let upload = primary_route.load_multipart_upload(&upload_id).unwrap();
            assert_eq!(
                primary_route
                    .load_in_progress_multipart_upload(&upload_id)
                    .unwrap(),
                upload
            );
            assert_eq!(
                primary_route
                    .load_in_progress_multipart_upload_for_listing(&upload_id)
                    .unwrap(),
                upload
            );
            upload
        });
        assert_eq!(multipart_upload.upload_id, upload_id);

        let put_finalize_snapshot = crate::clock::with_time_override(1_000, || {
            primary_route
                .load_stream_put_finalize_snapshot(&reservation_id)
                .unwrap()
        });
        let put_commit = crate::StreamPutCommitInput {
            versioning: BucketVersioningState::Suspended,
            version_id: VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("active-object-route-owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            etag_crc64: 0,
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
        };
        let put_finalize_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_stream_put_commit_command(
                    &reservation_id,
                    0,
                    &put_finalize_snapshot,
                    &put_commit,
                    &renewed_stream_proof,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap()
        });
        let MetadataCommandPayload::CommitDirectPutObject(put_finalize) =
            put_finalize_command.payload()
        else {
            panic!("active object route must build a stream PUT commit command");
        };
        assert!(put_finalize.matches_stream_session(&bucket, &key, &reservation_id));
        assert_eq!(put_finalize.bucket_write_reservation, renewed_stream_proof);

        let part_finalize_snapshot = crate::clock::with_time_override(1_000, || {
            primary_route
                .load_stream_part_finalize_snapshot(&upload_id, &upload_part_stream_session_id, 1)
                .unwrap()
        });
        let finalized_part = crate::MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 0,
            size: 0,
            payload_crc64: 0,
            etag: Vec::new(),
            etag_kind: crate::EtagKind::Crc64,
            part_vid: GenerationId::MIN,
            placement_cluster_epoch: config.cluster_epoch,
            ec_k: 4,
            ec_m: 2,
            last_modified: 1_000,
            checksum: None,
        };
        let part_finalize_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_stream_part_commit_command(
                    &upload_id,
                    &upload_part_stream_session_id,
                    1,
                    &part_finalize_snapshot,
                    &finalized_part,
                    &[],
                    &upload_part_finalize_proof,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap()
        });
        let MetadataCommandPayload::CommitStreamPart(part_finalize) =
            part_finalize_command.payload()
        else {
            panic!("active object route must build a stream part commit command");
        };
        assert_eq!(part_finalize.bucket, bucket);
        assert_eq!(part_finalize.key, key);
        assert_eq!(part_finalize.upload.upload_id, upload_id);
        assert_eq!(part_finalize.session_id, upload_part_stream_session_id);
        assert_eq!(
            part_finalize.bucket_write_reservation,
            upload_part_finalize_proof
        );

        let mut mismatched_put_finalize_snapshot = put_finalize_snapshot.clone();
        mismatched_put_finalize_snapshot.session.key =
            crate::tests::object_key("different-put-finalize-snapshot-key");
        let mismatched_put_finalize = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_put_commit_command(
                &reservation_id,
                0,
                &mismatched_put_finalize_snapshot,
                &put_commit,
                &renewed_stream_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match mismatched_put_finalize {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("snapshot subject does not match"));
            }
            other => panic!("mismatched stream PUT snapshot must fail: {other:?}"),
        }
        let crossed_put_finalize_proof = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_put_commit_command(
                &reservation_id,
                0,
                &put_finalize_snapshot,
                &put_commit,
                &upload_part_finalize_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match crossed_put_finalize_proof {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("crossed stream PUT finalize proof must fail: {other:?}"),
        }
        let mut substituted_put_finalize_proof = renewed_stream_proof.clone();
        substituted_put_finalize_proof.reservation_id =
            "different-active-object-route-proof".to_string();
        let substituted_put_finalize = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_put_commit_command(
                &reservation_id,
                0,
                &put_finalize_snapshot,
                &put_commit,
                &substituted_put_finalize_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match substituted_put_finalize {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("durable stream session"));
            }
            other => panic!("substituted stream PUT finalize proof must fail: {other:?}"),
        }
        let local_client = LocalStorageNodeClient::new(config.node_id, Arc::clone(&server._node));
        let substituted_local_put_finalize = crate::clock::with_time_override(1_000, || {
            ObjectMutationMetadataNodeClient::open_stream_put_finalization_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &reservation_id,
            )
            .and_then(|route| {
                route.build_commit_command(
                    BuildStreamPutCommitCommandReq {
                        total_size: 0,
                        expected_snapshot: &put_finalize_snapshot,
                        commit: &put_commit,
                        bucket_write_reservation: &substituted_put_finalize_proof,
                    },
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
            })
        });
        assert!(matches!(
            substituted_local_put_finalize,
            Err(ObjectPgActionError::InvalidRequest { reason })
                if reason.contains("durable stream session")
        ));

        let mut mismatched_part_finalize_snapshot = part_finalize_snapshot.clone();
        mismatched_part_finalize_snapshot.auth_snapshot.upload.key =
            crate::tests::object_key("different-part-finalize-snapshot-key");
        let mismatched_part_finalize = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_part_commit_command(
                &upload_id,
                &upload_part_stream_session_id,
                1,
                &mismatched_part_finalize_snapshot,
                &finalized_part,
                &[],
                &upload_part_finalize_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match mismatched_part_finalize {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("snapshot subject does not match"));
            }
            other => panic!("mismatched stream part snapshot must fail: {other:?}"),
        }
        let mut mismatched_finalized_part = finalized_part.clone();
        mismatched_finalized_part.upload_id =
            crate::tests::multipart_upload_id("different-finalized-part-upload");
        let mismatched_part_payload = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_part_commit_command(
                &upload_id,
                &upload_part_stream_session_id,
                1,
                &part_finalize_snapshot,
                &mismatched_finalized_part,
                &[],
                &upload_part_finalize_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match mismatched_part_payload {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("payload does not match"));
            }
            other => panic!("mismatched stream part payload must fail: {other:?}"),
        }
        let command_log_index_before_local_part_mismatch = server
            ._node
            .get_pg(primary_route.route.pg_id.get())
            .unwrap()
            .max_metadata_command_log_index(config.cluster_epoch)
            .unwrap();
        let mismatched_local_part_payload = crate::clock::with_time_override(1_000, || {
            ObjectMutationMetadataNodeClient::open_stream_part_finalization_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &upload_id,
                &upload_part_stream_session_id,
                1,
            )
            .and_then(|route| {
                route.build_commit_command(
                    BuildStreamPartCommitCommandReq {
                        expected_snapshot: &part_finalize_snapshot,
                        part: &mismatched_finalized_part,
                        segments: &[],
                        bucket_write_reservation: &upload_part_finalize_proof,
                    },
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
            })
        });
        assert!(matches!(
            mismatched_local_part_payload,
            Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "build stream part commit command",
                }
            ))
        ));
        assert_eq!(
            server
                ._node
                .get_pg(primary_route.route.pg_id.get())
                .unwrap()
                .max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            command_log_index_before_local_part_mismatch,
            "embedded route must reject a crossed part before allocating a command ID"
        );
        let crossed_part_finalize_proof = crate::clock::with_time_override(1_000, || {
            primary_route.build_stream_part_commit_command(
                &upload_id,
                &upload_part_stream_session_id,
                1,
                &part_finalize_snapshot,
                &finalized_part,
                &[],
                &renewed_stream_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match crossed_part_finalize_proof {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("crossed stream part finalize proof must fail: {other:?}"),
        }

        let expired_abort_cleanup = crate::clock::with_time_override(1_000, || {
            primary_route
                .load_abort_multipart_upload_cleanup(&upload_id)
                .unwrap()
                .expect("active upload must have abort cleanup")
        });
        let expired_authorized_upload =
            crate::types::AuthorizedMultipartUploadAbort::assume_authorized(
                multipart_upload.clone(),
            );
        let command_log_index_before_expired_builds = server
            ._node
            .get_pg(primary_route.route.pg_id.get())
            .unwrap()
            .max_metadata_command_log_index(config.cluster_epoch)
            .unwrap();
        let expired_effect_fence =
            AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000);
        let expired_local_put_finalize = crate::clock::with_time_override(4_500, || {
            ObjectMutationMetadataNodeClient::open_stream_put_finalization_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &reservation_id,
            )
            .and_then(|route| {
                route.build_commit_command(
                    BuildStreamPutCommitCommandReq {
                        total_size: 0,
                        expected_snapshot: &put_finalize_snapshot,
                        commit: &put_commit,
                        bucket_write_reservation: &renewed_stream_proof,
                    },
                    expired_effect_fence,
                )
            })
        });
        assert!(matches!(
            expired_local_put_finalize,
            Err(ObjectPgActionError::Store(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 4_500,
            })) if cluster_epoch == config.cluster_epoch
        ));
        let expired_local_part_finalize = crate::clock::with_time_override(4_500, || {
            ObjectMutationMetadataNodeClient::open_stream_part_finalization_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &upload_id,
                &upload_part_stream_session_id,
                1,
            )
            .and_then(|route| {
                route.build_commit_command(
                    BuildStreamPartCommitCommandReq {
                        expected_snapshot: &part_finalize_snapshot,
                        part: &finalized_part,
                        segments: &[],
                        bucket_write_reservation: &upload_part_finalize_proof,
                    },
                    expired_effect_fence,
                )
            })
        });
        assert!(matches!(
            expired_local_part_finalize,
            Err(ObjectPgActionError::Store(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 4_500,
            })) if cluster_epoch == config.cluster_epoch
        ));
        let expired_local_abort = crate::clock::with_time_override(4_500, || {
            ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &upload_id,
            )
            .and_then(|route| {
                route.build_abort_multipart_upload_command(
                    BuildAbortMultipartUploadCommandReq {
                        expected_cleanup: Some(&expired_abort_cleanup),
                        bucket_write_reservation: &abort_multipart_proof,
                    },
                    expired_effect_fence,
                )
            })
        });
        assert!(matches!(
            expired_local_abort,
            Err(ObjectPgActionError::Store(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 4_500,
            })) if cluster_epoch == config.cluster_epoch
        ));
        let expired_local_authorized_abort = crate::clock::with_time_override(4_500, || {
            ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
                &local_client,
                config.cluster_epoch,
                primary_route.route.pg_id,
                &bucket,
                &key,
                &upload_id,
            )
            .and_then(|route| {
                route.build_authorized_abort_multipart_upload_command(
                    BuildAuthorizedAbortMultipartUploadCommandReq {
                        authorized_upload: &expired_authorized_upload,
                        expected_cleanup: Some(&expired_abort_cleanup),
                        bucket_write_reservation: &abort_multipart_proof,
                    },
                    expired_effect_fence,
                )
            })
        });
        assert!(matches!(
            expired_local_authorized_abort,
            Err(ObjectPgActionError::Store(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 4_500,
            })) if cluster_epoch == config.cluster_epoch
        ));
        assert_eq!(
            server
                ._node
                .get_pg(primary_route.route.pg_id.get())
                .unwrap()
                .max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            command_log_index_before_expired_builds,
            "expired finalization and abort command builds must not allocate command IDs"
        );

        let put_stream_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_create_stream_upload_command(
                    &new_stream_create_request,
                    Some(4_000),
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                        require_generation_reservation: false,
                    },
                    &stream_proof,
                )
                .unwrap()
        });
        let MetadataCommandPayload::CreateStreamUpload(put_stream_create) =
            put_stream_command.payload()
        else {
            panic!("active object route must build a PutObject stream command");
        };
        assert_eq!(put_stream_create.session.bucket, bucket);
        assert_eq!(put_stream_create.session.key, key);
        assert_eq!(put_stream_create.cleanup_after, Some(4_000));
        assert_eq!(put_stream_create.bucket_write_reservation, stream_proof);

        let upload_part_stream_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_create_stream_upload_command(
                    &new_upload_part_stream_create_request,
                    None,
                    CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &multipart_upload,
                    },
                    &upload_part_stream_proof,
                )
                .unwrap()
        });
        let MetadataCommandPayload::CreateStreamUpload(upload_part_stream_create) =
            upload_part_stream_command.payload()
        else {
            panic!("active object route must build an UploadPart stream command");
        };
        assert_eq!(upload_part_stream_create.session.bucket, bucket);
        assert_eq!(upload_part_stream_create.session.key, key);
        assert_eq!(upload_part_stream_create.cleanup_after, None);
        assert_eq!(
            upload_part_stream_create.bucket_write_reservation,
            upload_part_stream_proof
        );

        let mut mismatched_stream_build_request = new_stream_create_request.clone();
        mismatched_stream_build_request.key =
            crate::tests::object_key("different-stream-build-key");
        let mismatched_stream_build = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_stream_upload_command(
                &mismatched_stream_build_request,
                None,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation: false,
                },
                &stream_proof,
            )
        });
        match mismatched_stream_build {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("subject does not match"));
            }
            other => panic!("mismatched stream build subject must fail: {other:?}"),
        }

        let mismatched_stream_precondition = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_stream_upload_command(
                &new_stream_create_request,
                None,
                CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &multipart_upload,
                },
                &stream_proof,
            )
        });
        match mismatched_stream_precondition {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("precondition does not match target"));
            }
            other => panic!("crossed stream build precondition must fail: {other:?}"),
        }

        let mut mismatched_stream_build_proof = stream_proof.clone();
        mismatched_stream_build_proof.operation_kind =
            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
        let mismatched_stream_build = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_stream_upload_command(
                &new_stream_create_request,
                None,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation: false,
                },
                &mismatched_stream_build_proof,
            )
        });
        match mismatched_stream_build {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("crossed stream build proof must fail: {other:?}"),
        }

        let mut mismatched_expected_upload = multipart_upload.clone();
        mismatched_expected_upload.upload_id =
            crate::tests::multipart_upload_id("different-expected-upload");
        let mismatched_upload_part_precondition = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_stream_upload_command(
                &new_upload_part_stream_create_request,
                None,
                CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &mismatched_expected_upload,
                },
                &upload_part_stream_proof,
            )
        });
        match mismatched_upload_part_precondition {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error
                    .message
                    .contains("does not match active object route or target"));
            }
            other => panic!("mismatched expected upload must fail: {other:?}"),
        }
        let expected_multipart_command =
            crate::metadata_command::CreateMultipartUploadCommand::from_parts_for_test(
                multipart_upload.clone(),
                create_multipart_proof.clone(),
            );
        assert_eq!(
            crate::clock::with_time_override(1_000, || {
                primary_route.matching_multipart_upload_initiated_at(
                    &existing_multipart_request,
                    Some(&expected_multipart_command),
                )
            })
            .unwrap(),
            Some(multipart_upload.initiated_at)
        );
        let mut crossed_multipart_request = existing_multipart_request.clone();
        crossed_multipart_request.metadata_blob = crate::SerializedMetadataBlob::new(vec![1]);
        let crossed_expected_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_multipart_upload_initiated_at(
                &crossed_multipart_request,
                Some(&expected_multipart_command),
            )
        });
        match crossed_expected_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("does not match create request"));
            }
            other => panic!("crossed multipart creation request must fail: {other:?}"),
        }
        let mut new_multipart_request = existing_multipart_request.clone();
        new_multipart_request.upload_id =
            crate::tests::multipart_upload_id("active-object-route-new-upload");
        assert_eq!(
            crate::clock::with_time_override(1_000, || {
                primary_route.matching_multipart_upload_initiated_at(&new_multipart_request, None)
            })
            .unwrap(),
            None
        );
        let authorized_upload = crate::types::AuthorizedMultipartUploadRecord::assume_authorized(
            multipart_upload.clone(),
        );
        let authorized_abort = crate::types::AuthorizedMultipartUploadAbort::assume_authorized(
            multipart_upload.clone(),
        );
        let (completion_snapshot, abort_cleanup) = crate::clock::with_time_override(1_000, || {
            let completion_snapshot = primary_route
                .load_multipart_completion_snapshot(&authorized_upload, &[2])
                .unwrap();
            assert_eq!(
                completion_snapshot.part_records,
                vec![terminal_multipart_part.clone()]
            );
            assert_eq!(
                primary_route
                    .load_multipart_completion_preflight(&authorized_upload)
                    .unwrap()
                    .existing_etag
                    .as_deref(),
                Some("\"0000000000000063\"")
            );
            let listed = read_route
                .list_multipart_parts_for_authorized_upload(&authorized_upload, None, 10)
                .unwrap();
            assert_eq!(listed.upload, multipart_upload);
            assert_eq!(listed.response.parts, vec![terminal_multipart_part.clone()]);
            assert!(matches!(
                read_route
                    .lookup_multipart_upload_management(&upload_id)
                    .unwrap(),
                crate::MultipartUploadManagementLookup::InProgress(upload)
                    if *upload == multipart_upload
            ));
            assert!(primary_route
                .load_multipart_completion_stale_payload_source()
                .unwrap()
                .is_some());
            let cleanup = primary_route
                .load_abort_multipart_upload_cleanup(&upload_id)
                .unwrap()
                .expect("in-progress upload must have abort cleanup");
            assert_eq!(cleanup.upload, multipart_upload);
            (completion_snapshot, cleanup)
        });
        let completion_snapshot = crate::AuthorizedMultipartCompletionSnapshot::new(
            completion_snapshot,
            multipart_upload.clone(),
        );
        let completion_request =
            completion_snapshot.into_commit_request(crate::CompleteMultipartCommitInput {
                completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes(
                    [0x71; 32],
                ),
                versioning: BucketVersioningState::Disabled,
                size: terminal_multipart_part.size,
                etag_crc64: [0x72; 8],
                system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
                conditional_completion: false,
            });
        assert_eq!(
            completion_request.generation_id,
            multipart_upload.object_generation_id
        );
        assert_eq!(completion_request.bucket, bucket);
        assert_eq!(completion_request.key, key);
        assert_eq!(completion_request.upload_id, upload_id);
        let complete_multipart_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_complete_multipart_object_command(
                    &completion_request,
                    VersionId::Null,
                    &complete_multipart_proof,
                )
                .unwrap()
        });
        assert!(matches!(
            complete_multipart_command.payload(),
            MetadataCommandPayload::CommitMultipartObject(command)
                if command.upload_id == upload_id
                    && command.bucket_write_reservation == complete_multipart_proof
        ));
        let abort_multipart_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_abort_multipart_upload_command(
                    &upload_id,
                    Some(&abort_cleanup),
                    &abort_multipart_proof,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap()
                .expect("active upload must produce an abort command")
        });
        assert!(matches!(
            abort_multipart_command.payload(),
            MetadataCommandPayload::AbortMultipartUpload(command)
                if command.upload_id == upload_id
                    && command.bucket_write_reservation == abort_multipart_proof
        ));
        let authorized_abort_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_authorized_abort_multipart_upload_command(
                    &authorized_abort,
                    Some(&abort_cleanup),
                    &abort_multipart_proof,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap()
                .expect("authorized active upload must produce an abort command")
        });
        assert!(matches!(
            authorized_abort_command.payload(),
            MetadataCommandPayload::AbortMultipartUpload(command)
                if command.upload_id == upload_id
                    && command.bucket_write_reservation == abort_multipart_proof
        ));

        let mut mismatched_completion_request = completion_request.clone();
        mismatched_completion_request.key =
            crate::tests::object_key("different-complete-multipart-key");
        let mismatched_completion = crate::clock::with_time_override(1_000, || {
            primary_route.build_complete_multipart_object_command(
                &mismatched_completion_request,
                VersionId::Null,
                &complete_multipart_proof,
            )
        });
        match mismatched_completion {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("request does not match"));
            }
            other => panic!("mismatched multipart completion subject must fail: {other:?}"),
        }
        let crossed_completion_proof = crate::clock::with_time_override(1_000, || {
            primary_route.build_complete_multipart_object_command(
                &completion_request,
                VersionId::Null,
                &abort_multipart_proof,
            )
        });
        match crossed_completion_proof {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("crossed multipart completion proof must fail: {other:?}"),
        }
        let mut mismatched_abort_cleanup = abort_cleanup.clone();
        mismatched_abort_cleanup.upload.key =
            crate::tests::object_key("different-abort-multipart-key");
        let mismatched_abort = crate::clock::with_time_override(1_000, || {
            primary_route.build_abort_multipart_upload_command(
                &upload_id,
                Some(&mismatched_abort_cleanup),
                &abort_multipart_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match mismatched_abort {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("cleanup does not match"));
            }
            other => panic!("mismatched multipart abort cleanup must fail: {other:?}"),
        }
        let crossed_abort_proof = crate::clock::with_time_override(1_000, || {
            primary_route.build_authorized_abort_multipart_upload_command(
                &authorized_abort,
                Some(&abort_cleanup),
                &complete_multipart_proof,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
        });
        match crossed_abort_proof {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("crossed authorized multipart abort proof must fail: {other:?}"),
        }

        let mismatched_subject_request = StorageRpcObjectRequest {
            key: crate::tests::object_key("different-multipart-route-key"),
            ..request.clone()
        };
        let mismatched_subject_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_primary_object_route(
                    &active_permit,
                    &mismatched_subject_request,
                    "mismatched multipart subject route",
                )
                .unwrap()
        });
        let mismatched_subject = crate::clock::with_time_override(1_000, || {
            mismatched_subject_route.load_multipart_completion_snapshot(&authorized_upload, &[])
        });
        match mismatched_subject {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("subject does not match"));
            }
            other => panic!("mismatched authorized upload must fail at capability: {other:?}"),
        }
        let mismatched_create_match = crate::clock::with_time_override(1_000, || {
            mismatched_subject_route.matching_multipart_upload_initiated_at(
                &existing_multipart_request,
                Some(&expected_multipart_command),
            )
        });
        match mismatched_create_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("subject does not match"));
            }
            other => panic!("mismatched multipart create request must fail: {other:?}"),
        }
        let mut mismatched_expected_command = expected_multipart_command.clone();
        mismatched_expected_command.upload_mut_for_test().key =
            crate::tests::object_key("different-expected-command-key");
        let mismatched_expected_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_multipart_upload_initiated_at(
                &new_multipart_request,
                Some(&mismatched_expected_command),
            )
        });
        match mismatched_expected_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error
                    .message
                    .contains("expected command subject does not match"));
            }
            other => panic!("mismatched multipart expected command must fail: {other:?}"),
        }
        let mut mismatched_expected_proof = expected_multipart_command.clone();
        mismatched_expected_proof
            .bucket_write_reservation_mut_for_test()
            .operation_kind = "put-object-metadata".to_string();
        let mismatched_expected_match = crate::clock::with_time_override(1_000, || {
            primary_route.matching_multipart_upload_initiated_at(
                &existing_multipart_request,
                Some(&mismatched_expected_proof),
            )
        });
        match mismatched_expected_match {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("mismatched multipart expected proof must fail: {other:?}"),
        }

        let (metadata_stored, current_delete_snapshot, specific_delete_snapshot) =
            crate::clock::with_time_override(1_000, || {
                let metadata_stored = primary_route
                    .load_put_object_metadata_snapshot(None)
                    .unwrap();
                let current = primary_route.load_current_object_delete_snapshot().unwrap();
                let specific = primary_route
                    .load_specific_object_delete_snapshot(VersionId::Null)
                    .unwrap();
                let lifecycle_versions =
                    primary_route.list_object_versions_for_lifecycle().unwrap();
                assert_eq!(current.stored.as_ref(), Some(&metadata_stored));
                assert_eq!(specific, current);
                assert_eq!(lifecycle_versions, vec![metadata_stored.clone()]);
                (metadata_stored, current, specific)
            });
        crate::clock::with_time_override(1_000, || {
            primary_route
                .build_create_stream_upload_command(
                    &new_stream_create_request,
                    Some(4_000),
                    CreateStreamUploadPrecondition::PutObject {
                        expected_current: Some(&metadata_stored),
                        require_generation_reservation: false,
                    },
                    &stream_proof,
                )
                .unwrap();
        });
        let mut mismatched_stream_expected_current = metadata_stored.clone();
        match &mut mismatched_stream_expected_current {
            crate::StoredObject::Live(object) => {
                object.key = crate::tests::object_key("different-stream-expected-key");
            }
            crate::StoredObject::DeleteMarker(_) => {
                panic!("active object route fixture must contain a live object");
            }
        }
        let mismatched_stream_expected = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_stream_upload_command(
                &new_stream_create_request,
                Some(4_000),
                CreateStreamUploadPrecondition::PutObject {
                    expected_current: Some(&mismatched_stream_expected_current),
                    require_generation_reservation: false,
                },
                &stream_proof,
            )
        });
        match mismatched_stream_expected {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("expected object does not match"));
            }
            other => panic!("mismatched expected stream object must fail: {other:?}"),
        }
        let create_multipart_command = crate::clock::with_time_override(1_000, || {
            primary_route
                .build_create_multipart_upload_command(
                    &new_multipart_request,
                    Some(&metadata_stored),
                    &create_multipart_proof,
                )
                .unwrap()
        });
        let MetadataCommandPayload::CreateMultipartUpload(create_multipart) =
            create_multipart_command.payload()
        else {
            panic!("active object route must build a create multipart upload command");
        };
        assert_eq!(create_multipart.upload().bucket, bucket);
        assert_eq!(create_multipart.upload().key, key);
        assert_eq!(
            create_multipart.bucket_write_reservation(),
            &create_multipart_proof
        );

        let mut mismatched_create_request = new_multipart_request.clone();
        mismatched_create_request.key = crate::tests::object_key("different-create-request-key");
        let mismatched_create_build = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_multipart_upload_command(
                &mismatched_create_request,
                Some(&metadata_stored),
                &create_multipart_proof,
            )
        });
        match mismatched_create_build {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("subject does not match"));
            }
            other => panic!("mismatched multipart create build must fail: {other:?}"),
        }
        let mut mismatched_create_proof = create_multipart_proof.clone();
        mismatched_create_proof.operation_kind = "put-object-metadata".to_string();
        let mismatched_create_build = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_multipart_upload_command(
                &new_multipart_request,
                Some(&metadata_stored),
                &mismatched_create_proof,
            )
        });
        match mismatched_create_build {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => panic!("mismatched multipart create proof must fail: {other:?}"),
        }
        let mut mismatched_expected_current = metadata_stored.clone();
        match &mut mismatched_expected_current {
            crate::StoredObject::Live(object) => {
                object.key = crate::tests::object_key("different-expected-object-key");
            }
            crate::StoredObject::DeleteMarker(_) => {
                panic!("active object route fixture must contain a live object");
            }
        }
        let mismatched_create_build = crate::clock::with_time_override(1_000, || {
            primary_route.build_create_multipart_upload_command(
                &new_multipart_request,
                Some(&mismatched_expected_current),
                &create_multipart_proof,
            )
        });
        match mismatched_create_build {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("expected object does not match"));
            }
            other => panic!("mismatched multipart expected object must fail: {other:?}"),
        }

        let mut mismatched_mutation_proof = metadata_proof.clone();
        mismatched_mutation_proof.bucket =
            crate::tests::bucket_name("different-mutation-proof-bucket");
        let mutation_proof_mismatch = crate::clock::with_time_override(1_000, || {
            primary_route.build_put_object_metadata_command(
                None,
                &metadata_stored,
                VersionId::Null,
                PutObjectMetadataMutation::PutTags(crate::tests::object_tags(serialized_tags)),
                &mismatched_mutation_proof,
            )
        });
        match mutation_proof_mismatch {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("proof does not match"));
            }
            other => {
                panic!("mismatched mutation proof must fail at the route capability: {other:?}")
            }
        }

        let mut wrong_operation_proof = metadata_proof.clone();
        wrong_operation_proof.operation_kind = "delete-object-version".to_string();
        let mut wrong_target_proof = metadata_proof.clone();
        wrong_target_proof.target_context = Some("different-object-key".to_string());
        let mut wrong_epoch_proof = metadata_proof.clone();
        wrong_epoch_proof.cluster_epoch = ClusterEpoch::new(2).unwrap();
        for (mismatch, mismatched_proof) in [
            ("operation", wrong_operation_proof),
            ("target", wrong_target_proof),
            ("epoch", wrong_epoch_proof),
        ] {
            let result = crate::clock::with_time_override(1_000, || {
                primary_route.build_put_object_metadata_command(
                    None,
                    &metadata_stored,
                    VersionId::Null,
                    PutObjectMetadataMutation::PutTags(crate::tests::object_tags(serialized_tags)),
                    &mismatched_proof,
                )
            });
            match result {
                Err(StorageNodeObjectRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                    assert!(error.message.contains("proof does not match"));
                }
                other => panic!(
                    "same-bucket mutation proof with mismatched {mismatch} must fail: {other:?}"
                ),
            }
        }

        let stale_payload_mismatch = crate::clock::with_time_override(1_000, || {
            primary_route.build_insert_delete_marker_command(
                current_delete_snapshot.stored.as_ref(),
                VersionId::from_u64(2),
                &crate::OwnerIdentity::from_principal("active-object-route-owner"),
                InsertDeleteMarkerStalePayload::Explicit(Some(
                    crate::metadata_command::ObjectPayloadReclaimCommand::Segments(
                        crate::ObjectSegmentsReclaimRecord {
                            bucket: crate::tests::bucket_name("different-stale-payload-bucket"),
                            key: key.clone(),
                            generation_id: GenerationId::new(9).unwrap(),
                            created_at: 1_000,
                            segments: Vec::new(),
                        },
                    ),
                )),
                None,
                &marker_proof,
            )
        });
        match stale_payload_mismatch {
            Err(StorageNodeObjectRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("stale payload mode does not match"));
            }
            other => {
                panic!("mismatched stale payload must fail at the route capability: {other:?}")
            }
        }

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(10_000)
        );

        let next_segment_vid_before_delayed_rpc = server
            ._node
            .get_pg(0)
            .unwrap()
            .get_stream_upload(&reservation_id)
            .unwrap()
            .next_segment_vid;
        let delayed_append_response = crate::clock::with_time_override(6_000, || {
            handler
                .stream_segment_append_prepare_response(
                    &active_permit,
                    StorageRpcStreamSegmentAppendPrepareRequest {
                        object: request.clone(),
                        request: PrepareStreamUploadSegmentAppendReq {
                            segment_index: 1,
                            ..stream_append_request.clone()
                        },
                        effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                            authority_valid_until_ms: 5_000,
                            portable_wall_valid_until_ms: 4_000,
                        }),
                    },
                )
                .unwrap()
        });
        let delayed_append_error = decode_storage_rpc_response_payload(&delayed_append_response)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            delayed_append_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
        assert_eq!(
            server
                ._node
                .get_pg(0)
                .unwrap()
                .get_stream_upload(&reservation_id)
                .unwrap()
                .next_segment_vid,
            next_segment_vid_before_delayed_rpc,
            "an expired delayed append RPC must not consume a segment VID"
        );

        crate::clock::with_time_override(6_000, || {
            match primary_route.load_multipart_upload(&upload_id) {
                Err(StorageNodeMultipartUploadRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                }
                Err(StorageNodeMultipartUploadRouteError::Upload(error)) => {
                    panic!("captured route should expire before multipart upload load: {error}")
                }
                Ok(_) => panic!("expired captured route performed multipart upload load"),
            }
            let expired_operations = [
                (
                    "generation reservation",
                    primary_route
                        .generation_reservation(&reservation_id)
                        .map(|_| ()),
                ),
                (
                    "generation allocation",
                    primary_route.next_generation_id().map(|_| ()),
                ),
                (
                    "version allocation",
                    acting_set_route.next_version_id().map(|_| ()),
                ),
                (
                    "direct PUT snapshot load",
                    primary_route
                        .load_direct_put_commit_snapshot(&reservation_id, reserved_generation)
                        .map(|_| ()),
                ),
                (
                    "direct PUT command build",
                    primary_route
                        .build_direct_put_commit_command(
                            &direct_put_request,
                            VersionId::Null,
                            &direct_put_snapshot,
                        )
                        .map(|_| ()),
                ),
                (
                    "object read authorization subject load",
                    read_route.load_object_read_auth_subject(None).map(|_| ()),
                ),
                (
                    "object read snapshot load",
                    read_route
                        .load_object_read_snapshot_for_subject(
                            None,
                            &read_subject.identity,
                            crate::ObjectReadSnapshotMode::StandardSegments,
                        )
                        .map(|_| ()),
                ),
                (
                    "stream upload retry match",
                    primary_route
                        .matching_stream_upload_exists(
                            &stream_create_request,
                            Some(&expected_stream_command),
                        )
                        .map(|_| ()),
                ),
                (
                    "UploadPart stream retry match",
                    primary_route
                        .matching_stream_upload_exists(
                            &upload_part_stream_create_request,
                            Some(&expected_upload_part_stream_command),
                        )
                        .map(|_| ()),
                ),
                (
                    "PutObject stream command build",
                    primary_route
                        .build_create_stream_upload_command(
                            &new_stream_create_request,
                            Some(4_000),
                            CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                                require_generation_reservation: false,
                            },
                            &stream_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "UploadPart stream command build",
                    primary_route
                        .build_create_stream_upload_command(
                            &new_upload_part_stream_create_request,
                            None,
                            CreateStreamUploadPrecondition::UploadPart {
                                expected_upload: &multipart_upload,
                            },
                            &upload_part_stream_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "stream upload session load",
                    primary_route
                        .load_stream_upload_session(&reservation_id)
                        .map(|_| ()),
                ),
                (
                    "stream upload segment list",
                    primary_route
                        .load_stream_upload_segments(&reservation_id)
                        .map(|_| ()),
                ),
                (
                    "stream PUT finalize snapshot load",
                    primary_route
                        .load_stream_put_finalize_snapshot(&reservation_id)
                        .map(|_| ()),
                ),
                (
                    "stream PUT commit command build",
                    primary_route
                        .build_stream_put_commit_command(
                            &reservation_id,
                            0,
                            &put_finalize_snapshot,
                            &put_commit,
                            &renewed_stream_proof,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "stream part finalize snapshot load",
                    primary_route
                        .load_stream_part_finalize_snapshot(
                            &upload_id,
                            &upload_part_stream_session_id,
                            1,
                        )
                        .map(|_| ()),
                ),
                (
                    "stream part commit command build",
                    primary_route
                        .build_stream_part_commit_command(
                            &upload_id,
                            &upload_part_stream_session_id,
                            1,
                            &part_finalize_snapshot,
                            &finalized_part,
                            &[],
                            &upload_part_finalize_proof,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "stream segment append preparation",
                    primary_route
                        .prepare_stream_segment_append(
                            &stream_append_request,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "stream upload reservation update",
                    primary_route
                        .update_stream_upload_bucket_write_reservation(
                            &reservation_id,
                            &renewed_stream_proof,
                            &renewed_stream_proof,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "object metadata PUT snapshot load",
                    primary_route
                        .load_put_object_metadata_snapshot(None)
                        .map(|_| ()),
                ),
                (
                    "current object delete snapshot load",
                    primary_route
                        .load_current_object_delete_snapshot()
                        .map(|_| ()),
                ),
                (
                    "specific object delete snapshot load",
                    primary_route
                        .load_specific_object_delete_snapshot(VersionId::Null)
                        .map(|_| ()),
                ),
                (
                    "lifecycle object version list load",
                    primary_route
                        .list_object_versions_for_lifecycle()
                        .map(|_| ()),
                ),
                (
                    "in-progress multipart upload load",
                    primary_route
                        .load_in_progress_multipart_upload(&upload_id)
                        .map(|_| ()),
                ),
                (
                    "listing in-progress multipart upload load",
                    primary_route
                        .load_in_progress_multipart_upload_for_listing(&upload_id)
                        .map(|_| ()),
                ),
                (
                    "multipart completion snapshot load",
                    primary_route
                        .load_multipart_completion_snapshot(&authorized_upload, &[])
                        .map(|_| ()),
                ),
                (
                    "multipart completion preflight load",
                    primary_route
                        .load_multipart_completion_preflight(&authorized_upload)
                        .map(|_| ()),
                ),
                (
                    "multipart parts list",
                    read_route
                        .list_multipart_parts_for_authorized_upload(&authorized_upload, None, 10)
                        .map(|_| ()),
                ),
                (
                    "multipart management lookup",
                    read_route
                        .lookup_multipart_upload_management(&upload_id)
                        .map(|_| ()),
                ),
                (
                    "multipart completion stale source load",
                    primary_route
                        .load_multipart_completion_stale_payload_source()
                        .map(|_| ()),
                ),
                (
                    "abort multipart cleanup load",
                    primary_route
                        .load_abort_multipart_upload_cleanup(&upload_id)
                        .map(|_| ()),
                ),
                (
                    "complete multipart command build",
                    primary_route
                        .build_complete_multipart_object_command(
                            &completion_request,
                            VersionId::Null,
                            &complete_multipart_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "abort multipart command build",
                    primary_route
                        .build_abort_multipart_upload_command(
                            &upload_id,
                            Some(&abort_cleanup),
                            &abort_multipart_proof,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "authorized abort multipart command build",
                    primary_route
                        .build_authorized_abort_multipart_upload_command(
                            &authorized_abort,
                            Some(&abort_cleanup),
                            &abort_multipart_proof,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "multipart upload retry match",
                    primary_route
                        .matching_multipart_upload_initiated_at(
                            &existing_multipart_request,
                            Some(&expected_multipart_command),
                        )
                        .map(|_| ()),
                ),
                (
                    "create multipart upload command build",
                    primary_route
                        .build_create_multipart_upload_command(
                            &new_multipart_request,
                            Some(&metadata_stored),
                            &create_multipart_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "object metadata PUT command build",
                    primary_route
                        .build_put_object_metadata_command(
                            None,
                            &metadata_stored,
                            VersionId::Null,
                            PutObjectMetadataMutation::PutTags(crate::tests::object_tags(
                                serialized_tags,
                            )),
                            &metadata_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "current object delete command build",
                    primary_route
                        .build_delete_current_object_command(
                            current_delete_snapshot.stored.as_ref(),
                            current_delete_snapshot.target.as_ref(),
                            &delete_current_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "specific object delete command build",
                    primary_route
                        .build_delete_specific_object_version_command(
                            VersionId::Null,
                            specific_delete_snapshot.stored.as_ref(),
                            specific_delete_snapshot.target.as_ref(),
                            None,
                            &delete_specific_proof,
                        )
                        .map(|_| ()),
                ),
                (
                    "insert delete marker command build",
                    primary_route
                        .build_insert_delete_marker_command(
                            current_delete_snapshot.stored.as_ref(),
                            VersionId::from_u64(2),
                            &crate::OwnerIdentity::from_principal("active-object-route-owner"),
                            InsertDeleteMarkerStalePayload::Explicit(None),
                            None,
                            &marker_proof,
                        )
                        .map(|_| ()),
                ),
            ];
            for (operation, result) in expired_operations {
                match result {
                    Err(StorageNodeObjectRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                        assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                    }
                    Err(StorageNodeObjectRouteError::Object(error)) => {
                        panic!("captured route should expire before {operation}: {error}")
                    }
                    Ok(()) => panic!("expired captured route performed {operation}"),
                }
            }
        });

        let pg = server._node.get_pg(0).unwrap();
        assert_eq!(
            PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            reserved_generation,
            "expired active object routes must leave durable reservation state exact"
        );
        assert_eq!(
            PgMetadataStore::get_stream_upload(&*pg, &reservation_id)
                .unwrap()
                .bucket_write_reservation
                .as_ref(),
            Some(&renewed_stream_proof),
            "expired active stream mutation must leave the durable proof exact"
        );
    }
    #[test]
    fn peering_metadata_read_route_is_exact_and_does_not_grant_write_authority() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("peering-readable-bucket");
        let changed_bucket = crate::tests::bucket_name("peering-proof-change-bucket");
        let proof = {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            pg.refresh_metadata_command_state_digest().unwrap();
            SharedStorageNode::pg_heartbeat_observation_from_pg(
                &pg,
                config.node_id,
                PgId::new(0),
                PgState::Peering,
            )
            .unwrap()
            .metadata_proof
        };

        let mut peering = config.clone();
        peering.cluster_epoch = ClusterEpoch::new(2).unwrap();
        peering.pg_routes[0].cluster_epoch = peering.cluster_epoch;
        peering.pg_routes[0].state = PgState::Peering;
        peering.pg_routes[0].primary_node_id = NodeId::new(8);
        peering.pg_routes[0].acting_set = vec![NodeId::new(8), config.node_id];
        peering.pg_routes[0].metadata_read_route =
            Some(PgMetadataReadRoute::new(config.node_id, proof));
        let peering_epoch = peering.cluster_epoch;
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(peering)
                .unwrap();
        });

        let handler = server.connection_handler();
        let permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcBucketRequest {
            node_id: config.node_id,
            cluster_epoch: peering_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
        };
        let read_route = crate::clock::with_time_override(1_000, || {
            handler
                .metadata_read_bucket_route(&permit, &request, "Peering bucket read")
                .unwrap()
        });
        crate::clock::with_time_override(1_000, || {
            assert_eq!(read_route.head_bucket(true).unwrap().name, bucket);
            match handler.active_bucket_route(&permit, &request, "Peering bucket mutation") {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute),
                Ok(_) => panic!("Peering metadata read certificate granted mutation authority"),
            }
        });

        {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &changed_bucket);
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        crate::clock::with_time_override(1_000, || match read_route.head_bucket(true) {
            Err(StorageNodeBucketRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                assert!(error.message.contains("certified metadata read proof"));
            }
            Err(error) => panic!("changed Peering proof returned wrong error: {error:?}"),
            Ok(info) => panic!("changed Peering proof served stale certificate: {info:?}"),
        });
    }

    #[test]
    fn active_bucket_route_atomically_captures_deadline_during_validity_extension() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        }));
        let bucket = crate::tests::bucket_name("active-route-bucket");
        crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
        });
        let mut handler = server.connection_handler();
        let route_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcBucketRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
        };
        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        match handler.active_bucket_route(&foreign_permit, &request, "test bucket read") {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign admission permit created an active bucket route"),
        }
        let cleanup_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.active_bucket_route(&cleanup_permit, &request, "test bucket read") {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires active route admission"));
            }
            Ok(_) => panic!("retained-cleanup admission created an active bucket route"),
        }
        drop(cleanup_permit);

        let capture_barrier = Arc::new(Barrier::new(2));
        let capture_hook_barrier = Arc::clone(&capture_barrier);
        *server
            .runtime_route_capture_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(move || {
            capture_hook_barrier.wait();
            capture_hook_barrier.wait();
        }));
        let (publish_attempt_tx, publish_attempt_rx) = mpsc::channel();
        let runtime_route_state = Arc::clone(&server.runtime_route_state);
        *server
            .runtime_route_before_publish_lock_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(move || {
            assert!(
                runtime_route_state.try_write().is_err(),
                "validity renewal must contend with the in-progress coherent route capture"
            );
            publish_attempt_tx.send(()).unwrap();
        }));

        let (captured_handler_tx, captured_handler_rx) = mpsc::channel();
        let capture = thread::spawn(move || {
            handler.refresh_config_snapshot();
            captured_handler_tx.send(handler).unwrap();
        });
        capture_barrier.wait();

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            crate::clock::with_time_override(1_000, || {
                installing_server
                    .install_control_plane_runtime_config(extended)
                    .unwrap();
            });
        });
        publish_attempt_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("validity renewal did not reach the coherent route-state write boundary");
        capture_barrier.wait();
        let handler = captured_handler_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("route capture did not finish after releasing its read lock hook");
        capture.join().unwrap();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(10_000)
        );

        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_bucket_route(&route_permit, &request, "test bucket read")
                .unwrap()
        });
        let read_route = crate::clock::with_time_override(1_000, || {
            handler
                .metadata_read_bucket_route(&route_permit, &request, "test bucket read")
                .unwrap()
        });
        let reservation = crate::clock::with_time_override(1_000, || {
            route
                .acquire_write_reservation(
                    DurableBucketWriteReservationAcquire {
                        name: &bucket,
                        reservation_id: "captured-active-route-reservation",
                        owner_token: "captured-active-route-owner",
                        cluster_epoch: config.cluster_epoch,
                        operation_kind: "test-active-route",
                        created_at: 1_000,
                        lease_deadline: 4_000,
                        target_context: Some("key=a"),
                    },
                    AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 5_000),
                )
                .unwrap()
        });
        crate::clock::with_time_override(1_000, || {
            assert_eq!(read_route.head_bucket(true).unwrap().name, bucket);
            assert_eq!(
                read_route
                    .get_subresource(BucketSubresourceKind::Cors)
                    .unwrap(),
                None
            );
            assert_eq!(
                read_route
                    .load_snapshot(BucketSnapshotRequest::default())
                    .unwrap()
                    .bucket
                    .name,
                bucket
            );
            route
                .validate_write_reservation(&BucketWriteReservationProof::from(&reservation))
                .unwrap();
        });

        crate::clock::with_time_override(3_500, || {
            match route.heartbeat_write_reservation(
                &BucketWriteReservationProof::from(&reservation),
                9_000,
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 3_000),
            ) {
                Err(StorageNodeBucketRouteError::Bucket(BucketSnapshotLoadError::Store(
                    StoreError::RouteMapExpired { .. },
                ))) => {}
                Err(error) => panic!(
                    "expired request effect fence returned an unexpected heartbeat error: {error:?}"
                ),
                Ok(record) => panic!(
                    "expired request effect fence unexpectedly renewed reservation {record:?}"
                ),
            }
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::durable_bucket_write_reservation(
                    &*pg,
                    &bucket,
                    &reservation.reservation_id,
                )
                .unwrap()
                .unwrap()
                .lease_deadline,
                reservation.lease_deadline,
                "expired request effect fence must reject before durable heartbeat mutation"
            );
        });

        crate::clock::with_time_override(6_000, || match read_route.head_bucket(true) {
            Err(StorageNodeBucketRouteError::Route(error)) => {
                assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                assert!(error.message.contains("expired at 5000ms, now 6000ms"));
            }
            Err(StorageNodeBucketRouteError::Bucket(error)) => {
                panic!("captured route should expire before node access: {error}")
            }
            Ok(info) => panic!("expired captured route unexpectedly loaded {info:?}"),
        });
        crate::clock::with_time_override(6_000, || {
            match route.heartbeat_write_reservation(
                &BucketWriteReservationProof::from(&reservation),
                9_000,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            ) {
                Err(StorageNodeBucketRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                }
                Err(StorageNodeBucketRouteError::Bucket(error)) => {
                    panic!("captured route should expire before reservation heartbeat: {error}")
                }
                Ok(record) => {
                    panic!("expired captured route unexpectedly renewed reservation {record:?}")
                }
            }
            match route.acquire_write_reservation(
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "expired-active-route-reservation",
                    owner_token: "expired-active-route-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: "test-expired-active-route",
                    created_at: 6_000,
                    lease_deadline: 9_000,
                    target_context: Some("key=b"),
                },
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 5_000),
            ) {
                Err(StorageNodeBucketRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                }
                Err(StorageNodeBucketRouteError::Bucket(error)) => {
                    panic!("captured route should expire before reservation acquire: {error}")
                }
                Ok(record) => {
                    panic!("expired captured route unexpectedly acquired reservation {record:?}")
                }
            }
        });
        let pg = server._node.get_pg(0).unwrap();
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                &reservation.reservation_id,
            )
            .unwrap(),
            Some(reservation.clone()),
            "expired active heartbeat must not mutate the reservation"
        );
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                "expired-active-route-reservation",
            )
            .unwrap()
            .is_none(),
            "expired active acquire must not create a reservation"
        );
        drop(pg);

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_write_reservation_route(
            &route_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &reservation,
            "test reservation release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active admission created retained cleanup authority"),
        }
        let foreign_retained_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_write_reservation_route(
            &foreign_retained_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &reservation,
            "test reservation release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign admission created retained cleanup authority"),
        }
        crate::clock::with_time_override(6_000, || {
            handler
                .retained_bucket_write_reservation_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    &reservation,
                    "test reservation release",
                )
                .unwrap()
                .release()
                .unwrap();
        });
        let pg = server._node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                &reservation.reservation_id,
            )
            .unwrap()
            .is_none(),
            "retained cleanup must release the exact expired active-route subject"
        );
    }

    #[test]
    fn storage_node_runtime_config_validity_shrink_drains_frames() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let admitted = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut shortened = config;
        shortened.route_map_validity = RouteMapValidity::until_ms(4_000).unwrap();

        let (installed_tx, installed_rx) = mpsc::channel();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(shortened)
                .unwrap();
            installed_tx.send(()).unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let transition = server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .transition;
            if transition == StorageNodeRouteTransitionState::Draining {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "validity shrink did not begin draining"
            );
            thread::yield_now();
        }
        assert!(matches!(
            installed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(admitted);
        installed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(4_000)
        );
    }

    #[test]
    fn storage_node_route_transition_orders_old_command_before_successor_activation() {
        let tmp = test_util::tempdir();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        private_socket_dir(config.socket_path.parent().unwrap());
        let socket_path = config.socket_path.clone();
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let _stderr_guard = server.suppress_metadata_command_lock_wait_stderr();
        let (lock_wait_tx, lock_wait_rx) = mpsc::channel();
        server
            .metadata_command_locks
            .set_before_wait_hook(Arc::new(move |pg_id| {
                assert_eq!(pg_id, PgId::new(0));
                let _ = lock_wait_tx.send(());
            }));
        {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &crate::tests::bucket_name("metadata-rpc-bucket"));
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let serving_server = Arc::clone(&server);
        let _server_thread = thread::spawn(move || serving_server.serve_forever().unwrap());

        let state_request = StorageRpcMetadataCommandStateRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };
        let state_payload = encode_metadata_command_state_request(&state_request);
        let mut lock_owner = UnixStream::connect(&socket_path).unwrap();
        let acquire = send_frame(
            &mut lock_owner,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            state_payload.clone(),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        let acquire_drained_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let active_frames = server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active_frames;
            if active_frames == 0 {
                break;
            }
            assert!(
                Instant::now() < acquire_drained_deadline,
                "PG-lock acquire frame did not leave route admission"
            );
            thread::yield_now();
        }

        let old_command = test_metadata_command(0, 1);
        let old_request = encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            command: old_command,
        })
        .unwrap();
        let blocked_socket_path = socket_path.clone();
        let (old_response_tx, old_response_rx) = mpsc::channel();
        let old_frame = thread::spawn(move || {
            let mut client = UnixStream::connect(blocked_socket_path).unwrap();
            let response = send_frame(
                &mut client,
                1,
                StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                old_request,
            );
            old_response_tx.send(response).unwrap();
        });

        lock_wait_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("old metadata command should wait for the PG lock");
        assert_eq!(
            server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active_frames,
            1,
            "the blocked command should hold the only admitted frame"
        );

        let source_route = config.pg_routes[0].clone();
        let mut peering_config = bounded_runtime_refresh_config(config.clone());
        peering_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        peering_config.pg_routes[0].cluster_epoch = peering_config.cluster_epoch;
        peering_config.pg_routes[0].state = PgState::Peering;
        peering_config.historical_pg_routes.push(source_route);
        let (installed_tx, installed_rx) = mpsc::channel();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(peering_config)
                .unwrap();
            installed_tx.send(()).unwrap();
        });

        let draining_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let transition = server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .transition;
            if transition == StorageNodeRouteTransitionState::Draining {
                break;
            }
            assert!(
                Instant::now() < draining_deadline,
                "Peering route install did not begin draining"
            );
            thread::yield_now();
        }
        assert!(matches!(
            installed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let release = send_frame(
            &mut lock_owner,
            2,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            state_payload,
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        drop(lock_owner);

        let old_response = old_response_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|error| {
                let (active_frames, transition) = {
                    let admission = server
                        .route_admission
                        .inner
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (admission.active_frames, admission.transition)
                };
                let lock_holder = server
                    .metadata_command_locks
                    .state
                    .held
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&PgId::new(0))
                    .copied();
                panic!(
                    "old command did not complete after PG-lock release: {error:?}; active_frames={} transition={:?} config_epoch={} lock_holder={lock_holder:?}",
                    active_frames,
                    transition,
                    server.config_snapshot().cluster_epoch.get()
                )
            });
        let old_payload = decode_storage_rpc_response_payload(&old_response.payload)
            .unwrap()
            .unwrap();
        let old_outcome = decode_metadata_command_state_outcome_response(&old_payload).unwrap();
        assert!(matches!(
            old_outcome.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    cluster_epoch,
                    applied_log_index: 1,
                    ..
                }
            ) if cluster_epoch == ClusterEpoch::new(1).unwrap()
        ));
        old_frame.join().unwrap();
        installed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().pg_routes[0].state,
            PgState::Peering
        );

        // Once the successor route is Active, its runtime configuration carries no
        // exact pending-command recovery authorization for the old epoch.
        let peering_route = server.config_snapshot().pg_routes[0].clone();
        let mut successor_config = bounded_runtime_refresh_config(server.config_snapshot());
        successor_config.cluster_epoch = ClusterEpoch::new(3).unwrap();
        successor_config.pg_routes[0].cluster_epoch = successor_config.cluster_epoch;
        successor_config.pg_routes[0].state = PgState::Active;
        successor_config.historical_pg_routes.push(peering_route);
        server
            .install_control_plane_runtime_config(successor_config)
            .unwrap();

        let before_rejected_old_command = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let stale_request = encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            command: test_metadata_command(0, 2),
        })
        .unwrap();
        let mut stale_client = UnixStream::connect(&socket_path).unwrap();
        let stale_response = send_frame(
            &mut stale_client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            stale_request,
        );
        let stale_error = decode_storage_rpc_response_payload(&stale_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(stale_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(stale_error.message.contains("not authorized"));
        assert_eq!(
            server
                ._node
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap(),
            before_rejected_old_command,
            "an old-epoch command must not change successor-visible metadata or its proof"
        );
    }

    #[test]
    fn storage_node_route_transition_allows_lock_release_during_drain() {
        let gate = StorageNodeRouteAdmissionGate::default();
        let admitted = gate.acquire(StorageNodeRouteAdmissionClass::Active);
        let transition_gate = gate.clone();
        let (publishing_tx, publishing_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let transition = thread::spawn(move || {
            let guard = transition_gate.begin_transition();
            publishing_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(guard);
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let state = gate
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.transition == StorageNodeRouteTransitionState::Draining {
                break;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "route transition did not begin draining"
            );
            thread::yield_now();
        }

        drop(gate.acquire(StorageNodeRouteAdmissionClass::RetainedCleanup));
        assert!(matches!(
            publishing_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(admitted);
        publishing_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let regular_gate = gate.clone();
        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (regular_tx, regular_rx) = mpsc::channel();
        let regular = thread::spawn(move || {
            attempted_tx.send(()).unwrap();
            let _permit = regular_gate.acquire(StorageNodeRouteAdmissionClass::Active);
            regular_tx.send(()).unwrap();
        });
        attempted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            regular_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        regular_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        regular.join().unwrap();
        transition.join().unwrap();
    }

    #[test]
    fn metadata_command_commit_guard_rolls_back_expired_route_mutation() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        create_probe_bucket_direct(&pg, &bucket);
        pg.refresh_metadata_command_state_digest().unwrap();
        let command = test_metadata_command(0, 1);

        let error = pg
            .apply_metadata_command_and_record_with_commit_guard(7, &command, || {
                Err(StoreError::RouteMapExpired {
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    valid_until_ms: 10,
                    now_ms: 10,
                })
            })
            .unwrap_err();
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::RouteMapExpired {
                    cluster_epoch,
                    valid_until_ms: 10,
                    now_ms: 10,
                }) if *cluster_epoch == ClusterEpoch::new(1).unwrap()
            ),
            "unexpected metadata command error: {error:?}"
        );
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::new(1).unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn expired_store_route_maps_are_retryable_stale_locations() {
        let error = StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::new(7).unwrap(),
            valid_until_ms: 10,
            now_ms: 11,
        };

        let response = store_error_response(error);

        assert_eq!(response.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(response.message.contains("cluster epoch 7"));
    }

    #[test]
    fn storage_node_server_accepts_second_client_while_first_session_is_held() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let accept_thread = thread::spawn(move || {
            server_for_thread.accept_and_spawn().unwrap();
            server_for_thread.accept_and_spawn().unwrap();
        });
        let location = test_location(1, 0, 7);

        let mut held_client = UnixStream::connect(&socket_path).unwrap();
        let acquire = send_frame(
            &mut held_client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("held-read", location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let mut health_client = UnixStream::connect(socket_path).unwrap();
        let health = send_frame(
            &mut health_client,
            8,
            StorageRpcMessageKind::Health,
            Vec::new(),
        );
        let health_payload = decode_storage_rpc_response_payload(&health.payload)
            .unwrap()
            .unwrap();
        let health = decode_health_response(&health_payload).unwrap();
        assert_eq!(health.node_id, NodeId::new(7));

        drop(health_client);
        drop(held_client);
        accept_thread.join().unwrap();
        wait_for_read_handle_count(&server, location, 0);
    }

    #[test]
    fn storage_node_server_disconnect_releases_handles_for_cleanup_probe() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        drop(client);
        join.join().unwrap();
        wait_for_read_handle_count(&server, location, 0);

        let mut handles = server
            .read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let shard_key = test_shard_key(location.shard_index().get());
        handles
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap();
        assert_eq!(handles.count(location), 1);
        handles.release(&[(location, shard_key)]);
        assert_eq!(handles.count(location), 0);
    }

    #[test]
    fn read_handle_session_survives_publication_and_releases_during_drain() {
        let tmp = test_util::tempdir();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, config.node_id.as_u32());

        let mut client = UnixStream::connect(socket_path).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-before-publication", first_location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 1);

        let mut second_config = bounded_runtime_refresh_config(config);
        second_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        second_config.pg_routes[0].cluster_epoch = second_config.cluster_epoch;
        server
            .install_control_plane_runtime_config(second_config.clone())
            .unwrap();
        assert_eq!(
            server.read_handle_count(first_location),
            1,
            "route publication must not terminate a live read-handle session"
        );
        let release_after_publication = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-before-publication"),
        );
        decode_storage_rpc_response_payload(&release_after_publication.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);

        let second_location = test_location(2, 0, second_config.node_id.as_u32());
        let acquire_during_current_route = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-during-drain", second_location),
        );
        decode_storage_rpc_response_payload(&acquire_during_current_route.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(second_location), 1);

        let admitted = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut third_config = bounded_runtime_refresh_config(second_config);
        third_config.cluster_epoch = ClusterEpoch::new(3).unwrap();
        third_config.pg_routes[0].cluster_epoch = third_config.cluster_epoch;
        let (installed_tx, installed_rx) = mpsc::channel();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(third_config)
                .unwrap();
            installed_tx.send(()).unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let transition = server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .transition;
            if transition == StorageNodeRouteTransitionState::Draining {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "route install did not begin draining"
            );
            thread::yield_now();
        }
        assert!(matches!(
            installed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let release_during_drain = send_frame(
            &mut client,
            10,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-during-drain"),
        );
        decode_storage_rpc_response_payload(&release_during_drain.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(second_location), 0);
        assert!(matches!(
            installed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        drop(admitted);
        installed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().cluster_epoch,
            ClusterEpoch::new(3).unwrap()
        );
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_server_idle_timeout_preserves_active_read_handles_until_release() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(location.shard_index().get());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        thread::sleep(STORAGE_RPC_SERVER_IDLE_TIMEOUT + Duration::from_millis(200));
        assert_eq!(
            server.read_handle_count(location),
            1,
            "server idle timeout must not release active read handles"
        );
        let delete_error = server
            .read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_begin_delete(location, &shard_key)
            .unwrap_err();
        assert_eq!(delete_error.code, StorageRpcErrorCode::ResourceExhausted);

        let release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        wait_for_read_handle_count(&server, location, 0);
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_server_idle_timeout_closes_session_with_read_handles_and_metadata_lock() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_first = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let first = thread::spawn(move || server_for_first.accept_one().unwrap());
        let metadata_request = StorageRpcMetadataCommandStateRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };
        let metadata_payload = encode_metadata_command_state_request(&metadata_request);
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(&socket_path).unwrap();
        let metadata_acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            metadata_payload.clone(),
        );
        decode_storage_rpc_response_payload(&metadata_acquire.payload)
            .unwrap()
            .unwrap();
        let read_acquire = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&read_acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        first.join().unwrap();
        wait_for_read_handle_count(&server, location, 0);

        let server_for_second = Arc::clone(&server);
        let second = thread::spawn(move || server_for_second.accept_one().unwrap());
        let mut second_client = UnixStream::connect(socket_path).unwrap();
        let reacquire = send_frame(
            &mut second_client,
            9,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            metadata_payload,
        );
        decode_storage_rpc_response_payload(&reacquire.payload)
            .unwrap()
            .unwrap();
        drop(client);
        drop(second_client);
        second.join().unwrap();
    }

    #[test]
    fn storage_node_server_rejects_non_private_socket_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o777),
        )
        .unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketDirectoryNotPrivate { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_special_mode_socket_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o1700),
        )
        .unwrap();

        let err = bind_error(config.clone());

        assert!(matches!(
            err,
            StorageNodeServerError::SocketDirectoryNotPrivate { mode: 0o1700, .. }
        ));
        let _ = fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        );
    }

    #[test]
    fn storage_node_server_creates_private_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        let mode = fs::metadata(&config.data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn storage_node_server_tightens_existing_readable_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        fs::create_dir_all(&config.data_dir).unwrap();
        fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o755)).unwrap();

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        let mode = fs::metadata(&config.data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn storage_node_server_rejects_writable_data_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        fs::create_dir_all(&config.data_dir).unwrap();
        fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o777)).unwrap();

        let err = bind_error(config.clone());

        assert!(matches!(
            err,
            StorageNodeServerError::Io { source, .. }
                if source.kind() == io::ErrorKind::PermissionDenied
        ));
        let _ = fs::set_permissions(&config.data_dir, fs::Permissions::from_mode(0o700));
    }

    #[test]
    fn storage_node_server_rejects_second_owner_for_same_data_dir() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _first = StorageNodeServer::bind(config.clone()).unwrap();
        let mut second = config;
        second.socket_path = tmp.path().join("sock").join("other.sock");

        let err = bind_error(second);

        assert!(matches!(
            err,
            StorageNodeServerError::DataDirAlreadyLocked { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_socket_paths() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicateSocketPath { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_pg_routes() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes.push(config.pg_routes[0].clone());

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicatePgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_unconfigured_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].pg_id = 9;

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::RoutePgNotConfigured { pg_id: 9 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_primary_outside_acting_set() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::RoutePrimaryNotInActingSet {
                pg_id: 0,
                primary_node_id: 8
            }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_bind_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);
        private_socket_dir(config.socket_path.parent().unwrap());

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_server_opens_only_configured_pg_directories() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![2];
        config.pg_routes = vec![test_route(2)];
        private_socket_dir(config.socket_path.parent().unwrap());

        let _server = StorageNodeServer::bind(config.clone()).unwrap();

        assert!(config.data_dir.join("pg-0002").is_dir());
        assert!(!config.data_dir.join("pg-0000").exists());
        assert!(!config.data_dir.join("pg-0001").exists());
    }

    #[test]
    fn storage_node_static_config_rejects_inconsistent_pg_routes() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");
        second.socket_path = tmp.path().join("sock").join("storage-2.sock");
        second.pg_routes[0].primary_node_id = NodeId::new(8);
        second.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::InconsistentPgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_removes_stale_socket_path_on_restart() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let stale = UnixListener::bind(&config.socket_path).unwrap();
        drop(stale);
        assert!(config.socket_path.exists());

        let _server = StorageNodeServer::bind(config).unwrap();
    }

    #[test]
    fn storage_node_server_restart_reopens_existing_pg_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = ShardKey::new(&[0xA5; 16], 7, 0);
        {
            let server = StorageNodeServer::bind(config.clone()).unwrap();
            let pg = server._node.get_pg(0).unwrap();
            pg.write_shard(&shard_key, b"persistent shard").unwrap();
        }

        let restarted = StorageNodeServer::bind(config).unwrap();
        assert_eq!(
            restarted._node.read_shard_file(0, &shard_key).unwrap(),
            b"persistent shard"
        );
        let pg = restarted._node.get_pg(0).unwrap();
        assert_eq!(pg.read_shard(&shard_key).unwrap().data, b"persistent shard");
    }

    #[test]
    fn storage_node_server_bind_preserves_terminal_pending_metadata_command() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bind-terminal-pending");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 123, 1).unwrap(),
            ),
        );
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
            pg.apply_metadata_command_and_record(7, &command).unwrap();
            assert!(pg
                .pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
                .unwrap()
                .is_some());
        }

        private_socket_dir(config.socket_path.parent().unwrap());
        let restarted = StorageNodeServer::bind(config).unwrap();
        let pg = restarted._node.get_pg(0).unwrap();
        assert!(
            pg.pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
                .unwrap()
                .is_some(),
            "bind recovery has no acting-set evidence and must preserve terminal pending metadata commands"
        );
        assert!(pg.head_bucket_record_raw(&bucket).is_ok());
    }

    #[test]
    fn storage_node_server_bind_repairs_cache_only_table_digest_drift() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("bind-cache-drift");
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
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
            pg.refresh_metadata_command_state_digest().unwrap();
            pg.test_increment_metadata_table_digest("buckets").unwrap();
            assert!(
                !pg.test_metadata_digest_table_mismatches()
                    .unwrap()
                    .is_empty(),
                "test setup should create cache-only table digest drift"
            );
        }

        private_socket_dir(config.socket_path.parent().unwrap());
        let restarted = StorageNodeServer::bind(config).unwrap();
        let pg = restarted._node.get_pg(0).unwrap();
        assert!(
            pg.test_metadata_digest_table_mismatches()
                .unwrap()
                .is_empty(),
            "bind recovery should refresh cache-only table digest drift"
        );
        assert!(pg.head_bucket_record_raw(&bucket).is_ok());
    }

    #[test]
    fn storage_node_server_bind_fails_closed_on_corrupted_metadata_state_digest() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            pg.test_increment_metadata_command_replica_state_digest()
                .unwrap();
        }

        private_socket_dir(config.socket_path.parent().unwrap());
        let error = bind_error(config);
        assert!(
            matches!(
                error.retained_store_error(),
                Some(StoreError::MetadataStateDigestMismatch {
                    node_id: 7,
                    pg_id: 0,
                    ..
                })
            ),
            "bind should fail closed on corrupted metadata state digest: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_store_error_reduces_diagnostics() {
        const SECRET_CONTEXT: &str = "secret storage-node open operation";
        const SECRET_SOURCE: &str = "secret storage-node open source";
        let error = StorageNodeServerError::from(StoreError::Io {
            context: SECRET_CONTEXT,
            source: std::io::Error::other(SECRET_SOURCE),
        });

        assert_eq!(
            error.to_string(),
            "failed to open storage node: storage operation failed"
        );
        let debug = format!("{error:?}");
        for secret in [SECRET_CONTEXT, SECRET_SOURCE] {
            assert!(!debug.contains(secret), "debug leaked {secret}: {debug}");
        }
        assert!(std::error::Error::source(&error).is_none());
        assert!(matches!(
            error.retained_store_error(),
            Some(StoreError::Io { context: SECRET_CONTEXT, source })
                if source.to_string() == SECRET_SOURCE
        ));
    }

    #[test]
    fn storage_node_server_rejects_active_socket_owner() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _active = UnixListener::bind(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_existing_non_socket_path() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        File::create(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_returns_unsupported_operation_error() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let before = observability::metrics_snapshot();

        let mut client = UnixStream::connect(socket_path).unwrap();
        let frame_bytes =
            encode_storage_rpc_frame(9, StorageRpcMessageKind::ClaimHeartbeat, b"").unwrap();
        client.write_all(&frame_bytes).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::UnsupportedOperation);
        let after = observability::metrics_snapshot();
        assert!(after.storage_rpc_error_total > before.storage_rpc_error_total);
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-9" && record.event == "storage_rpc_error"
            })
            .expect("storage-node RPC error should be recorded");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("rpc_kind=ClaimHeartbeat"));
        assert!(record.detail.contains("error_code=UnsupportedOperation"));
        assert!(record.detail.contains("message_len="));
        assert!(record.detail.contains("message_hash="));
    }

    #[test]
    fn storage_node_server_reads_shard_with_expected_ack() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"read payload".to_vec();
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(&payload),
        };
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &payload)
            .unwrap();
        drop(node);
        let request = StorageRpcShardReadRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
            expected_ack,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardRead,
            encode_shard_read_request(&request).unwrap(),
        );
        let mismatched_request = StorageRpcShardReadRequest {
            expected_ack: WriteAck {
                stored_size: expected_ack.stored_size,
                crc64: expected_ack.crc64 ^ 1,
            },
            ..request
        };
        let mismatch = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardRead,
            encode_shard_read_request(&mismatched_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let read_payload = decode_shard_read_response(&response_payload, expected_ack).unwrap();
        assert_eq!(read_payload, payload);
        let error = decode_storage_rpc_response_payload(&mismatch.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ShardIntegrity);
        assert!(error.message.contains("ack mismatch"));
    }

    #[test]
    fn storage_node_server_preserves_missing_historical_shard_error() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let request = StorageRpcHistoricalShardReadRequest {
            location: location.into(),
            shard_key: test_shard_key(0),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardHistoricalRead,
            crate::storage_rpc::encode_historical_shard_read_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::NotFound);
        assert_eq!(error.message, "not found");
    }

    #[test]
    fn storage_node_server_reads_shard_range_with_expected_ack() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"read range payload".to_vec();
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(&payload),
        };
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &payload)
            .unwrap();
        drop(node);
        let request = StorageRpcShardReadRangeRequest {
            location: location.into(),
            shard_key,
            expected_ack,
            offset: 5,
            length: 5,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardReadRange,
            encode_shard_read_range_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let read_payload =
            decode_shard_read_range_response(&response_payload, request.length as usize).unwrap();
        assert_eq!(read_payload, payload[5..10]);
    }

    #[test]
    fn storage_node_server_lists_scavenger_shard_files_over_rpc() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let key = test_shard_key(0);
        let payload = b"remote shard scavenger file";
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            node.get_pg(0).unwrap().write_shard(&key, payload).unwrap();
        }
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcScavengerListFilesRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            data_pg_id: PgId::new(0),
        };
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardScavengerListFiles,
            encode_scavenger_list_files_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let response_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let scan = decode_scavenger_list_files_response(&response_payload).unwrap();
        assert_eq!(scan.files.len(), 1);
        assert_eq!(scan.files[0].key, key);
        assert_eq!(scan.files[0].size, payload.len() as u64);
        assert!(scan.errors.is_empty());
    }

    #[test]
    fn storage_node_server_retries_lost_shard_write_without_overwrite() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"first payload".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            effect_deadline: None,
            payload: payload.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        let first_payload = decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let first_ack = decode_shard_write_ack(
            &first_payload,
            request.expected_size,
            request.expected_crc64,
        )
        .unwrap();
        let retry = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        let retry_payload = decode_storage_rpc_response_payload(&retry.payload)
            .unwrap()
            .unwrap();
        let retry_ack = decode_shard_write_ack(
            &retry_payload,
            request.expected_size,
            request.expected_crc64,
        )
        .unwrap();

        let different = b"different payload".to_vec();
        let different_request = StorageRpcShardWriteRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
            expected_size: different.len() as u64,
            expected_crc64: checksum::crc64::checksum(&different),
            effect_deadline: None,
            payload: different,
        };
        let mismatch = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ShardWrite,
            encode_shard_write_request(&different_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        assert_eq!(retry_ack, first_ack);
        let error = decode_storage_rpc_response_payload(&mismatch.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ShardIntegrity);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(reopened.read_shard_file(0, &shard_key).unwrap(), payload);
    }

    #[test]
    fn storage_node_server_repair_write_replaces_existing_shard() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let corrupt = b"corrupt shard".to_vec();
        let repaired = b"repaired shard".to_vec();
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        node.write_shard_file(location.data_pg_id().get(), &shard_key, &corrupt)
            .unwrap();
        drop(node);

        let request = StorageRpcShardWriteRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
            expected_size: repaired.len() as u64,
            expected_crc64: checksum::crc64::checksum(&repaired),
            effect_deadline: None,
            payload: repaired.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardRepairWrite,
            encode_shard_write_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let ack = decode_shard_write_ack(&payload, request.expected_size, request.expected_crc64)
            .unwrap();
        assert_eq!(ack.stored_size, request.expected_size);
        assert_eq!(ack.crc64, request.expected_crc64);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(reopened.read_shard_file(0, &shard_key).unwrap(), repaired);
    }

    #[test]
    fn storage_node_server_rejects_corrupt_shard_write_without_file() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let payload = b"corrupt-before-write".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            effect_deadline: None,
            payload,
        };
        let mut request_payload = encode_shard_write_request(&request).unwrap();
        let last = request_payload
            .last_mut()
            .expect("test shard write payload must be nonempty");
        *last ^= 0x01;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardWrite,
            request_payload,
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        assert!(error.message.contains("checksum mismatch"));
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_retries_lost_shard_delete_as_terminal_success() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(0);
        let request = StorageRpcShardDeleteRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .write_shard_file_if_absent(0, &shard_key, b"delete me")
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardDelete,
                encode_shard_delete_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_validates_shard_delete_route_before_missing_success() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let requests = [
            (
                test_location(2, 0, 7),
                StorageRpcErrorCode::StaleShardLocation,
            ),
            (test_location(1, 0, 8), StorageRpcErrorCode::UnknownNode),
            (test_location(1, 9, 7), StorageRpcErrorCode::UnknownPg),
        ];
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .write_shard_file_if_absent(0, &shard_key, b"retained delete canary")
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for (index, (location, expected_code)) in requests.into_iter().enumerate() {
            let request = StorageRpcShardDeleteRequest {
                location: location.into(),
                shard_key: shard_key.clone(),
            };
            let response = send_frame(
                &mut client,
                index as u64 + 1,
                StorageRpcMessageKind::ShardDelete,
                encode_shard_delete_request(&request).unwrap(),
            );
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, expected_code);
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            reopened.read_shard_file(0, &shard_key).unwrap(),
            b"retained delete canary"
        );
    }

    #[test]
    fn retained_data_delete_capabilities_bind_admission_route_and_subject() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let shard_key = test_shard_key(0);
        let payload = b"retained data capability payload";
        let ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(payload),
        };
        server
            ._node
            .write_shard_file_if_absent(0, &shard_key, payload)
            .unwrap();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .register_written_shards_batch_exact(&[(&shard_key, ack)])
            .unwrap();

        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let payload_request = StorageRpcShardDeleteRequest {
            location: test_location(1, 0, 7).into(),
            shard_key: shard_key.clone(),
        };
        let mismatched_payload_request = StorageRpcShardDeleteRequest {
            location: test_location_with_shard(1, 0, 7, 1).into(),
            shard_key: shard_key.clone(),
        };
        let ack_request = StorageRpcShardAckItemRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            shard_key: shard_key.clone(),
        };

        for result in [
            handler.retained_shard_payload_delete_route(
                &active_permit,
                &payload_request,
                "test shard payload delete",
            ),
            handler.retained_shard_payload_delete_route(
                &foreign_permit,
                &payload_request,
                "test shard payload delete",
            ),
        ] {
            match result {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                Ok(_) => panic!("invalid admission created a retained payload-delete route"),
            }
        }
        let mismatch = match handler.retained_shard_payload_delete_route(
            &retained_permit,
            &mismatched_payload_request,
            "test shard payload delete",
        ) {
            Err(error) => error,
            Ok(_) => panic!("mismatched shard index created a retained payload-delete route"),
        };
        assert_eq!(mismatch.code, StorageRpcErrorCode::PayloadDecode);
        assert_eq!(
            server._node.read_shard_file(0, &shard_key).unwrap(),
            payload
        );

        for result in [
            handler.retained_shard_ack_delete_route(
                &active_permit,
                &ack_request,
                "test shard ack delete",
            ),
            handler.retained_shard_ack_delete_route(
                &foreign_permit,
                &ack_request,
                "test shard ack delete",
            ),
        ] {
            match result {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                Ok(_) => panic!("invalid admission created a retained ack-delete route"),
            }
        }

        handler
            .retained_shard_payload_delete_route(
                &retained_permit,
                &payload_request,
                "test shard payload delete",
            )
            .unwrap()
            .delete()
            .unwrap();
        handler
            .retained_shard_ack_delete_route(
                &retained_permit,
                &ack_request,
                "test shard ack delete",
            )
            .unwrap()
            .delete()
            .unwrap();
        assert!(matches!(
            server._node.read_shard_file(0, &shard_key),
            Err(StoreError::NotFound)
        ));
        assert!(matches!(
            server._node.get_pg(0).unwrap().stat_shard(&shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn retained_data_inspection_capabilities_bind_exact_route_and_subject() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(4).unwrap();
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        config.historical_pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                state: PgState::Active,
                primary_node_id: config.node_id,
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![config.node_id],
            },
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(8),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![config.node_id, NodeId::new(8)],
            },
        ];
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let shard_key = test_shard_key(0);
        let payload = b"retained historical data inspection";
        let ack = server
            ._node
            .write_shard_file_if_absent(0, &shard_key, payload)
            .unwrap();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .register_written_shards_batch_exact(&[(&shard_key, ack)])
            .unwrap();

        let handler = server.connection_handler();
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let location = test_location(3, 0, config.node_id.as_u32());
        let payload_request = StorageRpcHistoricalShardReadRequest {
            location: location.into(),
            shard_key: shard_key.clone(),
        };
        let ack_request = StorageRpcShardAckItemRequest {
            node_id: config.node_id,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(0),
            shard_key: shard_key.clone(),
        };

        crate::clock::with_time_override(6_000, || {
            assert_eq!(
                handler
                    .retained_shard_inspection_route(
                        &retained_permit,
                        &payload_request,
                        "test historical shard inspection",
                    )
                    .unwrap()
                    .read()
                    .unwrap(),
                payload
            );
            assert_eq!(
                handler
                    .retained_shard_ack_inspection_route(
                        &retained_permit,
                        &ack_request,
                        "test historical shard ack inspection",
                    )
                    .unwrap()
                    .load()
                    .unwrap(),
                ack
            );
        });

        for result in [
            handler.retained_shard_inspection_route(
                &active_permit,
                &payload_request,
                "test historical shard inspection",
            ),
            handler.retained_shard_inspection_route(
                &foreign_permit,
                &payload_request,
                "test historical shard inspection",
            ),
        ] {
            match result {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                Ok(_) => panic!("invalid admission created historical shard authority"),
            }
        }
        for result in [
            handler.retained_shard_ack_inspection_route(
                &active_permit,
                &ack_request,
                "test historical shard ack inspection",
            ),
            handler.retained_shard_ack_inspection_route(
                &foreign_permit,
                &ack_request,
                "test historical shard ack inspection",
            ),
        ] {
            match result {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                Ok(_) => panic!("invalid admission created historical shard-ack authority"),
            }
        }
        let crossed_payload_request = StorageRpcHistoricalShardReadRequest {
            location: test_location_with_shard(3, 0, config.node_id.as_u32(), 1).into(),
            ..payload_request.clone()
        };
        let crossed = match handler.retained_shard_inspection_route(
            &retained_permit,
            &crossed_payload_request,
            "test historical shard inspection",
        ) {
            Err(error) => error,
            Ok(_) => panic!("crossed shard subject created historical inspection authority"),
        };
        assert_eq!(crossed.code, StorageRpcErrorCode::PayloadDecode);

        let unretained_payload_request = StorageRpcHistoricalShardReadRequest {
            location: test_location(2, 0, config.node_id.as_u32()).into(),
            ..payload_request.clone()
        };
        let unretained = match handler.retained_shard_inspection_route(
            &retained_permit,
            &unretained_payload_request,
            "test historical shard inspection",
        ) {
            Err(error) => error,
            Ok(_) => panic!("unretained route created historical shard authority"),
        };
        assert_eq!(unretained.code, StorageRpcErrorCode::StaleShardLocation);
        let unretained_ack_request = StorageRpcShardAckItemRequest {
            cluster_epoch: ClusterEpoch::new(2).unwrap(),
            ..ack_request.clone()
        };
        let unretained_ack = match handler.retained_shard_ack_inspection_route(
            &retained_permit,
            &unretained_ack_request,
            "test historical shard ack inspection",
        ) {
            Err(error) => error,
            Ok(_) => panic!("unretained route created historical shard-ack authority"),
        };
        assert_eq!(unretained_ack.code, StorageRpcErrorCode::StaleShardLocation);

        let wrong_primary_request = StorageRpcShardAckItemRequest {
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            ..ack_request.clone()
        };
        let wrong_primary = match handler.retained_shard_ack_inspection_route(
            &retained_permit,
            &wrong_primary_request,
            "test historical shard ack inspection",
        ) {
            Err(error) => error,
            Ok(_) => panic!("non-primary node created historical ack inspection authority"),
        };
        assert_eq!(wrong_primary.code, StorageRpcErrorCode::NonActingSetAccess);
    }

    #[test]
    fn active_data_capabilities_bind_admission_subject_and_captured_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let shard_key = test_shard_key(0);
        let other_shard_key = test_shard_key(1);
        let location = test_location(1, 0, 7);
        let payload = b"active data capability payload";
        let repaired_payload = b"active data capability repaired payload";
        let repaired_ack = WriteAck {
            stored_size: repaired_payload.len() as u64,
            crc64: checksum::crc64::checksum(repaired_payload),
        };
        let ack_item = StorageRpcShardAckItem {
            shard_key: shard_key.clone(),
            ack: repaired_ack,
        };

        let shard_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_shard_route(
                    &active_permit,
                    location.into(),
                    &shard_key,
                    "test active shard",
                )
                .unwrap()
        });
        let ack_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_primary_data_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test active shard ack",
                )
                .unwrap()
        });
        let data_scan_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_data_scan_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test active shard scan",
                )
                .unwrap()
        });
        crate::clock::with_time_override(1_000, || {
            shard_route
                .write_if_absent(
                    payload,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap();
            assert_eq!(shard_route.read().unwrap(), payload);
            assert_eq!(
                shard_route.repair_write(repaired_payload).unwrap(),
                repaired_ack
            );
            assert_eq!(shard_route.read().unwrap(), repaired_payload);
            ack_route
                .record_shard_acks(std::slice::from_ref(&ack_item))
                .unwrap();
            ack_route
                .validate_shard_acks(std::slice::from_ref(&ack_item))
                .unwrap();
            assert_eq!(ack_route.load_shard_ack(&shard_key).unwrap(), repaired_ack);
            assert_eq!(data_scan_route.list_shard_files().unwrap().files.len(), 1);
            assert_eq!(ack_route.list_scavenger_shard_rows().unwrap().len(), 1);
        });

        crate::clock::with_time_override(1_000, || {
            for result in [
                handler.active_shard_route(
                    &retained_permit,
                    location.into(),
                    &shard_key,
                    "test active shard",
                ),
                handler.active_shard_route(
                    &foreign_permit,
                    location.into(),
                    &shard_key,
                    "test active shard",
                ),
            ] {
                match result {
                    Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                    Ok(_) => panic!("invalid admission created an active shard capability"),
                }
            }
            for permit in [&retained_permit, &foreign_permit] {
                match handler.active_data_scan_route(
                    permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test active shard scan",
                ) {
                    Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                    Ok(_) => panic!("invalid admission created an active data-scan capability"),
                }
            }
            let mismatched_location = test_location_with_shard(1, 0, 7, 1);
            let mismatch = match handler.active_shard_route(
                &active_permit,
                mismatched_location.into(),
                &shard_key,
                "test active shard",
            ) {
                Err(error) => error,
                Ok(_) => panic!("mismatched shard index created an active shard capability"),
            };
            assert_eq!(mismatch.code, StorageRpcErrorCode::PayloadDecode);

            for result in [
                handler.active_primary_data_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test active shard ack",
                ),
                handler.active_primary_data_route(
                    &foreign_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test active shard ack",
                ),
            ] {
                match result {
                    Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                    Ok(_) => {
                        panic!("invalid admission created an active data-primary capability")
                    }
                }
            }
        });

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            fn assert_expired<T>(result: Result<T, StorageNodeDataRouteError>) {
                match result {
                    Err(StorageNodeDataRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeDataRouteError::Store(error)) => {
                        panic!("expired data capability reached node state: {error}")
                    }
                    Ok(_) => panic!("expired data capability reached node state"),
                }
            }

            assert_expired(shard_route.repair_write(b"must not replace"));
            assert_expired(shard_route.read());
            assert_expired(ack_route.record_shard_acks(&[StorageRpcShardAckItem {
                shard_key: other_shard_key.clone(),
                ack: repaired_ack,
            }]));
            assert_expired(ack_route.validate_shard_acks(std::slice::from_ref(&ack_item)));
            assert_expired(ack_route.load_shard_ack(&shard_key));
            assert_expired(ack_route.list_scavenger_shard_rows());
            assert_expired(data_scan_route.list_shard_files());
        });
        assert_eq!(
            server._node.read_shard_file(0, &shard_key).unwrap(),
            repaired_payload
        );
        assert!(matches!(
            server._node.get_pg(0).unwrap().stat_shard(&other_shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn read_handle_capabilities_bind_admission_subject_deadline_and_session() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let foreign_active_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let foreign_retained_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let mut session =
            StorageNodeSession::new(Arc::clone(&server.read_handles), Arc::clone(&server._node));
        let foreign_handles = Arc::new(Mutex::new(StorageNodeReadHandleState::default()));
        let mut foreign_session =
            StorageNodeSession::new(Arc::clone(&foreign_handles), Arc::clone(&server._node));
        let foreign_node =
            Arc::new(SharedStorageNode::topology_only(&[0], config.default_ec_shape).unwrap());
        let mut foreign_node_session =
            StorageNodeSession::new(Arc::clone(&server.read_handles), foreign_node);
        let first_location = test_location(1, 0, config.node_id.as_u32());
        let second_location = test_location_with_shard(1, 0, config.node_id.as_u32(), 1);
        let first_request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "capability-read-a".to_string(),
            locations: vec![first_location.into()],
            shard_keys: vec![test_shard_key(0)],
        };
        let second_request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "capability-read-b".to_string(),
            locations: vec![second_location.into()],
            shard_keys: vec![test_shard_key(1)],
        };

        crate::clock::with_time_override(1_000, || {
            for error in [
                match handler.active_read_handle_acquire_route(
                    &retained_permit,
                    &mut session,
                    &first_request,
                    "test read-handle acquire",
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("retained admission created read-handle acquire authority"),
                },
                match handler.active_read_handle_acquire_route(
                    &foreign_active_permit,
                    &mut session,
                    &first_request,
                    "test read-handle acquire",
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("foreign admission created read-handle acquire authority"),
                },
                match handler.active_read_handle_acquire_route(
                    &active_permit,
                    &mut foreign_session,
                    &first_request,
                    "test read-handle acquire",
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("foreign registry created read-handle acquire authority"),
                },
                match handler.active_read_handle_acquire_route(
                    &active_permit,
                    &mut foreign_node_session,
                    &first_request,
                    "test read-handle acquire",
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("foreign node created read-handle acquire authority"),
                },
            ] {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
            }
            let crossed_request = StorageRpcReadHandleAcquireRequest {
                shard_keys: vec![test_shard_key(1)],
                ..first_request.clone()
            };
            let crossed = match handler.active_read_handle_acquire_route(
                &active_permit,
                &mut session,
                &crossed_request,
                "test read-handle acquire",
            ) {
                Err(error) => error,
                Ok(_) => panic!("crossed shard subject created read-handle acquire authority"),
            };
            assert_eq!(crossed.code, StorageRpcErrorCode::PayloadDecode);
            let malformed_request = StorageRpcReadHandleAcquireRequest {
                read_operation_id: String::new(),
                ..first_request.clone()
            };
            let malformed = match handler.active_read_handle_acquire_route(
                &active_permit,
                &mut session,
                &malformed_request,
                "test read-handle acquire",
            ) {
                Err(error) => error,
                Ok(_) => panic!("malformed subject created read-handle acquire authority"),
            };
            assert_eq!(malformed.code, StorageRpcErrorCode::PayloadDecode);

            handler
                .active_read_handle_acquire_route(
                    &active_permit,
                    &mut session,
                    &first_request,
                    "test read-handle acquire",
                )
                .unwrap()
                .acquire()
                .unwrap();
            handler
                .active_read_handle_acquire_route(
                    &active_permit,
                    &mut session,
                    &second_request,
                    "test read-handle acquire",
                )
                .unwrap()
                .acquire()
                .unwrap();
        });
        assert_eq!(server.read_handle_count(first_location), 1);
        assert_eq!(server.read_handle_count(second_location), 1);

        let expiring_request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "capability-read-expired".to_string(),
            ..first_request.clone()
        };
        let expiring_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_read_handle_acquire_route(
                    &active_permit,
                    &mut session,
                    &expiring_request,
                    "test read-handle acquire",
                )
                .unwrap()
        });
        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            let error = expiring_route.acquire().unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        });
        assert_eq!(server.read_handle_count(first_location), 1);

        let first_release = StorageRpcReadHandleReleaseRequest {
            read_operation_id: first_request.read_operation_id.clone(),
        };
        for error in [
            match handler.retained_read_handle_release_route(
                &active_permit,
                &mut session,
                &first_release,
                "test read-handle release",
            ) {
                Err(error) => error,
                Ok(_) => panic!("active admission created read-handle release authority"),
            },
            match handler.retained_read_handle_release_route(
                &foreign_retained_permit,
                &mut session,
                &first_release,
                "test read-handle release",
            ) {
                Err(error) => error,
                Ok(_) => panic!("foreign admission created read-handle release authority"),
            },
            match handler.retained_read_handle_release_route(
                &retained_permit,
                &mut foreign_session,
                &first_release,
                "test read-handle release",
            ) {
                Err(error) => error,
                Ok(_) => panic!("foreign registry created read-handle release authority"),
            },
            match handler.retained_read_handle_release_route(
                &retained_permit,
                &mut foreign_node_session,
                &first_release,
                "test read-handle release",
            ) {
                Err(error) => error,
                Ok(_) => panic!("foreign node created read-handle release authority"),
            },
        ] {
            assert_eq!(error.code, StorageRpcErrorCode::Internal);
        }
        let malformed_release = StorageRpcReadHandleReleaseRequest {
            read_operation_id: String::new(),
        };
        let malformed = match handler.retained_read_handle_release_route(
            &retained_permit,
            &mut session,
            &malformed_release,
            "test read-handle release",
        ) {
            Err(error) => error,
            Ok(_) => panic!("malformed subject created read-handle release authority"),
        };
        assert_eq!(malformed.code, StorageRpcErrorCode::PayloadDecode);
        crate::clock::with_time_override(6_000, || {
            handler
                .retained_read_handle_release_route(
                    &retained_permit,
                    &mut session,
                    &first_release,
                    "test read-handle release",
                )
                .unwrap()
                .release()
                .unwrap();
        });
        assert_eq!(server.read_handle_count(first_location), 0);
        assert_eq!(server.read_handle_count(second_location), 1);

        let second_release = StorageRpcReadHandleReleaseRequest {
            read_operation_id: second_request.read_operation_id,
        };
        handler
            .retained_read_handle_release_route(
                &retained_permit,
                &mut session,
                &second_release,
                "test read-handle release",
            )
            .unwrap()
            .release()
            .unwrap();
        assert_eq!(server.read_handle_count(second_location), 0);
        assert_eq!(foreign_handles.lock().unwrap().live_read_operations, 0);
    }

    #[test]
    fn object_payload_lease_capabilities_separate_active_and_retained_controls() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let foreign_active = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let bucket = BucketName::new("lease-capability").unwrap();
        let key = ObjectKey::new("source").unwrap();
        let generation_id = GenerationId::new(1).unwrap();
        let active_request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: config.node_id,
            route_cluster_epoch: config.cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            operation: StorageRpcObjectPayloadLeaseControlOperation::Acquire,
            reclaim_authority: None,
        };
        let retained_request = StorageRpcObjectPayloadLeaseControlRequest {
            operation: StorageRpcObjectPayloadLeaseControlOperation::Release,
            ..active_request.clone()
        };
        let reclaim_request = StorageRpcObjectPayloadLeaseControlRequest {
            operation: StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin,
            reclaim_authority: Some(crate::metadata_command::ObjectPayloadReclaimClaimProof {
                bucket_incarnation_generation: 1,
                reclaim_kind: crate::ObjectPayloadReclaimKind::ObjectSegments,
                claim_id: "claim".to_string(),
                owner_token: "owner".to_string(),
                cluster_epoch: config.cluster_epoch,
            }),
            ..active_request.clone()
        };

        handler
            .active_object_payload_lease_control(&active_permit, &active_request)
            .unwrap()
            .require_valid_now()
            .unwrap();
        handler
            .active_object_payload_lease_control(&active_permit, &reclaim_request)
            .unwrap()
            .require_valid_now()
            .unwrap();
        handler
            .retained_object_payload_lease_control(&retained_permit, &retained_request)
            .unwrap()
            .require_valid_now()
            .unwrap();

        for result in [
            handler.active_object_payload_lease_control(&retained_permit, &active_request),
            handler.active_object_payload_lease_control(&foreign_active, &active_request),
        ] {
            match result {
                Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
                Ok(_) => panic!("invalid admission created an active lease capability"),
            }
        }
        match handler.retained_object_payload_lease_control(&active_permit, &retained_request) {
            Err(error) => assert_eq!(error.code, StorageRpcErrorCode::Internal),
            Ok(_) => panic!("active admission created a retained lease capability"),
        }
        let missing_authority = StorageRpcObjectPayloadLeaseControlRequest {
            reclaim_authority: None,
            ..reclaim_request
        };
        match handler.active_object_payload_lease_control(&active_permit, &missing_authority) {
            Err(error) => assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode),
            Ok(_) => panic!("reclaim begin without authority created a capability"),
        }
    }

    #[test]
    fn storage_node_server_retries_lost_shard_ack_record_exactly() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardAckRecord,
                encode_shard_ack_batch_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        let validate = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ShardAckValidate,
            encode_shard_ack_batch_request(&request).unwrap(),
        );
        decode_storage_rpc_response_payload(&validate.payload)
            .unwrap()
            .unwrap();

        let mismatch = StorageRpcShardAckBatchRequest {
            items: vec![StorageRpcShardAckItem {
                shard_key,
                ack: WriteAck {
                    stored_size: 13,
                    crc64: 0x1234,
                },
            }],
            ..request
        };
        let mismatch_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::ShardAckRecord,
            encode_shard_ack_batch_request(&mismatch).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&mismatch_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ShardIntegrity);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&mismatch.items[0].shard_key, ack)
            .unwrap();
    }

    #[test]
    fn storage_node_server_loads_and_retries_lost_shard_ack_delete() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let record = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let item = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            shard_key: shard_key.clone(),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let record_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardAckRecord,
            encode_shard_ack_batch_request(&record).unwrap(),
        );
        decode_storage_rpc_response_payload(&record_response.payload)
            .unwrap()
            .unwrap();

        let load_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardAckLoad,
            encode_shard_ack_item_request(&item),
        );
        let load_payload = decode_storage_rpc_response_payload(&load_response.payload)
            .unwrap()
            .unwrap();
        let loaded = decode_shard_ack_item_response(&load_payload).unwrap();
        assert_eq!(loaded.shard_key, shard_key);
        assert_eq!(loaded.ack, ack);

        for request_id in [3, 4] {
            let delete_response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::ShardAckDelete,
                encode_shard_ack_item_request(&item),
            );
            decode_storage_rpc_response_payload(&delete_response.payload)
                .unwrap()
                .unwrap();
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        assert!(matches!(
            pg.stat_shard(&shard_key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn storage_node_server_shard_ack_metadata_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.node_id = NodeId::new(8);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        config.pg_routes[0].primary_node_id = NodeId::new(7);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        config.historical_pg_routes = vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: PgState::Active,
            primary_node_id: NodeId::new(8),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7), NodeId::new(8)],
        }];
        private_socket_dir(config.socket_path.parent().unwrap());
        let existing_key = test_shard_key(0);
        let new_key = test_shard_key(1);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.register_written_shards_batch_exact(&[(&existing_key, ack)])
                .unwrap();
        }
        let record = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(8),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: new_key.clone(),
                ack,
            }],
        };
        let existing_record = StorageRpcShardAckBatchRequest {
            items: vec![StorageRpcShardAckItem {
                shard_key: existing_key.clone(),
                ack,
            }],
            ..record.clone()
        };
        let existing_item = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(8),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            shard_key: existing_key.clone(),
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let requests = [
            (
                StorageRpcMessageKind::ShardAckRecord,
                encode_shard_ack_batch_request(&record).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardAckValidate,
                encode_shard_ack_batch_request(&existing_record).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardAckLoad,
                encode_shard_ack_item_request(&existing_item),
            ),
            (
                StorageRpcMessageKind::ShardAckDelete,
                encode_shard_ack_item_request(&existing_item),
            ),
        ];
        for (index, (kind, payload)) in requests.into_iter().enumerate() {
            let response = send_frame(&mut client, index as u64 + 1, kind, payload);
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
        }
        let historical_load = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::ShardAckHistoricalLoad,
            encode_shard_ack_item_request(&StorageRpcShardAckItemRequest {
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                ..existing_item.clone()
            }),
        );
        let historical_payload = decode_storage_rpc_response_payload(&historical_load.payload)
            .unwrap()
            .unwrap();
        let historical_item = decode_shard_ack_item_response(&historical_payload).unwrap();
        assert_eq!(historical_item.shard_key, existing_key);
        assert_eq!(historical_item.ack, ack);
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&existing_key, ack).unwrap();
        assert!(matches!(pg.stat_shard(&new_key), Err(StoreError::NotFound)));
    }
