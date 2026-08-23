// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

    #[test]
    fn storage_node_server_rejects_unknown_data_pg_for_every_shard_ack_operation() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let record_key = test_shard_key(0);
        let canary_key = test_shard_key(1);
        let record_ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let canary_ack = WriteAck {
            stored_size: 34,
            crc64: 0x5678,
        };
        let batch = StorageRpcShardAckBatchRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(9),
            items: vec![StorageRpcShardAckItem {
                shard_key: record_key.clone(),
                ack: record_ack,
            }],
        };
        let unknown_record_item = StorageRpcShardAckItemRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(9),
            shard_key: record_key.clone(),
        };
        let unknown_canary_item = StorageRpcShardAckItemRequest {
            shard_key: canary_key.clone(),
            ..unknown_record_item.clone()
        };
        let configured_record_item = StorageRpcShardAckItemRequest {
            pg_id: PgId::new(0),
            ..unknown_record_item.clone()
        };
        let configured_canary_item = StorageRpcShardAckItemRequest {
            pg_id: PgId::new(0),
            shard_key: canary_key.clone(),
            ..unknown_record_item.clone()
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .register_written_shards_batch_exact(&[(&canary_key, canary_ack)])
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let record_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardAckRecord,
            encode_shard_ack_batch_request(&batch).unwrap(),
        );
        let record_error = decode_storage_rpc_response_payload(&record_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(record_error.code, StorageRpcErrorCode::UnknownPg);

        let absent_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::ShardAckLoad,
            encode_shard_ack_item_request(&configured_record_item),
        );
        let absent_error = decode_storage_rpc_response_payload(&absent_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(absent_error.code, StorageRpcErrorCode::NotFound);

        let canary_after_record = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ShardAckLoad,
            encode_shard_ack_item_request(&configured_canary_item),
        );
        let canary_after_record = decode_storage_rpc_response_payload(&canary_after_record.payload)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_shard_ack_item_response(&canary_after_record).unwrap(),
            StorageRpcShardAckItem {
                shard_key: canary_key.clone(),
                ack: canary_ack,
            }
        );

        let read_only_requests = [
            (
                StorageRpcMessageKind::ShardAckValidate,
                encode_shard_ack_batch_request(&batch).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardAckLoad,
                encode_shard_ack_item_request(&unknown_record_item),
            ),
            (
                StorageRpcMessageKind::ShardAckHistoricalLoad,
                encode_shard_ack_item_request(&unknown_record_item),
            ),
        ];
        for (index, (kind, payload)) in read_only_requests.into_iter().enumerate() {
            let response = send_frame(&mut client, index as u64 + 4, kind, payload);
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
        }

        let delete_response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ShardAckDelete,
            encode_shard_ack_item_request(&unknown_canary_item),
        );
        let delete_error = decode_storage_rpc_response_payload(&delete_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(delete_error.code, StorageRpcErrorCode::UnknownPg);

        let canary_after_delete = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ShardAckLoad,
            encode_shard_ack_item_request(&configured_canary_item),
        );
        let canary_after_delete = decode_storage_rpc_response_payload(&canary_after_delete.payload)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_shard_ack_item_response(&canary_after_delete).unwrap(),
            StorageRpcShardAckItem {
                shard_key: canary_key.clone(),
                ack: canary_ack,
            }
        );
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
            pg.stat_shard(&record_key),
            Err(StoreError::NotFound)
        ));
        pg.validate_written_shard_ack(&canary_key, canary_ack)
            .unwrap();
    }

    #[test]
    fn storage_node_server_repair_and_backfill_rpcs_validate_data_pg_before_rows() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![test_route(0), test_route(1)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let route = StorageRpcBucketPgRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(1),
        };
        let unknown_route = StorageRpcBucketPgRequest {
            pg_id: PgId::new(9),
            ..route.clone()
        };
        let request = SegmentStoredBytesRequest {
            data_pg_id: 0,
            segment_okh: [0xAC; 16],
            segment_vid: GenerationId::new(42).unwrap(),
            stored_size: 1024,
            segment_crc64: 0x1234,
            ec: EcShape { k: 4, m: 2 },
        };
        let repair_work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index: ShardIndex::new(5),
        };
        let backfill_work_item = PlacedSegmentShardBackfillWorkItem {
            request,
            source_cluster_epoch: ClusterEpoch::new(1).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
        };
        let repair_acquire = PlacedSegmentShardRepairClaimAcquire {
            claim_id: "repair-canary-claim".to_string(),
            owner_token: "repair-canary-owner".to_string(),
            cluster_epoch: config.cluster_epoch,
            claimed_at: 10,
            lease_deadline: 20,
            now: 10,
        };
        let backfill_acquire = PlacedSegmentShardBackfillClaimAcquire {
            claim_id: "backfill-canary-claim".to_string(),
            owner_token: "backfill-canary-owner".to_string(),
            cluster_epoch: config.cluster_epoch,
            claimed_at: 10,
            lease_deadline: 20,
            now: 10,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let (
            repair_records_before,
            repair_claim_before,
            backfill_records_before,
            backfill_claim_before,
        ) = {
            let pg = server._node.get_pg(0).unwrap();
            pg.record_placed_segment_shard_repair(&repair_work_item, None)
                .unwrap();
            let repair_claim = pg
                .acquire_placed_segment_shard_repair_claim(&repair_acquire)
                .unwrap()
                .unwrap();
            pg.record_placed_segment_shard_backfill(
                &backfill_work_item,
                backfill_work_item.request.ec.m,
                None,
            )
            .unwrap();
            let backfill_claim = pg
                .acquire_placed_segment_shard_backfill_claim(&backfill_acquire)
                .unwrap()
                .unwrap();
            (
                pg.list_placed_segment_shard_repairs().unwrap(),
                repair_claim,
                pg.list_placed_segment_shard_backfills().unwrap(),
                backfill_claim,
            )
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let mut client = UnixStream::connect(socket_path).unwrap();

        let mismatched_requests = [
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairRecord,
                encode_placed_segment_shard_repair_record_request(
                    &StorageRpcPlacedSegmentShardRepairRecordRequest {
                        route: route.clone(),
                        work_item: repair_work_item,
                        last_error: None,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairResolve,
                encode_placed_segment_shard_repair_item_request(
                    &StorageRpcPlacedSegmentShardRepairItemRequest {
                        route: route.clone(),
                        work_item: repair_work_item,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete,
                encode_placed_segment_shard_repair_claim_record_request(
                    &StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
                        route: route.clone(),
                        claim: repair_claim_before.clone(),
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairClaimError,
                encode_placed_segment_shard_repair_claim_error_request(
                    &StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
                        route: route.clone(),
                        claim: repair_claim_before.clone(),
                        last_error: "must not persist".to_string(),
                        next_attempt_after: 30,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillRecord,
                encode_placed_segment_shard_backfill_record_request(
                    &StorageRpcPlacedSegmentShardBackfillRecordRequest {
                        route: route.clone(),
                        work_item: backfill_work_item,
                        remaining_tolerance: backfill_work_item.request.ec.m,
                        last_error: None,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillExists,
                encode_placed_segment_shard_backfill_item_request(
                    &StorageRpcPlacedSegmentShardBackfillItemRequest {
                        route: route.clone(),
                        work_item: backfill_work_item,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillResolve,
                encode_placed_segment_shard_backfill_item_request(
                    &StorageRpcPlacedSegmentShardBackfillItemRequest {
                        route: route.clone(),
                        work_item: backfill_work_item,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete,
                encode_placed_segment_shard_backfill_claim_record_request(
                    &StorageRpcPlacedSegmentShardBackfillClaimRecordRequest {
                        route: route.clone(),
                        claim: backfill_claim_before.clone(),
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError,
                encode_placed_segment_shard_backfill_claim_error_request(
                    &StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
                        route,
                        claim: backfill_claim_before.clone(),
                        last_error: "must not persist".to_string(),
                        next_attempt_after: 30,
                    },
                )
                .unwrap(),
            ),
        ];
        let mut request_id = 1u64;
        for (kind, payload) in mismatched_requests {
            let response = send_frame(&mut client, request_id, kind, payload);
            request_id += 1;
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        }

        let unknown_requests = [
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairs,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire,
                encode_placed_segment_shard_repair_claim_acquire_request(
                    &StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
                        route: unknown_route.clone(),
                        claim_id: "unknown-repair-claim".to_string(),
                        owner_token: "unknown-repair-owner".to_string(),
                        claimed_at: 10,
                        lease_deadline: Some(20),
                        now: 10,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfills,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillCount,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire,
                encode_placed_segment_shard_backfill_claim_acquire_request(
                    &StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
                        route: unknown_route,
                        claim_id: "unknown-backfill-claim".to_string(),
                        owner_token: "unknown-backfill-owner".to_string(),
                        claimed_at: 10,
                        lease_deadline: Some(20),
                        now: 10,
                    },
                )
                .unwrap(),
            ),
        ];
        for (kind, payload) in unknown_requests {
            let response = send_frame(&mut client, request_id, kind, payload);
            request_id += 1;
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg0 = reopened.get_pg(0).unwrap();
        let repairs = pg0.list_placed_segment_shard_repairs().unwrap();
        assert_eq!(repairs, repair_records_before);
        let repair_claim_after = pg0
            .acquire_placed_segment_shard_repair_claim(&repair_acquire)
            .unwrap()
            .unwrap();
        assert_eq!(repair_claim_after, repair_claim_before);
        let backfills = pg0.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills, backfill_records_before);
        let backfill_claim_after = pg0
            .acquire_placed_segment_shard_backfill_claim(&backfill_acquire)
            .unwrap()
            .unwrap();
        assert_eq!(backfill_claim_after, backfill_claim_before);
        let pg1 = reopened.get_pg(1).unwrap();
        assert!(pg1.list_placed_segment_shard_repairs().unwrap().is_empty());
        assert!(pg1
            .list_placed_segment_shard_backfills()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn storage_node_server_shard_scavenger_rpcs_validate_roles_before_rows() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![test_route(0), test_route(1)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let pg0_key = test_shard_key(0);
        let pg0_observation = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: config.node_id.as_u32(),
                data_pg_id: 0,
                shard_index: pg0_key.shard_index(),
                shard_key: pg0_key,
            },
            data_size: Some(12),
            crc64: Some(0x1234),
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
            last_error: None,
        };
        let pg1_key = test_shard_key(1);
        let pg1_observation = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: config.node_id.as_u32(),
                data_pg_id: 1,
                shard_index: pg1_key.shard_index(),
                shard_key: pg1_key,
            },
            data_size: None,
            crc64: None,
            file_exists: false,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: Some("pg 1 canary".to_string()),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let (pg0_before, pg1_before) = {
            let pg0 = server._node.get_pg(0).unwrap();
            pg0.record_shard_scavenger_observation(&pg0_observation)
                .unwrap();
            let pg0_before = pg0.list_shard_scavenger_observations().unwrap();
            let pg1 = server._node.get_pg(1).unwrap();
            pg1.record_shard_scavenger_observation(&pg1_observation)
                .unwrap();
            let pg1_before = pg1.list_shard_scavenger_observations().unwrap();
            (pg0_before, pg1_before)
        };
        let pg1_route = StorageRpcBucketPgRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(1),
        };
        let mut changed_pg0_observation = pg0_observation.clone();
        changed_pg0_observation.last_error = Some("must not persist".to_string());
        let unknown_route = StorageRpcBucketPgRequest {
            pg_id: PgId::new(9),
            ..pg1_route.clone()
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let mut client = UnixStream::connect(socket_path).unwrap();

        let mismatched_requests = [
            (
                StorageRpcMessageKind::ShardScavengerObservationRecord,
                encode_scavenger_observation_record_request(
                    &StorageRpcScavengerObservationRecordRequest {
                        route: pg1_route.clone(),
                        observation: changed_pg0_observation,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationResolve,
                encode_scavenger_observation_key_request(
                    &StorageRpcScavengerObservationKeyRequest {
                        route: pg1_route,
                        key: pg0_observation.key.clone(),
                    },
                )
                .unwrap(),
            ),
        ];
        let mut request_id = 1u64;
        for (kind, payload) in mismatched_requests {
            let response = send_frame(&mut client, request_id, kind, payload);
            request_id += 1;
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        }

        let unknown_requests = [
            (
                StorageRpcMessageKind::ShardScavengerListFiles,
                encode_scavenger_list_files_request(&StorageRpcScavengerListFilesRequest {
                    node_id: config.node_id,
                    cluster_epoch: config.cluster_epoch,
                    data_pg_id: PgId::new(9),
                }),
            ),
            (
                StorageRpcMessageKind::ShardScavengerShardRows,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerPayloadReferences,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentBackfillReferencePage,
                encode_placed_segment_backfill_reference_page_request(
                    &StorageRpcPlacedSegmentBackfillReferencePageRequest {
                        route: unknown_route.clone(),
                        after: None,
                        limit: std::num::NonZeroU16::new(1).unwrap(),
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservations,
                encode_bucket_pg_request(&unknown_route).unwrap(),
            ),
        ];
        for (kind, payload) in unknown_requests {
            let response = send_frame(&mut client, request_id, kind, payload);
            request_id += 1;
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
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
            reopened
                .get_pg(0)
                .unwrap()
                .list_shard_scavenger_observations()
                .unwrap(),
            pg0_before
        );
        assert_eq!(
            reopened
                .get_pg(1)
                .unwrap()
                .list_shard_scavenger_observations()
                .unwrap(),
            pg1_before
        );
    }

    #[test]
    fn storage_node_server_shard_scavenger_metadata_requires_pg_primary() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.node_id = NodeId::new(8);
        config.pg_routes[0].primary_node_id = NodeId::new(7);
        config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(8),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };
        let observation_key = ShardScavengerObservationKey {
            node_id: 8,
            data_pg_id: 0,
            shard_index: shard_key.shard_index(),
            shard_key,
        };
        let observation = ShardScavengerObservationRecord {
            key: observation_key.clone(),
            data_size: Some(12),
            crc64: Some(0x1234),
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
            last_error: None,
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let requests = [
            (
                StorageRpcMessageKind::ShardScavengerShardRows,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerPayloadReferences,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::PlacedSegmentBackfillReferencePage,
                encode_placed_segment_backfill_reference_page_request(
                    &StorageRpcPlacedSegmentBackfillReferencePageRequest {
                        route: route.clone(),
                        after: None,
                        limit: std::num::NonZeroU16::new(1).unwrap(),
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationRecord,
                encode_scavenger_observation_record_request(
                    &StorageRpcScavengerObservationRecordRequest {
                        route: route.clone(),
                        observation,
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservations,
                encode_bucket_pg_request(&route).unwrap(),
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationResolve,
                encode_scavenger_observation_key_request(
                    &StorageRpcScavengerObservationKeyRequest {
                        route,
                        key: observation_key,
                    },
                )
                .unwrap(),
            ),
        ];
        for (index, (kind, payload)) in requests.into_iter().enumerate() {
            let response = send_frame(&mut client, index as u64 + 1, kind, payload);
            let error = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
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
        assert!(pg.list_shard_scavenger_observations().unwrap().is_empty());
    }

    #[test]
    fn storage_node_server_rejects_stale_shard_ack_validate_before_rows() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let shard_key = test_shard_key(0);
        let ack = WriteAck {
            stored_size: 12,
            crc64: 0x1234,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        pg.register_written_shards_batch_exact(&[(&shard_key, ack)])
            .unwrap();
        drop(pg);
        let stale_request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            items: vec![StorageRpcShardAckItem {
                shard_key: shard_key.clone(),
                ack,
            }],
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::ShardAckValidate,
            encode_shard_ack_batch_request(&stale_request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&shard_key, ack).unwrap();
    }

    #[test]
    fn storage_node_server_returns_metadata_command_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_command_state_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state, expected);
    }

    #[test]
    fn storage_node_server_compacts_metadata_command_log() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        pg.record_current_metadata_command_checkpoint(7, config.cluster_epoch)
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandLogCompact,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded =
            crate::storage_rpc::decode_metadata_command_log_compact_response(&payload).unwrap();
        assert_eq!(
            decoded.status,
            crate::node_runtime::pg_store::MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 1,
                compacted_before: 2,
            }
        );
    }

    #[test]
    fn storage_node_server_rejects_peering_metadata_command_log_compaction() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandLogCompact,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_log_hash_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        let expected = pg
            .retained_metadata_command_log_hashes(
                7,
                config.cluster_epoch,
                MetadataCommandLogIndex::new(1).unwrap(),
                MetadataCommandLogIndex::new(1).unwrap(),
            )
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            encode_metadata_command_log_hash_range_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_log_hash_range_response(&payload).unwrap();
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_log_entry_inspection() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let pg = server._node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        let expected = pg
            .retained_metadata_command_log_entries(
                7,
                config.cluster_epoch,
                MetadataCommandLogIndex::new(1).unwrap(),
                MetadataCommandLogIndex::new(1).unwrap(),
            )
            .unwrap();
        drop(pg);

        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            encode_metadata_command_log_hash_range_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded =
            crate::storage_rpc::decode_metadata_command_log_entry_range_response(
                &payload,
                &metadata_command_decode_authority_for_test(),
            )
            .unwrap();
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn storage_node_server_allows_peering_replay_apply() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert!(matches!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    applied_log_index: 1,
                    ..
                }
            )
        ));
    }

    #[test]
    fn metadata_command_apply_encodes_bound_stream_terminal_outcomes_as_no_such_upload() {
        let session_id = SessionId::try_from("16".repeat(16)).unwrap();
        let upload_id = UploadId::for_test("server-missing-append-upload");
        let bucket = crate::tests::bucket_name("server-append-rpc-bucket");
        let key = crate::tests::object_key("server-append-rpc-key");
        let append_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::AppendStreamSegment(Box::new(
                crate::metadata_command::AppendStreamSegmentCommand {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: StreamUploadTarget::UploadPart {
                        upload_id: upload_id.clone(),
                        part_number: 1,
                    },
                    segment: StreamUploadSegmentRecord {
                        session_id: session_id.clone(),
                        segment_index: 0,
                        size: 1,
                        segment_crc64: 2,
                        payload_crc64: 3,
                        segment_okh: [4; 16],
                        segment_vid: GenerationId::MIN,
                        data_pg_id: 0,
                        placement_cluster_epoch: ClusterEpoch::new(1).unwrap(),
                        ec_k: 1,
                        ec_m: 0,
                    },
                },
            )),
        );
        let create_session_id = SessionId::try_from("17".repeat(16)).unwrap();
        let create_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    CreateStreamUploadReq {
                        session_id: create_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::UploadPart {
                            upload_id: upload_id.clone(),
                            part_number: 1,
                        },
                        encryption: crate::ObjectEncryption::None,
                    },
                    1,
                    test_bucket_write_reservation_proof(bucket.clone(), &key),
                ),
            )),
        );
        let upload = crate::MultipartUploadRecord {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: 1,
            state: crate::UploadState::InProgress,
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: crate::OwnerIdentity::from_principal("owner"),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_generation_id: GenerationId::new(1).unwrap(),
            initiated_object_identity: None,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        let commit_session_id = SessionId::try_from("18".repeat(16)).unwrap();
        let commit_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::CommitStreamPart(Box::new(
                crate::metadata_command::CommitStreamPartCommand {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    session_id: commit_session_id.clone(),
                    upload,
                    part: crate::MultipartPartRecord {
                        upload_id: upload_id.clone(),
                        part_number: 1,
                        generation: 0,
                        size: 0,
                        payload_crc64: 0,
                        etag: vec![0; 8],
                        etag_kind: crate::EtagKind::Crc64,
                        part_vid: GenerationId::new(2).unwrap(),
                        placement_cluster_epoch: ClusterEpoch::new(1).unwrap(),
                        ec_k: 1,
                        ec_m: 0,
                        last_modified: 1,
                        checksum: None,
                    },
                    segments: Vec::new(),
                    existing_part: None,
                    displaced_segments: Vec::new(),
                    bucket_write_reservation: test_bucket_write_reservation_proof(
                        bucket.clone(),
                        &key,
                    ),
                },
            )),
        );

        for (command, expected_session_id) in [
            (append_command.clone(), session_id.clone()),
            (create_command.clone(), create_session_id.clone()),
            (commit_command.clone(), commit_session_id.clone()),
        ] {
            let response = metadata_command_state_result_response(
                &command,
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.as_str().to_string(),
                    },
                )),
            )
            .unwrap();
            let payload = decode_storage_rpc_response_payload(&response)
                .unwrap()
                .unwrap();
            let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
            assert_eq!(
                decoded.outcome,
                StorageRpcMetadataCommandStateOutcome::StreamUploadNoSuchUpload {
                    session_id: expected_session_id,
                    upload_id: upload_id.clone(),
                }
            );
        }

        for (command, expected_session_id) in [
            (&append_command, &session_id),
            (&commit_command, &commit_session_id),
        ] {
            for terminal_error in [
                MetadataError::StreamSessionNotFound {
                    session_id: expected_session_id.as_str().to_string(),
                },
                MetadataError::StreamSessionNotInProgress { state: 2 },
            ] {
                let response = metadata_command_state_result_response(
                    command,
                    Err(BucketSnapshotLoadError::Metadata(terminal_error)),
                )
                .unwrap();
                let payload = decode_storage_rpc_response_payload(&response)
                    .unwrap()
                    .unwrap();
                let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
                assert_eq!(
                    decoded.outcome,
                    StorageRpcMetadataCommandStateOutcome::StreamUploadNoSuchUpload {
                        session_id: expected_session_id.clone(),
                        upload_id: upload_id.clone(),
                    }
                );
            }
        }

        let response = metadata_command_state_result_response(
            &append_command,
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::StreamSessionNotFound {
                    session_id: "different-server-session".to_string(),
                },
            )),
        )
        .unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&response)
                .unwrap()
                .unwrap_err()
                .code,
            StorageRpcErrorCode::Internal
        );

        let put_object_command = match append_command.payload() {
            MetadataCommandPayload::AppendStreamSegment(append) => MetadataCommandEnvelope::new(
                append_command.id(),
                MetadataCommandPayload::AppendStreamSegment(Box::new(
                    crate::metadata_command::AppendStreamSegmentCommand {
                        target: StreamUploadTarget::PutObject,
                        ..append.as_ref().clone()
                    },
                )),
            ),
            _ => unreachable!(),
        };
        for terminal_error in [
            MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_string(),
            },
            MetadataError::StreamSessionNotInProgress { state: 2 },
        ] {
            let response = metadata_command_state_result_response(
                &put_object_command,
                Err(BucketSnapshotLoadError::Metadata(terminal_error)),
            )
            .unwrap();
            assert_eq!(
                decode_storage_rpc_response_payload(&response)
                    .unwrap()
                    .unwrap_err()
                    .code,
                StorageRpcErrorCode::Internal
            );
        }

        for terminal_error in [
            MetadataError::StreamSessionNotFound {
                session_id: create_session_id.as_str().to_string(),
            },
            MetadataError::StreamSessionNotInProgress { state: 2 },
        ] {
            let response = metadata_command_state_result_response(
                &create_command,
                Err(BucketSnapshotLoadError::Metadata(terminal_error)),
            )
            .unwrap();
            assert_eq!(
                decode_storage_rpc_response_payload(&response)
                    .unwrap()
                    .unwrap_err()
                    .code,
                StorageRpcErrorCode::Internal
            );
        }

        let response = metadata_command_state_result_response(
            &create_command,
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::NoSuchUpload {
                    upload_id: "different-server-upload".to_string(),
                },
            )),
        )
        .unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&response)
                .unwrap()
                .unwrap_err()
                .code,
            StorageRpcErrorCode::Internal
        );
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_destination_checks() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize,
            encode_metadata_command_state_request(&request),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = crate::storage_rpc::decode_metadata_command_bool_response(&payload).unwrap();
        assert!(decoded.value);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_empty_state_initialize() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .state_digest;
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize,
            encode_metadata_command_transfer_empty_state_request(
                &StorageRpcMetadataCommandTransferEmptyStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_eq!(decoded.state.applied_log_hash, 0);
        assert_eq!(decoded.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_matching_state_initialize() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command)
                .unwrap()
                .state_digest
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize,
            encode_metadata_command_transfer_matching_state_request(
                &StorageRpcMetadataCommandTransferMatchingStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    applied_log_index: 0,
                    applied_log_hash: crate::control_plane::MetadataCommandLogHash::genesis(),
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_eq!(decoded.state.applied_log_hash, 0);
        assert_eq!(decoded.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_checkpoint_base_install() {
        let (bucket, checkpoint) = test_metadata_checkpoint_with_bucket("metadata-rpc-checkpoint");

        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        assert_eq!(decoded.state.applied_log_index, 0);
        assert_ne!(decoded.state.state_digest, 0);

        let destination_node =
            crate::node::SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();
        let destination_pg = destination_node.get_pg(0).unwrap();
        let loaded = destination_pg.head_bucket(&bucket).unwrap();
        assert_eq!(loaded.name, bucket);
        let lifecycle = destination_pg
            .get_bucket_subresource(&bucket, crate::BucketSubresourceKind::Lifecycle)
            .unwrap()
            .unwrap();
        assert_eq!(lifecycle.body, "<LifecycleConfiguration/>");
    }

    #[test]
    fn storage_node_server_allows_skipped_transfer_destination_epoch_from_current_marker() {
        let (bucket, checkpoint) =
            test_metadata_checkpoint_with_bucket("metadata-rpc-skipped-destination");
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(3).unwrap();
        let mut destination_route = config.pg_routes[0].clone();
        destination_route.cluster_epoch = destination_epoch;
        destination_route.state = PgState::Active;
        config.cluster_epoch = current_epoch;
        config.pg_routes[0] = destination_route.clone();
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        config.historical_pg_routes.push(destination_route);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let state_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: destination_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let can_initialize_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: destination_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let state_payload = decode_storage_rpc_response_payload(&state_response.payload)
            .unwrap()
            .unwrap();
        let state = decode_metadata_command_state_response(&state_payload).unwrap();
        assert_eq!(state.state.applied_log_index, 0);
        let can_initialize_payload =
            decode_storage_rpc_response_payload(&can_initialize_response.payload)
                .unwrap()
                .unwrap();
        let can_initialize =
            crate::storage_rpc::decode_metadata_command_bool_response(&can_initialize_payload)
                .unwrap();
        assert!(can_initialize.value);

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_response(&payload).unwrap();
        assert_eq!(decoded.state.cluster_epoch, destination_epoch);
        let destination_node =
            crate::node::SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();
        assert_eq!(
            destination_node
                .get_pg(0)
                .unwrap()
                .head_bucket(&bucket)
                .unwrap()
                .name,
            bucket
        );
    }

    #[test]
    fn storage_node_server_rejects_retained_transfer_destination_without_current_marker() {
        let (_bucket, checkpoint) =
            test_metadata_checkpoint_with_bucket("metadata-rpc-stale-destination");
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(3).unwrap();
        let mut destination_route = config.pg_routes[0].clone();
        destination_route.cluster_epoch = destination_epoch;
        destination_route.state = PgState::Peering;
        destination_route.metadata_transfer_destination_epoch = Some(destination_epoch);
        config.cluster_epoch = current_epoch;
        config.pg_routes[0] = destination_route.clone();
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].metadata_transfer_destination_epoch = None;
        config.historical_pg_routes.push(destination_route);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(
            error
                .message
                .contains("does not retain metadata-transfer destination epoch"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_rejects_active_metadata_transfer_checkpoint_base_install() {
        let (_bucket, checkpoint) =
            test_metadata_checkpoint_with_bucket("metadata-rpc-checkpoint-active");
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            encode_metadata_command_transfer_checkpoint_base_request(
                &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    checkpoint,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
        assert!(
            error.message.contains("route is active"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_rejects_unproven_metadata_transfer_matching_state_proof() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let expected_state_digest = {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command)
                .unwrap()
                .state_digest
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize,
            encode_metadata_command_transfer_matching_state_request(
                &StorageRpcMetadataCommandTransferMatchingStateRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    applied_log_index: 7,
                    applied_log_hash: crate::control_plane::MetadataCommandLogHash::for_test(
                        0x1234,
                    ),
                    expected_state_digest,
                },
            ),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert!(
            error.message.contains("unsupported unproven proof tuple"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_node_server_allows_peering_metadata_transfer_adopt_and_validate() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let expected_state_digest;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command).unwrap();
            expected_state_digest = pg.metadata_command_replica_state().unwrap().state_digest;
        }
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                destination_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let adopt_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt,
            encode_metadata_command_transfer_adopt_request(
                &StorageRpcMetadataCommandTransferAdoptRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: destination_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                    commands: vec![MetadataTransferCommand {
                        command: rebased,
                pre_state_digest: crate::control_plane::CanonicalStateDigest::for_test(0),
                        post_state_digest: expected_state_digest,
                    }],
                },
            )
            .unwrap(),
        );
        let validate_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: destination_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let adopt_payload = decode_storage_rpc_response_payload(&adopt_response.payload)
            .unwrap()
            .unwrap();
        let adopted = decode_metadata_command_state_response(&adopt_payload).unwrap();
        assert_eq!(adopted.state.state_digest, expected_state_digest);

        let validate_payload = decode_storage_rpc_response_payload(&validate_response.payload)
            .unwrap()
            .unwrap();
        let validated = decode_metadata_command_state_response(&validate_payload).unwrap();
        assert_eq!(validated.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_classifies_active_historical_transfer_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let source_route_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(4).unwrap();
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: source_route_epoch,
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        });
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
        );
    }

    #[test]
    fn storage_node_server_rejects_bare_historical_active_metadata_command_recovery() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let source_route_epoch = ClusterEpoch::new(1).unwrap();
        let current_epoch = ClusterEpoch::new(4).unwrap();
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: source_route_epoch,
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        });
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let lock_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let insert_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(
                &StorageRpcMetadataCommandPendingSlotRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    command: command.clone(),
                    scope_bucket: Some(command.bucket_name().clone()),
                    effect_deadline: None,
                    operation_deadline: None,
                },
            )
            .unwrap(),
        );
        let apply_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let lock_error = decode_storage_rpc_response_payload(&lock_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(lock_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(lock_error.message.contains("not authorized"));

        let insert_error = decode_storage_rpc_response_payload(&insert_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(insert_error.code, StorageRpcErrorCode::StaleShardLocation);

        let apply_error = decode_storage_rpc_response_payload(&apply_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(apply_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(apply_error.message.contains("not authorized"));
    }

    #[test]
    fn storage_node_server_allows_exact_historical_recovery_from_current_runtime_map() {
        let tmp = test_util::tempdir();
        let source_route_epoch = ClusterEpoch::new(1).unwrap();
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        let command = test_metadata_command(0, 1);
        private_socket_dir(config.socket_path.parent().unwrap());
        let source_route = config.pg_routes[0].clone();
        let socket_path = config.socket_path.clone();
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    source_route_epoch,
                    std::num::NonZeroU64::MIN,
                    command.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        let installed = server.config_snapshot();
        let reloaded = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &installed.data_dir,
            installed.node_id,
            installed.default_ec_shape,
            &installed.socket_path,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            reloaded.pending_metadata_command_recoveries,
            installed.pending_metadata_command_recoveries
        );
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let lock_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        let unlisted_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command: test_metadata_command(0, 2),
            })
            .unwrap(),
        );
        let release_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        decode_storage_rpc_response_payload(&lock_response.payload)
            .unwrap()
            .unwrap();
        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let applied = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert!(matches!(
            applied.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    cluster_epoch,
                    applied_log_index: 1,
                    ..
                }
            ) if cluster_epoch == source_route_epoch
        ));
        let unlisted_error = decode_storage_rpc_response_payload(&unlisted_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(unlisted_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(unlisted_error.message.contains("not authorized"));
        decode_storage_rpc_response_payload(&release_response.payload)
            .unwrap()
            .unwrap();
    }

    #[test]
    fn storage_node_server_reissues_certified_historical_recovery_over_unix_rpc() {
        let tmp = test_util::tempdir();
        let source_route_epoch = ClusterEpoch::new(1).unwrap();
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        let source = test_metadata_command(0, 1);
        let conflicting = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                crate::tests::bucket_name("metadata-rpc-bucket"),
                crate::tests::object_key("other-object"),
                crate::tests::stream_session_id("rpc-conflict"),
                GenerationId::new(1).unwrap(),
                123,
            )),
        );
        let replacement = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            source.payload().clone(),
        );
        let gap_replacement = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            source.payload().clone(),
        );
        let invalid_replacement =
            MetadataCommandEnvelope::new(replacement.id(), conflicting.payload().clone());
        private_socket_dir(config.socket_path.parent().unwrap());
        let source_route = config.pg_routes[0].clone();
        let local_node_id = config.node_id;
        let socket_path = config.socket_path.clone();
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &source,
                Some(source.bucket_name()),
            )
            .unwrap();
            pg.apply_metadata_command_and_record(config.node_id.as_u32(), &conflicting)
                .unwrap();
        }
        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    source_route_epoch,
                    std::num::NonZeroU64::MIN,
                    source.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let lock_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let invalid_apply_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            crate::storage_rpc::encode_metadata_command_recovery_request(
                &StorageRpcMetadataCommandRecoveryRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: None,
                    command: invalid_replacement,
                },
            )
            .unwrap(),
        );
        let gap_replace_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            crate::storage_rpc::encode_metadata_command_recovery_pending_slot_replace_request(
                &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: None,
                    previous: source.clone(),
                    replacement: gap_replacement,
                    scope_bucket: Some(source.bucket_name().clone()),
                },
            )
            .unwrap(),
        );
        {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                pg.pending_metadata_command_envelope(local_node_id.as_u32(), source_route_epoch,)
                    .unwrap(),
                Some(source.clone()),
                "an invalid recovery index jump must leave the pending slot unchanged"
            );
        }
        let replace_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            crate::storage_rpc::encode_metadata_command_recovery_pending_slot_replace_request(
                &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: None,
                    previous: source.clone(),
                    replacement: replacement.clone(),
                    scope_bucket: Some(source.bucket_name().clone()),
                },
            )
            .unwrap(),
        );
        let apply_response = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            crate::storage_rpc::encode_metadata_command_recovery_request(
                &StorageRpcMetadataCommandRecoveryRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source,
                    abandoned_source: None,
                    command: replacement,
                },
            )
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        decode_storage_rpc_response_payload(&lock_response.payload)
            .unwrap()
            .unwrap();
        let invalid_apply_error =
            decode_storage_rpc_response_payload(&invalid_apply_response.payload)
                .unwrap()
                .unwrap_err();
        assert_eq!(invalid_apply_error.code, StorageRpcErrorCode::PayloadDecode);
        let gap_replace_error = decode_storage_rpc_response_payload(&gap_replace_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(gap_replace_error.code, StorageRpcErrorCode::PayloadDecode);
        decode_storage_rpc_response_payload(&replace_response.payload)
            .unwrap()
            .unwrap();
        let payload = decode_storage_rpc_response_payload(&apply_response.payload)
            .unwrap()
            .unwrap();
        let applied = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert!(matches!(
            applied.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    cluster_epoch,
                    applied_log_index: 2,
                    ..
                }
            ) if cluster_epoch == source_route_epoch
        ));
    }

    #[test]
    fn storage_node_server_applies_certified_reissued_abandonment_cleanup_over_unix_rpc() {
        let tmp = test_util::tempdir();
        let source_route_epoch = ClusterEpoch::new(1).unwrap();
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        let bucket = crate::tests::bucket_name("metadata-recovery-cleanup");
        let key = crate::tests::object_key("object");
        let session_id = crate::tests::stream_session_id("recovery");
        let source = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    test_bucket_write_reservation_proof(bucket.clone(), &key),
                ),
            )),
        );
        let reissued = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            source.payload().clone(),
        );
        let cleanup = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            source
                .payload()
                .abandoned_recovery_follow_up()
                .expect("PutObject stream creation requires generation cleanup"),
        );
        private_socket_dir(config.socket_path.parent().unwrap());
        let source_route = config.pg_routes[0].clone();
        let local_node_id = config.node_id;
        let socket_path = config.socket_path.clone();
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &session_id).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            let conflict = MetadataCommandEnvelope::new(
                source.id(),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        bucket.clone(),
                        crate::tests::object_key("other-object"),
                        crate::tests::stream_session_id("other-recovery"),
                        GenerationId::MIN,
                        122,
                    ),
                ),
            );
            pg.try_insert_pending_metadata_command_slot(
                local_node_id.as_u32(),
                &source,
                Some(&bucket),
            )
            .unwrap();
            pg.apply_metadata_command_and_record(local_node_id.as_u32(), &conflict)
                .unwrap();
        }
        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    source_route_epoch,
                    std::num::NonZeroU64::MIN,
                    source.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let lockless_historical_abandon_response = send_frame(
            &mut client,
            100,
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command: source.clone(),
            })
            .unwrap(),
        );
        let lock_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let reissue_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            crate::storage_rpc::encode_metadata_command_recovery_pending_slot_replace_request(
                &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: None,
                    previous: source.clone(),
                    replacement: reissued.clone(),
                    scope_bucket: Some(bucket.clone()),
                },
            )
            .unwrap(),
        );
        let cleanup_before_tombstone_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            crate::storage_rpc::encode_metadata_command_recovery_pending_slot_replace_request(
                &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: Some(reissued.clone()),
                    previous: reissued.clone(),
                    replacement: cleanup.clone(),
                    scope_bucket: Some(bucket.clone()),
                },
            )
            .unwrap(),
        );
        let abandoned_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command: reissued.clone(),
            })
            .unwrap(),
        );
        let replace_response = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            crate::storage_rpc::encode_metadata_command_recovery_pending_slot_replace_request(
                &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: Some(reissued.clone()),
                    previous: reissued.clone(),
                    replacement: cleanup.clone(),
                    scope_bucket: Some(bucket.clone()),
                },
            )
            .unwrap(),
        );
        let cleanup_response = send_frame(
            &mut client,
            6,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            crate::storage_rpc::encode_metadata_command_recovery_request(
                &StorageRpcMetadataCommandRecoveryRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    authorized_source: source.clone(),
                    abandoned_source: Some(reissued),
                    command: cleanup.clone(),
                },
            )
            .unwrap(),
        );
        let remove_response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            crate::storage_rpc::encode_metadata_command_pending_slot_request(
                &StorageRpcMetadataCommandPendingSlotRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    command: cleanup,
                    scope_bucket: None,
                    effect_deadline: None,
                    operation_deadline: None,
                },
            )
            .unwrap(),
        );
        let release_response = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let reissue_payload = decode_storage_rpc_response_payload(&reissue_response.payload)
            .unwrap()
            .unwrap();
        let reissue =
            decode_metadata_command_pending_slot_remove_response(&reissue_payload).unwrap();
        assert!(reissue.removed);
        let cleanup_before_tombstone_error =
            decode_storage_rpc_response_payload(&cleanup_before_tombstone_response.payload)
                .unwrap()
                .unwrap_err();
        assert_eq!(
            cleanup_before_tombstone_error.code,
            StorageRpcErrorCode::PayloadDecode
        );
        let lockless_historical_abandon_error =
            decode_storage_rpc_response_payload(&lockless_historical_abandon_response.payload)
                .unwrap()
                .unwrap_err();
        assert_eq!(
            lockless_historical_abandon_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
        assert!(lockless_historical_abandon_error
            .message
            .contains("held recovery-primary lock"));
        for response in [
            &lock_response,
            &abandoned_response,
            &replace_response,
            &cleanup_response,
            &remove_response,
            &release_response,
        ] {
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        let pg = server._node.get_pg(0).unwrap();
        assert!(matches!(
            PgMetadataStore::get_object_generation_reservation(&*pg, &bucket, &key, &session_id,),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            3
        );
        assert_eq!(
            pg.pending_metadata_command_envelope(local_node_id.as_u32(), source_route_epoch)
                .unwrap(),
            None
        );
    }

    #[test]
    fn storage_node_server_allows_historical_recovery_after_original_route_deadline() {
        let tmp = test_util::tempdir();
        let source_route_epoch = ClusterEpoch::new(1).unwrap();
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let command = test_metadata_command(0, 1);
        let mut config = test_config(&tmp);
        config.route_map_validity =
            RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_sub(1))
                .unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let source_route = config.pg_routes[0].clone();
        let socket_path = config.socket_path.clone();
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    source_route_epoch,
                    std::num::NonZeroU64::MIN,
                    command.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let applied = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert!(matches!(
            applied.outcome,
            StorageRpcMetadataCommandStateOutcome::State(
                crate::metadata_command::MetadataCommandReplicaState {
                    cluster_epoch,
                    applied_log_index: 1,
                    ..
                }
            ) if cluster_epoch == source_route_epoch
        ));
    }

    #[test]
    fn storage_node_server_allows_historical_peering_metadata_transfer_reads_not_adopt() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let source_route_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(4).unwrap();
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(8)];
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: source_route_epoch,
            state: PgState::Peering,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        });
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let pending_command = test_metadata_command(0, 2);
        let expected_state_digest;
        let expected_entries;
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &command).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &pending_command, None)
                .unwrap();
            expected_state_digest = pg.metadata_command_replica_state().unwrap().state_digest;
            expected_entries = pg
                .retained_metadata_command_log_entries(
                    7,
                    command.id().cluster_epoch(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                )
                .unwrap();
        }
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let adopt_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt,
            encode_metadata_command_transfer_adopt_request(
                &StorageRpcMetadataCommandTransferAdoptRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: source_route_epoch,
                    pg_id: PgId::new(0),
                    expected_state_digest,
                    commands: vec![MetadataTransferCommand {
                        command: rebased,
                        pre_state_digest: crate::control_plane::CanonicalStateDigest::for_test(0),
                        post_state_digest: expected_state_digest,
                    }],
                },
            )
            .unwrap(),
        );
        let state_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandReplicaState,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: source_route_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let entries_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            encode_metadata_command_log_hash_range_request(
                &StorageRpcMetadataCommandLogHashRangeRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: command.id().cluster_epoch(),
                    pg_id: PgId::new(0),
                    first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                    last_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                },
            ),
        );
        let pending_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: command.id().cluster_epoch(),
                pg_id: PgId::new(0),
            }),
        );
        let validate_response = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: NodeId::new(7),
                cluster_epoch: command.id().cluster_epoch(),
                pg_id: PgId::new(0),
            }),
        );
        drop(client);
        join.join().unwrap();

        let adopt_error = decode_storage_rpc_response_payload(&adopt_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(adopt_error.code, StorageRpcErrorCode::NonActingSetAccess);

        let state_payload = decode_storage_rpc_response_payload(&state_response.payload)
            .unwrap()
            .unwrap();
        let state = decode_metadata_command_state_response(&state_payload).unwrap();
        assert_eq!(state.state.state_digest, expected_state_digest);

        let entries_payload = decode_storage_rpc_response_payload(&entries_response.payload)
            .unwrap()
            .unwrap();
        let entries =
            crate::storage_rpc::decode_metadata_command_log_entry_range_response(
                &entries_payload,
                &metadata_command_decode_authority_for_test(),
            )
            .unwrap();
        assert_eq!(entries.entries, expected_entries);

        let pending_payload = decode_storage_rpc_response_payload(&pending_response.payload)
            .unwrap()
            .unwrap();
        let pending = decode_metadata_command_pending_envelope_response(
            &pending_payload,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        assert_eq!(pending.command, Some(pending_command));

        let validate_payload = decode_storage_rpc_response_payload(&validate_response.payload)
            .unwrap()
            .unwrap();
        let validated = decode_metadata_command_state_response(&validate_payload).unwrap();
        assert_eq!(validated.state.state_digest, expected_state_digest);
    }

    #[test]
    fn storage_node_server_rejects_peering_replay_apply_while_active() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_rejects_normal_apply_while_peering() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let command = test_metadata_command(0, 1);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_returns_metadata_command_read_and_allocator_state() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let first = test_metadata_command(0, 1);
        let applied = test_metadata_command(0, 2);
        let pending = test_metadata_command(0, 3);
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        pg.record_metadata_command_abandoned(7, &first).unwrap();
        pg.apply_metadata_command_and_record(7, &applied).unwrap();
        let applied_hashes = pg
            .applied_metadata_command_log_entry_hashes(7, &applied)
            .unwrap()
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(7, &pending, Some(&bucket))
            .unwrap();
        drop(pg);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());
        let state_request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };

        let mut client = UnixStream::connect(socket_path).unwrap();
        let max_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            encode_metadata_command_state_request(&state_request),
        );
        let max_payload = decode_storage_rpc_response_payload(&max_response.payload)
            .unwrap()
            .unwrap();
        let max = decode_metadata_command_max_log_index_response(&max_payload).unwrap();
        assert_eq!(max.max_log_index, 2);

        let pending_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            encode_metadata_command_state_request(&state_request),
        );
        let pending_payload = decode_storage_rpc_response_payload(&pending_response.payload)
            .unwrap()
            .unwrap();
        let decoded_pending =
            decode_metadata_command_pending_envelope_response(
                &pending_payload,
                &metadata_command_decode_authority_for_test(),
            )
            .unwrap();
        assert_eq!(
            decoded_pending.command.unwrap().command_bytes(),
            pending.command_bytes()
        );

        let replay_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            encode_metadata_command_state_request(&state_request),
        );
        let replay_payload = decode_storage_rpc_response_payload(&replay_response.payload)
            .unwrap()
            .unwrap();
        let replay_state = decode_metadata_command_state_response(&replay_payload).unwrap();
        assert_eq!(replay_state.state.applied_log_index, 2);

        let applied_hashes_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: applied.clone(),
            })
            .unwrap(),
        );
        let applied_hashes_payload =
            decode_storage_rpc_response_payload(&applied_hashes_response.payload)
                .unwrap()
                .unwrap();
        let decoded_hashes =
            decode_metadata_command_applied_hashes_response(&applied_hashes_payload).unwrap();
        assert_eq!(
            decoded_hashes.outcome,
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some(applied_hashes))
        );

        let matching_response = send_frame(
            &mut client,
            5,
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            encode_metadata_command_matching_applied_request(
                &StorageRpcMetadataCommandMatchingAppliedRequest {
                    node_id: NodeId::new(7),
                    cluster_epoch: ClusterEpoch::new(1).unwrap(),
                    pg_id: PgId::new(0),
                    command: applied.clone(),
                    expected_previous_log_hash: applied_hashes.0,
                },
            )
            .unwrap(),
        );
        let matching_payload = decode_storage_rpc_response_payload(&matching_response.payload)
            .unwrap()
            .unwrap();
        let matching = decode_metadata_command_bool_outcome_response(&matching_payload).unwrap();
        assert_eq!(
            matching.outcome,
            StorageRpcMetadataCommandBoolOutcome::Value(true)
        );

        let abandoned_response = send_frame(
            &mut client,
            6,
            StorageRpcMessageKind::MetadataCommandAbandoned,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: first.clone(),
            })
            .unwrap(),
        );
        let abandoned_payload = decode_storage_rpc_response_payload(&abandoned_response.payload)
            .unwrap()
            .unwrap();
        let abandoned = decode_metadata_command_bool_outcome_response(&abandoned_payload).unwrap();
        assert_eq!(
            abandoned.outcome,
            StorageRpcMetadataCommandBoolOutcome::Value(true)
        );

        let before_conflict = observability::metrics_snapshot();
        let next_response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::MetadataCommandNextId,
            encode_metadata_command_next_id_request(&StorageRpcMetadataCommandNextIdRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                min_log_index: 1,
            }),
        );
        let next_payload = decode_storage_rpc_response_payload(&next_response.payload)
            .unwrap()
            .unwrap();
        let next = decode_metadata_command_next_id_response(&next_payload).unwrap();
        assert_eq!(
            next.outcome,
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 3,
            }
        );
        let after_conflict = observability::metrics_snapshot();
        assert!(
            after_conflict.metadata_command_conflict_total
                > before_conflict.metadata_command_conflict_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-7"
                    && record.event == "metadata_command_conflict"
            })
            .expect("storage-node RPC conflict should be recorded without caller-attached trace");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("log_index=3"));
        assert!(record.detail.contains("kind=log_conflict"));
        assert!(record
            .detail
            .contains("command_kind=ReserveObjectGeneration"));
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_server_preserves_stale_object_version_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-version-rpc");
        let key = crate::tests::object_key("object");
        let pg = server._node.get_pg(0).unwrap();
        let applied = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                VersionId::from_u64(1),
            )),
        );
        pg.apply_metadata_command_and_record(7, &applied).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket,
                key,
                VersionId::from_u64(1),
            )),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id: VersionId::from_u64(1)
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_bucket_metadata_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-bucket-rpc");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let initial = pg.head_bucket_record_raw(&bucket).unwrap();
        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                initial.clone().with_execution_generation(2),
                crate::AclGrants::default(),
                BucketAclSummary {
                    public_read: true,
                    public_write: false,
                },
            )),
        );
        pg.apply_metadata_command_and_record(7, &newer).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                initial.with_execution_generation(1),
                crate::AclGrants::default(),
                BucketAclSummary {
                    public_read: false,
                    public_write: true,
                },
            )),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name: bucket,
                bucket_execution_generation: 1,
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_object_write_apply_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-object-rpc");
        let key = crate::tests::object_key("object");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let first = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(1),
                owner: owner.clone(),
                write_sequence: 1,
                last_modified_millis: 123,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &first).unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(2),
                owner,
                write_sequence: 1,
                last_modified_millis: 124,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence: 1,
                generation_id: None,
            }
        );
    }

    #[test]
    fn storage_node_server_preserves_stale_delete_marker_target_conflict() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("stale-marker-delete-rpc");
        let key = crate::tests::object_key("object");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        let create_config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &crate::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let create = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 123, 1).unwrap(),
            ),
        );
        pg.apply_metadata_command_and_record(7, &create).unwrap();
        let first_marker = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: owner.clone(),
                write_sequence: 1,
                last_modified_millis: 123,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &first_marker)
            .unwrap();

        let reservation_id = crate::SessionId::try_from("76".repeat(16)).unwrap();
        let reserve = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                GenerationId::MIN,
                124,
            )),
        );
        pg.apply_metadata_command_and_record(7, &reserve).unwrap();
        let replacement_live = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(4).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: crate::PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: owner.clone(),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: crate::ObjectEtag::single_part(0),
                    ec: EcShape { k: 2, m: 1 },
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                },
                segments: Vec::new(),
                generation_reservation_id: reservation_id,
                write_sequence: 2,
                last_modified_millis: 124,
                stale_payload: None,
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            })),
        );
        pg.apply_metadata_command_and_record(7, &replacement_live)
            .unwrap();
        let newer_marker = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(5).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner,
                write_sequence: 3,
                last_modified_millis: 125,
                stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
                    crate::ObjectSegmentsReclaimRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        generation_id: GenerationId::MIN,
                        created_at: 125,
                        segments: Vec::new(),
                    },
                )),
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            }),
        );
        pg.apply_metadata_command_and_record(7, &newer_marker)
            .unwrap();
        drop(pg);

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(6).unwrap(),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                mode: crate::metadata_command::DeleteObjectVersionMode::Current,
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 },
                bucket_write_reservation: test_bucket_write_reservation_proof(bucket.clone(), &key),
            })),
        );
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                command: stale,
            })
            .unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_state_outcome_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence: 1,
                generation_id: None,
            }
        );
    }

    #[test]
    fn storage_node_server_allocates_next_metadata_command_id() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let first = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .record_metadata_command_abandoned(7, &first)
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandNextId,
            encode_metadata_command_next_id_request(&StorageRpcMetadataCommandNextIdRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                min_log_index: 5,
            }),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let next = decode_metadata_command_next_id_response(&payload).unwrap();
        assert_eq!(
            next.outcome,
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                log_index: 5,
            }
        );
    }

    #[test]
    fn storage_node_server_serializes_metadata_command_pending_insert_per_pg() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config).unwrap();
        let handler = server.connection_handler();
        let pg_id = PgId::new(0);
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id,
            command: command.clone(),
            scope_bucket: Some(command.bucket_name().clone()),
            effect_deadline: None,
            operation_deadline: None,
        };
        let pg_guard = server
            .metadata_command_locks
            .acquire(NodeId::new(7), pg_id, None);
        let (tx, rx) = mpsc::channel();
        let handler_for_thread = handler.clone();
        let join = thread::spawn(move || {
            let session = StorageNodeSession::new(
                Arc::clone(&handler_for_thread.read_handles),
                Arc::clone(&handler_for_thread.node),
            );
            let response = handler_for_thread
                .metadata_command_pending_slot_insert_response(&session, request)
                .unwrap();
            tx.send(response).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(pg_guard);
        let response = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_pending_slot_insert_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted
        );
        assert!(server
            ._node
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_some());
    }

    #[test]
    fn current_replica_abandonment_rechecks_route_fence_after_pg_lock_wait() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let pg_id = PgId::new(0);
        let command = test_metadata_command(pg_id.get(), 1);
        let holder = server
            .metadata_command_locks
            .acquire(config.node_id, pg_id, None)
            .unwrap();
        let valid_until_ms = crate::clock::current_time_millis().saturating_add(1_500);
        config.route_map_validity = RouteMapValidity::until_ms_saturating(valid_until_ms);
        server.install_control_plane_runtime_config(config).unwrap();
        let handler = server.connection_handler();

        let (waiting_tx, waiting_rx) = mpsc::channel();
        let wait_count = Arc::new(AtomicU64::new(0));
        let release_wait = Arc::new(Barrier::new(2));
        let wait_count_hook = Arc::clone(&wait_count);
        let release_wait_hook = Arc::clone(&release_wait);
        server
            .metadata_command_locks
            .set_before_wait_hook(Arc::new(move |actual_pg_id| {
                assert_eq!(actual_pg_id, pg_id);
                if wait_count_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                    waiting_tx.send(()).unwrap();
                    release_wait_hook.wait();
                }
            }));

        let (response_tx, response_rx) = mpsc::channel();
        let join = thread::spawn(move || {
            let session = StorageNodeSession::new(
                Arc::clone(&handler.read_handles),
                Arc::clone(&handler.node),
            );
            let response = handler
                .metadata_command_record_abandoned_response(
                    &session,
                    StorageRpcMetadataCommandRequest {
                        node_id: NodeId::new(7),
                        cluster_epoch: ClusterEpoch::INITIAL,
                        pg_id,
                        command,
                    },
                )
                .unwrap();
            response_tx.send(response).unwrap();
        });

        if let Err(wait_error) = waiting_rx.recv_timeout(Duration::from_secs(2)) {
            drop(holder);
            let response = response_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            join.join().unwrap();
            panic!(
                "replica abandonment did not wait for the held PG lock: {wait_error:?}; response={:?}",
                decode_storage_rpc_response_payload(&response)
            );
        }
        let remaining_ms = valid_until_ms.saturating_sub(crate::clock::current_time_millis());
        thread::sleep(Duration::from_millis(remaining_ms.saturating_add(20)));
        drop(holder);
        release_wait.wait();

        let response = response_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        join.join().unwrap();
        let error = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(!server
            ._node
            .get_pg(pg_id.get())
            .unwrap()
            .metadata_command_abandoned(NodeId::new(7).as_u32(), &test_metadata_command(0, 1))
            .unwrap());
    }

    #[test]
    fn storage_node_server_metadata_command_pg_lock_spans_connection_session() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let socket_path = config.socket_path.clone();
        let server = StorageNodeServer::bind(config).unwrap();
        let _stderr_guard = server.suppress_metadata_command_lock_wait_stderr();
        let _server_thread = thread::spawn(move || server.serve_forever().unwrap());
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
        };
        let payload = encode_metadata_command_state_request(&request);
        let mut owner = UnixStream::connect(&socket_path).unwrap();
        let acquire = send_frame(
            &mut owner,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            payload.clone(),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();

        let owner_read = send_frame(
            &mut owner,
            2,
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            payload.clone(),
        );
        let owner_read_payload = decode_storage_rpc_response_payload(&owner_read.payload)
            .unwrap()
            .unwrap();
        let owner_max =
            decode_metadata_command_max_log_index_response(&owner_read_payload).unwrap();
        assert_eq!(owner_max.max_log_index, 0);

        let (tx, rx) = mpsc::channel();
        let blocked_socket_path = socket_path.clone();
        let blocked_payload = payload.clone();
        let blocked = thread::spawn(move || {
            let mut client = UnixStream::connect(blocked_socket_path).unwrap();
            let response = send_frame(
                &mut client,
                1,
                StorageRpcMessageKind::MetadataCommandMaxLogIndex,
                blocked_payload,
            );
            tx.send(response).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        let release = send_frame(
            &mut owner,
            3,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            payload,
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        let response = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        blocked.join().unwrap();
        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_max_log_index_response(&payload).unwrap();
        assert_eq!(decoded.max_log_index, 0);
    }

    #[test]
    fn storage_node_server_build_mark_deleting_returns_already_deleting_bucket() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("mark-deleting-already-rpc");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            crate::PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            crate::PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }

        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let command_id = MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                pg_id: PgId::new(0),
                bucket: bucket.clone(),
            },
            command_id,
        };
        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::BucketMarkDeletingCommandBuild,
            encode_bucket_mark_deleting_command_build_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_bucket_mark_deleting_command_build_response(
            &payload,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
                assert_eq!(info.name, bucket);
                assert_eq!(info.state, BucketState::Deleting);
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(_) => {
                panic!("expected already-deleting mark bucket response")
            }
        }
    }

    #[test]
    fn storage_node_server_returns_metadata_command_acceptance() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandAcceptance,
            encode_metadata_command_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_acceptance_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
                crate::metadata_command::MetadataCommandAcceptance::Apply
            )
        );
    }

    #[test]
    fn storage_node_server_rejects_stale_metadata_command_route_before_acceptance() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: test_metadata_command(0, 1),
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandAcceptance,
            encode_metadata_command_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn metadata_command_rpc_rejects_command_epoch_mismatch_before_acceptance() {
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::new(2).unwrap(),
                    PgId::new(0),
                    MetadataCommandLogIndex::new(1).unwrap(),
                ),
                test_metadata_command(0, 1).payload().clone(),
            ),
        };

        let error = validate_metadata_command_request_epoch(&request).unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        assert!(error.message.contains("does not match request route epoch"));
        assert!(matches!(
            encode_metadata_command_request(&request),
            Err(crate::storage_rpc::StorageRpcPayloadError::MetadataCommandRouteMismatch(_))
        ));
    }

    #[test]
    fn unix_durable_effects_reject_expired_effective_deadline_before_authority_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let pg = server._node.get_pg(0).unwrap();
        create_probe_bucket_direct(&pg, &bucket);
        drop(pg);
        let node = Arc::clone(&server._node);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || {
            crate::clock::with_time_override(4_500, || server.accept_one().unwrap());
        });

        let mut client = UnixStream::connect(socket_path).unwrap();
        let reservation_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::BucketWriteReservationAcquire,
            crate::storage_rpc::encode_bucket_write_reservation_acquire_request(
                &StorageRpcBucketWriteReservationAcquireRequest {
                    node_id: config.node_id,
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    bucket: bucket.clone(),
                    reservation_id: "expired-frontend-reservation".to_string(),
                    owner_token: "expired-frontend-owner".to_string(),
                    operation_kind: "put-object-metadata".to_string(),
                    created_at: 1_000,
                    lease_deadline: 9_000,
                    target_context: Some("object".to_string()),
                    effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                        authority_valid_until_ms: 5_000,
                        portable_wall_valid_until_ms: 4_000,
                    }),
                },
            )
            .unwrap(),
        );
        let reservation_error = decode_storage_rpc_response_payload(&reservation_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            reservation_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );

        let command = test_metadata_command(0, 1);
        let pending_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(
                &StorageRpcMetadataCommandPendingSlotRequest {
                    node_id: config.node_id,
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    command,
                    scope_bucket: Some(bucket.clone()),
                    effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                        authority_valid_until_ms: 5_000,
                        portable_wall_valid_until_ms: 4_000,
                    }),
                    operation_deadline: None,
                },
            )
            .unwrap(),
        );
        let pending_error = decode_storage_rpc_response_payload(&pending_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(pending_error.code, StorageRpcErrorCode::StaleShardLocation);
        let bucket_control_command = test_bucket_control_metadata_command(0, 1);
        let bucket_control_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
            encode_metadata_command_pending_slot_request(
                &StorageRpcMetadataCommandPendingSlotRequest {
                    node_id: config.node_id,
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    command: bucket_control_command,
                    scope_bucket: Some(bucket.clone()),
                    effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                        authority_valid_until_ms: 5_000,
                        portable_wall_valid_until_ms: 4_000,
                    }),
                    operation_deadline: None,
                },
            )
            .unwrap(),
        );
        let bucket_control_error =
            decode_storage_rpc_response_payload(&bucket_control_response.payload)
                .unwrap()
                .unwrap_err();
        assert_eq!(
            bucket_control_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
        drop(client);
        join.join().unwrap();

        let pg = node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty()
        );
        assert!(pg
            .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_node_server_retries_lost_pending_slot_insert_exactly() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
            scope_bucket: Some(crate::tests::bucket_name("metadata-rpc-bucket")),
            effect_deadline: None,
            operation_deadline: None,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for request_id in [1, 2] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                encode_metadata_command_pending_slot_request(&request).unwrap(),
            );
            decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
        }
        let conflict = StorageRpcMetadataCommandPendingSlotRequest {
            command: test_metadata_command(0, 2),
            ..request
        };
        let before_conflict = observability::metrics_snapshot();
        let conflict_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(&conflict).unwrap(),
        );
        let after_conflict = observability::metrics_snapshot();
        assert!(
            after_conflict.metadata_command_conflict_total
                > before_conflict.metadata_command_conflict_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-3"
                    && record.event == "metadata_command_conflict"
            })
            .expect(
                "storage-node pending conflict should be recorded without caller-attached trace",
            );
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("log_index=2"));
        assert!(record.detail.contains("kind=pending_slot_conflict"));
        assert!(record
            .detail
            .contains("command_kind=ReserveObjectGeneration"));
        drop(client);
        join.join().unwrap();

        let payload = decode_storage_rpc_response_payload(&conflict_response.payload)
            .unwrap()
            .unwrap();
        let decoded = decode_metadata_command_pending_slot_insert_response(&payload).unwrap();
        assert_eq!(
            decoded.outcome,
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                existing_log_index: 1,
                candidate_log_index: 2,
            }
        );
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pending = reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_bytes(), command.command_bytes());
    }

    #[test]
    fn storage_node_server_rejects_mismatched_pending_slot_scope_bucket() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: test_metadata_command(0, 1),
            scope_bucket: Some(crate::tests::bucket_name("wrong-scope-bucket")),
            effect_deadline: None,
            operation_deadline: None,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            encode_metadata_command_pending_slot_request(&request).unwrap(),
        );
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_node_server_retries_lost_pending_slot_remove_as_not_found() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
        let pg = server._node.get_pg(0).unwrap();
        pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
            .unwrap();
        pg.record_metadata_command_abandoned(7, &command).unwrap();
        drop(pg);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
            scope_bucket: None,
            effect_deadline: None,
            operation_deadline: None,
        };
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        for (request_id, expected_removed) in [(1, true), (2, false)] {
            let response = send_frame(
                &mut client,
                request_id,
                StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
                crate::storage_rpc::encode_metadata_command_pending_slot_request(&request).unwrap(),
            );
            let payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            let decoded =
                decode_metadata_command_pending_slot_cleanup_response(&payload).unwrap();
            assert_eq!(
                decoded.outcome,
                StorageRpcMetadataCommandPendingSlotCleanupOutcome::Value(expected_removed)
            );
        }
        drop(client);
        join.join().unwrap();

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(reopened
            .get_pg(0)
            .unwrap()
            .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_wrong_node() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 0, 8));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownNode);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_stale_location_epoch() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(2, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_unknown_pg() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 9, 7));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_stale_route_epoch() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_inactive_pg() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_non_acting_set() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
    }

    #[test]
    fn storage_node_read_handle_state_delete_fence_is_atomic_with_acquire() {
        let location = test_location(1, 0, 7);
        let shard_key = test_shard_key(7);
        let other_shard_key = ShardKey::new(&[0x99; 16], 99, 7);
        let mut state = StorageNodeReadHandleState::default();

        state.try_begin_delete(location, &shard_key).unwrap();
        state
            .try_acquire(&[(location, other_shard_key.clone())])
            .unwrap();
        state.release(&[(location, other_shard_key)]);
        let error = state
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ShardDeleteInProgress);
        assert!(error.message.contains("being deleted"));
        state.finish_delete(location, &shard_key);

        state.try_acquire(&[(location, shard_key.clone())]).unwrap();
        let error = state.try_begin_delete(location, &shard_key).unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error.message.contains("active read handles"));
        state.release(&[(location, shard_key.clone())]);
        state.try_begin_delete(location, &shard_key).unwrap();
    }

    #[test]
    fn storage_node_server_validates_route_before_acquiring_read_handle() {
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

        let success_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![location.into()]);
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_acquire_without_extra_count() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );

        for response in [first, second] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
            assert_eq!(acquired.locations, vec![location.into()]);
        }
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_release_without_error_or_leak() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let first_release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        let second_release = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );

        for response in [first_release, second_release] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            decode_read_handle_release_response(&success_payload).unwrap();
            assert_eq!(server.read_handle_count(location), 0);
        }
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_session_rejects_read_operation_count_over_limit() {
        let shared_handles = Arc::new(Mutex::new(StorageNodeReadHandleState::default()));
        let node =
            Arc::new(SharedStorageNode::topology_only(&[0], EcShape { k: 1, m: 0 }).unwrap());
        let mut session = StorageNodeSession::new(Arc::clone(&shared_handles), node);
        let location = test_location(1, 0, 7);

        for i in 0..STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION {
            session
                .acquire_read_handles(ValidatedReadHandleAcquireRequest {
                    read_operation_id: format!("read-op-{i}"),
                    entries: vec![(location, test_shard_key(location.shard_index().get()))],
                })
                .unwrap();
        }
        let error = session
            .acquire_read_handles(ValidatedReadHandleAcquireRequest {
                read_operation_id: "read-op-over-limit".to_string(),
                entries: vec![(location, test_shard_key(location.shard_index().get()))],
            })
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert_eq!(
            shared_handles.lock().unwrap().count(location),
            STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION
        );
    }

    #[test]
    fn storage_node_session_disconnect_releases_object_payload_lease() {
        let shared_handles = Arc::new(Mutex::new(StorageNodeReadHandleState::default()));
        let node =
            Arc::new(SharedStorageNode::topology_only(&[0], EcShape { k: 1, m: 0 }).unwrap());
        let bucket = BucketName::new("lease-disconnect").unwrap();
        let key = ObjectKey::new("source").unwrap();
        let generation_id = GenerationId::new(1).unwrap();

        {
            let mut session =
                StorageNodeSession::new(Arc::clone(&shared_handles), Arc::clone(&node));
            assert!(session
                .acquire_object_payload_lease(
                    ClusterEpoch::new(1).unwrap(),
                    &bucket,
                    &key,
                    generation_id,
                )
                .unwrap());
            assert!(session.has_active_read_state());
            assert_eq!(
                node.object_payload_lease_count(&bucket, &key, generation_id),
                1
            );
        }

        assert_eq!(
            node.object_payload_lease_count(&bucket, &key, generation_id),
            0
        );
    }

    #[test]
    fn storage_node_session_binds_object_payload_lease_release_to_acquisition() {
        let shared_handles = Arc::new(Mutex::new(StorageNodeReadHandleState::default()));
        let node =
            Arc::new(SharedStorageNode::topology_only(&[0], EcShape { k: 1, m: 0 }).unwrap());
        let bucket = BucketName::new("lease-subject").unwrap();
        let other_bucket = BucketName::new("other-lease-subject").unwrap();
        let key = ObjectKey::new("source").unwrap();
        let generation_id = GenerationId::new(1).unwrap();
        let epoch = ClusterEpoch::new(3).unwrap();
        let mut session = StorageNodeSession::new(shared_handles, Arc::clone(&node));

        assert!(session
            .acquire_object_payload_lease(epoch, &bucket, &key, generation_id)
            .unwrap());
        for (release_epoch, release_bucket) in [
            (ClusterEpoch::new(2).unwrap(), &bucket),
            (epoch, &other_bucket),
        ] {
            let error = session
                .release_object_payload_lease(release_epoch, release_bucket, &key, generation_id)
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
            assert_eq!(
                node.object_payload_lease_count(&bucket, &key, generation_id),
                1
            );
        }

        assert_eq!(
            session
                .release_object_payload_lease(epoch, &bucket, &key, generation_id)
                .unwrap(),
            0
        );
        assert_eq!(
            session
                .release_object_payload_lease(epoch, &bucket, &key, generation_id)
                .unwrap(),
            0,
            "lost release responses must be retryable without releasing another lease"
        );
    }

    #[test]
    fn object_payload_reclaim_fence_is_bound_to_exact_claim_authority() {
        let node = SharedStorageNode::topology_only(&[0], EcShape { k: 1, m: 0 }).unwrap();
        let bucket = BucketName::new("reclaim-authority").unwrap();
        let key = ObjectKey::new("source").unwrap();
        let generation_id = GenerationId::new(1).unwrap();
        let first = crate::metadata_command::ObjectPayloadReclaimClaimProof {
            bucket_incarnation_generation: 1,
            reclaim_kind: crate::ObjectPayloadReclaimKind::ObjectSegments,
            claim_id: "claim-a".to_string(),
            owner_token: "owner-a".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
        };
        let second = crate::metadata_command::ObjectPayloadReclaimClaimProof {
            claim_id: "claim-b".to_string(),
            owner_token: "owner-b".to_string(),
            ..first.clone()
        };

        assert!(node.try_begin_object_payload_reclaim(&bucket, &key, generation_id, &first));
        assert!(!node.finish_object_payload_reclaim(&bucket, &key, generation_id, &second, false,));
        assert!(!node.try_acquire_object_payload_lease(&bucket, &key, generation_id));

        assert!(node.finish_object_payload_reclaim(&bucket, &key, generation_id, &first, true,));
        assert!(node.try_begin_object_payload_reclaim(&bucket, &key, generation_id, &second));
        assert!(!node.clear_object_payload_reclaim_fence(&bucket, &key, generation_id, &first,));
        assert!(!node.try_acquire_object_payload_lease(&bucket, &key, generation_id));
        assert!(node.finish_object_payload_reclaim(&bucket, &key, generation_id, &second, false,));
        assert!(node.try_acquire_object_payload_lease(&bucket, &key, generation_id));
        assert_eq!(
            node.release_object_payload_lease(&bucket, &key, generation_id),
            0
        );
    }

    #[test]
    fn storage_node_read_handle_state_rejects_aggregate_limits() {
        let location = test_location(1, 0, 7);
        let mut operations_exhausted = StorageNodeReadHandleState {
            live_read_operations: STORAGE_NODE_MAX_LIVE_READ_OPERATIONS,
            ..StorageNodeReadHandleState::default()
        };
        let shard_key = test_shard_key(location.shard_index().get());
        let error = operations_exhausted
            .try_acquire(&[(location, shard_key.clone())])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);

        let mut locations_exhausted = StorageNodeReadHandleState {
            live_read_handle_locations: STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS,
            ..StorageNodeReadHandleState::default()
        };
        let error = locations_exhausted
            .try_acquire(&[(location, shard_key)])
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
    }

    #[test]
    fn storage_node_active_session_state_rejects_over_limit() {
        let mut active_sessions = StorageNodeActiveSessionState::default();

        assert!(active_sessions.try_acquire(2));
        assert!(active_sessions.try_acquire(2));
        assert!(!active_sessions.try_acquire(2));
        active_sessions.release(StorageNodeActiveSessionClass::Unclassified);
        assert!(active_sessions.try_acquire(2));
    }

    #[test]
    fn storage_node_active_sessions_reserve_capacity_from_retained_ordinary_connections() {
        let active_sessions = Arc::new(StorageNodeActiveSessions::default());
        let mut ordinary = active_sessions.try_acquire(2).unwrap();
        assert!(ordinary.classify_connection(false));

        let mut excess_ordinary = active_sessions.try_acquire(2).unwrap();
        assert!(!excess_ordinary.classify_connection(false));
        drop(excess_ordinary);

        let mut stateful = active_sessions.try_acquire(2).unwrap();
        assert!(stateful.classify_connection(true));
        assert!(active_sessions.try_acquire(2).is_none());

        drop(stateful);
        assert!(active_sessions.try_acquire(2).is_some());
    }

    #[test]
    fn storage_node_active_sessions_release_notifies_capacity() {
        let active_sessions = Arc::new(StorageNodeActiveSessions::default());
        let first = active_sessions.try_acquire(1).unwrap();
        assert!(active_sessions.try_acquire(1).is_none());

        drop(first);
        assert!(active_sessions.try_acquire(1).is_some());
    }

    #[test]
    fn storage_node_server_release_removes_completed_read_operation() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first_acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first_acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 1);

        let release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);

        let second_acquire = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );
        let success_payload = decode_storage_rpc_response_payload(&second_acquire.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![second_location.into()]);
        assert_eq!(server.read_handle_count(first_location), 0);
        assert_eq!(server.read_handle_count(second_location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(second_location), 0);
    }

    #[test]
    fn storage_node_server_rejects_read_operation_id_reuse_for_different_locations() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );

        let error = decode_storage_rpc_response_payload(&second.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert_eq!(server.read_handle_count(first_location), 1);
        assert_eq!(server.read_handle_count(second_location), 0);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);
    }
fn metadata_command_decode_authority_for_test() -> MetadataCommandDecodeAuthority {
    MetadataCommandDecodeAuthority::new_for_test()
}
