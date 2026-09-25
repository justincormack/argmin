// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

    use super::*;
    use crate::control_plane_auth::{
        ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
        ControlPlaneScopedCredentialStore,
    };
    use crate::node_client::{
        BucketMetadataNodeClient, BuildCompleteMultipartObjectCommandReq,
        BuildStreamPartCommitCommandReq, DirectPutMetadataNodeClient,
        LocalUnixStorageNodeClientAdmissionSettings, MetadataCommandApplyErrorKind,
        MetadataCommandInspectionNodeClient, MetadataCommandPendingSlotReplaceError,
        MetadataCommandPeeringNodeClient, MetadataCommandRecoveryNodeClient,
        ObjectMutationMetadataNodeClient, ObjectPayloadLeaseKind, ObjectPayloadLeaseNodeClient,
        PlacedShardNodeClient, ShardReadHandleNodeClient,
        RetainedBucketWriteReservationNodeClient, UnixStorageNodeClient,
    };
    use crate::storage_rpc_transport::StorageRpcClientEndpoint;
    use crate::storage_rpc::StorageRpcWireErrorCode;
    use crate::storage_rpc_auth::{
        encode_storage_rpc_auth_transport_frame_with_version_for_test,
        sign_storage_rpc_request,
        sign_storage_rpc_request_with_auth_envelope_version_for_test,
        sign_storage_rpc_request_with_binding_version_for_test,
        sign_storage_rpc_request_with_encoded_frame_for_test,
        write_storage_rpc_auth_transport_frame, StorageRpcAuthRequestInput,
    };
    use crate::{
        BucketAclSummary, LocalClusterMap, LocalUnixStorageNodeClientConfig, StorageCluster,
        StorageRpcClientAuthConfig,
    };
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Barrier, Condvar, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use s3_types::{AclGrants, BucketObjectLockConfig, BucketVersioningState};

    use crate::control_plane::{
        FileControlPlaneStore, NodeHeartbeat, NodeMembershipState, SingleAuthorityControlPlane,
        UnavailablePgTransitionMutationBinding,
    };
    use crate::metadata_command::{
        BucketPropertyMutation, BucketWriteReservationProof, CommitDirectPutObjectCommand,
        CreateBucketCommand, CreateStreamUploadCommand, DeleteObjectVersionCommand,
        DeleteObjectVersionTarget, InsertDeleteMarkerCommand, MetadataCommandAcceptance,
        MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
        MetadataCommandPayload, MetadataTransferCommand,
        PutBucketAclCommand, ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
    };
    use crate::node_runtime::traits::{PgMetadataStore, ShardStore};
    use crate::storage_rpc::{
        decode_bucket_mark_deleting_command_build_response, decode_health_response,
        decode_metadata_command_acceptance_response,
        decode_metadata_command_applied_hashes_response,
        decode_metadata_command_bool_outcome_response,
        decode_metadata_command_log_hash_range_response,
        decode_metadata_command_max_log_index_response, decode_metadata_command_next_id_response,
        decode_metadata_command_pending_envelope_response,
        decode_metadata_command_pending_slot_cleanup_response,
        decode_metadata_command_pending_slot_insert_response,
        decode_metadata_command_pending_slot_remove_response,
        decode_metadata_command_state_outcome_response, decode_metadata_command_state_response,
        decode_read_handle_acquire_response, decode_read_handle_release_response,
        decode_scavenger_list_files_response, decode_shard_ack_item_response,
        decode_shard_read_range_response, decode_shard_read_response, decode_shard_write_ack,
        decode_storage_rpc_response_payload, encode_bucket_mark_deleting_command_build_request,
        encode_bucket_pg_request, encode_metadata_command_log_hash_range_request,
        encode_metadata_command_matching_applied_request, encode_metadata_command_next_id_request,
        encode_metadata_command_pending_slot_cleanup_response,
        encode_metadata_command_pending_slot_request, encode_metadata_command_request,
        encode_metadata_command_request_with_raw_command_for_test,
        encode_metadata_command_state_request, encode_metadata_command_transfer_adopt_request,
        encode_metadata_command_transfer_checkpoint_base_request,
        encode_metadata_command_transfer_empty_state_request,
        encode_metadata_command_transfer_matching_state_request,
        encode_placed_segment_shard_backfill_claim_acquire_request,
        encode_placed_segment_shard_backfill_claim_error_request,
        encode_placed_segment_shard_backfill_claim_record_request,
        encode_placed_segment_shard_backfill_item_request,
        encode_placed_segment_shard_backfill_record_request,
        encode_placed_segment_shard_repair_claim_acquire_request,
        encode_placed_segment_shard_repair_claim_error_request,
        encode_placed_segment_shard_repair_claim_record_request,
        encode_placed_segment_shard_repair_item_request,
        encode_placed_segment_shard_repair_record_request, encode_read_handle_acquire_request,
        encode_read_handle_release_request, encode_scavenger_list_files_request,
        encode_scavenger_observation_key_request, encode_scavenger_observation_record_request,
        encode_shard_ack_batch_request, encode_shard_ack_item_request, encode_shard_delete_request,
        encode_shard_read_range_request, encode_shard_read_request, encode_shard_write_request,
        encode_storage_rpc_frame, encode_storage_rpc_frame_with_version_for_test,
        read_storage_rpc_frame_from, write_storage_rpc_frame_to,
        StorageRpcBucketMarkDeletingCommandBuildOutcome,
        StorageRpcBucketMarkDeletingCommandBuildRequest, StorageRpcBucketPgRequest,
        StorageRpcBucketRequest, StorageRpcMetadataCommandAcceptanceOutcome,
        StorageRpcMetadataCommandLogHashRangeRequest,
        StorageRpcMetadataCommandMatchingAppliedRequest, StorageRpcMetadataCommandNextIdRequest,
        StorageRpcMetadataCommandPendingSlotInsertOutcome,
        StorageRpcMetadataCommandPendingSlotRequest, StorageRpcMetadataCommandRequest,
        StorageRpcMetadataCommandStateOutcome, StorageRpcMetadataCommandStateRequest,
        StorageRpcMetadataCommandTransferAdoptRequest,
        StorageRpcMetadataCommandTransferCheckpointBaseRequest,
        StorageRpcMetadataCommandTransferEmptyStateRequest,
        StorageRpcMetadataCommandTransferMatchingStateRequest,
        StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
        StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
        StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
        StorageRpcPlacedSegmentShardBackfillItemRequest,
        StorageRpcPlacedSegmentShardBackfillRecordRequest,
        StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
        StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
        StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
        StorageRpcPlacedSegmentShardRepairItemRequest,
        StorageRpcPlacedSegmentShardRepairRecordRequest, StorageRpcReadHandleAcquireRequest,
        StorageRpcReadHandleReleaseRequest, StorageRpcScavengerListFilesRequest,
        StorageRpcScavengerObservationKeyRequest, StorageRpcScavengerObservationRecordRequest,
        StorageRpcShardAckBatchRequest, StorageRpcShardAckItem, StorageRpcShardAckItemRequest,
        StorageRpcShardDeleteRequest, StorageRpcShardReadRangeRequest, StorageRpcShardReadRequest,
        StorageRpcShardWriteRequest,
    };
    use crate::types::{
        BucketName, BucketSubresourceAux, BucketSubresourceKind, CreateBucketConfig, GenerationId,
        PgId, PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimRecord,
        PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimAcquire,
        PlacedSegmentShardRepairClaimRecord, PlacedSegmentShardRepairWorkItem,
        PutBucketSubresource, SegmentStoredBytesRequest, ShardIndex, ShardKey,
        ShardScavengerObservationKey, ShardScavengerObservationReason,
        ShardScavengerObservationRecord, VersionId,
    };

    #[test]
    fn placed_segment_shard_repair_claim_route_epoch_must_match_claim_epoch() {
        let route_epoch = ClusterEpoch::new(2).unwrap();
        let claim_epoch = ClusterEpoch::new(1).unwrap();
        let claim = PlacedSegmentShardRepairClaimRecord {
            work_item: PlacedSegmentShardRepairWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: [0xAC; 16],
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: 0x1234,
                    ec: EcShape { k: 4, m: 2 },
                },
                shard_index: ShardIndex::new(5),
            },
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: claim_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        };

        assert!(matches!(
            validate_placed_segment_shard_repair_claim_route_epoch(
                PgId::new(7),
                route_epoch,
                &claim
            ),
            Err(StoreError::StalePayloadOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == claim_epoch && current_epoch == route_epoch
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_claim_route_epoch_must_match_claim_epoch() {
        let route_epoch = ClusterEpoch::new(2).unwrap();
        let claim_epoch = ClusterEpoch::new(1).unwrap();
        let claim = PlacedSegmentShardBackfillClaimRecord {
            work_item: PlacedSegmentShardBackfillWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: [0xAC; 16],
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: 0x1234,
                    ec: EcShape { k: 4, m: 2 },
                },
                source_cluster_epoch: ClusterEpoch::new(1).unwrap(),
                desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            },
            remaining_tolerance: 2,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: claim_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        };

        assert!(matches!(
            validate_placed_segment_shard_backfill_claim_route_epoch(
                PgId::new(7),
                route_epoch,
                &claim
            ),
            Err(StoreError::StalePayloadOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == claim_epoch && current_epoch == route_epoch
        ));
    }

    fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
        StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("node"),
            default_ec_shape: EcShape { k: 4, m: 2 },
            pg_ids: vec![0],
            socket_path: tmp.path().join("sock").join("storage.sock"),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7)],
            }],
            historical_pg_routes: Vec::new(),
            pending_metadata_command_recoveries: Vec::new(),
        }
    }

    #[test]
    fn standalone_storage_node_route_identity_has_exact_baseline_and_binds_inputs() {
        let baseline = StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            route_map_validity: RouteMapValidity::Forever,
            data_dir: PathBuf::from("/data/node"),
            default_ec_shape: EcShape { k: 4, m: 2 },
            pg_ids: vec![0],
            socket_path: PathBuf::from("/run/node.sock"),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7)],
            }],
            historical_pg_routes: Vec::new(),
            pending_metadata_command_recoveries: Vec::new(),
        };
        let identity = baseline.standalone_route_identity().unwrap();
        assert_eq!(
            identity.0,
            [
                54, 173, 168, 164, 79, 238, 241, 136, 186, 45, 203, 51, 18, 114, 94, 181, 178, 238,
                22, 143, 165, 48, 13, 231, 250, 91, 2, 14, 82, 194, 201, 57,
            ]
        );

        let mut changed = Vec::new();
        let mut config = baseline.clone();
        config.data_dir = PathBuf::from("/data/other");
        changed.push(config);
        let mut config = baseline.clone();
        config.socket_path = PathBuf::from("/run/other.sock");
        changed.push(config);
        let mut config = baseline.clone();
        config.default_ec_shape = EcShape { k: 3, m: 2 };
        changed.push(config);
        let mut config = baseline.clone();
        config.pg_routes[0].state = PgState::Degraded;
        changed.push(config);
        let mut config = baseline.clone();
        config.pg_routes[0].acting_set.push(NodeId::new(8));
        changed.push(config);
        for config in changed {
            assert_ne!(config.standalone_route_identity().unwrap(), identity);
        }

        let mut dynamic = baseline;
        dynamic.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        assert!(matches!(
            dynamic.standalone_route_identity(),
            Err(crate::StandaloneRouteIdentityError::DynamicAuthority)
        ));
    }

    const STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn storage_rpc_auth_test_credential(
        principal: ControlPlaneAuthPrincipal,
    ) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "storage-node-server-auth-test".to_owned(),
            credential_id: "storage-node-server-caller".to_owned(),
            credential_version: 1,
            principal,
            secret: b"storage-node-server-auth-secret".to_vec(),
        })
        .unwrap()
    }

    fn storage_rpc_server_auth(
        credential: &ControlPlaneScopedCredential,
    ) -> StorageRpcServerAuthConfig {
        storage_rpc_server_auth_with_max_connections(
            credential,
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
        )
    }

    fn storage_rpc_test_transport_limits(
        max_connections: usize,
    ) -> crate::StorageRpcTransportLimits {
        let defaults = crate::StorageRpcTransportLimits::DEFAULT;
        storage_rpc_test_transport_limits_with_io_timeout(
            max_connections,
            defaults.io_timeout(),
        )
    }

    fn storage_rpc_test_transport_limits_with_io_timeout(
        max_connections: usize,
        io_timeout: Duration,
    ) -> crate::StorageRpcTransportLimits {
        let defaults = crate::StorageRpcTransportLimits::DEFAULT;
        crate::StorageRpcTransportLimits::new(
            defaults.max_frame_bytes(),
            max_connections,
            io_timeout,
        )
        .unwrap()
    }

    fn storage_rpc_server_auth_with_max_connections(
        credential: &ControlPlaneScopedCredential,
        max_connections: usize,
    ) -> StorageRpcServerAuthConfig {
        StorageRpcServerAuthConfig::new(
            credential.cluster_id(),
            ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap(),
            9,
            STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
        )
        .unwrap()
        .with_transport_limits(storage_rpc_test_transport_limits(max_connections))
    }

    fn storage_rpc_client_auth(
        credential: ControlPlaneScopedCredential,
        topology_generation: u64,
    ) -> Arc<StorageRpcClientAuthConfig> {
        storage_rpc_client_auth_with_max_connections(
            credential,
            topology_generation,
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
        )
    }

    fn storage_rpc_client_auth_with_max_connections(
        credential: ControlPlaneScopedCredential,
        topology_generation: u64,
        max_connections: usize,
    ) -> Arc<StorageRpcClientAuthConfig> {
        Arc::new(
            crate::FrontendStorageRpcClientCapability::new_with_transport_limits(
                credential,
                topology_generation,
                STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                storage_rpc_test_transport_limits(max_connections),
            )
            .unwrap()
            .into(),
        )
    }

    fn live_pg_metadata_transfer_storage_rpc_client_auth(
        credential: ControlPlaneScopedCredential,
    ) -> Arc<StorageRpcClientAuthConfig> {
        live_pg_metadata_transfer_storage_rpc_client_auth_with_transport_limits(
            credential,
            storage_rpc_test_transport_limits(
                crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
            ),
        )
    }

    fn live_pg_metadata_transfer_storage_rpc_client_auth_with_transport_limits(
        credential: ControlPlaneScopedCredential,
        transport_limits: crate::StorageRpcTransportLimits,
    ) -> Arc<StorageRpcClientAuthConfig> {
        Arc::new(
            crate::LivePgMetadataTransferStorageRpcClientCapability::new_with_transport_limits(
                credential,
                9,
                STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                transport_limits,
            )
            .unwrap()
            .into(),
        )
    }

    fn storage_rpc_tls_certified_key() -> Arc<rustls::sign::CertifiedKey> {
        let certificates = CertificateDer::pem_slice_iter(include_bytes!(
            "../../../s3-tests/testdata/localhost-cert.pem"
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../../../s3-tests/testdata/localhost-key.pem"
        ))
        .unwrap();
        Arc::new(
            rustls::sign::CertifiedKey::from_der(
                certificates,
                private_key,
                &tls_provider::build_provider(),
            )
            .unwrap(),
        )
    }

    fn storage_rpc_tls_trust_roots() -> Arc<rustls::RootCertStore> {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                CertificateDer::pem_slice_iter(include_bytes!(
                    "../../../s3-tests/testdata/ca-cert.pem"
                ))
                .next()
                .unwrap()
                .unwrap(),
            )
            .unwrap();
        Arc::new(roots)
    }

    fn storage_rpc_tls_server_config() -> Arc<rustls::ServerConfig> {
        crate::storage_rpc_transport::storage_rpc_tls_server_config(storage_rpc_tls_certified_key())
            .unwrap()
    }

    fn storage_rpc_tls_client_config() -> Arc<rustls::ClientConfig> {
        crate::storage_rpc_transport::storage_rpc_tls_client_config(storage_rpc_tls_trust_roots())
            .unwrap()
    }

    #[test]
    fn tls_tcp_listener_constructs_the_storage_owned_profile() {
        let listener = StorageNodeRpcListenerConfig::tls_tcp(
            "127.0.0.1:7701".parse().unwrap(),
            storage_rpc_tls_certified_key(),
        )
        .unwrap();

        let StorageNodeRpcListenerConfigInner::Tcp {
            tls_server_config, ..
        } = &listener.inner
        else {
            panic!("TLS constructor returned a Unix listener");
        };
        assert_eq!(
            tls_server_config.alpn_protocols,
            [crate::storage_rpc_transport::STORAGE_RPC_TLS_ALPN]
        );
        assert!(listener.is_tls_tcp());
    }

    fn bounded_runtime_refresh_config(
        mut config: StorageNodeProcessConfig,
    ) -> StorageNodeProcessConfig {
        config.route_map_validity =
            RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_add(60_000))
                .expect("test validity deadline must be representable");
        config
    }

    fn test_route(pg_id: u32) -> StorageNodePgRoute {
        StorageNodePgRoute {
            pg_id,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        }
    }

    fn assert_storage_node_process_config_eq(
        actual: &StorageNodeProcessConfig,
        expected: &StorageNodeProcessConfig,
    ) {
        assert_eq!(actual.node_id, expected.node_id);
        assert_eq!(actual.cluster_epoch, expected.cluster_epoch);
        assert_eq!(actual.route_map_validity, expected.route_map_validity);
        assert_eq!(actual.data_dir, expected.data_dir);
        assert_eq!(actual.default_ec_shape, expected.default_ec_shape);
        assert_eq!(actual.pg_ids, expected.pg_ids);
        assert_eq!(actual.socket_path, expected.socket_path);
        assert_eq!(actual.pg_routes, expected.pg_routes);
        assert_eq!(actual.historical_pg_routes, expected.historical_pg_routes);
    }

    fn replace_runtime_config_count(raw: &str, label: &str, count: usize) -> String {
        let prefix = format!("{label} ");
        let mut replaced = false;
        let mut lines: Vec<_> = raw
            .lines()
            .map(|line| {
                if line.starts_with(&prefix) {
                    assert!(!replaced, "runtime config label must be unique");
                    replaced = true;
                    format!("{label} {count}")
                } else {
                    line.to_owned()
                }
            })
            .collect();
        assert!(replaced, "runtime config label must exist");
        lines.push(String::new());
        lines.join("\n")
    }

    #[test]
    fn storage_node_process_config_new_rejects_invalid_route_table() {
        let tmp = test_util::tempdir();
        let err = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("node"),
            default_ec_shape: EcShape { k: 4, m: 2 },
            pg_ids: vec![0],
            socket_path: tmp.path().join("sock").join("storage.sock"),
            pg_routes: Vec::new(),
            historical_pg_routes: Vec::new(),
            pending_metadata_command_recoveries: Vec::new(),
        })
        .unwrap_err();
        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_round_trips() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(9).unwrap();
        config.route_map_validity = RouteMapValidity::until_ms(12_345).unwrap();
        config.socket_path = PathBuf::from("/run/argmin/storage-7.sock");
        config.pg_ids = vec![0, 2];
        config.pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(9).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7), NodeId::new(8)],
            },
            StorageNodePgRoute {
                pg_id: 2,
                cluster_epoch: ClusterEpoch::new(9).unwrap(),
                state: PgState::Peering,
                primary_node_id: NodeId::new(8),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: Some(crate::control_plane::PgMetadataReadRoute::new(
                    NodeId::new(7),
                    crate::control_plane::PgMetadataProof::current(101, 202, 303),
                )),
                acting_set: vec![NodeId::new(8), NodeId::new(7)],
            },
        ];
        config.historical_pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: Some(crate::control_plane::PgMetadataReadRoute::new(
                    NodeId::new(8),
                    crate::control_plane::PgMetadataProof::current(404, 505, 606),
                )),
                acting_set: vec![NodeId::new(7), NodeId::new(8)],
            },
            StorageNodePgRoute {
                pg_id: 2,
                cluster_epoch: ClusterEpoch::new(6).unwrap(),
                state: PgState::Peering,
                primary_node_id: NodeId::new(8),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(8), NodeId::new(7)],
            },
        ];

        assert_eq!(
            encode_control_plane_runtime_config(&config),
            concat!(
                "argmin-storage-node-runtime-config-v5\n",
                "node_id 7\n",
                "cluster_epoch 9\n",
                "route_map_validity until 12345\n",
                "ec_shape 4 2\n",
                "socket_path 2f72756e2f6172676d696e2f73746f726167652d372e736f636b\n",
                "pg_ids 2\n",
                "0\n",
                "2\n",
                "pg_routes 2\n",
                "0 9 1 7 - - - - - - - 2 7 8\n",
                "2 9 2 8 - 7 101 1 202 5 303 2 8 7\n",
                "historical_pg_routes 2\n",
                "0 3 1 7 - 8 404 1 505 5 606 2 7 8\n",
                "2 6 2 8 - - - - - - - 2 8 7\n",
                "pending_metadata_command_recoveries 0\n",
            )
        );

        config.persist_control_plane_runtime_config().unwrap();
        let loaded = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &config.data_dir,
            config.node_id,
            config.default_ec_shape,
            &config.socket_path,
        )
        .unwrap()
        .unwrap();

        assert_storage_node_process_config_eq(&loaded, &config);
    }

    #[test]
    fn storage_node_control_plane_runtime_config_distinguishes_unknown_magic() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let path = tmp.path().join("runtime-config");
        for magic in [
            "not-a-storage-node-runtime-config",
            "argmin-storage-node-runtime-config-v03",
            "argmin-storage-node-runtime-config-vx",
        ] {
            let raw = encode_control_plane_runtime_config(&config).replacen(
                "argmin-storage-node-runtime-config-v5",
                magic,
                1,
            );
            assert!(matches!(
                decode_control_plane_runtime_config(
                    &path,
                    config.data_dir.clone(),
                    config.default_ec_shape,
                    &raw,
                ),
                Err(StorageNodeServerError::RuntimeConfigUnknownMagic {
                    path: error_path
                }) if error_path == path
            ));
        }
    }

    #[test]
    fn storage_node_runtime_config_rejects_unsupported_or_incomplete_proof_carriers() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].metadata_read_route =
            Some(crate::control_plane::PgMetadataReadRoute::new(
                NodeId::new(7),
                crate::control_plane::PgMetadataProof::current(9, 10, 11),
            ));
        let current = encode_control_plane_runtime_config(&config);
        assert!(current.contains("0 1 1 7 - 7 9 1 10 5 11 1 7\n"));
        let path = tmp.path().join("runtime-config");

        for (unsupported, expected) in [
            (
                current.replacen(" 9 1 10 5 11 ", " 9 2 10 5 11 ", 1),
                "unsupported metadata-command log-hash encoding version 2",
            ),
            (
                current.replacen(" 9 1 10 5 11 ", " 9 1 10 6 11 ", 1),
                "unsupported canonical-state digest encoding version 6",
            ),
        ] {
            assert!(matches!(
                decode_control_plane_runtime_config(
                    &path,
                    config.data_dir.clone(),
                    config.default_ec_shape,
                    &unsupported,
                ),
                Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                    if message == expected
            ));
        }

        let incomplete = current.replacen(" 9 1 10 5 11 ", " 9 - 10 5 11 ", 1);
        let error = decode_control_plane_runtime_config(
            &path,
            config.data_dir,
            config.default_ec_shape,
            &incomplete,
        )
        .unwrap_err();
        match error {
            StorageNodeServerError::RuntimeConfigInvalid { message, .. } => assert_eq!(
                message,
                "pg_routes route has an incomplete metadata read route"
            ),
            other => panic!("unexpected incomplete proof-carrier error: {other:?}"),
        }
    }

    #[test]
    fn storage_node_restart_rejects_unsupported_runtime_config_before_storage_open() {
        for version in [4_u16, 6] {
            let tmp = test_util::tempdir();
            let config = test_config(&tmp);
            prepare_private_data_dir(&config.data_dir).unwrap();
            let path = control_plane_runtime_config_path(&config.data_dir);
            let raw = encode_control_plane_runtime_config(&config).replacen(
                "argmin-storage-node-runtime-config-v5",
                &format!("argmin-storage-node-runtime-config-v{version}"),
                1,
            );
            fs::write(&path, raw).unwrap();

            assert!(matches!(
                StorageNodeBootstrap::open_control_plane_managed(
                    config.node_id,
                    &config.data_dir,
                    &config.pg_ids,
                    config.default_ec_shape,
                    &config.socket_path,
                ),
                Err(StorageNodeServerError::RuntimeConfigUnsupportedVersion {
                    path: error_path,
                    actual,
                }) if error_path == path && actual == version
            ));
            assert!(
                !config.data_dir.join(STORAGE_NODE_INCARNATION_FILE).exists(),
                "unsupported runtime config v{version} must not advance the node incarnation"
            );
            assert!(
                !storage_node_pg_dir(&config.data_dir, config.pg_ids[0]).exists(),
                "unsupported runtime config v{version} must not open PG storage"
            );
        }
    }

    #[test]
    fn storage_node_control_plane_runtime_config_rejects_reserved_validity_deadline() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let raw = encode_control_plane_runtime_config(&config).replace(
            "route_map_validity forever\n",
            &format!("route_map_validity until {}\n", u64::MAX),
        );

        assert!(matches!(
            decode_control_plane_runtime_config(
                &tmp.path().join("runtime-config"),
                tmp.path().join("node"),
                config.default_ec_shape,
                &raw,
            ),
            Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                if message.contains("reserved unbounded sentinel")
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_rejects_counts_beyond_remaining_records() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let raw = encode_control_plane_runtime_config(&config);
        let path = tmp.path().join("runtime-config");

        for label in [
            "pg_ids",
            "pg_routes",
            "historical_pg_routes",
            "pending_metadata_command_recoveries",
        ] {
            let malformed = replace_runtime_config_count(&raw, label, usize::MAX);
            assert!(matches!(
                decode_control_plane_runtime_config(
                    &path,
                    tmp.path().join("node"),
                    config.default_ec_shape,
                    &malformed,
                ),
                Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                    if message.contains("count exceeds remaining runtime config records")
            ));
        }
    }

    #[test]
    fn storage_node_control_plane_runtime_config_rejects_overflowing_acting_set_count() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let raw = encode_control_plane_runtime_config(&config);
        let mut lines: Vec<_> = raw.lines().map(str::to_owned).collect();
        let route_header = lines.iter().position(|line| line == "pg_routes 1").unwrap();
        lines[route_header + 1] = format!("0 1 1 7 - - - - - - - {} 7", usize::MAX);
        lines.push(String::new());
        let malformed = lines.join("\n");

        assert!(matches!(
            decode_control_plane_runtime_config(
                &tmp.path().join("runtime-config"),
                tmp.path().join("node"),
                config.default_ec_shape,
                &malformed,
            ),
            Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                if message.contains("acting set length mismatch")
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_load_rejects_oversized_file() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let path = control_plane_runtime_config_path(&config.data_dir);
        let file = File::create(&path).unwrap();
        file.set_len(CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES as u64 + 1)
            .unwrap();

        assert!(matches!(
            StorageNodeProcessConfig::load_control_plane_runtime_config(
                &config.data_dir,
                config.node_id,
                config.default_ec_shape,
                &config.socket_path,
            ),
            Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                if message.contains("runtime config exceeds")
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_load_rejects_symlink() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let target = tmp.path().join("runtime-config-target");
        fs::write(&target, encode_control_plane_runtime_config(&config)).unwrap();
        let path = control_plane_runtime_config_path(&config.data_dir);
        std::os::unix::fs::symlink(&target, &path).unwrap();

        assert!(matches!(
            StorageNodeProcessConfig::load_control_plane_runtime_config(
                &config.data_dir,
                config.node_id,
                config.default_ec_shape,
                &config.socket_path,
            ),
            Err(StorageNodeServerError::RuntimeConfigRead { .. })
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_load_rejects_non_regular_file() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let path = control_plane_runtime_config_path(&config.data_dir);
        fs::create_dir(&path).unwrap();

        assert!(matches!(
            StorageNodeProcessConfig::load_control_plane_runtime_config(
                &config.data_dir,
                config.node_id,
                config.default_ec_shape,
                &config.socket_path,
            ),
            Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                if message.contains("not a regular file")
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_load_rejects_fifo_without_blocking() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let path = control_plane_runtime_config_path(&config.data_dir);
        let path_bytes = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path_bytes` is a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(path_bytes.as_ptr(), 0o600) }, 0);
        let data_dir = config.data_dir.clone();
        let socket_path = config.socket_path.clone();
        let node_id = config.node_id;
        let default_ec_shape = config.default_ec_shape;
        let (result_tx, result_rx) = mpsc::channel();
        let loader = thread::spawn(move || {
            result_tx
                .send(StorageNodeProcessConfig::load_control_plane_runtime_config(
                    data_dir,
                    node_id,
                    default_ec_shape,
                    socket_path,
                ))
                .unwrap();
        });

        let (blocked, result) = match result_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => (false, result),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Unblock a regressed blocking read so the test process can join cleanly.
                drop(OpenOptions::new().write(true).open(&path).unwrap());
                (
                    true,
                    result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
                )
            }
            Err(error) => panic!("runtime config loader disconnected: {error}"),
        };
        loader.join().unwrap();

        assert!(
            !blocked,
            "runtime config FIFO open blocked before validation"
        );
        assert!(matches!(
            result,
            Err(StorageNodeServerError::RuntimeConfigInvalid { message, .. })
                if message.contains("not a regular file")
        ));
    }

    #[test]
    fn storage_node_control_plane_runtime_config_persistence_rejects_staging_symlink() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let target = tmp.path().join("staging-symlink-target");
        fs::write(&target, b"preserve this target").unwrap();
        let staging_path =
            control_plane_runtime_config_path(&config.data_dir).with_extension("tmp");
        std::os::unix::fs::symlink(&target, &staging_path).unwrap();

        let error = config.persist_control_plane_runtime_config().unwrap_err();

        assert!(matches!(
            error,
            StorageNodeServerError::RuntimeConfigWrite { source, .. }
                if source.kind() == io::ErrorKind::InvalidInput
        ));
        assert_eq!(fs::read(&target).unwrap(), b"preserve this target");
        assert!(fs::symlink_metadata(staging_path)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn storage_node_control_plane_runtime_config_persistence_replaces_stale_regular_staging_file() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        prepare_private_data_dir(&config.data_dir).unwrap();
        let path = control_plane_runtime_config_path(&config.data_dir);
        let staging_path = path.with_extension("tmp");
        fs::write(&staging_path, b"stale crash residue").unwrap();

        config.persist_control_plane_runtime_config().unwrap();

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            encode_control_plane_runtime_config(&config)
        );
        assert!(!staging_path.exists());
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn storage_node_runtime_config_staging_sync_failure_preserves_previous_config() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);
        current.persist_control_plane_runtime_config().unwrap();
        let current_contents = encode_control_plane_runtime_config(&current);
        let mut next = current.clone();
        next.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next.pg_routes[0].cluster_epoch = next.cluster_epoch;
        let post_write_called = std::cell::Cell::new(false);

        let stage_result = next.stage_control_plane_runtime_config_with_file_sync(
            || post_write_called.set(true),
            |_| Err(io::Error::other("injected runtime-config file sync failure")),
        );
        let error = match stage_result {
            Ok(_) => panic!("runtime-config staging unexpectedly survived file sync failure"),
            Err(error) => error,
        };

        let path = control_plane_runtime_config_path(&current.data_dir);
        assert!(matches!(
            error,
            StorageNodeServerError::RuntimeConfigWrite {
                path: error_path,
                source,
            } if error_path == path.with_extension("tmp")
                && source.kind() == io::ErrorKind::Other
        ));
        assert!(
            !post_write_called.get(),
            "a config whose file sync failed must not reach the staged handoff"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), current_contents);
        assert!(!path.with_extension("tmp").exists());
        let restarted = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &current.data_dir,
            current.node_id,
            current.default_ec_shape,
            &current.socket_path,
        )
        .unwrap()
        .unwrap();
        assert_storage_node_process_config_eq(&restarted, &current);
    }

    #[test]
    fn storage_node_runtime_config_directory_sync_failure_is_may_have_applied_and_retryable() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);
        current.persist_control_plane_runtime_config().unwrap();
        let mut next = current.clone();
        next.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next.pg_routes[0].cluster_epoch = next.cluster_epoch;
        let next_contents = encode_control_plane_runtime_config(&next);
        let path = control_plane_runtime_config_path(&current.data_dir);

        let staged = next.stage_control_plane_runtime_config().unwrap();
        let error = staged
            .publish_with_directory_sync(|directory| {
                assert_eq!(directory, current.data_dir);
                assert_eq!(fs::read_to_string(&path).unwrap(), next_contents);
                assert!(!path.with_extension("tmp").exists());
                Err(io::Error::other(
                    "injected runtime-config directory sync failure",
                ))
            })
            .unwrap_err();

        assert!(matches!(
            error,
            StorageNodeServerError::RuntimeConfigWrite {
                path: error_path,
                source,
            } if error_path == current.data_dir && source.kind() == io::ErrorKind::Other
        ));
        let live_namespace_after_ambiguous_publish =
            StorageNodeProcessConfig::load_control_plane_runtime_config(
                &current.data_dir,
                current.node_id,
                current.default_ec_shape,
                &current.socket_path,
            )
            .unwrap()
            .unwrap();
        assert_storage_node_process_config_eq(&live_namespace_after_ambiguous_publish, &next);

        next.persist_control_plane_runtime_config().unwrap();
        let restarted_after_retry = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &current.data_dir,
            current.node_id,
            current.default_ec_shape,
            &current.socket_path,
        )
        .unwrap()
        .unwrap();
        assert_storage_node_process_config_eq(&restarted_after_retry, &next);
    }

    #[test]
    fn storage_node_runtime_config_unpublished_staging_keeps_previous_restart_config() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);
        current.persist_control_plane_runtime_config().unwrap();
        let mut next = current.clone();
        next.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next.pg_routes[0].cluster_epoch = next.cluster_epoch;
        let path = control_plane_runtime_config_path(&current.data_dir);

        let staged = next.stage_control_plane_runtime_config().unwrap();
        assert_eq!(
            fs::read_to_string(path.with_extension("tmp")).unwrap(),
            encode_control_plane_runtime_config(&next)
        );
        drop(staged);

        assert!(!path.with_extension("tmp").exists());
        let restarted = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &current.data_dir,
            current.node_id,
            current.default_ec_shape,
            &current.socket_path,
        )
        .unwrap()
        .unwrap();
        assert_storage_node_process_config_eq(&restarted, &current);
    }

    #[test]
    fn storage_node_runtime_refresh_persists_control_plane_runtime_config() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        config.historical_pg_routes.push(StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        });

        server
            .install_control_plane_runtime_config(config.clone())
            .unwrap();
        let loaded = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &config.data_dir,
            config.node_id,
            config.default_ec_shape,
            &config.socket_path,
        )
        .unwrap()
        .unwrap();

        assert_storage_node_process_config_eq(&loaded, &config);
    }

    #[test]
    fn storage_node_incarnation_advances_and_persists() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");

        assert_eq!(advance_storage_node_incarnation(&data_dir).unwrap(), 1);
        assert_eq!(advance_storage_node_incarnation(&data_dir).unwrap(), 2);
        assert_eq!(
            std::fs::read_to_string(data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "2\n"
        );
        assert!(!data_dir.join(STORAGE_NODE_INCARNATION_TMP_FILE).exists());
    }

    #[test]
    fn storage_node_state_initialization_uses_configured_epoch_and_ec_shape() {
        const TEST_IDENTITY: &[u8] = b"storage-node-test-identity";
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        let initial_epoch = ClusterEpoch::new(7).unwrap();
        let ec_shape = EcShape { k: 4, m: 2 };

        let initialization_guard = StorageNodeStateInitializationGuard::acquire(&data_dir).unwrap();
        initialize_storage_node_state(
            &initialization_guard,
            NodeId::new(11),
            &[0, 3],
            ec_shape,
            initial_epoch,
            TEST_IDENTITY,
        )
        .unwrap();
        drop(initialization_guard);
        let initialization_guard = StorageNodeStateInitializationGuard::acquire(&data_dir).unwrap();
        initialize_storage_node_state(
            &initialization_guard,
            NodeId::new(11),
            &[0, 3],
            ec_shape,
            initial_epoch,
            TEST_IDENTITY,
        )
        .unwrap();

        assert_eq!(
            inspect_initialized_storage_node_state(&data_dir, &[0, 3], TEST_IDENTITY).unwrap(),
            StorageNodeStateInspection::default()
        );

        for pg_id in [0, 3] {
            let store = PgStore::open(&data_dir.join(format!("pg-{pg_id:04}")), pg_id).unwrap();
            assert_eq!(
                store
                    .metadata_command_replica_state()
                    .unwrap()
                    .cluster_epoch,
                initial_epoch
            );
        }
        let reopened =
            SharedStorageNode::open_with_default_ec_shape(&data_dir, &[0, 3], ec_shape).unwrap();
        assert_eq!(reopened.default_ec_shape(), ec_shape);
    }

    #[test]
    fn storage_node_restart_preserves_older_pending_command_for_topology_recovery() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let old_epoch = config.cluster_epoch;
        let current_epoch = ClusterEpoch::new(old_epoch.get() + 1).unwrap();
        let bucket = crate::tests::bucket_name("split-restart-pending-route");
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "split-restart-reservation".to_owned(),
            owner_token: "split-restart-owner".to_owned(),
            cluster_epoch: old_epoch,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "insert-delete-marker".to_owned(),
            created_at: 1,
            lease_deadline: 60_000,
            target_context: Some("object".to_owned()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                old_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: proof,
                bucket: bucket.clone(),
                key: crate::tests::object_key("object"),
                version_id: VersionId::Null,
                owner: crate::OwnerIdentity::from_principal("owner"),
                write_sequence: 1,
                last_modified_millis: 1,
                stale_payload: None,
            }),
        );
        {
            let node = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();
            let mut state = pg.metadata_command_replica_state().unwrap();
            state.cluster_epoch = current_epoch;
            pg.test_replace_metadata_command_replica_state(state)
                .unwrap();
        }

        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let heartbeat = server
            .test_storage_node()
            .control_plane_heartbeat(
                config.node_id,
                1,
                "storage-node-7",
                current_epoch,
                1_000,
                [(PgId::new(0), PgState::Peering)],
            )
            .unwrap();
        let pending = heartbeat.pg_observations[0]
            .pending_metadata_command
            .expect("split restart must report the exact preserved pending slot");
        assert_eq!(pending.cluster_epoch(), old_epoch);
        assert_eq!(pending.log_index(), command.id().log_index().get());
        assert_eq!(pending.command_checksum(), command.checksum_crc64());
        assert!(heartbeat.cluster_map_history_route_references.iter().any(
            |reference| reference
                == crate::PgClusterMapHistoryRouteReference::new(
                    crate::PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
                    old_epoch,
                    PgId::new(0),
                )
        ));
    }

    #[test]
    fn storage_node_state_initialization_guard_rejects_symlinked_native_lock() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        let external = tmp.path().join("external-lock-target");
        prepare_private_data_dir(&data_dir).unwrap();
        fs::write(&external, b"external").unwrap();
        std::os::unix::fs::symlink(
            &external,
            data_dir.join(STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME),
        )
        .unwrap();

        let error = StorageNodeStateInitializationGuard::acquire(&data_dir).unwrap_err();

        assert!(matches!(
            error,
            StorageNodeServerError::Io {
                context: "open storage-node data-dir lock",
                ..
            }
        ));
        assert_eq!(fs::read(&external).unwrap(), b"external");
    }

    #[test]
    fn storage_node_initialization_entries_hide_and_validate_native_lock() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        let guard = StorageNodeStateInitializationGuard::acquire(&data_dir).unwrap();
        assert!(guard
            .data_dir_entry_names_excluding_held_lock()
            .unwrap()
            .is_empty());
        fs::write(data_dir.join("outer-b"), b"b").unwrap();
        fs::write(data_dir.join("outer-a"), b"a").unwrap();

        assert_eq!(
            guard.data_dir_entry_names_excluding_held_lock().unwrap(),
            ["outer-a", "outer-b"]
        );

        fs::remove_file(data_dir.join(STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME)).unwrap();
        fs::write(
            data_dir.join(STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME),
            b"replacement",
        )
        .unwrap();
        assert!(matches!(
            guard.data_dir_entry_names_excluding_held_lock(),
            Err(StorageNodeServerError::DataDirLockIdentityChanged { path })
                if path == data_dir
        ));
    }

    #[test]
    fn initialized_pg_state_error_keeps_implementation_diagnostic_opaque() {
        let tmp = test_util::tempdir();

        let error = inspect_initialized_storage_node_state(
            &tmp.path().join("missing-node"),
            &[3],
            b"storage-node-test-identity",
        )
        .unwrap_err();

        assert!(matches!(
            &error,
            StorageNodeServerError::InitializedPgStateInvalid { pg_id: 3, .. }
        ));
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(
            error.to_string(),
            "storage-node PG 3 initialized durable state is invalid"
        );
        let debug = format!("{error:?}");
        assert!(!debug.contains("StoreError"));
        assert!(!debug.contains("PgDurableIdentityInvalid"));
        assert!(!debug.contains("PG directory is unavailable"));
    }

    #[test]
    fn storage_node_bootstrap_owns_raw_startup_and_persists_runtime_config() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let data_dir = tmp.path().join("node");
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let default_ec_shape = EcShape { k: 1, m: 0 };
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let now_ms = crate::clock::current_time_millis();
        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 10_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                now_ms,
            )
            .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 10_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                now_ms.saturating_add(1),
            )
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        let runtime_map = authority
            .snapshot()
            .runtime_map(now_ms.saturating_add(2))
            .unwrap();

        let bootstrap = StorageNodeBootstrap::open_control_plane_managed(
            node_id,
            &data_dir,
            &[pg_id.get()],
            default_ec_shape,
            &socket_path,
        )
        .unwrap();
        assert_eq!(bootstrap.node_incarnation(), 1);
        let initial_heartbeat = bootstrap.control_plane_heartbeat(10_000).unwrap();
        assert_eq!(initial_heartbeat.node_id, node_id);
        assert_eq!(initial_heartbeat.node_incarnation, 1);
        assert_eq!(initial_heartbeat.observed_epoch, ClusterEpoch::INITIAL);
        assert_eq!(initial_heartbeat.endpoint, socket_path.to_str().unwrap());
        assert!(initial_heartbeat.pg_observations.is_empty());

        let prepared = bootstrap.prepare(&runtime_map).unwrap();
        let config = prepared.config();
        assert_eq!(config.node_id(), node_id);
        assert_eq!(config.cluster_epoch(), runtime_map.cluster_epoch());
        assert_eq!(config.socket_path(), socket_path);
        let loaded = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &data_dir,
            node_id,
            default_ec_shape,
            &socket_path,
        )
        .unwrap()
        .unwrap();
        assert_storage_node_process_config_eq(&loaded, config);
        let server = prepared.bind().unwrap();
        let served_heartbeat = server.control_plane_heartbeat(1, 10_000).unwrap();
        assert_eq!(served_heartbeat.observed_epoch, runtime_map.cluster_epoch());
        assert_eq!(served_heartbeat.pg_observations.len(), 1);
        drop(server);

        let restarted = StorageNodeBootstrap::open_control_plane_managed(
            node_id,
            &data_dir,
            &[pg_id.get()],
            default_ec_shape,
            &socket_path,
        )
        .unwrap();
        assert_eq!(restarted.node_incarnation(), 2);
        let restart_heartbeat = restarted.control_plane_heartbeat(10_000).unwrap();
        assert_eq!(
            restart_heartbeat.observed_epoch,
            runtime_map.cluster_epoch()
        );
        assert_eq!(restart_heartbeat.endpoint, socket_path.to_str().unwrap());
        assert_eq!(restart_heartbeat.pg_observations.len(), 1);
        assert_eq!(restart_heartbeat.pg_observations[0].pg_id, pg_id);
        let restarted_server = restarted.prepare(&runtime_map).unwrap().bind().unwrap();
        let served_restart_heartbeat = restarted_server.control_plane_heartbeat(2, 10_000).unwrap();
        assert_eq!(
            served_restart_heartbeat.observed_epoch,
            runtime_map.cluster_epoch()
        );
        drop(restarted_server);
    }

    #[test]
    fn storage_node_bind_rejects_config_older_than_persisted_runtime_config() {
        let tmp = test_util::tempdir();
        let mut persisted = test_config(&tmp);
        private_socket_dir(persisted.socket_path.parent().unwrap());
        let stale = persisted.clone();
        persisted.cluster_epoch = ClusterEpoch::new(2).unwrap();
        persisted.pg_routes[0].cluster_epoch = persisted.cluster_epoch;
        persisted.persist_control_plane_runtime_config().unwrap();

        assert!(matches!(
            StorageNodeServer::bind(stale),
            Err(StorageNodeServerError::PersistedRuntimeConfigMismatch { path })
                if path == control_plane_runtime_config_path(&persisted.data_dir)
        ));
    }

    #[test]
    fn storage_node_server_advances_incarnation_while_bound() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        assert_eq!(server.advance_control_plane_node_incarnation().unwrap(), 1);
        assert_eq!(
            std::fs::read_to_string(config.data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "1\n"
        );
    }

    #[test]
    fn storage_node_server_serializes_concurrent_incarnation_advances() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let caller_count = 8;
        let barrier = Arc::new(Barrier::new(caller_count));
        let mut joins = Vec::new();

        for _ in 0..caller_count {
            let server = Arc::clone(&server);
            let barrier = Arc::clone(&barrier);
            joins.push(thread::spawn(move || {
                barrier.wait();
                server.advance_control_plane_node_incarnation().unwrap()
            }));
        }

        let mut incarnations = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        incarnations.sort_unstable();

        assert_eq!(incarnations, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            std::fs::read_to_string(config.data_dir.join(STORAGE_NODE_INCARNATION_FILE)).unwrap(),
            "8\n"
        );
        assert!(!config
            .data_dir
            .join(STORAGE_NODE_INCARNATION_TMP_FILE)
            .exists());
    }

    #[test]
    fn storage_node_incarnation_rejects_invalid_persisted_value() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&data_dir).unwrap();
        let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
        std::fs::write(&path, "0\n").unwrap();

        assert!(matches!(
            advance_storage_node_incarnation(&data_dir),
            Err(StorageNodeServerError::InvalidNodeIncarnation {
                path: error_path,
                value,
            }) if error_path == path && value == "0\n"
        ));
    }

    #[test]
    fn storage_node_incarnation_rejects_overflow() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&data_dir).unwrap();
        let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
        std::fs::write(&path, format!("{}\n", u64::MAX)).unwrap();

        assert!(matches!(
            advance_storage_node_incarnation(&data_dir),
            Err(StorageNodeServerError::NodeIncarnationOverflow { path: error_path })
                if error_path == path
        ));
    }

    #[test]
    fn storage_node_process_config_preserves_route_map_validity() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(1_500).unwrap();

        assert_eq!(config.route_map_valid_until_ms(), Some(1_500));
        assert!(config.is_route_map_valid_at(1_499));
        assert!(matches!(
            config.require_route_map_valid_at(1_500),
            Err(StorageNodeServerError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 1_500,
                now_ms: 1_500,
            }) if cluster_epoch == config.cluster_epoch
        ));
    }

    #[test]
    fn storage_node_server_rejects_expired_route_map_for_serving_rpc() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let error = server
            .connection_handler()
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(error.message.contains("route map"));
        assert!(error.message.contains("expired"));
    }

    #[test]
    fn metadata_command_pg_lock_release_allows_expired_route_map_cleanup() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let mut session =
            StorageNodeSession::new(Arc::clone(&server.read_handles), Arc::clone(&server._node));
        session
            .acquire_metadata_command_pg_lock(
                &server.metadata_command_locks,
                config.node_id,
                StorageNodeMetadataCommandLockBinding {
                    pg_id: PgId::new(0),
                    cluster_epoch: config.cluster_epoch,
                    authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                },
                None,
            )
            .unwrap();
        assert!(session.holds_metadata_command_pg_lock(PgId::new(0)));

        let request = StorageRpcMetadataCommandStateRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
        };
        let response = handler
            .metadata_command_pg_lock_release_response(&mut session, request)
            .unwrap();
        decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();

        assert!(!session.holds_metadata_command_pg_lock(PgId::new(0)));
    }

    #[test]
    fn bucket_write_reservation_release_allows_expired_route_map_cleanup() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("expired-route-reservation-cleanup");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let record = {
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
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "reservation-expired-route-cleanup",
                    owner_token: "owner-token-expired-route-cleanup",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: "put-object",
                    created_at: 10,
                    lease_deadline: 20,
                    target_context: Some("key=a"),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            record
        };

        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let route_permit = handler
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let serving_error = handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(serving_error.code, StorageRpcErrorCode::StaleShardLocation);

        let request = StorageRpcBucketWriteReservationRecordRequest {
            node_id: config.node_id,
            route_cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            record: record.clone(),
        };
        let response = handler
            .bucket_write_reservation_release_response(&route_permit, request)
            .unwrap();
        decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();

        let pg = server._node.get_pg(0).unwrap();
        assert!(PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &bucket,
            &record.reservation_id,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn bucket_write_drain_capabilities_separate_active_and_retained_authority() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("bucket-write-drain-capability");
        crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcBucketRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
        };
        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_bucket_route(&active_permit, &request, "test bucket write drain")
                .unwrap()
        });
        let drain = crate::clock::with_time_override(1_000, || {
            let drain = route
                .begin_write_drain(
                    "captured-route-drain",
                    "captured-route-drain-owner",
                    config.cluster_epoch,
                    1_000,
                    4_000,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap();
            let drain = route.heartbeat_write_drain(&drain, 4_500).unwrap();
            assert!(route.write_drain_exists().unwrap());
            assert_eq!(route.write_drain().unwrap(), Some(drain.clone()));
            assert_eq!(route.clear_expired_write_drain(4_000).unwrap(), None);
            drain
        });

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

        crate::clock::with_time_override(6_000, || {
            let expired_operations = [
                (
                    "begin",
                    route
                        .begin_write_drain(
                            "expired-route-drain",
                            "expired-route-drain-owner",
                            config.cluster_epoch,
                            6_000,
                            9_000,
                            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                        )
                        .map(|_| ()),
                ),
                (
                    "heartbeat",
                    route.heartbeat_write_drain(&drain, 9_000).map(|_| ()),
                ),
                (
                    "clear expired",
                    route.clear_expired_write_drain(6_000).map(|_| ()),
                ),
                ("exists", route.write_drain_exists().map(|_| ())),
                ("get", route.write_drain().map(|_| ())),
            ];
            for (operation, result) in expired_operations {
                match result {
                    Err(StorageNodeBucketRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                        assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                    }
                    Err(StorageNodeBucketRouteError::Bucket(error)) => {
                        panic!("captured route should expire before drain {operation}: {error}")
                    }
                    Ok(()) => panic!("expired captured route performed drain {operation}"),
                }
            }
        });
        let assert_drain_unchanged = || {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket).unwrap(),
                Some(drain.clone())
            );
        };
        assert_drain_unchanged();

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_write_drain_route(
            &active_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &drain,
            "test bucket write drain clear",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active permit constructed retained drain authority"),
        }
        assert_drain_unchanged();

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_write_drain_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &drain,
            "test bucket write drain clear",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign permit constructed retained drain authority"),
        }
        assert_drain_unchanged();

        crate::clock::with_time_override(6_000, || {
            handler
                .retained_bucket_write_drain_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    &drain,
                    "test bucket write drain clear",
                )
                .unwrap()
                .clear()
                .unwrap();
        });
        let pg = server._node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
                .unwrap()
                .is_none(),
            "retained capability must clear the exact drain after active route expiry"
        );
    }

    #[test]
    fn bucket_delete_finalize_claim_capabilities_separate_active_and_retained_authority() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("bucket-delete-finalize-claim-capability");
        let bucket_incarnation_generation = crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcBucketRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
        };
        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_bucket_route(
                    &active_permit,
                    &request,
                    "test bucket delete finalize claim",
                )
                .unwrap()
        });
        let claim = crate::clock::with_time_override(1_000, || {
            let claim = route
                .acquire_bucket_delete_finalize_claim(
                    bucket_incarnation_generation,
                    "captured-route-finalize-claim",
                    "captured-route-finalize-owner",
                    config.cluster_epoch,
                    1_000,
                    None,
                    1_000,
                )
                .unwrap()
                .expect("active route should acquire a finalizer claim");
            assert_eq!(
                route.bucket_delete_finalize_claim().unwrap(),
                Some(claim.clone())
            );
            claim
        });

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            let expired_operations = [
                (
                    "acquire",
                    route
                        .acquire_bucket_delete_finalize_claim(
                            bucket_incarnation_generation,
                            "expired-route-finalize-claim",
                            "expired-route-finalize-owner",
                            config.cluster_epoch,
                            6_000,
                            Some(9_000),
                            6_000,
                        )
                        .map(|_| ()),
                ),
                ("get", route.bucket_delete_finalize_claim().map(|_| ())),
            ];
            for (operation, result) in expired_operations {
                match result {
                    Err(StorageNodeBucketRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                        assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                    }
                    Err(StorageNodeBucketRouteError::Bucket(error)) => {
                        panic!("captured route should expire before claim {operation}: {error}")
                    }
                    Ok(()) => panic!("expired captured route performed claim {operation}"),
                }
            }
        });
        let assert_claim_unchanged = || {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::bucket_delete_finalize_claim(&*pg, &bucket).unwrap(),
                Some(claim.clone())
            );
        };
        assert_claim_unchanged();

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_delete_finalize_claim_route(
            &active_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &claim,
            "test bucket delete finalize claim release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active permit constructed retained finalizer-claim authority"),
        }
        assert_claim_unchanged();

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_bucket_delete_finalize_claim_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &claim,
            "test bucket delete finalize claim release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign permit constructed retained finalizer-claim authority"),
        }
        assert_claim_unchanged();

        crate::clock::with_time_override(6_000, || {
            handler
                .retained_bucket_delete_finalize_claim_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    &claim,
                    "test bucket delete finalize claim release",
                )
                .unwrap()
                .release()
                .unwrap();
        });
        let pg = server._node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::bucket_delete_finalize_claim(&*pg, &bucket)
                .unwrap()
                .is_none(),
            "retained capability must release the exact claim after active route expiry"
        );
    }

    #[test]
    fn lifecycle_sweep_claim_capabilities_separate_active_and_retained_authority() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("lifecycle-sweep-claim-capability");
        let bucket_incarnation_generation = crate::clock::with_time_override(1_000, || {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation
        });
        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let request = StorageRpcBucketRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            bucket: bucket.clone(),
        };
        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_bucket_route(&active_permit, &request, "test lifecycle sweep claim")
                .unwrap()
        });
        let claim = crate::clock::with_time_override(1_000, || {
            route
                .acquire_lifecycle_sweep_claim(
                    bucket_incarnation_generation,
                    "captured-route-lifecycle-claim",
                    "captured-route-lifecycle-owner",
                    config.cluster_epoch,
                    1_000,
                    Some(4_500),
                    1_000,
                )
                .unwrap()
                .expect("active route should acquire a lifecycle claim")
        });
        let heartbeat = crate::clock::with_time_override(1_500, || {
            route
                .heartbeat_lifecycle_sweep_claim(&claim, 1_500, None)
                .unwrap()
        });
        let error_record = crate::clock::with_time_override(1_500, || {
            route
                .record_lifecycle_sweep_claim_error(&heartbeat, "transient lifecycle failure")
                .unwrap()
        });

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_500, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            let expired_operations = [
                (
                    "acquire",
                    route
                        .acquire_lifecycle_sweep_claim(
                            bucket_incarnation_generation,
                            "expired-route-lifecycle-claim",
                            "expired-route-lifecycle-owner",
                            config.cluster_epoch,
                            6_000,
                            Some(9_000),
                            6_000,
                        )
                        .map(|_| ()),
                ),
                (
                    "heartbeat",
                    route
                        .heartbeat_lifecycle_sweep_claim(&error_record, 6_000, Some(9_000))
                        .map(|_| ()),
                ),
                (
                    "record error",
                    route
                        .record_lifecycle_sweep_claim_error(&error_record, "must not be recorded")
                        .map(|_| ()),
                ),
            ];
            for (operation, result) in expired_operations {
                match result {
                    Err(StorageNodeBucketRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                        assert!(error.message.contains("expired at 5000ms, now 6000ms"));
                    }
                    Err(StorageNodeBucketRouteError::Bucket(error)) => {
                        panic!("captured route should expire before claim {operation}: {error}")
                    }
                    Ok(()) => panic!("expired captured route performed claim {operation}"),
                }
            }
        });
        let assert_claim_unchanged = || {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::acquire_lifecycle_sweep_claim(
                    &*pg,
                    &bucket,
                    bucket_incarnation_generation,
                    &error_record.claim_id,
                    &error_record.owner_token,
                    error_record.cluster_epoch,
                    error_record.claimed_at,
                    error_record.lease_deadline,
                    6_000,
                )
                .unwrap(),
                Some(error_record.clone())
            );
        };
        assert_claim_unchanged();

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_lifecycle_sweep_claim_route(
            &active_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &error_record,
            "test lifecycle sweep claim release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active permit constructed retained lifecycle-claim authority"),
        }
        assert_claim_unchanged();

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.retained_lifecycle_sweep_claim_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &error_record,
            "test lifecycle sweep claim release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign permit constructed retained lifecycle-claim authority"),
        }
        assert_claim_unchanged();

        crate::clock::with_time_override(6_000, || {
            handler
                .retained_lifecycle_sweep_claim_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    &error_record,
                    "test lifecycle sweep claim release",
                )
                .unwrap()
                .release()
                .unwrap();
        });
        let pg = server._node.get_pg(0).unwrap();
        let replacement = PgMetadataStore::acquire_lifecycle_sweep_claim(
            &*pg,
            &bucket,
            bucket_incarnation_generation,
            "post-release-lifecycle-claim",
            "post-release-lifecycle-owner",
            config.cluster_epoch,
            6_000,
            None,
            6_000,
        )
        .unwrap()
        .expect("retained capability must release the exact non-expiring claim");
        PgMetadataStore::release_lifecycle_sweep_claim(
            &*pg,
            &replacement.bucket,
            replacement.bucket_incarnation_generation,
            &replacement.claim_id,
            &replacement.owner_token,
            replacement.cluster_epoch,
        )
        .unwrap();
    }

    #[test]
    fn metadata_command_proof_release_requires_exact_retained_capability() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let bucket = crate::tests::bucket_name("retained-metadata-command-proof-cleanup");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let record = {
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
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "retained-metadata-command-proof",
                    owner_token: "retained-metadata-command-proof-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: "put-object",
                    created_at: 10,
                    lease_deadline: 20,
                    target_context: Some("key=a"),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            record
        };
        let proof = BucketWriteReservationProof::from(&record);

        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let retained_permit = handler
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let active_permit = handler
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);

        let assert_record_unchanged = || {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::durable_bucket_write_reservation(
                    &*pg,
                    &bucket,
                    &record.reservation_id,
                )
                .unwrap(),
                Some(record.clone())
            );
        };

        match handler.retained_metadata_command_proof_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &proof,
            "test metadata command proof release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign permit constructed retained proof authority"),
        }
        assert_record_unchanged();

        match handler.retained_metadata_command_proof_route(
            &active_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &proof,
            "test metadata command proof release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active permit constructed retained proof authority"),
        }
        assert_record_unchanged();

        let mismatched_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
        match handler.retained_metadata_command_proof_route(
            &retained_permit,
            config.node_id,
            mismatched_epoch,
            PgId::new(0),
            &proof,
            "test metadata command proof release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
                assert!(error.message.contains("does not match proof epoch"));
            }
            Ok(_) => panic!("mismatched route epoch constructed retained proof authority"),
        }
        assert_record_unchanged();

        let route = handler
            .retained_metadata_command_proof_route(
                &retained_permit,
                config.node_id,
                config.cluster_epoch,
                PgId::new(0),
                &proof,
                "test metadata command proof release",
            )
            .unwrap();
        route.release().unwrap();
        let pg = server._node.get_pg(0).unwrap();
        assert!(PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &bucket,
            &record.reservation_id,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn bucket_write_reservation_release_uses_retained_historical_primary_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let historical_epoch = config.cluster_epoch;
        let bucket = crate::tests::bucket_name("historical-route-reservation-cleanup");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let record = {
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
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "reservation-historical-route-cleanup",
                    owner_token: "owner-token-historical-route-cleanup",
                    cluster_epoch: historical_epoch,
                    operation_kind: "put-object",
                    created_at: 10,
                    lease_deadline: 20,
                    target_context: Some("key=a"),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            record
        };

        let historical_route = config.pg_routes[0].clone();
        config.cluster_epoch = ClusterEpoch::new(historical_epoch.get() + 1).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        config.pg_routes[0].primary_node_id = NodeId::new(8);
        config.pg_routes[0].acting_set = vec![config.node_id, NodeId::new(8)];
        config.historical_pg_routes.push(historical_route);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();
        let route_permit = handler
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let request = StorageRpcBucketWriteReservationRecordRequest {
            node_id: config.node_id,
            route_cluster_epoch: historical_epoch,
            pg_id: PgId::new(0),
            record: record.clone(),
        };
        let response = handler
            .bucket_write_reservation_release_response(&route_permit, request)
            .unwrap();
        decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap();

        let pg = server._node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                &record.reservation_id,
            )
            .unwrap()
            .is_none(),
            "retained cleanup must use the historical route primary rather than the successor"
        );
    }

    #[test]
    fn storage_node_runtime_refresh_allows_route_table_changes() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);
        let mut candidate = current.clone();
        candidate.cluster_epoch = ClusterEpoch::new(2).unwrap();
        candidate.route_map_validity = RouteMapValidity::until_ms(3_000).unwrap();
        candidate.pg_ids = vec![0, 1];
        candidate.pg_routes.push(StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: candidate.cluster_epoch,
            state: PgState::Peering,
            primary_node_id: candidate.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![candidate.node_id],
        });
        candidate.pg_routes[0].cluster_epoch = candidate.cluster_epoch;

        candidate.validate_runtime_refresh_from(&current).unwrap();
    }

    #[test]
    fn storage_node_runtime_refresh_rejects_process_identity_changes() {
        let tmp = test_util::tempdir();
        let current = test_config(&tmp);

        let mut changed_node = current.clone();
        changed_node.node_id = NodeId::new(8);
        assert!(matches!(
            changed_node.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshNodeChanged {
                current: 7,
                candidate: 8,
            })
        ));

        let mut changed_data = current.clone();
        changed_data.data_dir = tmp.path().join("other-node");
        assert!(matches!(
            changed_data.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshDataDirChanged { .. })
        ));

        let mut changed_ec = current.clone();
        changed_ec.default_ec_shape = EcShape { k: 2, m: 1 };
        assert!(matches!(
            changed_ec.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshEcShapeChanged { .. })
        ));

        let mut changed_socket = current.clone();
        changed_socket.socket_path = tmp.path().join("sock").join("other.sock");
        assert!(matches!(
            changed_socket.validate_runtime_refresh_from(&current),
            Err(StorageNodeServerError::RuntimeRefreshSocketPathChanged { .. })
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_pg_set_changes() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut candidate = config.clone();
        candidate.pg_ids = vec![0, 1];
        candidate.pg_routes.push(test_route(1));

        assert!(matches!(
            server.install_control_plane_runtime_config(candidate),
            Err(StorageNodeServerError::RuntimeRefreshPgSetChanged {
                current,
                candidate,
            }) if current == vec![0] && candidate == vec![0, 1]
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_validates_route_table() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut candidate = config.clone();
        candidate.cluster_epoch = ClusterEpoch::new(2).unwrap();

        assert!(matches!(
            server.install_control_plane_runtime_config(candidate),
            Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: 0,
                route_epoch,
                config_epoch,
            }) if route_epoch == ClusterEpoch::new(1).unwrap()
                && config_epoch == ClusterEpoch::new(2).unwrap()
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_epoch_downgrade() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut stale = config.clone();
        stale.cluster_epoch = ClusterEpoch::new(1).unwrap();
        stale.pg_routes[0].cluster_epoch = stale.cluster_epoch;

        assert!(matches!(
            server.install_control_plane_runtime_config(stale),
            Err(StorageNodeServerError::RuntimeRefreshEpochDowngrade {
                current,
                candidate,
            }) if current == ClusterEpoch::new(2).unwrap()
                && candidate == ClusterEpoch::new(1).unwrap()
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_same_epoch_unbounded_validity() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let mut unbounded = config.clone();
        unbounded.route_map_validity = RouteMapValidity::Forever;
        assert!(matches!(
            server.install_control_plane_runtime_config(unbounded),
            Err(
                StorageNodeServerError::RuntimeRefreshUnboundedRouteMapValidity {
                    candidate
                }
            ) if candidate == config.cluster_epoch
        ));
    }

    #[test]
    fn storage_node_runtime_config_install_rejects_later_epoch_unbounded_validity() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let mut unbounded = config.clone();
        unbounded.cluster_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1)
            .expect("test epoch should not overflow");
        unbounded.route_map_validity = RouteMapValidity::Forever;
        for route in &mut unbounded.pg_routes {
            route.cluster_epoch = unbounded.cluster_epoch;
        }

        assert!(matches!(
            server.install_control_plane_runtime_config(unbounded),
            Err(
                StorageNodeServerError::RuntimeRefreshUnboundedRouteMapValidity {
                    candidate
                }
            ) if candidate == ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap()
        ));
        assert_eq!(server.config_snapshot().cluster_epoch, config.cluster_epoch);
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(5_000)
        );
    }

    #[test]
    fn storage_node_runtime_config_install_accepts_same_epoch_shorter_bounded_validity() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut shorter = config;
        shorter.route_map_validity = RouteMapValidity::until_ms(4_000).unwrap();

        server
            .install_control_plane_runtime_config(shorter)
            .unwrap();
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(4_000)
        );
    }

    #[test]
    fn storage_node_runtime_config_install_accepts_bounded_authoritative_refresh() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::Forever;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut authoritative = config.clone();
        authoritative.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();

        server
            .install_control_plane_runtime_config(authoritative)
            .unwrap();
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(5_000)
        );
    }

    #[test]
    fn expired_route_map_rejects_new_work_but_allows_cleanup_route_validation() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let handler = server.connection_handler();

        let new_work_error = handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(new_work_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(new_work_error
            .message
            .contains("storage-node route map for cluster epoch"));

        handler
            .validate_pg_route_for_cleanup(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap();

        let stale_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
        let stale_epoch_error = handler
            .validate_pg_route_for_cleanup(config.node_id, stale_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(
            stale_epoch_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
    }

    #[test]
    fn storage_node_process_config_builds_control_plane_heartbeat() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids = vec![0, 1];
        config.pg_routes = vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: config.cluster_epoch,
                state: PgState::Peering,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7)],
            },
            StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: config.cluster_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7)],
            },
        ];
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        let heartbeat = config.control_plane_heartbeat(&node, 12, 2_000).unwrap();

        assert_eq!(heartbeat.node_id, config.node_id);
        assert_eq!(heartbeat.node_incarnation, 12);
        assert_eq!(heartbeat.endpoint, config.socket_path.to_str().unwrap());
        assert_eq!(heartbeat.observed_epoch, config.cluster_epoch);
        assert_eq!(heartbeat.requested_lease_duration_ms, 2_000);
        assert_eq!(
            heartbeat.cluster_map_history_route_references,
            node.cluster_map_history_route_references().unwrap()
        );
        assert_eq!(heartbeat.pg_observations.len(), 2);
        assert_eq!(heartbeat.pg_observations[0].pg_id, PgId::new(0));
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Peering);
        assert_eq!(heartbeat.pg_observations[1].pg_id, PgId::new(1));
        assert_eq!(heartbeat.pg_observations[1].state, PgState::Active);
        for observation in &heartbeat.pg_observations {
            let metadata_state = {
                let pg = node.get_pg(observation.pg_id.get()).unwrap();
                pg.metadata_command_replica_state().unwrap()
            };
            assert_eq!(
                observation.metadata_proof.applied_log_index,
                metadata_state.applied_log_index
            );
            assert_eq!(
                observation.metadata_proof.applied_log_hash,
                metadata_state.applied_log_hash
            );
            assert_eq!(
                observation.metadata_proof.state_digest,
                metadata_state.state_digest
            );
        }
    }

    #[test]
    fn storage_node_process_config_rejects_route_epoch_mismatch_for_heartbeat() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(1).unwrap();
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        assert!(matches!(
            config.control_plane_heartbeat(&node, 12, 2_000),
            Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: 0,
                route_epoch,
                config_epoch,
            }) if route_epoch == ClusterEpoch::new(1).unwrap()
                && config_epoch == ClusterEpoch::new(2).unwrap()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn storage_node_process_config_rejects_non_utf8_heartbeat_endpoint() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from(OsString::from_vec(vec![0xff]));
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids).unwrap();

        assert!(matches!(
            config.control_plane_heartbeat(&node, 12, 2_000),
            Err(StorageNodeServerError::SocketPathNotUtf8 { path }) if path == config.socket_path
        ));
    }

    #[test]
    fn storage_node_server_builds_control_plane_heartbeat() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();

        let heartbeat = server.control_plane_heartbeat(12, 2_000).unwrap();

        assert_eq!(heartbeat.node_id, config.node_id);
        assert_eq!(heartbeat.node_incarnation, 12);
        assert_eq!(heartbeat.endpoint, config.socket_path.to_str().unwrap());
        assert_eq!(heartbeat.observed_epoch, config.cluster_epoch);
        assert_eq!(heartbeat.requested_lease_duration_ms, 2_000);
        assert_eq!(
            heartbeat.cluster_map_history_route_references.summary(),
            server
                ._node
                .cluster_map_history_reference_summary()
                .unwrap()
        );
        assert_eq!(heartbeat.pg_observations.len(), 1);
        assert_eq!(heartbeat.pg_observations[0].pg_id, PgId::new(0));
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Active);
        let metadata_state = {
            let pg = server._node.get_pg(0).unwrap();
            pg.metadata_command_replica_state().unwrap()
        };
        assert_eq!(
            heartbeat.pg_observations[0]
                .metadata_proof
                .applied_log_index,
            metadata_state.applied_log_index
        );
        assert_eq!(
            heartbeat.pg_observations[0].metadata_proof.applied_log_hash,
            metadata_state.applied_log_hash
        );
        assert_eq!(
            heartbeat.pg_observations[0].metadata_proof.state_digest,
            metadata_state.state_digest
        );
    }

    #[test]
    fn runtime_map_config_opens_non_acting_pgs_without_heartbeat_observation() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let acting_node_id = NodeId::new(8);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage-7.sock");
        let acting_socket_path = tmp.path().join("sock").join("storage-8.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let base_time_ms = crate::clock::current_time_millis();
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        for (idx, (heartbeat_node_id, heartbeat_socket_path)) in [
            (node_id, socket_path.clone()),
            (acting_node_id, acting_socket_path),
        ]
        .into_iter()
        .enumerate()
        {
            let heartbeat_at_ms = base_time_ms + (idx as u64 * 2);
            authority
                .set_node_membership(heartbeat_node_id, NodeMembershipState::Active)
                .unwrap();
            let first = authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: crate::control_plane::MAX_HEARTBEAT_LEASE_MS,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms,
                )
                .unwrap();
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: first.cluster_epoch(),
                        requested_lease_duration_ms: crate::control_plane::MAX_HEARTBEAT_LEASE_MS,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms + 1,
                )
                .unwrap();
        }
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        authority
            .set_pg_acting_set(pg_id, vec![acting_node_id])
            .unwrap();
        let observed_epoch = authority.snapshot().cluster_epoch();
        for (idx, (heartbeat_node_id, heartbeat_socket_path)) in [
            (node_id, socket_path.clone()),
            (
                acting_node_id,
                tmp.path().join("sock").join("storage-8.sock"),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let heartbeat_at_ms = base_time_ms + 4 + idx as u64;
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch,
                        requested_lease_duration_ms: crate::control_plane::MAX_HEARTBEAT_LEASE_MS,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms,
                )
                .unwrap();
        }

        let runtime_map = authority.snapshot().runtime_map(base_time_ms + 6).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();

        assert_eq!(config.pg_ids, vec![pg_id.get()]);
        assert_eq!(config.pg_routes.len(), 1);
        assert_eq!(config.pg_routes[0].acting_set, vec![acting_node_id]);

        let server = StorageNodeServer::bind(config).unwrap();
        let heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert!(heartbeat.pg_observations.is_empty());

        let live_error = server
            .connection_handler()
            .validate_pg_route(node_id, runtime_map.cluster_epoch(), pg_id)
            .unwrap_err();
        assert!(matches!(
            live_error.code,
            StorageRpcErrorCode::InactivePgRoute | StorageRpcErrorCode::NonActingSetAccess
        ));
        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let request = StorageRpcHistoricalShardReadRequest {
            location: ShardLocation::new(
                runtime_map.cluster_epoch(),
                DataPgId::new_for_test(pg_id),
                ShardIndex::new(0),
                node_id,
            )
            .into(),
            shard_key: test_shard_key(0),
        };
        let error = match server.connection_handler().retained_shard_inspection_route(
            &retained_permit,
            &request,
            "test historical shard inspection",
        ) {
            Err(error) => error,
            Ok(_) => panic!("non-acting current route created historical inspection authority"),
        };
        assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
    }

    #[test]
    fn runtime_map_storage_node_server_heartbeat_updates_authority_pg_observation() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let server = StorageNodeServer::bind(config).unwrap();
        let heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert_eq!(heartbeat.observed_epoch, runtime_map.cluster_epoch());
        assert_eq!(heartbeat.pg_observations.len(), 1);
        assert_eq!(heartbeat.pg_observations[0].pg_id, pg_id);
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Peering);
        let proof = heartbeat.pg_observations[0].metadata_proof;

        let lease = server
            .heartbeat_control_plane(&mut authority, 12, 1_000, 1_003)
            .unwrap();
        assert_eq!(lease.node_id(), node_id);
        assert_eq!(lease.cluster_epoch(), runtime_map.cluster_epoch());

        let observation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .pg_observation(pg_id)
            .unwrap();
        assert_eq!(observation.state(), PgState::Peering);
        assert_eq!(observation.observed_epoch(), runtime_map.cluster_epoch());
        assert_eq!(observation.observed_at_ms(), 1_003);
        assert_eq!(observation.metadata_proof(), proof);
    }

    #[test]
    fn storage_node_refreshes_control_plane_runtime_map_candidate() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let mut config = config;
        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let stale_error = server
            .connection_handler()
            .validate_pg_route(node_id, runtime_map.cluster_epoch(), pg_id)
            .unwrap_err();
        assert_eq!(stale_error.code, StorageRpcErrorCode::StaleShardLocation);

        let refresh = server
            .refresh_control_plane_runtime_map(&mut authority, 12, 1_000, 1_003)
            .unwrap();
        assert_eq!(refresh.lease().node_id(), node_id);
        assert_eq!(
            refresh.lease().cluster_epoch(),
            refresh.runtime_map().cluster_epoch()
        );
        assert_eq!(refresh.next_config().node_id, node_id);
        assert_eq!(
            refresh.next_config().cluster_epoch,
            refresh.runtime_map().cluster_epoch()
        );
        assert_eq!(refresh.next_config().data_dir, config.data_dir);
        assert_eq!(refresh.next_config().pg_ids, vec![pg_id.get()]);
        assert_eq!(refresh.next_config().pg_routes.len(), 1);
        assert_eq!(refresh.next_config().pg_routes[0].state, PgState::Active);

        let installed_epoch = refresh.next_config().cluster_epoch;
        let lease = server.install_control_plane_refresh(refresh).unwrap();
        assert_eq!(lease.node_id(), node_id);
        assert!(
            !lease.serving(),
            "peering completion bumps the epoch before the node observes it"
        );
        let installed_config = server.config_snapshot();
        assert_eq!(installed_config.cluster_epoch, installed_epoch);
        assert_eq!(installed_config.pg_routes[0].state, PgState::Active);
        assert!(installed_config.route_map_valid_until_ms().is_some());
        let installed_heartbeat = server.control_plane_heartbeat(12, 1_000).unwrap();
        assert_eq!(installed_heartbeat.observed_epoch, installed_epoch);
        assert_eq!(
            installed_heartbeat.pg_observations[0].state,
            PgState::Active
        );
        assert!(
            authority
                .snapshot()
                .node(node_id)
                .unwrap()
                .pg_observation(pg_id)
                .is_none(),
            "peering completion clears observations until the node heartbeats the new epoch"
        );
        let active_lease = server
            .heartbeat_control_plane(&mut authority, 12, 1_000, 1_004)
            .unwrap();
        assert!(active_lease.serving());

        let observation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .pg_observation(pg_id)
            .unwrap();
        assert_eq!(observation.state(), PgState::Active);
        assert_eq!(observation.observed_epoch(), installed_epoch);
    }

    #[test]
    fn storage_node_refresh_config_merges_retained_history_delta() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let current_runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let mut current = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &current_runtime_map,
        )
        .unwrap();
        let retained_epoch = ClusterEpoch::new(1).unwrap();
        current.historical_pg_routes.push(StorageNodePgRoute::from(
            &PgRouteSnapshot::reconstructed(
                retained_epoch,
                pg_id,
                node_id,
                vec![node_id],
                PgState::Active,
            ),
        ));

        authority
            .set_node_membership(NodeId::new(8), NodeMembershipState::Active)
            .unwrap();
        let delta_runtime_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(
                1_003,
                node_id,
                current_runtime_map.cluster_epoch(),
            )
            .unwrap();
        assert!(!delta_runtime_map
            .historical_pg_routes()
            .iter()
            .any(|route| route.cluster_epoch() == retained_epoch));

        let next = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &delta_runtime_map,
            crate::PgClusterMapHistoryReferenceSummary {
                oldest_live_placement_epoch: Some(retained_epoch),
                oldest_durable_backfill_epoch: None,
                oldest_metadata_command_resource_epoch: None,
                oldest_object_payload_reclaim_claim_epoch: None,
            },
        )
        .unwrap();
        assert!(next
            .historical_pg_routes
            .iter()
            .any(|route| route.cluster_epoch == retained_epoch && route.pg_id == pg_id.get()));
        assert!(next.historical_pg_routes.iter().any(|route| {
            route.cluster_epoch == current_runtime_map.cluster_epoch() && route.pg_id == pg_id.get()
        }));
    }

    #[test]
    fn storage_node_refresh_installs_remote_backfill_source_route() {
        let tmp = test_util::tempdir();
        let source_node_id = NodeId::new(1);
        let metadata_node_id = NodeId::new(2);
        let unrelated_node_id = NodeId::new(3);
        let pg_id = PgId::new(0);
        let source_socket_path = tmp.path().join("sock").join("source.sock");
        private_socket_dir(source_socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        for (index, node_id) in [source_node_id, metadata_node_id, unrelated_node_id]
            .into_iter()
            .enumerate()
        {
            authority
                .set_node_membership(node_id, NodeMembershipState::Active)
                .unwrap();
            let endpoint = if node_id == source_node_id {
                source_socket_path.clone()
            } else {
                tmp.path().join("sock").join(format!("node-{index}.sock"))
            };
            let first = authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 12,
                        endpoint: endpoint.to_str().unwrap().to_owned(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    1_000 + index as u64 * 2,
                )
                .unwrap();
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 12,
                        endpoint: endpoint.to_str().unwrap().to_owned(),
                        observed_epoch: first.cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    1_001 + index as u64 * 2,
                )
                .unwrap();
        }

        authority
            .set_pg_acting_set(pg_id, vec![source_node_id])
            .unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        authority
            .set_pg_acting_set(pg_id, vec![metadata_node_id])
            .unwrap();
        let current_epoch = authority.snapshot().cluster_epoch();
        let history_references = crate::PgClusterMapHistoryRouteReferences::try_from_iter([
            crate::PgClusterMapHistoryRouteReference::new(
                crate::PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
                source_epoch,
                pg_id,
            ),
        ])
        .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id: metadata_node_id,
                    node_incarnation: 12,
                    endpoint: tmp
                        .path()
                        .join("sock")
                        .join("node-1.sock")
                        .to_str()
                        .unwrap()
                        .to_owned(),
                    observed_epoch: current_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(2).unwrap(),
                    cluster_map_history_route_references: history_references,
                    pg_observations: Vec::new(),
                },
                2_000,
            )
            .unwrap();

        let refresh_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(2_001, source_node_id, current_epoch)
            .unwrap();
        assert!(refresh_map
            .historical_pg_routes()
            .iter()
            .any(|route| { route.cluster_epoch() == source_epoch && route.pg_id() == pg_id }));
        let mut current = StorageNodeProcessConfig::from_runtime_map(
            source_node_id,
            tmp.path().join("source-node"),
            EcShape { k: 1, m: 0 },
            &refresh_map,
        )
        .unwrap();
        current.historical_pg_routes.clear();
        let next = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &refresh_map,
            crate::PgClusterMapHistoryReferenceSummary::default(),
        )
        .unwrap();
        assert!(next
            .historical_pg_routes
            .iter()
            .any(|route| { route.cluster_epoch == source_epoch && route.pg_id == pg_id.get() }));

        let server = crate::clock::with_time_override(2_001, || {
            StorageNodeServer::bind(next.clone()).unwrap()
        });
        let shard_key = test_shard_key(0);
        let payload = b"globally protected backfill source";
        let ack = server
            ._node
            .write_shard_file_if_absent(pg_id.get(), &shard_key, payload)
            .unwrap();
        server
            ._node
            .get_pg(pg_id.get())
            .unwrap()
            .register_written_shards_batch_exact(&[(&shard_key, ack)])
            .unwrap();
        let permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let loaded = server
            .connection_handler()
            .retained_shard_ack_inspection_route(
                &permit,
                &StorageRpcShardAckItemRequest {
                    node_id: source_node_id,
                    cluster_epoch: source_epoch,
                    pg_id,
                    shard_key,
                },
                "test globally protected historical shard ack inspection",
            )
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(loaded, ack);
    }

    #[test]
    fn storage_node_refresh_config_prunes_unreferenced_history_growth() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let mut current = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        for step in 0_u32..8 {
            authority
                .set_node_membership(NodeId::new(100 + step), NodeMembershipState::Active)
                .unwrap();
            let delta_runtime_map = authority
                .snapshot()
                .runtime_map_for_storage_node_refresh(
                    2_000 + u64::from(step),
                    node_id,
                    current.cluster_epoch,
                )
                .unwrap();
            current = StorageNodeProcessConfig::from_runtime_map_refresh(
                &current,
                &delta_runtime_map,
                crate::PgClusterMapHistoryReferenceSummary::default(),
            )
            .unwrap();
            assert_eq!(
                current.historical_pg_routes.len(),
                current.pg_routes.len(),
                "unreferenced local history should retain only the previous current route set"
            );
        }
    }

    #[test]
    fn storage_node_refresh_config_keeps_predecessor_for_retained_floor() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        for step in 0_u32..10 {
            authority
                .set_node_membership(NodeId::new(200 + step), NodeMembershipState::Active)
                .unwrap();
        }

        let runtime_map = authority.snapshot().runtime_map(1_100).unwrap();
        let mut current = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        for raw_epoch in [2, 4, 6, 8] {
            let cluster_epoch = ClusterEpoch::new(raw_epoch).unwrap();
            current.historical_pg_routes.push(StorageNodePgRoute {
                pg_id: pg_id.get(),
                cluster_epoch,
                state: PgState::Active,
                primary_node_id: node_id,
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![node_id],
            });
        }

        authority
            .set_node_membership(NodeId::new(250), NodeMembershipState::Active)
            .unwrap();
        let delta_runtime_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(1_200, node_id, current.cluster_epoch)
            .unwrap();
        let next = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &delta_runtime_map,
            crate::PgClusterMapHistoryReferenceSummary {
                oldest_live_placement_epoch: Some(ClusterEpoch::new(5).unwrap()),
                oldest_durable_backfill_epoch: None,
                oldest_metadata_command_resource_epoch: None,
                oldest_object_payload_reclaim_claim_epoch: None,
            },
        )
        .unwrap();
        let retained_epochs: Vec<u64> = next
            .historical_pg_routes
            .iter()
            .filter(|route| route.pg_id == pg_id.get())
            .map(|route| route.cluster_epoch.get())
            .collect();

        assert!(!retained_epochs.contains(&2));
        assert!(retained_epochs.contains(&4));
        assert!(retained_epochs.contains(&6));
        assert!(retained_epochs.contains(&8));
        assert!(retained_epochs.contains(&current.cluster_epoch.get()));
    }

    #[test]
    fn storage_node_refresh_config_keeps_metadata_transfer_route_epochs_across_restart() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let destination_node_id = NodeId::new(8);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        let destination_socket_path = tmp.path().join("sock").join("storage-8.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        for (idx, (heartbeat_node_id, heartbeat_socket_path)) in [
            (node_id, socket_path.clone()),
            (destination_node_id, destination_socket_path.clone()),
        ]
        .into_iter()
        .enumerate()
        {
            let heartbeat_at_ms = 1_000 + idx as u64 * 2;
            authority
                .set_node_membership(heartbeat_node_id, NodeMembershipState::Active)
                .unwrap();
            let first = authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms,
                )
                .unwrap();
            authority
                .heartbeat(
                    NodeHeartbeat {
                        node_id: heartbeat_node_id,
                        node_incarnation: 12,
                        endpoint: heartbeat_socket_path.to_str().unwrap().to_owned(),
                        observed_epoch: first.cluster_epoch(),
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms + 1,
                )
                .unwrap();
        }
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        let active_proof = crate::control_plane::PgMetadataProof::current(9, 10, 11);
        let peering_epoch = authority.snapshot().cluster_epoch();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Peering,
                        metadata_proof: active_proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                },
                2_000,
            )
            .unwrap();
        authority
            .complete_pg_peering(pg_id, node_id, 12, 2_001)
            .unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: source_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                },
                2_002,
            )
            .unwrap();
        let source_runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
        let mut current = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &source_runtime_map,
        )
        .unwrap();
        assert_eq!(current.cluster_epoch, source_epoch);
        assert!(current
            .historical_pg_routes
            .iter()
            .all(|route| { route.pg_id != pg_id.get() || route.cluster_epoch < source_epoch }));

        authority
            .set_pg_acting_set_with_metadata_transfer(
                pg_id,
                vec![destination_node_id],
                crate::control_plane::PgMetadataTransferProof::new(source_epoch, active_proof),
            )
            .unwrap();
        let destination_epoch = authority.snapshot().cluster_epoch();
        for step in 0_u32..8 {
            authority
                .set_node_membership(NodeId::new(300 + step), NodeMembershipState::Active)
                .unwrap();
        }

        let delta_runtime_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(3_000, node_id, current.cluster_epoch)
            .unwrap();
        assert!(delta_runtime_map
            .pg_routes()
            .iter()
            .any(|route| route.pg_id() == pg_id
                && route.peering_metadata_transfer_source_route_epoch() == Some(source_epoch)));
        assert!(delta_runtime_map
            .historical_pg_routes()
            .iter()
            .any(|route| {
                route.pg_id() == pg_id
                    && route.cluster_epoch() == destination_epoch
                    && route.peering_metadata_transfer_destination_epoch()
                        == Some(destination_epoch)
            }));
        current = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &delta_runtime_map,
            crate::PgClusterMapHistoryReferenceSummary::default(),
        )
        .unwrap();

        assert!(current
            .historical_pg_routes
            .iter()
            .any(|route| route.pg_id == pg_id.get() && route.cluster_epoch == source_epoch));
        assert!(current.historical_pg_routes.iter().any(|route| {
            route.pg_id == pg_id.get()
                && route.cluster_epoch == destination_epoch
                && route.metadata_transfer_destination_epoch == Some(destination_epoch)
        }));

        current.persist_control_plane_runtime_config().unwrap();
        let restarted = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &current.data_dir,
            current.node_id,
            current.default_ec_shape,
            &current.socket_path,
        )
        .unwrap()
        .unwrap();
        assert!(restarted.historical_pg_routes.iter().any(|route| {
            route.pg_id == pg_id.get()
                && route.cluster_epoch == destination_epoch
                && route.metadata_transfer_destination_epoch == Some(destination_epoch)
        }));

        let completion_epoch = authority.snapshot().cluster_epoch();
        authority
            .heartbeat(
                NodeHeartbeat {
                    node_id: destination_node_id,
                    node_incarnation: 12,
                    endpoint: destination_socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: completion_epoch,
                    requested_lease_duration_ms: 5_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Peering,
                        metadata_proof: active_proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                },
                3_001,
            )
            .unwrap();
        authority
            .complete_pg_peering(pg_id, destination_node_id, 12, 4_003)
            .unwrap();
        let completed_runtime_map = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(4_004, node_id, current.cluster_epoch)
            .unwrap();
        let completed = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &completed_runtime_map,
            crate::PgClusterMapHistoryReferenceSummary::default(),
        )
        .unwrap();
        assert!(!completed.historical_pg_routes.iter().any(|route| {
            route.pg_id == pg_id.get() && route.cluster_epoch == destination_epoch
        }));
    }

    struct RejectFirstHeartbeatAuthority<S> {
        authority: SingleAuthorityControlPlane<S>,
        reject_first: bool,
        delay_next_success: Option<Duration>,
    }

    impl<S: crate::control_plane::ControlPlaneStore> ControlPlaneHeartbeatRuntimeMapSource
        for RejectFirstHeartbeatAuthority<S>
    {
        fn refresh_node_heartbeat(
            &mut self,
            heartbeat: NodeHeartbeat,
            authority_now_ms: u64,
        ) -> Result<crate::control_plane::ControlPlaneHeartbeatRefresh, ControlPlaneError> {
            if std::mem::take(&mut self.reject_first) {
                return Err(ControlPlaneError::AuthorityClockSourceUnavailable);
            }
            if let Some(delay) = self.delay_next_success.take() {
                thread::sleep(delay);
            }
            self.authority
                .refresh_node_heartbeat(heartbeat, authority_now_ms)
        }
    }

    #[test]
    fn failed_scan_submission_cannot_suppress_authority_lease_renewal() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let heartbeat = |epoch| {
            NodeHeartbeat::test_fixture(
                node_id,
                12,
                socket_path.to_str().unwrap().to_owned(),
                epoch,
                5_000,
                Default::default(),
                Vec::new(),
            )
        };
        let first = authority
            .heartbeat(heartbeat(authority.snapshot().cluster_epoch()), 1_000)
            .unwrap();
        let initial = authority
            .heartbeat(heartbeat(first.cluster_epoch()), 1_001)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let server = StorageNodeServer::bind(config).unwrap();
        let report = heartbeat(authority.snapshot().cluster_epoch());
        let submission = Mutex::new(StorageNodeHeartbeatSubmissionState {
            control_plane: RejectFirstHeartbeatAuthority {
                authority,
                reject_first: true,
                delay_next_success: None,
            },
            latest_submitted: None,
            last_accepted_submission_started_at: None,
        });
        let now = Mutex::new(|| 2_000);

        assert!(server
            .submit_serialized_control_plane_heartbeat(&submission, &now, report)
            .is_err());
        assert!(submission
            .lock()
            .unwrap()
            .last_accepted_submission_started_at
            .is_none());
        submission.lock().unwrap().control_plane.delay_next_success =
            Some(Duration::from_millis(150));
        let renewed = StorageNodeServer::renew_latest_serialized_control_plane_heartbeat(
            &submission,
            &now,
            Duration::from_millis(100),
        )
        .expect("a rejected scan must not suppress renewal")
        .unwrap();
        assert!(renewed.lease_deadline_ms() > initial.lease_deadline_ms());
        StorageNodeServer::renew_latest_serialized_control_plane_heartbeat(
            &submission,
            &now,
            Duration::from_millis(100),
        )
        .expect("a slow successful renewal must not start a new throttle window")
        .unwrap();

        submission.lock().unwrap().control_plane.delay_next_success =
            Some(Duration::from_millis(150));
        server
            .submit_serialized_control_plane_heartbeat(
                &submission,
                &now,
                heartbeat(runtime_map.cluster_epoch()),
            )
            .unwrap();
        StorageNodeServer::renew_latest_serialized_control_plane_heartbeat(
            &submission,
            &now,
            Duration::from_millis(100),
        )
        .expect("a slow successful scan must not start a new throttle window")
        .unwrap();
    }

    struct RecordingHeartbeatAuthority<S> {
        authority: SingleAuthorityControlPlane<S>,
        accepted_reports: Arc<Mutex<Vec<(std::num::NonZeroU64, HeartbeatLease)>>>,
    }

    impl<S: crate::control_plane::ControlPlaneStore> ControlPlaneHeartbeatRuntimeMapSource
        for RecordingHeartbeatAuthority<S>
    {
        fn refresh_node_heartbeat(
            &mut self,
            heartbeat: NodeHeartbeat,
            authority_now_ms: u64,
        ) -> Result<crate::control_plane::ControlPlaneHeartbeatRefresh, ControlPlaneError> {
            let generation = heartbeat.cluster_map_history_route_scan_generation;
            let refresh = self
                .authority
                .refresh_node_heartbeat(heartbeat, authority_now_ms)?;
            self.accepted_reports
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((generation, refresh.lease().clone()));
            Ok(refresh)
        }
    }

    #[test]
    fn storage_node_control_plane_refresh_loop_installs_runtime_maps() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();

        let first = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let second = authority
            .heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 12,
                    endpoint: socket_path.to_str().unwrap().to_owned(),
                    observed_epoch: first.cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                1_001,
            )
            .unwrap();
        assert!(second.serving());
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let mut config = config;
        config.route_map_validity = RouteMapValidity::until_ms(1).unwrap();
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        let now = Arc::new(AtomicU64::new(1_003));
        let loop_now = Arc::clone(&now);
        let mut refresh_loop = Arc::clone(&server)
            .spawn_control_plane_refresh_loop(
                authority,
                12,
                STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
                move || loop_now.fetch_add(1, Ordering::SeqCst),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if refresh_loop.status().scan_publication_successes > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "control-plane refresh loop did not install a runtime map: {:?}",
                refresh_loop.status()
            );
            thread::sleep(Duration::from_millis(1));
        }

        let installed_config = server.config_snapshot();
        assert!(installed_config.cluster_epoch > runtime_map.cluster_epoch());
        assert!(installed_config.route_map_valid_until_ms().is_some());
        assert_eq!(installed_config.pg_routes.len(), 1);
        assert_eq!(installed_config.pg_routes[0].state, PgState::Active);
        assert_eq!(refresh_loop.status().scan_publication_failures, 0);

        refresh_loop.stop();
        let attempts_after_stop = refresh_loop.status().scan_publication_attempts;
        thread::sleep(Duration::from_millis(15));
        assert_eq!(
            refresh_loop.status().scan_publication_attempts,
            attempts_after_stop
        );
    }

    #[test]
    fn storage_node_lease_renews_while_heartbeat_scan_or_runtime_install_is_blocked() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(0);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        let first = authority
            .heartbeat(
                NodeHeartbeat::test_fixture(
                    node_id,
                    12,
                    socket_path.to_str().unwrap().to_owned(),
                    authority.snapshot().cluster_epoch(),
                    STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
                    Default::default(),
                    Vec::new(),
                ),
                1_000,
            )
            .unwrap();
        authority
            .heartbeat(
                NodeHeartbeat::test_fixture(
                    node_id,
                    12,
                    socket_path.to_str().unwrap().to_owned(),
                    first.cluster_epoch(),
                    STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
                    Default::default(),
                    Vec::new(),
                ),
                1_001,
            )
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_002).unwrap();
        let config = StorageNodeProcessConfig::from_runtime_map(
            node_id,
            tmp.path().join("node"),
            EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        let scan_gate = DeterministicTestGate::new();
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let hook_gate = Arc::clone(&scan_gate);
        let hook_calls = Arc::clone(&scan_calls);
        server.set_control_plane_heartbeat_scan_test_hook(Arc::new(move || {
            if hook_calls.fetch_add(1, Ordering::SeqCst) == 1 {
                hook_gate.block_until_released();
            }
        }));
        let accepted_reports = Arc::new(Mutex::new(Vec::new()));
        let source = RecordingHeartbeatAuthority {
            authority,
            accepted_reports: Arc::clone(&accepted_reports),
        };
        let now = Arc::new(AtomicU64::new(1_003));
        let loop_now = Arc::clone(&now);
        let mut refresh_loop = Arc::clone(&server)
            .spawn_control_plane_refresh_loop(
                source,
                12,
                STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
                move || loop_now.fetch_add(100, Ordering::SeqCst),
            )
            .unwrap();
        let _scan_gate_release = scan_gate.release_on_drop();

        scan_gate.wait_until_arrived(Duration::from_secs(2));
        let initial_report = accepted_reports.lock().unwrap()[0].clone();
        let initial_generation = initial_report.0;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let reports = accepted_reports.lock().unwrap().clone();
            if reports.len() >= 3 {
                assert!(
                    reports
                        .iter()
                        .all(|(generation, _)| *generation == initial_generation),
                    "blocked scan must renew only from the latest complete report: {reports:?}"
                );
                assert!(
                    reports.last().unwrap().1.lease_deadline_ms()
                        > initial_report.1.lease_deadline_ms(),
                    "accepted cached heartbeats must advance the authority lease: {reports:?}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cached heartbeat did not renew while the complete scan was blocked: {:?}",
                refresh_loop.status()
            );
            thread::sleep(Duration::from_millis(10));
        }

        scan_gate.release();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let reports = accepted_reports.lock().unwrap().clone();
            if reports
                .iter()
                .any(|(generation, _)| *generation > initial_generation)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "released complete heartbeat scan did not publish fresh evidence: {reports:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }

        let install_gate = DeterministicTestGate::new();
        let _install_gate_release = install_gate.release_on_drop();
        let hook_gate = Arc::clone(&install_gate);
        *server
            .runtime_route_after_publish_lock_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Arc::new(move || hook_gate.block_until_released()));
        install_gate.wait_until_arrived(Duration::from_secs(2));
        let reports_at_block = accepted_reports.lock().unwrap().clone();
        let blocked_generation = reports_at_block.last().unwrap().0;
        let blocked_lease_deadline = reports_at_block.last().unwrap().1.lease_deadline_ms();
        let report_count_at_block = reports_at_block.len();
        let scan_successes_at_block = refresh_loop.status().scan_publication_successes;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let reports = accepted_reports.lock().unwrap().clone();
            if reports.len() >= report_count_at_block + 2 {
                assert!(
                    reports[report_count_at_block..]
                        .iter()
                        .all(|(generation, _)| *generation == blocked_generation),
                    "blocked runtime-map installation must not prevent cached lease renewal: {reports:?}"
                );
                assert!(
                    reports.last().unwrap().1.lease_deadline_ms() > blocked_lease_deadline,
                    "accepted cached heartbeats must advance the authority lease while publication is blocked: {reports:?}"
                );
                assert_eq!(
                    refresh_loop.status().scan_publication_successes,
                    scan_successes_at_block,
                    "renewal-only success must not advance scan/publication health"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cached heartbeat did not renew while runtime-map installation was blocked: {:?}",
                refresh_loop.status()
            );
            thread::sleep(Duration::from_millis(10));
        }
        install_gate.release();
        *server
            .runtime_route_after_publish_lock_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        refresh_loop.stop();
        assert!(scan_calls.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn storage_node_control_plane_heartbeat_schedule_is_lease_derived_and_jittered() {
        let node_7_first =
            storage_node_control_plane_heartbeat_interval(NodeId::new(7), 10_000, 1).unwrap();
        let node_7_second =
            storage_node_control_plane_heartbeat_interval(NodeId::new(7), 10_000, 2).unwrap();
        let node_8_first =
            storage_node_control_plane_heartbeat_interval(NodeId::new(8), 10_000, 1).unwrap();
        assert!((Duration::from_millis(750)..=Duration::from_secs(1)).contains(&node_7_first));
        assert!((Duration::from_millis(750)..=Duration::from_secs(1)).contains(&node_7_second));
        assert!((Duration::from_millis(750)..=Duration::from_secs(1)).contains(&node_8_first));
        assert_ne!(node_7_first, node_7_second);
        assert_ne!(node_7_first, node_8_first);

        let default_lease = storage_node_control_plane_heartbeat_interval(
            NodeId::new(7),
            STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
            1,
        )
        .unwrap();
        assert!((Duration::from_millis(249)..=Duration::from_millis(333)).contains(&default_lease));
    }

    #[test]
    fn storage_node_minimum_heartbeat_lease_remains_valid_through_first_renewal() {
        let local_wall_ms = 20_000;
        let local_monotonic_ms = 30_000;
        let validity = RouteMapValidity::until_ms(
            local_wall_ms + STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
        )
        .unwrap();
        let bound = bind_storage_node_route_map_lease_at(
            validity,
            local_wall_ms,
            local_monotonic_ms,
            Some(local_wall_ms),
        )
        .unwrap()
        .unwrap();
        let first_renewal = storage_node_control_plane_heartbeat_interval(
            NodeId::new(7),
            STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
            1,
        )
        .unwrap();
        let first_renewal_ms = u64::try_from(first_renewal.as_millis()).unwrap();

        assert!(bound.is_valid_at_monotonic(local_monotonic_ms));
        assert!(bound.is_valid_at_monotonic(local_monotonic_ms + first_renewal_ms));
        assert_eq!(
            bound.local_valid_until_monotonic_ms(),
            local_monotonic_ms + STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_USABLE_LEASE_MS
        );
    }

    #[test]
    fn storage_node_control_plane_refresh_loop_rejects_too_short_lease() {
        let tmp = test_util::tempdir();
        let node_id = NodeId::new(7);
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let config = StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("node"),
            default_ec_shape: EcShape { k: 1, m: 0 },
            pg_ids: vec![0],
            socket_path,
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: node_id,
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![node_id],
            }],

            historical_pg_routes: Vec::new(),
            pending_metadata_command_recoveries: Vec::new(),
        };
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();

        assert!(matches!(
            Arc::clone(&server).spawn_control_plane_refresh_loop(
                authority,
                12,
                STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS - 1,
                || 1_000,
            ),
            Err(StorageNodeServerError::ControlPlaneRefreshLoopLeaseTooShort { .. })
        ));
    }

    #[test]
    fn metadata_command_lock_wait_emits_diagnostic() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let first = locks.acquire(NodeId::new(7), pg_id, None).unwrap();
        let before = observability::metrics_snapshot();
        let (wait_tx, wait_rx) = mpsc::channel();
        locks.set_before_wait_hook(Arc::new(move |actual_pg_id| {
            assert_eq!(actual_pg_id, pg_id);
            let _ = wait_tx.send(());
        }));
        let waiting_locks = locks.clone();

        let waiter = thread::spawn(move || {
            let _attached = observability::AttachedTrace::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "trace-metadata-command-lock-wait".to_string(),
                    request_id: "request-metadata-command-lock-wait".to_string(),
                }),
            );
            let _guard = waiting_locks.acquire(NodeId::new(7), pg_id, None).unwrap();
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter should enter metadata-command lock wait");
        drop(first);
        waiter
            .join()
            .expect("waiter should acquire and release lock");

        let after = observability::metrics_snapshot();
        assert!(
            after.metadata_command_session_wait_total > before.metadata_command_session_wait_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| record.request_id == "request-metadata-command-lock-wait")
            .expect("lock wait should be recorded in flight recorder");
        assert_eq!(record.event, "metadata_command_session_wait");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("wait_us="));
    }

    #[test]
    fn metadata_command_lock_wait_times_out_with_contention_error() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let first = locks.acquire(NodeId::new(7), pg_id, None).unwrap();
        let _stderr_guard = locks.suppress_lock_wait_stderr();
        let started = Instant::now();
        let err = match locks.acquire_with_timeout(
            NodeId::new(7),
            pg_id,
            None,
            Duration::from_millis(25),
        ) {
            Ok(_) => panic!("metadata-command lock acquisition should time out"),
            Err(error) => error,
        };

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "metadata-command lock wait should return a bounded contention error"
        );
        assert_eq!(err.code, StorageRpcErrorCode::MetadataCommandContention);
        assert!(err.message.contains("metadata command lock wait"));
        assert_eq!(
            locks.waiting_for_test(pg_id),
            0,
            "a timed-out ticket must be removed from the FIFO"
        );

        drop(first);
        let _successor = locks
            .acquire_with_timeout(
                NodeId::new(7),
                pg_id,
                None,
                Duration::from_millis(250),
            )
            .expect("a successor must acquire after the timed-out ticket is removed");
    }

    #[test]
    fn metadata_command_lock_wait_honors_operation_deadline() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let _first = locks.acquire(NodeId::new(7), pg_id, None).unwrap();
        let _stderr_guard = locks.suppress_lock_wait_stderr();
        let deadline = Instant::now() + Duration::from_millis(25);
        let err = match locks.acquire_until(NodeId::new(7), pg_id, None, deadline) {
            Ok(_) => panic!("metadata-command lock acquisition should honor operation deadline"),
            Err(error) => error,
        };

        assert!(
            Instant::now() < deadline + Duration::from_millis(250),
            "metadata-command lock wait should not use its independent default timeout"
        );
        assert_eq!(err.code, StorageRpcErrorCode::MetadataCommandContention);
        assert!(err.message.contains("metadata command lock wait"));
    }

    #[test]
    fn authenticated_unix_metadata_command_lock_preserves_first_waiter_across_clients() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        const FIFO_TEST_TIMEOUT: Duration = Duration::from_secs(30);
        let transport_limits = storage_rpc_test_transport_limits_with_io_timeout(
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
            FIFO_TEST_TIMEOUT,
        );
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(
                    storage_rpc_server_auth(&credential).with_transport_limits(transport_limits),
                )
                .bind()
                .unwrap(),
        );
        let _wait_timeout_override = server
            .metadata_command_locks
            .override_wait_timeout_for_test(FIFO_TEST_TIMEOUT);
        let _stderr_guard = server.suppress_metadata_command_lock_wait_stderr();
        let pg_id = PgId::new(0);
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
        const WAITER_COUNT: usize = 2;
        let mut accept_threads = Vec::new();
        for _ in 0..=WAITER_COUNT {
            let accepting = Arc::clone(&server);
            accept_threads.push(thread::spawn(move || accepting.accept_one()));
        }
        let client_auth: Arc<StorageRpcClientAuthConfig> = Arc::new(
            crate::FrontendStorageRpcClientCapability::new_with_transport_limits(
                credential.clone(),
                9,
                STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                transport_limits,
            )
            .unwrap()
            .into(),
        );
        let new_client = || {
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                StorageRpcClientEndpoint::unix(config.socket_path.clone()),
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(Arc::clone(&client_auth)),
            )
        };
        let owner = new_client();
        owner
            .rpc_request(StorageRpcMessageKind::MetadataCommandPgLockAcquire, payload.clone())
            .unwrap();

        let first_waiter_gate = DeterministicTestGate::new();
        let _first_waiter_release = first_waiter_gate.release_on_drop();
        let later_waiter_gate = DeterministicTestGate::new();
        let _later_waiter_release = later_waiter_gate.release_on_drop();
        let later_post_release_gate = DeterministicTestGate::new();
        let _later_post_release = later_post_release_gate.release_on_drop();
        let first_waiter = Arc::new(AtomicBool::new(true));
        let later_waiter_before_release = Arc::new(AtomicBool::new(true));
        let later_waiter_after_release = Arc::new(AtomicBool::new(true));
        let later_waiter_still_queued = Arc::new(AtomicBool::new(true));
        let owner_released = Arc::new(AtomicBool::new(false));
        let first_waiter_gate_hook = Arc::clone(&first_waiter_gate);
        let later_waiter_gate_hook = Arc::clone(&later_waiter_gate);
        let later_post_release_gate_hook = Arc::clone(&later_post_release_gate);
        let owner_released_hook = Arc::clone(&owner_released);
        let (still_queued_tx, still_queued_rx) = mpsc::sync_channel(1);
        server.metadata_command_locks.set_before_wait_hook(Arc::new(
            move |actual_pg_id| {
                assert_eq!(actual_pg_id, pg_id);
                if first_waiter.swap(false, Ordering::SeqCst) {
                    first_waiter_gate_hook.block_until_released();
                    return;
                }
                if !owner_released_hook.load(Ordering::SeqCst) {
                    if later_waiter_before_release.swap(false, Ordering::SeqCst) {
                        later_waiter_gate_hook.block_until_released();
                    }
                    return;
                }
                if later_waiter_after_release.swap(false, Ordering::SeqCst) {
                    later_post_release_gate_hook.block_until_released();
                    return;
                }
                if later_waiter_still_queued.swap(false, Ordering::SeqCst) {
                    let _ = still_queued_tx.send(());
                }
            },
        ));

        let (acquired_tx, acquired_rx) = mpsc::channel();
        let mut waiters = Vec::new();
        for waiter_id in 0..WAITER_COUNT {
            let waiter = new_client();
            let waiter_payload = payload.clone();
            let waiter_acquired = acquired_tx.clone();
            waiters.push(thread::spawn(move || {
                waiter
                    .rpc_request(
                        StorageRpcMessageKind::MetadataCommandPgLockAcquire,
                        waiter_payload,
                    )
                    .unwrap();
                waiter_acquired.send(waiter_id).unwrap();
            }));
            let queue_deadline = Instant::now() + Duration::from_secs(2);
            while server.metadata_command_locks.waiting_for_test(pg_id) != waiter_id + 1 {
                assert!(
                    Instant::now() < queue_deadline,
                    "waiter {waiter_id} did not enter the server FIFO"
                );
                thread::yield_now();
            }
            if waiter_id == 0 {
                first_waiter_gate.wait_until_arrived(Duration::from_secs(2));
            } else {
                later_waiter_gate.wait_until_arrived(Duration::from_secs(2));
                later_waiter_gate.release();
            }
        }
        drop(acquired_tx);

        owner
            .rpc_request(StorageRpcMessageKind::MetadataCommandPgLockRelease, payload)
            .unwrap();
        owner_released.store(true, Ordering::SeqCst);
        later_post_release_gate.wait_until_arrived(Duration::from_secs(2));
        later_post_release_gate.release();
        still_queued_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("the later waiter must remain queued behind the paused first waiter");

        first_waiter_gate.release();
        let acquired = (0..WAITER_COUNT)
            .map(|_| acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(acquired, (0..WAITER_COUNT).collect::<Vec<_>>());

        drop(owner);
        for waiter in waiters {
            waiter.join().unwrap();
        }
        for accepting in accept_threads {
            assert!(accepting.join().unwrap().is_ok());
        }
    }

    #[test]
    fn metadata_command_lock_wait_emits_blocked_holder_diagnostic() {
        let locks = StorageNodeMetadataCommandLocks::default();
        let pg_id = PgId::new(0);
        let first = locks
            .acquire(
                NodeId::new(7),
                pg_id,
                Some(StorageNodeMetadataCommandLockContext {
                    request_id: 41,
                    kind: StorageRpcMessageKind::MetadataCommandPgLockAcquire,
                }),
            )
            .unwrap();
        locks.update_context(
            pg_id,
            Some(StorageNodeMetadataCommandLockContext {
                request_id: 43,
                kind: StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            }),
        );
        let _stderr_guard = locks.suppress_lock_wait_stderr();
        let (wait_tx, wait_rx) = mpsc::channel();
        locks.set_before_wait_hook(Arc::new(move |actual_pg_id| {
            assert_eq!(actual_pg_id, pg_id);
            let _ = wait_tx.send(());
        }));
        let waiting_locks = locks.clone();

        let waiter = thread::spawn(move || {
            let _attached = observability::AttachedTrace::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "trace-metadata-command-lock-blocked".to_string(),
                    request_id: "request-metadata-command-lock-blocked".to_string(),
                }),
            );
            let _guard = waiting_locks
                .acquire_with_timeout(
                    NodeId::new(7),
                    pg_id,
                    Some(StorageNodeMetadataCommandLockContext {
                        request_id: 42,
                        kind: StorageRpcMessageKind::MetadataCommandPendingEnvelope,
                    }),
                    Duration::from_secs(2),
                )
                .unwrap();
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter should enter metadata-command lock wait");
        let deadline = Instant::now() + Duration::from_secs(3);
        let record = loop {
            if let Some(record) = observability::flight_recorder_snapshot()
                .into_iter()
                .rev()
                .find(|record| {
                    record.request_id == "request-metadata-command-lock-blocked"
                        && record.event == "metadata_command_lock_wait_blocked"
                })
            {
                break record;
            }
            assert!(
                Instant::now() < deadline,
                "blocked lock diagnostic should be emitted before waiter acquires"
            );
            thread::sleep(Duration::from_millis(25));
        };
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("waiter_request_id=42"));
        assert!(record
            .detail
            .contains("waiter_kind=\"metadata command pending envelope\""));
        assert!(record.detail.contains("holder_request_id=41"));
        assert!(record
            .detail
            .contains("holder_kind=\"metadata command PG lock acquire\""));
        assert!(record.detail.contains("holder_held_us="));
        assert!(record.detail.contains("holder_current_request_id=43"));
        assert!(record
            .detail
            .contains("holder_current_kind=\"metadata command apply and record\""));
        assert!(record.detail.contains("holder_current_elapsed_us="));
        locks.update_context(pg_id, None);
        {
            let table = locks.state.table.lock().unwrap_or_else(|e| e.into_inner());
            let holder = table
                .held
                .get(&pg_id)
                .expect("holder should still be present before release");
            assert!(holder.current_context.is_none());
            assert!(holder.current_started_at.is_none());
        }

        drop(first);
        waiter
            .join()
            .expect("waiter should acquire and release lock");
    }

    #[test]
    fn storage_node_rpc_metadata_command_wait_records_frame_trace() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(0),
            command: command.clone(),
            scope_bucket: Some(command.bucket_name().clone()),
            effect_deadline: None,
            operation_deadline: None,
        };
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let _stderr_guard = server.suppress_metadata_command_lock_wait_stderr();
        let pg_guard = server
            .metadata_command_locks
            .acquire(NodeId::new(7), PgId::new(0), None);
        let (wait_tx, wait_rx) = mpsc::channel();
        server
            .metadata_command_locks
            .set_before_wait_hook(Arc::new(move |actual_pg_id| {
                assert_eq!(actual_pg_id, PgId::new(0));
                let _ = wait_tx.send(());
            }));
        let socket_path = config.socket_path.clone();
        let accept = thread::spawn(move || server.accept_one().unwrap());
        let before = observability::metrics_snapshot();

        let client = thread::spawn(move || {
            let mut client = UnixStream::connect(socket_path).unwrap();
            send_frame(
                &mut client,
                11,
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                encode_metadata_command_pending_slot_request(&request).unwrap(),
            )
        });

        wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("RPC handler should enter metadata-command lock wait");
        drop(pg_guard);
        let response = client.join().expect("client should receive response");
        accept.join().unwrap();
        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();

        let after = observability::metrics_snapshot();
        assert!(
            after.metadata_command_session_wait_total > before.metadata_command_session_wait_total
        );
        let records = observability::flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "storage-node-7-rpc-11"
                    && record.event == "metadata_command_session_wait"
            })
            .expect("storage-node RPC wait should be recorded without caller-attached trace");
        assert!(record.detail.contains("node_id=7"));
        assert!(record.detail.contains("pg_id=0"));
        assert!(record.detail.contains("wait_us="));
    }

    fn private_socket_dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn bind_error(config: StorageNodeProcessConfig) -> StorageNodeServerError {
        match StorageNodeServer::bind(config) {
            Ok(_) => panic!("expected storage-node bind to fail"),
            Err(error) => error,
        }
    }

    fn read_handle_acquire_payload(read_operation_id: &str, location: ShardLocation) -> Vec<u8> {
        encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.to_string(),
            locations: vec![location.into()],
            shard_keys: vec![test_shard_key(location.shard_index().get())],
        })
        .unwrap()
    }

    fn test_location(epoch: u64, pg_id: u32, node_id: u32) -> ShardLocation {
        test_location_with_shard(epoch, pg_id, node_id, 0)
    }

    fn test_location_with_shard(
        epoch: u64,
        pg_id: u32,
        node_id: u32,
        shard_index: u8,
    ) -> ShardLocation {
        ShardLocation::new(
            ClusterEpoch::new(epoch).unwrap(),
            DataPgId::new_for_test(PgId::new(pg_id)),
            ShardIndex::new(shard_index),
            NodeId::new(node_id),
        )
    }

    fn test_shard_key(shard_index: u8) -> ShardKey {
        ShardKey::new(&[0x42; 16], 99, shard_index)
    }

    fn test_metadata_command(pg_id: u32, log_index: u64) -> MetadataCommandEnvelope {
        test_metadata_command_for_subject(
            pg_id,
            log_index,
            crate::tests::bucket_name("metadata-rpc-bucket"),
            crate::tests::object_key("object"),
        )
    }

    fn test_metadata_command_for_subject(
        pg_id: u32,
        log_index: u64,
        bucket: BucketName,
        key: crate::ObjectKey,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket,
                key,
                crate::tests::stream_session_id("metadata-rpc"),
                GenerationId::new(1).unwrap(),
                123,
            )),
        )
    }

    fn test_metadata_rpc_bucket_record() -> crate::metadata_command::BucketRecord {
        let owner = crate::OwnerIdentity::from_principal("owner");
        crate::metadata_command::BucketRecord::from_create_config(
            &CreateBucketConfig {
                name: "metadata-rpc-bucket",
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
            },
            1,
            1,
        )
        .unwrap()
    }

    fn test_bucket_control_metadata_command(
        pg_id: u32,
        log_index: u64,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::PutBucketVersioning(
                crate::metadata_command::PutBucketVersioningCommand::from_bucket(
                    test_metadata_rpc_bucket_record(),
                    BucketVersioningState::Enabled,
                ),
            ),
        )
    }

    fn test_mark_bucket_deleting_command(
        pg_id: u32,
        log_index: u64,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::MarkBucketDeleting(
                crate::metadata_command::MarkBucketDeletingCommand::from_bucket(
                    test_metadata_rpc_bucket_record(),
                ),
            ),
        )
    }

    fn create_probe_bucket_direct(store: &PgStore, bucket: &BucketName) {
        let owner = crate::OwnerIdentity::from_principal("owner");
        store
            .create_bucket_with_config(&CreateBucketConfig {
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
            })
            .unwrap();
    }

    fn put_probe_lifecycle_direct(store: &PgStore, bucket: &BucketName) {
        store
            .put_bucket_subresource(
                bucket,
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: BucketSubresourceAux::None,
                },
            )
            .unwrap();
    }

    fn test_metadata_checkpoint_with_bucket(
        bucket_name: &str,
    ) -> (
        BucketName,
        crate::node_runtime::pg_store::MetadataCommandCheckpoint,
    ) {
        let source_tmp = test_util::tempdir();
        let source_node = crate::node::SharedStorageNode::open(source_tmp.path(), &[0]).unwrap();
        let bucket = crate::tests::bucket_name(bucket_name);
        let checkpoint = {
            let source_pg = source_node.get_pg(0).unwrap();
            create_probe_bucket_direct(&source_pg, &bucket);
            put_probe_lifecycle_direct(&source_pg, &bucket);
            source_pg.refresh_metadata_command_state_digest().unwrap();
            source_pg
                .metadata_command_checkpoint(11, ClusterEpoch::INITIAL)
                .unwrap()
        };
        (bucket, checkpoint)
    }

    fn test_bucket_write_reservation_proof(
        bucket: crate::BucketName,
        key: &crate::ObjectKey,
    ) -> BucketWriteReservationProof {
        BucketWriteReservationProof {
            bucket,
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "storage-node-rpc-test".to_string(),
            created_at: 1,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        }
    }

    fn read_handle_release_payload(read_operation_id: &str) -> Vec<u8> {
        encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.to_string(),
        })
        .unwrap()
    }

    #[test]
    fn metadata_checkpoint_success_response_returns_structured_error_when_frame_too_large() {
        let success = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint export",
            b"ok",
            256,
        )
        .unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&success).unwrap(),
            Ok(b"ok".to_vec())
        );

        let oversized_payload = vec![42; 300];
        let response = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint export",
            &oversized_payload,
            256,
        )
        .unwrap();
        let error = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error
            .message
            .contains("metadata command checkpoint export response is too large"));
        assert!(error
            .message
            .contains("exceeds storage RPC payload limit 256 bytes"));

        let response = encode_metadata_command_checkpoint_success_response(
            "metadata command checkpoint candidates",
            &oversized_payload,
            256,
        )
        .unwrap();
        let error = decode_storage_rpc_response_payload(&response)
            .unwrap()
            .unwrap_err();

        assert_eq!(error.code, StorageRpcErrorCode::ResourceExhausted);
        assert!(error
            .message
            .contains("metadata command checkpoint candidates response is too large"));
    }

    #[test]
    fn metadata_checkpoint_candidates_for_frame_skips_oversized_candidates_anywhere() {
        let tmp = test_util::tempdir();
        let node = crate::node::SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let pg = node.get_pg(0).unwrap();
        let bucket = crate::tests::bucket_name("metadata-checkpoint-frame-candidate");
        create_probe_bucket_direct(&pg, &bucket);
        pg.refresh_metadata_command_state_digest().unwrap();
        let small = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();
        pg.put_bucket_subresource(
            &bucket,
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let second_small = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();

        let mut large_checkpoints = Vec::new();
        for suffix in
            0..crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES
        {
            let large_body = format!(
                "<LifecycleConfiguration>{}{suffix}</LifecycleConfiguration>",
                "x".repeat(64 * 1024)
            );
            pg.put_bucket_subresource(
                &bucket,
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: &large_body,
                    aux: BucketSubresourceAux::None,
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            large_checkpoints.push(
                pg.record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
                    .unwrap(),
            );
        }

        let small_payload = encode_metadata_command_checkpoint_candidates_response(
            &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                checkpoints: vec![small.clone(), second_small.clone()],
            },
        )
        .unwrap();
        let small_response_len = encode_storage_rpc_success_response(&small_payload).len();
        for large in &large_checkpoints {
            let large_payload = encode_metadata_command_checkpoint_candidates_response(
                &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                    checkpoints: vec![large.clone()],
                },
            )
            .unwrap();
            assert!(
                encode_storage_rpc_success_response(&large_payload).len() > small_response_len
            );
        }

        let rows = pg
            .metadata_command_checkpoint_candidate_rows(ClusterEpoch::INITIAL, u64::MAX)
            .unwrap();
        drop(pg);
        let row_for = |expected: &MetadataCommandCheckpoint| {
            rows
                .iter()
                .find(|row| {
                    decode_metadata_command_checkpoint_candidate_rows(
                        vec![(*row).clone()],
                        ClusterEpoch::INITIAL,
                        PgId::new(0),
                        1,
                    ) == vec![expected.clone()]
                })
                .unwrap()
                .clone()
        };
        let mut oversized_prefix = large_checkpoints
            .iter()
            .map(&row_for)
            .collect::<Vec<_>>();
        oversized_prefix.push(row_for(&small));
        let candidates = metadata_command_checkpoint_candidates_for_frame(
            oversized_prefix,
            ClusterEpoch::INITIAL,
            PgId::new(0),
            1,
            small_response_len,
        );

        assert_eq!(candidates, vec![small.clone()]);

        let candidates = metadata_command_checkpoint_candidates_for_frame(
            vec![
                row_for(&small),
                row_for(&large_checkpoints[0]),
                row_for(&second_small),
            ],
            ClusterEpoch::INITIAL,
            PgId::new(0),
            2,
            small_response_len,
        );

        assert_eq!(candidates, vec![small, second_small]);
    }

    #[test]
    fn checkpoint_candidate_frame_selection_releases_pg_serialization_after_capture() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let node_id = config.node_id;
        let pg_id = PgId::new(0);
        let pg = server._node.get_pg(pg_id.get()).unwrap();
        let bucket = crate::tests::bucket_name("checkpoint-capture-lock-lifetime");
        create_probe_bucket_direct(&pg, &bucket);
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.record_current_metadata_command_checkpoint(
            node_id.as_u32(),
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        drop(pg);

        let gate = DeterministicTestGate::new();
        let _release = gate.release_on_drop();
        let hook_gate = Arc::clone(&gate);
        server.set_metadata_checkpoint_rows_captured_test_hook(Arc::new(move || {
            hook_gate.block_until_released();
        }));
        let handler = server.connection_handler();
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel();
        let checkpoint_handler = handler.clone();
        let checkpoint_join = thread::spawn(move || {
            let session = StorageNodeSession::new(
                Arc::clone(&checkpoint_handler.read_handles),
                Arc::clone(&checkpoint_handler.node),
            );
            let response = checkpoint_handler
                .metadata_command_checkpoint_candidates_response(
                    &session,
                    StorageRpcMetadataCommandCheckpointCandidatesRequest {
                        node_id,
                        cluster_epoch: ClusterEpoch::INITIAL,
                        pg_id,
                        max_applied_log_index: u64::MAX,
                        limit: 1,
                    },
                )
                .unwrap();
            checkpoint_tx.send(response).unwrap();
        });

        gate.wait_until_arrived(Duration::from_secs(5));
        let (read_tx, read_rx) = mpsc::channel();
        let read_handler = handler.clone();
        let read_join = thread::spawn(move || {
            let session = StorageNodeSession::new(
                Arc::clone(&read_handler.read_handles),
                Arc::clone(&read_handler.node),
            );
            let response = read_handler
                .metadata_command_max_log_index_response(
                    &session,
                    StorageRpcMetadataCommandStateRequest {
                        node_id,
                        cluster_epoch: ClusterEpoch::INITIAL,
                        pg_id,
                    },
                )
                .unwrap();
            read_tx.send(response).unwrap();
        });

        let read_response = read_rx.recv_timeout(Duration::from_secs(2));
        gate.release();
        let checkpoint_response = checkpoint_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        checkpoint_join.join().unwrap();
        let eventual_read_response = match read_response {
            Ok(response) => response,
            Err(error) => {
                let response = read_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                read_join.join().unwrap();
                panic!(
                    "metadata read remained blocked during checkpoint frame selection: {error:?}; response={response:?}"
                );
            }
        };
        read_join.join().unwrap();

        decode_storage_rpc_response_payload(&checkpoint_response)
            .unwrap()
            .unwrap();
        let read_payload = decode_storage_rpc_response_payload(&eventual_read_response)
            .unwrap()
            .unwrap();
        decode_metadata_command_max_log_index_response(&read_payload).unwrap();
    }

    fn send_frame(
        client: &mut UnixStream,
        request_id: u64,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> StorageRpcFrame {
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        write_storage_rpc_frame_to(client, &request).unwrap();
        read_storage_rpc_frame_from(client).unwrap()
    }

    fn send_read_handle_acquire(
        config: StorageNodeProcessConfig,
        location: ShardLocation,
    ) -> StorageRpcErrorResponse {
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 7,
            kind: StorageRpcMessageKind::ReadHandlesAcquire,
            payload: read_handle_acquire_payload("read-op", location),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err()
    }

    fn wait_for_read_handle_count(
        server: &StorageNodeServer,
        location: ShardLocation,
        expected: usize,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let actual = server.read_handle_count(location);
            if actual == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "read handle count for {location:?} stayed at {actual}, expected {expected}"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn storage_node_server_answers_health_request() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 42,
            kind: StorageRpcMessageKind::Health,
            payload: Vec::new(),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        assert_eq!(response.request_id, 42);
        assert_eq!(response.kind, StorageRpcMessageKind::Health);
        let health_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let health = decode_health_response(&health_payload).unwrap();
        assert_eq!(health.node_id, NodeId::new(7));
        assert_eq!(health.cluster_epoch, ClusterEpoch::new(1).unwrap());
    }

    #[test]
    fn authenticated_unix_storage_rpc_reuses_connection_across_real_server_boundary() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .bind()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            StorageRpcClientEndpoint::unix(socket_path),
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        for _ in 0..2 {
            let payload = client
                .rpc_request(StorageRpcMessageKind::Health, Vec::new())
                .unwrap();
            let health = decode_health_response(&payload).unwrap();

            assert_eq!(health.node_id, config.node_id);
            assert_eq!(health.cluster_epoch, config.cluster_epoch);
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_storage_rpc_rejects_resealed_unsupported_checkpoint_before_mutation() {
        let (_bucket, checkpoint) =
            test_metadata_checkpoint_with_bucket("authenticated-unsupported-checkpoint");
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        config.cluster_epoch = destination_epoch;
        config.pg_routes[0].cluster_epoch = destination_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .bind()
            .unwrap();
        let destination_node = Arc::clone(&server._node);
        let state_before = destination_node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            StorageRpcClientEndpoint::unix(socket_path),
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        for unsupported_version in [1_u16, 2, 4] {
            let mut unsupported = checkpoint.clone();
            PgStore::test_reseal_metadata_command_checkpoint_for_encoding_version(
                &mut unsupported,
                unsupported_version,
            );
            let mut payload =
                encode_metadata_command_transfer_checkpoint_base_request(
                    &StorageRpcMetadataCommandTransferCheckpointBaseRequest {
                        node_id: config.node_id,
                        cluster_epoch: destination_epoch,
                        pg_id: PgId::new(0),
                        checkpoint: unsupported,
                    },
                )
                .unwrap();
            const REQUEST_PREFIX_LEN: usize = 4 + 8 + 4;
            let version_offset = REQUEST_PREFIX_LEN
                + crate::node_runtime::pg_store::METADATA_COMMAND_CHECKPOINT_MAGIC.len();
            payload[version_offset..version_offset + 2]
                .copy_from_slice(&unsupported_version.to_be_bytes());

            let error = client
                .rpc_request_result(
                    StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
                    payload,
                )
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
            assert!(
                error.message.contains(&format!(
                    "unsupported metadata checkpoint encoding version {unsupported_version}"
                )),
                "unexpected error: {error:?}"
            );
            assert_eq!(
                destination_node
                    .get_pg(0)
                    .unwrap()
                    .metadata_command_replica_state()
                    .unwrap(),
                state_before,
                "unsupported checkpoint version {unsupported_version} mutated destination state"
            );
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_storage_rpc_rejects_unsupported_metadata_command_before_mutation_dispatch() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .bind()
            .unwrap();
        let destination_node = Arc::clone(&server._node);
        let state_before = destination_node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let mutation_dispatches = Arc::new(AtomicUsize::new(0));
        let mutation_dispatches_for_hook = Arc::clone(&mutation_dispatches);
        server.set_metadata_command_before_commit_test_hook(Arc::new(move |_, _| {
            mutation_dispatches_for_hook.fetch_add(1, Ordering::SeqCst);
        }));
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            StorageRpcClientEndpoint::unix(socket_path),
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let command = test_metadata_command(0, 1);
        let request = StorageRpcMetadataCommandRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            command: command.clone(),
        };

        for unsupported_version in [7_u16, 9] {
            let command_bytes =
                command.command_bytes_with_encoding_version_for_test(unsupported_version);
            let payload = encode_metadata_command_request_with_raw_command_for_test(
                &request,
                &command_bytes,
            );
            let error = client
                .rpc_request_result(
                    StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                    payload,
                )
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
            assert_eq!(error.message, "invalid metadata command envelope");
            assert_eq!(
                mutation_dispatches.load(Ordering::SeqCst),
                0,
                "unsupported command v{unsupported_version} reached mutation dispatch"
            );
            assert_eq!(
                destination_node
                    .get_pg(0)
                    .unwrap()
                    .metadata_command_replica_state()
                    .unwrap(),
                state_before,
                "unsupported command v{unsupported_version} mutated replica state"
            );
            assert_eq!(
                destination_node
                    .get_pg(0)
                    .unwrap()
                    .max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap(),
                0,
                "unsupported command v{unsupported_version} published a durable log entry"
            );
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn storage_node_server_rejects_unsupported_outer_frames_before_mutation_dispatch() {
        for authenticated in [false, true] {
            for unsupported_version in [30_u16, 32] {
                let tmp = test_util::tempdir();
                let config = test_config(&tmp);
                private_socket_dir(config.socket_path.parent().unwrap());
                let credential =
                    storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
                        instance_id: "frontend-1".to_owned(),
                    });
                let mut prepared = PreparedStorageNodeServer::new(config.clone());
                if authenticated {
                    prepared = prepared.with_rpc_auth(storage_rpc_server_auth(&credential));
                }
                let server = prepared.bind().unwrap();
                let destination_node = Arc::clone(&server._node);
                let state_before = destination_node
                    .get_pg(0)
                    .unwrap()
                    .metadata_command_replica_state()
                    .unwrap();
                let mutation_dispatches = Arc::new(AtomicUsize::new(0));
                let mutation_dispatches_for_hook = Arc::clone(&mutation_dispatches);
                server.set_metadata_command_before_commit_test_hook(Arc::new(move |_, _| {
                    mutation_dispatches_for_hook.fetch_add(1, Ordering::SeqCst);
                }));
                let socket_path = config.socket_path.clone();
                let join = thread::spawn(move || server.accept_one());

                let command = test_metadata_command(0, 1);
                let payload = encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                    node_id: config.node_id,
                    cluster_epoch: config.cluster_epoch,
                    pg_id: PgId::new(0),
                    command,
                })
                .unwrap();
                let frame = StorageRpcFrame {
                    request_id: 1,
                    kind: StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                    payload,
                };
                let encoded_frame = encode_storage_rpc_frame_with_version_for_test(
                    frame.request_id,
                    frame.kind,
                    &frame.payload,
                    unsupported_version,
                );
                let mut client = UnixStream::connect(socket_path).unwrap();
                if authenticated {
                    let now_ms = crate::clock::current_time_millis();
                    let envelope = sign_storage_rpc_request_with_encoded_frame_for_test(
                        StorageRpcAuthRequestInput {
                            credential: &credential,
                            target_node_id: config.node_id,
                            topology_generation: 9,
                            topology_digest: STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                            issued_at_ms: now_ms,
                            expires_at_ms: now_ms + 5_000,
                            frame: &frame,
                        },
                        &encoded_frame,
                    )
                    .unwrap();
                    write_storage_rpc_auth_transport_frame(&mut client, &envelope).unwrap();
                } else {
                    client.write_all(&encoded_frame).unwrap();
                }
                drop(client);

                let error = join.join().unwrap().unwrap_err();
                let message = match error {
                    StorageNodeServerError::RpcStream { message } => message,
                    other => panic!(
                        "unsupported outer frame v{unsupported_version}, authenticated={authenticated}, returned {other:?}"
                    ),
                };
                if authenticated {
                    assert_eq!(
                        message,
                        "storage RPC stream I/O error: storage RPC authentication rejected: Malformed",
                        "authenticator-valid outer frame v{unsupported_version} was not rejected by inner-frame decoding"
                    );
                } else {
                    assert_eq!(
                        message,
                        format!(
                            "unsupported storage RPC frame encoding version {unsupported_version}"
                        )
                    );
                }
                assert_eq!(
                    mutation_dispatches.load(Ordering::SeqCst),
                    0,
                    "unsupported outer frame v{unsupported_version}, authenticated={authenticated}, reached mutation dispatch"
                );
                assert_eq!(
                    destination_node
                        .get_pg(0)
                        .unwrap()
                        .metadata_command_replica_state()
                        .unwrap(),
                    state_before,
                    "unsupported outer frame v{unsupported_version}, authenticated={authenticated}, mutated replica state"
                );
                assert_eq!(
                    destination_node
                        .get_pg(0)
                        .unwrap()
                        .max_metadata_command_log_index(config.cluster_epoch)
                        .unwrap(),
                    0,
                    "unsupported outer frame v{unsupported_version}, authenticated={authenticated}, published a durable log entry"
                );
            }
        }
    }

    fn assert_authenticated_transport_rejected_before_mutation_dispatch(
        label: &str,
        expected_server_message: &str,
        encode_transport: impl FnOnce(
            &ControlPlaneScopedCredential,
            NodeId,
            &StorageRpcFrame,
        ) -> Vec<u8>,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .bind()
            .unwrap();
        let destination_node = Arc::clone(&server._node);
        let state_before = destination_node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        let mutation_dispatches = Arc::new(AtomicUsize::new(0));
        let mutation_dispatches_for_hook = Arc::clone(&mutation_dispatches);
        server.set_metadata_command_before_commit_test_hook(Arc::new(move |_, _| {
            mutation_dispatches_for_hook.fetch_add(1, Ordering::SeqCst);
        }));
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one());

        let command = test_metadata_command(0, 1);
        let payload = encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
            node_id: config.node_id,
            cluster_epoch: config.cluster_epoch,
            pg_id: PgId::new(0),
            command,
        })
        .unwrap();
        let frame = StorageRpcFrame {
            request_id: 1,
            kind: StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            payload,
        };
        let encoded = encode_transport(&credential, config.node_id, &frame);
        let mut client = UnixStream::connect(socket_path).unwrap();
        client.write_all(&encoded).unwrap();
        drop(client);

        let error = join.join().unwrap().unwrap_err();
        let message = match error {
            StorageNodeServerError::RpcStream { message } => message,
            other => panic!("{label} returned {other:?}"),
        };
        assert_eq!(message, expected_server_message, "unexpected {label} error");
        assert_eq!(
            mutation_dispatches.load(Ordering::SeqCst),
            0,
            "{label} reached mutation dispatch"
        );
        assert_eq!(
            destination_node
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap(),
            state_before,
            "{label} mutated replica state"
        );
        assert_eq!(
            destination_node
                .get_pg(0)
                .unwrap()
                .max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            0,
            "{label} published a durable log entry"
        );
    }

    #[test]
    fn authenticated_storage_rpc_rejects_unsupported_binding_before_mutation_dispatch() {
        for unsupported_version in [0_u16, 3] {
            assert_authenticated_transport_rejected_before_mutation_dispatch(
                &format!("authenticator-valid binding v{unsupported_version}"),
                "storage RPC stream I/O error: storage RPC authentication rejected: Malformed",
                |credential, target_node_id, frame| {
                    let now_ms = crate::clock::current_time_millis();
                    let envelope = sign_storage_rpc_request_with_binding_version_for_test(
                        StorageRpcAuthRequestInput {
                            credential,
                            target_node_id,
                            topology_generation: 9,
                            topology_digest: STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                            issued_at_ms: now_ms,
                            expires_at_ms: now_ms + 5_000,
                            frame,
                        },
                        unsupported_version,
                    )
                    .unwrap();
                    encode_storage_rpc_auth_transport_frame_with_version_for_test(&envelope, 1)
                },
            );
        }
    }

    #[test]
    fn authenticated_storage_rpc_rejects_unsupported_auth_envelope_before_mutation_dispatch() {
        for unsupported_version in [0_u16, 1, 3] {
            assert_authenticated_transport_rejected_before_mutation_dispatch(
                &format!("authenticator-valid auth envelope v{unsupported_version}"),
                "storage RPC stream I/O error: storage RPC authentication rejected: Envelope(UnsupportedVersion)",
                |credential, target_node_id, frame| {
                    let now_ms = crate::clock::current_time_millis();
                    let envelope = sign_storage_rpc_request_with_auth_envelope_version_for_test(
                        StorageRpcAuthRequestInput {
                            credential,
                            target_node_id,
                            topology_generation: 9,
                            topology_digest: STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                            issued_at_ms: now_ms,
                            expires_at_ms: now_ms + 5_000,
                            frame,
                        },
                        unsupported_version,
                    )
                    .unwrap();
                    encode_storage_rpc_auth_transport_frame_with_version_for_test(&envelope, 1)
                },
            );
        }
    }

    #[test]
    fn authenticated_storage_rpc_rejects_unsupported_transport_before_auth_or_mutation_dispatch() {
        for unsupported_version in [0_u16, 2] {
            assert_authenticated_transport_rejected_before_mutation_dispatch(
                &format!("signed transport v{unsupported_version}"),
                &format!(
                    "storage RPC stream I/O error: unsupported authenticated storage RPC transport version {unsupported_version}"
                ),
                |credential, target_node_id, frame| {
                    let now_ms = crate::clock::current_time_millis();
                    let envelope = sign_storage_rpc_request(StorageRpcAuthRequestInput {
                        credential,
                        target_node_id,
                        topology_generation: 9,
                        topology_digest: STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                        issued_at_ms: now_ms,
                        expires_at_ms: now_ms + 5_000,
                        frame,
                    })
                    .unwrap();
                    encode_storage_rpc_auth_transport_frame_with_version_for_test(
                        &envelope,
                        unsupported_version,
                    )
                },
            );
        }
    }

    #[test]
    fn authenticated_tls_tcp_storage_rpc_crosses_real_server_boundary() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
            .bind()
            .unwrap();
        let address = server.tcp_listener_addr_for_test();
        let join = thread::spawn(move || server.accept_one());
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        for _ in 0..2 {
            let payload = client
                .rpc_request(StorageRpcMessageKind::Health, Vec::new())
                .unwrap();
            let health = decode_health_response(&payload).unwrap();

            assert_eq!(health.node_id, config.node_id);
            assert_eq!(health.cluster_epoch, config.cluster_epoch);
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_tls_payload_lease_release_reuses_the_session_connection() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
            .bind()
            .unwrap();
        let address = server.tcp_listener_addr_for_test();
        let join = thread::spawn(move || server.accept_one());
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let bucket = BucketName::new("tls-lease-reuse-bucket").unwrap();
        let generation_id = GenerationId::new(1).unwrap();

        for attempt in 0..2 {
            let key = crate::ObjectKey::new(format!("tls-lease-reuse-key-{attempt}")).unwrap();
            let mut lease = client
                .open_object_payload_lease_route(
                    config.cluster_epoch,
                    &bucket,
                    &key,
                    generation_id,
                )
                .unwrap()
                .acquire_object_payload_lease(ObjectPayloadLeaseKind::BroadSnapshot)
                .unwrap()
                .expect("lease should be acquired");

            assert_eq!(client.idle_session_connection_count_for_test(), 0);
            assert_eq!(lease.release().unwrap(), 0);
            drop(lease);
            assert_eq!(client.idle_session_connection_count_for_test(), 1);
        }

        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_tls_multi_client_lease_saturation_preserves_read_handoff() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth_with_max_connections(&credential, 2))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap(),
        );
        let address = server.tcp_listener_addr_for_test();
        let joins = (0..3)
            .map(|_| {
                let server = Arc::clone(&server);
                thread::spawn(move || server.accept_one())
            })
            .collect::<Vec<_>>();
        let new_client = || {
            let endpoint = StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap();
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                endpoint,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(storage_rpc_client_auth_with_max_connections(
                    credential.clone(),
                    9,
                    2,
                )),
            )
        };
        let first_client = new_client();
        let second_client = new_client();
        let bucket = BucketName::new("tls-lease-handoff-bucket").unwrap();
        let key = crate::ObjectKey::new("tls-lease-handoff-key").unwrap();
        let generation_id = GenerationId::new(1).unwrap();
        let mut lease = first_client
            .open_object_payload_lease_route(
                config.cluster_epoch,
                &bucket,
                &key,
                generation_id,
            )
            .unwrap()
            .acquire_object_payload_lease(ObjectPayloadLeaseKind::BroadSnapshot)
            .unwrap()
            .expect("first client must acquire the globally admitted lease");

        let error = match second_client
            .open_object_payload_lease_route(
                config.cluster_epoch,
                &bucket,
                &key,
                generation_id,
            )
            .unwrap()
            .acquire_object_payload_lease(ObjectPayloadLeaseKind::BroadSnapshot)
        {
            Ok(_) => panic!("second client consumed the read-handoff reservation"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            StoreError::StorageRpcResourceExhausted { .. }
        ));

        let location = test_location(config.cluster_epoch.get(), 0, config.node_id.as_u32());
        let shard_key = test_shard_key(location.shard_index().get());
        let shard_payload = b"read data through the saturated read-handle session";
        let expected_ack = server
            ._node
            .write_shard_file(location.data_pg_id().get(), &shard_key, shard_payload)
            .unwrap();
        let mut read_handle = ShardReadHandleNodeClient::open_shard_read_handle_route(
            &first_client,
            config.cluster_epoch,
            "saturated-read-handoff",
            vec![(location, shard_key.clone())],
        )
        .unwrap()
        .acquire()
        .expect("the reserved server slot must admit the read-handle handoff");
        let mut read_payload = vec![0; shard_payload.len()];
        read_handle
            .read_placed_shard_into(location, &shard_key, expected_ack, &mut read_payload)
            .expect("the retained read-handle session must carry the shard read");
        assert_eq!(read_payload, shard_payload);
        read_handle.release().unwrap();
        let released_error = read_handle
            .read_placed_shard_into(location, &shard_key, expected_ack, &mut read_payload)
            .unwrap_err();
        assert!(matches!(
            released_error,
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "read placed shard through released read-handle lease",
            }
        ));
        assert_eq!(lease.release().unwrap(), 0);
        drop(read_handle);
        drop(lease);
        drop(first_client);
        drop(second_client);
        for join in joins {
            assert!(join.join().unwrap().is_ok());
        }
    }

    #[test]
    fn authenticated_tls_shard_reads_require_exact_active_session_handle() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap(),
        );
        let address = server.tcp_listener_addr_for_test();
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one());
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let location = test_location(config.cluster_epoch.get(), 0, config.node_id.as_u32());
        let shard_key = test_shard_key(location.shard_index().get());
        let payload = b"session-bound shard payload";
        let expected_ack = server
            ._node
            .write_shard_file(location.data_pg_id().get(), &shard_key, payload)
            .unwrap();
        let foreign_location = test_location_with_shard(
            config.cluster_epoch.get(),
            0,
            config.node_id.as_u32(),
            1,
        );
        let foreign_key = test_shard_key(foreign_location.shard_index().get());
        let foreign_payload = b"foreign session-bound shard payload";
        let foreign_ack = server
            ._node
            .write_shard_file(
                foreign_location.data_pg_id().get(),
                &foreign_key,
                foreign_payload,
            )
            .unwrap();
        let mut session = client.open_read_handle_session_for_test().unwrap();

        let mut read = vec![0; payload.len()];
        let missing_full_error = session
            .read_full_placed_shard_for_test(location, &shard_key, expected_ack)
            .unwrap_err();
        assert!(matches!(
            missing_full_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
        let missing_error = session
            .read_placed_shard_into_for_test(location, &shard_key, expected_ack, &mut read)
            .unwrap_err();
        assert!(matches!(
            missing_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));

        session
            .acquire_read_handles("exact-read", vec![(location, shard_key.clone())])
            .unwrap();
        let mut foreign_read = vec![0; foreign_payload.len()];
        let foreign_full_error = session
            .read_full_placed_shard_for_test(foreign_location, &foreign_key, foreign_ack)
            .unwrap_err();
        assert!(matches!(
            foreign_full_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
        let foreign_error = session
            .read_placed_shard_into_for_test(
                foreign_location,
                &foreign_key,
                foreign_ack,
                &mut foreign_read,
            )
            .unwrap_err();
        assert!(matches!(
            foreign_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
        let full_read = session
            .read_full_placed_shard_for_test(location, &shard_key, expected_ack)
            .unwrap();
        assert_eq!(full_read, payload);
        session
            .read_placed_shard_into_for_test(location, &shard_key, expected_ack, &mut read)
            .unwrap();
        assert_eq!(read, payload);

        session.release_read_handles("exact-read").unwrap();
        let released_full_error = session
            .read_full_placed_shard_for_test(location, &shard_key, expected_ack)
            .unwrap_err();
        assert!(matches!(
            released_full_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
        let released_error = session
            .read_placed_shard_into_for_test(location, &shard_key, expected_ack, &mut read)
            .unwrap_err();
        assert!(matches!(
            released_error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
        drop(session);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    fn authenticated_skipped_transfer_destination_epoch_uses_current_marker(tcp: bool) {
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        let destination_epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch = ClusterEpoch::new(3).unwrap();
        let mut skipped_destination_route = config.pg_routes[0].clone();
        skipped_destination_route.cluster_epoch = destination_epoch;
        skipped_destination_route.state = PgState::Active;
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
        config.historical_pg_routes.push(skipped_destination_route);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let expected_state_digest = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .state_digest;
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            destination_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let state = MetadataCommandInspectionNodeClient::metadata_command_replica_state(
            &client,
            PgId::new(0),
        )
        .unwrap();
        assert_eq!(state.applied_log_index, 0);
        assert!(
            MetadataCommandInspectionNodeClient::metadata_command_replica_state_can_initialize(
                &client,
                PgId::new(0),
                destination_epoch,
            )
            .unwrap()
        );
        let route = client
            .open_metadata_command_peering_route(PgId::new(0), destination_epoch)
            .unwrap();
        let initialized = route
            .initialize_metadata_transfer_empty_state(expected_state_digest)
            .unwrap();
        assert_eq!(initialized.cluster_epoch, destination_epoch);
        assert_eq!(initialized.state_digest, expected_state_digest);

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_skipped_transfer_destination_epoch_uses_current_marker() {
        authenticated_skipped_transfer_destination_epoch_uses_current_marker(false);
    }

    #[test]
    fn authenticated_tls_skipped_transfer_destination_epoch_uses_current_marker() {
        authenticated_skipped_transfer_destination_epoch_uses_current_marker(true);
    }

    fn authenticated_live_pg_transfer_replays_nonempty_suffix(tcp: bool) {
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        config.pg_routes[0].state = PgState::Peering;
        config.pg_routes[0].metadata_transfer_destination_epoch = Some(config.cluster_epoch);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Admin {
            instance_id: "live-pg-transfer-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(live_pg_metadata_transfer_storage_rpc_client_auth(
                credential,
            )),
        );
        let route = client
            .open_metadata_command_peering_route(PgId::new(0), config.cluster_epoch)
            .unwrap();
        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = AclGrants::default();
        let create_config = CreateBucketConfig {
            name: "metadata-rpc-bucket",
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let first = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                config.cluster_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 1_234, 1).unwrap(),
            ),
        );
        let first_state = route.replay_metadata_command_for_peering(&first).unwrap();
        assert_eq!(first_state.applied_log_index, 1);

        let validated = route
            .validate_metadata_command_replay_state_preserving_pending_slot()
            .unwrap();
        assert_eq!(validated, first_state);

        let second = test_metadata_command_for_subject(
            0,
            2,
            crate::tests::bucket_name("metadata-rpc-bucket"),
            crate::tests::object_key("second-object"),
        );
        let second_state = route.replay_metadata_command_for_peering(&second).unwrap();
        assert_eq!(second_state.applied_log_index, 2);
        assert_ne!(second_state.state_digest, first_state.state_digest);

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_live_pg_transfer_replays_nonempty_suffix() {
        authenticated_live_pg_transfer_replays_nonempty_suffix(false);
    }

    #[test]
    fn authenticated_tls_live_pg_transfer_replays_nonempty_suffix() {
        authenticated_live_pg_transfer_replays_nonempty_suffix(true);
    }

    fn authenticated_metadata_transfer_staging_publication_round_trip(
        tcp: bool,
        delay_artifact_chunks_past_auth_window: bool,
        enforce_absolute_artifact_read_deadline: bool,
    ) {
        let tmp = test_util::tempdir();
        let staging =
            crate::control_plane::tests::transitions::authenticated_staging_authorization_fixture();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        config.node_id = staging.destination_node_id;
        config.cluster_epoch = staging.runtime_map.cluster_epoch();
        config.pg_routes[0].cluster_epoch = config.cluster_epoch;
        config.pg_routes[0].primary_node_id = staging.destination_node_id;
        config.pg_routes[0].acting_set = vec![staging.destination_node_id];
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Admin {
            instance_id: "metadata-transfer-staging-1".to_owned(),
        });
        let transport_limits = storage_rpc_test_transport_limits_with_io_timeout(
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
            if delay_artifact_chunks_past_auth_window {
                Duration::from_secs(7)
            } else if enforce_absolute_artifact_read_deadline {
                Duration::from_secs(2)
            } else {
                crate::StorageRpcTransportLimits::DEFAULT.io_timeout()
            },
        );
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(
                storage_rpc_server_auth(&credential).with_transport_limits(transport_limits),
            )
            .with_metadata_transfer_staging_node_incarnation(11);
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = Arc::new(prepared.bind().unwrap());
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(
                live_pg_metadata_transfer_storage_rpc_client_auth_with_transport_limits(
                    credential,
                    transport_limits,
                ),
            ),
        );
        let transition_epoch = staging.intent.transition_epoch();
        let initial_destination_epoch = staging.initial_destination_epoch;
        let rebased_destination_epoch = staging.rebased_destination_epoch;
        let not_observed = client
            .create_metadata_transfer_staging_intent(&staging.authorization, &staging.intent)
            .unwrap_err();
        assert_eq!(
            not_observed.operation_failure_class(),
            crate::error::StoreOperationFailureClass::RetryableConvergence
        );
        assert!(!server
            .metadata_transfer_staging_store
            .as_ref()
            .unwrap()
            .has_intent_for_test(
                staging.intent.pg_id(),
                staging.intent.staging_generation(),
            ));

        server.install_authority_runtime_map_for_test(&staging.cross_member_runtime_map);
        assert!(client
            .create_metadata_transfer_staging_intent(
                &staging.cross_member_authorization,
                &staging.cross_member_intent,
            )
            .is_err());
        assert!(!server
            .metadata_transfer_staging_store
            .as_ref()
            .unwrap()
            .has_intent_for_test(
                staging.cross_member_intent.pg_id(),
                staging.cross_member_intent.staging_generation(),
            ));

        let misrouted_binding = UnavailablePgTransitionMutationBinding::new(
            PgId::new(0),
            transition_epoch,
            staging.intent.source_epoch(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
        );
        let misrouted_artifact =
            crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
                &misrouted_binding,
                initial_destination_epoch,
            );
        let misrouted_intent =
            crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
                &misrouted_binding,
                checksum::sha256::digest(&misrouted_artifact),
                u64::try_from(misrouted_artifact.len()).unwrap(),
                crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            )
            .unwrap();
        let misrouted_authorization =
            crate::pg_store::committed_staging_authorization_for_intent_for_test(
                &misrouted_intent,
            );
        assert!(client
            .create_metadata_transfer_staging_intent(
                &misrouted_authorization,
                &misrouted_intent,
            )
            .is_err());
        assert!(client
            .publish_metadata_transfer_staging_artifact(
                &misrouted_authorization,
                &misrouted_intent,
                &misrouted_artifact,
            )
            .is_err());

        let malformed_artifact =
            b"ARGMIN-METADATA-TRANSFER-ARTIFACT-V2\0malformed".as_slice();
        let malformed_intent =
            crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
                &UnavailablePgTransitionMutationBinding::new(
                    PgId::new(1),
                    transition_epoch,
                    staging.intent.source_epoch(),
                    vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
                    vec![config.node_id, NodeId::new(2), NodeId::new(3)],
                ),
                checksum::sha256::digest(malformed_artifact),
                u64::try_from(malformed_artifact.len()).unwrap(),
                crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            )
            .unwrap();
        let malformed_authorization =
            crate::pg_store::committed_staging_authorization_for_intent_for_test(
                &malformed_intent,
            );
        assert!(client
            .create_metadata_transfer_staging_intent(
                &malformed_authorization,
                &malformed_intent,
            )
            .is_err());
        assert!(!server
            .metadata_transfer_staging_store
            .as_ref()
            .unwrap()
            .has_intent_for_test(
                malformed_intent.pg_id(),
                malformed_intent.staging_generation(),
            ));
        let intent = staging.intent;
        let artifact = staging.artifact;
        let authorization = staging.authorization;
        server.install_authority_runtime_map_for_test(&staging.runtime_map);

        let mut malformed_presentation = authorization.presentation().clone();
        let mut malformed_digest = malformed_presentation.batch_members_digest();
        malformed_digest[0] ^= 0x80;
        malformed_presentation.test_set_batch_members_digest(malformed_digest);
        let malformed_error = client
            .create_metadata_transfer_staging_intent_with_presentation_for_test(
                &malformed_presentation,
                &intent,
            )
            .unwrap_err();
        assert_eq!(
            malformed_error.operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );

        let mut stale_presentation = authorization.presentation().clone();
        stale_presentation.test_set_committed_epoch(
            ClusterEpoch::new(config.cluster_epoch.get().checked_sub(1).unwrap()).unwrap(),
        );
        let stale_error = client
            .create_metadata_transfer_staging_intent_with_presentation_for_test(
                &stale_presentation,
                &intent,
            )
            .unwrap_err();
        assert_eq!(
            stale_error.operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );
        assert!(!server
            .metadata_transfer_staging_store
            .as_ref()
            .unwrap()
            .has_intent_for_test(intent.pg_id(), intent.staging_generation()));

        assert!(client
            .create_metadata_transfer_staging_intent(&malformed_authorization, &intent)
            .is_err());

        assert!(matches!(
            client.read_metadata_transfer_staging_artifact(&authorization, &intent),
            Err(crate::StoreError::NotFound)
        ));
        client
            .create_metadata_transfer_staging_intent(&authorization, &intent)
            .unwrap();
        client
            .create_metadata_transfer_staging_intent(&authorization, &intent)
            .unwrap();
        assert!(matches!(
            client.read_metadata_transfer_staging_artifact(&authorization, &intent),
            Err(crate::StoreError::NotFound)
        ));
        let first = client
            .publish_metadata_transfer_staging_artifact(&authorization, &intent, &artifact)
            .unwrap();
        let replay = client
            .publish_metadata_transfer_staging_artifact(&authorization, &intent, &artifact)
            .unwrap();
        assert_eq!(replay, first);
        assert!(!first.as_bytes().is_empty());
        assert_eq!(
            client
                .read_metadata_transfer_staging_artifact(&authorization, &intent)
                .unwrap(),
            artifact
        );
        if delay_artifact_chunks_past_auth_window {
            let response_count = Arc::new(AtomicUsize::new(0));
            let response_count_for_hook = Arc::clone(&response_count);
            server.set_response_envelope_test_hook(Arc::new(move |kind, _envelope| {
                if kind == StorageRpcMessageKind::MetadataTransferStagingArtifactRead {
                    response_count_for_hook.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(
                        crate::storage_rpc_auth::STORAGE_RPC_AUTH_REPLAY_WINDOW_MS + 100,
                    ));
                }
            }));
            let started = Instant::now();
            assert_eq!(
                client
                    .read_metadata_transfer_staging_artifact(&authorization, &intent)
                    .unwrap(),
                artifact
            );
            assert_eq!(response_count.load(Ordering::SeqCst), 1);
            assert!(
                started.elapsed()
                    > Duration::from_millis(
                        crate::storage_rpc_auth::STORAGE_RPC_AUTH_REPLAY_WINDOW_MS,
                    ),
                "one signed chunk response must cross the fixed authentication window"
            );
            server.set_response_envelope_test_hook(Arc::new(|_, _| {}));
        }
        if enforce_absolute_artifact_read_deadline {
            let response_count = Arc::new(AtomicUsize::new(0));
            let response_count_for_hook = Arc::clone(&response_count);
            server.set_response_envelope_test_hook(Arc::new(move |kind, _envelope| {
                if kind == StorageRpcMessageKind::MetadataTransferStagingArtifactRead {
                    let response_index = response_count_for_hook.fetch_add(1, Ordering::SeqCst);
                    if response_index == 1 {
                        thread::sleep(Duration::from_millis(1_200));
                    }
                }
            }));
            let chunk_bytes = artifact.len().div_ceil(2);
            assert!(client
                .read_metadata_transfer_staging_artifact_with_deadline_for_test(
                    &authorization,
                    &intent,
                    u32::try_from(chunk_bytes).unwrap(),
                    Duration::from_secs(1),
                )
                .is_err());
            server.set_response_envelope_test_hook(Arc::new(|_, _| {}));
            assert_eq!(response_count.load(Ordering::SeqCst), 2);
            drop(client);
            let _connection_result = join.join().expect("storage server thread must not panic");
            return;
        }
        for invalid in [&malformed_presentation, &stale_presentation] {
            let error = client
                .read_metadata_transfer_staging_artifact_with_presentation_for_test(
                    invalid, &intent,
                )
                .unwrap_err();
            assert_eq!(
                error.operation_failure_class(),
                crate::error::StoreOperationFailureClass::Other
            );
        }
        let assert_published_artifact_retained = || {
            let store = server.metadata_transfer_staging_store.as_ref().unwrap();
            assert!(store.has_intent_for_test(intent.pg_id(), intent.staging_generation()));
            assert_eq!(store.read_artifact(&intent).unwrap(), artifact);
        };
        let malformed_tombstone_error = client
            .tombstone_metadata_transfer_staging_artifact_with_presentation_for_test(
                &malformed_presentation,
                &intent,
            )
            .unwrap_err();
        assert_eq!(
            malformed_tombstone_error.operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );
        assert_published_artifact_retained();
        let stale_tombstone_error = client
            .tombstone_metadata_transfer_staging_artifact_with_presentation_for_test(
                &stale_presentation,
                &intent,
            )
            .unwrap_err();
        assert_eq!(
            stale_tombstone_error.operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );
        assert_published_artifact_retained();
        server.install_authority_runtime_map_for_test(
            &staging.cross_member_tombstone_runtime_map,
        );
        let cross_member_tombstone_error = client
            .tombstone_metadata_transfer_staging_artifact_with_presentation_for_test(
                &staging.cross_member_tombstone_presentation,
                &intent,
            )
            .unwrap_err();
        assert_eq!(
            cross_member_tombstone_error.operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );
        assert_eq!(
            client
                .read_metadata_transfer_staging_artifact_with_presentation_for_test(
                    &staging.cross_member_tombstone_presentation,
                    &intent,
                )
                .unwrap_err()
                .operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );
        assert_published_artifact_retained();
        server.install_authority_runtime_map_for_test(&staging.runtime_map);
        let initial = crate::pg_store::decode_staging_evidence(first.as_bytes()).unwrap();
        assert_eq!(initial.target_epoch(), Some(initial_destination_epoch));
        let rebased = client
            .publish_metadata_transfer_staging_proof(
                &authorization,
                &intent,
                rebased_destination_epoch,
            )
            .unwrap();
        let rebased_evidence =
            crate::pg_store::decode_staging_evidence(rebased.as_bytes()).unwrap();
        assert_eq!(
            rebased_evidence.target_epoch(),
            Some(rebased_destination_epoch)
        );
        assert_ne!(rebased_evidence.transfer(), initial.transfer());
        assert_eq!(
            client
                .publish_metadata_transfer_staging_proof(
                    &authorization,
                    &intent,
                    rebased_destination_epoch,
                )
                .unwrap(),
            rebased
        );
        let tombstone = client
            .tombstone_metadata_transfer_staging_artifact(&authorization, &intent)
            .unwrap();
        assert_eq!(
            client
                .tombstone_metadata_transfer_staging_artifact(&authorization, &intent)
                .unwrap(),
            tombstone
        );
        let tombstone_evidence =
            crate::pg_store::decode_staging_evidence(tombstone.as_bytes()).unwrap();
        assert_eq!(
            tombstone_evidence.kind(),
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone
        );
        assert_eq!(tombstone_evidence.intent(), &intent);
        assert_eq!(tombstone_evidence.actor().node_id(), config.node_id);
        assert_eq!(tombstone_evidence.target_epoch(), None);
        assert_eq!(tombstone_evidence.transfer(), None);
        assert!(matches!(
            server
                .metadata_transfer_staging_store
                .as_ref()
                .unwrap()
                .read_artifact(&intent),
            Err(crate::pg_store::MetadataTransferStagingError::GenerationRetired)
        ));
        assert_eq!(
            client
                .read_metadata_transfer_staging_artifact(&authorization, &intent)
                .unwrap_err()
                .operation_failure_class(),
            crate::error::StoreOperationFailureClass::Other
        );

        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_metadata_transfer_staging_publication_round_trip() {
        authenticated_metadata_transfer_staging_publication_round_trip(false, false, false);
    }

    #[test]
    fn authenticated_tls_metadata_transfer_staging_publication_round_trip() {
        authenticated_metadata_transfer_staging_publication_round_trip(true, false, false);
    }

    #[test]
    fn authenticated_chunked_artifact_read_remains_fresh_past_single_response_window() {
        authenticated_metadata_transfer_staging_publication_round_trip(false, true, false);
    }

    #[test]
    fn authenticated_chunked_artifact_read_uses_one_absolute_deadline() {
        authenticated_metadata_transfer_staging_publication_round_trip(false, false, true);
    }

    #[test]
    fn storage_node_bootstrap_preserves_committed_staging_authorizations() {
        let tmp = test_util::tempdir();
        let staging =
            crate::control_plane::tests::transitions::authenticated_staging_authorization_fixture();
        let destination_node_id = NodeId::new(2);
        let socket_path = PathBuf::from("/tmp/transition-node-2.sock");
        let bootstrap = StorageNodeBootstrap::open_control_plane_managed(
            destination_node_id,
            tmp.path().join("node"),
            &[70, 71],
            EcShape { k: 2, m: 1 },
            &socket_path,
        )
        .unwrap();
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Admin {
            instance_id: "metadata-transfer-bootstrap-1".to_owned(),
        });
        let prepared = bootstrap
            .prepare(&staging.runtime_map)
            .unwrap()
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .with_metadata_transfer_staging_node_incarnation(11)
            .with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        assert!(prepared.verifies_staging_authorization_for_test(
            destination_node_id,
            staging.intent.pg_id(),
            staging.authorization.presentation(),
        ));
        let server = Arc::new(prepared.bind().unwrap());
        let address = server.tcp_listener_addr_for_test();
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            destination_node_id,
            staging.runtime_map.cluster_epoch(),
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(live_pg_metadata_transfer_storage_rpc_client_auth(
                credential,
            )),
        );
        let authorization =
            crate::control_plane::committed_staging_authorization_from_presentation_for_test(
                staging.authorization.presentation().clone(),
                destination_node_id,
                staging.intent.pg_id(),
            );

        client
            .create_metadata_transfer_staging_intent(&authorization, &staging.intent)
            .unwrap();
        assert!(server
            .metadata_transfer_staging_store
            .as_ref()
            .unwrap()
            .has_intent_for_test(
                staging.intent.pg_id(),
                staging.intent.staging_generation(),
            ));

        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    fn authenticated_checkpoint_export_uses_current_peering_source_fence(tcp: bool) {
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        let source_epoch = config.cluster_epoch;
        let current_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
        let mut source_route = config.pg_routes[0].clone();
        source_route.state = PgState::Peering;
        config.cluster_epoch = current_epoch;
        config.pg_routes[0].cluster_epoch = current_epoch;
        config.pg_routes[0].state = PgState::Peering;
        config.historical_pg_routes.push(source_route);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let expected_checkpoint = server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_checkpoint(config.node_id.as_u32(), source_epoch)
            .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            current_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let checkpoint = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
            &client,
            PgId::new(0),
            source_epoch,
        );

        drop(client);
        let server_result = join.join().unwrap();
        assert!(server_result.is_ok(), "{server_result:?}");
        assert_eq!(checkpoint.unwrap(), expected_checkpoint);
    }

    #[test]
    fn authenticated_unix_checkpoint_export_uses_current_peering_source_fence() {
        authenticated_checkpoint_export_uses_current_peering_source_fence(false);
    }

    #[test]
    fn authenticated_tls_checkpoint_export_uses_current_peering_source_fence() {
        authenticated_checkpoint_export_uses_current_peering_source_fence(true);
    }

    fn authenticated_bucket_subresource_get_preserves_bucket_not_found(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        )
        .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
        let bucket = crate::tests::bucket_name("missing-subresource-rpc-bucket");
        let route = client
            .open_bucket_metadata_route(
                config.cluster_epoch,
                crate::BucketPgId::new_for_test(PgId::new(0)),
                &bucket,
            )
            .unwrap();

        let error = route
            .get_bucket_subresource(BucketSubresourceKind::Cors)
            .unwrap_err();
        assert!(matches!(
            error,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::BucketNotFound {
                name
            }) if name == bucket
        ));

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_bucket_subresource_get_preserves_bucket_not_found() {
        authenticated_bucket_subresource_get_preserves_bucket_not_found(false);
    }

    #[test]
    fn authenticated_tls_tcp_bucket_subresource_get_preserves_bucket_not_found() {
        authenticated_bucket_subresource_get_preserves_bucket_not_found(true);
    }

    fn authenticated_object_payload_reclaim_claim_adopts_same_owner_retry(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential =
            storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::LocalMaintenance {
                process_id: "reclaim-worker-1".to_owned(),
            });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let bucket = crate::tests::bucket_name("authenticated-reclaim-claim-adoption");
        let key = crate::tests::object_key("retained-root");
        let generation_id = GenerationId::new(41).unwrap();
        let pg = server._node.get_pg(0).unwrap();
        PgMetadataStore::put_object_segments_reclaim(
            &*pg,
            &crate::ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
                created_at: 1,
                segments: Vec::new(),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        drop(pg);

        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let node = Arc::clone(&server._node);
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(Arc::new(
                crate::MaintenanceStorageRpcClientCapability::new(
                    credential,
                    9,
                    STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                )
                .unwrap()
                .into(),
            )),
        )
        .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &client,
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            generation_id,
        )
        .unwrap();
        let first = route
            .acquire_claim(
                AcquireObjectPayloadReclaimClaimReq {
                    reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                    bucket_incarnation_generation: 1,
                    claim_id: "first-authenticated-claim",
                    owner_token: "authenticated-reclaim-owner",
                    claimed_at: 100,
                    lease_deadline: Some(1_000),
                    now: 100,
                },
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap()
            .expect("first authenticated request must acquire the reclaim root");
        let adopted = route
            .acquire_claim(
                AcquireObjectPayloadReclaimClaimReq {
                    reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                    bucket_incarnation_generation: 1,
                    claim_id: "retry-proposed-claim",
                    owner_token: "authenticated-reclaim-owner",
                    claimed_at: 200,
                    lease_deadline: Some(2_000),
                    now: 200,
                },
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap()
            .expect("same authenticated owner must adopt its retained exact-root claim");
        assert_eq!(adopted.claim_id, first.claim_id);
        assert_eq!(adopted.owner_token, first.owner_token);
        assert_eq!(adopted.claimed_at, 200);
        assert_eq!(adopted.lease_deadline, Some(2_000));
        assert_eq!(
            PgMetadataStore::object_payload_reclaim_claim(&*node.get_pg(0).unwrap())
                .unwrap(),
            Some(adopted)
        );

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_object_payload_reclaim_claim_adopts_same_owner_retry() {
        authenticated_object_payload_reclaim_claim_adopts_same_owner_retry(false);
    }

    #[test]
    fn authenticated_tls_object_payload_reclaim_claim_adopts_same_owner_retry() {
        authenticated_object_payload_reclaim_claim_adopts_same_owner_retry(true);
    }

    fn authenticated_bucket_pending_match_binds_requested_mutation(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let bucket = crate::tests::bucket_name("authenticated-bucket-pending-match");
        let owner = crate::CanonicalUserId::from_principal("owner");
        let pg = server._node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        drop(pg);
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        )
        .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
        let route = client
            .open_bucket_metadata_route(
                config.cluster_epoch,
                crate::BucketPgId::new_for_test(PgId::new(0)),
                &bucket,
            )
            .unwrap();
        let command_id = |log_index| {
            MetadataCommandId::new(
                config.cluster_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            )
        };

        let versioning = route
            .build_put_bucket_versioning_command(
                command_id(1),
                crate::BucketVersioningState::Enabled,
            )
            .unwrap();
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &versioning,
        )
        .unwrap();
        let MetadataCommandPayload::PutBucketVersioning(versioning) = versioning.payload() else {
            panic!("unexpected versioning command payload")
        };
        assert!(route
            .pending_put_bucket_versioning_command_matches_current(
                versioning,
                crate::BucketVersioningState::Enabled,
            )
            .unwrap());
        assert!(
            !route
                .pending_put_bucket_versioning_command_matches_current(
                    versioning,
                    crate::BucketVersioningState::Suspended,
                )
                .unwrap(),
            "an authenticated applied Enabled command must not satisfy Suspended"
        );

        let acl_grants = crate::AclGrants::new(vec![s3_types::AclGrant::new(
            s3_types::AclGrantee::CanonicalUser(crate::CanonicalUserId::from_principal(
                "authenticated-first-acl-grantee",
            )),
            s3_types::AclPermission::FullControl,
        )]);
        let different_acl_grants = crate::AclGrants::new(vec![s3_types::AclGrant::new(
            s3_types::AclGrantee::CanonicalUser(crate::CanonicalUserId::from_principal(
                "authenticated-different-acl-grantee",
            )),
            s3_types::AclPermission::FullControl,
        )]);
        let acl_summary = crate::BucketAclSummary {
            public_read: false,
            public_write: false,
        };
        let acl = route
            .build_put_bucket_acl_command(command_id(2), &acl_grants, acl_summary)
            .unwrap();
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &acl,
        )
        .unwrap();
        let MetadataCommandPayload::PutBucketAcl(acl) = acl.payload() else {
            panic!("unexpected ACL command payload")
        };
        assert!(route
            .pending_put_bucket_acl_command_matches_current(acl, &acl_grants, acl_summary)
            .unwrap());
        assert!(
            !route
                .pending_put_bucket_acl_command_matches_current(
                    acl,
                    &different_acl_grants,
                    acl_summary,
                )
                .unwrap(),
            "an authenticated applied ACL must not satisfy different canonical grants"
        );
        assert!(
            !route
                .pending_put_bucket_acl_command_matches_current(
                    acl,
                    &acl_grants,
                    crate::BucketAclSummary {
                        public_read: true,
                        public_write: false,
                    },
                )
                .unwrap(),
            "an authenticated applied ACL must not satisfy a different public-read summary"
        );
        assert!(
            !route
                .pending_put_bucket_acl_command_matches_current(
                    acl,
                    &acl_grants,
                    crate::BucketAclSummary {
                        public_read: false,
                        public_write: true,
                    },
                )
                .unwrap(),
            "an authenticated applied ACL must not satisfy a different public-write summary"
        );

        let public_access_block = crate::PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: false,
            restrict_public_buckets: true,
        };
        let put = BucketPropertyMutation::PublicAccessBlock(Some(public_access_block));
        let delete = BucketPropertyMutation::PublicAccessBlock(None);
        let command = route
            .build_put_bucket_property_command(
                command_id(3),
                &put,
            )
            .unwrap();
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap();
        let MetadataCommandPayload::PutBucketProperty(property) = command.payload() else {
            panic!("unexpected property command payload")
        };
        assert!(route
            .pending_put_bucket_property_command_matches_current(property, &put)
            .unwrap());
        assert!(
            !route
                .pending_put_bucket_property_command_matches_current(property, &delete)
                .unwrap(),
            "an authenticated applied PUT must not satisfy a same-effect DELETE"
        );

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_bucket_pending_match_binds_requested_mutation() {
        authenticated_bucket_pending_match_binds_requested_mutation(false);
    }

    #[test]
    fn authenticated_tls_bucket_pending_match_binds_requested_mutation() {
        authenticated_bucket_pending_match_binds_requested_mutation(true);
    }

    fn authenticated_complete_multipart_command_build_preserves_no_such_upload(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let bucket = crate::tests::bucket_name("missing-complete-multipart-rpc-bucket");
        let key = crate::tests::object_key("missing-complete-multipart-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("missing-complete-multipart-rpc-upload");
        let reservation = {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            let now = crate::clock::current_time_millis();
            let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "missing-complete-multipart-rpc-reservation",
                    owner_token: "missing-complete-multipart-rpc-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    created_at: now,
                    lease_deadline: now.saturating_add(60_000),
                    target_context: Some(key.as_str()),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            BucketWriteReservationProof::from(&reservation)
        };
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let topology = Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        )
        .with_pg_topology(Arc::clone(&topology));
        let part = crate::MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 1,
            size: 12,
            payload_crc64: 0,
            etag: vec![1; 8],
            etag_kind: crate::EtagKind::Crc64,
            part_vid: GenerationId::new(2).unwrap(),
            placement_cluster_epoch: config.cluster_epoch,
            ec_k: 4,
            ec_m: 2,
            last_modified: 1,
            checksum: None,
        };
        let request = crate::CompleteMultipartCommitRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes([0xA5; 32]),
            versioning: BucketVersioningState::Disabled,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(3).unwrap(),
            size: part.size,
            etag_crc64: [4; 8],
            tags: None,
            metadata_blob: Some(crate::SerializedMetadataBlob::default()),
            system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            expected_stale_payload_source: None,
            expected_current_object_identity: None,
            conditional_completion: false,
            part_records: vec![part],
            selected_streaming_segments: Vec::new(),
            expected_cleanup: crate::CompleteMultipartCommitCleanup::default(),
        };
        let expected_object_parts = crate::node_client::complete_multipart_expected_object_parts(
            &request,
            VersionId::Null,
            &topology,
        );
        let route = client
            .open_multipart_completion_mutation_metadata_route(
                config.cluster_epoch,
                crate::ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .unwrap();

        let error = route
            .build_complete_multipart_object_command(BuildCompleteMultipartObjectCommandReq {
                request: &request,
                version_id: VersionId::Null,
                expected_object_parts: &expected_object_parts,
                bucket_write_reservation: &reservation,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload {
                upload_id: missing
            }) if missing == upload_id.as_str()
        ));

        drop(route);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_complete_multipart_command_build_preserves_no_such_upload() {
        authenticated_complete_multipart_command_build_preserves_no_such_upload(false);
    }

    #[test]
    fn authenticated_tls_tcp_complete_multipart_command_build_preserves_no_such_upload() {
        authenticated_complete_multipart_command_build_preserves_no_such_upload(true);
    }

    fn authenticated_stream_part_finalize_preserves_no_such_upload(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = Arc::new(prepared.bind().unwrap());
        let node = Arc::clone(&server._node);
        let bucket = crate::tests::bucket_name("missing-stream-part-rpc-bucket");
        let key = crate::tests::object_key("missing-stream-part-rpc-key");
        let upload_id = crate::tests::multipart_upload_id("missing-stream-part-rpc-upload");
        let session_id = crate::tests::stream_session_id("part-rpc");
        let reservation = {
            let pg = node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            PgMetadataStore::create_multipart_upload(
                &*pg,
                &CreateMultipartUploadReq {
                    upload_id: upload_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    initiator: crate::OwnerIdentity::from_principal("owner"),
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    object_lock: crate::ObjectLockState::default(),
                    checksum: None,
                    encryption: crate::ObjectEncryption::None,
                },
            )
            .unwrap();
            PgMetadataStore::create_stream_upload(
                &*pg,
                &CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::UploadPart {
                        upload_id: upload_id.clone(),
                        part_number: 1,
                    },
                    encryption: crate::ObjectEncryption::None,
                },
            )
            .unwrap();
            let now = crate::clock::current_time_millis();
            let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "missing-stream-part-rpc-reservation",
                    owner_token: "missing-stream-part-rpc-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                    created_at: now,
                    lease_deadline: now.saturating_add(60_000),
                    target_context: Some(key.as_str()),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            BucketWriteReservationProof::from(&reservation)
        };
        let tcp_address = tcp.then(|| server.tcp_listener_addr_for_test());
        let endpoint = if let Some(address) = tcp_address {
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let stop = Arc::new(AtomicBool::new(false));
        let server_thread = Arc::clone(&server);
        let server_stop = Arc::clone(&stop);
        let join = thread::spawn(move || {
            server_thread
                .serve_until_stop_for_test(&server_stop)
                .unwrap();
        });
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        )
        .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
        let route = client
            .open_stream_part_finalization_metadata_route(
                config.cluster_epoch,
                crate::ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
                &upload_id,
                &session_id,
                1,
            )
            .unwrap();
        let snapshot = route.load_snapshot().unwrap();

        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::delete_multipart_upload(&*pg, &upload_id).unwrap();
        PgMetadataStore::delete_stream_upload(&*pg, &session_id).unwrap();
        drop(pg);

        let snapshot_error = route.load_snapshot().unwrap_err();
        assert!(matches!(
            &snapshot_error,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload {
                upload_id: missing
            }) if missing == upload_id.as_str()
        ), "{snapshot_error:?}");

        let part = crate::MultipartPartRecord {
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
            last_modified: 1,
            checksum: None,
        };
        let build_error = route
            .build_commit_command(
                BuildStreamPartCommitCommandReq {
                    expected_snapshot: &snapshot,
                    part: &part,
                    segments: &[],
                    bucket_write_reservation: &reservation,
                },
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap_err();
        assert!(matches!(
            build_error,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload {
                upload_id: missing
            }) if missing == upload_id.as_str()
        ));

        drop(route);
        drop(client);
        stop.store(true, Ordering::Release);
        if let Some(address) = tcp_address {
            let _ = std::net::TcpStream::connect(address);
        } else {
            let _ = UnixStream::connect(&config.socket_path);
        }
        join.join().unwrap();
    }

    #[test]
    fn authenticated_unix_stream_part_finalize_preserves_no_such_upload() {
        authenticated_stream_part_finalize_preserves_no_such_upload(false);
    }

    #[test]
    fn authenticated_tls_tcp_stream_part_finalize_preserves_no_such_upload() {
        authenticated_stream_part_finalize_preserves_no_such_upload(true);
    }

    fn authenticated_metadata_hash_inspection_preserves_integrity_failure(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let mut prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        if tcp {
            prepared = prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ]);
        }
        let server = prepared.bind().unwrap();
        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = AclGrants::default();
        let bucket = crate::tests::bucket_name("metadata-integrity-rpc-bucket");
        let create_config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
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
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 1_234, 1).unwrap(),
            ),
        );
        {
            let pg = server._node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(config.node_id.as_u32(), &command)
                .unwrap();
            pg.test_set_metadata_command_log_checksum(
                1,
                command.checksum_crc64().wrapping_add(1),
            )
            .unwrap();
        }
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let error = MetadataCommandInspectionNodeClient::applied_metadata_command_log_entry_hashes(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            StoreError::StorageRpc {
                failure: StorageRpcErrorCode::MetadataCommandIntegrity,
                ..
            }
        ));
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_metadata_hash_inspection_preserves_integrity_failure() {
        authenticated_metadata_hash_inspection_preserves_integrity_failure(false);
    }

    #[test]
    fn authenticated_tls_metadata_hash_inspection_preserves_integrity_failure() {
        authenticated_metadata_hash_inspection_preserves_integrity_failure(true);
    }

    fn authenticated_metadata_apply_preconnect_failure_is_not_sent(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let endpoint = if tcp {
            // Port zero deterministically fails before connection and cannot
            // be acquired by another parallel test after a listener is dropped.
            let address = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let error = MetadataCommandNodeClient::apply_metadata_command_and_record_until(
            &client,
            PgId::new(0),
            &test_metadata_command(0, 1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();

        assert_eq!(error.kind(), MetadataCommandApplyErrorKind::NotSent);
    }

    #[test]
    fn authenticated_unix_metadata_apply_preconnect_failure_is_not_sent() {
        authenticated_metadata_apply_preconnect_failure_is_not_sent(false);
    }

    #[test]
    fn authenticated_tls_tcp_metadata_apply_preconnect_failure_is_not_sent() {
        authenticated_metadata_apply_preconnect_failure_is_not_sent(true);
    }

    fn authenticated_metadata_apply_response_auth_failure_is_may_have_applied(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let node = Arc::clone(&server._node);
        server.set_response_envelope_test_hook(Arc::new(|kind, envelope| {
            if kind == StorageRpcMessageKind::MetadataCommandApplyAndRecord {
                let last = envelope
                    .last_mut()
                    .expect("authenticated response envelope must not be empty");
                *last ^= 1;
            }
        }));
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let command = test_metadata_command(0, 1);
        let error = MetadataCommandNodeClient::apply_metadata_command_and_record_until(
            &client,
            PgId::new(0),
            &command,
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap_err();

        assert_eq!(error.kind(), MetadataCommandApplyErrorKind::MayHaveApplied);
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .applied_log_index,
            command.id().log_index().get(),
            "server must commit before the authenticated response is corrupted"
        );
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_metadata_apply_response_auth_failure_is_may_have_applied() {
        authenticated_metadata_apply_response_auth_failure_is_may_have_applied(false);
    }

    #[test]
    fn authenticated_tls_tcp_metadata_apply_response_auth_failure_is_may_have_applied() {
        authenticated_metadata_apply_response_auth_failure_is_may_have_applied(true);
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedConditionalSnapshotKind {
        DirectPut,
        StreamPut,
    }

    fn authenticated_conditional_snapshot_read_honors_deadline(
        tcp: bool,
        snapshot_kind: AuthenticatedConditionalSnapshotKind,
    ) {
        const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
        const TRANSPORT_TIMEOUT: Duration = Duration::from_secs(30);
        const RESULT_GRACE: Duration = Duration::from_secs(5);
        assert!(OPERATION_TIMEOUT + RESULT_GRACE < TRANSPORT_TIMEOUT);

        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let transport_limits = storage_rpc_test_transport_limits_with_io_timeout(
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
            TRANSPORT_TIMEOUT,
        );
        let server_auth = StorageRpcServerAuthConfig::new(
            credential.cluster_id(),
            ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap(),
            9,
            STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
        )
        .unwrap()
        .with_transport_limits(transport_limits);
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(server_auth);
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let delayed_kind = match snapshot_kind {
            AuthenticatedConditionalSnapshotKind::DirectPut => {
                StorageRpcMessageKind::DirectPutCommitSnapshotLoad
            }
            AuthenticatedConditionalSnapshotKind::StreamPut => {
                StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad
            }
        };
        let (arrived_tx, arrived_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        server.set_response_frame_test_hook(Arc::new(move |kind, _response| {
            if kind == delayed_kind {
                arrived_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .recv()
                    .unwrap();
            }
        }));
        let server_join = thread::spawn(move || {
            let _ = server.accept_one();
        });
        let (deadline_tx, deadline_rx) = mpsc::sync_channel(1);
        let (client_result_tx, client_result_rx) = mpsc::sync_channel(1);
        let client_join = thread::spawn(move || {
            let client_auth = Arc::new(
                crate::FrontendStorageRpcClientCapability::new_with_transport_limits(
                    credential,
                    9,
                    STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                    transport_limits,
                )
                .unwrap()
                .into(),
            );
            let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                endpoint,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(client_auth),
            );
            let bucket = crate::tests::bucket_name("authenticated-reinspection-deadline-bucket");
            let key = crate::tests::object_key("authenticated-reinspection-deadline-key");
            let session_id = crate::tests::stream_session_id("auth-reinspect");
            let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
            let deadline = Instant::now() + OPERATION_TIMEOUT;
            deadline_tx.send(deadline).unwrap();
            let result = match snapshot_kind {
                AuthenticatedConditionalSnapshotKind::DirectPut => client
                    .open_direct_put_metadata_route(
                        ClusterEpoch::INITIAL,
                        pg_id,
                        &bucket,
                        &key,
                    )
                    .unwrap()
                    .load_direct_put_commit_snapshot_until(
                        &session_id,
                        GenerationId::MIN,
                        deadline,
                    )
                    .map(|_| ()),
                AuthenticatedConditionalSnapshotKind::StreamPut => client
                    .open_stream_put_finalization_metadata_route(
                        ClusterEpoch::INITIAL,
                        pg_id,
                        &bucket,
                        &key,
                        &session_id,
                    )
                    .unwrap()
                    .load_snapshot_until(deadline)
                    .map(|_| ()),
            };
            let _ = client_result_tx.send(result);
        });

        let deadline = deadline_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("authenticated snapshot client did not publish its deadline");
        arrived_rx
            .recv_timeout(OPERATION_TIMEOUT + RESULT_GRACE)
            .expect("authenticated snapshot request did not reach the response gate");
        let client_result_before_release = client_result_rx.recv_timeout(
            deadline.saturating_duration_since(Instant::now()) + RESULT_GRACE,
        );
        release_tx.send(()).unwrap();
        server_join.join().unwrap();
        let client_result = match client_result_before_release {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let late_result = client_result_rx.recv_timeout(Duration::from_secs(2));
                if late_result.is_ok() {
                    client_join.join().unwrap();
                }
                panic!(
                    "authenticated snapshot client had not returned before the delayed response was released: {late_result:?}"
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                client_join.join().unwrap();
                panic!("authenticated snapshot client exited without reporting a result");
            }
        };
        client_join.join().unwrap();
        let error = client_result
            .expect_err("withheld snapshot response must exhaust the absolute deadline");

        assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure,
                    ..
                }) if failure.wire_code() == StorageRpcWireErrorCode::TransportTimeout
            ),
            "delayed authenticated snapshot returned {error:?}"
        );
        assert!(error.is_operation_deadline_exhaustion());
    }

    #[test]
    fn authenticated_unix_conditional_snapshot_reads_honor_deadline() {
        for snapshot_kind in [
            AuthenticatedConditionalSnapshotKind::DirectPut,
            AuthenticatedConditionalSnapshotKind::StreamPut,
        ] {
            authenticated_conditional_snapshot_read_honors_deadline(false, snapshot_kind);
        }
    }

    #[test]
    fn authenticated_tls_tcp_conditional_snapshot_reads_honor_deadline() {
        for snapshot_kind in [
            AuthenticatedConditionalSnapshotKind::DirectPut,
            AuthenticatedConditionalSnapshotKind::StreamPut,
        ] {
            authenticated_conditional_snapshot_read_honors_deadline(true, snapshot_kind);
        }
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedMetadataSessionFailure {
        PreSendDeadline,
        ResponseAuthentication,
        DefinitiveConflict,
        ExactLogGap,
    }

    fn authenticated_metadata_session_classifies_apply_failure(
        tcp: bool,
        failure: AuthenticatedMetadataSessionFailure,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let node = Arc::clone(&server._node);
        let applied = test_metadata_command(0, 1);
        let command = if matches!(failure, AuthenticatedMetadataSessionFailure::DefinitiveConflict) {
            node.get_pg(0)
                .unwrap()
                .apply_metadata_command_and_record(config.node_id.as_u32(), &applied)
                .unwrap();
            MetadataCommandEnvelope::new(
                applied.id(),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        crate::tests::bucket_name("metadata-rpc-bucket"),
                        crate::tests::object_key("conflicting-object"),
                        crate::tests::stream_session_id("meta-conflict"),
                        GenerationId::new(1).unwrap(),
                        123,
                    ),
                ),
            )
        } else if matches!(failure, AuthenticatedMetadataSessionFailure::ExactLogGap) {
            test_metadata_command(0, 2)
        } else {
            applied
        };
        if matches!(
            failure,
            AuthenticatedMetadataSessionFailure::ResponseAuthentication
        ) {
            server.set_response_envelope_test_hook(Arc::new(|kind, envelope| {
                if kind == StorageRpcMessageKind::MetadataCommandApplyAndRecord {
                    let last = envelope
                        .last_mut()
                        .expect("authenticated response envelope must not be empty");
                    *last ^= 1;
                }
            }));
        }
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let section = MetadataCommandNodeClient::open_metadata_command_critical_section_until(
            &client,
            PgId::new(0),
            config.cluster_epoch,
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        let deadline = match failure {
            AuthenticatedMetadataSessionFailure::PreSendDeadline => Instant::now(),
            AuthenticatedMetadataSessionFailure::ResponseAuthentication
            | AuthenticatedMetadataSessionFailure::DefinitiveConflict
            | AuthenticatedMetadataSessionFailure::ExactLogGap => {
                Instant::now() + Duration::from_secs(2)
            }
        };
        let error = section
            .apply_metadata_command_and_record_until(&command, deadline)
            .unwrap_err();

        let expected_kind = match failure {
            AuthenticatedMetadataSessionFailure::PreSendDeadline => {
                MetadataCommandApplyErrorKind::NotSent
            }
            AuthenticatedMetadataSessionFailure::ResponseAuthentication => {
                MetadataCommandApplyErrorKind::MayHaveApplied
            }
            AuthenticatedMetadataSessionFailure::DefinitiveConflict
            | AuthenticatedMetadataSessionFailure::ExactLogGap => {
                MetadataCommandApplyErrorKind::Definitive
            }
        };
        assert_eq!(error.kind(), expected_kind);
        if matches!(failure, AuthenticatedMetadataSessionFailure::DefinitiveConflict) {
            assert!(matches!(
                error.into_source(),
                BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index: 1,
                }) if node_id == config.node_id.as_u32()
            ));
        } else if matches!(failure, AuthenticatedMetadataSessionFailure::ExactLogGap) {
            assert!(matches!(
                error.into_source(),
                BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogGap {
                    node_id,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index: 2,
                    expected_log_index: 1,
                }) if node_id == config.node_id.as_u32()
            ));
        }
        let applied_log_index = node
            .get_pg(0)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .applied_log_index;
        assert_eq!(
            applied_log_index,
            if matches!(
                failure,
                AuthenticatedMetadataSessionFailure::PreSendDeadline
                    | AuthenticatedMetadataSessionFailure::ExactLogGap
            ) {
                0
            } else {
                1
            }
        );
        drop(section);
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[test]
    fn authenticated_unix_metadata_session_pre_send_failure_is_not_sent() {
        authenticated_metadata_session_classifies_apply_failure(
            false,
            AuthenticatedMetadataSessionFailure::PreSendDeadline,
        );
    }

    #[test]
    fn authenticated_tls_tcp_metadata_session_pre_send_failure_is_not_sent() {
        authenticated_metadata_session_classifies_apply_failure(
            true,
            AuthenticatedMetadataSessionFailure::PreSendDeadline,
        );
    }

    #[test]
    fn authenticated_unix_metadata_session_response_auth_failure_may_have_applied() {
        authenticated_metadata_session_classifies_apply_failure(
            false,
            AuthenticatedMetadataSessionFailure::ResponseAuthentication,
        );
    }

    #[test]
    fn authenticated_tls_tcp_metadata_session_response_auth_failure_may_have_applied() {
        authenticated_metadata_session_classifies_apply_failure(
            true,
            AuthenticatedMetadataSessionFailure::ResponseAuthentication,
        );
    }

    #[test]
    fn authenticated_unix_metadata_session_signed_conflict_is_definitive() {
        authenticated_metadata_session_classifies_apply_failure(
            false,
            AuthenticatedMetadataSessionFailure::DefinitiveConflict,
        );
    }

    #[test]
    fn authenticated_tls_tcp_metadata_session_signed_conflict_is_definitive() {
        authenticated_metadata_session_classifies_apply_failure(
            true,
            AuthenticatedMetadataSessionFailure::DefinitiveConflict,
        );
    }

    #[test]
    fn authenticated_unix_metadata_session_preserves_exact_log_gap() {
        authenticated_metadata_session_classifies_apply_failure(
            false,
            AuthenticatedMetadataSessionFailure::ExactLogGap,
        );
    }

    #[test]
    fn authenticated_tls_tcp_metadata_session_preserves_exact_log_gap() {
        authenticated_metadata_session_classifies_apply_failure(
            true,
            AuthenticatedMetadataSessionFailure::ExactLogGap,
        );
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedPendingSlotDomainCase {
        BucketControlRejectsMark,
        GenericRejectsBucketControl,
    }

    fn authenticated_pending_slot_endpoint_domains_are_disjoint(
        tcp: bool,
        case: AuthenticatedPendingSlotDomainCase,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let command = match case {
            AuthenticatedPendingSlotDomainCase::BucketControlRejectsMark => {
                test_mark_bucket_deleting_command(0, 1)
            }
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl => {
                test_bucket_control_metadata_command(0, 1)
            }
        };
        let bucket = command.bucket_name().clone();
        if matches!(
            case,
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl
        ) {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            let now = crate::clock::current_time_millis();
            PgMetadataStore::begin_durable_bucket_write_drain(
                &*pg,
                &bucket,
                "authenticated-generic-domain-drain",
                "authenticated-generic-domain-owner",
                config.cluster_epoch,
                now,
                now + 60_000,
            )
            .unwrap();
        }
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let error = match case {
            AuthenticatedPendingSlotDomainCase::BucketControlRejectsMark => {
                MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
                    &client,
                    PgId::new(0),
                    &command,
                    &bucket,
                )
                .unwrap_err()
            }
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl => {
                MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
                    &client,
                    PgId::new(0),
                    &command,
                    Some(&bucket),
                )
                .unwrap_err()
            }
        };
        assert!(
            matches!(
                error,
                StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                }
            ),
            "unexpected pending-slot endpoint-domain rejection: {error:?}"
        );
        drop(client);
        assert!(join.join().unwrap().is_ok());

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(
            reopened
                .get_pg(0)
                .unwrap()
                .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
                .unwrap()
                .is_none(),
            "authenticated endpoint-domain rejection must not install the command"
        );
        if matches!(
            case,
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl
        ) {
            assert!(
                PgMetadataStore::durable_bucket_write_drain(
                    &*reopened.get_pg(0).unwrap(),
                    &bucket,
                )
                .unwrap()
                .is_some(),
                "generic endpoint rejection must preserve the live delete drain"
            );
        }
    }

    #[test]
    fn authenticated_unix_bucket_control_pending_slot_rejects_mark_bucket_deleting() {
        authenticated_pending_slot_endpoint_domains_are_disjoint(
            false,
            AuthenticatedPendingSlotDomainCase::BucketControlRejectsMark,
        );
    }

    #[test]
    fn authenticated_tls_bucket_control_pending_slot_rejects_mark_bucket_deleting() {
        authenticated_pending_slot_endpoint_domains_are_disjoint(
            true,
            AuthenticatedPendingSlotDomainCase::BucketControlRejectsMark,
        );
    }

    #[test]
    fn authenticated_unix_generic_pending_slot_rejects_bucket_control_during_drain() {
        authenticated_pending_slot_endpoint_domains_are_disjoint(
            false,
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl,
        );
    }

    #[test]
    fn authenticated_tls_generic_pending_slot_rejects_bucket_control_during_drain() {
        authenticated_pending_slot_endpoint_domains_are_disjoint(
            true,
            AuthenticatedPendingSlotDomainCase::GenericRejectsBucketControl,
        );
    }

    fn authenticated_pending_slot_replacement_rejects_omitted_scope_and_cross_domain_commands(
        tcp: bool,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let previous = test_metadata_command(0, 1);
        let bucket = previous.bucket_name().clone();
        {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            pg.try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &previous,
                Some(&bucket),
            )
            .unwrap();
            let now = crate::clock::current_time_millis();
            PgMetadataStore::begin_durable_bucket_write_drain(
                &*pg,
                &bucket,
                "authenticated-replacement-domain-drain",
                "authenticated-replacement-domain-owner",
                config.cluster_epoch,
                now,
                now + 60_000,
            )
            .unwrap();
        }
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let section =
            MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
                &client,
                PgId::new(0),
                config.cluster_epoch,
            )
            .unwrap();
        let replacement = test_metadata_command(0, 2);
        let foreign_scope = crate::tests::bucket_name("foreign-replacement-scope");
        for (scope_bucket, expected_detail) in [
            (
                None,
                "metadata command replacement requires canonical bucket scope",
            ),
            (
                Some(&foreign_scope),
                "metadata command replacement scope bucket does not match command bucket",
            ),
        ] {
            let error = section
                .replace_pending_metadata_command_slot_for_reissue(
                    &previous,
                    &replacement,
                    scope_bucket,
                )
                .unwrap_err();
            assert_authenticated_pending_slot_replacement_scope_error(error, expected_detail);
        }
        for replacement in [
            test_mark_bucket_deleting_command(0, 2),
            test_bucket_control_metadata_command(0, 2),
            test_metadata_command_for_subject(
                0,
                2,
                crate::tests::bucket_name("foreign-replacement-bucket"),
                crate::tests::object_key("object"),
            ),
            test_metadata_command_for_subject(
                0,
                2,
                bucket.clone(),
                crate::tests::object_key("different-object"),
            ),
        ] {
            let error = section
                .replace_pending_metadata_command_slot_for_reissue(
                    &previous,
                    &replacement,
                    Some(&bucket),
                )
                .unwrap_err();
            assert_eq!(error.kind(), MetadataCommandApplyErrorKind::Definitive);
            assert!(
                matches!(
                    error.into_source(),
                    StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        ..
                    }
                ),
                "cross-domain replacement must be rejected by authenticated request validation"
            );
        }
        drop(section);
        drop(client);
        assert!(join.join().unwrap().is_ok());

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        let pending = pg
            .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_bytes, previous.command_bytes());
        assert!(
            PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
                .unwrap()
                .is_some(),
            "authenticated cross-domain rejection must preserve the live delete drain"
        );
    }

    #[test]
    fn authenticated_unix_pending_slot_replacement_rejects_omitted_scope_and_cross_domain_commands()
    {
        authenticated_pending_slot_replacement_rejects_omitted_scope_and_cross_domain_commands(
            false,
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_replacement_rejects_omitted_scope_and_cross_domain_commands()
    {
        authenticated_pending_slot_replacement_rejects_omitted_scope_and_cross_domain_commands(
            true,
        );
    }

    #[test]
    fn authenticated_unix_current_active_recovery_applies_exact_command() {
        authenticated_current_active_recovery_applies_exact_command(false);
    }

    #[test]
    fn authenticated_tls_current_active_recovery_applies_exact_command() {
        authenticated_current_active_recovery_applies_exact_command(true);
    }

    fn authenticated_current_active_recovery_applies_exact_command(tcp: bool) {
        let tmp = test_util::tempdir();
        let mut config = bounded_runtime_refresh_config(test_config(&tmp));
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let replacement = test_metadata_command(0, 2);
        config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                config.node_id,
                PendingMetadataCommandObservation::new(
                    config.cluster_epoch,
                    std::num::NonZeroU64::new(command.id().log_index().get()).unwrap(),
                    command.checksum_crc64(),
                ),
            ),
        ));
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        create_probe_bucket_direct(
            &server._node.get_pg(0).unwrap(),
            command.bucket_name(),
        );
        server
            ._node
            .get_pg(0)
            .unwrap()
            .try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &command,
                Some(command.bucket_name()),
            )
            .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let section =
            MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
                &client,
                PgId::new(0),
                config.cluster_epoch,
            )
            .unwrap();
        assert_eq!(
            section.pending_metadata_command_envelope().unwrap(),
            Some(command.clone())
        );
        assert!(section
            .replace_pending_metadata_command_slot_for_recovery(
                &command,
                None,
                &command,
                &replacement,
                Some(command.bucket_name()),
            )
            .unwrap());
        assert_eq!(
            section.pending_metadata_command_envelope().unwrap(),
            Some(replacement)
        );
        drop(section);

        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    fn assert_authenticated_pending_slot_replacement_scope_error(
        error: MetadataCommandPendingSlotReplaceError,
        expected_detail: &str,
    ) {
        assert_eq!(error.kind(), MetadataCommandApplyErrorKind::Definitive);
        match error.into_source() {
            StoreError::StorageRpc {
                failure,
                detail,
                ..
            } => {
                assert_eq!(failure, StorageRpcErrorCode::PayloadDecode);
                assert_eq!(detail.as_str(), expected_detail);
            }
            error => panic!("unexpected authenticated replacement scope error: {error:?}"),
        }
    }

    fn authenticated_recovery_pending_slot_replacement_rejects_noncanonical_scope(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let source_epoch = config.cluster_epoch;
        let current_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
        let source_route = config.pg_routes[0].clone();
        let previous = test_metadata_command(0, 1);
        let replacement = test_metadata_command(0, 2);
        let bucket = previous.bucket_name().clone();
        {
            let pg = server._node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            pg.try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &previous,
                Some(&bucket),
            )
            .unwrap();
            let now = crate::clock::current_time_millis();
            PgMetadataStore::begin_durable_bucket_write_drain(
                &*pg,
                &bucket,
                "authenticated-recovery-replacement-scope-drain",
                "authenticated-recovery-replacement-scope-owner",
                source_epoch,
                now,
                now + 60_000,
            )
            .unwrap();
        }
        let mut next_config = bounded_runtime_refresh_config(config.clone());
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                config.node_id,
                PendingMetadataCommandObservation::new(
                    source_epoch,
                    std::num::NonZeroU64::new(previous.id().log_index().get()).unwrap(),
                    previous.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            source_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let section =
            MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
                &client,
                PgId::new(0),
                source_epoch,
            )
            .unwrap();
        let foreign_scope = crate::tests::bucket_name("foreign-recovery-replacement-scope");
        for (scope_bucket, expected_detail) in [
            (
                None,
                "metadata command replacement requires canonical bucket scope",
            ),
            (
                Some(&foreign_scope),
                "metadata command replacement scope bucket does not match command bucket",
            ),
        ] {
            let error = section
                .replace_pending_metadata_command_slot_for_recovery(
                    &previous,
                    None,
                    &previous,
                    &replacement,
                    scope_bucket,
                )
                .unwrap_err();
            assert_authenticated_pending_slot_replacement_scope_error(error, expected_detail);
        }
        drop(section);
        drop(client);
        assert!(join.join().unwrap().is_ok());

        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = reopened.get_pg(0).unwrap();
        let pending = pg
            .pending_metadata_command_slot(config.node_id.as_u32(), source_epoch)
            .unwrap()
            .unwrap();
        assert_eq!(pending.command_bytes, previous.command_bytes());
        assert!(
            PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
                .unwrap()
                .is_some(),
            "authenticated recovery scope rejection must preserve the live delete drain"
        );
    }

    #[test]
    fn authenticated_unix_recovery_pending_slot_replacement_rejects_noncanonical_scope() {
        authenticated_recovery_pending_slot_replacement_rejects_noncanonical_scope(false);
    }

    #[test]
    fn authenticated_tls_recovery_pending_slot_replacement_rejects_noncanonical_scope() {
        authenticated_recovery_pending_slot_replacement_rejects_noncanonical_scope(true);
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedPendingSlotReplaceFailure {
        SignedPreMutationRejection,
        SignedPostCommitFailure,
        ResponseAuthentication,
    }

    fn authenticated_pending_slot_replace_preserves_failure_certainty(
        tcp: bool,
        failure: AuthenticatedPendingSlotReplaceFailure,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let previous = test_metadata_command(0, 1);
        let replacement = test_metadata_command(0, 2);
        let bucket = previous.bucket_name().clone();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &previous,
                Some(&bucket),
            )
            .unwrap();
        if matches!(
            failure,
            AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication
        ) {
            server.set_response_envelope_test_hook(Arc::new(|kind, envelope| {
                if kind == StorageRpcMessageKind::MetadataCommandPendingSlotReplace {
                    *envelope
                        .last_mut()
                        .expect("authenticated response envelope must not be empty") ^= 1;
                }
            }));
        }
        if matches!(
            failure,
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure
        ) {
            server
                ._node
                .get_pg(0)
                .unwrap()
                .fail_next_pending_slot_replace_after_commit();
        }
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let section =
            MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
                &client,
                PgId::new(0),
                config.cluster_epoch,
            )
            .unwrap();
        let scope_bucket = match failure {
            AuthenticatedPendingSlotReplaceFailure::SignedPreMutationRejection => {
                crate::tests::bucket_name("foreign-replacement-scope")
            }
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure
            | AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication => bucket.clone(),
        };
        let error = section
            .replace_pending_metadata_command_slot_for_reissue(
                &previous,
                &replacement,
                Some(&scope_bucket),
            )
            .unwrap_err();
        let expected_kind = match failure {
            AuthenticatedPendingSlotReplaceFailure::SignedPreMutationRejection => {
                MetadataCommandApplyErrorKind::Definitive
            }
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure
            | AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication => {
                MetadataCommandApplyErrorKind::MayHaveApplied
            }
        };
        assert_eq!(error.kind(), expected_kind);
        drop(section);
        drop(client);
        assert!(join.join().unwrap().is_ok());

        let pending = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap()
        .get_pg(0)
        .unwrap()
        .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
        .unwrap()
        .unwrap();
        let expected = match failure {
            AuthenticatedPendingSlotReplaceFailure::SignedPreMutationRejection => &previous,
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure
            | AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication => &replacement,
        };
        assert_eq!(pending.command_bytes, expected.command_bytes());
    }

    #[test]
    fn authenticated_unix_pending_slot_replace_signed_rejection_is_definitive() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            false,
            AuthenticatedPendingSlotReplaceFailure::SignedPreMutationRejection,
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_replace_signed_rejection_is_definitive() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            true,
            AuthenticatedPendingSlotReplaceFailure::SignedPreMutationRejection,
        );
    }

    #[test]
    fn authenticated_unix_pending_slot_replace_signed_post_commit_error_is_ambiguous() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            false,
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure,
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_replace_signed_post_commit_error_is_ambiguous() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            true,
            AuthenticatedPendingSlotReplaceFailure::SignedPostCommitFailure,
        );
    }

    #[test]
    fn authenticated_unix_pending_slot_replace_response_loss_is_ambiguous() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            false,
            AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication,
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_replace_response_loss_is_ambiguous() {
        authenticated_pending_slot_replace_preserves_failure_certainty(
            true,
            AuthenticatedPendingSlotReplaceFailure::ResponseAuthentication,
        );
    }

    fn authenticated_pending_slot_remove_preserves_terminal_state(
        tcp: bool,
        divergent_terminal_row: bool,
        subject_mismatch: Option<AuthenticatedPendingSlotRemoveSubjectMismatch>,
    ) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential));
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let command = test_metadata_command(0, 1);
        let bucket = command.bucket_name().clone();
        server
            ._node
            .get_pg(0)
            .unwrap()
            .try_insert_pending_metadata_command_slot(
                config.node_id.as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();
        if divergent_terminal_row {
            let divergent = MetadataCommandEnvelope::new(
                command.id(),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        crate::tests::bucket_name("metadata-rpc-bucket"),
                        crate::tests::object_key("object"),
                        crate::tests::stream_session_id("rpc-divergent"),
                        GenerationId::new(2).unwrap(),
                        456,
                    ),
                ),
            );
            server
                ._node
                .get_pg(0)
                .unwrap()
                .record_metadata_command_abandoned(config.node_id.as_u32(), &divergent)
                .unwrap();
        }
        if let Some(subject_mismatch) = subject_mismatch {
            server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                if kind != StorageRpcMessageKind::MetadataCommandPendingSlotRemove {
                    return;
                }
                let payload = decode_storage_rpc_response_payload(&frame.payload)
                    .expect("decode pending-slot response envelope")
                    .expect("pending-slot response must be successful");
                let mut response = decode_metadata_command_pending_slot_cleanup_response(&payload)
                    .expect("decode pending-slot cleanup response");
                let (node_id, log_index) = match &mut response.outcome {
                    StorageRpcMetadataCommandPendingSlotCleanupOutcome::TerminalEntryPending {
                        node_id,
                        log_index,
                        ..
                    }
                    | StorageRpcMetadataCommandPendingSlotCleanupOutcome::LogConflict {
                        node_id,
                        log_index,
                        ..
                    } => (node_id, log_index),
                    StorageRpcMetadataCommandPendingSlotCleanupOutcome::Value(_) => {
                        panic!("expected terminal cleanup error response")
                    }
                };
                match subject_mismatch {
                    AuthenticatedPendingSlotRemoveSubjectMismatch::Node => *node_id += 1,
                    AuthenticatedPendingSlotRemoveSubjectMismatch::LogIndex => *log_index += 1,
                }
                let payload = encode_metadata_command_pending_slot_cleanup_response(&response);
                frame.payload = encode_storage_rpc_success_response(&payload);
            }));
        }
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let error = MetadataCommandNodeClient::remove_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap_err();
        if subject_mismatch.is_some() {
            assert!(
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        ..
                    }
                ),
                "unexpected mismatched pending-slot response error: {error:?}"
            );
        } else if divergent_terminal_row {
            assert!(
                matches!(
                    error,
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: 0,
                        cluster_epoch: ClusterEpoch::INITIAL,
                        log_index: 1,
                    } if node_id == config.node_id.as_u32()
                ),
                "unexpected divergent pending-slot removal error: {error:?}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    StoreError::MetadataCommandTerminalEntryPending {
                        node_id,
                        pg_id: 0,
                        cluster_epoch: ClusterEpoch::INITIAL,
                        log_index: 1,
                    } if node_id == config.node_id.as_u32()
                ),
                "unexpected nonterminal pending-slot removal error: {error:?}"
            );
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedPendingSlotRemoveSubjectMismatch {
        Node,
        LogIndex,
    }

    #[test]
    fn authenticated_unix_pending_slot_remove_preserves_nonterminal_state() {
        authenticated_pending_slot_remove_preserves_terminal_state(false, false, None);
    }

    #[test]
    fn authenticated_tls_pending_slot_remove_preserves_nonterminal_state() {
        authenticated_pending_slot_remove_preserves_terminal_state(true, false, None);
    }

    fn authenticated_proof_release_honors_operation_deadline_while_pg_locked(tcp: bool) {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server_transport_limits = storage_rpc_test_transport_limits_with_io_timeout(
            crate::StorageRpcTransportLimits::DEFAULT.max_connections(),
            Duration::from_millis(1_500),
        );
        let prepared = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(
                storage_rpc_server_auth(&credential)
                    .with_transport_limits(server_transport_limits),
            );
        let server = if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap();
        let endpoint = if tcp {
            let address = server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(config.socket_path.clone())
        };
        let bucket = crate::tests::bucket_name("proof-release-deadline-bucket");
        let reservation = {
            let pg = server._node.get_pg(0).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &crate::CanonicalUserId::from_principal("owner"),
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            let created_at = crate::clock::current_time_millis();
            PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: "proof-release-deadline-reservation",
                    owner_token: "proof-release-deadline-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: "put-object",
                    created_at,
                    lease_deadline: created_at + 60_000,
                    target_context: Some("key=object"),
                },
            )
            .unwrap()
        };
        let proof = BucketWriteReservationProof::from(&reservation);
        let storage_node = Arc::clone(&server._node);
        let held_pg = storage_node.get_pg(0).unwrap();
        let (server_done_tx, server_done_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            server_done_tx.send(server.accept_one()).unwrap();
        });
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );
        let route_deadline = Instant::now() + Duration::from_secs(10);
        let route = RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route_until(
            &client,
            BucketPgId::new_for_test(PgId::new(0)),
            &bucket,
            route_deadline,
        )
        .unwrap();
        // Cross-process projection reserves one second for clock skew. The
        // server therefore waits for about two seconds, past the initial
        // 1.5-second connection I/O deadline, before writing the response.
        let deadline = Instant::now() + Duration::from_secs(3);
        let error = route
            .release_metadata_command_bucket_write_reservation_until(&proof, deadline)
            .expect_err("proof release must expire while the bucket PG is held");
        assert!(
            matches!(
                error,
                BucketSnapshotLoadError::Store(StoreError::OperationDeadlineExceeded { .. })
                    | BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::TransportTimeout,
                        ..
                    })
            ),
            "unexpected proof-release deadline error: {error:?}"
        );
        server_done_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("storage worker must stop waiting for the held PG at the operation deadline")
            .unwrap();

        drop(route);
        drop(client);
        drop(held_pg);
        server_thread.join().unwrap();
        let pg = storage_node.get_pg(0).unwrap();
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                &proof.reservation_id,
            )
            .unwrap(),
            Some(reservation),
            "deadline expiry must retain the exact bucket-write reservation"
        );
    }

    #[test]
    fn authenticated_unix_proof_release_honors_operation_deadline_while_pg_locked() {
        authenticated_proof_release_honors_operation_deadline_while_pg_locked(false);
    }

    #[test]
    fn authenticated_tls_proof_release_honors_operation_deadline_while_pg_locked() {
        authenticated_proof_release_honors_operation_deadline_while_pg_locked(true);
    }

    #[test]
    fn authenticated_unix_pending_slot_remove_preserves_log_divergence() {
        authenticated_pending_slot_remove_preserves_terminal_state(false, true, None);
    }

    #[test]
    fn authenticated_tls_pending_slot_remove_preserves_log_divergence() {
        authenticated_pending_slot_remove_preserves_terminal_state(true, true, None);
    }

    #[test]
    fn authenticated_unix_pending_slot_remove_rejects_wrong_response_node() {
        authenticated_pending_slot_remove_preserves_terminal_state(
            false,
            false,
            Some(AuthenticatedPendingSlotRemoveSubjectMismatch::Node),
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_remove_rejects_wrong_response_log_index() {
        authenticated_pending_slot_remove_preserves_terminal_state(
            true,
            false,
            Some(AuthenticatedPendingSlotRemoveSubjectMismatch::LogIndex),
        );
    }

    #[test]
    fn authenticated_unix_pending_slot_remove_rejects_wrong_log_conflict_index() {
        authenticated_pending_slot_remove_preserves_terminal_state(
            false,
            true,
            Some(AuthenticatedPendingSlotRemoveSubjectMismatch::LogIndex),
        );
    }

    #[test]
    fn authenticated_tls_pending_slot_remove_rejects_wrong_log_conflict_node() {
        authenticated_pending_slot_remove_preserves_terminal_state(
            true,
            true,
            Some(AuthenticatedPendingSlotRemoveSubjectMismatch::Node),
        );
    }

    struct AuthenticatedFanoutServerSet {
        stop: Arc<AtomicBool>,
        servers: Vec<Arc<StorageNodeServer>>,
        joins: Vec<thread::JoinHandle<()>>,
    }

    #[derive(Default)]
    struct DeterministicTestGate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl DeterministicTestGate {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn block_until_released(&self) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn wait_until_arrived(&self, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !state.0 {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .expect("operation did not reach the deterministic test gate");
                let (next, timeout) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state = next;
                assert!(
                    !timeout.timed_out() || state.0,
                    "operation did not reach the deterministic test gate"
                );
            }
        }

        fn release(&self) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.1 = true;
            self.changed.notify_all();
        }

        fn release_on_drop(self: &Arc<Self>) -> DeterministicTestGateRelease {
            DeterministicTestGateRelease(Arc::clone(self))
        }
    }

    struct DeterministicTestGateRelease(Arc<DeterministicTestGate>);

    impl Drop for DeterministicTestGateRelease {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl Drop for AuthenticatedFanoutServerSet {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            for server in &self.servers {
                let _ = UnixStream::connect(server.socket_path_for_test());
            }
            for join in self.joins.drain(..) {
                if let Err(panic) = join.join() {
                    if thread::panicking() {
                        return;
                    }
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }

    fn authenticated_fanout_cluster(
        tcp: bool,
        namespace: &str,
    ) -> (
        test_util::TempDir,
        AuthenticatedFanoutServerSet,
        Arc<StorageCluster>,
    ) {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let primary_node_id = NodeId::new(1);
        let ec_shape = EcShape { k: 2, m: 1 };
        let route_map_validity = RouteMapValidity::until_ms_saturating(
            crate::clock::current_time_millis().saturating_add(60_000),
        );
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: format!("{namespace}-frontend"),
        });
        let mut servers = Vec::new();
        let mut client_configs = Vec::new();

        for node_id in node_ids {
            let socket_path = tmp
                .path()
                // Keep the AF_UNIX path independent of the descriptive test
                // namespace so it fits Darwin's 104-byte `sun_path`.
                .join("s")
                .join(format!("node-{}.sock", node_id.as_u32()));
            private_socket_dir(socket_path.parent().unwrap());
            let config = StorageNodeProcessConfig {
                node_id,
                cluster_epoch: ClusterEpoch::INITIAL,
                route_map_validity,
                data_dir: tmp
                    .path()
                    .join(format!("{namespace}-node-{}", node_id.as_u32())),
                default_ec_shape: ec_shape,
                pg_ids: vec![0],
                socket_path: socket_path.clone(),
                pg_routes: vec![StorageNodePgRoute {
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: PgState::Active,
                    primary_node_id,
                    metadata_transfer_destination_epoch: None,
                    metadata_read_route: None,
                    acting_set: node_ids.to_vec(),
                }],
                historical_pg_routes: Vec::new(),
                pending_metadata_command_recoveries: Vec::new(),
            };
            let mut prepared = PreparedStorageNodeServer::new(config)
                .with_rpc_auth(storage_rpc_server_auth(&credential));
            if tcp {
                prepared = prepared.with_rpc_listeners(vec![
                    StorageNodeRpcListenerConfig::unix(socket_path.clone()),
                    StorageNodeRpcListenerConfig::tls_tcp_with_config(
                        "127.0.0.1:0".parse().unwrap(),
                        storage_rpc_tls_server_config(),
                    ),
                ]);
            }
            let server = Arc::new(prepared.bind().unwrap());
            let endpoint = if tcp {
                let address = server.tcp_listener_addr_for_test();
                StorageRpcClientEndpoint::tcp_with_config(
                    format!("tcp://localhost:{}", address.port()),
                    vec![address],
                    "localhost",
                    storage_rpc_tls_client_config(),
                )
                .unwrap()
            } else {
                StorageRpcClientEndpoint::unix(socket_path)
            };
            client_configs.push(
                LocalUnixStorageNodeClientConfig::with_rpc_endpoint_and_admission_settings(
                    node_id,
                    endpoint,
                    LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                )
                .with_frontend_rpc_auth(
                    crate::FrontendStorageRpcClientCapability::new(
                        credential.clone(),
                        9,
                        STORAGE_RPC_AUTH_TEST_TOPOLOGY_DIGEST,
                    )
                    .unwrap(),
                ),
            );
            servers.push(server);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let joins = servers
            .iter()
            .map(|server| {
                let server = Arc::clone(server);
                let stop = Arc::clone(&stop);
                thread::spawn(move || server.serve_until_stop_for_test(&stop).unwrap())
            })
            .collect();
        let server_set = AuthenticatedFanoutServerSet {
            stop,
            servers,
            joins,
        };

        let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
            primary_node_id,
            node_ids,
            &[0],
            ec_shape,
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        map.install_unix_storage_node_clients(client_configs)
            .unwrap();
        let cluster = StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
        (tmp, server_set, cluster)
    }

    #[derive(Clone, Copy)]
    enum AuthenticatedFanoutFailurePoint {
        Witness,
        Primary,
        TrailingRetryable,
    }

    fn authenticated_metadata_fanout_handles_apply_failure(
        tcp: bool,
        failure_point: AuthenticatedFanoutFailurePoint,
    ) {
        let primary_node_id = NodeId::new(1);
        let witness_node_id = NodeId::new(0);
        let corrupt_node_id = match failure_point {
            AuthenticatedFanoutFailurePoint::Witness => Some(witness_node_id),
            AuthenticatedFanoutFailurePoint::Primary => Some(primary_node_id),
            AuthenticatedFanoutFailurePoint::TrailingRetryable => None,
        };
        let corrupted = Arc::new(AtomicBool::new(false));
        let signed_retryable_response_observed = Arc::new(AtomicBool::new(false));
        let (_tmp, server_set, cluster) = authenticated_fanout_cluster(tcp, "fanout");

        for server in &server_set.servers {
            let node_id = server.config_snapshot().node_id;
            if Some(node_id) == corrupt_node_id {
                let corrupted_hook = Arc::clone(&corrupted);
                server.set_response_envelope_test_hook(Arc::new(move |kind, envelope| {
                    if kind == StorageRpcMessageKind::MetadataCommandApplyAndRecord
                        && !corrupted_hook.swap(true, Ordering::AcqRel)
                    {
                        *envelope
                            .last_mut()
                            .expect("authenticated response envelope must not be empty") ^= 1;
                    }
                }));
            } else if node_id == NodeId::new(2)
                && matches!(
                    failure_point,
                    AuthenticatedFanoutFailurePoint::TrailingRetryable
                )
            {
                let response_observed = Arc::clone(&signed_retryable_response_observed);
                server.set_response_envelope_test_hook(Arc::new(move |kind, _envelope| {
                    if kind == StorageRpcMessageKind::MetadataCommandApplyAndRecord {
                        response_observed.store(true, Ordering::Release);
                    }
                }));
            }
        }
        let retryable_error_injected = Arc::new(AtomicBool::new(false));
        let apply_hook = if matches!(
            failure_point,
            AuthenticatedFanoutFailurePoint::TrailingRetryable
        ) {
            let trailing_server = Arc::clone(
                server_set
                    .servers
                    .iter()
                    .find(|server| server.config_snapshot().node_id == NodeId::new(2))
                    .unwrap(),
            );
            let retryable_error_injected = Arc::clone(&retryable_error_injected);
            Some(cluster.test_install_before_metadata_command_apply_hook(Arc::new(
                move |node_id, _command| {
                    if node_id == NodeId::new(2)
                        && !retryable_error_injected.swap(true, Ordering::AcqRel)
                    {
                        let mut next = trailing_server.config_snapshot();
                        let previous_routes = next.pg_routes.clone();
                        let next_epoch = ClusterEpoch::new(2).unwrap();
                        next.cluster_epoch = next_epoch;
                        for route in &mut next.pg_routes {
                            route.cluster_epoch = next_epoch;
                        }
                        next.historical_pg_routes = previous_routes;
                        trailing_server
                            .install_control_plane_runtime_config(next)
                            .unwrap();
                    }
                    Ok(())
                },
            )))
        } else {
            None
        };
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let attempt_hook = if matches!(failure_point, AuthenticatedFanoutFailurePoint::Witness) {
            let attempt_count = Arc::clone(&attempt_count);
            Some(cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(
                move |_command| {
                    if attempt_count.fetch_add(1, Ordering::AcqRel) == 1 {
                        return Err(StoreError::StorageRpc {
                            node_id: primary_node_id.as_u32(),
                            operation: "test post-witness pre-dispatch failure",
                            failure: StorageRpcErrorCode::PayloadDecode,
                            detail: crate::StorageNodeFailureDetail::new(
                                "synthetic definitive pre-dispatch protocol failure",
                            ),
                        });
                    }
                    Ok(())
                },
            )))
        } else {
            None
        };
        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = AclGrants::default();
        let bucket = crate::tests::bucket_name("authenticated-fanout-bucket");
        let create_config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
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
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 1_234, 1).unwrap(),
            ),
        );

        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(primary_node_id, &command)
            .unwrap();
        drop(apply_hook);
        drop(attempt_hook);

        if matches!(
            failure_point,
            AuthenticatedFanoutFailurePoint::TrailingRetryable
        ) {
            assert!(retryable_error_injected.load(Ordering::Acquire));
            assert!(signed_retryable_response_observed.load(Ordering::Acquire));
        } else {
            assert!(corrupted.load(Ordering::Acquire));
        }
        if matches!(failure_point, AuthenticatedFanoutFailurePoint::Witness) {
            assert!(
                attempt_count.load(Ordering::Acquire) >= 3,
                "witness ambiguity must survive the later definitive pre-dispatch failure"
            );
        }
        for server in &server_set.servers {
            let applied_log_index = server
                ._node
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .applied_log_index;
            let expected = if matches!(
                failure_point,
                AuthenticatedFanoutFailurePoint::Primary
                    | AuthenticatedFanoutFailurePoint::TrailingRetryable
            )
                && server.config_snapshot().node_id == NodeId::new(2)
            {
                0
            } else {
                1
            };
            assert_eq!(
                applied_log_index,
                expected,
                "unexpected applied index on node {}",
                server.config_snapshot().node_id.as_u32()
            );
        }
    }

    fn authenticated_fanout_publication_start_fences_delayed_witness(tcp: bool) {
        let primary_node_id = NodeId::new(1);
        let witness_node_id = NodeId::new(0);
        let (_tmp, server_set, cluster) =
            authenticated_fanout_cluster(tcp, "publication-start-fence");
        let witness = server_set
            .servers
            .iter()
            .find(|server| server.config_snapshot().node_id == witness_node_id)
            .cloned()
            .unwrap();
        let primary = server_set
            .servers
            .iter()
            .find(|server| server.config_snapshot().node_id == primary_node_id)
            .cloned()
            .unwrap();

        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = AclGrants::default();
        let bucket = crate::tests::bucket_name("publication-start-fence-bucket");
        let create_config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
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
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(&create_config, 1_234, 1).unwrap(),
            ),
        );

        let gate = DeterministicTestGate::new();
        let _release = gate.release_on_drop();
        let gate_for_hook = Arc::clone(&gate);
        let command_for_hook = command.clone();
        witness.set_metadata_command_before_commit_test_hook(Arc::new(
            move |_node_id, received| {
                if received == &command_for_hook {
                    gate_for_hook.block_until_released();
                }
            },
        ));
        primary
            ._node
            .get_pg(0)
            .unwrap()
            .try_insert_pending_metadata_command_slot(
                primary_node_id.as_u32(),
                &command,
                Some(&bucket),
            )
            .unwrap();

        let (result_tx, result_rx) = mpsc::channel();
        let owner_cluster = Arc::clone(&cluster);
        let owner_command = command.clone();
        let owner_join = thread::spawn(move || {
            let result = owner_cluster
                .test_apply_pending_metadata_command_to_acting_set_from_origin(
                    primary_node_id,
                    &owner_command,
                );
            let _ = result_tx.send(result);
        });

        gate.wait_until_arrived(Duration::from_secs(5));
        let owner_result = result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("owner must stop at its publication confirmation deadline");
        assert!(matches!(
            owner_result,
            Err(BucketSnapshotLoadError::Store(
                StoreError::MetadataCommandIrrevocableConvergencePending { .. }
                    | StoreError::MetadataCommandOutcomeUnconfirmed { .. }
            ))
        ));
        owner_join.join().unwrap();

        let primary_pg = primary._node.get_pg(0).unwrap();
        assert!(
            primary_pg
                .pending_metadata_command_publication_started(
                    primary_node_id.as_u32(),
                    &command,
                )
                .unwrap(),
            "publication-start marker must precede witness dispatch"
        );
        drop(primary_pg);
        let abandonment = cluster
            .test_record_abandoned_metadata_command_to_acting_set_until(
                &command,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err();
        assert!(matches!(
            abandonment,
            BucketSnapshotLoadError::Store(
                StoreError::MetadataCommandIrrevocableConvergencePending { .. }
            )
        ));

        gate.release();
        let witness_commit_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if witness
                ._node
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .applied_log_index
                == 1
            {
                break;
            }
            assert!(
                Instant::now() < witness_commit_deadline,
                "released witness request did not commit"
            );
            thread::yield_now();
        }
        assert_eq!(
            witness
                ._node
                .get_pg(0)
                .unwrap()
                .metadata_command_acceptance(witness_node_id.as_u32(), &command)
                .unwrap(),
            MetadataCommandAcceptance::AlreadyApplied,
            "the pre-loss witness request must retain the exact command"
        );
        let primary_pg = primary._node.get_pg(0).unwrap();
        assert_eq!(
            primary_pg
                .metadata_command_acceptance(primary_node_id.as_u32(), &command)
                .unwrap(),
            MetadataCommandAcceptance::Apply,
            "recovery must not abandon the publication-started command"
        );
        assert!(primary_pg
            .pending_metadata_command_publication_started(primary_node_id.as_u32(), &command)
            .unwrap());
    }

    #[test]
    fn authenticated_unix_publication_start_fences_delayed_witness_after_owner_loss() {
        authenticated_fanout_publication_start_fences_delayed_witness(false);
    }

    #[test]
    fn authenticated_tls_publication_start_fences_delayed_witness_after_owner_loss() {
        authenticated_fanout_publication_start_fences_delayed_witness(true);
    }

    #[test]
    fn authenticated_unix_fanout_confirms_witness_after_response_auth_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            false,
            AuthenticatedFanoutFailurePoint::Witness,
        );
    }

    #[test]
    fn authenticated_tls_fanout_confirms_witness_after_response_auth_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            true,
            AuthenticatedFanoutFailurePoint::Witness,
        );
    }

    #[test]
    fn authenticated_unix_fanout_confirms_primary_after_response_auth_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            false,
            AuthenticatedFanoutFailurePoint::Primary,
        );
    }

    #[test]
    fn authenticated_tls_fanout_confirms_primary_after_response_auth_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            true,
            AuthenticatedFanoutFailurePoint::Primary,
        );
    }

    #[test]
    fn authenticated_unix_fanout_hands_off_signed_retryable_trailing_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            false,
            AuthenticatedFanoutFailurePoint::TrailingRetryable,
        );
    }

    #[test]
    fn authenticated_tls_fanout_hands_off_signed_retryable_trailing_failure() {
        authenticated_metadata_fanout_handles_apply_failure(
            true,
            AuthenticatedFanoutFailurePoint::TrailingRetryable,
        );
    }

    fn authenticated_object_version_allocator_retries_remote_contention(tcp: bool) {
        let (_tmp, server_set, cluster) =
            authenticated_fanout_cluster(tcp, "allocator-contention");
        let witness = server_set
            .servers
            .iter()
            .find(|server| server.config_snapshot().node_id == NodeId::new(0))
            .cloned()
            .unwrap();
        let _stderr_guard = witness.suppress_metadata_command_lock_wait_stderr();
        let held_witness_lock = Arc::new(Mutex::new(None));
        let contention_observed = Arc::new(AtomicBool::new(false));
        let held_witness_lock_for_response = Arc::clone(&held_witness_lock);
        let contention_observed_for_hook = Arc::clone(&contention_observed);
        witness.set_response_envelope_test_hook(Arc::new(move |kind, _envelope| {
            if kind == StorageRpcMessageKind::MetadataCommandApplyAndRecord
                && !contention_observed_for_hook.swap(true, Ordering::AcqRel)
            {
                drop(
                    held_witness_lock_for_response
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                        .expect("witness apply response must release the injected lock"),
                );
            }
        }));
        let apply_armed = Arc::new(AtomicBool::new(true));
        let apply_armed_for_hook = Arc::clone(&apply_armed);
        let held_witness_lock_for_apply = Arc::clone(&held_witness_lock);
        let witness_for_apply = Arc::clone(&witness);
        let _apply_hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                if node_id == NodeId::new(0)
                    && matches!(
                        command.payload(),
                        MetadataCommandPayload::ReserveObjectVersion(_)
                    )
                    && apply_armed_for_hook.swap(false, Ordering::AcqRel)
                {
                    let guard = witness_for_apply
                        .metadata_command_locks
                        .acquire(NodeId::new(0), PgId::new(0), None)
                        .unwrap();
                    let previous = held_witness_lock_for_apply
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .replace(guard);
                    assert!(previous.is_none());
                }
                Ok(())
            },
        ));

        let bucket = crate::tests::bucket_name("authenticated-allocator-bucket");
        let key = crate::tests::object_key("key");
        let reserved = cluster
            .test_reserve_next_object_version(PgId::new(0), &bucket, &key)
            .expect("remote contention must be followed by allocator reinspection");

        assert!(!apply_armed.load(Ordering::Acquire));
        assert!(contention_observed.load(Ordering::Acquire));
        assert_eq!(
            reserved,
            VersionId::from_u64(1),
            "publication-started allocator command must converge as the request outcome"
        );
        for server in &server_set.servers {
            let state = server
                ._node
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap();
            assert_eq!(
                state.applied_log_index,
                1,
                "allocator reinspection must converge the publication-started command on node {}",
                server.config_snapshot().node_id.as_u32()
            );
            let config = server.config_snapshot();
            assert!(
                server
                    ._node
                    .get_pg(0)
                    .unwrap()
                    .pending_metadata_command_slot(
                        config.node_id.as_u32(),
                        config.cluster_epoch,
                    )
                    .unwrap()
                    .is_none(),
                "allocator reinspection must leave no pending command on node {}",
                config.node_id.as_u32()
            );
        }
    }

    #[test]
    fn authenticated_unix_object_version_allocator_retries_remote_contention() {
        authenticated_object_version_allocator_retries_remote_contention(false);
    }

    #[test]
    fn authenticated_tls_object_version_allocator_retries_remote_contention() {
        authenticated_object_version_allocator_retries_remote_contention(true);
    }

    fn authenticated_object_command_contention_is_abandoned_before_return(tcp: bool) {
        let namespace = if tcp {
            "tls-object-contention-abandon"
        } else {
            "unix-object-contention-abandon"
        };
        let (_tmp, server_set, cluster) = authenticated_fanout_cluster(tcp, namespace);
        let bucket = crate::tests::bucket_name("authenticated-object-contention-bucket");
        let key = crate::tests::object_key("key");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = AclGrants::default();
        cluster
            .create_bucket_with_config_and_load_info(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Suspended,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            })
            .expect("authenticated bucket creation must converge before contention injection");

        let witness = server_set
            .servers
            .iter()
            .find(|server| server.config_snapshot().node_id == NodeId::new(0))
            .cloned()
            .unwrap();
        let _stderr_guard = witness.suppress_metadata_command_lock_wait_stderr();
        let held_witness_lock = Arc::new(Mutex::new(Some(
            witness
                .metadata_command_locks
                .acquire(NodeId::new(0), PgId::new(0), None)
                .unwrap(),
        )));
        let retry_hook_ran = Arc::new(AtomicBool::new(false));
        let retry_hook_ran_for_hook = Arc::clone(&retry_hook_ran);
        let held_witness_lock_for_hook = Arc::clone(&held_witness_lock);
        let _retry_hook = cluster.test_install_object_metadata_command_definitive_retry_hook(
            Arc::new(move |command, source| {
                if !matches!(
                    command.payload(),
                    MetadataCommandPayload::InsertDeleteMarker(_)
                ) {
                    return false;
                }
                assert!(matches!(
                    source,
                    BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                        failure,
                        ..
                    }) if failure.wire_code()
                        == crate::storage_rpc::StorageRpcWireErrorCode::MetadataCommandContention
                ), "retry must follow authenticated RPC contention, got {source:?}");
                retry_hook_ran_for_hook.store(true, Ordering::Release);
                drop(
                    held_witness_lock_for_hook
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                        .expect("definitive retry must release the injected witness lock"),
                );
                true
            }),
        );

        let error = cluster
            .insert_current_delete_marker_if(
                &bucket,
                &key,
                BucketVersioningState::Suspended,
                owner,
                |_| Ok::<(), ()>(()),
            )
            .expect_err("definitive authenticated contention must exhaust the request budget");

        assert!(
            error.is_metadata_command_contention(),
            "unexpected definitive contention result: {}",
            match &error {
                crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                    operation,
                    failure,
                    ..
                }) => format!("{operation}: {:?}", failure.wire_code()),
                crate::ObjectPgActionError::Store(source) => format!("{source:?}"),
                _ => format!("{error:?}"),
            }
        );
        assert!(retry_hook_ran.load(Ordering::Acquire));
        assert!(
            held_witness_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none(),
            "the injected lock must be released before abandonment"
        );
        for server in &server_set.servers {
            let config = server.config_snapshot();
            let pg = server._node.get_pg(0).unwrap();
            assert!(
                pg.pending_metadata_command_slot(
                    config.node_id.as_u32(),
                    config.cluster_epoch,
                )
                .unwrap()
                .is_none(),
                "definitively unapplied command remained recoverable on node {}",
                config.node_id.as_u32()
            );
            assert!(matches!(
                PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }
    }

    #[test]
    fn authenticated_unix_object_command_contention_is_abandoned_before_return() {
        authenticated_object_command_contention_is_abandoned_before_return(false);
    }

    #[test]
    fn authenticated_tls_object_command_contention_is_abandoned_before_return() {
        authenticated_object_command_contention_is_abandoned_before_return(true);
    }

    #[derive(Clone, Copy)]
    enum DirectPutPendingInstallResponseLossFollowup {
        ExpireBudget,
        IntegrityFailure,
        UnrelatedContender,
    }

    fn authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
        tcp: bool,
        followup: DirectPutPendingInstallResponseLossFollowup,
    ) {
        let namespace = match (tcp, followup) {
            (true, DirectPutPendingInstallResponseLossFollowup::ExpireBudget) => {
                "tls-direct-put-install-loss"
            }
            (false, DirectPutPendingInstallResponseLossFollowup::ExpireBudget) => {
                "unix-direct-put-install-loss"
            }
            (true, DirectPutPendingInstallResponseLossFollowup::IntegrityFailure) => {
                "tls-direct-put-install-loss-integrity-failure"
            }
            (false, DirectPutPendingInstallResponseLossFollowup::IntegrityFailure) => {
                "unix-direct-put-install-loss-integrity-failure"
            }
            (true, DirectPutPendingInstallResponseLossFollowup::UnrelatedContender) => {
                "tls-direct-put-install-loss-unrelated-contender"
            }
            (false, DirectPutPendingInstallResponseLossFollowup::UnrelatedContender) => {
                "unix-direct-put-install-loss-unrelated-contender"
            }
        };
        let (_tmp, server_set, cluster) = authenticated_fanout_cluster(tcp, namespace);
        let bucket = crate::tests::bucket_name("authenticated-direct-put-install-loss-bucket");
        let key = crate::tests::object_key("key");
        let owner = crate::OwnerIdentity::from_principal("owner");
        cluster
            .create_bucket_with_config_and_load_info(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &AclGrants::default(),
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Suspended,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            })
            .unwrap();

        let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
        let admission = handle.admit_current_route().unwrap();
        let route = admission.active_put_object_route(&bucket, &key).unwrap();
        let generation_reservation_id = crate::tests::stream_session_id("install-loss");
        let generation_id = route
            .reserve_generation(&generation_reservation_id)
            .unwrap();
        let data = b"authenticated direct PUT pending install response loss";
        let payload = route
            .write_direct_object_payload(
                &generation_reservation_id,
                generation_id,
                data.len() as u64,
                data,
            )
            .unwrap();
        let data_pg_id = payload.written.data_pg_id;
        let written_shards = payload.written.written_shards.clone();

        let primary = server_set
            .servers
            .iter()
            .find(|server| server.config_snapshot().node_id == NodeId::new(1))
            .cloned()
            .unwrap();
        let proof = {
            let pg = primary._node.get_pg(0).unwrap();
            let now = crate::clock::current_time_millis();
            let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
                &*pg,
                DurableBucketWriteReservationAcquire {
                    name: &bucket,
                    reservation_id: namespace,
                    owner_token: namespace,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    operation_kind:
                        crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    created_at: now,
                    lease_deadline: now.saturating_add(60_000),
                    target_context: Some(key.as_str()),
                },
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            BucketWriteReservationProof::from(&reservation)
        };
        let prepared = crate::PreparedDirectPutObjectCommit {
            versioning: BucketVersioningState::Suspended,
            owner,
            acl_grants: AclGrants::default(),
            public_read: false,
            etag_crc64: checksum::crc64::checksum(data),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            bucket_write_reservation: proof,
        };

        let response_lost = Arc::new(AtomicBool::new(false));
        let response_lost_for_hook = Arc::clone(&response_lost);
        primary.set_response_envelope_test_hook(Arc::new(move |kind, envelope| {
            if kind == StorageRpcMessageKind::MetadataCommandPendingSlotInsert
                && !response_lost_for_hook.swap(true, Ordering::AcqRel)
            {
                *envelope
                    .last_mut()
                    .expect("authenticated response envelope must not be empty") ^= 1;
            }
        }));
        let unrelated_contender = Arc::new(Mutex::new(None));
        let unrelated_contender_installed = Arc::new(AtomicBool::new(false));
        let _pending_install_hook = if matches!(
            followup,
            DirectPutPendingInstallResponseLossFollowup::UnrelatedContender
        ) {
            let primary = Arc::clone(&primary);
            let bucket = bucket.clone();
            let contender_slot = Arc::clone(&unrelated_contender);
            let installed = Arc::clone(&unrelated_contender_installed);
            Some(
                cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(
                    move || {
                        if installed.swap(true, Ordering::AcqRel) {
                            return;
                        }
                        let pg = primary._node.get_pg(0).unwrap();
                        let log_index = MetadataCommandLogIndex::new(
                            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                                .unwrap()
                                + 1,
                        )
                        .unwrap();
                        let contender = MetadataCommandEnvelope::new(
                            MetadataCommandId::new(
                                ClusterEpoch::INITIAL,
                                PgId::new(0),
                                log_index,
                            ),
                            MetadataCommandPayload::ReserveObjectGeneration(
                                ReserveObjectGenerationCommand::new(
                                    bucket.clone(),
                                    crate::tests::object_key("unrelated-contender"),
                                    crate::tests::stream_session_id("other"),
                                    GenerationId::new(1).unwrap(),
                                    crate::clock::current_time_millis(),
                                ),
                            ),
                        );
                        pg.try_insert_pending_metadata_command_slot(
                            primary.config_snapshot().node_id.as_u32(),
                            &contender,
                            Some(&bucket),
                        )
                        .unwrap();
                        *contender_slot.lock().unwrap() = Some(contender);
                    },
                )),
            )
        } else {
            None
        };
        let integrity_failure = Arc::new(AtomicBool::new(false));
        if matches!(
            followup,
            DirectPutPendingInstallResponseLossFollowup::IntegrityFailure
        ) {
            let integrity_failure_for_hook = Arc::clone(&integrity_failure);
            let response_lost_for_frame_hook = Arc::clone(&response_lost);
            primary.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                if kind == StorageRpcMessageKind::MetadataCommandPendingEnvelope
                    && response_lost_for_frame_hook.load(Ordering::Acquire)
                    && !integrity_failure_for_hook.swap(true, Ordering::AcqRel)
                {
                    frame.payload = encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::MetadataCommandIntegrity,
                        message: "injected metadata command integrity failure".to_owned(),
                    })
                    .unwrap();
                }
            }));
        }
        let budget_expired = Arc::new(AtomicBool::new(false));
        let _uncertainty_hook = if matches!(
            followup,
            DirectPutPendingInstallResponseLossFollowup::ExpireBudget
                | DirectPutPendingInstallResponseLossFollowup::UnrelatedContender
        ) {
            let budget_expired_for_hook = Arc::clone(&budget_expired);
            Some(
                cluster.test_install_direct_put_pending_install_uncertainty_hook(Arc::new(
                    move || !budget_expired_for_hook.swap(true, Ordering::AcqRel),
                )),
            )
        } else {
            None
        };

        let error = route
            .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(() ))
            .expect_err("ambiguous pending installation must not report direct PUT success");
        assert_eq!(
            error.kind(),
            match followup {
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget => {
                    crate::DirectPutFailureKind::SnapshotReinspectionConflict
                }
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => {
                    crate::DirectPutFailureKind::SnapshotReinspectionConflict
                }
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => {
                    crate::DirectPutFailureKind::InternalError
                }
            },
            "unexpected direct PUT failure classification: {}",
            error.diagnostic_cause_label(),
        );
        assert_eq!(
            error.diagnostic_cause_label(),
            match followup {
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget => {
                    "snapshot_reinspection_conflict"
                }
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => {
                    "snapshot_reinspection_conflict"
                }
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => {
                    "store_integrity_failure"
                }
            }
        );
        assert!(response_lost.load(Ordering::Acquire));
        assert_eq!(
            budget_expired.load(Ordering::Acquire),
            matches!(
                followup,
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget
                    | DirectPutPendingInstallResponseLossFollowup::UnrelatedContender
            )
        );
        assert_eq!(
            integrity_failure.load(Ordering::Acquire),
            matches!(
                followup,
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure
            )
        );
        assert_eq!(
            unrelated_contender_installed.load(Ordering::Acquire),
            matches!(
                followup,
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender
            )
        );

        let primary_pg = primary._node.get_pg(0).unwrap();
        let pending = primary_pg
            .pending_metadata_command_slot(1, ClusterEpoch::INITIAL)
            .unwrap();
        match followup {
            DirectPutPendingInstallResponseLossFollowup::ExpireBudget => {
                assert!(
                    pending.is_none(),
                    "budget exhaustion must abandon the exact unpublished command"
                );
            }
            DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => {
                let pending = pending
                    .as_ref()
                    .expect("integrity failure must preserve the durably inserted command");
                assert_eq!(pending.scope_bucket.as_ref(), Some(&bucket));
                assert!(!pending.publication_started);
            }
            DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => {
                let contender = unrelated_contender
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("the unrelated contender must have been installed");
                if let Some(pending) = pending.as_ref() {
                    assert_eq!(pending.id, contender.id());
                    assert_eq!(pending.command_bytes, contender.command_bytes());
                    assert_eq!(pending.scope_bucket.as_ref(), Some(&bucket));
                } else {
                    assert!(primary_pg
                        .applied_metadata_command_log_entry_hashes(
                            primary.config_snapshot().node_id.as_u32(),
                            &contender,
                        )
                        .unwrap()
                        .is_some());
                }
            }
        }
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
                .unwrap()
                .len(),
            match followup {
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget => 0,
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => 1,
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => 0,
            },
            "bucket-write reservation ownership must follow the terminal classification"
        );
        drop(primary_pg);

        for server in &server_set.servers {
            let pg = server._node.get_pg(0).unwrap();
            let generation_reservation = PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &generation_reservation_id,
            );
            match followup {
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget => {
                    assert!(generation_reservation.is_err());
                }
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => {
                    assert!(generation_reservation.is_err());
                }
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => {
                    assert!(generation_reservation.is_ok());
                }
            }
            assert!(matches!(
                PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }
        for written in &written_shards {
            let node_id = NodeId::new(u32::from(written.key.shard_index().get()));
            let server = server_set
                .servers
                .iter()
                .find(|server| server.config_snapshot().node_id == node_id)
                .unwrap();
            let shard = server._node.read_shard_file(data_pg_id, &written.key);
            match followup {
                DirectPutPendingInstallResponseLossFollowup::ExpireBudget => {
                    assert!(shard.is_err());
                }
                DirectPutPendingInstallResponseLossFollowup::UnrelatedContender => {
                    assert!(shard.is_err());
                }
                DirectPutPendingInstallResponseLossFollowup::IntegrityFailure => {
                    assert!(shard.is_ok());
                }
            }
        }
    }

    #[test]
    fn authenticated_unix_direct_put_pending_install_response_loss_cleans_unpublished_command() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            false,
            DirectPutPendingInstallResponseLossFollowup::ExpireBudget,
        );
    }

    #[test]
    fn authenticated_tls_direct_put_pending_install_response_loss_cleans_unpublished_command() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            true,
            DirectPutPendingInstallResponseLossFollowup::ExpireBudget,
        );
    }

    #[test]
    fn authenticated_unix_direct_put_pending_install_response_loss_preserves_integrity_failure() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            false,
            DirectPutPendingInstallResponseLossFollowup::IntegrityFailure,
        );
    }

    #[test]
    fn authenticated_tls_direct_put_pending_install_response_loss_preserves_integrity_failure() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            true,
            DirectPutPendingInstallResponseLossFollowup::IntegrityFailure,
        );
    }

    #[test]
    fn authenticated_unix_direct_put_lost_pending_conflict_cleans_uninstalled_candidate() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            false,
            DirectPutPendingInstallResponseLossFollowup::UnrelatedContender,
        );
    }

    #[test]
    fn authenticated_tls_direct_put_lost_pending_conflict_cleans_uninstalled_candidate() {
        authenticated_direct_put_pending_install_response_loss_classifies_terminal_state(
            true,
            DirectPutPendingInstallResponseLossFollowup::UnrelatedContender,
        );
    }

    #[test]
    fn tls_tcp_ordinary_pool_reserves_single_connection_limit_for_stateful_session() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth_with_max_connections(&credential, 1))
            .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
            .bind()
            .unwrap();
        let address = server.tcp_listener_addr_for_test();
        let join = thread::spawn(move || {
            server.accept_one().unwrap();
            server.accept_one().unwrap();
        });
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth_with_max_connections(
                credential, 9, 1,
            )),
        );

        let payload = client
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .unwrap();
        assert_eq!(
            decode_health_response(&payload).unwrap().node_id,
            config.node_id
        );

        let critical_section = MetadataCommandNodeClient::open_metadata_command_critical_section(
            &client,
            PgId::new(0),
            config.cluster_epoch,
        )
        .unwrap();
        drop(critical_section);
        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn tls_tcp_distinct_ordinary_pools_leave_reserved_stateful_capacity() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth_with_max_connections(&credential, 2))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap(),
        );
        let address = server.tcp_listener_addr_for_test();
        let new_client = || {
            let endpoint = StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap();
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                endpoint,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(storage_rpc_client_auth_with_max_connections(
                    credential.clone(),
                    9,
                    2,
                )),
            )
        };
        let first = new_client();
        let second = new_client();

        let serving = Arc::clone(&server);
        let accept = thread::spawn(move || serving.accept_and_spawn().unwrap());
        let payload = first
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .unwrap();
        assert_eq!(
            decode_health_response(&payload).unwrap().node_id,
            config.node_id
        );
        accept.join().unwrap();

        let serving = Arc::clone(&server);
        let accept = thread::spawn(move || serving.accept_and_spawn().unwrap());
        let payload = second
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .unwrap();
        assert_eq!(
            decode_health_response(&payload).unwrap().node_id,
            config.node_id
        );
        accept.join().unwrap();

        let serving = Arc::clone(&server);
        let accept = thread::spawn(move || serving.accept_and_spawn().unwrap());
        let critical_section = MetadataCommandNodeClient::open_metadata_command_critical_section(
            &second,
            PgId::new(0),
            config.cluster_epoch,
        )
        .unwrap();
        accept.join().unwrap();
        drop(critical_section);
        drop(first);
        drop(second);
    }

    #[test]
    fn tls_tcp_durable_effect_deadline_rebinds_to_storage_host_monotonic_clock() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = crate::clock::with_time_and_monotonic_override(1_000, 900_000, || {
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap()
        });
        let command = test_metadata_command(0, 1);
        let bucket = command.bucket_name().clone();
        let drain_bucket = BucketName::try_from("tcp-portable-drain-bucket").unwrap();
        let expired_drain_bucket = BucketName::try_from("tcp-expired-drain-bucket").unwrap();
        let shard_key = test_shard_key(0);
        let expired_shard_key = test_shard_key(1);
        let pg = server._node.get_pg(0).unwrap();
        create_probe_bucket_direct(&pg, &bucket);
        create_probe_bucket_direct(&pg, &drain_bucket);
        create_probe_bucket_direct(&pg, &expired_drain_bucket);
        drop(pg);
        let node = Arc::clone(&server._node);
        let address = server.tcp_listener_addr_for_test();
        let join = thread::spawn(move || {
            crate::clock::with_time_and_monotonic_override(2_500, 901_500, || {
                server.accept_one().unwrap()
            });
            crate::clock::with_time_and_monotonic_override(2_800, 901_800, || {
                server.accept_one().unwrap()
            });
            crate::clock::with_time_and_monotonic_override(2_800, 901_800, || {
                server.accept_one().unwrap()
            });
            crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                server.accept_one().unwrap()
            });
            crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                server.accept_one().unwrap()
            });
            crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                server.accept_one().unwrap()
            });
        });
        let new_client = || {
            let endpoint = StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap();
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                endpoint,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(storage_rpc_client_auth(credential.clone(), 9)),
            )
            .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()))
        };
        // The frontend's captured deadline is 1,500 ms away on its local
        // monotonic clock. The production client must project that to the
        // portable wall deadline (4,000 ms); no monotonic timestamp may cross
        // the TLS/TCP boundary.
        let effect_fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500);
        let client = new_client();
        let reservation_route = client
            .open_bucket_write_reservation_route(
                config.cluster_epoch,
                BucketPgId::new_for_test(PgId::new(0)),
                &bucket,
            )
            .unwrap();
        let reservation = crate::clock::with_time_and_monotonic_override(2_500, 10_000, || {
            reservation_route
                .acquire_durable_bucket_write_reservation_with_effect_fence(
                    DurableBucketWriteReservationAcquire {
                        name: &bucket,
                        reservation_id: "tcp-portable-reservation",
                        owner_token: "tcp-portable-owner",
                        cluster_epoch: config.cluster_epoch,
                        operation_kind: "put-object-metadata",
                        created_at: 1_000,
                        lease_deadline: 9_000,
                        target_context: Some("object"),
                    },
                    effect_fence,
                )
                .unwrap()
        });
        assert_eq!(reservation.bucket, bucket);
        drop(reservation_route);
        drop(client);

        let client = new_client();
        let drain_route = client
            .open_bucket_write_reservation_route(
                config.cluster_epoch,
                BucketPgId::new_for_test(PgId::new(0)),
                &drain_bucket,
            )
            .unwrap();
        let drain = crate::clock::with_time_and_monotonic_override(2_600, 10_100, || {
            drain_route
                .begin_durable_bucket_write_drain_with_effect_fence(
                    "tcp-portable-drain",
                    "tcp-portable-drain-owner",
                    1_000,
                    9_000,
                    effect_fence,
                )
                .unwrap()
        });
        assert_eq!(drain.bucket, drain_bucket);
        drop(drain_route);
        drop(client);

        let shard_payload = b"portable fenced shard";
        let data_pg_id = DataPgId::new_for_test(PgId::new(0));
        let client = new_client();
        let shard_route = client
            .open_placed_shard_route(
                crate::cluster::ShardLocation::new(
                    config.cluster_epoch,
                    data_pg_id,
                    shard_key.shard_index(),
                    config.node_id,
                ),
                &shard_key,
            )
            .unwrap();
        let shard_ack = crate::clock::with_time_and_monotonic_override(2_600, 10_100, || {
            shard_route
                .write_placed_shard_with_effect_fence(shard_payload, effect_fence)
                .unwrap()
        });
        assert_eq!(shard_ack.stored_size, shard_payload.len() as u64);
        assert_eq!(node.read_shard_file(0, &shard_key).unwrap(), shard_payload);
        drop(shard_route);
        drop(client);

        let client = new_client();
        let expired_drain_route = client
            .open_bucket_write_reservation_route(
                config.cluster_epoch,
                BucketPgId::new_for_test(PgId::new(0)),
                &expired_drain_bucket,
            )
            .unwrap();
        let drain_error = crate::clock::with_time_and_monotonic_override(3_500, 11_000, || {
            expired_drain_route
                .begin_durable_bucket_write_drain_with_effect_fence(
                    "tcp-expired-drain",
                    "tcp-expired-drain-owner",
                    1_000,
                    9_000,
                    effect_fence,
                )
                .unwrap_err()
        });
        assert!(
            matches!(
                &drain_error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{drain_error:?}"
        );
        drop(expired_drain_route);
        drop(client);

        let client = new_client();
        let pending_error = crate::clock::with_time_and_monotonic_override(3_500, 11_000, || {
            MetadataCommandNodeClient::try_insert_pending_metadata_command_slot_with_effect_fence(
                &client,
                PgId::new(0),
                &command,
                Some(command.bucket_name()),
                effect_fence,
            )
            .unwrap_err()
        });
        assert!(
            matches!(
                &pending_error,
                StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                }
            ),
            "{pending_error:?}"
        );
        drop(client);

        let client = new_client();
        let expired_shard_route = client
            .open_placed_shard_route(
                crate::cluster::ShardLocation::new(
                    config.cluster_epoch,
                    data_pg_id,
                    expired_shard_key.shard_index(),
                    config.node_id,
                ),
                &expired_shard_key,
            )
            .unwrap();
        let shard_error = crate::clock::with_time_and_monotonic_override(3_500, 11_000, || {
            expired_shard_route
                .write_placed_shard_with_effect_fence(b"must not be written", effect_fence)
                .unwrap_err()
        });
        assert!(
            matches!(
                &shard_error,
                StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                }
            ),
            "{shard_error:?}"
        );
        drop(expired_shard_route);
        drop(client);
        join.join().unwrap();

        assert_eq!(node.read_shard_file(0, &shard_key).unwrap(), shard_payload);
        assert!(matches!(
            node.read_shard_file(0, &expired_shard_key),
            Err(StoreError::NotFound)
        ));
        let pg = node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_drain(&*pg, &expired_drain_bucket)
                .unwrap()
                .is_none()
        );
        assert!(pg
            .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
            .unwrap()
            .is_none());
    }

    #[test]
    fn unix_multipart_abort_build_rebinds_and_rejects_expired_effect_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_and_monotonic_override(1_000, 900_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("unix-expired-abort-bucket");
        let key = crate::tests::object_key("unix-expired-abort-key");
        let upload_id = crate::tests::multipart_upload_id("unix-expired-abort-upload");
        let create = CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: crate::OwnerIdentity::from_principal("unix-expired-abort-owner"),
            owner: crate::OwnerIdentity::from_principal("unix-expired-abort-owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        let node = Arc::clone(&server._node);
        let (upload, cleanup, command_log_index_before) =
            crate::clock::with_time_override(1_000, || {
                let pg = node.get_pg(0).unwrap();
                PgMetadataStore::create_multipart_upload(&*pg, &create).unwrap();
                let upload = PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
                let cleanup = pg
                    .prepare_abort_multipart_upload_cleanup(&bucket, &key, &upload_id)
                    .unwrap()
                    .expect("in-progress upload must have abort cleanup");
                let command_log_index = pg
                    .max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap();
                (upload, cleanup, command_log_index)
            });
        let authorized_upload =
            crate::types::AuthorizedMultipartUploadAbort::assume_authorized(upload);
        let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        proof.operation_kind = ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();

        let serving = thread::spawn(move || {
            for _ in 0..2 {
                crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                    server.accept_one().unwrap();
                });
            }
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let route = ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
            &client,
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &upload_id,
        )
        .unwrap();
        // The frontend still has 1,500 ms on its local monotonic clock and
        // projects a portable wall deadline of 4,000 ms. The storage host
        // receives the request at 4,500 ms and must reject it after rebinding
        // that portable deadline to its unrelated monotonic clock.
        let effect_fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500);
        let ordinary_error = crate::clock::with_time_and_monotonic_override(2_500, 10_000, || {
            route
                .build_abort_multipart_upload_command(
                    BuildAbortMultipartUploadCommandReq {
                        expected_cleanup: Some(&cleanup),
                        bucket_write_reservation: &proof,
                    },
                    effect_fence,
                )
                .unwrap_err()
        });
        assert!(
            matches!(
                &ordinary_error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{ordinary_error:?}"
        );
        let authorized_error =
            crate::clock::with_time_and_monotonic_override(2_500, 10_000, || {
                route
                    .build_authorized_abort_multipart_upload_command(
                        BuildAuthorizedAbortMultipartUploadCommandReq {
                            authorized_upload: &authorized_upload,
                            expected_cleanup: Some(&cleanup),
                            bucket_write_reservation: &proof,
                        },
                        effect_fence,
                    )
                    .unwrap_err()
            });
        assert!(
            matches!(
                &authorized_error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{authorized_error:?}"
        );
        serving.join().unwrap();

        let pg = node.get_pg(0).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            command_log_index_before,
            "expired Unix abort builds must not advance the PG command log"
        );
    }

    #[test]
    fn unix_payload_reclaim_claim_acquire_rebinds_and_rejects_expired_effect_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_and_monotonic_override(1_000, 900_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("unix-expired-reclaim-claim-bucket");
        let key = crate::tests::object_key("unix-expired-reclaim-claim-key");
        let generation_id = GenerationId::new(91).unwrap();
        let node = Arc::clone(&server._node);
        {
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::put_object_segments_reclaim(
                &*pg,
                &crate::ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id,
                    created_at: 1_000,
                    segments: Vec::new(),
                },
            )
            .unwrap();
        }

        let serving = thread::spawn(move || {
            crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                server.accept_one().unwrap();
            });
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &client,
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            generation_id,
        )
        .unwrap();
        let effect_fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500);
        let error = crate::clock::with_time_and_monotonic_override(2_500, 10_000, || {
            route
                .acquire_claim(
                    AcquireObjectPayloadReclaimClaimReq {
                        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                        bucket_incarnation_generation: 1,
                        claim_id: "unix-expired-reclaim-claim",
                        owner_token: "unix-expired-reclaim-owner",
                        claimed_at: 2_500,
                        lease_deadline: Some(9_000),
                        now: 2_500,
                    },
                    effect_fence,
                )
                .unwrap_err()
        });
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{error:?}"
        );
        serving.join().unwrap();

        assert!(
            PgMetadataStore::object_payload_reclaim_claim(&*node.get_pg(0).unwrap())
                .unwrap()
                .is_none(),
            "expired portable effect authority must not insert a reclaim claim"
        );
    }

    #[test]
    fn unix_payload_reclaim_claim_acquire_is_bounded_by_server_route_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("unix-server-expired-reclaim-claim-bucket");
        let key = crate::tests::object_key("unix-server-expired-reclaim-claim-key");
        let generation_id = GenerationId::new(92).unwrap();
        let node = Arc::clone(&server._node);
        {
            let pg = node.get_pg(0).unwrap();
            PgMetadataStore::put_object_segments_reclaim(
                &*pg,
                &crate::ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id,
                    created_at: 1_000,
                    segments: Vec::new(),
                },
            )
            .unwrap();
        }

        let server_node = Arc::clone(&node);
        let serving = thread::spawn(move || {
            let clock = crate::clock::test_time_override_guard(1_000);
            let hook_clock = clock.control();
            server_node
                .get_pg(0)
                .unwrap()
                .test_install_before_object_payload_reclaim_claim_effect_check_hook(move || {
                    hook_clock.set(4_500);
                });
            server.accept_one().unwrap();
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &client,
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            generation_id,
        )
        .unwrap();
        let client_fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 10_000);
        let error = crate::clock::with_time_and_monotonic_override(1_000, 1_000, || {
            route
                .acquire_claim(
                    AcquireObjectPayloadReclaimClaimReq {
                        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                        bucket_incarnation_generation: 1,
                        claim_id: "unix-server-expired-reclaim-claim",
                        owner_token: "unix-server-expired-reclaim-owner",
                        claimed_at: 1_000,
                        lease_deadline: Some(9_000),
                        now: 1_000,
                    },
                    client_fence,
                )
                .unwrap_err()
        });
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{error:?}"
        );
        serving.join().unwrap();

        assert!(
            PgMetadataStore::object_payload_reclaim_claim(&*node.get_pg(0).unwrap())
                .unwrap()
                .is_none(),
            "the shorter server route fence must prevent durable claim insertion"
        );
    }

    #[test]
    fn unix_payload_reclaim_build_rebinds_and_rejects_expired_effect_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_and_monotonic_override(1_000, 900_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("unix-expired-reclaim-build-bucket");
        let key = crate::tests::object_key("unix-expired-reclaim-build-key");
        let generation_id = GenerationId::new(91).unwrap();
        let reclaim = ObjectPayloadReclaimCommand::Segments(crate::ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 1_000,
            segments: Vec::new(),
        });
        let node = Arc::clone(&server._node);
        let (claim, command_log_index_before) = {
            let pg = node.get_pg(0).unwrap();
            let ObjectPayloadReclaimCommand::Segments(record) = &reclaim else {
                unreachable!("test reclaim uses the object-segments layout")
            };
            PgMetadataStore::put_object_segments_reclaim(&*pg, record).unwrap();
            let claim = PgMetadataStore::acquire_object_payload_reclaim_claim(
                &*pg,
                &bucket,
                1,
                &key,
                generation_id,
                ObjectPayloadReclaimKind::ObjectSegments,
                "unix-expired-reclaim-build-claim",
                "unix-expired-reclaim-build-owner",
                config.cluster_epoch,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                1_000,
                Some(9_000),
                1_000,
            )
            .unwrap()
            .expect("seeded reclaim must be claimable");
            let command_log_index = pg
                .max_metadata_command_log_index(config.cluster_epoch)
                .unwrap();
            (claim, command_log_index)
        };

        let serving = thread::spawn(move || {
            crate::clock::with_time_and_monotonic_override(4_500, 903_500, || {
                server.accept_one().unwrap();
            });
        });
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &client,
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            generation_id,
        )
        .unwrap();
        let effect_fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500);
        let error = crate::clock::with_time_and_monotonic_override(2_500, 10_000, || {
            route
                .build_delete_object_payload_reclaim_command(
                    BuildDeleteObjectPayloadReclaimCommandReq {
                        payload: &reclaim,
                        claim: &claim,
                    },
                    effect_fence,
                )
                .unwrap_err()
        });
        assert!(
            matches!(
                &error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{error:?}"
        );
        serving.join().unwrap();

        let pg = node.get_pg(0).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            command_log_index_before,
            "expired Unix reclaim build must not advance the PG command log"
        );
    }

    #[test]
    fn stalled_tls_handshake_does_not_block_the_next_storage_rpc_connection() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap(),
        );
        let address = server.tcp_listener_addr_for_test();
        let stalled = TcpStream::connect(address).unwrap();

        server.accept_and_spawn().unwrap();

        let serving = Arc::clone(&server);
        let accept = thread::spawn(move || serving.accept_and_spawn().unwrap());
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let payload = client
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .unwrap();
        let health = decode_health_response(&payload).unwrap();

        assert_eq!(health.node_id, config.node_id);
        accept.join().unwrap();
        drop(stalled);
    }

    #[test]
    fn malformed_tls_handshake_is_contained_to_its_storage_rpc_connection() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap(),
        );
        let address = server.tcp_listener_addr_for_test();
        let mut malformed = TcpStream::connect(address).unwrap();
        malformed.write_all(b"not a TLS handshake").unwrap();
        malformed.shutdown(std::net::Shutdown::Write).unwrap();

        server.accept_and_spawn().unwrap();

        let serving = Arc::clone(&server);
        let accept = thread::spawn(move || serving.accept_and_spawn().unwrap());
        let endpoint = StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap();
        let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(storage_rpc_client_auth(credential, 9)),
        );

        let payload = client
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .unwrap();
        let health = decode_health_response(&payload).unwrap();

        assert_eq!(health.node_id, config.node_id);
        accept.join().unwrap();
    }

    #[test]
    fn tls_tcp_storage_rpc_listener_requires_rpc_authentication() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        let result = PreparedStorageNodeServer::new(config)
            .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
            .bind();

        assert!(matches!(
            result,
            Err(StorageNodeServerError::TcpRpcListenerRequiresAuthentication { .. })
        ));
    }

    #[test]
    fn storage_rpc_listener_bind_failure_removes_previously_bound_unix_socket() {
        let tmp = test_util::tempdir();
        private_socket_dir(tmp.path());
        let socket_path = tmp.path().join("partial-bind.sock");
        let result = bind_storage_node_rpc_listeners(
            vec![
                StorageNodeRpcListenerConfig::unix(&socket_path),
                StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                ),
            ],
            None,
        );

        assert!(matches!(
            result,
            Err(StorageNodeServerError::TcpRpcListenerRequiresAuthentication { .. })
        ));
        assert!(!socket_path.exists());
    }

    #[test]
    fn authenticated_unix_storage_rpc_rejects_legacy_frame_before_dispatch() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = PreparedStorageNodeServer::new(config.clone())
            .with_rpc_auth(storage_rpc_server_auth(&credential))
            .bind()
            .unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one());
        let client = UnixStorageNodeClient::new(config.node_id, config.cluster_epoch, socket_path);

        assert!(client
            .rpc_request(StorageRpcMessageKind::Health, Vec::new())
            .is_err());
        let server_error = join.join().unwrap().unwrap_err();
        assert!(server_error.to_string().contains("transport magic"));
    }

    #[test]
    fn authenticated_unix_storage_rpc_rejects_wrong_target_and_topology() {
        for (client_node_id, topology_generation, expected_error) in [
            (NodeId::new(8), 9, "WrongTarget"),
            (NodeId::new(7), 10, "WrongTopology"),
        ] {
            let tmp = test_util::tempdir();
            let config = test_config(&tmp);
            private_socket_dir(config.socket_path.parent().unwrap());
            let credential =
                storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-1".to_owned(),
                });
            let server = PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .bind()
                .unwrap();
            let socket_path = config.socket_path.clone();
            let join = thread::spawn(move || server.accept_one());
            let client = UnixStorageNodeClient::with_rpc_admission_settings_and_auth(
                client_node_id,
                config.cluster_epoch,
                socket_path,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(storage_rpc_client_auth(credential, topology_generation)),
            );

            assert!(client
                .rpc_request(StorageRpcMessageKind::Health, Vec::new())
                .is_err());
            let server_error = join.join().unwrap().unwrap_err();
            assert!(server_error.to_string().contains(expected_error));
        }
    }

    #[test]
    fn storage_node_server_drops_idle_rpc_session_after_timeout() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let started = Instant::now();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let _client = UnixStream::connect(socket_path).unwrap();
        join.join().unwrap();
        assert!(
            started.elapsed() < STORAGE_RPC_SERVER_IDLE_TIMEOUT + Duration::from_secs(2),
            "idle storage RPC session should be closed by server timeout"
        );
    }

    #[test]
    fn storage_node_connection_refreshes_config_for_each_frame() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(&mut client, 1, StorageRpcMessageKind::Health, Vec::new());
        let first_payload = decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let first_health = decode_health_response(&first_payload).unwrap();
        assert_eq!(first_health.cluster_epoch, ClusterEpoch::new(1).unwrap());

        let mut next_config = bounded_runtime_refresh_config(config.clone());
        next_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next_config.pg_routes[0].cluster_epoch = next_config.cluster_epoch;
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();

        let second = send_frame(&mut client, 2, StorageRpcMessageKind::Health, Vec::new());
        let second_payload = decode_storage_rpc_response_payload(&second.payload)
            .unwrap()
            .unwrap();
        let second_health = decode_health_response(&second_payload).unwrap();
        assert_eq!(second_health.cluster_epoch, ClusterEpoch::new(2).unwrap());

        let stale_read_acquire = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("stale-read", test_location(1, 0, 7)),
        );
        let stale_error = decode_storage_rpc_response_payload(&stale_read_acquire.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(stale_error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(stale_error
            .message
            .contains("does not match storage-node epoch 2"));
        assert_eq!(server.read_handle_count(test_location(1, 0, 7)), 0);

        drop(client);
        join.join().unwrap();
    }

    #[test]
    fn storage_node_session_rejects_current_primary_lock_for_historical_abandonment() {
        let tmp = test_util::tempdir();
        let source_epoch = ClusterEpoch::INITIAL;
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let config = bounded_runtime_refresh_config(test_config(&tmp));
        private_socket_dir(config.socket_path.parent().unwrap());
        let command = test_metadata_command(0, 1);
        let source_route = config.pg_routes[0].clone();
        let local_node_id = config.node_id;
        let socket_path = config.socket_path.clone();
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let serving = Arc::clone(&server);
        let join = thread::spawn(move || serving.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let current_lock_response = send_frame(
            &mut client,
            1,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: local_node_id,
                cluster_epoch: source_epoch,
                pg_id: PgId::new(0),
            }),
        );
        decode_storage_rpc_response_payload(&current_lock_response.payload)
            .unwrap()
            .unwrap();

        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = current_epoch;
        next_config.pg_routes[0].cluster_epoch = current_epoch;
        next_config.pg_routes[0].state = PgState::Peering;
        next_config.historical_pg_routes.push(source_route);
        next_config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                local_node_id,
                PendingMetadataCommandObservation::new(
                    source_epoch,
                    std::num::NonZeroU64::MIN,
                    command.checksum_crc64(),
                ),
            ),
        ));
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();

        let historical_lock_response = send_frame(
            &mut client,
            2,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: local_node_id,
                cluster_epoch: source_epoch,
                pg_id: PgId::new(0),
            }),
        );
        let historical_lock_error =
            decode_storage_rpc_response_payload(&historical_lock_response.payload)
                .unwrap()
                .unwrap_err();
        assert_eq!(
            historical_lock_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
        assert!(historical_lock_error.message.contains("CurrentPrimary"));
        assert!(historical_lock_error
            .message
            .contains("HistoricalRecoveryPrimary"));

        let abandonment_response = send_frame(
            &mut client,
            3,
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            encode_metadata_command_request(&StorageRpcMetadataCommandRequest {
                node_id: local_node_id,
                cluster_epoch: source_epoch,
                pg_id: PgId::new(0),
                command: command.clone(),
            })
            .unwrap(),
        );
        let abandonment_error = decode_storage_rpc_response_payload(&abandonment_response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            abandonment_error.code,
            StorageRpcErrorCode::StaleShardLocation
        );
        assert!(abandonment_error
            .message
            .contains("exact held recovery-primary lock binding"));

        let release_response = send_frame(
            &mut client,
            4,
            StorageRpcMessageKind::MetadataCommandPgLockRelease,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: local_node_id,
                cluster_epoch: source_epoch,
                pg_id: PgId::new(0),
            }),
        );
        decode_storage_rpc_response_payload(&release_response.payload)
            .unwrap()
            .unwrap();
        drop(client);
        join.join().unwrap();

        assert!(!server
            ._node
            .get_pg(0)
            .unwrap()
            .metadata_command_abandoned(local_node_id.as_u32(), &command)
            .unwrap());
    }

    #[test]
    fn storage_node_connection_route_validation_uses_refreshed_config() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let mut handler = server.connection_handler();

        handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap();

        let mut next_config = bounded_runtime_refresh_config(config.clone());
        next_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next_config.pg_routes[0].cluster_epoch = next_config.cluster_epoch;
        server
            .install_control_plane_runtime_config(next_config)
            .unwrap();
        handler.refresh_config_snapshot();

        let error = handler
            .validate_pg_route(config.node_id, config.cluster_epoch, PgId::new(0))
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
        assert!(error
            .message
            .contains("does not match storage-node epoch 2"));
    }

    #[test]
    fn storage_node_runtime_config_install_drains_admitted_route_permit() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let admitted = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next_config.pg_routes[0].cluster_epoch = next_config.cluster_epoch;

        let (installed_tx, installed_rx) = mpsc::channel();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(next_config)
                .unwrap();
            installed_tx.send(()).unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let state = server
                .route_admission
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
                "route install did not begin draining"
            );
            thread::yield_now();
        }
        assert!(matches!(
            installed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(
            server.config_snapshot().cluster_epoch,
            ClusterEpoch::new(1).unwrap()
        );
        drop(admitted);
        installed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().cluster_epoch,
            ClusterEpoch::new(2).unwrap()
        );
    }

    #[test]
    fn storage_node_runtime_config_staging_keeps_route_admission_open() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let stage_barrier = Arc::new(std::sync::Barrier::new(2));
        let hook_barrier = Arc::clone(&stage_barrier);
        *server
            .runtime_config_stage_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(move || {
            hook_barrier.wait();
            hook_barrier.wait();
        }));

        let mut next_config = bounded_runtime_refresh_config(config);
        next_config.cluster_epoch = ClusterEpoch::new(2).unwrap();
        next_config.pg_routes[0].cluster_epoch = next_config.cluster_epoch;
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(next_config)
                .unwrap();
        });

        stage_barrier.wait();
        assert_eq!(
            server
                .route_admission
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .transition,
            StorageNodeRouteTransitionState::Open,
            "runtime-config staging must not close route admission"
        );
        let admitted = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        drop(admitted);
        stage_barrier.wait();
        installer.join().unwrap();
        assert_eq!(
            server.config_snapshot().cluster_epoch,
            ClusterEpoch::new(2).unwrap()
        );
    }

    #[test]
    fn storage_node_runtime_config_validity_extension_is_memory_only_and_does_not_drain_frames() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        config.persist_control_plane_runtime_config().unwrap();
        let persisted_path = control_plane_runtime_config_path(&config.data_dir);
        let persisted_before = fs::read(&persisted_path).unwrap();
        let staged = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let staged_from_hook = Arc::clone(&staged);
        *server
            .runtime_config_stage_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(move || {
            staged_from_hook.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));
        let admitted = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(6_000).unwrap();

        let (installed_tx, installed_rx) = mpsc::channel();
        let installing_server = Arc::clone(&server);
        let installer = thread::spawn(move || {
            installing_server
                .install_control_plane_runtime_config(extended)
                .unwrap();
            installed_tx.send(()).unwrap();
        });

        installed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("validity-only extension must not wait for admitted frames");
        assert_eq!(
            server.config_snapshot().route_map_valid_until_ms(),
            Some(6_000)
        );
        assert_eq!(
            staged.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "validity-only extension must not stage the unchanged route map"
        );
        assert_eq!(fs::read(&persisted_path).unwrap(), persisted_before);
        assert_eq!(
            StorageNodeProcessConfig::load_control_plane_runtime_config(
                &config.data_dir,
                config.node_id,
                config.default_ec_shape,
                &config.socket_path,
            )
            .unwrap()
            .unwrap()
            .route_map_valid_until_ms(),
            Some(5_000),
            "the durable route map remains a fail-closed restart checkpoint"
        );
        drop(admitted);
        installer.join().unwrap();
    }

    #[test]
    fn bucket_metadata_scan_capability_binds_admission_domain_and_deadline() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("bucket-metadata-scan-capability");
        let owner = crate::CanonicalUserId::from_principal("owner");
        {
            let pg = server._node.get_pg(0).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }

        let handler = server.connection_handler();
        let active_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_bucket_scan_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test bucket metadata scan",
                )
                .unwrap()
        });
        let read_route = crate::clock::with_time_override(1_000, || {
            handler
                .metadata_read_bucket_scan_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test bucket metadata read scan",
                )
                .unwrap()
        });
        crate::clock::with_time_override(1_000, || {
            assert_eq!(read_route.list_buckets(owner.as_str()).unwrap().len(), 1);
            assert!(route
                .load_bucket_execution_generations(std::slice::from_ref(&bucket))
                .unwrap()
                .contains_key(&bucket));
            assert!(route
                .load_bucket_fast_path_identities(std::slice::from_ref(&bucket))
                .unwrap()
                .contains_key(&bucket));
        });

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        match handler.active_bucket_scan_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            "test bucket metadata scan",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign admission created a bucket metadata scan route"),
        }

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.active_bucket_scan_route(
            &retained_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            "test bucket metadata scan",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires active route admission"));
            }
            Ok(_) => panic!("retained admission created a bucket metadata scan route"),
        }

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            fn assert_expired<T>(result: Result<T, StorageNodeBucketRouteError>) {
                match result {
                    Err(StorageNodeBucketRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeBucketRouteError::Bucket(error)) => {
                        panic!("expired scan reached bucket storage: {error}")
                    }
                    Ok(_) => panic!("expired bucket metadata scan route remained usable"),
                }
            }

            assert_expired(read_route.list_buckets(owner.as_str()));
            assert_expired(route.load_bucket_execution_generations(std::slice::from_ref(&bucket)));
            assert_expired(route.load_bucket_fast_path_identities(std::slice::from_ref(&bucket)));
        });
    }

    #[test]
    fn object_payload_reclaim_capabilities_capture_deadline_and_retain_exact_claim_cleanup() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = crate::clock::with_time_override(1_000, || {
            StorageNodeServer::bind(config.clone()).unwrap()
        });
        let bucket = crate::tests::bucket_name("reclaim-capability-bucket");
        let key = crate::tests::object_key("reclaim-capability-key");
        let generation_id = GenerationId::new(91).unwrap();
        let reclaim = ObjectPayloadReclaimCommand::Segments(crate::ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 1_000,
            segments: Vec::new(),
        });
        {
            let pg = server._node.get_pg(0).unwrap();
            let ObjectPayloadReclaimCommand::Segments(reclaim) = &reclaim else {
                unreachable!("test reclaim is an object-segments record")
            };
            PgMetadataStore::put_object_segments_reclaim(&*pg, reclaim).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }

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
        let route = crate::clock::with_time_override(1_000, || {
            handler
                .active_primary_object_route(
                    &active_permit,
                    &request,
                    "test object payload reclaim",
                )
                .unwrap()
        });
        let claim = crate::clock::with_time_override(1_000, || {
            assert!(route.payload_reclaim_exists(generation_id).unwrap());
            assert_eq!(
                route.load_object_payload_reclaim(generation_id).unwrap(),
                Some(reclaim.clone())
            );
            route
                .acquire_object_payload_reclaim_claim(
                    1,
                    generation_id,
                    ObjectPayloadReclaimKind::ObjectSegments,
                    "reclaim-capability-claim",
                    "reclaim-capability-owner",
                    1_000,
                    Some(4_000),
                    1_000,
                    AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000),
                )
                .unwrap()
                .expect("active reclaim route must acquire the exact claim")
        });
        let scan_route = crate::clock::with_time_override(1_000, || {
            handler
                .active_primary_object_scan_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test object payload reclaim scan",
                )
                .unwrap()
        });
        let read_scan_route = crate::clock::with_time_override(1_000, || {
            handler
                .metadata_read_object_scan_route(
                    &active_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    "test object metadata read scan",
                )
                .unwrap()
        });
        crate::clock::with_time_override(1_000, || {
            let expected_root = PayloadReclaimRoot {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
            };
            assert_eq!(
                scan_route.bucket_payload_reclaim_root(&bucket).unwrap(),
                Some(expected_root.clone())
            );
            assert_eq!(
                scan_route.payload_reclaim_root().unwrap(),
                Some(expected_root)
            );
            assert_eq!(
                scan_route.object_payload_reclaim_claim().unwrap(),
                Some(claim.clone())
            );
            assert!(read_scan_route
                .list_objects_page(&crate::ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    start_after: None,
                    start_at: None,
                    max_keys: 1,
                })
                .unwrap()
                .objects
                .is_empty());
            assert!(read_scan_route
                .list_object_versions_page(&crate::ListObjectVersionsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    key_marker: None,
                    version_id_marker: None,
                    start_at: None,
                    max_keys: 1,
                })
                .unwrap()
                .versions
                .is_empty());
            assert!(read_scan_route
                .list_multipart_uploads_page(&crate::ListMultipartUploadsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    page_start: None,
                    max_uploads: 1,
                })
                .unwrap()
                .uploads
                .is_empty());
            assert!(scan_route
                .list_stream_uploads_for_bucket_page(&bucket, None, 1)
                .unwrap()
                .uploads
                .is_empty());
            assert!(scan_route
                .list_all_stream_uploads_page(None, 1)
                .unwrap()
                .uploads
                .is_empty());
            assert!(scan_route
                .list_shard_scavenger_payload_references()
                .unwrap()
                .is_empty());
        });

        let foreign_permit = StorageNodeRouteAdmissionGate::default()
            .acquire(StorageNodeRouteAdmissionClass::Active);
        match handler.active_primary_object_scan_route(
            &foreign_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            "test object payload reclaim scan",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("different admission domain"));
            }
            Ok(_) => panic!("foreign admission created an active object scan route"),
        }

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        match handler.active_primary_object_scan_route(
            &retained_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            "test object payload reclaim scan",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires active route admission"));
            }
            Ok(_) => panic!("retained admission created an active object scan route"),
        }
        match handler.retained_object_payload_reclaim_claim_route(
            &active_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &claim,
            "test object payload reclaim claim release",
        ) {
            Err(error) => {
                assert_eq!(error.code, StorageRpcErrorCode::Internal);
                assert!(error.message.contains("requires retained-cleanup"));
            }
            Ok(_) => panic!("active admission created retained reclaim cleanup authority"),
        }
        let mut wrong_pg_claim = claim.clone();
        wrong_pg_claim.pg_id = 1;
        match handler.retained_object_payload_reclaim_claim_route(
            &retained_permit,
            config.node_id,
            config.cluster_epoch,
            PgId::new(0),
            &wrong_pg_claim,
            "test object payload reclaim claim release",
        ) {
            Err(error) => assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode),
            Ok(_) => panic!("mismatched claim PG created retained reclaim cleanup authority"),
        }

        let mut extended = config.clone();
        extended.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        crate::clock::with_time_override(1_000, || {
            server
                .install_control_plane_runtime_config(extended)
                .unwrap();
        });
        crate::clock::with_time_override(6_000, || {
            fn assert_scan_route_expired<T>(
                result: Result<T, StorageNodeObjectPayloadReclaimRouteError>,
            ) {
                match result {
                    Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                        panic!("captured scan route should expire before node access: {error}")
                    }
                    Ok(_) => panic!("expired captured scan route reached node access"),
                }
            }

            fn assert_bucket_scan_route_expired<T>(result: Result<T, StorageNodeBucketRouteError>) {
                match result {
                    Err(StorageNodeBucketRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeBucketRouteError::Bucket(error)) => {
                        panic!("captured scan route should expire before node access: {error}")
                    }
                    Ok(_) => panic!("expired captured scan route reached node access"),
                }
            }

            fn assert_object_scan_route_expired<T>(result: Result<T, StorageNodeObjectRouteError>) {
                match result {
                    Err(StorageNodeObjectRouteError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeObjectRouteError::Object(error)) => {
                        panic!("captured scan route should expire before node access: {error}")
                    }
                    Ok(_) => panic!("expired captured scan route reached node access"),
                }
            }

            fn assert_store_scan_route_expired<T>(
                result: Result<T, StorageNodeObjectScanStoreError>,
            ) {
                match result {
                    Err(StorageNodeObjectScanStoreError::Route(error)) => {
                        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                    }
                    Err(StorageNodeObjectScanStoreError::Store(error)) => {
                        panic!("captured scan route should expire before node access: {error}")
                    }
                    Ok(_) => panic!("expired captured scan route reached node access"),
                }
            }

            assert_scan_route_expired(scan_route.bucket_payload_reclaim_root(&bucket));
            assert_scan_route_expired(scan_route.payload_reclaim_root());
            assert_scan_route_expired(scan_route.object_payload_reclaim_claim());
            assert_bucket_scan_route_expired(read_scan_route.list_objects_page(
                &crate::ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    start_after: None,
                    start_at: None,
                    max_keys: 1,
                },
            ));
            assert_bucket_scan_route_expired(read_scan_route.list_object_versions_page(
                &crate::ListObjectVersionsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    key_marker: None,
                    version_id_marker: None,
                    start_at: None,
                    max_keys: 1,
                },
            ));
            assert_bucket_scan_route_expired(read_scan_route.list_multipart_uploads_page(
                &crate::ListMultipartUploadsReq {
                    bucket: bucket.clone(),
                    prefix: None,
                    page_start: None,
                    max_uploads: 1,
                },
            ));
            assert_object_scan_route_expired(
                scan_route.list_stream_uploads_for_bucket_page(&bucket, None, 1),
            );
            assert_object_scan_route_expired(scan_route.list_all_stream_uploads_page(None, 1));
            assert_store_scan_route_expired(scan_route.list_shard_scavenger_payload_references());
            match route.payload_reclaim_exists(generation_id) {
                Err(StorageNodeObjectRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                }
                Err(StorageNodeObjectRouteError::Object(error)) => {
                    panic!("captured route should expire before reclaim existence read: {error}")
                }
                Ok(exists) => panic!("expired captured route returned reclaim existence {exists}"),
            }
            match route.load_object_payload_reclaim(generation_id) {
                Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                }
                Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                    panic!("captured route should expire before reclaim load: {error}")
                }
                Ok(loaded) => panic!("expired captured route loaded reclaim {loaded:?}"),
            }
            match route.acquire_object_payload_reclaim_claim(
                1,
                generation_id,
                ObjectPayloadReclaimKind::ObjectSegments,
                "expired-reclaim-capability-claim",
                "reclaim-capability-owner",
                6_000,
                Some(9_000),
                6_000,
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000),
            ) {
                Err(StorageNodeObjectPayloadReclaimRouteError::Route(error)) => {
                    assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
                }
                Err(StorageNodeObjectPayloadReclaimRouteError::Reclaim(error)) => {
                    panic!("captured route should expire before reclaim claim mutation: {error}")
                }
                Ok(record) => panic!("expired captured route returned reclaim claim {record:?}"),
            }
        });
        {
            let pg = server._node.get_pg(0).unwrap();
            assert_eq!(
                PgMetadataStore::object_payload_reclaim_claim(&*pg).unwrap(),
                Some(claim.clone()),
                "expired active route must leave the durable reclaim claim exact"
            );
        }
        drop(active_permit);
        drop(retained_permit);

        let history_summary = server
            ._node
            .cluster_map_history_reference_summary()
            .unwrap();
        assert_eq!(
            history_summary.oldest_object_payload_reclaim_claim_epoch,
            Some(config.cluster_epoch),
            "the durable claim must retain its acquisition route epoch"
        );
        let epoch_one_route = server.config_snapshot().pg_routes[0].clone();
        let mut epoch_two = server.config_snapshot();
        epoch_two.cluster_epoch = ClusterEpoch::new(2).unwrap();
        epoch_two.route_map_validity = RouteMapValidity::until_ms(20_000).unwrap();
        epoch_two.pg_routes[0].cluster_epoch = epoch_two.cluster_epoch;
        epoch_two.historical_pg_routes = prune_refresh_historical_pg_routes(
            BTreeMap::from([(
                (epoch_one_route.cluster_epoch, epoch_one_route.pg_id),
                epoch_one_route.clone(),
            )]),
            config.cluster_epoch,
            history_summary,
            &BTreeSet::new(),
        );
        crate::clock::with_time_override(6_000, || {
            server
                .install_control_plane_runtime_config(epoch_two.clone())
                .unwrap();
        });

        let epoch_two_route = epoch_two.pg_routes[0].clone();
        let mut epoch_three = epoch_two.clone();
        epoch_three.cluster_epoch = ClusterEpoch::new(3).unwrap();
        epoch_three.pg_routes[0].cluster_epoch = epoch_three.cluster_epoch;
        let historical_candidates = epoch_two
            .historical_pg_routes
            .iter()
            .cloned()
            .chain(std::iter::once(epoch_two_route))
            .map(|route| ((route.cluster_epoch, route.pg_id), route))
            .collect();
        epoch_three.historical_pg_routes = prune_refresh_historical_pg_routes(
            historical_candidates,
            epoch_two.cluster_epoch,
            history_summary,
            &BTreeSet::new(),
        );
        assert!(epoch_three.historical_pg_routes.iter().any(|route| {
            route.cluster_epoch == config.cluster_epoch && route.pg_id == PgId::new(0).get()
        }));
        crate::clock::with_time_override(6_000, || {
            server
                .install_control_plane_runtime_config(epoch_three)
                .unwrap();
        });

        let retained_permit = server
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        crate::clock::with_time_override(6_000, || {
            handler
                .retained_object_payload_reclaim_claim_route(
                    &retained_permit,
                    config.node_id,
                    config.cluster_epoch,
                    PgId::new(0),
                    &claim,
                    "test object payload reclaim claim release",
                )
                .unwrap()
                .release()
                .unwrap();
        });
        let pg = server._node.get_pg(0).unwrap();
        assert!(
            PgMetadataStore::object_payload_reclaim_claim(&*pg)
                .unwrap()
                .is_none(),
            "retained cleanup must release the exact expired-route reclaim claim"
        );
        drop(pg);
        assert_eq!(
            server
                ._node
                .cluster_map_history_reference_summary()
                .unwrap()
                .oldest_object_payload_reclaim_claim_epoch,
            None,
            "exact claim release must also release its historical-route reference"
        );
    }
