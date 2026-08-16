// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone)]
struct StorageNodeConnectionHandler {
    config: Arc<StorageNodeProcessConfig>,
    route_map_lease: Option<BoundRouteMapLease>,
    runtime_route_source: Arc<RwLock<StorageNodeRuntimeRouteState>>,
    route_admission: StorageNodeRouteAdmissionGate,
    node: Arc<SharedStorageNode>,
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    metadata_command_locks: StorageNodeMetadataCommandLocks,
    rpc_auth: Option<Arc<StorageRpcServerAuthConfig>>,
    #[cfg(test)]
    runtime_route_capture_test_hook: Arc<Mutex<Option<RuntimeConfigStageTestHook>>>,
    #[cfg(test)]
    response_envelope_test_hook: Arc<Mutex<Option<StorageRpcResponseEnvelopeTestHook>>>,
    #[cfg(test)]
    response_frame_test_hook: Arc<Mutex<Option<StorageRpcResponseFrameTestHook>>>,
    #[cfg(test)]
    metadata_command_before_commit_test_hook:
        Arc<Mutex<Option<MetadataCommandBeforeCommitTestHook>>>,
}
#[derive(Debug)]
enum StorageNodeBucketRouteError {
    Route(StorageRpcErrorResponse),
    Bucket(BucketSnapshotLoadError),
}

#[derive(Debug)]
enum StorageNodeObjectRouteError {
    Route(StorageRpcErrorResponse),
    Object(ObjectPgActionError),
}

#[derive(Debug)]
enum StorageNodeMultipartUploadRouteError {
    Route(StorageRpcErrorResponse),
    Upload(BucketSnapshotLoadError),
}

#[derive(Debug)]
enum StorageNodeObjectPayloadReclaimRouteError {
    Route(StorageRpcErrorResponse),
    Reclaim(BucketSnapshotLoadError),
}

fn object_payload_reclaim_route_open_error(
    error: ObjectPgActionError,
) -> StorageNodeObjectPayloadReclaimRouteError {
    match error {
        ObjectPgActionError::Store(error) => StorageNodeObjectPayloadReclaimRouteError::Reclaim(
            BucketSnapshotLoadError::Store(error),
        ),
        ObjectPgActionError::Metadata(error) => StorageNodeObjectPayloadReclaimRouteError::Reclaim(
            BucketSnapshotLoadError::Metadata(error),
        ),
        error => StorageNodeObjectPayloadReclaimRouteError::Route(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: format!("open object payload reclaim route failed: {error}"),
        }),
    }
}

#[derive(Debug)]
enum StorageNodeObjectScanStoreError {
    Route(StorageRpcErrorResponse),
    Store(StoreError),
}

#[derive(Debug)]
enum StorageNodeDataRouteError {
    Route(StorageRpcErrorResponse),
    Store(StoreError),
}

enum StorageNodeRetainedStreamAbortApplyError {
    Route(StorageRpcErrorResponse),
    Apply(BucketSnapshotLoadError),
}

enum StorageNodeRetainedStreamAbortFinishError {
    Route(StorageRpcErrorResponse),
    Finish(StoreError),
}

struct StorageNodeActiveBucketRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: BucketPgId,
    bucket: &'a BucketName,
}

struct StorageNodeMetadataReadBucketRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: BucketPgId,
    bucket: &'a BucketName,
}

struct StorageNodeActiveBucketScanRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: BucketPgId,
}

struct StorageNodeMetadataReadBucketScanRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: BucketPgId,
}

struct StorageNodeBucketDeleteReplicaHeadRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: BucketPgId,
    bucket: &'a BucketName,
}

struct StorageNodeActiveObjectRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
}

struct StorageNodeActivePrimaryObjectRoute<'a> {
    route: StorageNodeActiveObjectRoute<'a>,
}

struct StorageNodeMetadataReadObjectRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
}

struct StorageNodeActivePrimaryObjectScanRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: ObjectMetadataScanPgId,
}

struct StorageNodeMetadataReadObjectScanRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: ObjectMetadataScanPgId,
}

struct StorageNodeActiveShardRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    location: ShardLocation,
    shard_key: &'a ShardKey,
}

struct StorageNodeActiveReadHandleAcquireRoute<'a> {
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    session: &'a mut StorageNodeSession,
    read_operation_id: String,
    entries: Vec<(ShardLocation, ShardKey)>,
}

struct StorageNodeActivePrimaryDataRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: DataPgId,
}

struct StorageNodeActiveDataScanRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    pg_id: DataPgId,
}

struct StorageNodeActiveObjectPayloadLeaseControl<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    fence: StorageNodeRouteFence,
    request: &'a StorageRpcObjectPayloadLeaseControlRequest,
}

struct StorageNodeRetainedObjectPayloadLeaseControl<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    request: &'a StorageRpcObjectPayloadLeaseControlRequest,
}

struct StorageNodeRetainedBucketWriteReservationRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: BucketPgId,
    record: &'a BucketWriteReservationRecord,
}

struct StorageNodeRetainedMetadataCommandProofRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: BucketPgId,
    proof: &'a BucketWriteReservationProof,
}

struct StorageNodeRetainedBucketWriteDrainRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: BucketPgId,
    record: &'a BucketWriteDrainRecord,
}

struct StorageNodeRetainedBucketDeleteFinalizeClaimRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: BucketPgId,
    claim: &'a BucketDeleteFinalizeClaimRecord,
}

struct StorageNodeRetainedLifecycleSweepClaimRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: BucketPgId,
    claim: &'a LifecycleSweepClaimRecord,
}

struct StorageNodeRetainedObjectPayloadReclaimClaimRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: ObjectMetadataPgId,
    claim: &'a ObjectPayloadReclaimClaimRecord,
}

struct StorageNodeRetainedShardPayloadDeleteRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    location: ShardLocation,
    shard_key: &'a ShardKey,
}

struct StorageNodeRetainedShardInspectionRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    location: ShardLocation,
    shard_key: &'a ShardKey,
}

struct StorageNodeRetainedReadHandleReleaseRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    session: &'a mut StorageNodeSession,
    read_operation_id: String,
}

struct StorageNodeRetainedShardAckDeleteRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: DataPgId,
    shard_key: &'a ShardKey,
}

struct StorageNodeRetainedShardAckInspectionRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: DataPgId,
    shard_key: &'a ShardKey,
}

struct StorageNodeRetainedStreamAbortRoute<'a> {
    handler: &'a StorageNodeConnectionHandler,
    route_permit: &'a StorageNodeRouteAdmissionPermit,
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
}

struct StorageNodeRetainedStreamAbortSubject<'a> {
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    raw_pg_id: PgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
}

struct StorageNodeRetainedPrimaryStreamAbortRoute<'a> {
    route: StorageNodeRetainedStreamAbortRoute<'a>,
}

struct StorageNodeRetainedPrimaryStreamAbortSessionRoute<'a> {
    route: StorageNodeRetainedPrimaryStreamAbortRoute<'a>,
    session_id: &'a SessionId,
}

struct StorageNodeRetainedStreamAbortCommandRoute<'a> {
    route: StorageNodeRetainedStreamAbortRoute<'a>,
    prepared: PreparedRetainedStreamUploadAbort,
}

struct StorageNodeRetainedPrimaryStreamAbortCommandRoute<'a> {
    route: StorageNodeRetainedPrimaryStreamAbortRoute<'a>,
    prepared: PreparedRetainedStreamUploadAbort,
}

impl StorageNodeMetadataReadBucketRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn with_local_route<T>(
        &self,
        action: impl FnOnce(&PgStore) -> Result<T, BucketSnapshotLoadError>,
    ) -> Result<T, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        self.handler
            .with_metadata_read_pg(
                self.handler.config.node_id,
                self.fence.cluster_epoch,
                self.pg_id.pg_id(),
                action,
            )
            .map_err(StorageNodeBucketRouteError::Route)?
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn head_bucket(&self, filtered: bool) -> Result<BucketInfo, StorageNodeBucketRouteError> {
        self.with_local_route(|pg| {
            Ok(if filtered {
                PgMetadataStore::head_bucket(pg, self.bucket)?
            } else {
                PgMetadataStore::head_bucket_raw(pg, self.bucket)?
            })
        })
    }

    fn get_subresource(
        &self,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, StorageNodeBucketRouteError> {
        self.with_local_route(|pg| {
            Ok(
                PgMetadataStore::get_bucket_subresource(pg, self.bucket, kind)?
                    .map(|stored| stored.body),
            )
        })
    }

    fn load_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, StorageNodeBucketRouteError> {
        self.with_local_route(|pg| {
            SharedStorageNode::load_bucket_snapshot_from_pg(pg, self.bucket, request)
        })
    }
}

impl StorageNodeActiveBucketRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn open_local_bucket_write_reservation_route<'a>(
        &self,
        client: &'a LocalStorageNodeClient,
    ) -> Result<Box<dyn BucketWriteReservationRoute + 'a>, StorageNodeBucketRouteError> {
        client
            .open_bucket_write_reservation_route(self.fence.cluster_epoch, self.pg_id, self.bucket)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn acquire_write_reservation(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if acquire.name != self.bucket {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket write reservation acquire subject does not match active route"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .acquire_durable_bucket_write_reservation_with_effect_fence(acquire, effect_fence)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn validate_write_reservation(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if proof.bucket != *self.bucket {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket write reservation proof subject does not match active route"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .validate_bucket_write_reservation_proof(proof)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn heartbeat_write_reservation(
        &self,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        effect_fence
            .require_valid_for(self.fence.cluster_epoch)
            .map_err(BucketSnapshotLoadError::Store)
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        if proof.bucket != *self.bucket {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message:
                        "bucket write reservation heartbeat subject does not match active route"
                            .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                proof,
                lease_deadline,
                effect_fence,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_write_drain(
        &self,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if cluster_epoch != self.fence.cluster_epoch {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket write drain begin epoch does not match active route"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .begin_durable_bucket_write_drain_with_effect_fence(
                drain_id,
                owner_token,
                created_at,
                lease_deadline,
                effect_fence,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn clear_expired_write_drain(
        &self,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .clear_expired_durable_bucket_write_drain(now)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn heartbeat_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if record.bucket != *self.bucket {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket write drain heartbeat subject does not match active route"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .heartbeat_durable_bucket_write_drain(record, lease_deadline)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn write_drain_exists(&self) -> Result<bool, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .durable_bucket_write_drain_exists()
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn write_drain(&self) -> Result<Option<BucketWriteDrainRecord>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .durable_bucket_write_drain()
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_bucket_delete_finalize_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if cluster_epoch != self.fence.cluster_epoch {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket delete finalize claim epoch does not match active route"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .acquire_bucket_delete_finalize_claim(
                bucket_incarnation_generation,
                claim_id,
                owner_token,
                claimed_at,
                lease_deadline,
                now,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn bucket_delete_finalize_claim(
        &self,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .bucket_delete_finalize_claim()
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lifecycle_sweep_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        if cluster_epoch != self.fence.cluster_epoch {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "lifecycle sweep claim epoch does not match active route".to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .acquire_lifecycle_sweep_claim(
                bucket_incarnation_generation,
                claim_id,
                owner_token,
                claimed_at,
                lease_deadline,
                now,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn validate_lifecycle_sweep_claim_subject(
        &self,
        claim: &LifecycleSweepClaimRecord,
        operation: &'static str,
    ) -> Result<(), StorageNodeBucketRouteError> {
        if claim.bucket != *self.bucket
            || claim.cluster_epoch != self.fence.cluster_epoch
            || claim.pg_id != self.pg_id.get()
        {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!("{operation} claim does not match active route identity"),
                },
            ));
        }
        Ok(())
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        self.validate_lifecycle_sweep_claim_subject(claim, "lifecycle sweep claim heartbeat")?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .heartbeat_lifecycle_sweep_claim(claim, heartbeat_at, lease_deadline)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        self.validate_lifecycle_sweep_claim_subject(claim, "lifecycle sweep claim error")?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_route(&local_client)?;
        route
            .record_lifecycle_sweep_claim_error(claim, last_error)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeActiveBucketScanRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn map_scan_error(error: BucketSnapshotLoadError) -> StorageNodeBucketRouteError {
        match error {
            BucketSnapshotLoadError::Store(
                error @ StoreError::RouteCapabilitySubjectMismatch { .. },
            ) => StorageNodeBucketRouteError::Route(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
            error => StorageNodeBucketRouteError::Bucket(error),
        }
    }

    fn open_local_bucket_metadata_scan_route<'a>(
        &self,
        client: &'a LocalStorageNodeClient,
    ) -> Result<Box<dyn BucketMetadataScanRoute + 'a>, StorageNodeBucketRouteError> {
        BucketMetadataNodeClient::open_bucket_metadata_scan_route(
            client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .map_err(Self::map_scan_error)
    }

    fn open_local_bucket_write_reservation_scan_route<'a>(
        &self,
        client: &'a LocalStorageNodeClient,
    ) -> Result<Box<dyn BucketWriteReservationScanRoute + 'a>, StorageNodeBucketRouteError> {
        client
            .open_bucket_write_reservation_scan_route(self.fence.cluster_epoch, self.pg_id)
            .map_err(Self::map_scan_error)
    }

    fn load_bucket_execution_generations(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_metadata_scan_route(&local_client)?;
        route
            .load_bucket_execution_generations(buckets)
            .map_err(Self::map_scan_error)
    }

    fn load_bucket_fast_path_identities(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_metadata_scan_route(&local_client)?;
        route
            .load_bucket_fast_path_identities(buckets)
            .map_err(Self::map_scan_error)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_scan_route(&local_client)?;
        route
            .get_bucket_delete_finalize_roots(now, limit)
            .map_err(Self::map_scan_error)
    }

    fn get_bucket_delete_begin_roots(
        &self,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_scan_route(&local_client)?;
        route
            .get_bucket_delete_begin_roots(now, start_after_bucket, limit)
            .map_err(Self::map_scan_error)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_scan_route(&local_client)?;
        route
            .get_lifecycle_sweep_roots(now, limit)
            .map_err(Self::map_scan_error)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = self.open_local_bucket_write_reservation_scan_route(&local_client)?;
        route
            .list_buckets_with_lifecycle()
            .map_err(Self::map_scan_error)
    }
}

impl StorageNodeMetadataReadBucketScanRoute<'_> {
    fn list_buckets(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, StorageNodeBucketRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeBucketRouteError::Route)?;
        self.handler
            .with_metadata_read_pg(
                self.handler.config.node_id,
                self.fence.cluster_epoch,
                self.pg_id.pg_id(),
                |pg| Ok(PgMetadataStore::list_buckets(pg, owner_canonical_id)?),
            )
            .map_err(StorageNodeBucketRouteError::Route)?
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeBucketDeleteReplicaHeadRoute<'_> {
    fn head_bucket(&self) -> Result<BucketInfo, StorageNodeBucketRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeBucketRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let result = BucketMetadataNodeClient::open_bucket_delete_replica_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
            self.bucket,
        )
        .map_err(StorageNodeBucketRouteError::Bucket)?
        .head_bucket_replica_for_delete()
        .map_err(StorageNodeBucketRouteError::Bucket);
        result
    }
}

impl StorageNodeActiveObjectRoute<'_> {
    fn require_valid_now_response(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn require_valid_now(&self) -> Result<(), StorageNodeObjectRouteError> {
        self.require_valid_now_response()
            .map_err(StorageNodeObjectRouteError::Route)
    }

    fn next_version_id(&self) -> Result<s3_types::VersionId, StorageNodeObjectRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        ObjectVersionMetadataNodeClient::open_object_version_metadata_route(
            &local_client,
            self.handler.config.cluster_epoch,
            self.pg_id,
            self.bucket,
            self.key,
        )
        .and_then(|route| route.next_object_version_id())
        .map_err(StorageNodeObjectRouteError::Object)
    }
}

impl StorageNodeMetadataReadObjectRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeObjectRouteError> {
        self.fence
            .validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(StorageNodeObjectRouteError::Route)
    }

    fn with_local_route<T>(
        &self,
        action: impl FnOnce(&PgStore) -> Result<T, ObjectPgActionError>,
    ) -> Result<T, StorageNodeObjectRouteError> {
        self.require_valid_now()?;
        self.handler
            .with_metadata_read_pg(
                self.handler.config.node_id,
                self.fence.cluster_epoch,
                self.pg_id.pg_id(),
                action,
            )
            .map_err(StorageNodeObjectRouteError::Route)?
            .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_object_read_auth_subject(
        &self,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<crate::ObjectReadAuthSubject, StorageNodeObjectRouteError> {
        self.with_local_route(|pg| {
            SharedStorageNode::load_object_read_auth_subject_from_object_pg(
                pg,
                self.bucket,
                self.key,
                version_id,
            )
        })
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &crate::ObjectReadAuthSubjectIdentity,
        snapshot_mode: crate::ObjectReadSnapshotMode,
    ) -> Result<crate::ObjectReadSnapshot, StorageNodeObjectRouteError> {
        self.with_local_route(|pg| {
            SharedStorageNode::load_object_read_snapshot_for_subject_from_object_pg(
                pg,
                self.bucket,
                self.key,
                version_id,
                expected_identity,
                snapshot_mode,
            )
        })
    }

    fn load_bound_multipart_upload(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let upload = PgMetadataStore::get_multipart_upload(pg, upload_id)?;
        if upload.bucket != *bucket || upload.key != *key {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        Ok(upload)
    }

    fn lookup_multipart_upload_management(
        &self,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadManagementLookup, StorageNodeObjectRouteError> {
        self.with_local_route(|pg| {
            match Self::load_bound_multipart_upload(pg, self.bucket, self.key, upload_id) {
                Ok(upload) if upload.state == crate::UploadState::InProgress => {
                    return Ok(crate::MultipartUploadManagementLookup::InProgress(
                        Box::new(upload),
                    ));
                }
                Ok(upload) => {
                    return Ok(crate::MultipartUploadManagementLookup::NonInProgress(
                        Box::new(upload),
                    ));
                }
                Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => {}
                Err(error) => return Err(error),
            }
            if let Some(completed) =
                pg.get_multipart_completion_replay(self.bucket, self.key, upload_id)?
            {
                return Ok(crate::MultipartUploadManagementLookup::Replay(Box::new(
                    completed,
                )));
            }
            Ok(crate::MultipartUploadManagementLookup::Missing)
        })
    }

    fn list_multipart_parts_for_authorized_upload(
        &self,
        authorized_upload: &crate::types::AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<crate::ListedMultipartParts, StorageNodeObjectRouteError> {
        self.with_local_route(|pg| {
            let upload = Self::load_bound_multipart_upload(
                pg,
                self.bucket,
                self.key,
                &authorized_upload.upload_id,
            )?;
            if upload.state != crate::UploadState::InProgress
                || upload != *authorized_upload.record()
            {
                return Err(MetadataError::NoSuchUpload {
                    upload_id: authorized_upload.upload_id.to_string(),
                }
                .into());
            }
            let response = PgMetadataStore::list_multipart_parts(
                pg,
                &crate::types::ListPartsReq {
                    upload_id: authorized_upload.upload_id.clone(),
                    part_number_marker,
                    max_parts,
                },
            )?;
            crate::ListedMultipartParts::from_storage(upload, response).map_err(|reason| {
                MetadataError::InvariantViolation {
                    context: "project multipart parts listing",
                    reason: reason.to_string(),
                }
                .into()
            })
        })
    }
}

impl StorageNodeActivePrimaryObjectRoute<'_> {
    fn payload_reclaim_exists(
        &self,
        generation_id: GenerationId,
    ) -> Result<bool, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            generation_id,
        )
        .and_then(|route| route.exists())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_object_payload_reclaim(
        &self,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, StorageNodeObjectPayloadReclaimRouteError>
    {
        self.route
            .require_valid_now_response()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            generation_id,
        )
        .map_err(object_payload_reclaim_route_open_error)?;
        route
            .load_payload()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_object_payload_reclaim_claim(
        &self,
        bucket_incarnation_generation: u64,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, StorageNodeObjectPayloadReclaimRouteError>
    {
        self.route
            .require_valid_now_response()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        effect_fence
            .require_valid_for(self.route.fence.cluster_epoch)
            .map_err(BucketSnapshotLoadError::Store)
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)?;
        let effect_fence = self
            .route
            .fence
            .intersect_effect_fence(effect_fence)
            .map_err(BucketSnapshotLoadError::Store)
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            generation_id,
        )
        .map_err(object_payload_reclaim_route_open_error)?;
        let expected_payload = route
            .load_payload()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)?;
        let Some(expected_payload) = expected_payload else {
            return Ok(None);
        };
        if expected_payload.kind() != reclaim_kind {
            return Err(StorageNodeObjectPayloadReclaimRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "object payload reclaim claim kind does not match the routed root"
                        .to_string(),
                },
            ));
        }
        route
            .acquire_claim(
                AcquireObjectPayloadReclaimClaimReq {
                    reclaim_kind,
                    bucket_incarnation_generation,
                    claim_id,
                    owner_token,
                    claimed_at,
                    lease_deadline,
                    now,
                },
                effect_fence,
            )
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)
    }

    fn build_delete_object_payload_reclaim_command(
        &self,
        generation_id: GenerationId,
        payload: &ObjectPayloadReclaimCommand,
        claim: &ObjectPayloadReclaimClaimRecord,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let route = ObjectMutationMetadataNodeClient::open_object_payload_reclaim_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            generation_id,
        )
        .map_err(StorageNodeObjectRouteError::Object)?;
        route
            .build_delete_object_payload_reclaim_command(
                BuildDeleteObjectPayloadReclaimCommandReq { payload, claim },
                effect_fence,
            )
            .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_create_stream_upload_subject(
        &self,
        request: &CreateStreamUploadReq,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if request.bucket != *self.route.bucket || request.key != *self.route.key {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} create request subject does not match active object route"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn matching_stream_upload_exists(
        &self,
        request: &CreateStreamUploadReq,
        expected_command: Option<&crate::metadata_command::CreateStreamUploadCommand>,
    ) -> Result<bool, StorageNodeObjectRouteError> {
        self.require_create_stream_upload_subject(request, "stream upload match")?;
        if let Some(command) = expected_command {
            if !command.matches_request(request) {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "stream upload match expected command subject does not match active object route or request".to_string(),
                    },
                ));
            }
            let expected_operation_kind = match request.target {
                StreamUploadTarget::PutObject => {
                    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                }
                StreamUploadTarget::UploadPart { .. } => {
                    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                }
            };
            self.require_object_mutation_proof(
                &command.bucket_write_reservation,
                expected_operation_kind,
                "stream upload match",
            )?;
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_upload_creation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.matching_stream_upload_exists(request, expected_command))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_stream_upload_session(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_upload_session_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            session_id,
        )
        .and_then(|route| route.load_session())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_stream_put_finalize_snapshot_subject(
        &self,
        session_id: &SessionId,
        snapshot: &crate::StreamPutFinalizeStorageSnapshot,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if snapshot.session.session_id != *session_id
            || snapshot.session.bucket != *self.route.bucket
            || snapshot.session.key != *self.route.key
            || snapshot.session.target != StreamUploadTarget::PutObject
            || snapshot.session.state != crate::StreamUploadState::InProgress
            || snapshot
                .staging_segments
                .iter()
                .any(|segment| segment.session_id != *session_id)
            || snapshot
                .stale_payload_source
                .as_ref()
                .is_some_and(|stored| {
                    stored.bucket() != self.route.bucket || stored.key() != self.route.key
                })
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} snapshot subject does not match active object route"
                    ),
                },
            ));
        }
        let reclaim_matches_route = match snapshot.stale_payload.as_ref() {
            Some(crate::metadata_command::ObjectPayloadReclaimCommand::Segments(reclaim)) => {
                reclaim.bucket == *self.route.bucket && reclaim.key == *self.route.key
            }
            Some(crate::metadata_command::ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
                reclaim.bucket == *self.route.bucket && reclaim.key == *self.route.key
            }
            None => true,
        };
        let reclaim_matches_source = match (
            snapshot.stale_payload.as_ref(),
            snapshot.stale_payload_source.as_ref(),
        ) {
            (None, None) => true,
            (
                Some(crate::metadata_command::ObjectPayloadReclaimCommand::Segments(reclaim)),
                Some(crate::StoredObject::Live(live)),
            ) => {
                live.version_id.is_null()
                    && live.layout == crate::ObjectLayout::Standard
                    && reclaim.generation_id == live.generation_id
            }
            (
                Some(crate::metadata_command::ObjectPayloadReclaimCommand::Multipart(reclaim)),
                Some(crate::StoredObject::Live(live)),
            ) => {
                live.version_id.is_null()
                    && matches!(live.layout, crate::ObjectLayout::MultipartManifest { .. })
                    && reclaim.generation_id == live.generation_id
            }
            _ => false,
        };
        if !reclaim_matches_route || !reclaim_matches_source {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} stale payload does not match active object route or snapshot"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        session_id: &SessionId,
    ) -> Result<crate::StreamPutFinalizeStorageSnapshot, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_put_finalization_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            session_id,
        )
        .and_then(|route| route.load_snapshot())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_stream_put_commit_command(
        &self,
        session_id: &SessionId,
        total_size: u64,
        expected_snapshot: &crate::StreamPutFinalizeStorageSnapshot,
        commit: &crate::StreamPutCommitInput,
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_stream_put_finalize_snapshot_subject(
            session_id,
            expected_snapshot,
            "stream PUT commit command build",
        )?;
        self.require_object_mutation_proof(
            bucket_write_reservation,
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            "stream PUT commit command build",
        )?;
        if expected_snapshot.session.bucket_write_reservation.as_ref()
            != Some(bucket_write_reservation)
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "stream PUT commit reservation proof does not match the durable stream session"
                        .to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_put_finalization_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            session_id,
        )
        .and_then(|route| {
            route.build_commit_command(
                BuildStreamPutCommitCommandReq {
                    total_size,
                    expected_snapshot,
                    commit,
                    bucket_write_reservation,
                },
                effect_fence,
            )
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_stream_part_finalize_snapshot_subject(
        &self,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        snapshot: &crate::StreamUploadPartStorageSnapshot,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let auth = &snapshot.auth_snapshot;
        if auth.session.session_id != *session_id
            || auth.session.bucket != *self.route.bucket
            || auth.session.key != *self.route.key
            || auth.session.target
                != (StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                })
            || auth.session.state != crate::StreamUploadState::InProgress
            || auth.upload.upload_id != *upload_id
            || auth.upload.bucket != *self.route.bucket
            || auth.upload.key != *self.route.key
            || auth.upload.state != crate::UploadState::InProgress
            || auth
                .staging_segments
                .iter()
                .any(|segment| segment.session_id != *session_id)
            || snapshot
                .existing_part
                .as_ref()
                .is_some_and(|part| part.upload_id != *upload_id || part.part_number != part_number)
            || snapshot.displaced_segments.iter().any(|segment| {
                segment.bucket != *self.route.bucket
                    || segment.key != *self.route.key
                    || segment.upload_id != *upload_id
                    || segment.part_number != part_number
            })
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} snapshot subject does not match active object route or part target"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn load_stream_part_finalize_snapshot(
        &self,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<crate::StreamUploadPartStorageSnapshot, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_part_finalization_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            upload_id,
            session_id,
            part_number,
        )
        .and_then(|route| route.load_snapshot())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_stream_part_commit_command(
        &self,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        expected_snapshot: &crate::StreamUploadPartStorageSnapshot,
        part: &crate::MultipartPartRecord,
        segments: &[crate::MultipartPartSegmentRecord],
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_stream_part_finalize_snapshot_subject(
            upload_id,
            session_id,
            part_number,
            expected_snapshot,
            "stream part commit command build",
        )?;
        if part.upload_id != *upload_id
            || part.part_number != part_number
            || segments.iter().any(|segment| {
                segment.bucket != *self.route.bucket
                    || segment.key != *self.route.key
                    || segment.upload_id != *upload_id
                    || segment.part_number != part_number
            })
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "stream part commit payload does not match active object route or part target".to_string(),
                },
            ));
        }
        self.require_object_mutation_proof(
            bucket_write_reservation,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
            "stream part commit command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_part_finalization_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            upload_id,
            session_id,
            part_number,
        )
        .and_then(|route| {
            route.build_commit_command(
                BuildStreamPartCommitCommandReq {
                    expected_snapshot,
                    part,
                    segments,
                    bucket_write_reservation,
                },
                effect_fence,
            )
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_create_stream_upload_command(
        &self,
        request: &CreateStreamUploadReq,
        cleanup_after: Option<u64>,
        precondition: CreateStreamUploadPrecondition<'_>,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_create_stream_upload_subject(request, "stream upload command build")?;
        let expected_operation_kind = match (&request.target, &precondition) {
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck { .. },
            ) => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObject {
                    expected_current, ..
                },
            ) => {
                if expected_current.is_some_and(|stored| {
                    stored.bucket() != self.route.bucket || stored.key() != self.route.key
                }) {
                    return Err(StorageNodeObjectRouteError::Route(
                        StorageRpcErrorResponse {
                            code: StorageRpcErrorCode::PayloadDecode,
                            message: "stream upload command expected object does not match active object route".to_string(),
                        },
                    ));
                }
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            (
                StreamUploadTarget::UploadPart { upload_id, .. },
                CreateStreamUploadPrecondition::UploadPart { expected_upload },
            ) => {
                if expected_upload.bucket != *self.route.bucket
                    || expected_upload.key != *self.route.key
                    || expected_upload.upload_id != *upload_id
                {
                    return Err(StorageNodeObjectRouteError::Route(
                        StorageRpcErrorResponse {
                            code: StorageRpcErrorCode::PayloadDecode,
                            message: "stream upload command expected multipart upload does not match active object route or target".to_string(),
                        },
                    ));
                }
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            _ => {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "stream upload command precondition does not match target"
                            .to_string(),
                    },
                ));
            }
        };
        self.require_object_mutation_proof(
            bucket_write_reservation,
            expected_operation_kind,
            "stream upload command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_upload_creation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                request,
                cleanup_after,
                precondition,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn update_stream_upload_bucket_write_reservation(
        &self,
        session_id: &SessionId,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), StorageNodeObjectRouteError> {
        effect_fence
            .require_valid_for(self.route.fence.cluster_epoch)
            .map_err(ObjectPgActionError::Store)
            .map_err(StorageNodeObjectRouteError::Object)?;
        self.require_stream_reservation_proof_subject(
            current,
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            "stream upload bucket write reservation update",
        )?;
        self.require_stream_reservation_proof_subject(
            renewed,
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            "stream upload bucket write reservation update",
        )?;
        if !current.has_same_stable_identity(renewed) {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "stream upload bucket write reservation update proofs do not have the same stable identity".to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let route = ObjectMutationMetadataNodeClient::open_stream_upload_session_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            session_id,
        )
        .map_err(StorageNodeObjectRouteError::Object)?;
        let session = route
            .load_session()
            .map_err(StorageNodeObjectRouteError::Object)?;
        if session.target != StreamUploadTarget::PutObject
            || session.bucket_write_reservation.as_ref() != Some(current)
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "stream upload bucket write reservation update does not match the durable PutObject session".to_string(),
                },
            ));
        }
        route
            .update_put_bucket_write_reservation(current, renewed, effect_fence)
            .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_stream_upload_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_stream_upload_session_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            session_id,
        )
        .and_then(|route| route.load_segments())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn prepare_stream_segment_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let (target, mut segment) =
            ObjectMutationMetadataNodeClient::open_stream_upload_session_metadata_route(
                &local_client,
                self.route.fence.cluster_epoch,
                self.route.pg_id,
                self.route.bucket,
                self.route.key,
                &request.session_id,
            )
            .and_then(|route| route.prepare_segment_append(request, effect_fence))
            .map_err(StorageNodeObjectRouteError::Object)?;
        segment.placement_cluster_epoch = self.route.fence.cluster_epoch;
        Ok((target, segment))
    }

    fn require_create_multipart_upload_subject(
        &self,
        request: &CreateMultipartUploadReq,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if request.bucket != *self.route.bucket || request.key != *self.route.key {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} create request subject does not match active object route"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn matching_multipart_upload_initiated_at(
        &self,
        request: &CreateMultipartUploadReq,
        expected_command: Option<&crate::metadata_command::CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, StorageNodeObjectRouteError> {
        self.require_create_multipart_upload_subject(request, "multipart upload match")?;
        if let Some(command) = expected_command {
            if command.upload().bucket != *self.route.bucket
                || command.upload().key != *self.route.key
            {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "multipart upload match expected command subject does not match active object route".to_string(),
                    },
                ));
            }
            if !command.matches_request(request) {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message:
                            "multipart upload match expected command does not match create request"
                                .to_string(),
                    },
                ));
            }
            self.require_object_mutation_proof(
                command.bucket_write_reservation(),
                CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                "multipart upload match",
            )?;
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_upload_creation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.matching_multipart_upload_initiated_at(request, expected_command))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_create_multipart_upload_command(
        &self,
        request: &CreateMultipartUploadReq,
        expected_current: Option<&crate::StoredObject>,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_create_multipart_upload_subject(request, "multipart upload command build")?;
        self.require_object_mutation_proof(
            bucket_write_reservation,
            CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            "multipart upload command build",
        )?;
        if expected_current.is_some_and(|stored| {
            stored.bucket() != self.route.bucket || stored.key() != self.route.key
        }) {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "multipart upload command expected object does not match active object route".to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_upload_creation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_create_multipart_upload_command(BuildCreateMultipartUploadCommandReq {
                request,
                expected_current,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_authorized_multipart_upload_subject(
        &self,
        authorized_upload: &crate::types::MultipartUploadRecord,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if authorized_upload.bucket != *self.route.bucket
            || authorized_upload.key != *self.route.key
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} authorized multipart upload subject does not match active object route"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn load_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, StorageNodeMultipartUploadRouteError> {
        self.route
            .require_valid_now_response()
            .map_err(StorageNodeMultipartUploadRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let lookup_route =
            ObjectMutationMetadataNodeClient::open_multipart_upload_lookup_metadata_route(
                &local_client,
                self.route.fence.cluster_epoch,
                self.route.pg_id,
                self.route.bucket,
                self.route.key,
            )
            .map_err(|error| match error {
                ObjectPgActionError::Store(store) => BucketSnapshotLoadError::Store(store),
                ObjectPgActionError::Metadata(metadata) => {
                    BucketSnapshotLoadError::Metadata(metadata)
                }
                other => BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "open multipart upload lookup metadata route",
                    source: std::io::Error::other(other.to_string()),
                }),
            })
            .map_err(StorageNodeMultipartUploadRouteError::Upload)?;
        lookup_route
            .load_multipart_upload(upload_id)
            .map_err(StorageNodeMultipartUploadRouteError::Upload)
    }

    fn load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_upload_lookup_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_in_progress_multipart_upload(upload_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_upload_lookup_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_in_progress_multipart_upload_for_listing(upload_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: &crate::types::AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<crate::MultipartCompletionSnapshot, StorageNodeObjectRouteError> {
        self.require_authorized_multipart_upload_subject(
            authorized_upload.record(),
            "multipart completion snapshot load",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_authorized_multipart_upload_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            authorized_upload,
        )
        .and_then(|route| route.load_multipart_completion_snapshot(requested_part_numbers))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_multipart_completion_preflight(
        &self,
        authorized_upload: &crate::types::AuthorizedMultipartUploadRecord,
    ) -> Result<crate::MultipartCompletionPreflight, StorageNodeObjectRouteError> {
        self.require_authorized_multipart_upload_subject(
            authorized_upload.record(),
            "multipart completion preflight load",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_authorized_multipart_upload_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            authorized_upload,
        )
        .and_then(|route| route.load_multipart_completion_preflight())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_multipart_completion_stale_payload_source(
        &self,
    ) -> Result<Option<crate::StoredObject>, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_completion_mutation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_stale_payload_source())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_abort_multipart_upload_cleanup(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<crate::AbortMultipartUploadCleanup>, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            upload_id,
        )
        .and_then(|route| route.load_cleanup())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_terminal_stream_cleanup_subject(
        &self,
        upload_id: &UploadId,
        stream_uploads: &[crate::TerminalStreamCleanupRecord],
        stream_upload_segments: &[StreamUploadSegmentRecord],
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        for stream in stream_uploads {
            let matches_upload = matches!(
                &stream.target,
                StreamUploadTarget::UploadPart {
                    upload_id: stream_upload_id,
                    ..
                } if stream_upload_id == upload_id
            );
            if stream.bucket != *self.route.bucket
                || stream.key != *self.route.key
                || !matches_upload
            {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: format!(
                            "{operation} stream cleanup does not match active object route or upload"
                        ),
                    },
                ));
            }
        }
        if stream_upload_segments.iter().any(|segment| {
            !stream_uploads
                .iter()
                .any(|stream| stream.session_id == segment.session_id)
        }) {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} stream segment cleanup does not match a routed stream session"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn require_abort_multipart_cleanup_subject(
        &self,
        upload_id: &UploadId,
        cleanup: &crate::AbortMultipartUploadCleanup,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        if cleanup.upload.bucket != *self.route.bucket
            || cleanup.upload.key != *self.route.key
            || cleanup.upload.upload_id != *upload_id
            || cleanup
                .parts
                .iter()
                .any(|part| part.upload_id != *upload_id)
            || cleanup.streaming_segments.iter().any(|segment| {
                segment.bucket != *self.route.bucket
                    || segment.key != *self.route.key
                    || segment.upload_id != *upload_id
            })
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} cleanup does not match active object route or upload"
                    ),
                },
            ));
        }
        self.require_terminal_stream_cleanup_subject(
            upload_id,
            &cleanup.stream_uploads,
            &cleanup.stream_upload_segments,
            operation,
        )
    }

    fn require_complete_multipart_subject(
        &self,
        request: &crate::CompleteMultipartCommitRequest,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if request.bucket != *self.route.bucket
            || request.key != *self.route.key
            || request
                .part_records
                .iter()
                .chain(request.expected_cleanup.omitted_parts.iter())
                .any(|part| part.upload_id != request.upload_id)
            || request
                .selected_streaming_segments
                .iter()
                .chain(request.expected_cleanup.omitted_streaming_segments.iter())
                .any(|segment| {
                    segment.bucket != *self.route.bucket
                        || segment.key != *self.route.key
                        || segment.upload_id != request.upload_id
                })
            || request
                .expected_stale_payload_source
                .as_ref()
                .is_some_and(|stored| {
                    !stored.version_id().is_null()
                        || stored.as_live().is_none()
                        || stored.bucket() != self.route.bucket
                        || stored.key() != self.route.key
                })
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} request does not match active object route or upload"
                    ),
                },
            ));
        }
        self.require_terminal_stream_cleanup_subject(
            &request.upload_id,
            &request.expected_cleanup.stream_uploads,
            &request.expected_cleanup.stream_upload_segments,
            operation,
        )
    }

    fn build_complete_multipart_object_command(
        &self,
        request: &crate::CompleteMultipartCommitRequest,
        version_id: s3_types::VersionId,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_complete_multipart_subject(request, "complete multipart command build")?;
        self.require_object_mutation_proof(
            bucket_write_reservation,
            COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            "complete multipart command build",
        )?;
        let expected_object_parts = complete_multipart_expected_object_parts(
            request,
            version_id,
            self.route.handler.node.pg_topology(),
        );
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_completion_mutation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_complete_multipart_object_command(BuildCompleteMultipartObjectCommandReq {
                request,
                version_id,
                expected_object_parts: &expected_object_parts,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_abort_multipart_upload_command(
        &self,
        upload_id: &UploadId,
        expected_cleanup: Option<&crate::AbortMultipartUploadCleanup>,
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, StorageNodeObjectRouteError> {
        self.require_object_mutation_proof(
            bucket_write_reservation,
            ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            "abort multipart command build",
        )?;
        if let Some(cleanup) = expected_cleanup {
            self.require_abort_multipart_cleanup_subject(
                upload_id,
                cleanup,
                "abort multipart command build",
            )?;
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            upload_id,
        )
        .and_then(|route| {
            route.build_abort_multipart_upload_command(
                BuildAbortMultipartUploadCommandReq {
                    expected_cleanup,
                    bucket_write_reservation,
                },
                effect_fence,
            )
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        authorized_upload: &crate::types::AuthorizedMultipartUploadAbort,
        expected_cleanup: Option<&crate::AbortMultipartUploadCleanup>,
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, StorageNodeObjectRouteError> {
        self.require_authorized_multipart_upload_subject(
            authorized_upload.record(),
            "authorized abort multipart command build",
        )?;
        self.require_object_mutation_proof(
            bucket_write_reservation,
            ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            "authorized abort multipart command build",
        )?;
        if let Some(cleanup) = expected_cleanup {
            if cleanup.upload != *authorized_upload.record() {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message:
                            "authorized abort multipart cleanup does not match authorized upload"
                                .to_string(),
                    },
                ));
            }
            self.require_abort_multipart_cleanup_subject(
                &authorized_upload.record().upload_id,
                cleanup,
                "authorized abort multipart command build",
            )?;
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_multipart_abort_mutation_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
            &authorized_upload.record().upload_id,
        )
        .and_then(|route| {
            route.build_authorized_abort_multipart_upload_command(
                BuildAuthorizedAbortMultipartUploadCommandReq {
                    authorized_upload,
                    expected_cleanup,
                    bucket_write_reservation,
                },
                effect_fence,
            )
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn require_object_mutation_proof(
        &self,
        bucket_write_reservation: &BucketWriteReservationProof,
        expected_operation_kind: &'static str,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if bucket_write_reservation.bucket != *self.route.bucket
            || bucket_write_reservation.cluster_epoch != self.route.fence.cluster_epoch
            || bucket_write_reservation.operation_kind != expected_operation_kind
            || bucket_write_reservation.target_context.as_deref() != Some(self.route.key.as_str())
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} bucket write reservation proof does not match the active object route, epoch, or operation"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn require_stream_reservation_proof_subject(
        &self,
        bucket_write_reservation: &BucketWriteReservationProof,
        expected_operation_kind: &'static str,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if bucket_write_reservation.bucket != *self.route.bucket
            || bucket_write_reservation.operation_kind != expected_operation_kind
            || bucket_write_reservation.target_context.as_deref() != Some(self.route.key.as_str())
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "{operation} bucket write reservation proof does not match the active object subject or operation"
                    ),
                },
            ));
        }
        Ok(())
    }

    fn next_generation_id(&self) -> Result<GenerationId, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectGenerationMetadataNodeClient::open_object_generation_metadata_route(
            &local_client,
            self.route.handler.config.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.next_object_generation_id())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn generation_reservation(
        &self,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectGenerationMetadataNodeClient::open_object_generation_metadata_route(
            &local_client,
            self.route.handler.config.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.object_generation_reservation(reservation_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_put_object_metadata_snapshot(
        &self,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<crate::StoredObject, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_put_object_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_put_object_metadata_snapshot(version_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_current_object_delete_snapshot(
        &self,
    ) -> Result<ObjectDeleteStorageSnapshot, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_current_object_delete_snapshot())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_specific_object_delete_snapshot(
        &self,
        version_id: s3_types::VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_specific_object_delete_snapshot(version_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn list_object_versions_for_lifecycle(
        &self,
    ) -> Result<Vec<crate::StoredObject>, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.list_object_versions_for_lifecycle())
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_put_object_metadata_command(
        &self,
        requested_version_id: Option<s3_types::VersionId>,
        expected_stored: &crate::StoredObject,
        version_id: s3_types::VersionId,
        mutation: PutObjectMetadataMutation,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_object_mutation_proof(
            bucket_write_reservation,
            PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
            "object metadata PUT command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_put_object_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_put_object_metadata_command(BuildPutObjectMetadataCommandReq {
                requested_version_id,
                expected_stored,
                version_id,
                mutation,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_delete_specific_object_version_command(
        &self,
        version_id: s3_types::VersionId,
        expected_stored: Option<&crate::StoredObject>,
        expected_target: Option<&crate::metadata_command::DeleteObjectVersionTarget>,
        expected_version_list: Option<&[crate::StoredObject]>,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<Option<MetadataCommandEnvelope>, StorageNodeObjectRouteError> {
        self.require_object_mutation_proof(
            bucket_write_reservation,
            DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
            "delete-specific object command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_delete_specific_object_version_command(
                BuildDeleteSpecificObjectVersionCommandReq {
                    version_id,
                    expected_stored,
                    expected_target,
                    expected_version_list,
                    bucket_write_reservation,
                },
            )
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_delete_current_object_command(
        &self,
        expected_current: Option<&crate::StoredObject>,
        expected_target: Option<&crate::metadata_command::DeleteObjectVersionTarget>,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<Option<MetadataCommandEnvelope>, StorageNodeObjectRouteError> {
        self.require_object_mutation_proof(
            bucket_write_reservation,
            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
            "delete-current object command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_delete_current_object_command(BuildDeleteCurrentObjectCommandReq {
                expected_current,
                expected_target,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_insert_delete_marker_command(
        &self,
        expected_current: Option<&crate::StoredObject>,
        version_id: s3_types::VersionId,
        owner: &crate::OwnerIdentity,
        stale_payload: InsertDeleteMarkerStalePayload,
        expected_stale_payload_source: Option<&crate::StoredObject>,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.require_object_mutation_proof(
            bucket_write_reservation,
            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
            "insert-delete-marker command build",
        )?;
        let stale_payload_matches_marker_version = match (&stale_payload, version_id) {
            (
                InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { .. },
                s3_types::VersionId::Null,
            ) => match expected_stale_payload_source {
                None => true,
                Some(crate::StoredObject::Live(record)) => {
                    record.bucket == *self.route.bucket
                        && record.key == *self.route.key
                        && record.version_id.is_null()
                }
                Some(crate::StoredObject::DeleteMarker(_)) => false,
            },
            (InsertDeleteMarkerStalePayload::Explicit(None), s3_types::VersionId::Versioned(_)) => {
                expected_stale_payload_source.is_none()
            }
            (InsertDeleteMarkerStalePayload::Explicit(Some(_)), _)
            | (InsertDeleteMarkerStalePayload::Explicit(None), s3_types::VersionId::Null)
            | (
                InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { .. },
                s3_types::VersionId::Versioned(_),
            ) => false,
        };
        if !stale_payload_matches_marker_version {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "insert-delete-marker stale payload mode does not match the marker version or expected null live-object source".to_string(),
                },
            ));
        }
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_delete_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_insert_delete_marker_command(BuildInsertDeleteMarkerCommandReq {
                expected_current,
                version_id,
                owner,
                stale_payload,
                expected_stale_payload_source,
                bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn load_direct_put_commit_snapshot(
        &self,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<crate::DirectPutCommitStorageSnapshot, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        DirectPutMetadataNodeClient::open_direct_put_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| route.load_direct_put_commit_snapshot(reservation_id, generation_id))
        .map_err(StorageNodeObjectRouteError::Object)
    }

    fn build_direct_put_commit_command(
        &self,
        request: &crate::CommitDirectPutObjectReq,
        version_id: s3_types::VersionId,
        expected_snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<MetadataCommandEnvelope, StorageNodeObjectRouteError> {
        self.route.require_valid_now()?;
        if request.bucket != *self.route.bucket
            || request.key != *self.route.key
            || request.bucket_write_reservation.bucket != *self.route.bucket
        {
            return Err(StorageNodeObjectRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "direct PUT command request does not match active object route"
                        .to_string(),
                },
            ));
        }
        self.require_object_mutation_proof(
            &request.bucket_write_reservation,
            PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            "direct PUT command build",
        )?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        DirectPutMetadataNodeClient::open_direct_put_metadata_route(
            &local_client,
            self.route.fence.cluster_epoch,
            self.route.pg_id,
            self.route.bucket,
            self.route.key,
        )
        .and_then(|route| {
            route.build_direct_put_commit_command(BuildDirectPutCommitCommandReq {
                request,
                version_id,
                expected_snapshot,
                bucket_write_reservation: &request.bucket_write_reservation,
            })
        })
        .map_err(StorageNodeObjectRouteError::Object)
    }
}

impl StorageNodeMetadataReadObjectScanRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn validate_listing_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation: &'static str,
    ) -> Result<(), StorageNodeBucketRouteError> {
        self.handler
            .validate_pg_for_object(self.pg_id.pg_id(), bucket, key, operation)
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn map_listing_error(error: BucketSnapshotLoadError) -> StorageNodeBucketRouteError {
        match error {
            BucketSnapshotLoadError::Store(
                error @ StoreError::RouteCapabilitySubjectMismatch { .. },
            ) => StorageNodeBucketRouteError::Route(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
            error => StorageNodeBucketRouteError::Bucket(error),
        }
    }

    fn with_local_route<T>(
        &self,
        action: impl FnOnce(&PgStore) -> Result<T, BucketSnapshotLoadError>,
    ) -> Result<T, StorageNodeBucketRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeBucketRouteError::Route)?;
        self.handler
            .with_metadata_read_pg(
                self.handler.config.node_id,
                self.fence.cluster_epoch,
                self.pg_id.pg_id(),
                action,
            )
            .map_err(StorageNodeBucketRouteError::Route)?
            .map_err(Self::map_listing_error)
    }

    fn list_objects_page(
        &self,
        request: &crate::ListObjectsReq,
    ) -> Result<crate::ListObjectsResp, StorageNodeBucketRouteError> {
        let response = self.with_local_route(|pg| Ok(pg.list_objects(request)?))?;
        for object in &response.objects {
            self.validate_listing_subject(
                object.bucket(),
                object.key(),
                "object listing response scan PG",
            )?;
        }
        Ok(response)
    }

    fn list_object_versions_page(
        &self,
        request: &crate::ListObjectVersionsReq,
    ) -> Result<crate::ListObjectVersionsResp, StorageNodeBucketRouteError> {
        let response = self.with_local_route(|pg| Ok(pg.list_object_versions(request)?))?;
        for object in &response.versions {
            self.validate_listing_subject(
                object.bucket(),
                object.key(),
                "object version listing response scan PG",
            )?;
        }
        Ok(response)
    }

    fn list_multipart_uploads_page(
        &self,
        request: &crate::ListMultipartUploadsReq,
    ) -> Result<crate::ListMultipartUploadsResp, StorageNodeBucketRouteError> {
        let response = self.with_local_route(|pg| Ok(pg.list_multipart_uploads(request)?))?;
        for upload in &response.uploads {
            self.validate_listing_subject(
                &upload.bucket,
                &upload.key,
                "multipart upload listing response scan PG",
            )?;
        }
        Ok(response)
    }
}

impl StorageNodeActivePrimaryObjectScanRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn map_reclaim_scan_error(
        error: BucketSnapshotLoadError,
    ) -> StorageNodeObjectPayloadReclaimRouteError {
        match error {
            BucketSnapshotLoadError::Store(
                error @ StoreError::RouteCapabilitySubjectMismatch { .. },
            ) => StorageNodeObjectPayloadReclaimRouteError::Route(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
            error => StorageNodeObjectPayloadReclaimRouteError::Reclaim(error),
        }
    }

    fn map_object_scan_error(error: ObjectPgActionError) -> StorageNodeObjectRouteError {
        match error {
            ObjectPgActionError::Store(
                error @ StoreError::RouteCapabilitySubjectMismatch { .. },
            ) => StorageNodeObjectRouteError::Route(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
            error => StorageNodeObjectRouteError::Object(error),
        }
    }

    fn validate_root(
        &self,
        root: &PayloadReclaimRoot,
        expected_bucket: Option<&BucketName>,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectPayloadReclaimRouteError> {
        if expected_bucket.is_some_and(|bucket| bucket != &root.bucket) {
            return Err(StorageNodeObjectPayloadReclaimRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!("{operation} root bucket does not match request"),
                },
            ));
        }
        self.handler
            .validate_pg_for_object(self.pg_id.pg_id(), &root.bucket, &root.key, operation)
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)
    }

    fn bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, StorageNodeObjectPayloadReclaimRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let root = ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .map_err(object_payload_reclaim_route_open_error)?
        .get_bucket_payload_reclaim_root(bucket)
        .map_err(Self::map_reclaim_scan_error)?;
        if let Some(root) = &root {
            self.validate_root(root, Some(bucket), "object bucket payload reclaim root")?;
        }
        Ok(root)
    }

    fn payload_reclaim_root(
        &self,
    ) -> Result<Option<PayloadReclaimRoot>, StorageNodeObjectPayloadReclaimRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let root = ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .map_err(object_payload_reclaim_route_open_error)?
        .get_payload_reclaim_root()
        .map_err(Self::map_reclaim_scan_error)?;
        if let Some(root) = &root {
            self.validate_root(root, None, "object payload reclaim root")?;
        }
        Ok(root)
    }

    fn object_payload_reclaim_claim(
        &self,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, StorageNodeObjectPayloadReclaimRouteError>
    {
        self.require_valid_now()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let claim = ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .map_err(object_payload_reclaim_route_open_error)?
        .object_payload_reclaim_claim()
        .map_err(Self::map_reclaim_scan_error)?;
        if let Some(claim) = &claim {
            if claim.pg_id != self.pg_id.get() {
                return Err(StorageNodeObjectPayloadReclaimRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "object payload reclaim claim does not match scan PG".to_string(),
                    },
                ));
            }
            self.handler
                .validate_pg_for_object(
                    self.pg_id.pg_id(),
                    &claim.bucket,
                    &claim.key,
                    "object payload reclaim claim get",
                )
                .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        }
        Ok(claim)
    }

    fn list_aborting_multipart_upload_bucket_witnesses(
        &self,
    ) -> Result<Vec<crate::types::AbortingMultipartUploadBucketWitness>, StorageNodeObjectRouteError>
    {
        self.require_valid_now()
            .map_err(StorageNodeObjectRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .and_then(|route| route.list_aborting_multipart_upload_bucket_witnesses())
        .map_err(Self::map_object_scan_error)
    }

    fn validate_stream_uploads(
        &self,
        page: &crate::StreamUploadRecordPage,
        expected_bucket: Option<&BucketName>,
        operation: &'static str,
    ) -> Result<(), StorageNodeObjectRouteError> {
        for upload in &page.uploads {
            if expected_bucket.is_some_and(|bucket| bucket != &upload.bucket) {
                return Err(StorageNodeObjectRouteError::Route(
                    StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: format!("{operation} upload bucket does not match request"),
                    },
                ));
            }
            self.handler
                .validate_pg_for_object(self.pg_id.pg_id(), &upload.bucket, &upload.key, operation)
                .map_err(StorageNodeObjectRouteError::Route)?;
        }
        Ok(())
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<crate::StreamUploadRecordPage, StorageNodeObjectRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeObjectRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let page = ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .and_then(|route| {
            route.list_stream_uploads_for_bucket_page(bucket, session_id_marker, limit)
        })
        .map_err(Self::map_object_scan_error)?;
        self.validate_stream_uploads(&page, Some(bucket), "object stream uploads list")?;
        Ok(page)
    }

    fn list_all_stream_uploads_page(
        &self,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<crate::StreamUploadRecordPage, StorageNodeObjectRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeObjectRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let page = ObjectMutationMetadataNodeClient::open_object_mutation_scan_metadata_route(
            &local_client,
            self.fence.cluster_epoch,
            self.pg_id,
        )
        .and_then(|route| route.list_all_stream_uploads_page(session_id_marker, limit))
        .map_err(Self::map_object_scan_error)?;
        self.validate_stream_uploads(&page, None, "object stream uploads PG list")?;
        Ok(page)
    }

    fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<crate::types::ShardScavengerPayloadReference>, StorageNodeObjectScanStoreError>
    {
        self.require_valid_now()
            .map_err(StorageNodeObjectScanStoreError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        ShardScavengerNodeClient::open_shard_scavenger_object_scan_route(
            &local_client,
            self.handler.config.cluster_epoch,
            self.pg_id,
        )
        .and_then(|route| route.list_shard_scavenger_payload_references())
        .map_err(StorageNodeObjectScanStoreError::Store)
    }

    fn list_placed_segment_backfill_reference_page(
        &self,
        after: Option<&crate::types::PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<crate::types::PlacedSegmentBackfillReferencePage, StorageNodeObjectScanStoreError>
    {
        self.require_valid_now()
            .map_err(StorageNodeObjectScanStoreError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        ShardScavengerNodeClient::open_shard_scavenger_object_scan_route(
            &local_client,
            self.handler.config.cluster_epoch,
            self.pg_id,
        )
        .and_then(|route| route.list_placed_segment_backfill_reference_page(after, limit))
        .map_err(StorageNodeObjectScanStoreError::Store)
    }
}

impl StorageNodeActiveDataScanRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn list_shard_files(
        &self,
    ) -> Result<crate::node_runtime::pg_store::ScavengerShardFileScan, StorageNodeDataRouteError>
    {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        local_client
            .open_shard_scavenger_data_route(self.handler.config.cluster_epoch, self.pg_id)
            .and_then(|route| route.list_scavenger_shard_files())
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeRetainedBucketWriteReservationRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        self.handler
            .validate_retained_bucket_write_reservation_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                self.record,
                "bucket write reservation release",
            )
            .map(|_| ())
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn release(self) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route =
            RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route(
                &local_client,
                self.pg_id,
                &self.record.bucket,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .release_durable_bucket_write_reservation(self.record)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeRetainedMetadataCommandProofRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        if self.route_cluster_epoch != self.proof.cluster_epoch {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: format!(
                        "metadata command bucket write proof release route epoch {} does not match proof epoch {}",
                        self.route_cluster_epoch.get(),
                        self.proof.cluster_epoch.get()
                    ),
                },
            ));
        }
        self.handler
            .validate_retained_bucket_write_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                &self.proof.bucket,
                "metadata command bucket write proof release",
            )
            .map(|_| ())
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn release(self) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route =
            RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route(
                &local_client,
                self.pg_id,
                &self.proof.bucket,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .release_metadata_command_bucket_write_reservation(self.proof)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }

    fn release_until(self, deadline: Instant) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        crate::node_client::require_metadata_command_operation_deadline(deadline)
            .map_err(BucketSnapshotLoadError::Store)
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route_until(
            &local_client,
            self.pg_id,
            &self.proof.bucket,
            deadline,
        )
        .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .release_metadata_command_bucket_write_reservation_until(self.proof, deadline)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeRetainedBucketWriteDrainRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        self.handler
            .validate_retained_bucket_write_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                &self.record.bucket,
                "bucket write drain clear",
            )
            .map(|_| ())
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn clear(self) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route =
            RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route(
                &local_client,
                self.pg_id,
                &self.record.bucket,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .clear_durable_bucket_write_drain(self.record)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeRetainedBucketDeleteFinalizeClaimRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        if self.route_cluster_epoch != self.claim.cluster_epoch
            || self.raw_pg_id.get() != self.claim.pg_id
        {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "bucket delete finalize claim does not match retained route identity"
                        .to_string(),
                },
            ));
        }
        self.handler
            .validate_retained_bucket_write_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                &self.claim.bucket,
                "bucket delete finalize claim release",
            )
            .map(|_| ())
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn release(self) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route =
            RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route(
                &local_client,
                self.pg_id,
                &self.claim.bucket,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .release_bucket_delete_finalize_claim(self.claim)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeRetainedLifecycleSweepClaimRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageNodeBucketRouteError> {
        if self.route_cluster_epoch != self.claim.cluster_epoch
            || self.raw_pg_id.get() != self.claim.pg_id
        {
            return Err(StorageNodeBucketRouteError::Route(
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "lifecycle sweep claim does not match retained route identity"
                        .to_string(),
                },
            ));
        }
        self.handler
            .validate_retained_bucket_write_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                &self.claim.bucket,
                "lifecycle sweep claim release",
            )
            .map(|_| ())
            .map_err(StorageNodeBucketRouteError::Route)
    }

    fn release(self) -> Result<(), StorageNodeBucketRouteError> {
        self.require_valid_now()?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route =
            RetainedBucketWriteReservationNodeClient::open_retained_bucket_write_reservation_route(
                &local_client,
                self.pg_id,
                &self.claim.bucket,
            )
            .map_err(StorageNodeBucketRouteError::Bucket)?;
        route
            .release_lifecycle_sweep_claim(self.claim)
            .map_err(StorageNodeBucketRouteError::Bucket)
    }
}

impl StorageNodeRetainedObjectPayloadReclaimClaimRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.handler
            .validate_retained_object_payload_reclaim_claim_route(
                self.route_permit,
                self.node_id,
                self.route_cluster_epoch,
                self.raw_pg_id,
                self.claim,
                "object payload reclaim claim release",
            )?;
        Ok(())
    }

    fn release(self) -> Result<(), StorageNodeObjectPayloadReclaimRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        let route = RetainedObjectMutationMetadataNodeClient::open_retained_object_mutation_route(
            &local_client,
            self.pg_id,
            self.route_cluster_epoch,
            &self.claim.bucket,
            &self.claim.key,
        )
        .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)?;
        route
            .release_object_payload_reclaim_claim(self.claim)
            .map_err(StorageNodeObjectPayloadReclaimRouteError::Reclaim)
    }
}

impl StorageNodeActiveShardRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn write_if_absent(
        &self,
        payload: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        effect_fence
            .require_valid_for(self.location.cluster_epoch())
            .map_err(|error| StorageNodeDataRouteError::Route(store_error_response(error)))?;
        self.handler
            .node
            .write_shard_file_if_absent(self.location.data_pg_id().get(), self.shard_key, payload)
            .map_err(StorageNodeDataRouteError::Store)
    }

    fn repair_write(&self, payload: &[u8]) -> Result<WriteAck, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .write_shard_file(self.location.data_pg_id().get(), self.shard_key, payload)
            .map_err(StorageNodeDataRouteError::Store)
    }

    fn read(&self) -> Result<Vec<u8>, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .read_shard_file(self.location.data_pg_id().get(), self.shard_key)
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeActiveReadHandleAcquireRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn acquire(self) -> Result<Vec<ShardLocation>, StorageRpcErrorResponse> {
        self.require_valid_now()?;
        self.session
            .acquire_read_handles(ValidatedReadHandleAcquireRequest {
                read_operation_id: self.read_operation_id,
                entries: self.entries,
            })
    }
}

impl StorageNodeActivePrimaryDataRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }

    fn list_scavenger_shard_rows(
        &self,
    ) -> Result<Vec<crate::node_runtime::pg_store::ScavengerShardRow>, StorageNodeDataRouteError>
    {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.handler.config.node_id,
            Arc::clone(&self.handler.node),
        );
        local_client
            .open_shard_scavenger_data_route(self.handler.config.cluster_epoch, self.pg_id)
            .and_then(|route| route.list_scavenger_shard_rows())
            .map_err(StorageNodeDataRouteError::Store)
    }

    fn record_shard_acks(
        &self,
        items: &[StorageRpcShardAckItem],
    ) -> Result<(), StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        let shard_batch: Vec<(&ShardKey, WriteAck)> = items
            .iter()
            .map(|item| (&item.shard_key, item.ack))
            .collect();
        self.handler
            .node
            .get_pg(self.pg_id.get())
            .and_then(|pg| pg.register_written_shards_batch_exact(&shard_batch))
            .map_err(StorageNodeDataRouteError::Store)
    }

    fn validate_shard_acks(
        &self,
        items: &[StorageRpcShardAckItem],
    ) -> Result<(), StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .validate_shard_ack_batch(self.pg_id.pg_id(), items)
            .map_err(StorageNodeDataRouteError::Store)
    }

    fn load_shard_ack(&self, shard_key: &ShardKey) -> Result<WriteAck, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .get_pg(self.pg_id.get())
            .and_then(|pg| {
                let stat = pg.stat_shard(shard_key)?;
                Ok(WriteAck {
                    crc64: stat.crc64,
                    stored_size: stat.size,
                })
            })
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeActiveObjectPayloadLeaseControl<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.handler
            .validate_object_payload_lease_control_admission(
                self.route_permit,
                StorageNodeRouteAdmissionClass::Active,
                self.request,
            )?;
        self.handler
            .validate_node_epoch(self.request.node_id, self.request.route_cluster_epoch)?;
        Self::validate_fence(self.fence)
    }

    fn validate_fence(fence: StorageNodeRouteFence) -> Result<(), StorageRpcErrorResponse> {
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )
    }
}

impl StorageNodeRetainedObjectPayloadLeaseControl<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.handler
            .validate_object_payload_lease_control_admission(
                self.route_permit,
                StorageNodeRouteAdmissionClass::RetainedCleanup,
                self.request,
            )?;
        if self.request.node_id != self.handler.config.node_id
            || self.request.route_cluster_epoch > self.handler.config.cluster_epoch
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "object-payload lease cleanup route changed after capability validation"
                    .to_string(),
            });
        }
        StorageNodeConnectionHandler::validate_object_payload_reclaim_authority(self.request)
    }
}

impl StorageNodeRetainedShardPayloadDeleteRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        let pg_id = self.handler.validate_retained_shard_route(
            self.route_permit,
            self.node_id,
            self.route_cluster_epoch,
            self.raw_pg_id,
            self.location.shard_index(),
            self.shard_key,
            "shard payload delete",
        )?;
        if pg_id != self.location.data_pg_id() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "shard payload delete capability changed its validated data PG"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn delete(self) -> Result<(), StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        let _delete_fence = self
            .handler
            .try_begin_shard_delete(self.location, self.shard_key)
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .delete_shard_file(self.location.data_pg_id().get(), self.shard_key)
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeRetainedShardInspectionRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        let pg_id = self.handler.validate_retained_shard_route(
            self.route_permit,
            self.node_id,
            self.route_cluster_epoch,
            self.raw_pg_id,
            self.location.shard_index(),
            self.shard_key,
            "historical shard inspection",
        )?;
        if pg_id != self.location.data_pg_id() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "historical shard inspection capability changed its validated data PG"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn read(&self) -> Result<Vec<u8>, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .read_shard_file(self.location.data_pg_id().get(), self.shard_key)
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeRetainedReadHandleReleaseRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        self.handler
            .validate_retained_cleanup_admission(self.route_permit, "read handle release")
    }

    fn release(self) -> Result<(), StorageRpcErrorResponse> {
        self.require_valid_now()?;
        self.session.release_read_handles(&self.read_operation_id);
        Ok(())
    }
}

impl StorageNodeRetainedShardAckDeleteRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        let pg_id = self.handler.validate_retained_data_route(
            self.route_permit,
            self.node_id,
            self.route_cluster_epoch,
            self.raw_pg_id,
            true,
            "shard ack delete",
        )?;
        if pg_id != self.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "shard ack delete capability changed its validated data PG".to_string(),
            });
        }
        Ok(())
    }

    fn delete(self) -> Result<(), StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .get_pg(self.pg_id.get())
            .and_then(|pg| pg.delete_shard_record(self.shard_key))
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeRetainedShardAckInspectionRoute<'_> {
    fn require_valid_now(&self) -> Result<(), StorageRpcErrorResponse> {
        let pg_id = self.handler.validate_retained_data_route(
            self.route_permit,
            self.node_id,
            self.route_cluster_epoch,
            self.raw_pg_id,
            true,
            "historical shard ack inspection",
        )?;
        if pg_id != self.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "historical shard ack inspection capability changed its validated data PG"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn load(&self) -> Result<WriteAck, StorageNodeDataRouteError> {
        self.require_valid_now()
            .map_err(StorageNodeDataRouteError::Route)?;
        self.handler
            .node
            .get_pg(self.pg_id.get())
            .and_then(|pg| {
                let stat = pg.stat_shard(self.shard_key)?;
                Ok(WriteAck {
                    crc64: stat.crc64,
                    stored_size: stat.size,
                })
            })
            .map_err(StorageNodeDataRouteError::Store)
    }
}

impl StorageNodeRetainedStreamAbortRoute<'_> {
    fn require_valid_now(&self, operation: &'static str) -> Result<(), StorageRpcErrorResponse> {
        self.handler.validate_retained_object_route(
            self.route_permit,
            &StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.raw_pg_id,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            false,
            operation,
        )
    }
}

impl StorageNodeRetainedPrimaryStreamAbortRoute<'_> {
    fn require_valid_now(&self, operation: &'static str) -> Result<(), StorageRpcErrorResponse> {
        self.route.handler.validate_retained_object_route(
            self.route.route_permit,
            &StorageRpcObjectRequest {
                node_id: self.route.node_id,
                cluster_epoch: self.route.route_cluster_epoch,
                pg_id: self.route.raw_pg_id,
                bucket: self.route.bucket.clone(),
                key: self.route.key.clone(),
            },
            true,
            operation,
        )
    }
}

impl StorageNodeRetainedPrimaryStreamAbortSessionRoute<'_> {
    fn prepare(
        self,
    ) -> Result<Option<PreparedRetainedStreamUploadAbort>, StorageNodeObjectRouteError> {
        self.route
            .require_valid_now("retained stream abort prepare")
            .map_err(StorageNodeObjectRouteError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.route.handler.config.node_id,
            Arc::clone(&self.route.route.handler.node),
        );
        let route = RetainedObjectMutationMetadataNodeClient::open_retained_object_mutation_route(
            &local_client,
            self.route.route.pg_id,
            self.route.route.route_cluster_epoch,
            self.route.route.bucket,
            self.route.route.key,
        )
        .map_err(|error| {
            StorageNodeObjectRouteError::Object(match error {
                BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
                BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
            })
        })?;
        route
            .prepare_retained_stream_upload_abort(self.session_id)
            .map_err(StorageNodeObjectRouteError::Object)
    }
}

impl StorageNodeRetainedStreamAbortCommandRoute<'_> {
    fn apply(
        self,
    ) -> Result<
        crate::metadata_command::MetadataCommandReplicaState,
        StorageNodeRetainedStreamAbortApplyError,
    > {
        self.route
            .require_valid_now("retained stream abort apply")
            .map_err(StorageNodeRetainedStreamAbortApplyError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.handler.config.node_id,
            Arc::clone(&self.route.handler.node),
        );
        let route = RetainedMetadataCommandNodeClient::open_retained_stream_upload_abort_route(
            &local_client,
            &self.prepared,
        )
        .map_err(|error| {
            StorageNodeRetainedStreamAbortApplyError::Apply(BucketSnapshotLoadError::Store(error))
        })?;
        route
            .apply()
            .map_err(StorageNodeRetainedStreamAbortApplyError::Apply)
    }
}

impl StorageNodeRetainedPrimaryStreamAbortCommandRoute<'_> {
    fn finish(self) -> Result<bool, StorageNodeRetainedStreamAbortFinishError> {
        self.route
            .require_valid_now("retained stream abort finish")
            .map_err(StorageNodeRetainedStreamAbortFinishError::Route)?;
        let local_client = LocalStorageNodeClient::new(
            self.route.route.handler.config.node_id,
            Arc::clone(&self.route.route.handler.node),
        );
        let route = RetainedMetadataCommandNodeClient::open_retained_stream_upload_abort_route(
            &local_client,
            &self.prepared,
        )
        .map_err(StorageNodeRetainedStreamAbortFinishError::Finish)?;
        route
            .finish()
            .map_err(StorageNodeRetainedStreamAbortFinishError::Finish)
    }
}

macro_rules! metadata_command_pg_guard_or_return {
    ($handler:expr, $session:expr, $pg_id:expr) => {
        match $handler.metadata_command_pg_guard($session, $pg_id) {
            Ok(guard) => guard,
            Err(error) => return encode_storage_rpc_error_response(&error),
        }
    };
}

macro_rules! metadata_mutation_route_guard_or_return {
    ($handler:expr) => {
        if let Err(error) = $handler.validate_metadata_mutation_route_not_expired() {
            return encode_storage_rpc_error_response(&error);
        }
    };
}
