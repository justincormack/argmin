// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

fn trace_storage_rpc_lifecycle(kind: StorageRpcMessageKind) -> bool {
    matches!(
        kind,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope
            | StorageRpcMessageKind::MetadataCommandPendingSlotInsert
            | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
            | StorageRpcMessageKind::MetadataCommandPendingSlotReplace
            | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
    )
}

#[derive(Debug, Default)]
struct StorageNodeActiveSessionState {
    active: usize,
    retained_ordinary: usize,
}

#[derive(Debug, Default)]
struct StorageNodeActiveSessions {
    state: Mutex<StorageNodeActiveSessionState>,
    available: Condvar,
}

impl StorageNodeActiveSessions {
    fn acquire(self: &Arc<Self>, limit: usize) -> StorageNodeActiveSessionGuard {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while !state.try_acquire(limit) {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        StorageNodeActiveSessionGuard {
            active_sessions: Arc::clone(self),
            limit,
            class: StorageNodeActiveSessionClass::Unclassified,
        }
    }

    #[cfg(test)]
    fn try_acquire(self: &Arc<Self>, limit: usize) -> Option<StorageNodeActiveSessionGuard> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.try_acquire(limit) {
            Some(StorageNodeActiveSessionGuard {
                active_sessions: Arc::clone(self),
                limit,
                class: StorageNodeActiveSessionClass::Unclassified,
            })
        } else {
            None
        }
    }

    fn classify_connection(
        &self,
        class: &mut StorageNodeActiveSessionClass,
        limit: usize,
        stateful: bool,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.classify_connection(class, limit, stateful)
    }

    fn release(&self, class: StorageNodeActiveSessionClass) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release(class);
        self.available.notify_one();
    }
}

impl StorageNodeActiveSessionState {
    fn try_acquire(&mut self, limit: usize) -> bool {
        if self.active >= limit {
            return false;
        }
        self.active += 1;
        true
    }

    fn classify_connection(
        &mut self,
        class: &mut StorageNodeActiveSessionClass,
        limit: usize,
        stateful: bool,
    ) -> bool {
        if stateful {
            if *class == StorageNodeActiveSessionClass::RetainedOrdinary {
                self.retained_ordinary = self
                    .retained_ordinary
                    .checked_sub(1)
                    .expect("retained ordinary storage session count underflow");
            }
            *class = StorageNodeActiveSessionClass::Stateful;
            return true;
        }

        if *class == StorageNodeActiveSessionClass::RetainedOrdinary {
            return true;
        }
        let ordinary_limit = limit.saturating_sub(1);
        if self.retained_ordinary >= ordinary_limit {
            return false;
        }
        self.retained_ordinary += 1;
        *class = StorageNodeActiveSessionClass::RetainedOrdinary;
        true
    }

    fn release(&mut self, class: StorageNodeActiveSessionClass) {
        self.active = self
            .active
            .checked_sub(1)
            .expect("storage-node active session release without acquire");
        if class == StorageNodeActiveSessionClass::RetainedOrdinary {
            self.retained_ordinary = self
                .retained_ordinary
                .checked_sub(1)
                .expect("retained ordinary storage session release without classification");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageNodeActiveSessionClass {
    Unclassified,
    RetainedOrdinary,
    Stateful,
}

struct StorageNodeActiveSessionGuard {
    active_sessions: Arc<StorageNodeActiveSessions>,
    limit: usize,
    class: StorageNodeActiveSessionClass,
}

impl StorageNodeActiveSessionGuard {
    fn classify_connection(&mut self, stateful: bool) -> bool {
        self.active_sessions
            .classify_connection(&mut self.class, self.limit, stateful)
    }
}

impl Drop for StorageNodeActiveSessionGuard {
    fn drop(&mut self) {
        self.active_sessions.release(self.class);
    }
}

#[derive(Debug, Default)]
struct StorageNodeReadHandleState {
    handle_counts: BTreeMap<ReadHandleShardKey, usize>,
    delete_fences: BTreeSet<ReadHandleShardKey>,
    live_read_operations: usize,
    live_read_handle_locations: usize,
}

impl StorageNodeReadHandleState {
    fn try_acquire(
        &mut self,
        entries: &[(ShardLocation, ShardKey)],
    ) -> Result<(), StorageRpcErrorResponse> {
        if self.live_read_operations >= STORAGE_NODE_MAX_LIVE_READ_OPERATIONS {
            return Err(resource_exhausted_response(format!(
                "storage-node live read operation limit {} is exhausted",
                STORAGE_NODE_MAX_LIVE_READ_OPERATIONS
            )));
        }
        let live_read_handle_locations = self
            .live_read_handle_locations
            .checked_add(entries.len())
            .ok_or_else(|| {
                resource_exhausted_response(
                    "storage-node live read handle location counter overflowed".to_string(),
                )
            })?;
        if live_read_handle_locations > STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS {
            return Err(resource_exhausted_response(format!(
                "storage-node live read handle location limit {} is exhausted",
                STORAGE_NODE_MAX_LIVE_READ_HANDLE_LOCATIONS
            )));
        }
        for (location, shard_key) in entries {
            let key = ReadHandleShardKey::new(*location, shard_key);
            if self.delete_fences.contains(&key) {
                return Err(shard_delete_in_progress_response(format!(
                    "shard {:?} at {:?} is being deleted",
                    shard_key, location
                )));
            }
        }
        for (location, shard_key) in entries {
            *self
                .handle_counts
                .entry(ReadHandleShardKey::new(*location, shard_key))
                .or_insert(0) += 1;
        }
        self.live_read_operations += 1;
        self.live_read_handle_locations = live_read_handle_locations;
        Ok(())
    }

    fn release(&mut self, entries: &[(ShardLocation, ShardKey)]) {
        self.live_read_operations = self
            .live_read_operations
            .checked_sub(1)
            .expect("read handle operation release without acquire");
        self.live_read_handle_locations = self
            .live_read_handle_locations
            .checked_sub(entries.len())
            .expect("read handle location release without acquire");
        for (location, shard_key) in entries {
            let key = ReadHandleShardKey::new(*location, shard_key);
            let entry = self
                .handle_counts
                .get_mut(&key)
                .expect("read handle release without acquire");
            *entry -= 1;
            if *entry == 0 {
                self.handle_counts.remove(&key);
            }
        }
    }

    #[cfg(test)]
    fn count(&self, location: ShardLocation) -> usize {
        let location_key = ShardLocationKey::from(location);
        self.handle_counts
            .iter()
            .filter(|(key, _)| key.location == location_key)
            .map(|(_, count)| *count)
            .sum()
    }

    fn try_begin_delete(
        &mut self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<(), StorageRpcErrorResponse> {
        let key = ReadHandleShardKey::new(location, shard_key);
        if self.handle_counts.get(&key).copied().unwrap_or(0) > 0 {
            return Err(resource_exhausted_response(format!(
                "shard {:?} at {:?} has active read handles",
                shard_key, location
            )));
        }
        if !self.delete_fences.insert(key) {
            return Err(resource_exhausted_response(format!(
                "shard {:?} at {:?} is already being deleted",
                shard_key, location
            )));
        }
        Ok(())
    }

    fn finish_delete(&mut self, location: ShardLocation, shard_key: &ShardKey) {
        self.delete_fences
            .remove(&ReadHandleShardKey::new(location, shard_key));
    }
}

struct StorageNodeShardDeleteFence {
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    location: ShardLocation,
    shard_key: ShardKey,
}

impl Drop for StorageNodeShardDeleteFence {
    fn drop(&mut self) {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish_delete(self.location, &self.shard_key);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShardLocationKey {
    cluster_epoch: u64,
    data_pg_id: u32,
    shard_index: u8,
    node_id: u32,
}

impl From<ShardLocation> for ShardLocationKey {
    fn from(location: ShardLocation) -> Self {
        Self {
            cluster_epoch: location.cluster_epoch().get(),
            data_pg_id: location.data_pg_id().get(),
            shard_index: location.shard_index().get(),
            node_id: location.node_id().as_u32(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadHandleShardKey {
    location: ShardLocationKey,
    shard_key: ShardKey,
}

impl ReadHandleShardKey {
    fn new(location: ShardLocation, shard_key: &ShardKey) -> Self {
        Self {
            location: ShardLocationKey::from(location),
            shard_key: shard_key.clone(),
        }
    }
}

impl PartialOrd for ReadHandleShardKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadHandleShardKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.location
            .cmp(&other.location)
            .then_with(|| self.shard_key.as_bytes().cmp(other.shard_key.as_bytes()))
    }
}

struct StorageNodeSession {
    shared_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    node: Arc<SharedStorageNode>,
    read_operations: BTreeMap<String, SessionReadHandle>,
    object_payload_lease: Option<SessionObjectPayloadLease>,
    metadata_command_guards: BTreeMap<PgId, StorageNodeSessionMetadataCommandGuard>,
    current_rpc_context: Option<StorageNodeMetadataCommandLockContext>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageNodeMetadataCommandLockAuthority {
    CurrentPrimary,
    HistoricalRecoveryPrimary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageNodeMetadataCommandLockBinding {
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    authority: StorageNodeMetadataCommandLockAuthority,
}

struct StorageNodeSessionMetadataCommandGuard {
    binding: StorageNodeMetadataCommandLockBinding,
    _guard: StorageNodeMetadataCommandGuard,
}

struct ValidatedReadHandleAcquireRequest {
    read_operation_id: String,
    entries: Vec<(ShardLocation, ShardKey)>,
}

impl StorageNodeSession {
    fn new(
        shared_handles: Arc<Mutex<StorageNodeReadHandleState>>,
        node: Arc<SharedStorageNode>,
    ) -> Self {
        Self {
            shared_handles,
            node,
            read_operations: BTreeMap::new(),
            object_payload_lease: None,
            metadata_command_guards: BTreeMap::new(),
            current_rpc_context: None,
        }
    }

    fn set_current_rpc_context(&mut self, request_id: u64, kind: StorageRpcMessageKind) {
        self.current_rpc_context = Some(StorageNodeMetadataCommandLockContext { request_id, kind });
    }

    fn current_rpc_context(&self) -> Option<StorageNodeMetadataCommandLockContext> {
        self.current_rpc_context
    }

    fn holds_metadata_command_pg_lock(&self, pg_id: PgId) -> bool {
        self.metadata_command_guards.contains_key(&pg_id)
    }

    fn holds_metadata_command_pg_lock_with_binding(
        &self,
        binding: StorageNodeMetadataCommandLockBinding,
    ) -> bool {
        self.metadata_command_guards
            .get(&binding.pg_id)
            .is_some_and(|held| held.binding == binding)
    }

    fn has_metadata_command_pg_locks(&self) -> bool {
        !self.metadata_command_guards.is_empty()
    }

    fn update_metadata_command_lock_context(
        &self,
        locks: &StorageNodeMetadataCommandLocks,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) {
        for &pg_id in self.metadata_command_guards.keys() {
            locks.update_context(pg_id, context);
        }
    }

    fn clear_metadata_command_lock_context(&self, locks: &StorageNodeMetadataCommandLocks) {
        self.update_metadata_command_lock_context(locks, None);
    }

    fn acquire_metadata_command_pg_lock(
        &mut self,
        locks: &StorageNodeMetadataCommandLocks,
        node_id: NodeId,
        binding: StorageNodeMetadataCommandLockBinding,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) -> Result<(), StorageRpcErrorResponse> {
        if let Some(held) = self.metadata_command_guards.get(&binding.pg_id) {
            if held.binding == binding {
                return Ok(());
            }
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "metadata command lock for PG {} is bound to epoch {} {:?}, not epoch {} {:?}",
                    binding.pg_id.get(),
                    held.binding.cluster_epoch.get(),
                    held.binding.authority,
                    binding.cluster_epoch.get(),
                    binding.authority,
                ),
            });
        }
        let guard = locks.acquire(node_id, binding.pg_id, context)?;
        self.metadata_command_guards.insert(
            binding.pg_id,
            StorageNodeSessionMetadataCommandGuard {
                binding,
                _guard: guard,
            },
        );
        Ok(())
    }

    fn release_metadata_command_pg_lock(&mut self, pg_id: PgId) {
        self.metadata_command_guards.remove(&pg_id);
    }

    fn has_active_read_state(&self) -> bool {
        self.object_payload_lease
            .as_ref()
            .is_some_and(|lease| lease.is_acquired)
            || self
                .read_operations
                .values()
                .any(|existing| existing.is_acquired)
    }

    fn acquire_read_handles(
        &mut self,
        request: ValidatedReadHandleAcquireRequest,
    ) -> Result<Vec<ShardLocation>, StorageRpcErrorResponse> {
        match self.read_operations.get(&request.read_operation_id) {
            Some(existing) if existing.entries == request.entries && existing.is_acquired => {
                return Ok(existing
                    .entries
                    .iter()
                    .map(|(location, _)| *location)
                    .collect());
            }
            Some(_) => {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::Internal,
                    message: format!(
                        "read operation {} was already acquired with different shard locations",
                        request.read_operation_id
                    ),
                });
            }
            None => {}
        }
        if self.read_operations.len() >= STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION {
            return Err(resource_exhausted_response(format!(
                "storage-node session read operation limit {} is exhausted",
                STORAGE_NODE_MAX_READ_OPERATIONS_PER_SESSION
            )));
        }

        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_acquire(&request.entries)?;
        let locations = request
            .entries
            .iter()
            .map(|(location, _)| *location)
            .collect();
        self.read_operations.insert(
            request.read_operation_id,
            SessionReadHandle {
                entries: request.entries,
                is_acquired: true,
            },
        );
        Ok(locations)
    }

    fn release_read_handles(&mut self, read_operation_id: &str) {
        let Some(existing) = self.read_operations.remove(read_operation_id) else {
            return;
        };
        if !existing.is_acquired {
            return;
        }
        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release(&existing.entries);
    }

    fn acquire_object_payload_lease(
        &mut self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, StorageRpcErrorResponse> {
        let root = (bucket.clone(), key.clone(), generation_id);
        if let Some(existing) = self.object_payload_lease.as_ref() {
            if existing.route_cluster_epoch == route_cluster_epoch && existing.root == root {
                return Ok(existing.is_acquired);
            }
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message:
                    "object-payload lease session is already bound to a different route or subject"
                        .to_string(),
            });
        }
        if !self
            .node
            .try_acquire_object_payload_lease(bucket, key, generation_id)
        {
            return Ok(false);
        }
        self.object_payload_lease = Some(SessionObjectPayloadLease {
            route_cluster_epoch,
            root,
            is_acquired: true,
            remaining_after_release: None,
        });
        Ok(true)
    }

    fn release_object_payload_lease(
        &mut self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<usize, StorageRpcErrorResponse> {
        let root = (bucket.clone(), key.clone(), generation_id);
        let Some(existing) = self.object_payload_lease.as_mut() else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "object-payload lease release has no session-bound acquisition"
                    .to_string(),
            });
        };
        if existing.route_cluster_epoch != route_cluster_epoch || existing.root != root {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message:
                    "object-payload lease release does not match its session-bound acquisition"
                        .to_string(),
            });
        }
        if !existing.is_acquired {
            return Ok(existing.remaining_after_release.unwrap_or(0));
        }
        let remaining = self
            .node
            .release_object_payload_lease(bucket, key, generation_id);
        existing.is_acquired = false;
        existing.remaining_after_release = Some(remaining);
        Ok(remaining)
    }
}

impl Drop for StorageNodeSession {
    fn drop(&mut self) {
        let mut shared_handles = self
            .shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for existing in self.read_operations.values_mut() {
            if existing.is_acquired {
                shared_handles.release(&existing.entries);
                existing.is_acquired = false;
            }
        }
        if let Some(existing) = self.object_payload_lease.take() {
            if existing.is_acquired {
                let (bucket, key, generation_id) = existing.root;
                self.node
                    .release_object_payload_lease(&bucket, &key, generation_id);
            }
        }
    }
}

#[derive(Debug)]
struct SessionReadHandle {
    entries: Vec<(ShardLocation, ShardKey)>,
    is_acquired: bool,
}

struct SessionObjectPayloadLease {
    route_cluster_epoch: ClusterEpoch,
    root: (BucketName, ObjectKey, GenerationId),
    is_acquired: bool,
    remaining_after_release: Option<usize>,
}

fn storage_node_rpc_io_timeout(rpc_auth: Option<&StorageRpcServerAuthConfig>) -> Duration {
    rpc_auth
        .map(|auth| auth.transport_limits().io_timeout())
        .unwrap_or(STORAGE_RPC_SERVER_IDLE_TIMEOUT)
}

fn rpc_stream_error(error: StorageRpcStreamError) -> StorageNodeServerError {
    StorageNodeServerError::RpcStream {
        message: error.to_string(),
    }
}

fn resource_exhausted_response(message: String) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ResourceExhausted,
        message,
    }
}

fn shard_delete_in_progress_response(message: String) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ShardDeleteInProgress,
        message,
    }
}

fn validate_placed_segment_shard_repair_claim_route_epoch(
    pg_id: PgId,
    route_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StoreError> {
    if claim.cluster_epoch != route_epoch {
        return Err(StoreError::StalePayloadOperation {
            pg_id: pg_id.get(),
            operation_epoch: claim.cluster_epoch,
            current_epoch: route_epoch,
        });
    }
    Ok(())
}

fn validated_request_data_pg(
    node: &SharedStorageNode,
    pg_id: PgId,
    request_data_pg_id: u32,
    operation: &'static str,
) -> Result<DataPgId, StorageRpcErrorResponse> {
    if request_data_pg_id != pg_id.get() {
        return Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!(
                "{operation} data PG {} does not match routed PG {}",
                request_data_pg_id,
                pg_id.get()
            ),
        });
    }
    Ok(node
        .data_pg(pg_id)
        .expect("validated request data PG must belong to the installed topology"))
}

fn validate_placed_segment_shard_backfill_claim_route_epoch(
    pg_id: PgId,
    route_epoch: ClusterEpoch,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) -> Result<(), StoreError> {
    if claim.cluster_epoch != route_epoch {
        return Err(StoreError::StalePayloadOperation {
            pg_id: pg_id.get(),
            operation_epoch: claim.cluster_epoch,
            current_epoch: route_epoch,
        });
    }
    Ok(())
}

fn validate_metadata_command_request_epoch(
    request: &StorageRpcMetadataCommandRequest,
) -> Result<(), StorageRpcErrorResponse> {
    let command_epoch = request.command.id().cluster_epoch();
    if command_epoch != request.cluster_epoch {
        return Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!(
                "metadata command epoch {} does not match request route epoch {}",
                command_epoch.get(),
                request.cluster_epoch.get()
            ),
        });
    }
    Ok(())
}

fn store_error_response(error: StoreError) -> StorageRpcErrorResponse {
    match error {
        StoreError::NotFound => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::NotFound,
            message: "not found".to_string(),
        },
        StoreError::MetadataCommandContention { context } => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: format!("metadata command contention during {context}"),
        },
        error @ StoreError::OperationDeadlineExceeded { .. } => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::TransportTimeout,
            message: error.to_string(),
        },
        error @ StoreError::ClusterMapHistoryReferenceLimitExceeded { .. } => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ResourceExhausted,
                message: error.to_string(),
            }
        }
        error @ StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms,
            now_ms,
        } => {
            let _ = observability::emit_flight_event(
                "storage",
                "storage_rpc_admitted_route_expired",
                format!(
                    "cluster_epoch={} valid_until_ms={valid_until_ms} now_ms={now_ms}",
                    cluster_epoch.get()
                ),
            );
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: error.to_string(),
            }
        }
        error @ StoreError::StaleMetadataReadProof { .. } => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: error.to_string(),
        },
        error @ (StoreError::IntegrityError { .. } | StoreError::ShardAckMismatch { .. }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ShardIntegrity,
                message: error.to_string(),
            }
        }
        error @ (StoreError::MetadataCommandLogChecksumMismatch { .. }
        | StoreError::MetadataCommandLogHashMismatch { .. }
        | StoreError::MetadataCommandReplicaStateEncodingVersion { .. }
        | StoreError::MetadataCommandReplicaStateDiverged { .. }
        | StoreError::MetadataStateDigestMismatch { .. }
        | StoreError::MetadataCheckpointInvalid { .. }) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandIntegrity,
            message: error.to_string(),
        },
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn admitted_route_effect_fence(
    cluster_epoch: ClusterEpoch,
    deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
) -> AdmittedRouteEffectFence {
    deadline.map_or_else(
        || AdmittedRouteEffectFence::unbounded(cluster_epoch),
        |deadline| {
            AdmittedRouteEffectFence::bind_portable(
                cluster_epoch,
                deadline.authority_valid_until_ms,
                deadline.portable_wall_valid_until_ms,
            )
        },
    )
}

fn bucket_snapshot_error_response(error: BucketSnapshotLoadError) -> StorageRpcErrorResponse {
    match error {
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimNotFound { claim_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ReclaimClaimNotFound,
                message: claim_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict { claim_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::ReclaimClaimConflict,
                message: claim_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id,
        }) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::BucketWriteReservationConflict,
            message: reservation_id,
        },
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationNotFound {
            reservation_id,
        }) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::BucketWriteReservationNotFound,
            message: reservation_id,
        },
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention { context }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: format!("metadata command contention during {context}"),
            }
        }
        BucketSnapshotLoadError::Store(error @ StoreError::OperationDeadlineExceeded { .. }) => {
            store_error_response(error)
        }
        BucketSnapshotLoadError::Store(error @ StoreError::RouteMapExpired { .. }) => {
            store_error_response(error)
        }
        BucketSnapshotLoadError::Store(
            error @ StoreError::RouteCapabilitySubjectMismatch { .. },
        ) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: error.to_string(),
        },
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn bucket_write_drain_heartbeat_error_response(
    error: BucketSnapshotLoadError,
) -> StorageRpcErrorResponse {
    match error {
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDrainConflict { drain_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::BucketWriteDrainConflict,
                message: drain_id,
            }
        }
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDrainNotFound { drain_id }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::BucketWriteDrainNotFound,
                message: drain_id,
            }
        }
        error => bucket_snapshot_error_response(error),
    }
}

fn object_pg_error_response(error: ObjectPgActionError) -> StorageRpcErrorResponse {
    match error {
        ObjectPgActionError::MultipartConditionalRequestConflict => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MultipartConditionalRequestConflict,
            message: "multipart completion current object differs from initiation identity"
                .to_string(),
        },
        ObjectPgActionError::Store(StoreError::MetadataCommandContention { context }) => {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: format!("metadata command contention during {context}"),
            }
        }
        ObjectPgActionError::Store(error @ StoreError::RouteMapExpired { .. }) => {
            store_error_response(error)
        }
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

fn object_metadata_command_build_error_outcome(
    error: ObjectPgActionError,
    command_kind: Option<&'static str>,
) -> Result<StorageRpcObjectMetadataCommandBuildOutcome, StorageRpcErrorResponse> {
    match error {
        ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        }) => {
            emit_storage_node_metadata_command_log_conflict(
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
                command_kind,
            );
            Ok(StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            })
        }
        error => Err(object_pg_error_response(error)),
    }
}

fn report_storage_node_connection_failure(error: &StorageNodeServerError) {
    #[cfg(not(test))]
    eprintln!("storage-node connection failed: {error}");
    #[cfg(test)]
    let _ = error;
}

pub(crate) fn advance_storage_node_incarnation(
    data_dir: &Path,
) -> Result<u64, StorageNodeServerError> {
    fs::create_dir_all(data_dir).map_err(|source| StorageNodeServerError::Io {
        context: "create storage-node data directory for incarnation",
        path: data_dir.to_path_buf(),
        source,
    })?;
    let path = data_dir.join(STORAGE_NODE_INCARNATION_FILE);
    let current = match fs::read_to_string(&path) {
        Ok(contents) => parse_storage_node_incarnation(&path, &contents)?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(StorageNodeServerError::Io {
                context: "read storage-node incarnation",
                path,
                source,
            });
        }
    };
    let next = current
        .checked_add(1)
        .filter(|value| *value != 0)
        .ok_or_else(|| StorageNodeServerError::NodeIncarnationOverflow { path: path.clone() })?;
    persist_storage_node_incarnation(data_dir, &path, next)?;
    Ok(next)
}

fn parse_storage_node_incarnation(
    path: &Path,
    contents: &str,
) -> Result<u64, StorageNodeServerError> {
    let trimmed = contents.trim();
    let incarnation =
        trimmed
            .parse::<u64>()
            .map_err(|_| StorageNodeServerError::InvalidNodeIncarnation {
                path: path.to_path_buf(),
                value: contents.to_owned(),
            })?;
    if incarnation == 0 {
        return Err(StorageNodeServerError::InvalidNodeIncarnation {
            path: path.to_path_buf(),
            value: contents.to_owned(),
        });
    }
    Ok(incarnation)
}

fn persist_storage_node_incarnation(
    data_dir: &Path,
    path: &Path,
    incarnation: u64,
) -> Result<(), StorageNodeServerError> {
    let tmp_path = data_dir.join(STORAGE_NODE_INCARNATION_TMP_FILE);
    {
        let mut tmp_file =
            File::create(&tmp_path).map_err(|source| StorageNodeServerError::Io {
                context: "create storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
        tmp_file
            .write_all(format!("{incarnation}\n").as_bytes())
            .map_err(|source| StorageNodeServerError::Io {
                context: "write storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
        tmp_file
            .sync_all()
            .map_err(|source| StorageNodeServerError::Io {
                context: "sync storage-node incarnation",
                path: tmp_path.clone(),
                source,
            })?;
    }
    fs::rename(&tmp_path, path).map_err(|source| StorageNodeServerError::Io {
        context: "commit storage-node incarnation",
        path: path.to_path_buf(),
        source,
    })?;
    File::open(data_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StorageNodeServerError::Io {
            context: "sync storage-node incarnation directory",
            path: data_dir.to_path_buf(),
            source,
        })?;
    Ok(())
}

struct StorageNodeDataDirLock {
    file: File,
}

impl StorageNodeDataDirLock {
    fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        prepare_private_data_dir(data_dir).map_err(|source| StorageNodeServerError::Io {
            context: "prepare private storage-node data directory",
            path: data_dir.to_path_buf(),
            source,
        })?;
        let path = data_dir.join(STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME);
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let file = options
            .open(&path)
            .map_err(|source| StorageNodeServerError::Io {
                context: "open storage-node data-dir lock",
                path: path.clone(),
                source,
            })?;
        let metadata = file
            .metadata()
            .map_err(|source| StorageNodeServerError::Io {
                context: "inspect storage-node data-dir lock",
                path: path.clone(),
                source,
            })?;
        if !metadata.is_file() {
            return Err(StorageNodeServerError::DataDirLockNotRegularFile { path });
        }
        // SAFETY: flock operates on a valid file descriptor owned by `file`.
        // The descriptor remains open for the lifetime of StorageNodeDataDirLock.
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if rc != 0 {
            let source = io::Error::last_os_error();
            return if source.kind() == io::ErrorKind::WouldBlock {
                Err(StorageNodeServerError::DataDirAlreadyLocked {
                    path: data_dir.to_path_buf(),
                })
            } else {
                Err(StorageNodeServerError::Io {
                    context: "lock storage-node data directory",
                    path,
                    source,
                })
            };
        }
        Ok(Self { file })
    }

    fn entry_names_excluding_held_lock(
        &self,
        data_dir: &Path,
    ) -> Result<Vec<std::ffi::OsString>, StorageNodeServerError> {
        let held_metadata =
            self.file
                .metadata()
                .map_err(|source| StorageNodeServerError::DataDirInspectionIo {
                    path: data_dir.to_path_buf(),
                    source,
                })?;
        let entries = fs::read_dir(data_dir).map_err(|source| {
            StorageNodeServerError::DataDirInspectionIo {
                path: data_dir.to_path_buf(),
                source,
            }
        })?;
        let mut found_held_lock = false;
        let mut entry_names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| StorageNodeServerError::DataDirInspectionIo {
                path: data_dir.to_path_buf(),
                source,
            })?;
            if entry.file_name() != STORAGE_NODE_DATA_DIR_LOCK_FILE_NAME {
                entry_names.push(entry.file_name());
                continue;
            }
            let entry_metadata = fs::symlink_metadata(entry.path()).map_err(|source| {
                StorageNodeServerError::DataDirInspectionIo {
                    path: data_dir.to_path_buf(),
                    source,
                }
            })?;
            if entry_metadata.file_type().is_symlink()
                || !entry_metadata.is_file()
                || entry_metadata.dev() != held_metadata.dev()
                || entry_metadata.ino() != held_metadata.ino()
            {
                return Err(StorageNodeServerError::DataDirLockIdentityChanged {
                    path: data_dir.to_path_buf(),
                });
            }
            found_held_lock = true;
        }
        if !found_held_lock {
            return Err(StorageNodeServerError::DataDirLockIdentityChanged {
                path: data_dir.to_path_buf(),
            });
        }
        entry_names.sort_unstable();
        Ok(entry_names)
    }
}

impl std::fmt::Debug for StorageNodeDataDirLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNodeDataDirLock")
            .finish_non_exhaustive()
    }
}

impl Drop for StorageNodeDataDirLock {
    fn drop(&mut self) {}
}

fn validate_pg_ids(pg_ids: &[u32]) -> Result<(), StorageNodeServerError> {
    if pg_ids.is_empty() {
        return Err(StorageNodeServerError::EmptyPgSet);
    }
    let mut seen = BTreeMap::<u32, ()>::new();
    for &pg_id in pg_ids {
        if seen.insert(pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgId { pg_id });
        }
    }
    Ok(())
}

fn validate_pg_routes(
    pg_ids: &[u32],
    routes: &[StorageNodePgRoute],
) -> Result<(), StorageNodeServerError> {
    let configured: BTreeMap<u32, ()> = pg_ids.iter().map(|&pg_id| (pg_id, ())).collect();
    let mut seen = BTreeMap::<u32, ()>::new();
    for route in routes {
        if seen.insert(route.pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgRoute { pg_id: route.pg_id });
        }
        if !configured.contains_key(&route.pg_id) {
            return Err(StorageNodeServerError::RoutePgNotConfigured { pg_id: route.pg_id });
        }
        if !route.acting_set.contains(&route.primary_node_id) {
            return Err(StorageNodeServerError::RoutePrimaryNotInActingSet {
                pg_id: route.pg_id,
                primary_node_id: route.primary_node_id.as_u32(),
            });
        }
    }
    for &pg_id in pg_ids {
        if !seen.contains_key(&pg_id) {
            return Err(StorageNodeServerError::MissingPgRoute { pg_id });
        }
    }
    Ok(())
}

fn validate_process_config_route_table(
    config: &StorageNodeProcessConfig,
) -> Result<(), StorageNodeServerError> {
    validate_pg_ids(&config.pg_ids)?;
    validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
    for route in &config.pg_routes {
        if route.cluster_epoch != config.cluster_epoch {
            return Err(StorageNodeServerError::RouteEpochMismatch {
                pg_id: route.pg_id,
                route_epoch: route.cluster_epoch,
                config_epoch: config.cluster_epoch,
            });
        }
    }
    let mut recovery_pgs = BTreeSet::new();
    for (pg_id, recovery) in &config.pending_metadata_command_recoveries {
        if config.route_map_valid_until_ms().is_none() {
            return Err(
                StorageNodeServerError::InvalidPendingMetadataCommandRecovery {
                    pg_id: pg_id.get(),
                    reason: "authorization requires bounded runtime-map validity".to_owned(),
                },
            );
        }
        if !recovery_pgs.insert(*pg_id) {
            return Err(
                StorageNodeServerError::InvalidPendingMetadataCommandRecovery {
                    pg_id: pg_id.get(),
                    reason: "duplicate authorization".to_owned(),
                },
            );
        }
        let current_route = config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get());
        let historical_route = config.historical_pg_routes.iter().find(|route| {
            route.pg_id == pg_id.get() && route.cluster_epoch == recovery.pending().cluster_epoch()
        });
        if current_route.is_none_or(|route| route.state != PgState::Peering)
            || recovery.pending().cluster_epoch() >= config.cluster_epoch
            || historical_route.is_none_or(|route| {
                route.state != PgState::Active
                    || route.primary_node_id != recovery.reporting_node_id()
            })
        {
            return Err(
                StorageNodeServerError::InvalidPendingMetadataCommandRecovery {
                    pg_id: pg_id.get(),
                    reason: "authorization does not reference a current Peering route and its Active historical primary".to_owned(),
                },
            );
        }
    }
    Ok(())
}

fn validate_socket_directory(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    validate_absolute_socket_path(socket_path)?;
    let parent =
        socket_path
            .parent()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
                path: socket_path.to_path_buf(),
            })?;
    socket_path
        .file_name()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
            path: socket_path.to_path_buf(),
        })?;
    let metadata = fs::metadata(parent).map_err(|source| StorageNodeServerError::Io {
        context: "stat storage-node socket directory",
        path: parent.to_path_buf(),
        source,
    })?;
    let mode = metadata.permissions().mode() & 0o7777;
    if !metadata.is_dir() || mode != 0o700 {
        return Err(StorageNodeServerError::SocketDirectoryNotPrivate {
            path: parent.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

fn canonical_socket_path(path: &Path) -> Result<PathBuf, StorageNodeServerError> {
    validate_absolute_socket_path(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context: "canonicalize storage-node socket directory",
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}

fn cleanup_stale_socket_path(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    let metadata =
        match fs::symlink_metadata(socket_path).map_err(|source| StorageNodeServerError::Io {
            context: "stat storage-node socket path",
            path: socket_path.to_path_buf(),
            source,
        }) {
            Ok(metadata) => metadata,
            Err(StorageNodeServerError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(())
            }
            Err(error) => return Err(error),
        };
    if !metadata.file_type().is_socket() {
        return Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        });
    }
    match UnixStream::connect(socket_path) {
        Ok(_) => Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        }),
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(socket_path).map_err(|source| StorageNodeServerError::Io {
                context: "remove stale storage-node socket",
                path: socket_path.to_path_buf(),
                source,
            })
        }
        Err(source) => Err(StorageNodeServerError::Io {
            context: "connect existing storage-node socket",
            path: socket_path.to_path_buf(),
            source,
        }),
    }
}

fn validate_absolute_socket_path(path: &Path) -> Result<(), StorageNodeServerError> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(StorageNodeServerError::SocketPathNotAbsolute {
            path: path.to_path_buf(),
        })
    }
}

fn canonicalize_existing_or_parent(
    path: &Path,
    context: &'static str,
) -> Result<PathBuf, StorageNodeServerError> {
    if path.exists() {
        return path
            .canonicalize()
            .map_err(|source| StorageNodeServerError::Io {
                context,
                path: path.to_path_buf(),
                source,
            });
    }
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context,
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}
