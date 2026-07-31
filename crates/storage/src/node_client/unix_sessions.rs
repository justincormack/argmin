use super::*;

pub(crate) struct UnixStorageNodeReadHandleSession {
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    stream: BoxStorageRpcStream,
    next_request_id: u64,
    io_timeout: Duration,
    rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    _rpc_permit: Option<UnixStorageNodeRpcAdmissionPermit>,
    _object_payload_lease_permit: Option<UnixStorageNodeObjectPayloadLeaseAdmissionPermit>,
}

pub(crate) struct UnixStorageNodeMetadataCommandSession {
    node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    _rpc_permit: UnixStorageNodeRpcAdmissionPermit,
    inner: Mutex<UnixStorageNodeMetadataCommandSessionInner>,
}

struct UnixStorageNodeMetadataCommandSessionInner {
    stream: BoxStorageRpcStream,
    next_request_id: u64,
    io_timeout: Duration,
    pg_id: PgId,
    released: bool,
}

struct UnixStorageNodeReadHandleLease {
    session: UnixStorageNodeReadHandleSession,
    read_operation_id: String,
    released: bool,
}

struct UnixShardReadHandleRoute<'a> {
    client: &'a UnixStorageNodeClient,
    read_operation_id: String,
    entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
}

struct UnixObjectPayloadLease {
    session: UnixStorageNodeReadHandleSession,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    released: bool,
}

struct UnixObjectPayloadLeaseRoute<'a> {
    client: &'a UnixStorageNodeClient,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

struct UnixRetainedObjectPayloadReclaimRoute<'a> {
    client: &'a UnixStorageNodeClient,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    authority: ObjectPayloadReclaimClaimProof,
}

impl UnixStorageNodeClient {
    fn connect_session_stream(
        &self,
        context: &'static str,
    ) -> Result<(BoxStorageRpcStream, Duration), StoreError> {
        let io_timeout = storage_rpc_io_timeout(self.rpc_auth.as_deref());
        let deadline = Instant::now()
            .checked_add(io_timeout)
            .ok_or_else(|| StoreError::Io {
                context,
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "storage-node RPC session deadline overflowed",
                ),
            })?;
        self.endpoint
            .connect(deadline)
            .map(|stream| (stream, io_timeout))
            .map_err(|source| StoreError::Io { context, source })
    }

    #[cfg(test)]
    pub(crate) fn active_admitted_session_count_for_test(&self) -> usize {
        self.rpc_admission.active_session_count_for_test()
    }

    fn open_read_handle_session(&self) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(StorageRpcMessageKind::ReadHandlesAcquire)?;
        let (stream, io_timeout) =
            self.connect_session_stream("connect storage-node read-handle RPC endpoint")?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            stream,
            next_request_id: 1,
            io_timeout,
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: Some(rpc_permit),
            _object_payload_lease_permit: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_read_handle_session_for_test(
        &self,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        self.open_read_handle_session()
    }

    fn open_object_payload_lease_session(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let lease_permit = self.acquire_object_payload_lease_session_admission(kind)?;
        let (stream, io_timeout) =
            self.connect_session_stream("connect storage-node object-payload lease RPC endpoint")?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            stream,
            next_request_id: 1,
            io_timeout,
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: None,
            _object_payload_lease_permit: Some(lease_permit),
        })
    }

    fn acquire_object_payload_lease_session_admission(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<UnixStorageNodeObjectPayloadLeaseAdmissionPermit, StoreError> {
        let wait_timeout = self
            .rpc_admission
            .wait_timeout_for_class(UnixStorageNodeRpcAdmissionClass::Read);
        let broad = kind == ObjectPayloadLeaseKind::BroadSnapshot;
        match self.rpc_admission.acquire_object_payload_lease(broad) {
            UnixStorageNodeObjectPayloadLeaseAdmissionAcquire::Acquired(permit) => Ok(permit),
            UnixStorageNodeObjectPayloadLeaseAdmissionAcquire::TimedOut => {
                Err(StoreError::StorageRpcResourceExhausted {
                    node_id: self.node_id.as_u32(),
                    operation: match kind {
                        ObjectPayloadLeaseKind::BroadSnapshot => {
                            "broad object payload lease acquire"
                        }
                        ObjectPayloadLeaseKind::ShardLocations => {
                            "shard object payload lease acquire"
                        }
                    },
                    detail: crate::StorageNodeFailureDetail::new(format!(
                        "storage-node client object-payload lease session limit {} is exhausted after waiting {} ms",
                        self.rpc_admission.limit,
                        wait_timeout.as_millis()
                    )),
                })
            }
        }
    }

    fn object_payload_lease_control_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        operation: StorageRpcObjectPayloadLeaseControlOperation,
        reclaim_authority: Option<&ObjectPayloadReclaimClaimProof>,
    ) -> Result<u64, StoreError> {
        let request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            operation,
            reclaim_authority: reclaim_authority.cloned(),
        };
        let payload = encode_object_payload_lease_control_request(&request);
        let response =
            self.rpc_request(StorageRpcMessageKind::ObjectPayloadLeaseControl, payload)?;
        let response =
            decode_object_payload_lease_control_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode object-payload lease control response",
                    error.to_string(),
                )
            })?;
        Ok(response.value)
    }

    pub(crate) fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let rpc_permit =
            self.acquire_rpc_admission(StorageRpcMessageKind::MetadataCommandPgLockAcquire)?;
        let (stream, io_timeout) =
            self.connect_session_stream("connect storage-node metadata command RPC endpoint")?;
        let session = UnixStorageNodeMetadataCommandSession {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: rpc_permit,
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream,
                next_request_id: 1,
                io_timeout,
                pg_id,
                released: false,
            }),
        };
        let payload = session.encode_metadata_command_state_request(pg_id);
        session.rpc_request(StorageRpcMessageKind::MetadataCommandPgLockAcquire, payload)?;
        Ok(session)
    }
}

#[allow(dead_code)]
impl UnixStorageNodeReadHandleSession {
    pub(crate) fn acquire_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Vec<crate::cluster::ShardLocation>, StoreError> {
        let (locations, shard_keys): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.into(),
            locations: locations.iter().copied().map(Into::into).collect(),
            shard_keys,
        };
        let payload = encode_read_handle_acquire_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle acquire request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesAcquire, payload)?;
        let response = decode_read_handle_acquire_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle acquire response", error.to_string())
        })?;
        if response.locations != request.locations {
            return Err(self.rpc_payload_error(
                "validate read handle acquire response",
                format!(
                    "expected locations {:?}, got {:?}",
                    request.locations, response.locations
                ),
            ));
        }
        Ok(locations)
    }

    pub(crate) fn release_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.into(),
        };
        let payload = encode_read_handle_release_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle release request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesRelease, payload)?;
        decode_read_handle_release_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle release response", error.to_string())
        })?;
        Ok(())
    }

    fn object_payload_lease_control(
        &mut self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        operation: StorageRpcObjectPayloadLeaseControlOperation,
        reclaim_authority: Option<&ObjectPayloadReclaimClaimProof>,
    ) -> Result<u64, StoreError> {
        let request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.route_cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            operation,
            reclaim_authority: reclaim_authority.cloned(),
        };
        let payload = encode_object_payload_lease_control_request(&request);
        let response =
            self.rpc_request(StorageRpcMessageKind::ObjectPayloadLeaseControl, payload)?;
        let response =
            decode_object_payload_lease_control_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode object-payload lease control response",
                    error.to_string(),
                )
            })?;
        Ok(response.value)
    }

    fn rpc_request(
        &mut self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        let deadline = Instant::now().checked_add(self.io_timeout).ok_or_else(|| {
            self.rpc_payload_error(
                "set read-handle RPC deadline",
                "storage-node read-handle RPC deadline overflowed".to_string(),
            )
        })?;
        self.stream
            .set_operation_deadline(deadline)
            .map_err(|source| StoreError::Io {
                context: "set storage-node read-handle RPC deadline",
                source,
            })?;
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or_else(|| {
            self.rpc_payload_error(
                "allocate read-handle request id",
                "storage-node read-handle request id overflowed".to_string(),
            )
        })?;
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        let request_proof = write_unix_storage_rpc_request(
            &mut self.stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            &request,
            "write read-handle RPC request",
        )?;
        let response = read_unix_storage_rpc_response(
            &mut self.stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            request_proof.as_ref(),
            "read read-handle RPC response",
        )?;
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate read-handle RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        match decode_storage_rpc_response_payload(&response.payload).map_err(|error| {
            self.rpc_payload_error("decode read-handle RPC response", error.to_string())
        })? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        storage_rpc_response_error(self.node_id, kind, error)
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(message),
        }
    }
}

impl UnixStorageNodeMetadataCommandSession {
    fn metadata_command_pg_id(&self) -> PgId {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).pg_id
    }

    fn encode_metadata_command_state_request(&self, pg_id: PgId) -> Vec<u8> {
        encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        })
    }

    fn encode_metadata_command_request(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error("encode metadata command request", error.to_string())
        })
    }

    fn rpc_request(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        match self.rpc_request_result(kind, payload)? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_request_result(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = Instant::now()
            .checked_add(inner.io_timeout)
            .ok_or_else(|| {
                self.rpc_payload_error(
                    "set metadata command session RPC deadline",
                    "storage-node metadata command session RPC deadline overflowed".to_string(),
                )
            })?;
        inner
            .stream
            .set_operation_deadline(deadline)
            .map_err(|source| StoreError::Io {
                context: "set storage-node metadata command session RPC deadline",
                source,
            })?;
        let request_id = inner.next_request_id;
        inner.next_request_id = inner.next_request_id.checked_add(1).ok_or_else(|| {
            self.rpc_payload_error(
                "allocate metadata command session request id",
                "metadata command session request id overflowed".to_string(),
            )
        })?;
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        let request_proof = write_unix_storage_rpc_request(
            &mut inner.stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            &request,
            "write metadata command session RPC request",
        )?;
        let response = read_unix_storage_rpc_response(
            &mut inner.stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            request_proof.as_ref(),
            "read metadata command session RPC response",
        )?;
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate metadata command session RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        decode_storage_rpc_response_payload(&response.payload).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command session RPC response",
                error.to_string(),
            )
        })
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        storage_rpc_response_error(self.node_id, kind, error)
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(message),
        }
    }

    fn close_metadata_command_pg_lock_on_drop(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.released {
            return;
        }
        let _ = inner.stream.shutdown(std::net::Shutdown::Both);
        inner.released = true;
    }

    fn metadata_command_acceptance_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(kind, payload)?;
        let response = decode_metadata_command_acceptance_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command acceptance response",
                error.to_string(),
            )
        })?;
        match response.outcome {
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance) => Ok(acceptance),
            StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command acceptance response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn metadata_command_apply_and_record_with_kind(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        decode_context: &'static str,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.metadata_command_apply_and_record_with_payload(
            pg_id,
            command,
            kind,
            payload,
            decode_context,
        )
    }

    fn metadata_command_apply_and_record_with_payload(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        decode_context: &'static str,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let response = self
            .rpc_request(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_metadata_command_state_outcome_response(&response)
            .map_err(|error| self.rpc_payload_error(decode_context, error.to_string()))
            .map_err(BucketSnapshotLoadError::Store)?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(BucketSnapshotLoadError::Store(
                metadata_command_log_conflict_error(
                    self.cluster_epoch,
                    pg_id,
                    decode_context,
                    |operation, message| self.rpc_payload_error(operation, message),
                    MetadataCommandLogConflictRpcFields {
                        node_id,
                        pg_id: conflict_pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                reservation_id,
                generation_id,
            } => Err(BucketSnapshotLoadError::Metadata(
                MetadataError::ObjectGenerationReservationConflict {
                    reservation_id: reservation_id.into_string(),
                    generation_id: generation_id.get(),
                },
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id,
            } => Err(BucketSnapshotLoadError::Metadata(
                MetadataError::ObjectVersionReservationConflict { version_id },
            )),
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name,
                bucket_execution_generation,
            } => match stale_bucket_metadata_command_error(
                command,
                name,
                bucket_execution_generation,
                decode_context,
                |operation, message| self.rpc_payload_error(operation, message),
            ) {
                Ok(error) => Err(BucketSnapshotLoadError::Metadata(error)),
                Err(error) => Err(BucketSnapshotLoadError::Store(error)),
            },
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence,
                generation_id,
            } => match stale_object_write_command_error(
                command,
                bucket,
                key,
                write_sequence,
                generation_id,
                decode_context,
                |operation, message| self.rpc_payload_error(operation, message),
            ) {
                Ok(error) => Err(BucketSnapshotLoadError::Metadata(error)),
                Err(error) => Err(BucketSnapshotLoadError::Store(error)),
            },
            StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index } => {
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::StreamSegmentConflict { segment_index },
                ))
            }
        }
    }

    fn try_insert_pending_metadata_command_slot_with_effect_deadline(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: bucket.cloned(),
            effect_deadline,
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
            payload,
        )?;
        let response =
            decode_metadata_command_pending_slot_insert_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => Ok(()),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            } => Err(StoreError::MetadataCommandPendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            }),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command pending slot insert response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }
}
impl Drop for UnixStorageNodeMetadataCommandSession {
    fn drop(&mut self) {
        self.close_metadata_command_pg_lock_on_drop();
    }
}

impl MetadataCommandRecoveryCriticalSection for UnixStorageNodeMetadataCommandSession {
    fn max_metadata_command_log_index(&self) -> Result<u64, StoreError> {
        MetadataCommandNodeClient::max_metadata_command_log_index(
            self,
            self.metadata_command_pg_id(),
            self.cluster_epoch,
        )
    }

    fn pending_metadata_command_envelope(
        &self,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandNodeClient::pending_metadata_command_envelope(
            self,
            self.metadata_command_pg_id(),
            self.cluster_epoch,
        )
    }

    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn metadata_command_abandon_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_abandon_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandPendingSlotReplaceRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload =
            encode_metadata_command_pending_slot_replace_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command pending slot replace request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot replace response",
                    error.to_string(),
                )
            })
    }

    fn replace_pending_metadata_command_slot_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            authorized_source: authorized_source.clone(),
            abandoned_source: abandoned_source.cloned(),
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload = encode_metadata_command_recovery_pending_slot_replace_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command recovery pending slot replace request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command recovery pending slot replace response",
                    error.to_string(),
                )
            })
    }

    fn apply_metadata_command_and_record_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandRecoveryRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            authorized_source: authorized_source.clone(),
            abandoned_source: abandoned_source.cloned(),
            command: command.clone(),
        };
        let payload = encode_metadata_command_recovery_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command recovery apply and record request",
                    error.to_string(),
                )
            })
            .map_err(BucketSnapshotLoadError::Store)?;
        self.metadata_command_apply_and_record_with_payload(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            payload,
            "decode metadata command recovery apply and record response",
        )
    }

    fn record_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
        )?;
        let response =
            decode_metadata_command_state_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command record abandoned response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command record abandoned response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                ..
            } => Err(self.rpc_payload_error(
                "decode metadata command record abandoned response",
                "record abandoned response cannot contain object generation reservation conflict"
                    .to_string(),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict { .. } => {
                Err(self.rpc_payload_error(
                    "decode metadata command record abandoned response",
                    "record abandoned response cannot contain object version reservation conflict"
                        .to_string(),
                ))
            }
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand { .. } => Err(self
                .rpc_payload_error(
                    "decode metadata command record abandoned response",
                    "record abandoned response cannot contain stale bucket metadata command"
                        .to_string(),
                )),
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand { .. } => Err(self
                .rpc_payload_error(
                    "decode metadata command record abandoned response",
                    "record abandoned response cannot contain stale object write command"
                        .to_string(),
                )),
            StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { .. } => Err(self
                .rpc_payload_error(
                    "decode metadata command record abandoned response",
                    "record abandoned response cannot contain stream segment conflict".to_string(),
                )),
        }
    }
}

impl MetadataCommandCriticalSection for UnixStorageNodeMetadataCommandSession {
    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn apply_metadata_command_and_record(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }
}

impl MetadataCommandNodeClient for UnixStorageNodeMetadataCommandSession {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandCriticalSection>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let held_pg_id = self.inner.lock().unwrap_or_else(|e| e.into_inner()).pg_id;
        if pg_id != held_pg_id {
            return Err(self.rpc_payload_error(
                "open nested metadata command critical section",
                format!(
                    "session holds metadata command PG {}, not requested PG {}",
                    held_pg_id.get(),
                    pg_id.get()
                ),
            ));
        }
        Err(self.rpc_payload_error(
            "open nested metadata command critical section",
            "nested metadata command critical sections are not supported".to_string(),
        ))
    }

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandMaxLogIndex, payload)?;
        decode_metadata_command_max_log_index_response(&response)
            .map(|response| response.max_log_index)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command max log index response",
                    error.to_string(),
                )
            })
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            min_log_index: min_log_index.get(),
        };
        let payload = encode_metadata_command_next_id_request(&request);
        let response = self.rpc_request(StorageRpcMessageKind::MetadataCommandNextId, payload)?;
        let decoded = decode_metadata_command_next_id_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command next id response",
                error.to_string(),
            )
        })?;
        match decoded.outcome {
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch,
                pg_id: decoded_pg_id,
                log_index,
            } => {
                let Some(log_index) = MetadataCommandLogIndex::new(log_index) else {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log index must not be zero".to_string(),
                    ));
                };
                if cluster_epoch != self.cluster_epoch || decoded_pg_id != pg_id {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command id route mismatch".to_string(),
                    ));
                }
                Ok(MetadataCommandId::new(
                    cluster_epoch,
                    decoded_pg_id,
                    log_index,
                ))
            }
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
        )?;
        let response =
            decode_metadata_command_pending_envelope_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    error.to_string(),
                )
            })?;
        if let Some(command) = response.command.as_ref() {
            if command.id().cluster_epoch() != self.cluster_epoch || command.id().pg_id() != pg_id {
                return Err(self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    "metadata command pending envelope route mismatch".to_string(),
                ));
            }
        }
        Ok(response.command)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline(
            pg_id, command, bucket, None,
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_pending_metadata_command_slot_with_effect_deadline(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
            pg_id,
            command,
            bucket,
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<bool, StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command bucket-control pending slot request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
            payload,
        )?;
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command bucket-control pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(value),
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command bucket-control pending slot insert response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot remove response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandReplicaState, payload)?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replica state response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointExport,
            payload,
        )?;
        decode_metadata_command_checkpoint_response(&response)
            .map(|response| response.checkpoint)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint response",
                    error.to_string(),
                )
            })
    }

    fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint record current response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let limit = u32::try_from(limit).map_err(|_| StoreError::StorageRpc {
            operation: "metadata command checkpoint candidates",
            node_id: self.node_id.as_u32(),
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(format!(
                "checkpoint candidate limit {limit} exceeds u32::MAX"
            )),
        })?;
        let payload = encode_metadata_command_checkpoint_candidates_request(
            &StorageRpcMetadataCommandCheckpointCandidatesRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                max_applied_log_index,
                limit,
            },
        );
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointCandidates,
            payload,
        )?;
        decode_metadata_command_checkpoint_candidates_response(&response)
            .map(|response| response.checkpoints)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint candidates response",
                    error.to_string(),
                )
            })
    }

    fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandLogCompact, payload)?;
        decode_metadata_command_log_compact_response(&response)
            .map(|response| response.status)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command log compact response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            pg_id,
            command,
        )
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            pg_id,
            command,
        )
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
        )?;
        let response =
            decode_metadata_command_applied_hashes_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command applied hashes response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes) => Ok(hashes),
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command applied hashes response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            first_log_index,
            last_log_index,
        };
        let payload = encode_metadata_command_log_hash_range_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            payload,
        )?;
        decode_metadata_command_log_hash_range_response(&response)
            .map(|response| response.entries)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command retained log hashes response",
                    error.to_string(),
                )
            })
    }

    fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            first_log_index,
            last_log_index,
        };
        let payload = encode_metadata_command_log_hash_range_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            payload,
        )?;
        decode_metadata_command_log_entry_range_response(&response)
            .map(|response| response.entries)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command retained log entries response",
                    error.to_string(),
                )
            })
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandMatchingAppliedRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            expected_previous_log_hash,
        };
        let payload =
            encode_metadata_command_matching_applied_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command matching applied request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            payload,
        )?;
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command matching applied response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(value),
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command matching applied response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.metadata_command_apply_and_record_with_kind(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            "decode metadata command apply and record response",
        )
    }

    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let held_pg_id = self.metadata_command_pg_id();
        if pg_id != held_pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "record metadata command abandoned on bound replica",
            });
        }
        MetadataCommandRecoveryCriticalSection::record_metadata_command_abandoned(self, command)
    }
}
impl ShardReadHandleNodeClient for UnixStorageNodeClient {
    fn open_shard_read_handle_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: entries
                    .first()
                    .map_or(0, |(location, _)| location.data_pg_id().get()),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        if entries.is_empty() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open shard read-handle route",
            });
        }
        for (location, key) in &entries {
            if location.node_id() != self.node_id
                || location.cluster_epoch() != route_cluster_epoch
                || location.shard_index() != key.shard_index()
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "open shard read-handle route",
                });
            }
        }
        Ok(Box::new(UnixShardReadHandleRoute {
            client: self,
            read_operation_id: read_operation_id.to_string(),
            entries,
        }))
    }
}

impl ShardReadHandleRoute for UnixShardReadHandleRoute<'_> {
    fn acquire(self: Box<Self>) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        let mut session = self.client.open_read_handle_session()?;
        session.acquire_read_handles(&self.read_operation_id, self.entries)?;
        Ok(Box::new(UnixStorageNodeReadHandleLease {
            session,
            read_operation_id: self.read_operation_id,
            released: false,
        }))
    }
}

impl ObjectPayloadLeaseNodeClient for UnixStorageNodeClient {
    fn open_object_payload_lease_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadLeaseRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: route_cluster_epoch,
                operation_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixObjectPayloadLeaseRoute {
            client: self,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }))
    }
}

impl ObjectPayloadLeaseRoute for UnixObjectPayloadLeaseRoute<'_> {
    fn acquire_object_payload_lease(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        let mut session = self.client.open_object_payload_lease_session(kind)?;
        let acquired = session.object_payload_lease_control(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Acquire,
            None,
        )?;
        match acquired {
            0 => Ok(None),
            1 => Ok(Some(Box::new(UnixObjectPayloadLease {
                session,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                generation_id: self.generation_id,
                released: false,
            }))),
            _ => Err(StoreError::Io {
                context: "validate storage-node object-payload lease acquire response",
                source: io::Error::new(io::ErrorKind::InvalidData, "invalid acquired value"),
            }),
        }
    }

    fn try_begin_object_payload_reclaim(
        &self,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError> {
        if authority.cluster_epoch != self.client.cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "begin object payload reclaim",
            });
        }
        match self.client.object_payload_lease_control_request(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin,
            Some(authority),
        )? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StoreError::Io {
                context: "validate storage-node object-payload reclaim begin response",
                source: io::Error::new(io::ErrorKind::InvalidData, "invalid acquired value"),
            }),
        }
    }

    fn object_payload_lease_count(&self) -> Result<usize, StoreError> {
        let count = self.client.object_payload_lease_control_request(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Count,
            None,
        )?;
        usize::try_from(count).map_err(|_| StoreError::Io {
            context: "validate storage-node object-payload lease count response",
            source: io::Error::new(io::ErrorKind::InvalidData, "lease count exceeds usize"),
        })
    }
}

impl RetainedObjectPayloadReclaimNodeClient for UnixStorageNodeClient {
    fn open_retained_object_payload_reclaim_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<Box<dyn RetainedObjectPayloadReclaimRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: route_cluster_epoch,
                operation_epoch: self.cluster_epoch,
            });
        }
        if authority.cluster_epoch != route_cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained object payload reclaim route",
            });
        }
        Ok(Box::new(UnixRetainedObjectPayloadReclaimRoute {
            client: self,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            authority: authority.clone(),
        }))
    }
}

impl RetainedObjectPayloadReclaimRoute for UnixRetainedObjectPayloadReclaimRoute<'_> {
    fn finish_object_payload_reclaim(&self, keep_fence: bool) -> Result<(), StoreError> {
        let operation = if keep_fence {
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
        } else {
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
        };
        let mut last_error = None;
        for _ in 0..2 {
            let result = self
                .client
                .object_payload_lease_control_request(
                    &self.bucket,
                    &self.key,
                    self.generation_id,
                    operation,
                    Some(&self.authority),
                )
                .map(|_| ());
            match result {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("object-payload reclaim finish attempted at least once"))
    }

    fn clear_object_payload_reclaim_fence(&self) -> Result<(), StoreError> {
        let mut last_error = None;
        for _ in 0..2 {
            let result = self
                .client
                .object_payload_lease_control_request(
                    &self.bucket,
                    &self.key,
                    self.generation_id,
                    StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear,
                    Some(&self.authority),
                )
                .map(|_| ());
            match result {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("object-payload reclaim fence clear attempted at least once"))
    }
}

impl ObjectPayloadLeaseNodeLease for UnixObjectPayloadLease {
    fn release(&mut self) -> Result<usize, StoreError> {
        if self.released {
            return Ok(0);
        }
        let remaining = self.session.object_payload_lease_control(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Release,
            None,
        )?;
        self.released = true;
        usize::try_from(remaining).map_err(|_| StoreError::Io {
            context: "validate storage-node object-payload lease release response",
            source: io::Error::new(io::ErrorKind::InvalidData, "lease count exceeds usize"),
        })
    }
}

impl Drop for UnixObjectPayloadLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl ShardReadHandleLease for UnixStorageNodeReadHandleLease {
    fn release(&mut self) -> Result<(), StoreError> {
        if self.released {
            return Ok(());
        }
        self.session
            .release_read_handles(self.read_operation_id.as_str())?;
        self.released = true;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_rpc_transport::accepted_unix_stream;
    use std::os::unix::net::UnixStream;

    fn test_rpc_admission_permit() -> UnixStorageNodeRpcAdmissionPermit {
        Arc::new(UnixStorageNodeRpcAdmission::new(1))
            .try_acquire_for_test()
            .unwrap()
    }

    #[test]
    fn storage_rpc_resource_exhaustion_decodes_to_typed_store_error() {
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            "/tmp/unopened-storage-node.sock",
        );
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "active read handles exceed limit".to_string(),
        };
        assert!(matches!(
            client.rpc_response_error(StorageRpcMessageKind::ShardDelete, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "shard delete",
                ref detail,
            } if detail.as_str().contains("active read handles")
        ));

        let (read_stream, _read_peer) = UnixStream::pair().unwrap();
        let io_timeout = Duration::from_secs(1);
        let read_session = UnixStorageNodeReadHandleSession {
            node_id: NodeId::new(8),
            route_cluster_epoch: ClusterEpoch::new(1).unwrap(),
            stream: accepted_unix_stream(read_stream, Instant::now() + io_timeout).unwrap(),
            next_request_id: 1,
            io_timeout,
            rpc_auth: None,
            _rpc_permit: Some(test_rpc_admission_permit()),
            _object_payload_lease_permit: None,
        };
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "read handle session limit reached".to_string(),
        };
        assert!(matches!(
            read_session.rpc_response_error(StorageRpcMessageKind::ReadHandlesAcquire, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 8,
                operation: "read handles acquire",
                ref detail,
            } if detail.as_str().contains("session limit")
        ));

        let (metadata_stream, _metadata_peer) = UnixStream::pair().unwrap();
        let io_timeout = Duration::from_secs(1);
        let metadata_session = UnixStorageNodeMetadataCommandSession {
            node_id: NodeId::new(9),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            rpc_auth: None,
            _rpc_permit: test_rpc_admission_permit(),
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream: accepted_unix_stream(metadata_stream, Instant::now() + io_timeout).unwrap(),
                next_request_id: 1,
                io_timeout,
                pg_id: PgId::new(3),
                released: true,
            }),
        };
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "metadata command session limit reached".to_string(),
        };
        assert!(matches!(
            metadata_session
                .rpc_response_error(StorageRpcMessageKind::MetadataCommandPgLockAcquire, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 9,
                operation: "metadata command PG lock acquire",
                ref detail,
            } if detail.as_str().contains("session limit")
        ));

        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ShardDeleteInProgress,
            message: "shard is being deleted".to_string(),
        };
        assert!(matches!(
            metadata_session
                .rpc_response_error(StorageRpcMessageKind::MetadataCommandPgLockAcquire, err),
            StoreError::StorageRpcShardDeleteInProgress {
                node_id: 9,
                operation: "metadata command PG lock acquire",
                ref detail,
            } if detail.as_str().contains("being deleted")
        ));
    }
}
