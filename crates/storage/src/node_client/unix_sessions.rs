use super::*;

pub(crate) struct UnixStorageNodeReadHandleSession {
    node_id: NodeId,
    stream: UnixStream,
    next_request_id: u64,
    _rpc_permit: UnixStorageNodeRpcAdmissionPermit,
}

pub(crate) struct UnixStorageNodeMetadataCommandSession {
    node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    _rpc_permit: UnixStorageNodeRpcAdmissionPermit,
    inner: Mutex<UnixStorageNodeMetadataCommandSessionInner>,
}

struct UnixStorageNodeMetadataCommandSessionInner {
    stream: UnixStream,
    next_request_id: u64,
    pg_id: PgId,
    released: bool,
}

struct UnixStorageNodeReadHandleLease {
    session: UnixStorageNodeReadHandleSession,
    read_operation_id: String,
    released: bool,
}

impl UnixStorageNodeClient {
    pub(crate) fn open_read_handle_session(
        &self,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(StorageRpcMessageKind::ReadHandlesAcquire)?;
        let stream = UnixStream::connect(&self.socket_path).map_err(|source| StoreError::Io {
            context: "connect storage-node read-handle RPC socket",
            source,
        })?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            stream,
            next_request_id: 1,
            _rpc_permit: rpc_permit,
        })
    }

    pub(crate) fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let rpc_permit =
            self.acquire_rpc_admission(StorageRpcMessageKind::MetadataCommandPgLockAcquire)?;
        let stream = UnixStream::connect(&self.socket_path).map_err(|source| StoreError::Io {
            context: "connect storage-node metadata command RPC socket",
            source,
        })?;
        let session = UnixStorageNodeMetadataCommandSession {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            _rpc_permit: rpc_permit,
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream,
                next_request_id: 1,
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
            locations,
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
        Ok(response.locations)
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

    fn rpc_request(
        &mut self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
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
        write_storage_rpc_frame_to(&mut self.stream, &request).map_err(|error| {
            self.rpc_payload_error("write read-handle RPC request", error.to_string())
        })?;
        let response = read_storage_rpc_frame_from(&mut self.stream).map_err(|error| {
            self.rpc_payload_error("read read-handle RPC response", error.to_string())
        })?;
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
            code: StorageRpcErrorCode::PayloadDecode,
            message,
        }
    }
}

impl UnixStorageNodeMetadataCommandSession {
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
        write_storage_rpc_frame_to(&mut inner.stream, &request).map_err(|error| {
            self.rpc_payload_error(
                "write metadata command session RPC request",
                error.to_string(),
            )
        })?;
        let response = read_storage_rpc_frame_from(&mut inner.stream).map_err(|error| {
            self.rpc_payload_error(
                "read metadata command session RPC response",
                error.to_string(),
            )
        })?;
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
            code: StorageRpcErrorCode::PayloadDecode,
            message,
        }
    }

    fn release_metadata_command_pg_lock(&self) -> Result<(), StoreError> {
        let pg_id = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.released {
                return Ok(());
            }
            inner.pg_id
        };
        let payload = self.encode_metadata_command_state_request(pg_id);
        self.rpc_request(StorageRpcMessageKind::MetadataCommandPgLockRelease, payload)?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.released = true;
        Ok(())
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
}
impl Drop for UnixStorageNodeMetadataCommandSession {
    fn drop(&mut self) {
        let _ = self.release_metadata_command_pg_lock();
    }
}
impl MetadataCommandNodeClient for UnixStorageNodeMetadataCommandSession {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandNodeClient>, StoreError> {
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
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: bucket.cloned(),
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

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
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

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
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
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!("checkpoint candidate limit {limit} exceeds u32::MAX"),
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

    fn validate_metadata_command_replay_state(
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
            StorageRpcMessageKind::MetadataCommandValidateReplayState,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replay state response",
                    error.to_string(),
                )
            })
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
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
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replay state response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_replica_state_can_initialize(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<bool, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replica state can initialize response",
                    error.to_string(),
                )
            })
    }

    fn initialize_metadata_transfer_empty_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandTransferEmptyStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            expected_state_digest,
        };
        let payload = encode_metadata_command_transfer_empty_state_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command transfer empty state response",
                    error.to_string(),
                )
            })
    }

    fn initialize_metadata_transfer_matching_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandTransferMatchingStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            applied_log_index,
            applied_log_hash,
            expected_state_digest,
        };
        let payload = encode_metadata_command_transfer_matching_state_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command transfer matching state response",
                    error.to_string(),
                )
            })
    }

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        commands: &[MetadataTransferCommand],
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandTransferAdoptRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            expected_state_digest,
            commands: commands.to_vec(),
        };
        let payload =
            encode_metadata_command_transfer_adopt_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command transfer state adopt request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandTransferStateAdopt,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command transfer state adopt response",
                    error.to_string(),
                )
            })
    }

    fn install_metadata_transfer_checkpoint_base(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandTransferCheckpointBaseRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            checkpoint: checkpoint.clone(),
        };
        let payload = encode_metadata_command_transfer_checkpoint_base_request(&request).map_err(
            |error| {
                self.rpc_payload_error(
                    "encode metadata command transfer checkpoint base request",
                    error.to_string(),
                )
            },
        )?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command transfer checkpoint base response",
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

    fn replay_metadata_command_for_peering(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.metadata_command_apply_and_record_with_kind(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord,
            "decode metadata command peering replay response",
        )
    }

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
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

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandAbandoned, payload)?;
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command abandoned response",
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
                "decode metadata command abandoned response",
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
impl ShardReadHandleNodeClient for UnixStorageNodeClient {
    fn acquire_read_handles(
        &self,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        let mut session = self.open_read_handle_session()?;
        session.acquire_read_handles(read_operation_id, entries)?;
        Ok(Box::new(UnixStorageNodeReadHandleLease {
            session,
            read_operation_id: read_operation_id.to_string(),
            released: false,
        }))
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
                ref message,
            } if message.contains("active read handles")
        ));

        let (read_stream, _read_peer) = UnixStream::pair().unwrap();
        let read_session = UnixStorageNodeReadHandleSession {
            node_id: NodeId::new(8),
            stream: read_stream,
            next_request_id: 1,
            _rpc_permit: test_rpc_admission_permit(),
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
                ref message,
            } if message.contains("session limit")
        ));

        let (metadata_stream, _metadata_peer) = UnixStream::pair().unwrap();
        let metadata_session = UnixStorageNodeMetadataCommandSession {
            node_id: NodeId::new(9),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            _rpc_permit: test_rpc_admission_permit(),
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream: metadata_stream,
                next_request_id: 1,
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
                ref message,
            } if message.contains("session limit")
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
                ref message,
            } if message.contains("being deleted")
        ));
    }
}
