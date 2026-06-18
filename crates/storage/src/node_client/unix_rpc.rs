use super::*;

impl UnixStorageNodeClient {
    pub(crate) fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(StorageRpcMessageKind::ShardWrite)?;
        let expected_size = data.len() as u64;
        let expected_crc64 = checksum::crc64::checksum(data);
        let request = StorageRpcShardWriteRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_size,
            expected_crc64,
            payload: data.to_vec(),
        };
        let payload = encode_shard_write_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard write request", error.to_string())
        })?;
        let response =
            self.rpc_request_with_permit(StorageRpcMessageKind::ShardWrite, payload, rpc_permit)?;
        decode_shard_write_ack(&response, expected_size, expected_crc64).map_err(|error| {
            self.rpc_payload_error("decode shard write response", error.to_string())
        })
    }

    pub(crate) fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_ack,
        };
        let payload = encode_shard_read_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard read request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardRead, payload)?;
        decode_shard_read_response(&response, expected_ack).map_err(|error| {
            self.rpc_payload_error("decode shard read response", error.to_string())
        })
    }

    pub(crate) fn read_placed_shard_range(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRangeRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
            expected_ack,
            offset,
            length,
        };
        let payload = encode_shard_read_range_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard read range request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardReadRange, payload)?;
        decode_shard_read_range_response(&response, length as usize).map_err(|error| {
            self.rpc_payload_error("decode shard read range response", error.to_string())
        })
    }

    pub(crate) fn delete_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        let request = StorageRpcShardDeleteRequest {
            location: self.shard_location(data_pg_id, key),
            shard_key: key.clone(),
        };
        let payload = encode_shard_delete_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard delete request", error.to_string())
        })?;
        self.rpc_request(StorageRpcMessageKind::ShardDelete, payload)
            .map(|_| ())
    }

    pub(crate) fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckRecord, payload)
            .map(|_| ())
    }

    pub(crate) fn validate_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckValidate, payload)
            .map(|_| ())
    }

    pub(crate) fn load_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        let payload = self.encode_shard_ack_item(pg_id, key);
        let response = self.rpc_request(StorageRpcMessageKind::ShardAckLoad, payload)?;
        let item = decode_shard_ack_item_response(&response).map_err(|error| {
            self.rpc_payload_error("decode shard ack load response", error.to_string())
        })?;
        if item.shard_key != *key {
            return Err(self.rpc_payload_error(
                "validate shard ack load response",
                "shard key does not match request".to_string(),
            ));
        }
        Ok(item.ack)
    }

    pub(crate) fn delete_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_item(pg_id, key);
        let response = self.rpc_request(StorageRpcMessageKind::ShardAckDelete, payload)?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate shard ack delete response",
                "shard ack delete response payload must be empty".to_string(),
            ))
        }
    }

    pub(crate) fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        let request = StorageRpcScavengerListFilesRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            data_pg_id,
        };
        let payload = encode_scavenger_list_files_request(&request);
        let response = self.rpc_request(StorageRpcMessageKind::ShardScavengerListFiles, payload)?;
        decode_scavenger_list_files_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode shard scavenger list files response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn list_scavenger_shard_rows(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ScavengerShardRow>, StoreError> {
        let request = self.bucket_pg_request(pg_id);
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode shard scavenger shard rows request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardScavengerShardRows, payload)?;
        decode_scavenger_shard_rows_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode shard scavenger shard rows response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn list_shard_scavenger_payload_references(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let request = self.bucket_pg_request(pg_id);
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode shard scavenger payload references request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::ShardScavengerPayloadReferences,
            payload,
        )?;
        decode_scavenger_payload_references_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode shard scavenger payload references response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn record_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        let request = StorageRpcScavengerObservationRecordRequest {
            route: self.bucket_pg_request(pg_id),
            observation: observation.clone(),
        };
        let payload = encode_scavenger_observation_record_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode shard scavenger observation record request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::ShardScavengerObservationRecord,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate shard scavenger observation record response",
                "shard scavenger observation record response payload must be empty".to_string(),
            ))
        }
    }

    pub(crate) fn list_shard_scavenger_observations(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let request = self.bucket_pg_request(pg_id);
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode shard scavenger observations request",
                error.to_string(),
            )
        })?;
        let response =
            self.rpc_request(StorageRpcMessageKind::ShardScavengerObservations, payload)?;
        decode_scavenger_observations_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode shard scavenger observations response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn resolve_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        let request = StorageRpcScavengerObservationKeyRequest {
            route: self.bucket_pg_request(pg_id),
            key: key.clone(),
        };
        let payload = encode_scavenger_observation_key_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode shard scavenger observation resolve request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::ShardScavengerObservationResolve,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate shard scavenger observation resolve response",
                "shard scavenger observation resolve response payload must be empty".to_string(),
            ))
        }
    }

    pub(crate) fn record_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairRecordRequest {
            route: self.bucket_pg_request(pg_id),
            work_item: *work_item,
            last_error: last_error.map(ToOwned::to_owned),
        };
        let payload =
            encode_placed_segment_shard_repair_record_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard repair record request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardRepairRecord,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate placed segment shard repair record response",
                "placed segment shard repair record response payload must be empty".to_string(),
            ))
        }
    }

    pub(crate) fn list_placed_segment_shard_repairs(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let request = self.bucket_pg_request(pg_id);
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode placed segment shard repairs request",
                error.to_string(),
            )
        })?;
        let response =
            self.rpc_request(StorageRpcMessageKind::PlacedSegmentShardRepairs, payload)?;
        decode_placed_segment_shard_repairs_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode placed segment shard repairs response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn acquire_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        acquire: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: self.bucket_pg_request(pg_id),
            claim_id: acquire.claim_id.clone(),
            owner_token: acquire.owner_token.clone(),
            claimed_at: acquire.claimed_at,
            lease_deadline: acquire.lease_deadline,
            now: acquire.now,
        };
        let payload = encode_placed_segment_shard_repair_claim_acquire_request(&request).map_err(
            |error| {
                self.rpc_payload_error(
                    "encode placed segment shard repair claim acquire request",
                    error.to_string(),
                )
            },
        )?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire,
            payload,
        )?;
        decode_placed_segment_shard_repair_claim_optional_record_response(&response)
            .map(|response| response.record)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard repair claim acquire response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn complete_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
            route: self.bucket_pg_request(pg_id),
            claim: claim.clone(),
        };
        let payload =
            encode_placed_segment_shard_repair_claim_record_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard repair claim complete request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard repair claim complete response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn record_placed_segment_shard_repair_claim_error(
        &self,
        pg_id: PgId,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
            route: self.bucket_pg_request(pg_id),
            claim: claim.clone(),
            last_error: last_error.to_string(),
            next_attempt_after,
        };
        let payload =
            encode_placed_segment_shard_repair_claim_error_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard repair claim error request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimError,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard repair claim error response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn resolve_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairItemRequest {
            route: self.bucket_pg_request(pg_id),
            work_item: *work_item,
        };
        let payload =
            encode_placed_segment_shard_repair_item_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard repair resolve request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardRepairResolve,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate placed segment shard repair resolve response",
                "placed segment shard repair resolve response payload must be empty".to_string(),
            ))
        }
    }

    fn encode_shard_ack_batch(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardAckBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            items: shard_batch
                .iter()
                .map(|(shard_key, ack)| StorageRpcShardAckItem {
                    shard_key: (*shard_key).clone(),
                    ack: *ack,
                })
                .collect(),
        };
        encode_shard_ack_batch_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard ack batch request", error.to_string())
        })
    }

    fn encode_shard_ack_item(&self, pg_id: PgId, key: &ShardKey) -> Vec<u8> {
        encode_shard_ack_item_request(&StorageRpcShardAckItemRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            shard_key: key.clone(),
        })
    }
    fn shard_location(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> crate::cluster::ShardLocation {
        crate::cluster::ShardLocation::new(
            self.cluster_epoch,
            data_pg_id,
            key.shard_index(),
            self.node_id,
        )
    }
}

impl PlacedShardNodeClient for UnixStorageNodeClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::write_placed_shard(self, data_pg_id, key, data)
    }

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        UnixStorageNodeClient::read_placed_shard(self, data_pg_id, key, expected_ack)
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        if dst.len() as u64 != expected_ack.stored_size {
            return Err(self.rpc_payload_error(
                "shard read range",
                format!(
                    "remote shard read buffer is {} bytes for expected {} byte shard",
                    dst.len(),
                    expected_ack.stored_size
                ),
            ));
        }
        let data = UnixStorageNodeClient::read_placed_shard_range(
            self,
            data_pg_id,
            key,
            expected_ack,
            0,
            dst.len() as u64,
        )?;
        dst.copy_from_slice(&data);
        Ok(())
    }

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_placed_shard(self, data_pg_id, key)
    }
}

impl ShardAckNodeClient for UnixStorageNodeClient {
    fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::register_written_shard_acks(self, pg_id, shard_batch)
    }

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::validate_written_shard_acks(self, pg_id, &[(key, ack)])
    }

    fn load_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::load_written_shard_ack(self, pg_id, key)
    }

    fn delete_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_written_shard_ack(self, pg_id, key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::record_placed_segment_shard_repair(
            self, pg_id, work_item, last_error,
        )
    }

    fn list_placed_segment_shard_repairs(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        UnixStorageNodeClient::list_placed_segment_shard_repairs(self, pg_id)
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::acquire_placed_segment_shard_repair_claim(self, pg_id, request)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::complete_placed_segment_shard_repair_claim(self, pg_id, claim)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::record_placed_segment_shard_repair_claim_error(
            self,
            pg_id,
            claim,
            last_error,
            next_attempt_after,
        )
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::resolve_placed_segment_shard_repair(self, pg_id, work_item)
    }
}

impl ShardScavengerNodeClient for UnixStorageNodeClient {
    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_files(self, data_pg_id)
    }

    fn list_scavenger_shard_rows(&self, pg_id: PgId) -> Result<Vec<ScavengerShardRow>, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_rows(self, pg_id)
    }

    fn list_shard_scavenger_payload_references(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        UnixStorageNodeClient::list_shard_scavenger_payload_references(self, pg_id)
    }

    fn record_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::record_shard_scavenger_observation(self, pg_id, observation)
    }

    fn list_shard_scavenger_observations(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        UnixStorageNodeClient::list_shard_scavenger_observations(self, pg_id)
    }

    fn resolve_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::resolve_shard_scavenger_observation(self, pg_id, key)
    }
}

#[allow(dead_code)]
impl UnixStorageNodeClient {
    pub(crate) fn new(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        let socket_path = socket_path.into();
        let rpc_admission = shared_unix_storage_node_rpc_admission(
            node_id,
            &socket_path,
            UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
        );
        Self::with_rpc_admission(node_id, cluster_epoch, socket_path, rpc_admission)
    }

    pub(crate) fn with_rpc_admission(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: PathBuf,
        rpc_admission: Arc<UnixStorageNodeRpcAdmission>,
    ) -> Self {
        Self {
            node_id,
            cluster_epoch,
            socket_path,
            next_request_id: AtomicU64::new(1),
            rpc_admission,
        }
    }

    pub(crate) fn with_rpc_admission_settings(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
        rpc_admission_limit: usize,
        rpc_admission_wait_timeout: Duration,
        rpc_control_admission_wait_timeout: Duration,
    ) -> Self {
        Self::with_rpc_admission(
            node_id,
            cluster_epoch,
            socket_path.into(),
            Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
                rpc_admission_limit,
                rpc_admission_wait_timeout,
                rpc_control_admission_wait_timeout,
            )),
        )
    }

    pub(crate) fn with_rpc_admission_limit(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
        rpc_admission_limit: usize,
    ) -> Self {
        Self::with_rpc_admission_settings(
            node_id,
            cluster_epoch,
            socket_path,
            rpc_admission_limit,
            UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
        )
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn rpc_request(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_with_admission_class(kind, payload, storage_rpc_admission_class(kind))
    }

    pub(crate) fn rpc_request_with_admission_class(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Result<Vec<u8>, StoreError> {
        let rpc_permit = self.acquire_rpc_admission_with_class(kind, class)?;
        self.rpc_request_with_permit(kind, payload, rpc_permit)
    }

    pub(crate) fn rpc_request_with_permit(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        rpc_permit: UnixStorageNodeRpcAdmissionPermit,
    ) -> Result<Vec<u8>, StoreError> {
        match self.rpc_request_result_with_permit(kind, payload, rpc_permit)? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    pub(crate) fn rpc_request_result(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(kind)?;
        self.rpc_request_result_with_permit(kind, payload, rpc_permit)
    }

    pub(crate) fn rpc_request_result_with_permit(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        rpc_permit: UnixStorageNodeRpcAdmissionPermit,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let _pending_envelope_guard =
            (kind == StorageRpcMessageKind::MetadataCommandPendingEnvelope).then(|| {
                observability::storage_rpc_pending_envelope_guard(
                    observability::StorageRpcActiveSummary {
                        node_id: self.node_id.as_u32(),
                        rpc_request_id: request_id,
                        rpc_kind: kind.operation_name(),
                        admission_class: rpc_permit.class.as_str(),
                    },
                )
            });
        let trace_rpc_lifecycle = trace_storage_rpc_lifecycle(kind);
        if trace_rpc_lifecycle {
            let _ = observability::emit_flight_event(
                "storage_rpc_client",
                "storage_rpc_client_start",
                format!(
                    "node_id={} rpc_request_id={} kind={}",
                    self.node_id.as_u32(),
                    request_id,
                    kind.operation_name()
                ),
            );
        }
        let mut stream = match UnixStream::connect(&self.socket_path) {
            Ok(stream) => {
                if trace_rpc_lifecycle {
                    let _ = observability::emit_flight_event(
                        "storage_rpc_client",
                        "storage_rpc_client_connected",
                        format!(
                            "node_id={} rpc_request_id={} kind={} elapsed_us={}",
                            self.node_id.as_u32(),
                            request_id,
                            kind.operation_name(),
                            started.elapsed().as_micros()
                        ),
                    );
                }
                stream
            }
            Err(source) => {
                if trace_rpc_lifecycle {
                    let _ = observability::emit_flight_event(
                        "storage_rpc_client",
                        "storage_rpc_client_connect_failed",
                        format!(
                            "node_id={} rpc_request_id={} kind={} elapsed_us={} error={}",
                            self.node_id.as_u32(),
                            request_id,
                            kind.operation_name(),
                            started.elapsed().as_micros(),
                            source
                        ),
                    );
                }
                return Err(StoreError::Io {
                    context: "connect storage-node RPC socket",
                    source,
                });
            }
        };
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        if let Err(error) = write_storage_rpc_frame_to(&mut stream, &request) {
            if trace_rpc_lifecycle {
                let _ = observability::emit_flight_event(
                    "storage_rpc_client",
                    "storage_rpc_client_write_failed",
                    format!(
                        "node_id={} rpc_request_id={} kind={} elapsed_us={} error={}",
                        self.node_id.as_u32(),
                        request_id,
                        kind.operation_name(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
            }
            return Err(self.rpc_payload_error("write storage RPC request", error.to_string()));
        }
        if trace_rpc_lifecycle {
            let _ = observability::emit_flight_event(
                "storage_rpc_client",
                "storage_rpc_client_request_written",
                format!(
                    "node_id={} rpc_request_id={} kind={} elapsed_us={}",
                    self.node_id.as_u32(),
                    request_id,
                    kind.operation_name(),
                    started.elapsed().as_micros()
                ),
            );
        }
        let response = match read_storage_rpc_frame_from(&mut stream) {
            Ok(response) => {
                if trace_rpc_lifecycle {
                    let _ = observability::emit_flight_event(
                        "storage_rpc_client",
                        "storage_rpc_client_response_read",
                        format!(
                            "node_id={} rpc_request_id={} kind={} elapsed_us={}",
                            self.node_id.as_u32(),
                            request_id,
                            kind.operation_name(),
                            started.elapsed().as_micros()
                        ),
                    );
                }
                response
            }
            Err(error) => {
                if trace_rpc_lifecycle {
                    let _ = observability::emit_flight_event(
                        "storage_rpc_client",
                        "storage_rpc_client_read_failed",
                        format!(
                            "node_id={} rpc_request_id={} kind={} elapsed_us={} error={}",
                            self.node_id.as_u32(),
                            request_id,
                            kind.operation_name(),
                            started.elapsed().as_micros(),
                            error
                        ),
                    );
                }
                return Err(self.rpc_payload_error("read storage RPC response", error.to_string()));
            }
        };
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate storage RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        decode_storage_rpc_response_payload(&response.payload).map_err(|error| {
            self.rpc_payload_error("decode storage RPC response", error.to_string())
        })
    }

    pub(crate) fn acquire_rpc_admission(
        &self,
        kind: StorageRpcMessageKind,
    ) -> Result<UnixStorageNodeRpcAdmissionPermit, StoreError> {
        self.acquire_rpc_admission_with_class(kind, storage_rpc_admission_class(kind))
    }

    pub(crate) fn acquire_rpc_admission_with_class(
        &self,
        kind: StorageRpcMessageKind,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Result<UnixStorageNodeRpcAdmissionPermit, StoreError> {
        observability::emit_storage_rpc_admission_attempt();
        let wait_timeout = self.rpc_admission.wait_timeout_for_class(class);
        match self.rpc_admission.acquire_with_kind(class, kind) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, wait_us } => {
                if wait_us > 0 {
                    let _ = observability::emit_storage_rpc_admission_wait(
                        "storage_node_client",
                        observability::StorageRpcAdmissionSummary {
                            node_id: self.node_id.as_u32(),
                            rpc_kind: kind.operation_name(),
                            admission_class: class.as_str(),
                            wait_us,
                            timeout_us: Some(wait_timeout.as_micros()),
                        },
                    );
                }
                Ok(permit)
            }
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { wait_us } => {
                let _ = observability::emit_storage_rpc_admission_timeout(
                    "storage_node_client",
                    observability::StorageRpcAdmissionSummary {
                        node_id: self.node_id.as_u32(),
                        rpc_kind: kind.operation_name(),
                        admission_class: class.as_str(),
                        wait_us,
                        timeout_us: Some(wait_timeout.as_micros()),
                    },
                );
                Err(StoreError::StorageRpcResourceExhausted {
                    node_id: self.node_id.as_u32(),
                    operation: kind.operation_name(),
                    message: format!(
                    "storage-node client RPC admission limit {} is exhausted after waiting {} ms",
                    self.rpc_admission.limit,
                    wait_timeout.as_millis()
                ),
                })
            }
        }
    }

    pub(crate) fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        if error.code == StorageRpcErrorCode::ShardDeleteInProgress {
            return StoreError::StorageRpcShardDeleteInProgress {
                node_id: self.node_id.as_u32(),
                operation: kind.operation_name(),
                message: error.message,
            };
        }
        if error.code == StorageRpcErrorCode::ResourceExhausted {
            return StoreError::StorageRpcResourceExhausted {
                node_id: self.node_id.as_u32(),
                operation: kind.operation_name(),
                message: error.message,
            };
        }
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation: kind.operation_name(),
            message: format!("{:?}: {}", error.code, error.message),
        }
    }

    pub(crate) fn bucket_snapshot_rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> BucketSnapshotLoadError {
        match error.code {
            StorageRpcErrorCode::ReclaimClaimNotFound => {
                BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimNotFound {
                    claim_id: error.message,
                })
            }
            StorageRpcErrorCode::ReclaimClaimConflict => {
                BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict {
                    claim_id: error.message,
                })
            }
            _ => BucketSnapshotLoadError::Store(self.rpc_response_error(kind, error)),
        }
    }

    pub(crate) fn rpc_request_bucket_snapshot(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, BucketSnapshotLoadError> {
        match self
            .rpc_request_result(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?
        {
            Ok(response) => Ok(response),
            Err(error) => Err(self.bucket_snapshot_rpc_response_error(kind, error)),
        }
    }

    pub(crate) fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            message,
        }
    }
}

impl UnixStorageNodeClient {
    pub(crate) fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
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

    pub(crate) fn max_metadata_command_log_index(&self, pg_id: PgId) -> Result<u64, StoreError> {
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

    #[allow(dead_code)]
    pub(crate) fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
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

    #[allow(dead_code)]
    pub(crate) fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
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

    pub(crate) fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
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

    pub(crate) fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least_with_admission_class(
            pg_id,
            min_log_index,
            storage_rpc_admission_class(StorageRpcMessageKind::MetadataCommandNextId),
        )
    }

    pub(crate) fn next_completion_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least_with_admission_class(
            pg_id,
            min_log_index,
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }

    fn next_metadata_command_id_at_least_with_admission_class(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Result<MetadataCommandId, StoreError> {
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            min_log_index: min_log_index.get(),
        };
        let payload = encode_metadata_command_next_id_request(&request);
        let response = self.rpc_request_with_admission_class(
            StorageRpcMessageKind::MetadataCommandNextId,
            payload,
            class,
        )?;
        let decoded = decode_metadata_command_next_id_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command next id response",
                error.to_string(),
            )
        })?;
        let (cluster_epoch, decoded_pg_id, log_index) = match decoded.outcome {
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch,
                pg_id,
                log_index,
            } => (cluster_epoch, pg_id, log_index),
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            } => {
                return Err(metadata_command_log_conflict_error(
                    self.cluster_epoch,
                    request.pg_id,
                    "decode metadata command next id response",
                    |operation, message| self.rpc_payload_error(operation, message),
                    MetadataCommandLogConflictRpcFields {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ));
            }
        };
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

    pub(crate) fn encode_metadata_command_state_request(&self, pg_id: PgId) -> Vec<u8> {
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

    pub(crate) fn metadata_command_acceptance(
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

    pub(crate) fn metadata_command_abandon_acceptance(
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

    pub(crate) fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        preserve_pending_slot: bool,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let kind = if preserve_pending_slot {
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        } else {
            StorageRpcMessageKind::MetadataCommandValidateReplayState
        };
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(kind, payload)?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replay state response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn applied_metadata_command_log_entry_hashes(
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

    pub(crate) fn has_matching_applied_metadata_command_log_entry(
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

    pub(crate) fn metadata_command_abandoned(
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

    pub(crate) fn record_metadata_command_abandoned(
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

    pub(crate) fn apply_metadata_command_and_record(
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

    #[allow(dead_code)]
    pub(crate) fn replay_metadata_command_for_peering(
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
                    "decode metadata command apply and record response",
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
                "decode metadata command apply and record response",
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
                "decode metadata command apply and record response",
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

    pub(crate) fn try_insert_pending_metadata_command_slot(
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
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command pending slot insert response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command pending slot insert response",
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

    pub(crate) fn try_insert_bucket_control_pending_metadata_command_slot(
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
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command bucket-control pending slot insert response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command bucket-control pending slot insert response",
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

    pub(crate) fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        let payload = encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot remove request",
                error.to_string(),
            )
        })?;
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

    pub(crate) fn replace_pending_metadata_command_slot_for_reissue(
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

    fn metadata_command_acceptance_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        let payload = encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error("encode metadata command request", error.to_string())
        })?;
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
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command acceptance response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command acceptance response",
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
}

impl MetadataCommandNodeClient for UnixStorageNodeClient {
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
        Ok(Box::new(
            UnixStorageNodeClient::open_metadata_command_critical_section(self, pg_id)?,
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
        UnixStorageNodeClient::max_metadata_command_log_index(self, pg_id)
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
        UnixStorageNodeClient::next_metadata_command_id_at_least(self, pg_id, min_log_index)
    }

    fn next_completion_metadata_command_id_at_least(
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
        UnixStorageNodeClient::next_completion_metadata_command_id_at_least(
            self,
            pg_id,
            min_log_index,
        )
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
        UnixStorageNodeClient::pending_metadata_command_envelope(self, pg_id)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::try_insert_pending_metadata_command_slot(
            self, pg_id, command, bucket,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
            self, pg_id, command, bucket,
        )
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::remove_pending_metadata_command_slot(self, pg_id, command)
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::replace_pending_metadata_command_slot_for_reissue(
            self,
            pg_id,
            previous,
            replacement,
            bucket,
        )
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::validate_metadata_command_replay_state(self, pg_id, false)
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::validate_metadata_command_replay_state(self, pg_id, true)
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        UnixStorageNodeClient::metadata_command_acceptance(self, pg_id, command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        UnixStorageNodeClient::metadata_command_abandon_acceptance(self, pg_id, command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        UnixStorageNodeClient::applied_metadata_command_log_entry_hashes(self, pg_id, command)
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        UnixStorageNodeClient::retained_metadata_command_log_hashes(
            self,
            pg_id,
            cluster_epoch,
            first_log_index,
            last_log_index,
        )
    }

    fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
        UnixStorageNodeClient::retained_metadata_command_log_entries(
            self,
            pg_id,
            cluster_epoch,
            first_log_index,
            last_log_index,
        )
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::has_matching_applied_metadata_command_log_entry(
            self,
            pg_id,
            command,
            expected_previous_log_hash,
        )
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        UnixStorageNodeClient::apply_metadata_command_and_record(self, pg_id, command)
    }

    fn replay_metadata_command_for_peering(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        UnixStorageNodeClient::replay_metadata_command_for_peering(self, pg_id, command)
    }

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::record_metadata_command_abandoned(self, pg_id, command)
    }

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::metadata_command_abandoned(self, pg_id, command)
    }
}

impl BucketMetadataNodeClient for UnixStorageNodeClient {
    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadRaw, pg_id, bucket)
    }

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadInfo, pg_id, bucket)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            request,
        };
        let payload = encode_bucket_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketSnapshotLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_snapshot_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket snapshot response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
                if snapshot.bucket.name != *bucket || snapshot.request != request.request {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot response",
                        "bucket snapshot response identity does not match request".to_string(),
                    )));
                }
                Ok(*snapshot)
            }
            StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
        }
    }

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotPairRequest {
            source: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: source_pg_id,
                    bucket: source.0.clone(),
                },
                request: source.1,
            },
            destination: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: destination_pg_id,
                    bucket: destination.0.clone(),
                },
                request: destination.1,
            },
        };
        let payload = encode_bucket_snapshot_pair_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketSnapshotPairLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_snapshot_pair_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket snapshot pair response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcBucketSnapshotPairOutcome::Loaded(pair) => {
                self.validate_bucket_snapshot_pair_response(&pair, source, destination)?;
                Ok(*pair)
            }
            StorageRpcBucketSnapshotPairOutcome::BucketNotFound { name } => Err(
                BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }),
            ),
        }
    }

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        let request = StorageRpcCreateBucketCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            command_id,
            config: StorageRpcCreateBucketConfig {
                name: BucketName::try_from(config.name).map_err(|reason| {
                    BucketSnapshotLoadError::Metadata(MetadataError::InvalidBucketName {
                        reason: reason.to_string(),
                    })
                })?,
                owner_principal: config.owner_principal.to_string(),
                owner_canonical_id: config.owner_canonical_id.clone(),
                acl_grants: config.acl_grants.clone(),
                public_read: config.public_read,
                public_write: config.public_write,
                versioning: config.versioning,
                object_lock: config.object_lock,
                ownership_controls: config.ownership_controls,
            },
        };
        let payload = encode_create_bucket_command_build_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode create-bucket command build request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketCreateCommandBuild, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_create_bucket_command_build_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode create-bucket command build response",
                error.to_string(),
            ))
        })?;
        self.validate_create_bucket_command_build_outcome(
            response.outcome,
            bucket,
            command_id,
            config,
        )
    }

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        let request = StorageRpcCompletedMultipartOrderCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            command_id,
        };
        let payload =
            encode_completed_multipart_order_command_build_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode completed multipart order command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::CompletedMultipartOrderCommandBuild,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_completed_multipart_order_command_build_response(&response).map_err(
            |error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode completed multipart order command build response",
                    error.to_string(),
                ))
            },
        )?;
        self.validate_completed_multipart_order_command_build_response(
            response.completion_order,
            response.command,
            bucket,
            command_id,
        )
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::MarkBucketDeleting(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id,
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::MarkDeleting,
        )
    }

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            command_id,
        };
        let payload =
            encode_bucket_mark_deleting_command_build_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket mark-deleting command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketMarkDeletingCommandBuild,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_mark_deleting_command_build_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket mark-deleting command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
                self.validate_mark_bucket_deleting_already_deleting_response(&info, bucket)?;
                Ok(MarkBucketDeletingCommandBuild::AlreadyDeleting)
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command) => {
                let command = self.validate_mark_bucket_deleting_command_build_response(
                    *command, bucket, command_id,
                )?;
                Ok(MarkBucketDeletingCommandBuild::Command(Box::new(command)))
            }
        }
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketVersioning(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id,
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Versioning(state),
        )
    }

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id,
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Versioning(state),
        )
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketAcl(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id,
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: acl_grants.clone(),
                public_read,
                public_write,
            },
        )
    }

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id,
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: acl_grants.clone(),
                public_read,
                public_write,
            },
        )
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketProperty(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id,
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Property(mutation.clone()),
        )
    }

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id,
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Property(mutation.clone()),
        )
    }

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id,
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Subresource(mutation.clone()),
        )
    }

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSubresourceGetRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            kind,
        };
        let payload = encode_bucket_subresource_get_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketSubresourceGet, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_subresource_get_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket subresource get response", error.to_string()),
            )
        })?;
        Ok(response.body)
    }

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketListRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            owner_canonical_id: owner_canonical_id.to_string(),
        };
        let payload = encode_bucket_list_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode bucket list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_list_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode bucket list response", error.to_string()),
            )
        })?;
        for bucket in &response.buckets {
            if bucket.owner_canonical_id.as_str() != owner_canonical_id {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket list response",
                    format!(
                        "bucket {} owner does not match requested owner",
                        bucket.name.as_str()
                    ),
                )));
            }
        }
        Ok(response.buckets)
    }

    fn load_bucket_execution_generations(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            buckets: buckets.to_vec(),
        };
        let payload = encode_bucket_batch_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket execution generations request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketExecutionGenerations, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_execution_generations_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket execution generations response",
                    error.to_string(),
                ))
            })?;
        for bucket in response.generations.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket execution generations response",
                    format!("unexpected bucket {}", bucket.as_str()),
                )));
            }
        }
        Ok(response.generations)
    }

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            buckets: buckets.to_vec(),
        };
        let payload = encode_bucket_batch_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket fast-path identities request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketFastPathIdentities, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_fast_path_identities_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket fast-path identities response",
                error.to_string(),
            ))
        })?;
        for bucket in response.identities.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket fast-path identities response",
                    format!("unexpected bucket {}", bucket.as_str()),
                )));
            }
        }
        Ok(response.identities)
    }
}
