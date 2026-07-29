use super::*;
use crate::BucketAclSummary;

impl UnixStorageNodeClient {
    pub(crate) fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.write_placed_shard_with_kind(
            StorageRpcMessageKind::ShardWrite,
            data_pg_id,
            key,
            data,
            None,
        )
    }

    pub(crate) fn write_placed_shard_with_effect_fence(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StoreError> {
        if operation_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: data_pg_id.get(),
                operation_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        effect_fence.require_valid_for(operation_epoch)?;
        self.write_placed_shard_with_kind(
            StorageRpcMessageKind::ShardWrite,
            data_pg_id,
            key,
            data,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
        )
    }

    pub(crate) fn repair_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.write_placed_shard_with_kind(
            StorageRpcMessageKind::ShardRepairWrite,
            data_pg_id,
            key,
            data,
            None,
        )
    }

    fn write_placed_shard_with_kind(
        &self,
        kind: StorageRpcMessageKind,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    ) -> Result<WriteAck, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(kind)?;
        let expected_size = data.len() as u64;
        let expected_crc64 = checksum::crc64::checksum(data);
        let request = StorageRpcShardWriteRequest {
            location: self.shard_location(data_pg_id, key).into(),
            shard_key: key.clone(),
            expected_size,
            expected_crc64,
            effect_deadline,
            payload: data.to_vec(),
        };
        let payload = encode_shard_write_request(&request).map_err(|error| {
            self.rpc_payload_error("encode shard write request", error.to_string())
        })?;
        let response = self.rpc_request_with_permit(kind, payload, rpc_permit)?;
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
            location: self.shard_location(data_pg_id, key).into(),
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

    pub(crate) fn read_placed_shard_for_historical_inspection(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRequest {
            location: location.into(),
            shard_key: key.clone(),
            expected_ack,
        };
        let payload = encode_shard_read_request(&request).map_err(|error| {
            self.rpc_payload_error("encode historical shard read request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardHistoricalRead, payload)?;
        decode_shard_read_response(&response, expected_ack).map_err(|error| {
            self.rpc_payload_error("decode historical shard read response", error.to_string())
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
            location: self.shard_location(data_pg_id, key).into(),
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
        self.delete_placed_shard_at_location(self.shard_location(data_pg_id, key), key)
    }

    pub(crate) fn delete_placed_shard_at_location(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        let request = StorageRpcShardDeleteRequest {
            location: location.into(),
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
        data_pg_id: DataPgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(data_pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckRecord, payload)
            .map(|_| ())
    }

    pub(crate) fn validate_written_shard_acks(
        &self,
        data_pg_id: DataPgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let payload = self.encode_shard_ack_batch(data_pg_id, shard_batch)?;
        self.rpc_request(StorageRpcMessageKind::ShardAckValidate, payload)
            .map(|_| ())
    }

    pub(crate) fn load_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        let payload = self.encode_shard_ack_item(data_pg_id, key);
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

    pub(crate) fn load_written_shard_ack_for_historical_inspection(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        let payload = encode_shard_ack_item_request(&StorageRpcShardAckItemRequest {
            node_id: self.node_id,
            cluster_epoch: route_cluster_epoch,
            pg_id: data_pg_id.pg_id(),
            shard_key: key.clone(),
        });
        let response = self.rpc_request(StorageRpcMessageKind::ShardAckHistoricalLoad, payload)?;
        let item = decode_shard_ack_item_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode historical shard ack load response",
                error.to_string(),
            )
        })?;
        if item.shard_key != *key {
            return Err(self.rpc_payload_error(
                "validate historical shard ack load response",
                "shard key does not match request".to_string(),
            ));
        }
        Ok(item.ack)
    }

    pub(crate) fn delete_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        self.delete_written_shard_ack_at_epoch(self.cluster_epoch, data_pg_id, key)
    }

    pub(crate) fn delete_written_shard_ack_at_epoch(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        let payload = encode_shard_ack_item_request(&StorageRpcShardAckItemRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id: data_pg_id.pg_id(),
            shard_key: key.clone(),
        });
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
            data_pg_id: data_pg_id.pg_id(),
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
        data_pg_id: DataPgId,
    ) -> Result<Vec<ScavengerShardRow>, StoreError> {
        let request = self.bucket_pg_request(data_pg_id.pg_id());
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
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let request = self.bucket_pg_request(pg_id.pg_id());
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
        data_pg_id: DataPgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        let request = StorageRpcScavengerObservationRecordRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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
        data_pg_id: DataPgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let request = self.bucket_pg_request(data_pg_id.pg_id());
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
        data_pg_id: DataPgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        let request = StorageRpcScavengerObservationKeyRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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

    pub(crate) fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError> {
        let request = StorageRpcClusterMapHistoryReferenceSummaryRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
        };
        let payload = encode_cluster_map_history_reference_summary_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::ClusterMapHistoryReferenceSummary,
            payload,
        )?;
        decode_cluster_map_history_reference_summary_response(&response)
            .map(|response| response.references)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode cluster map history reference summary response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn record_placed_segment_shard_repair(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairRecordRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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
        data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let request = self.bucket_pg_request(data_pg_id.pg_id());
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
        data_pg_id: DataPgId,
        acquire: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            claim_id: acquire.claim_id.clone(),
            owner_token: acquire.owner_token.clone(),
            claimed_at: acquire.claimed_at,
            lease_deadline: Some(acquire.lease_deadline),
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
        data_pg_id: DataPgId,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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
        data_pg_id: DataPgId,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardRepairItemRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
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

    pub(crate) fn record_placed_segment_shard_backfill(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillRecordRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            work_item: *work_item,
            remaining_tolerance,
            last_error: last_error.map(ToOwned::to_owned),
        };
        let payload =
            encode_placed_segment_shard_backfill_record_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill record request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillRecord,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate placed segment shard backfill record response",
                "placed segment shard backfill record response payload must be empty".to_string(),
            ))
        }
    }

    pub(crate) fn list_placed_segment_shard_backfills(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let request = self.bucket_pg_request(data_pg_id.pg_id());
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode placed segment shard backfills request",
                error.to_string(),
            )
        })?;
        let response =
            self.rpc_request(StorageRpcMessageKind::PlacedSegmentShardBackfills, payload)?;
        decode_placed_segment_shard_backfills_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode placed segment shard backfills response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn count_placed_segment_shard_backfills(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<usize, StoreError> {
        let request = self.bucket_pg_request(data_pg_id.pg_id());
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode placed segment shard backfill count request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillCount,
            payload,
        )?;
        decode_placed_segment_shard_backfill_count_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode placed segment shard backfill count response",
                error.to_string(),
            )
        })
    }

    pub(crate) fn placed_segment_shard_backfill_exists(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillItemRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            work_item: *work_item,
        };
        let payload =
            encode_placed_segment_shard_backfill_item_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill exists request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillExists,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard backfill exists response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn acquire_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: DataPgId,
        acquire: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            claim_id: acquire.claim_id.clone(),
            owner_token: acquire.owner_token.clone(),
            claimed_at: acquire.claimed_at,
            lease_deadline: Some(acquire.lease_deadline),
            now: acquire.now,
        };
        let payload = encode_placed_segment_shard_backfill_claim_acquire_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill claim acquire request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire,
            payload,
        )?;
        decode_placed_segment_shard_backfill_claim_optional_record_response(&response)
            .map(|response| response.record)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard backfill claim acquire response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn complete_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: DataPgId,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillClaimRecordRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            claim: claim.clone(),
        };
        let payload = encode_placed_segment_shard_backfill_claim_record_request(&request).map_err(
            |error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill claim complete request",
                    error.to_string(),
                )
            },
        )?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard backfill claim complete response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn record_placed_segment_shard_backfill_claim_error(
        &self,
        data_pg_id: DataPgId,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            claim: claim.clone(),
            last_error: last_error.to_string(),
            next_attempt_after,
        };
        let payload = encode_placed_segment_shard_backfill_claim_error_request(&request).map_err(
            |error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill claim error request",
                    error.to_string(),
                )
            },
        )?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError,
            payload,
        )?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode placed segment shard backfill claim error response",
                    error.to_string(),
                )
            })
    }

    pub(crate) fn resolve_placed_segment_shard_backfill(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        let request = StorageRpcPlacedSegmentShardBackfillItemRequest {
            route: self.bucket_pg_request(data_pg_id.pg_id()),
            work_item: *work_item,
        };
        let payload =
            encode_placed_segment_shard_backfill_item_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment shard backfill resolve request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentShardBackfillResolve,
            payload,
        )?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(self.rpc_payload_error(
                "validate placed segment shard backfill resolve response",
                "placed segment shard backfill resolve response payload must be empty".to_string(),
            ))
        }
    }

    fn encode_shard_ack_batch(
        &self,
        data_pg_id: DataPgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardAckBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: data_pg_id.pg_id(),
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

    fn encode_shard_ack_item(&self, data_pg_id: DataPgId, key: &ShardKey) -> Vec<u8> {
        encode_shard_ack_item_request(&StorageRpcShardAckItemRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: data_pg_id.pg_id(),
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

    fn write_placed_shard_with_effect_fence(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::write_placed_shard_with_effect_fence(
            self,
            operation_epoch,
            data_pg_id,
            key,
            data,
            effect_fence,
        )
    }

    fn repair_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::repair_placed_shard(self, data_pg_id, key, data)
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

impl RetainedPlacedShardNodeClient for UnixStorageNodeClient {
    fn read_placed_shard_for_historical_inspection(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        UnixStorageNodeClient::read_placed_shard_for_historical_inspection(
            self,
            location,
            key,
            expected_ack,
        )
    }

    fn delete_placed_shard_for_historical_cleanup(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_placed_shard_at_location(self, location, key)
    }
}

impl ShardAckNodeClient for UnixStorageNodeClient {
    fn register_written_shard_acks(
        &self,
        data_pg_id: DataPgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::register_written_shard_acks(self, data_pg_id, shard_batch)
    }

    fn validate_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::validate_written_shard_acks(self, data_pg_id, &[(key, ack)])
    }

    fn load_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::load_written_shard_ack(self, data_pg_id, key)
    }

    fn delete_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_written_shard_ack(self, data_pg_id, key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::record_placed_segment_shard_repair(
            self, data_pg_id, work_item, last_error,
        )
    }

    fn list_placed_segment_shard_repairs(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        UnixStorageNodeClient::list_placed_segment_shard_repairs(self, data_pg_id)
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        data_pg_id: DataPgId,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::acquire_placed_segment_shard_repair_claim(self, data_pg_id, request)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        data_pg_id: DataPgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::complete_placed_segment_shard_repair_claim(self, data_pg_id, claim)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        data_pg_id: DataPgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::record_placed_segment_shard_repair_claim_error(
            self,
            data_pg_id,
            claim,
            last_error,
            next_attempt_after,
        )
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::resolve_placed_segment_shard_repair(self, data_pg_id, work_item)
    }

    fn record_placed_segment_shard_backfill(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::record_placed_segment_shard_backfill(
            self,
            data_pg_id,
            work_item,
            remaining_tolerance,
            last_error,
        )
    }

    fn list_placed_segment_shard_backfills(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        UnixStorageNodeClient::list_placed_segment_shard_backfills(self, data_pg_id)
    }

    fn count_placed_segment_shard_backfills(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<usize, StoreError> {
        UnixStorageNodeClient::count_placed_segment_shard_backfills(self, data_pg_id)
    }

    fn placed_segment_shard_backfill_exists(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::placed_segment_shard_backfill_exists(self, data_pg_id, work_item)
    }

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: DataPgId,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::acquire_placed_segment_shard_backfill_claim(
            self, data_pg_id, request,
        )
    }

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: DataPgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::complete_placed_segment_shard_backfill_claim(self, data_pg_id, claim)
    }

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        data_pg_id: DataPgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::record_placed_segment_shard_backfill_claim_error(
            self,
            data_pg_id,
            claim,
            last_error,
            next_attempt_after,
        )
    }

    fn resolve_placed_segment_shard_backfill(
        &self,
        data_pg_id: DataPgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::resolve_placed_segment_shard_backfill(self, data_pg_id, work_item)
    }
}

impl RetainedShardAckNodeClient for UnixStorageNodeClient {
    fn load_written_shard_ack_for_historical_inspection(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::load_written_shard_ack_for_historical_inspection(
            self,
            route_cluster_epoch,
            data_pg_id,
            key,
        )
    }

    fn delete_written_shard_ack_at_retained_epoch(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_written_shard_ack_at_epoch(
            self,
            cluster_epoch,
            data_pg_id,
            key,
        )
    }
}

impl ShardScavengerNodeClient for UnixStorageNodeClient {
    fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError> {
        UnixStorageNodeClient::cluster_map_history_route_references(self)
    }

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_files(self, data_pg_id)
    }

    fn list_scavenger_shard_rows(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Vec<ScavengerShardRow>, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_rows(self, data_pg_id)
    }

    fn list_shard_scavenger_payload_references(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        UnixStorageNodeClient::list_shard_scavenger_payload_references(self, pg_id)
    }
}

impl ShardScavengerObservationNodeClient for UnixStorageNodeClient {
    fn record_shard_scavenger_observation(
        &self,
        data_pg_id: DataPgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::record_shard_scavenger_observation(self, data_pg_id, observation)
    }

    fn list_shard_scavenger_observations(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        UnixStorageNodeClient::list_shard_scavenger_observations(self, data_pg_id)
    }

    fn resolve_shard_scavenger_observation(
        &self,
        data_pg_id: DataPgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        UnixStorageNodeClient::resolve_shard_scavenger_observation(self, data_pg_id, key)
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
        Self::with_rpc_admission(node_id, cluster_epoch, socket_path, rpc_admission, None)
    }

    pub(crate) fn with_rpc_admission(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: PathBuf,
        rpc_admission: Arc<UnixStorageNodeRpcAdmission>,
        rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    ) -> Self {
        Self::with_endpoint_and_rpc_admission(
            node_id,
            cluster_epoch,
            StorageRpcClientEndpoint::unix(socket_path),
            rpc_admission,
            rpc_auth,
        )
    }

    fn with_endpoint_and_rpc_admission(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        endpoint: StorageRpcClientEndpoint,
        rpc_admission: Arc<UnixStorageNodeRpcAdmission>,
        rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    ) -> Self {
        Self {
            node_id,
            cluster_epoch,
            endpoint,
            next_request_id: AtomicU64::new(1),
            rpc_admission,
            rpc_auth,
        }
    }

    pub(crate) fn with_rpc_admission_settings(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Self {
        Self::with_rpc_admission_settings_and_auth(
            node_id,
            cluster_epoch,
            socket_path.into(),
            settings,
            None,
        )
    }

    pub(crate) fn with_rpc_admission_settings_and_auth(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
        rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    ) -> Self {
        Self::with_rpc_admission(
            node_id,
            cluster_epoch,
            socket_path.into(),
            Arc::new(UnixStorageNodeRpcAdmission::new_with_settings(
                settings.into(),
            )),
            rpc_auth,
        )
    }

    pub(crate) fn with_endpoint_rpc_admission_settings_and_auth(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        endpoint: StorageRpcClientEndpoint,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
        rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    ) -> Self {
        Self::with_endpoint_and_rpc_admission(
            node_id,
            cluster_epoch,
            endpoint,
            Arc::new(UnixStorageNodeRpcAdmission::new_with_settings(
                settings.into(),
            )),
            rpc_auth,
        )
    }

    pub(crate) fn with_rpc_admission_limit(
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        socket_path: impl Into<PathBuf>,
        rpc_admission_limit: usize,
    ) -> Self {
        let settings = LocalUnixStorageNodeClientAdmissionSettings {
            rpc_admission_limit,
            rpc_admission_wait_timeout: UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            rpc_control_admission_wait_timeout:
                UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
        };
        Self::with_rpc_admission_settings(node_id, cluster_epoch, socket_path, settings)
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
        let deadline = started
            .checked_add(storage_rpc_io_timeout(self.rpc_auth.as_deref()))
            .ok_or_else(|| StoreError::Io {
                context: "compute storage-node RPC deadline",
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "storage-node RPC deadline overflowed",
                ),
            })?;
        let mut stream = match self.endpoint.connect(deadline) {
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
                    context: "connect storage-node RPC endpoint",
                    source,
                });
            }
        };
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        let request_proof = match write_unix_storage_rpc_request(
            &mut stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            &request,
            "write storage RPC request",
        ) {
            Ok(request_proof) => request_proof,
            Err(error) => {
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
                return Err(error);
            }
        };
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
        let response = match read_unix_storage_rpc_response(
            &mut stream,
            self.node_id,
            self.rpc_auth.as_deref(),
            request_proof.as_ref(),
            "read storage RPC response",
        ) {
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
                return Err(error);
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
                        observability::StorageRpcAdmissionWaitSummary {
                            node_id: self.node_id.as_u32(),
                            rpc_kind: kind.operation_name(),
                            admission_class: class,
                            wait_us,
                        },
                    );
                }
                Ok(permit)
            }
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { wait_us } => {
                let _ = observability::emit_storage_rpc_admission_timeout(
                    "storage_node_client",
                    observability::StorageRpcAdmissionTimeoutSummary {
                        node_id: self.node_id.as_u32(),
                        rpc_kind: kind.operation_name(),
                        admission_class: class,
                        wait_us,
                        timeout_us: wait_timeout.as_micros(),
                    },
                );
                Err(StoreError::StorageRpcResourceExhausted {
                    node_id: self.node_id.as_u32(),
                    operation: kind.operation_name(),
                    detail: crate::StorageNodeFailureDetail::new(format!(
                    "storage-node client RPC admission limit {} is exhausted after waiting {} ms",
                    self.rpc_admission.limit,
                    wait_timeout.as_millis()
                )),
                })
            }
        }
    }

    pub(crate) fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        storage_rpc_response_error(self.node_id, kind, error)
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
            StorageRpcErrorCode::BucketWriteReservationConflict => {
                BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
                    reservation_id: error.message,
                })
            }
            StorageRpcErrorCode::BucketWriteReservationNotFound => {
                BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationNotFound {
                    reservation_id: error.message,
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
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(message),
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

    pub(crate) fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
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

    pub(crate) fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
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

    pub(crate) fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        let limit = u32::try_from(limit).map_err(|_| StoreError::StorageRpc {
            operation: "metadata command checkpoint candidates",
            node_id: self.node_id.as_u32(),
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(format!(
                "checkpoint candidate limit {limit} exceeds u32::MAX"
            )),
        })?;
        let request = StorageRpcMetadataCommandCheckpointCandidatesRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            max_applied_log_index,
            limit,
        };
        let payload = encode_metadata_command_checkpoint_candidates_request(&request);
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

    pub(crate) fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
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

    pub(crate) fn pending_metadata_command_envelope_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
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
            if command.id().cluster_epoch() != cluster_epoch || command.id().pg_id() != pg_id {
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
        cluster_epoch: ClusterEpoch,
        preserve_pending_slot: bool,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let kind = if preserve_pending_slot {
            StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        } else {
            StorageRpcMessageKind::MetadataCommandValidateReplayState
        };
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
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

    pub(crate) fn metadata_command_replica_state_can_initialize(
        &self,
        pg_id: PgId,
    ) -> Result<bool, StoreError> {
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

    pub(crate) fn initialize_metadata_transfer_empty_state(
        &self,
        pg_id: PgId,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandTransferEmptyStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
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

    pub(crate) fn initialize_metadata_transfer_matching_state(
        &self,
        pg_id: PgId,
        applied_log_index: u64,
        applied_log_hash: u64,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandTransferMatchingStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
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

    pub(crate) fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        pg_id: PgId,
        commands: &[MetadataTransferCommand],
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandTransferAdoptRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
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

    #[allow(dead_code)]
    pub(crate) fn install_metadata_transfer_checkpoint_base(
        &self,
        pg_id: PgId,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandTransferCheckpointBaseRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
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
        self.try_insert_pending_metadata_command_slot_with_effect_deadline(
            pg_id, command, bucket, None,
        )
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
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline(
            pg_id, command, bucket, None,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
            effect_deadline,
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

impl RetainedMetadataCommandNodeClient for UnixStorageNodeClient {
    fn apply_retained_stream_upload_abort(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.metadata_command_apply_and_record_with_kind(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandRetainedAbortApply,
            "decode retained stream abort apply response",
        )
    }

    fn finish_retained_stream_upload_abort(
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
                "encode retained stream abort finish request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedAbortFinish,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode retained stream abort finish response",
                    error.to_string(),
                )
            })
    }
}

impl MetadataCommandInspectionNodeClient for UnixStorageNodeClient {
    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        MetadataCommandNodeClient::max_metadata_command_log_index(self, pg_id, cluster_epoch)
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandNodeClient::pending_metadata_command_envelope(self, pg_id, cluster_epoch)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        MetadataCommandNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        MetadataCommandNodeClient::metadata_command_checkpoint(self, pg_id, cluster_epoch)
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        MetadataCommandNodeClient::metadata_command_checkpoint_candidates(
            self,
            pg_id,
            cluster_epoch,
            max_applied_log_index,
            limit,
        )
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
        UnixStorageNodeClient::metadata_command_replica_state_can_initialize(self, pg_id)
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(self, pg_id, command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_abandon_acceptance(self, pg_id, command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        MetadataCommandNodeClient::applied_metadata_command_log_entry_hashes(self, pg_id, command)
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        MetadataCommandNodeClient::retained_metadata_command_log_hashes(
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
        MetadataCommandNodeClient::retained_metadata_command_log_entries(
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
        MetadataCommandNodeClient::has_matching_applied_metadata_command_log_entry(
            self,
            pg_id,
            command,
            expected_previous_log_hash,
        )
    }

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::metadata_command_abandoned(self, pg_id, command)
    }
}

impl MetadataCommandPeeringNodeClient for UnixStorageNodeClient {
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
        UnixStorageNodeClient::validate_metadata_command_replay_state(
            self,
            pg_id,
            cluster_epoch,
            false,
        )
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
        UnixStorageNodeClient::validate_metadata_command_replay_state(
            self,
            pg_id,
            cluster_epoch,
            true,
        )
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
        UnixStorageNodeClient::initialize_metadata_transfer_empty_state(
            self,
            pg_id,
            expected_state_digest,
        )
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
        UnixStorageNodeClient::initialize_metadata_transfer_matching_state(
            self,
            pg_id,
            applied_log_index,
            applied_log_hash,
            expected_state_digest,
        )
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
        UnixStorageNodeClient::adopt_metadata_transfer_state_from_rebased_commands(
            self,
            pg_id,
            commands,
            expected_state_digest,
        )
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
        UnixStorageNodeClient::install_metadata_transfer_checkpoint_base(self, pg_id, checkpoint)
    }

    fn replay_metadata_command_for_peering(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        UnixStorageNodeClient::replay_metadata_command_for_peering(self, pg_id, command)
    }
}

impl MetadataCommandRecoveryNodeClient for UnixStorageNodeClient {
    fn open_metadata_command_recovery_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandRecoveryCriticalSection>, StoreError> {
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
        UnixStorageNodeClient::pending_metadata_command_envelope_at_epoch(
            self,
            pg_id,
            cluster_epoch,
        )
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
        UnixStorageNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
            self, pg_id, command, bucket,
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
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline(
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

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        UnixStorageNodeClient::remove_pending_metadata_command_slot(self, pg_id, command)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        UnixStorageNodeClient::metadata_command_checkpoint(self, pg_id, cluster_epoch)
    }

    fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::record_current_metadata_command_checkpoint(
            self,
            pg_id,
            cluster_epoch,
        )
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        UnixStorageNodeClient::metadata_command_checkpoint_candidates(
            self,
            pg_id,
            cluster_epoch,
            max_applied_log_index,
            limit,
        )
    }

    fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        UnixStorageNodeClient::compact_metadata_command_log(self, pg_id, cluster_epoch)
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
}

impl BucketMetadataNodeClient for UnixStorageNodeClient {
    fn head_bucket_replica_for_delete(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(
            StorageRpcMessageKind::BucketDeleteReplicaHead,
            pg_id.pg_id(),
            bucket,
        )
    }

    fn head_bucket_raw(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadRaw, pg_id.pg_id(), bucket)
    }

    fn head_bucket_info(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.head_bucket_with_kind(StorageRpcMessageKind::BucketHeadInfo, pg_id.pg_id(), bucket)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
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
        source_pg_id: BucketPgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: BucketPgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSnapshotPairRequest {
            source: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: source_pg_id.pg_id(),
                    bucket: source.0.clone(),
                },
                request: source.1,
            },
            destination: StorageRpcBucketSnapshotRequest {
                bucket: StorageRpcBucketRequest {
                    node_id: self.node_id,
                    cluster_epoch: self.cluster_epoch,
                    pg_id: destination_pg_id.pg_id(),
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
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        let request = StorageRpcCreateBucketCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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

    fn build_advance_multipart_completion_barrier_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        let request = StorageRpcMultipartCompletionBarrierCommandBuildRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
            command_id,
            completion_target_context: completion_target_context.to_string(),
            bucket_write_reservation: bucket_write_reservation.clone(),
        };
        let payload = encode_multipart_completion_barrier_command_build_request(&request).map_err(
            |error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode multipart completion barrier command build request",
                    error.to_string(),
                ))
            },
        )?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::MultipartCompletionBarrierCommandBuild,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_multipart_completion_barrier_command_build_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode multipart completion barrier command build response",
                    error.to_string(),
                ))
            })?;
        self.validate_multipart_completion_barrier_command_build_response(
            response.barrier_sequence,
            response.command,
            bucket,
            command_id,
        )
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id.pg_id(),
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::MarkBucketDeleting(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id.pg_id(),
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::MarkDeleting,
        )
    }

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
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
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id.pg_id(),
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketVersioning(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id.pg_id(),
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Versioning(state),
        )
    }

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id.pg_id(),
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Versioning(state),
        )
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id.pg_id(),
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketAcl(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id.pg_id(),
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: acl_grants.clone(),
                summary,
            },
        )
    }

    fn build_put_bucket_acl_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id.pg_id(),
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: acl_grants.clone(),
                summary,
            },
        )
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                self.cluster_epoch,
                pg_id.pg_id(),
                MetadataCommandLogIndex::new(1).expect("nonzero log index"),
            ),
            MetadataCommandPayload::PutBucketProperty(command.clone()),
        );
        self.bucket_metadata_control_pending_match(
            pg_id.pg_id(),
            bucket,
            &command,
            StorageRpcBucketMetadataControlMutation::Property(mutation.clone()),
        )
    }

    fn build_put_bucket_property_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id.pg_id(),
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Property(mutation.clone()),
        )
    }

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.bucket_metadata_control_command_build(
            pg_id.pg_id(),
            bucket,
            command_id,
            StorageRpcBucketMetadataControlMutation::Subresource(mutation.clone()),
        )
    }

    fn get_bucket_subresource(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketSubresourceGetRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
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
        pg_id: BucketPgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketListRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
        pg_id: BucketPgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
        pg_id: BucketPgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketBatchRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
