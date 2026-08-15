// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::validate_metadata_command_recovery_certificate;
use crate::BucketAclSummary;

struct UnixRetainedPlacedShardRoute<'a> {
    client: &'a UnixStorageNodeClient,
    location: crate::cluster::ShardLocation,
    key: ShardKey,
}

struct UnixPlacedShardRoute<'a> {
    client: &'a UnixStorageNodeClient,
    location: crate::cluster::ShardLocation,
    key: ShardKey,
}

struct UnixRetainedShardAckRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    key: ShardKey,
}

struct UnixShardAckRoute<'a> {
    client: &'a UnixStorageNodeClient,
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
}

struct UnixShardScavengerDataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    data_pg_id: DataPgId,
}

struct UnixShardScavengerObjectScanRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataScanPgId,
}

struct UnixBucketMetadataScanRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    pg_topology: Arc<PgTopology>,
}

struct UnixBucketMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
}

struct UnixBucketDeleteReplicaMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
}

impl UnixStorageNodeClient {
    fn write_placed_shard(
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

    fn write_placed_shard_with_effect_fence(
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

    fn repair_placed_shard(
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

    fn read_placed_shard(
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
    ) -> Result<(Vec<u8>, WriteAck), StoreError> {
        let request = StorageRpcHistoricalShardReadRequest {
            location: location.into(),
            shard_key: key.clone(),
        };
        let payload = encode_historical_shard_read_request(&request).map_err(|error| {
            self.rpc_payload_error("encode historical shard read request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardHistoricalRead, payload)?;
        decode_historical_shard_read_response(&response).map_err(|error| {
            self.rpc_payload_error("decode historical shard read response", error.to_string())
        })
    }

    fn read_placed_shard_range(
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

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
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

    fn list_scavenger_shard_files(
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

    fn list_scavenger_shard_rows(
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

    fn list_shard_scavenger_payload_references(
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

    fn list_placed_segment_backfill_reference_page(
        &self,
        pg_id: ObjectMetadataScanPgId,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<PlacedSegmentBackfillReferencePage, StoreError> {
        let request = StorageRpcPlacedSegmentBackfillReferencePageRequest {
            route: self.bucket_pg_request(pg_id.pg_id()),
            after: after.cloned(),
            limit,
        };
        let payload =
            encode_placed_segment_backfill_reference_page_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode placed segment backfill reference page request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::PlacedSegmentBackfillReferencePage,
            payload,
        )?;
        decode_placed_segment_backfill_reference_page_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode placed segment backfill reference page response",
                error.to_string(),
            )
        })
    }

    fn record_shard_scavenger_observation(
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

    fn list_shard_scavenger_observations(
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

    fn resolve_shard_scavenger_observation(
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

    #[cfg(test)]
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

    fn open_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn PlacedShardRoute + '_>, StoreError> {
        if location.node_id() != self.node_id {
            return Err(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            });
        }
        if location.cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: location.data_pg_id().get(),
                operation_epoch: location.cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open active placed shard route",
            });
        }
        Ok(Box::new(UnixPlacedShardRoute {
            client: self,
            location,
            key: key.clone(),
        }))
    }
}

impl PlacedShardRoute for UnixPlacedShardRoute<'_> {
    fn write_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::write_placed_shard(
            self.client,
            self.location.data_pg_id(),
            &self.key,
            data,
        )
    }

    fn write_placed_shard_with_effect_fence(
        &self,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::write_placed_shard_with_effect_fence(
            self.client,
            self.location.cluster_epoch(),
            self.location.data_pg_id(),
            &self.key,
            data,
            effect_fence,
        )
    }

    fn repair_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::repair_placed_shard(
            self.client,
            self.location.data_pg_id(),
            &self.key,
            data,
        )
    }

    fn read_placed_shard(&self, expected_ack: WriteAck) -> Result<Vec<u8>, StoreError> {
        UnixStorageNodeClient::read_placed_shard(
            self.client,
            self.location.data_pg_id(),
            &self.key,
            expected_ack,
        )
    }

    fn read_placed_shard_into(
        &self,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        if dst.len() as u64 != expected_ack.stored_size {
            return Err(self.client.rpc_payload_error(
                "shard read range",
                format!(
                    "remote shard read buffer is {} bytes for expected {} byte shard",
                    dst.len(),
                    expected_ack.stored_size
                ),
            ));
        }
        let data = UnixStorageNodeClient::read_placed_shard_range(
            self.client,
            self.location.data_pg_id(),
            &self.key,
            expected_ack,
            0,
            dst.len() as u64,
        )?;
        dst.copy_from_slice(&data);
        Ok(())
    }

    fn delete_placed_shard(&self) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_placed_shard(
            self.client,
            self.location.data_pg_id(),
            &self.key,
        )
    }
}

impl RetainedPlacedShardNodeClient for UnixStorageNodeClient {
    fn open_retained_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedPlacedShardRoute + '_>, StoreError> {
        if location.node_id() != self.node_id {
            return Err(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            });
        }
        if location.cluster_epoch() > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: location.data_pg_id().get(),
                operation_epoch: location.cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained placed shard route",
            });
        }
        Ok(Box::new(UnixRetainedPlacedShardRoute {
            client: self,
            location,
            key: key.clone(),
        }))
    }
}

impl RetainedPlacedShardRoute for UnixRetainedPlacedShardRoute<'_> {
    fn read_placed_shard_for_historical_inspection(
        &self,
    ) -> Result<(Vec<u8>, WriteAck), StoreError> {
        UnixStorageNodeClient::read_placed_shard_for_historical_inspection(
            self.client,
            self.location,
            &self.key,
        )
    }

    fn delete_placed_shard_for_historical_cleanup(&self) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_placed_shard_at_location(
            self.client,
            self.location,
            &self.key,
        )
    }
}

impl ShardAckNodeClient for UnixStorageNodeClient {
    fn open_shard_ack_route(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardAckRoute + '_>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixShardAckRoute {
            client: self,
            cluster_epoch,
            data_pg_id,
        }))
    }
}

impl ShardAckRoute for UnixShardAckRoute<'_> {
    fn register_shard_acks(&self, shard_batch: &[(&ShardKey, WriteAck)]) -> Result<(), StoreError> {
        UnixStorageNodeClient::register_written_shard_acks(
            self.client,
            self.data_pg_id,
            shard_batch,
        )
    }

    fn validate_shard_ack(&self, key: &ShardKey, ack: WriteAck) -> Result<(), StoreError> {
        UnixStorageNodeClient::validate_written_shard_acks(
            self.client,
            self.data_pg_id,
            &[(key, ack)],
        )
    }

    fn load_shard_ack(&self, key: &ShardKey) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::load_written_shard_ack(self.client, self.data_pg_id, key)
    }

    fn delete_shard_ack(&self, key: &ShardKey) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_written_shard_ack(self.client, self.data_pg_id, key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), work_item)?;
        UnixStorageNodeClient::record_placed_segment_shard_repair(
            self.client,
            self.data_pg_id,
            work_item,
            last_error,
        )
    }

    fn list_placed_segment_shard_repairs(
        &self,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let repairs =
            UnixStorageNodeClient::list_placed_segment_shard_repairs(self.client, self.data_pg_id)?;
        for repair in &repairs {
            validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &repair.work_item)
                .map_err(|error| {
                    self.client.rpc_payload_error(
                        "validate placed segment shard repairs response",
                        error.to_string(),
                    )
                })?;
        }
        Ok(repairs)
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let claim = UnixStorageNodeClient::acquire_placed_segment_shard_repair_claim(
            self.client,
            self.data_pg_id,
            request,
        )?;
        if let Some(claim) = &claim {
            validate_placed_segment_shard_repair_claim_epoch(
                self.data_pg_id.pg_id(),
                self.cluster_epoch,
                claim,
            )
            .and_then(|()| {
                validate_placed_segment_shard_repair_route(
                    self.data_pg_id.pg_id(),
                    &claim.work_item,
                )
            })
            .map_err(|error| {
                self.client.rpc_payload_error(
                    "validate placed segment shard repair claim response",
                    error.to_string(),
                )
            })?;
            if claim.claim_id != request.claim_id
                || claim.owner_token != request.owner_token
                || claim.claimed_at != request.claimed_at
                || claim.lease_deadline != Some(request.lease_deadline)
            {
                return Err(self.client.rpc_payload_error(
                    "validate placed segment shard repair claim response",
                    "claim response identity does not match request".to_string(),
                ));
            }
        }
        Ok(claim)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        UnixStorageNodeClient::complete_placed_segment_shard_repair_claim(
            self.client,
            self.data_pg_id,
            claim,
        )
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        UnixStorageNodeClient::record_placed_segment_shard_repair_claim_error(
            self.client,
            self.data_pg_id,
            claim,
            last_error,
            next_attempt_after,
        )
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_route(self.data_pg_id.pg_id(), work_item)?;
        UnixStorageNodeClient::resolve_placed_segment_shard_repair(
            self.client,
            self.data_pg_id,
            work_item,
        )
    }

    fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        UnixStorageNodeClient::record_placed_segment_shard_backfill(
            self.client,
            self.data_pg_id,
            work_item,
            remaining_tolerance,
            last_error,
        )
    }

    fn list_placed_segment_shard_backfills(
        &self,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let backfills = UnixStorageNodeClient::list_placed_segment_shard_backfills(
            self.client,
            self.data_pg_id,
        )?;
        for backfill in &backfills {
            validate_placed_segment_shard_backfill_route(
                self.data_pg_id.pg_id(),
                &backfill.work_item,
            )
            .map_err(|error| {
                self.client.rpc_payload_error(
                    "validate placed segment shard backfills response",
                    error.to_string(),
                )
            })?;
        }
        Ok(backfills)
    }

    fn count_placed_segment_shard_backfills(&self) -> Result<usize, StoreError> {
        UnixStorageNodeClient::count_placed_segment_shard_backfills(self.client, self.data_pg_id)
    }

    fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        UnixStorageNodeClient::placed_segment_shard_backfill_exists(
            self.client,
            self.data_pg_id,
            work_item,
        )
    }

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        if request.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: request.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let claim = UnixStorageNodeClient::acquire_placed_segment_shard_backfill_claim(
            self.client,
            self.data_pg_id,
            request,
        )?;
        if let Some(claim) = &claim {
            validate_placed_segment_shard_backfill_claim_epoch(
                self.data_pg_id.pg_id(),
                self.cluster_epoch,
                claim,
            )
            .and_then(|()| {
                validate_placed_segment_shard_backfill_route(
                    self.data_pg_id.pg_id(),
                    &claim.work_item,
                )
            })
            .map_err(|error| {
                self.client.rpc_payload_error(
                    "validate placed segment shard backfill claim response",
                    error.to_string(),
                )
            })?;
            if claim.claim_id != request.claim_id
                || claim.owner_token != request.owner_token
                || claim.claimed_at != request.claimed_at
                || claim.lease_deadline != Some(request.lease_deadline)
            {
                return Err(self.client.rpc_payload_error(
                    "validate placed segment shard backfill claim response",
                    "claim response identity does not match request".to_string(),
                ));
            }
        }
        Ok(claim)
    }

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        UnixStorageNodeClient::complete_placed_segment_shard_backfill_claim(
            self.client,
            self.data_pg_id,
            claim,
        )
    }

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        if claim.cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), &claim.work_item)?;
        UnixStorageNodeClient::record_placed_segment_shard_backfill_claim_error(
            self.client,
            self.data_pg_id,
            claim,
            last_error,
            next_attempt_after,
        )
    }

    fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_route(self.data_pg_id.pg_id(), work_item)?;
        UnixStorageNodeClient::resolve_placed_segment_shard_backfill(
            self.client,
            self.data_pg_id,
            work_item,
        )
    }
}

impl RetainedShardAckNodeClient for UnixStorageNodeClient {
    fn open_retained_shard_ack_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedShardAckRoute + '_>, StoreError> {
        if route_cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixRetainedShardAckRoute {
            client: self,
            route_cluster_epoch,
            data_pg_id,
            key: key.clone(),
        }))
    }
}

impl RetainedShardAckRoute for UnixRetainedShardAckRoute<'_> {
    fn load_written_shard_ack_for_historical_inspection(&self) -> Result<WriteAck, StoreError> {
        UnixStorageNodeClient::load_written_shard_ack_for_historical_inspection(
            self.client,
            self.route_cluster_epoch,
            self.data_pg_id,
            &self.key,
        )
    }

    fn delete_retained_shard_ack(&self) -> Result<(), StoreError> {
        UnixStorageNodeClient::delete_written_shard_ack_at_epoch(
            self.client,
            self.route_cluster_epoch,
            self.data_pg_id,
            &self.key,
        )
    }
}

impl ShardScavengerNodeClient for UnixStorageNodeClient {
    #[cfg(test)]
    fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError> {
        UnixStorageNodeClient::cluster_map_history_route_references(self)
    }

    fn open_shard_scavenger_data_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerDataRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixShardScavengerDataRoute {
            client: self,
            data_pg_id,
        }))
    }

    fn open_shard_scavenger_object_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ShardScavengerObjectScanRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixShardScavengerObjectScanRoute {
            client: self,
            pg_id,
        }))
    }
}

impl ShardScavengerDataRoute for UnixShardScavengerDataRoute<'_> {
    fn list_scavenger_shard_files(&self) -> Result<ScavengerShardFileScan, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_files(self.client, self.data_pg_id)
    }

    fn list_scavenger_shard_rows(&self) -> Result<Vec<ScavengerShardRow>, StoreError> {
        UnixStorageNodeClient::list_scavenger_shard_rows(self.client, self.data_pg_id)
    }
}

impl ShardScavengerObjectScanRoute for UnixShardScavengerObjectScanRoute<'_> {
    fn list_placed_segment_backfill_reference_page(
        &self,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<PlacedSegmentBackfillReferencePage, StoreError> {
        UnixStorageNodeClient::list_placed_segment_backfill_reference_page(
            self.client,
            self.pg_id,
            after,
            limit,
        )
    }

    fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        UnixStorageNodeClient::list_shard_scavenger_payload_references(self.client, self.pg_id)
    }
}

struct UnixShardScavengerObservationRoute<'a> {
    client: &'a UnixStorageNodeClient,
    data_pg_id: DataPgId,
}

impl UnixShardScavengerObservationRoute<'_> {
    fn validate_key(&self, key: &ShardScavengerObservationKey) -> Result<(), StoreError> {
        if key.data_pg_id != self.data_pg_id.get() {
            return Err(StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: self.data_pg_id.get(),
                observation_pg_id: key.data_pg_id,
            });
        }
        Ok(())
    }
}

impl ShardScavengerObservationNodeClient for UnixStorageNodeClient {
    fn open_shard_scavenger_observation_route(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerObservationRoute + '_>, StoreError> {
        Ok(Box::new(UnixShardScavengerObservationRoute {
            client: self,
            data_pg_id,
        }))
    }
}

impl ShardScavengerObservationRoute for UnixShardScavengerObservationRoute<'_> {
    fn record_shard_scavenger_observation(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_key(&observation.key)?;
        UnixStorageNodeClient::record_shard_scavenger_observation(
            self.client,
            self.data_pg_id,
            observation,
        )
    }

    fn list_shard_scavenger_observations(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let observations =
            UnixStorageNodeClient::list_shard_scavenger_observations(self.client, self.data_pg_id)?;
        for observation in &observations {
            self.validate_key(&observation.key).map_err(|error| {
                self.client.rpc_payload_error(
                    "validate shard scavenger observations response",
                    error.to_string(),
                )
            })?;
        }
        Ok(observations)
    }

    fn resolve_shard_scavenger_observation(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        self.validate_key(key)?;
        UnixStorageNodeClient::resolve_shard_scavenger_observation(
            self.client,
            self.data_pg_id,
            key,
        )
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
        // Unit-test servers commonly serve one connection synchronously per
        // `accept_one`; production listeners always use per-connection workers.
        #[cfg(test)]
        let endpoint = StorageRpcClientEndpoint::unpooled_unix_for_test(socket_path);
        #[cfg(not(test))]
        let endpoint = StorageRpcClientEndpoint::unix(socket_path);
        Self::with_endpoint_and_rpc_admission(
            node_id,
            cluster_epoch,
            endpoint,
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
            pg_topology: None,
            next_request_id: AtomicU64::new(1),
            rpc_admission,
            rpc_auth,
        }
    }

    pub(crate) fn with_pg_topology(mut self, pg_topology: Arc<PgTopology>) -> Self {
        self.pg_topology = Some(pg_topology);
        self
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
        let io_timeout = storage_rpc_io_timeout(self.rpc_auth.as_deref());
        let deadline = Instant::now()
            .checked_add(io_timeout)
            .ok_or_else(|| StoreError::Io {
                context: "compute storage-node RPC deadline",
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "storage-node RPC deadline overflowed",
                ),
            })?;
        self.rpc_request_result_with_permit_until(kind, payload, rpc_permit, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)
    }

    pub(super) fn rpc_request_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        match self
            .rpc_request_result_until(kind, payload, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
        {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_request_result_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcRequestDispatchFailure> {
        let rpc_permit = self
            .acquire_rpc_admission_with_class_until(
                kind,
                storage_rpc_admission_class(kind),
                deadline,
            )
            .map_err(StorageRpcRequestDispatchFailure::NotSent)?;
        self.rpc_request_result_with_permit_until(kind, payload, rpc_permit, deadline)
    }

    fn rpc_request_result_with_permit_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        rpc_permit: UnixStorageNodeRpcAdmissionPermit,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcRequestDispatchFailure> {
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
        let io_timeout = deadline
            .checked_duration_since(started)
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| {
                StorageRpcRequestDispatchFailure::NotSent(storage_rpc_endpoint_deadline_expired(
                    self.node_id,
                    "start storage-node RPC request",
                ))
            })?;
        let max_connections = self
            .rpc_auth
            .as_deref()
            .map_or(self.rpc_admission.limit, |auth| {
                auth.transport_limits().max_connections()
            });
        let mut connection =
            match self
                .endpoint
                .connect_request(deadline, io_timeout, max_connections)
            {
                Ok(connection) => {
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
                    connection
                }
                Err(source) => {
                    if trace_rpc_lifecycle {
                        let _ = observability::emit_flight_event(
                            "storage_rpc_client",
                            "storage_rpc_client_connect_failed",
                            format!(
                                "node_id={} rpc_request_id={} kind={} elapsed_us={} error={:?}",
                                self.node_id.as_u32(),
                                request_id,
                                kind.operation_name(),
                                started.elapsed().as_micros(),
                                source
                            ),
                        );
                    }
                    return Err(StorageRpcRequestDispatchFailure::NotSent(
                        storage_rpc_endpoint_connect_error(
                            self.node_id,
                            "connect storage-node RPC endpoint",
                            source,
                        ),
                    ));
                }
            };
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        let request_proof = match write_unix_storage_rpc_request_classified(
            connection.stream_mut(),
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
                            "node_id={} rpc_request_id={} kind={} elapsed_us={} error={:?}",
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
            connection.stream_mut(),
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
                return Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error));
            }
        };
        if response.request_id != request_id || response.kind != kind {
            return Err(StorageRpcRequestDispatchFailure::MayHaveApplied(
                self.rpc_payload_error(
                    "validate storage RPC response",
                    format!(
                        "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                        response.request_id, response.kind
                    ),
                ),
            ));
        }
        let response =
            decode_storage_rpc_response_payload_with_connection_disposition(&response.payload)
                .map_err(|error| {
                    StorageRpcRequestDispatchFailure::MayHaveApplied(
                        self.rpc_payload_error("decode storage RPC response", error.to_string()),
                    )
                })?;
        if response.connection_reusable {
            connection.mark_reusable();
        }
        Ok(response.response)
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
        let deadline = Instant::now() + self.rpc_admission.wait_timeout_for_class(class);
        self.acquire_rpc_admission_with_class_until(kind, class, deadline)
    }

    pub(crate) fn acquire_rpc_admission_with_class_until(
        &self,
        kind: StorageRpcMessageKind,
        class: UnixStorageNodeRpcAdmissionClass,
        deadline: Instant,
    ) -> Result<UnixStorageNodeRpcAdmissionPermit, StoreError> {
        observability::emit_storage_rpc_admission_attempt();
        let wait_timeout = deadline.saturating_duration_since(Instant::now());
        match self
            .rpc_admission
            .acquire_with_kind_until(class, kind, deadline)
        {
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

    pub(crate) fn rpc_request_bucket_snapshot_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, BucketSnapshotLoadError> {
        match self
            .rpc_request_result_until(kind, payload, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)
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
        let deadline = Instant::now() + storage_rpc_io_timeout(self.rpc_auth.as_deref());
        self.metadata_command_replica_state_until(pg_id, deadline)
    }

    fn metadata_command_replica_state_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        };
        let payload = encode_metadata_command_state_request(&request);
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandReplicaState,
            payload,
            deadline,
        )?;
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
        decode_metadata_command_log_entry_range_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
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
        let response = decode_metadata_command_pending_envelope_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
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

    fn metadata_command_acceptance_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request_until(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            pg_id,
            command,
            deadline,
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

    fn validate_metadata_command_replay_state(
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

    fn initialize_metadata_transfer_empty_state(
        &self,
        pg_id: PgId,
        expected_state_digest: CanonicalStateDigest,
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

    fn initialize_metadata_transfer_matching_state(
        &self,
        pg_id: PgId,
        applied_log_index: u64,
        applied_log_hash: MetadataCommandLogHash,
        expected_state_digest: CanonicalStateDigest,
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

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        pg_id: PgId,
        commands: &[MetadataTransferCommand],
        expected_state_digest: CanonicalStateDigest,
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
    fn install_metadata_transfer_checkpoint_base(
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
        let deadline = Instant::now() + storage_rpc_io_timeout(self.rpc_auth.as_deref());
        self.applied_metadata_command_log_entry_hashes_until(pg_id, command, deadline)
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
            deadline,
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

    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
        )?;
        self.decode_metadata_command_abandonment_response(
            pg_id,
            &response,
            "decode metadata command replica abandonment response",
        )
    }

    fn decode_metadata_command_abandonment_response(
        &self,
        pg_id: PgId,
        response: &[u8],
        context: &'static str,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let response = decode_metadata_command_state_outcome_response(response)
            .map_err(|error| self.rpc_payload_error(context, error.to_string()))?;
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
                context,
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
            _ => {
                Err(self
                    .rpc_payload_error(context, "unexpected metadata command outcome".to_owned()))
            }
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

    fn apply_metadata_command_and_record_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(MetadataCommandApplyError::not_sent)?;
        self.metadata_command_apply_and_record_with_payload_until(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            payload,
            "decode metadata command apply and record response",
            deadline,
        )
    }

    #[allow(dead_code)]
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
        let deadline = Instant::now()
            .checked_add(storage_rpc_io_timeout(self.rpc_auth.as_deref()))
            .ok_or_else(|| {
                BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "compute metadata command apply RPC deadline",
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "metadata command apply RPC deadline overflowed",
                    ),
                })
            })?;
        self.metadata_command_apply_and_record_with_payload_until(
            pg_id,
            command,
            kind,
            payload,
            decode_context,
            deadline,
        )
        .map_err(MetadataCommandApplyError::into_source)
    }

    fn metadata_command_apply_and_record_with_payload_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        decode_context: &'static str,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let response = match self.rpc_request_result_until(kind, payload, deadline) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(MetadataCommandApplyError::definitive(
                    self.bucket_snapshot_rpc_response_error(kind, error),
                ));
            }
            Err(error) => return Err(error.into_metadata_command_apply_error()),
        };
        let response = decode_metadata_command_state_outcome_response(&response)
            .map_err(|error| self.rpc_payload_error(decode_context, error.to_string()))
            .map_err(MetadataCommandApplyError::may_have_applied)?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Store(metadata_command_log_conflict_error(
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
                )),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                reservation_id,
                generation_id,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectGenerationReservationConflict {
                        reservation_id: reservation_id.into_string(),
                        generation_id: generation_id.get(),
                    },
                ),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectVersionReservationConflict { version_id },
                ),
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
                Ok(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Metadata(error),
                )),
                Err(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Store(error),
                )),
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
                Ok(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Metadata(error),
                )),
                Err(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Store(error),
                )),
            },
            StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index } => Err(
                MetadataCommandApplyError::definitive(BucketSnapshotLoadError::Metadata(
                    MetadataError::StreamSegmentConflict { segment_index },
                )),
            ),
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
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_deadline,
            None,
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_deadline_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
        deadline: Option<Instant>,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_classified_until(
            pg_id,
            command,
            bucket,
            effect_deadline,
            deadline,
        )
        .map_err(MetadataCommandPendingSlotInsertError::into_source)
    }

    fn try_insert_pending_metadata_command_slot_with_effect_deadline_classified_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
        deadline: Option<Instant>,
    ) -> Result<(), MetadataCommandPendingSlotInsertError> {
        let operation_deadline = deadline.map(StorageRpcOperationDeadline::from_instant);
        let effect_deadline = operation_deadline.map_or(effect_deadline, |operation_deadline| {
            Some(
                StorageRpcAdmittedRouteEffectDeadline::intersect_existing_operation_deadline(
                    effect_deadline,
                    operation_deadline.portable_wall_valid_until_ms,
                ),
            )
        });
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: bucket.cloned(),
            effect_deadline,
            operation_deadline,
        };
        let payload = encode_metadata_command_pending_slot_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command pending slot request",
                    error.to_string(),
                )
            })
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        let kind = StorageRpcMessageKind::MetadataCommandPendingSlotInsert;
        let response = match deadline {
            Some(deadline) => self.rpc_request_result_until(kind, payload, deadline),
            None => self
                .rpc_request_result(kind, payload)
                .map_err(StorageRpcRequestDispatchFailure::MayHaveApplied),
        };
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let uncertain = error.code == StorageRpcErrorCode::MetadataCommandMutationUncertain;
                let source = self.rpc_response_error(kind, error);
                return Err(if uncertain {
                    MetadataCommandPendingSlotInsertError::may_have_applied(source)
                } else {
                    MetadataCommandPendingSlotInsertError::definitive(source)
                });
            }
            Err(error) => {
                return Err(error.into_metadata_command_pending_slot_insert_error());
            }
        };
        let response = decode_metadata_command_pending_slot_insert_response(&response)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot insert response",
                    error.to_string(),
                )
            })
            .map_err(MetadataCommandPendingSlotInsertError::may_have_applied)?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => Ok(()),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            } => Err(MetadataCommandPendingSlotInsertError::definitive(
                StoreError::MetadataCommandPendingConflict {
                    pg_id,
                    cluster_epoch,
                    existing_log_index,
                    candidate_log_index,
                },
            )),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(MetadataCommandPendingSlotInsertError::may_have_applied(
                        self.rpc_payload_error(
                            "decode metadata command pending slot insert response",
                            "metadata command log conflict route mismatch".to_string(),
                        ),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(MetadataCommandPendingSlotInsertError::may_have_applied(
                        self.rpc_payload_error(
                            "decode metadata command pending slot insert response",
                            "metadata command log conflict index must not be zero".to_string(),
                        ),
                    ));
                }
                Err(MetadataCommandPendingSlotInsertError::definitive(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: conflict_pg_id,
                        cluster_epoch,
                        log_index,
                    },
                ))
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
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_deadline,
            None,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
        deadline: Option<Instant>,
    ) -> Result<bool, StoreError> {
        let operation_deadline = deadline.map(StorageRpcOperationDeadline::from_instant);
        let effect_deadline = operation_deadline.map_or(effect_deadline, |operation_deadline| {
            Some(
                StorageRpcAdmittedRouteEffectDeadline::intersect_existing_operation_deadline(
                    effect_deadline,
                    operation_deadline.portable_wall_valid_until_ms,
                ),
            )
        });
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
            effect_deadline,
            operation_deadline,
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command bucket-control pending slot request",
                error.to_string(),
            )
        })?;
        let response = match deadline {
            Some(deadline) => self.rpc_request_until(
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                payload,
                deadline,
            )?,
            None => self.rpc_request(
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                payload,
            )?,
        };
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
        let response =
            decode_metadata_command_pending_slot_cleanup_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot remove response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::Value(removed) => Ok(removed),
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::TerminalEntryPending {
                node_id,
                pg_id: pending_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_terminal_entry_pending_error(
                self.node_id,
                command.id().cluster_epoch(),
                pg_id,
                command.id().log_index(),
                "decode metadata command pending slot remove response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: pending_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_terminal_log_conflict_error(
                self.node_id,
                command.id().cluster_epoch(),
                pg_id,
                command.id().log_index(),
                "decode metadata command pending slot remove response",
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
        self.decode_metadata_command_acceptance_response(pg_id, &response)
    }

    fn metadata_command_acceptance_request_until(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
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
        let response = match self
            .rpc_request_result_until(kind, payload, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
        {
            Ok(response) => response,
            Err(error) => return Err(self.rpc_response_error(kind, error)),
        };
        self.decode_metadata_command_acceptance_response(pg_id, &response)
    }

    fn decode_metadata_command_acceptance_response(
        &self,
        pg_id: PgId,
        response: &[u8],
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let response = decode_metadata_command_acceptance_response(response).map_err(|error| {
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

struct UnixRetainedStreamUploadAbortMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    prepared: &'a PreparedRetainedStreamUploadAbort,
}

impl RetainedMetadataCommandNodeClient for UnixStorageNodeClient {
    fn open_retained_stream_upload_abort_route<'a>(
        &'a self,
        prepared: &'a PreparedRetainedStreamUploadAbort,
    ) -> Result<Box<dyn RetainedStreamUploadAbortMetadataRoute + 'a>, StoreError> {
        let prepared_epoch = prepared.command().id().cluster_epoch();
        if prepared_epoch != self.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: prepared_epoch,
                operation_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixRetainedStreamUploadAbortMetadataRoute {
            client: self,
            prepared,
        }))
    }
}

impl RetainedStreamUploadAbortMetadataRoute for UnixRetainedStreamUploadAbortMetadataRoute<'_> {
    fn apply(&self) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.client.metadata_command_apply_and_record_with_kind(
            self.prepared.pg_id().pg_id(),
            self.prepared.command(),
            StorageRpcMessageKind::MetadataCommandRetainedAbortApply,
            "decode retained stream abort apply response",
        )
    }

    fn finish(&self) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.client.cluster_epoch,
            pg_id: self.prepared.pg_id().pg_id(),
            command: self.prepared.command().clone(),
        };
        let payload = encode_metadata_command_request(&request).map_err(|error| {
            self.client.rpc_payload_error(
                "encode retained stream abort finish request",
                error.to_string(),
            )
        })?;
        let response = self.client.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedAbortFinish,
            payload,
        )?;
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                self.client.rpc_payload_error(
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

    fn max_metadata_command_log_index_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<u64, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            payload,
            deadline,
        )?;
        decode_metadata_command_max_log_index_response(&response)
            .map(|response| response.max_log_index)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command max log index response",
                    error.to_string(),
                )
            })
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandNodeClient::pending_metadata_command_envelope(self, pg_id, cluster_epoch)
    }

    fn pending_metadata_command_envelope_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
            deadline,
        )?;
        let response = decode_metadata_command_pending_envelope_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
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

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        MetadataCommandNodeClient::metadata_command_replica_state(self, pg_id)
    }

    fn metadata_command_replica_state_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::metadata_command_replica_state_until(self, pg_id, deadline)
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

    fn metadata_command_acceptance_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        UnixStorageNodeClient::metadata_command_acceptance_until(self, pg_id, command, deadline)
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

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        UnixStorageNodeClient::applied_metadata_command_log_entry_hashes_until(
            self, pg_id, command, deadline,
        )
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

struct UnixMetadataCommandPeeringRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
}

impl UnixMetadataCommandPeeringRoute<'_> {
    fn require_current_epoch(&self) -> Result<(), StoreError> {
        if self.cluster_epoch != self.client.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: self.cluster_epoch,
                current_epoch: self.client.cluster_epoch,
            });
        }
        Ok(())
    }

    fn validate_command_route(&self, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        if command.id().pg_id() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: self.client.node_id.as_u32(),
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id.get(),
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(())
    }

    fn validate_transfer_commands(
        &self,
        commands: &[MetadataTransferCommand],
    ) -> Result<(), StoreError> {
        commands
            .iter()
            .try_for_each(|command| self.validate_command_route(&command.command))
    }

    fn validate_checkpoint_pg(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<(), StoreError> {
        if checkpoint.pg_id != self.pg_id {
            return Err(StoreError::MetadataCheckpointInvalid {
                node_id: self.client.node_id.as_u32(),
                pg_id: self.pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                reason: format!(
                    "checkpoint PG {} does not match captured peering PG {}",
                    checkpoint.pg_id.get(),
                    self.pg_id.get()
                ),
            });
        }
        Ok(())
    }
}

impl MetadataCommandPeeringNodeClient for UnixStorageNodeClient {
    fn open_metadata_command_peering_route(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandPeeringRoute + '_>, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixMetadataCommandPeeringRoute {
            client: self,
            pg_id,
            cluster_epoch,
        }))
    }
}

impl MetadataCommandPeeringRoute for UnixMetadataCommandPeeringRoute<'_> {
    fn validate_metadata_command_replay_state(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::validate_metadata_command_replay_state(
            self.client,
            self.pg_id,
            self.cluster_epoch,
            false,
        )
    }

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        UnixStorageNodeClient::validate_metadata_command_replay_state(
            self.client,
            self.pg_id,
            self.cluster_epoch,
            true,
        )
    }

    fn initialize_metadata_transfer_empty_state(
        &self,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.require_current_epoch()?;
        UnixStorageNodeClient::initialize_metadata_transfer_empty_state(
            self.client,
            self.pg_id,
            expected_state_digest,
        )
    }

    fn initialize_metadata_transfer_matching_state(
        &self,
        applied_log_index: u64,
        applied_log_hash: MetadataCommandLogHash,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.require_current_epoch()?;
        UnixStorageNodeClient::initialize_metadata_transfer_matching_state(
            self.client,
            self.pg_id,
            applied_log_index,
            applied_log_hash,
            expected_state_digest,
        )
    }

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        commands: &[MetadataTransferCommand],
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.require_current_epoch()?;
        self.validate_transfer_commands(commands)?;
        UnixStorageNodeClient::adopt_metadata_transfer_state_from_rebased_commands(
            self.client,
            self.pg_id,
            commands,
            expected_state_digest,
        )
    }

    fn install_metadata_transfer_checkpoint_base(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.require_current_epoch()?;
        self.validate_checkpoint_pg(checkpoint)?;
        UnixStorageNodeClient::install_metadata_transfer_checkpoint_base(
            self.client,
            self.pg_id,
            checkpoint,
        )
    }

    fn replay_metadata_command_for_peering(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.require_current_epoch()
            .and_then(|()| self.validate_command_route(command))
            .map_err(BucketSnapshotLoadError::Store)?;
        UnixStorageNodeClient::replay_metadata_command_for_peering(self.client, self.pg_id, command)
    }
}

struct UnixMetadataCommandRecoveryReplicaRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    authorized_source: &'a MetadataCommandEnvelope,
    abandoned_source: Option<&'a MetadataCommandEnvelope>,
    command: &'a MetadataCommandEnvelope,
}

impl<'a> UnixMetadataCommandRecoveryReplicaRoute<'a> {
    fn new(
        client: &'a UnixStorageNodeClient,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<Self, StoreError> {
        if cluster_epoch != client.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: client.cluster_epoch,
            });
        }
        let route = Self {
            client,
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        };
        route.validate_command_route(authorized_source)?;
        if let Some(abandoned_source) = abandoned_source {
            route.validate_command_route(abandoned_source)?;
        }
        route.validate_command_route(command)?;
        validate_metadata_command_recovery_certificate(
            authorized_source,
            abandoned_source,
            command,
        )
        .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
            operation: "open metadata command recovery replica route",
        })?;
        Ok(route)
    }

    fn validate_command_route(&self, command: &MetadataCommandEnvelope) -> Result<(), StoreError> {
        if command.id().pg_id() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: self.client.node_id.as_u32(),
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id.get(),
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(())
    }

    fn request(&self) -> StorageRpcMetadataCommandRecoveryRequest {
        StorageRpcMetadataCommandRecoveryRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: self.pg_id,
            authorized_source: self.authorized_source.clone(),
            abandoned_source: self.abandoned_source.cloned(),
            command: self.command.clone(),
        }
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

    fn open_metadata_command_recovery_critical_section_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<Box<dyn MetadataCommandRecoveryCriticalSection>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(
            UnixStorageNodeClient::open_metadata_command_critical_section_until(
                self, pg_id, deadline,
            )?,
        ))
    }

    fn open_metadata_command_recovery_replica_apply_route<'a>(
        &'a self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<Box<dyn MetadataCommandRecoveryReplicaApplyRoute + 'a>, StoreError> {
        Ok(Box::new(UnixMetadataCommandRecoveryReplicaRoute::new(
            self,
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        )?))
    }

    fn open_metadata_command_recovery_replica_abandon_route<'a>(
        &'a self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &'a MetadataCommandEnvelope,
        abandoned_source: Option<&'a MetadataCommandEnvelope>,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<Box<dyn MetadataCommandRecoveryReplicaAbandonRoute + 'a>, StoreError> {
        Ok(Box::new(UnixMetadataCommandRecoveryReplicaRoute::new(
            self,
            pg_id,
            cluster_epoch,
            authorized_source,
            abandoned_source,
            command,
        )?))
    }
}

impl MetadataCommandRecoveryReplicaApplyRoute for UnixMetadataCommandRecoveryReplicaRoute<'_> {
    fn apply(self: Box<Self>) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let payload = encode_metadata_command_recovery_request(&self.request())
            .map_err(|error| {
                self.client.rpc_payload_error(
                    "encode metadata command recovery replica apply request",
                    error.to_string(),
                )
            })
            .map_err(BucketSnapshotLoadError::Store)?;
        self.client.metadata_command_apply_and_record_with_payload(
            self.pg_id,
            self.command,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            payload,
            "decode metadata command recovery replica apply response",
        )
    }

    fn apply_until(
        self: Box<Self>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let payload = encode_metadata_command_recovery_request(&self.request())
            .map_err(|error| {
                self.client.rpc_payload_error(
                    "encode metadata command recovery replica apply request",
                    error.to_string(),
                )
            })
            .map_err(MetadataCommandApplyError::not_sent)?;
        self.client
            .metadata_command_apply_and_record_with_payload_until(
                self.pg_id,
                self.command,
                StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
                payload,
                "decode metadata command recovery replica apply response",
                deadline,
            )
    }
}

impl MetadataCommandRecoveryReplicaAbandonRoute for UnixMetadataCommandRecoveryReplicaRoute<'_> {
    fn record_abandoned(self: Box<Self>) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload =
            encode_metadata_command_recovery_request(&self.request()).map_err(|error| {
                self.client.rpc_payload_error(
                    "encode metadata command recovery replica abandonment request",
                    error.to_string(),
                )
            })?;
        let response = self.client.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned,
            payload,
        )?;
        self.client.decode_metadata_command_abandonment_response(
            self.pg_id,
            &response,
            "decode metadata command recovery replica abandonment response",
        )
    }

    fn record_abandoned_until(
        self: Box<Self>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload =
            encode_metadata_command_recovery_request(&self.request()).map_err(|error| {
                self.client.rpc_payload_error(
                    "encode metadata command recovery replica abandonment request",
                    error.to_string(),
                )
            })?;
        let response = self.client.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned,
            payload,
            deadline,
        )?;
        self.client.decode_metadata_command_abandonment_response(
            self.pg_id,
            &response,
            "decode metadata command recovery replica abandonment response",
        )
    }
}

impl MetadataCommandNodeClient for UnixStorageNodeClient {
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
        Ok(Box::new(
            UnixStorageNodeClient::open_metadata_command_critical_section(self, pg_id)?,
        ))
    }

    fn open_metadata_command_critical_section_until(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<Box<dyn MetadataCommandCriticalSection>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(
            UnixStorageNodeClient::open_metadata_command_critical_section_until(
                self, pg_id, deadline,
            )?,
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

    fn try_insert_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            None,
            Some(deadline),
        )
    }

    fn try_insert_pending_metadata_command_slot_classified_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<(), MetadataCommandPendingSlotInsertError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_classified_until(
            pg_id,
            command,
            bucket,
            None,
            Some(deadline),
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

    fn try_insert_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
            Some(deadline),
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence_classified_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), MetadataCommandPendingSlotInsertError> {
        effect_fence
            .require_valid_for(command.id().cluster_epoch())
            .map_err(MetadataCommandPendingSlotInsertError::not_sent)?;
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_classified_until(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
            Some(deadline),
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

    fn try_insert_bucket_control_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            None,
            Some(deadline),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
            Some(deadline),
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

    fn apply_metadata_command_and_record_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        UnixStorageNodeClient::apply_metadata_command_and_record_until(
            self, pg_id, command, deadline,
        )
    }

    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::record_metadata_command_abandoned_on_replica(self, pg_id, command)
    }

    fn record_metadata_command_abandoned_on_replica_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if command.id().cluster_epoch() != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: command.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        UnixStorageNodeClient::record_metadata_command_abandoned_on_replica_until(
            self, pg_id, command, deadline,
        )
    }
}

impl UnixStorageNodeClient {
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

    fn record_metadata_command_abandoned_on_replica_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
            deadline,
        )?;
        self.decode_metadata_command_abandonment_response(
            pg_id,
            &response,
            "decode metadata command replica abandonment response",
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
            StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => {
                if name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot response",
                        "bucket-not-found response name does not match request".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketNotFound { name },
                ))
            }
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
        let response = decode_create_bucket_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
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
        let response = decode_multipart_completion_barrier_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
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
        let response = decode_bucket_mark_deleting_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
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
        match response.outcome {
            StorageRpcBucketSubresourceGetOutcome::Loaded(body) => Ok(body),
            StorageRpcBucketSubresourceGetOutcome::BucketNotFound { name } => {
                if name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket subresource get response",
                        "bucket-not-found response name does not match request".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketNotFound { name },
                ))
            }
        }
    }

    fn open_bucket_metadata_scan_route_impl(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        let pg_topology = self.pg_topology.as_ref().ok_or_else(|| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "open bucket metadata scan route",
                "bucket metadata scan client has no installed PG topology".to_string(),
            ))
        })?;
        Ok(Box::new(UnixBucketMetadataScanRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            pg_topology: Arc::clone(pg_topology),
        }))
    }
}

impl BucketMetadataNodeClient for UnixStorageNodeClient {
    fn open_bucket_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.validate_bucket_route_subject(
            route_cluster_epoch,
            pg_id,
            bucket,
            "open bucket metadata route",
        )?;
        Ok(Box::new(UnixBucketMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_bucket_metadata_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn BucketMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.open_bucket_metadata_route(route_cluster_epoch, pg_id, bucket)
    }

    fn open_bucket_delete_replica_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketDeleteReplicaMetadataRoute + '_>, BucketSnapshotLoadError> {
        self.validate_bucket_route_subject(
            route_cluster_epoch,
            pg_id,
            bucket,
            "open bucket delete replica metadata route",
        )?;
        Ok(Box::new(UnixBucketDeleteReplicaMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_bucket_metadata_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        self.open_bucket_metadata_scan_route_impl(route_cluster_epoch, pg_id)
    }

    fn open_bucket_metadata_read_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn BucketMetadataScanRoute + '_>, BucketSnapshotLoadError> {
        self.open_bucket_metadata_scan_route_impl(route_cluster_epoch, pg_id)
    }
}

impl UnixBucketMetadataRoute<'_> {
    fn require_command_id(
        &self,
        command_id: MetadataCommandId,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if command_id.cluster_epoch() != self.route_cluster_epoch
            || command_id.pg_id() != self.pg_id.pg_id()
        {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "metadata command ID does not match the scoped bucket metadata route"
                        .to_string(),
                ),
            ));
        }
        Ok(())
    }

    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if bucket != &self.bucket {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "command bucket does not match the scoped bucket metadata route".to_string(),
                ),
            ));
        }
        Ok(())
    }
}

impl BucketMetadataRoute for UnixBucketMetadataRoute<'_> {
    fn head_bucket_raw(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        self.client.head_bucket_raw(self.pg_id, &self.bucket)
    }

    fn head_bucket_info(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        self.client.head_bucket_info(self.pg_id, &self.bucket)
    }

    fn load_bucket_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        self.client
            .load_bucket_snapshot(self.pg_id, &self.bucket, request)
    }

    fn build_create_bucket_command(
        &self,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build create bucket command")?;
        if config.name != self.bucket.as_str() {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    "build create bucket command",
                    "create-bucket config does not match the scoped bucket metadata route"
                        .to_string(),
                ),
            ));
        }
        self.client
            .build_create_bucket_command(self.pg_id, &self.bucket, command_id, config)
    }

    fn build_advance_multipart_completion_barrier_command(
        &self,
        command_id: MetadataCommandId,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build multipart completion barrier command")?;
        if !bucket_write_reservation.matches_exact_mutation_subject(
            self.route_cluster_epoch,
            &self.bucket,
            crate::metadata_command::COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            Some(completion_target_context),
        ) {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: bucket_write_reservation.reservation_id.clone(),
            }
            .into());
        }
        self.client
            .build_advance_multipart_completion_barrier_command(
                self.pg_id,
                &self.bucket,
                command_id,
                completion_target_context,
                bucket_write_reservation,
            )
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending mark bucket deleting command",
        )?;
        self.client
            .pending_mark_bucket_deleting_command_matches_current(self.pg_id, &self.bucket, command)
    }

    fn build_mark_bucket_deleting_command(
        &self,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build mark bucket deleting command")?;
        self.client
            .build_mark_bucket_deleting_command(self.pg_id, &self.bucket, command_id)
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket versioning command",
        )?;
        self.client
            .pending_put_bucket_versioning_command_matches_current(
                self.pg_id,
                &self.bucket,
                command,
                state,
            )
    }

    fn build_put_bucket_versioning_command(
        &self,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket versioning command")?;
        self.client
            .build_put_bucket_versioning_command(self.pg_id, &self.bucket, command_id, state)
    }

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket ACL command",
        )?;
        self.client.pending_put_bucket_acl_command_matches_current(
            self.pg_id,
            &self.bucket,
            command,
            acl_grants,
            summary,
        )
    }

    fn build_put_bucket_acl_command(
        &self,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket ACL command")?;
        self.client.build_put_bucket_acl_command(
            self.pg_id,
            &self.bucket,
            command_id,
            acl_grants,
            summary,
        )
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.require_bucket(
            command.bucket_name(),
            "match pending put bucket property command",
        )?;
        self.client
            .pending_put_bucket_property_command_matches_current(
                self.pg_id,
                &self.bucket,
                command,
                mutation,
            )
    }

    fn build_put_bucket_property_command(
        &self,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket property command")?;
        self.client.build_put_bucket_property_command(
            self.pg_id,
            &self.bucket,
            command_id,
            mutation,
        )
    }

    fn build_put_bucket_subresource_command(
        &self,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        self.require_command_id(command_id, "build put bucket subresource command")?;
        self.client.build_put_bucket_subresource_command(
            self.pg_id,
            &self.bucket,
            command_id,
            mutation,
        )
    }

    fn get_bucket_subresource(
        &self,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_bucket_metadata_read_hook(&self.bucket);
        self.client
            .get_bucket_subresource(self.pg_id, &self.bucket, kind)
    }
}

impl BucketDeleteReplicaMetadataRoute for UnixBucketDeleteReplicaMetadataRoute<'_> {
    fn head_bucket_replica_for_delete(&self) -> Result<BucketInfo, BucketSnapshotLoadError> {
        debug_assert_eq!(self.route_cluster_epoch, self.client.cluster_epoch);
        self.client
            .head_bucket_replica_for_delete(self.pg_id, &self.bucket)
    }
}

impl UnixBucketMetadataScanRoute<'_> {
    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if self.pg_topology.bucket_pg_for(bucket) != self.pg_id.get() {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "bucket does not belong to the scoped bucket metadata PG".to_string(),
                ),
            ));
        }
        Ok(())
    }
}

impl BucketMetadataScanRoute for UnixBucketMetadataScanRoute<'_> {
    fn list_buckets(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketListRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            owner_canonical_id: owner_canonical_id.to_string(),
        };
        let payload = encode_bucket_list_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.client
                    .rpc_payload_error("encode bucket list request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::BucketList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_list_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.client
                    .rpc_payload_error("decode bucket list response", error.to_string()),
            )
        })?;
        let mut seen_bucket_names = BTreeSet::new();
        for bucket in &response.buckets {
            if !seen_bucket_names.insert(&bucket.name) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket list response",
                        format!(
                            "bucket {} appears more than once in the response",
                            bucket.name.as_str()
                        ),
                    ),
                ));
            }
            if bucket.owner_canonical_id.as_str() != owner_canonical_id {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket list response",
                        format!(
                            "bucket {} owner does not match requested owner",
                            bucket.name.as_str()
                        ),
                    ),
                ));
            }
            self.require_bucket(&bucket.name, "validate bucket list response")?;
        }
        Ok(response.buckets)
    }

    fn load_bucket_execution_generations(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        for bucket in buckets {
            self.require_bucket(bucket, "load bucket execution generations")?;
        }
        let request = StorageRpcBucketBatchRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            buckets: buckets.to_vec(),
        };
        let payload = encode_bucket_batch_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                "encode bucket execution generations request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::BucketExecutionGenerations, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_execution_generations_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                    "decode bucket execution generations response",
                    error.to_string(),
                ))
            })?;
        for bucket in response.generations.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket execution generations response",
                        format!("unexpected bucket {}", bucket.as_str()),
                    ),
                ));
            }
            self.require_bucket(bucket, "validate bucket execution generations response")?;
        }
        Ok(response.generations)
    }

    fn load_bucket_fast_path_identities(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        for bucket in buckets {
            self.require_bucket(bucket, "load bucket fast-path identities")?;
        }
        let request = StorageRpcBucketBatchRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            buckets: buckets.to_vec(),
        };
        let payload = encode_bucket_batch_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                "encode bucket fast-path identities request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::BucketFastPathIdentities, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_fast_path_identities_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                "decode bucket fast-path identities response",
                error.to_string(),
            ))
        })?;
        for bucket in response.identities.keys() {
            if !buckets.iter().any(|requested| requested == bucket) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket fast-path identities response",
                        format!("unexpected bucket {}", bucket.as_str()),
                    ),
                ));
            }
            self.require_bucket(bucket, "validate bucket fast-path identities response")?;
        }
        Ok(response.identities)
    }
}
