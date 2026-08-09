// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

struct StorageRpcDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> StorageRpcDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn finish(&self) -> Result<(), StorageRpcPayloadError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageRpcPayloadError::TrailingBytes)
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], StorageRpcPayloadError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or(StorageRpcPayloadError::Truncated)?;
        if end > self.bytes.len() {
            return Err(StorageRpcPayloadError::Truncated);
        }
        let slice = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(slice)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_bytes_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(too_large_error);
        }
        self.read_exact(len)
    }

    fn read_bytes_with_payload_limit(
        &mut self,
        limit: usize,
    ) -> Result<&'a [u8], StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(StorageRpcPayloadError::PayloadTooLarge { len, limit });
        }
        self.read_exact(len)
    }

    fn read_metadata_command_item(
        &mut self,
    ) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
        let command_checksum = self.read_u64()?;
        let command_bytes = self
            .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN)?
            .to_vec();
        validate_metadata_command_item(command_checksum, command_bytes)
    }

    fn read_metadata_command_envelope_bytes(
        &mut self,
        authority: &MetadataCommandDecodeAuthority,
    ) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
        let command_bytes = self
            .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN)?
            .to_vec();
        decode_metadata_command_envelope(&command_bytes, authority)
            .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
    }

    fn read_string(&mut self) -> Result<String, StorageRpcPayloadError> {
        std::str::from_utf8(self.read_bytes()?)
            .map(str::to_owned)
            .map_err(|_| StorageRpcPayloadError::InvalidUtf8)
    }

    fn read_string_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<String, StorageRpcPayloadError> {
        std::str::from_utf8(self.read_bytes_with_limit(limit, too_large_error)?)
            .map(str::to_owned)
            .map_err(|_| StorageRpcPayloadError::InvalidUtf8)
    }

    fn read_count_with_limit(&mut self, limit: usize) -> Result<usize, StorageRpcPayloadError> {
        let len = self.read_u32()? as usize;
        if len > limit {
            return Err(StorageRpcPayloadError::PayloadTooLarge { len, limit });
        }
        Ok(len)
    }

    fn read_string_vec_with_limit(
        &mut self,
        limit: usize,
    ) -> Result<Vec<String>, StorageRpcPayloadError> {
        let count = self.read_count_with_limit(limit)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.read_string()?);
        }
        Ok(values)
    }

    fn read_bucket_name(&mut self) -> Result<BucketName, StorageRpcPayloadError> {
        BucketName::try_from(self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_NAME_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("bucket name exceeds maximum length"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid bucket name"))
    }

    fn read_object_key(&mut self) -> Result<ObjectKey, StorageRpcPayloadError> {
        ObjectKey::try_from(self.read_string_with_limit(
            STORAGE_RPC_MAX_OBJECT_KEY_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("object key exceeds maximum length"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid object key"))
    }

    fn read_optional_object_key(&mut self) -> Result<Option<ObjectKey>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_object_key()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object key tag",
            )),
        }
    }

    fn read_admitted_route_effect_deadline(
        &mut self,
        operation: &'static str,
    ) -> Result<Option<StorageRpcAdmittedRouteEffectDeadline>, StorageRpcPayloadError> {
        let deadline = match self.read_u8()? {
            0 => None,
            1 => Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: self.read_u64()?,
                portable_wall_valid_until_ms: self.read_u64()?,
            }),
            _ => {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid admitted route effect deadline tag",
                ))
            }
        };
        if deadline
            .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                operation,
            ));
        }
        Ok(deadline)
    }

    fn read_rpc_object_request(
        &mut self,
    ) -> Result<StorageRpcObjectRequest, StorageRpcPayloadError> {
        let node_id = NodeId::new(self.read_u32()?);
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = PgId::new(self.read_u32()?);
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        Ok(StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        })
    }

    fn read_session_id(&mut self) -> Result<SessionId, StorageRpcPayloadError> {
        SessionId::try_from(self.read_string_with_limit(
            SESSION_ID_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("session id is too large"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid session id"))
    }

    fn read_optional_session_id(&mut self) -> Result<Option<SessionId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_session_id()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional session id tag",
            )),
        }
    }

    fn read_generation_id(&mut self) -> Result<GenerationId, StorageRpcPayloadError> {
        GenerationId::new(self.read_u64()?).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
            "generation id must not be zero",
        ))
    }

    fn read_optional_generation_id(
        &mut self,
    ) -> Result<Option<GenerationId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_generation_id()?)),
            _ => Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "invalid optional generation id tag",
            )),
        }
    }

    fn read_optional_payload_reclaim_root(
        &mut self,
    ) -> Result<Option<PayloadReclaimRoot>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(PayloadReclaimRoot {
                bucket: self.read_bucket_name()?,
                key: self.read_object_key()?,
                generation_id: self.read_generation_id()?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional payload reclaim root tag",
            )),
        }
    }

    fn read_object_payload_reclaim_kind(
        &mut self,
    ) -> Result<ObjectPayloadReclaimKind, StorageRpcPayloadError> {
        ObjectPayloadReclaimKind::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidDurableClaimToken("invalid object reclaim kind"),
        )
    }

    fn read_shard_key(&mut self) -> Result<ShardKey, StorageRpcPayloadError> {
        let bytes = self.read_bytes()?;
        ShardKey::from_bytes(bytes).map_err(|_| StorageRpcPayloadError::Truncated)
    }

    fn read_shard_location(&mut self) -> Result<StorageRpcShardLocation, StorageRpcPayloadError> {
        let cluster_epoch = ClusterEpoch::new(self.read_u64()?).ok_or(
            StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "cluster epoch must not be zero",
            ),
        )?;
        let pg_id = PgId::new(self.read_u32()?);
        let shard_index = ShardIndex::new(self.read_u8()?);
        let node_id = NodeId::new(self.read_u32()?);
        Ok(StorageRpcShardLocation {
            cluster_epoch,
            pg_id,
            shard_index,
            node_id,
        })
    }

    fn read_bucket_claim_token(
        &mut self,
    ) -> Result<StorageRpcBucketClaimToken, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        Ok(StorageRpcBucketClaimToken {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
        })
    }

    fn read_object_payload_reclaim_claim_token(
        &mut self,
    ) -> Result<StorageRpcObjectPayloadReclaimClaimToken, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let reclaim_kind = self.read_object_payload_reclaim_kind()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        Ok(StorageRpcObjectPayloadReclaimClaimToken {
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
        })
    }

    fn read_claim_token(&mut self) -> Result<StorageRpcDurableClaimToken, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcDurableClaimToken::ObjectPayloadReclaim(
                self.read_object_payload_reclaim_claim_token()?,
            )),
            1 => Ok(StorageRpcDurableClaimToken::BucketDeleteFinalize(
                self.read_bucket_claim_token()?,
            )),
            2 => Ok(StorageRpcDurableClaimToken::LifecycleSweep(
                self.read_bucket_claim_token()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "invalid claim token kind",
            )),
        }
    }

    fn read_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<BucketWriteReservationProof, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let reservation_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "reservation id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let operation_kind = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "operation kind exceeds maximum length",
            ),
        )?;
        let created_at = self.read_u64()?;
        let lease_deadline = self.read_u64()?;
        let target_context = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "target context exceeds maximum length",
            ),
        )?;
        Ok(BucketWriteReservationProof {
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            bucket_execution_generation,
            bucket_incarnation_generation,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        })
    }

    fn read_bucket_write_reservation_record(
        &mut self,
    ) -> Result<BucketWriteReservationRecord, StorageRpcPayloadError> {
        let proof = self.read_bucket_write_reservation_proof()?;
        Ok(BucketWriteReservationRecord {
            bucket: proof.bucket,
            reservation_id: proof.reservation_id,
            owner_token: proof.owner_token,
            cluster_epoch: proof.cluster_epoch,
            bucket_execution_generation: proof.bucket_execution_generation,
            bucket_incarnation_generation: proof.bucket_incarnation_generation,
            operation_kind: proof.operation_kind,
            created_at: proof.created_at,
            lease_deadline: proof.lease_deadline,
            target_context: proof.target_context,
        })
    }

    fn read_bucket_write_drain_record(
        &mut self,
    ) -> Result<BucketWriteDrainRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let drain_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "drain id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let state = match self.read_u8()? {
            0 => BucketWriteDrainState::Draining,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket write drain state",
                ));
            }
        };
        let created_at = self.read_u64()?;
        let lease_deadline = self.read_u64()?;
        Ok(BucketWriteDrainRecord {
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            bucket_execution_generation,
            state,
            created_at,
            lease_deadline,
        })
    }

    fn read_bucket_delete_attempt_outcome_record(
        &mut self,
    ) -> Result<BucketDeleteAttemptOutcomeRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
        })?;
        let drain_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "drain id exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let bucket_execution_generation = self.read_u64()?;
        let outcome = match self.read_u8()? {
            0 => BucketDeleteAttemptOutcomeKind::Retryable,
            1 => BucketDeleteAttemptOutcomeKind::NotEmpty,
            2 => BucketDeleteAttemptOutcomeKind::StaleGeneration,
            3 => BucketDeleteAttemptOutcomeKind::MarkDeleting,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket delete attempt outcome",
                ));
            }
        };
        let phase = match self.read_u8()? {
            0 => BucketDeleteAttemptPhase::Initial,
            1 => BucketDeleteAttemptPhase::ReservationWait,
            2 => BucketDeleteAttemptPhase::PostReservationObjectDrain,
            3 => BucketDeleteAttemptPhase::StreamCleanup,
            4 => BucketDeleteAttemptPhase::FinalVisibilityCheck,
            5 => BucketDeleteAttemptPhase::FinalVisibilityProven,
            6 => BucketDeleteAttemptPhase::MarkDeleting,
            7 => BucketDeleteAttemptPhase::PostReservationStreamCleanup,
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                    "invalid bucket delete attempt phase",
                ));
            }
        };
        let detail = self.read_string_with_limit(
            BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "bucket delete attempt outcome detail exceeds maximum length",
            ),
        )?;
        let post_reservation_next_object_pg_id = self.read_optional_u32()?;
        let stream_cleanup_next_object_pg_id = self.read_optional_u32()?;
        let stream_cleanup_next_session_id_marker = self.read_optional_session_id()?;
        let stream_cleanup_aborted_uploads = self.read_bool()?;
        let final_visibility_next_object_pg_id = self.read_optional_u32()?;
        let finalizer_next_object_pg_id = self.read_optional_u32()?;
        let updated_at = self.read_u64()?;
        Ok(BucketDeleteAttemptOutcomeRecord {
            bucket,
            drain_id,
            cluster_epoch,
            bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id,
            stream_cleanup_next_object_pg_id,
            stream_cleanup_next_session_id_marker,
            stream_cleanup_aborted_uploads,
            final_visibility_next_object_pg_id,
            finalizer_next_object_pg_id,
            updated_at,
        })
    }

    fn read_bucket_delete_finalize_root(
        &mut self,
    ) -> Result<BucketDeleteFinalizeRoot, StorageRpcPayloadError> {
        Ok(BucketDeleteFinalizeRoot {
            bucket: self.read_bucket_name()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_delete_begin_root(
        &mut self,
    ) -> Result<BucketDeleteBeginRoot, StorageRpcPayloadError> {
        Ok(BucketDeleteBeginRoot {
            bucket: self.read_bucket_name()?,
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_delete_finalize_claim_record(
        &mut self,
    ) -> Result<BucketDeleteFinalizeClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "claim id exceeds maximum length",
            ),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "owner token exceeds maximum length",
            ),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "last error exceeds maximum length",
            ),
        )?;
        Ok(BucketDeleteFinalizeClaimRecord {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_object_payload_reclaim_claim_record(
        &mut self,
    ) -> Result<ObjectPayloadReclaimClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let reclaim_kind = self.read_object_payload_reclaim_kind()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("last error exceeds maximum length"),
        )?;
        Ok(ObjectPayloadReclaimClaimRecord {
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_lifecycle_sweep_root(&mut self) -> Result<LifecycleSweepRoot, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let source = match self.read_u8()? {
            0 => LifecycleSweepRootSource::ExpiredClaim,
            1 => LifecycleSweepRootSource::BusyClaim,
            2 => LifecycleSweepRootSource::LifecycleConfig,
            3 => LifecycleSweepRootSource::AbortingMultipartUpload,
            _ => {
                return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                    "invalid lifecycle sweep root source",
                ))
            }
        };
        Ok(LifecycleSweepRoot {
            bucket,
            bucket_incarnation_generation,
            source,
        })
    }

    fn read_lifecycle_sweep_claim_record(
        &mut self,
    ) -> Result<LifecycleSweepClaimRecord, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let claim_id = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
        )?;
        let owner_token = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
        )?;
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = self.read_u32()?;
        let claimed_at = self.read_u64()?;
        let heartbeat_at = self.read_u64()?;
        let lease_deadline = self.read_optional_u64()?;
        let attempt_count = self.read_u64()?;
        let last_error = self.read_optional_string_with_limit(
            4096,
            StorageRpcPayloadError::InvalidDurableClaimToken(
                "lifecycle claim error exceeds maximum length",
            ),
        )?;
        Ok(LifecycleSweepClaimRecord {
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            pg_id,
            claimed_at,
            heartbeat_at,
            lease_deadline,
            attempt_count,
            last_error,
        })
    }

    fn read_bucket_request(&mut self) -> Result<StorageRpcBucketRequest, StorageRpcPayloadError> {
        Ok(StorageRpcBucketRequest {
            node_id: NodeId::new(self.read_u32()?),
            cluster_epoch: self.read_cluster_epoch()?,
            pg_id: PgId::new(self.read_u32()?),
            bucket: self.read_bucket_name()?,
        })
    }

    fn read_bucket_pg_request(
        &mut self,
    ) -> Result<StorageRpcBucketPgRequest, StorageRpcPayloadError> {
        Ok(StorageRpcBucketPgRequest {
            node_id: NodeId::new(self.read_u32()?),
            cluster_epoch: self.read_cluster_epoch()?,
            pg_id: PgId::new(self.read_u32()?),
        })
    }

    fn read_cluster_epoch(&mut self) -> Result<ClusterEpoch, StorageRpcPayloadError> {
        ClusterEpoch::new(self.read_u64()?).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
            "cluster epoch must not be zero",
        ))
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional u64 tag",
            )),
        }
    }

    fn read_optional_u32(&mut self) -> Result<Option<u32>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u32()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional u32 tag",
            )),
        }
    }

    fn read_optional_version_id(&mut self) -> Result<Option<VersionId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(VersionId::from_u64(self.read_u64()?))),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional version id tag",
            )),
        }
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional string tag",
            )),
        }
    }

    fn read_optional_string_with_limit(
        &mut self,
        limit: usize,
        too_large_error: StorageRpcPayloadError,
    ) -> Result<Option<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string_with_limit(limit, too_large_error)?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional string tag",
            )),
        }
    }

    fn read_create_bucket_config(
        &mut self,
    ) -> Result<StorageRpcCreateBucketConfig, StorageRpcPayloadError> {
        Ok(StorageRpcCreateBucketConfig {
            name: self.read_bucket_name()?,
            owner_principal: self.read_string_with_limit(
                STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "owner principal is too large",
                ),
            )?,
            owner_canonical_id: self.read_canonical_user_id()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            public_write: self.read_bool()?,
            versioning: self.read_bucket_versioning_state()?,
            object_lock: self.read_bucket_object_lock_config()?,
            ownership_controls: self.read_bucket_ownership_controls()?,
        })
    }

    fn read_bucket_info(&mut self) -> Result<BucketInfo, StorageRpcPayloadError> {
        let name = self.read_bucket_name()?;
        let owner_principal = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
            StorageRpcPayloadError::InvalidResponseEnvelope("owner principal is too large"),
        )?;
        let owner_canonical_id = self.read_canonical_user_id()?;
        let created_at = self.read_u64()?;
        let region = self.read_u16()?;
        let state = self.read_bucket_state()?;
        let versioning = self.read_bucket_versioning_state()?;
        let object_lock = self.read_bucket_object_lock_config()?;
        let acl_grants = self.read_acl_grants()?;
        let public_read = self.read_bool()?;
        let public_write = self.read_bool()?;
        let public_access_block = self.read_optional_public_access_block_config()?;
        let ownership_controls = self.read_optional_bucket_ownership_controls()?;
        let bucket_policy_present = self.read_bool()?;
        let bucket_policy_public = self.read_bool()?;
        let bucket_policy_generation = self.read_u64()?;
        let bucket_lifecycle_present = self.read_bool()?;
        let bucket_lifecycle_generation = self.read_u64()?;
        let bucket_execution_generation = self.read_u64()?;
        let bucket_incarnation_generation = self.read_u64()?;
        let multipart_upload_id_key = self
            .read_bytes()?
            .try_into()
            .map(MultipartUploadIdKey::from_bytes)
            .map_err(|_| {
                StorageRpcPayloadError::InvalidResponseEnvelope(
                    "multipart upload ID key must be 32 bytes",
                )
            })?;
        Ok(BucketInfo {
            name,
            owner_principal,
            owner_canonical_id,
            created_at,
            region,
            state,
            versioning,
            object_lock,
            acl_grants,
            public_read,
            public_write,
            public_access_block,
            ownership_controls,
            bucket_policy_present,
            bucket_policy_public,
            bucket_policy_generation,
            bucket_lifecycle_present,
            bucket_lifecycle_generation,
            bucket_execution_generation,
            bucket_incarnation_generation,
            multipart_upload_id_key,
            bucket_abac_enabled: self.read_bool()?,
            encryption: self.read_effective_bucket_encryption_config()?,
        })
    }

    fn read_bucket_fast_path_identity(
        &mut self,
    ) -> Result<BucketFastPathIdentity, StorageRpcPayloadError> {
        Ok(BucketFastPathIdentity {
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
        })
    }

    fn read_bucket_snapshot_request(
        &mut self,
    ) -> Result<BucketSnapshotRequest, StorageRpcPayloadError> {
        Ok(BucketSnapshotRequest {
            policy: self.read_bool()?,
            tags: match self.read_u8()? {
                0 => BucketSnapshotTagsRequest::NotRequested,
                1 => BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                2 => BucketSnapshotTagsRequest::Always,
                _ => {
                    return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "invalid bucket snapshot tags request",
                    ));
                }
            },
            lifecycle: self.read_bool()?,
            cors: self.read_bool()?,
        })
    }

    fn read_rpc_bucket_snapshot_request(
        &mut self,
    ) -> Result<StorageRpcBucketSnapshotRequest, StorageRpcPayloadError> {
        let node_id = NodeId::new(self.read_u32()?);
        let cluster_epoch = self.read_cluster_epoch()?;
        let pg_id = PgId::new(self.read_u32()?);
        let bucket = self.read_bucket_name()?;
        let request = self.read_bucket_snapshot_request()?;
        Ok(StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id,
                cluster_epoch,
                pg_id,
                bucket,
            },
            request,
        })
    }

    fn read_bucket_snapshot(&mut self) -> Result<BucketSnapshot, StorageRpcPayloadError> {
        Ok(BucketSnapshot {
            bucket: self.read_bucket_info()?,
            request: self.read_bucket_snapshot_request()?,
            policy: self.read_loaded_bucket_subresource()?,
            tags: self.read_loaded_bucket_tags()?,
            lifecycle: self.read_loaded_bucket_subresource()?,
            cors: self.read_loaded_bucket_subresource()?,
        })
    }

    fn read_loaded_bucket_subresource(
        &mut self,
    ) -> Result<LoadedBucketSubresource<String>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(LoadedBucketSubresource::NotRequested),
            1 => Ok(LoadedBucketSubresource::Missing),
            2 => Ok(LoadedBucketSubresource::Loaded(self.read_string()?)),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid loaded bucket subresource tag",
            )),
        }
    }

    fn read_loaded_bucket_tags(
        &mut self,
    ) -> Result<LoadedBucketSubresource<SerializedBucketTagSet>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(LoadedBucketSubresource::NotRequested),
            1 => Ok(LoadedBucketSubresource::Missing),
            2 => SerializedBucketTagSet::from_current_xml(self.read_string()?)
                .map(LoadedBucketSubresource::Loaded)
                .map_err(|_| {
                    StorageRpcPayloadError::InvalidResponseEnvelope("invalid bucket tags")
                }),
            _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid loaded bucket subresource tag",
            )),
        }
    }

    fn read_direct_put_commit_storage_snapshot(
        &mut self,
    ) -> Result<DirectPutCommitStorageSnapshot, StorageRpcPayloadError> {
        let existing_etag = self.read_optional_string()?;
        let current = self.read_optional_stored_object()?;
        let committed_segments = self.read_optional_object_segments()?;
        let committed_stale_generation_id = self.read_optional_generation_id()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let stale_payload = self.read_optional_object_payload_reclaim()?;
        Ok(DirectPutCommitStorageSnapshot {
            auth_snapshot: crate::DirectPutCommitSnapshot { existing_etag },
            current,
            committed_segments,
            committed_stale_generation_id,
            stale_payload_source,
            stale_payload,
        })
    }

    fn read_optional_object_segments(
        &mut self,
    ) -> Result<Option<Vec<ObjectSegmentRecord>>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let segment_count = self.read_bounded_remaining_count(
                    STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN,
                    "direct PUT committed segment count exceeds payload",
                )?;
                let mut segments = Vec::with_capacity(segment_count);
                for _ in 0..segment_count {
                    segments.push(self.read_object_segment_record()?);
                }
                Ok(Some(segments))
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object segments tag",
            )),
        }
    }

    fn read_optional_object_payload_reclaim(
        &mut self,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_object_payload_reclaim()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object payload reclaim tag",
            )),
        }
    }

    fn read_object_payload_reclaim(
        &mut self,
    ) -> Result<ObjectPayloadReclaimCommand, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectPayloadReclaimCommand::Segments(
                self.read_object_segments_reclaim_record()?,
            )),
            1 => Ok(ObjectPayloadReclaimCommand::Multipart(
                self.read_multipart_reclaim_record()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object payload reclaim tag",
            )),
        }
    }

    fn read_object_segments_reclaim_record(
        &mut self,
    ) -> Result<ObjectSegmentsReclaimRecord, StorageRpcPayloadError> {
        const MIN_SEGMENT_LEN: usize = 4 + 4 + 16 + 8 + 4 + 2;

        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let created_at = self.read_u64()?;
        let segment_count = self.read_bounded_remaining_count(
            MIN_SEGMENT_LEN,
            "object segment reclaim count exceeds payload",
        )?;
        let mut segments = Vec::new();
        for _ in 0..segment_count {
            segments.push(ObjectSegmentsReclaimSegmentRecord {
                segment_index: self.read_u32()?,
                segment_okh: self.read_fixed_16_bytes("object segment reclaim OKH")?,
                segment_vid: self.read_generation_id()?,
                data_pg_id: self.read_u32()?,
                ec: self.read_ec_shape()?,
            });
        }
        Ok(ObjectSegmentsReclaimRecord {
            bucket,
            key,
            generation_id,
            created_at,
            segments,
        })
    }

    fn read_multipart_reclaim_record(
        &mut self,
    ) -> Result<MultipartReclaimRecord, StorageRpcPayloadError> {
        const MIN_PART_LEN: usize = 4;
        const MIN_SEGMENT_LEN: usize = 4 + 4 + 16 + 8 + 4 + 2;

        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_id = self.read_generation_id()?;
        let created_at = self.read_u64()?;
        let part_count = self.read_bounded_remaining_count(
            MIN_PART_LEN,
            "multipart reclaim count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            let part_number = self.read_u32()?;
            let segment_count = self.read_bounded_remaining_count(
                MIN_SEGMENT_LEN,
                "multipart reclaim segment count exceeds payload",
            )?;
            let mut segments = Vec::new();
            for _ in 0..segment_count {
                segments.push(MultipartReclaimPartSegmentRecord {
                    part_number,
                    segment_index: self.read_u32()?,
                    segment_okh: self.read_fixed_16_bytes("multipart reclaim segment OKH")?,
                    segment_vid: self.read_generation_id()?,
                    data_pg_id: self.read_u32()?,
                    ec: self.read_ec_shape()?,
                });
            }
            parts.push(MultipartReclaimPartRecord {
                part_number,
                segments,
            });
        }
        Ok(MultipartReclaimRecord {
            bucket,
            key,
            generation_id,
            created_at,
            parts,
        })
    }

    fn read_bounded_remaining_count(
        &mut self,
        min_item_len: usize,
        message: &'static str,
    ) -> Result<usize, StorageRpcPayloadError> {
        let count = self.read_u32()? as usize;
        let remaining = self.bytes.len().saturating_sub(self.cursor);
        if count > remaining / min_item_len {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                message,
            ));
        }
        Ok(count)
    }

    fn read_limited_bounded_remaining_count(
        &mut self,
        min_item_len: usize,
        message: &'static str,
        limit: u32,
    ) -> Result<usize, StorageRpcPayloadError> {
        let count = self.read_u32()? as usize;
        if count > limit as usize {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: count,
                limit: limit as usize,
            });
        }
        let remaining = self.bytes.len().saturating_sub(self.cursor);
        if count > remaining / min_item_len {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                message,
            ));
        }
        Ok(count)
    }

    fn read_fixed_16_bytes(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; 16], StorageRpcPayloadError> {
        self.read_bytes()?
            .try_into()
            .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest(field))
    }

    fn read_commit_direct_put_object_req(
        &mut self,
    ) -> Result<CommitDirectPutObjectReq, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let generation_reservation_id = self.read_session_id()?;
        let versioning = match self.read_u8()? {
            0 => BucketVersioningState::Disabled,
            1 => BucketVersioningState::Enabled,
            2 => BucketVersioningState::Suspended,
            _ => {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid bucket versioning state",
                ));
            }
        };
        let owner = self.read_owner_identity()?;
        let acl_grants = self.read_acl_grants()?;
        let public_read = self.read_bool()?;
        let generation_id = self.read_generation_id()?;
        let size = self.read_u64()?;
        let etag_crc64 = self.read_u64()?;
        let ec = self.read_ec_shape()?;
        let tags = self.read_optional_serialized_tag_set()?;
        let metadata_blob = SerializedMetadataBlob::new(self.read_bytes()?.to_vec());
        let system_metadata_blob = SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec());
        let object_lock = self.read_object_lock_state()?;
        let encryption = self.read_object_encryption()?;
        let segment_index = self.read_u32()?;
        let segment_crc64 = self.read_u64()?;
        let segment_okh_bytes = self.read_bytes()?;
        let segment_okh: [u8; 16] = segment_okh_bytes.try_into().map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "direct PUT segment object key hash must be 16 bytes",
            )
        })?;
        let segment_vid = self.read_generation_id()?;
        let data_pg_id = self.read_u32()?;
        let bucket_write_reservation = self.read_bucket_write_reservation_proof()?;
        Ok(CommitDirectPutObjectReq {
            bucket,
            key,
            generation_reservation_id,
            versioning,
            owner,
            acl_grants,
            public_read,
            generation_id,
            size,
            etag_crc64,
            ec,
            tags,
            metadata_blob,
            system_metadata_blob,
            object_lock,
            encryption,
            segment_index,
            segment_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            bucket_write_reservation,
        })
    }

    fn read_optional_stored_object(
        &mut self,
    ) -> Result<Option<StoredObject>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_stored_object()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stored object tag",
            )),
        }
    }

    fn read_optional_stored_object_list(
        &mut self,
    ) -> Result<Option<Vec<StoredObject>>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let count = self.read_bounded_remaining_count(1, "stored object list too large")?;
                let mut objects = Vec::new();
                for _ in 0..count {
                    objects.push(self.read_stored_object()?);
                }
                Ok(Some(objects))
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stored object list tag",
            )),
        }
    }

    fn read_stored_object_list(&mut self) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let count = self.read_bounded_remaining_count(1, "stored object list too large")?;
        self.read_stored_object_list_items(count)
    }

    fn read_stored_object_list_with_limit(
        &mut self,
        limit: u32,
    ) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let count =
            self.read_limited_bounded_remaining_count(1, "stored object list too large", limit)?;
        self.read_stored_object_list_items(count)
    }

    fn read_stored_object_list_items(
        &mut self,
        count: usize,
    ) -> Result<Vec<StoredObject>, StorageRpcPayloadError> {
        let mut objects = Vec::new();
        for _ in 0..count {
            objects.push(self.read_stored_object()?);
        }
        Ok(objects)
    }

    fn read_stored_object(&mut self) -> Result<StoredObject, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StoredObject::Live(self.read_live_object_record()?)),
            1 => Ok(StoredObject::DeleteMarker(
                self.read_delete_marker_record()?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stored object tag",
            )),
        }
    }

    fn read_put_object_metadata_mutation(
        &mut self,
    ) -> Result<PutObjectMetadataMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => {
                let tags =
                    SerializedTagSet::from_current_xml(self.read_string()?).map_err(|_| {
                        StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "invalid canonical object tags",
                        )
                    })?;
                Ok(PutObjectMetadataMutation::PutTags(tags))
            }
            1 => Ok(PutObjectMetadataMutation::DeleteTags),
            2 => Ok(PutObjectMetadataMutation::PutRetention(ObjectRetention {
                retain_until_unix_seconds: self.read_u64()?,
                mode: self.read_object_lock_mode()?,
            })),
            3 => Ok(PutObjectMetadataMutation::PutLegalHold(
                StoredLegalHoldStatus::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid stored legal hold status",
                    ),
                )?,
            )),
            4 => Ok(PutObjectMetadataMutation::PutAcl {
                acl_grants: self.read_acl_grants()?,
                public_read: self.read_bool()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object metadata mutation tag",
            )),
        }
    }

    fn read_insert_delete_marker_stale_payload(
        &mut self,
    ) -> Result<StorageRpcInsertDeleteMarkerStalePayload, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcInsertDeleteMarkerStalePayload::Explicit(
                self.read_optional_object_payload_reclaim()?,
            )),
            1 => Ok(
                StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: self.read_u64()?,
                },
            ),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid insert delete marker stale payload tag",
            )),
        }
    }

    fn read_create_stream_upload_req(
        &mut self,
    ) -> Result<CreateStreamUploadReq, StorageRpcPayloadError> {
        Ok(CreateStreamUploadReq {
            session_id: SessionId::try_from(self.read_string()?).map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream session id")
            })?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_prepare_stream_segment_append_req(
        &mut self,
    ) -> Result<PrepareStreamUploadSegmentAppendReq, StorageRpcPayloadError> {
        Ok(PrepareStreamUploadSegmentAppendReq {
            session_id: self.read_session_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("stream segment append OKH")?,
        })
    }

    fn read_create_multipart_upload_req(
        &mut self,
    ) -> Result<CreateMultipartUploadReq, StorageRpcPayloadError> {
        Ok(CreateMultipartUploadReq {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            initiator: self.read_owner_identity()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            object_lock: self.read_object_lock_state()?,
            checksum: self.read_optional_multipart_checksum_config()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_optional_multipart_checksum_config(
        &mut self,
    ) -> Result<Option<MultipartChecksumConfig>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let algorithm = ChecksumAlgorithm::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart checksum algorithm",
                    ),
                )?;
                let checksum_type = ChecksumType::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart checksum type",
                    ),
                )?;
                Ok(Some(
                    MultipartChecksumConfig::new(algorithm, Some(checksum_type)).map_err(|_| {
                        StorageRpcPayloadError::InvalidObjectMetadataRequest(
                            "invalid multipart checksum config",
                        )
                    })?,
                ))
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart checksum tag",
            )),
        }
    }

    fn read_stream_upload_target(&mut self) -> Result<StreamUploadTarget, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StreamUploadTarget::PutObject),
            1 => Ok(StreamUploadTarget::UploadPart {
                upload_id: self.read_upload_id()?,
                part_number: self.read_u32()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload target tag",
            )),
        }
    }

    fn read_create_stream_upload_precondition(
        &mut self,
    ) -> Result<StorageRpcCreateStreamUploadPrecondition, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(
                StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation: self.read_bool()?,
                },
            ),
            1 => Ok(StorageRpcCreateStreamUploadPrecondition::PutObject {
                expected_current: self.read_optional_stored_object()?,
                require_generation_reservation: self.read_bool()?,
            }),
            2 => Ok(StorageRpcCreateStreamUploadPrecondition::UploadPart {
                expected_upload: self.read_multipart_upload_record()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload precondition tag",
            )),
        }
    }

    fn read_optional_create_stream_upload_command(
        &mut self,
    ) -> Result<Option<CreateStreamUploadCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_create_stream_upload_command()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional stream upload command tag",
            )),
        }
    }

    fn read_create_stream_upload_command(
        &mut self,
    ) -> Result<CreateStreamUploadCommand, StorageRpcPayloadError> {
        Ok(CreateStreamUploadCommand {
            session: self.read_stream_upload_command_record()?,
            initial_next_segment_vid: GenerationId::new(self.read_u64()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid initial stream segment generation",
                ),
            )?,
            cleanup_after: self.read_optional_u64()?,
            bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
        })
    }

    fn read_optional_create_multipart_upload_command(
        &mut self,
        authority: &MetadataCommandDecodeAuthority,
    ) -> Result<Option<CreateMultipartUploadCommand>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(
                self.read_create_multipart_upload_command(authority)?,
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart upload command tag",
            )),
        }
    }

    fn read_create_multipart_upload_command(
        &mut self,
        authority: &MetadataCommandDecodeAuthority,
    ) -> Result<CreateMultipartUploadCommand, StorageRpcPayloadError> {
        Ok(CreateMultipartUploadCommand::from_decoded_parts(
            authority,
            self.read_multipart_upload_record()?,
            self.read_bucket_write_reservation_proof()?,
        ))
    }

    fn read_multipart_upload_record(
        &mut self,
    ) -> Result<MultipartUploadRecord, StorageRpcPayloadError> {
        Ok(MultipartUploadRecord {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            initiated_at: self.read_u64()?,
            state: UploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid multipart upload state",
                ),
            )?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            initiator: self.read_owner_identity()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            object_generation_id: GenerationId::new(self.read_u64()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid multipart object generation id",
                ),
            )?,
            initiated_object_identity: self.read_optional_multipart_object_identity()?,
            object_lock: self.read_object_lock_state()?,
            checksum: self.read_optional_multipart_checksum_config()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_optional_multipart_object_identity(
        &mut self,
    ) -> Result<Option<MultipartObjectIdentity>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(MultipartObjectIdentity::Live {
                version_id: VersionId::from_u64(self.read_u64()?),
                generation_id: GenerationId::new(self.read_u64()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid multipart initiation live generation",
                    ),
                )?,
            })),
            2 => Ok(Some(MultipartObjectIdentity::DeleteMarker {
                version_id: VersionId::from_u64(self.read_u64()?),
                write_sequence: self.read_u64()?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart object identity tag",
            )),
        }
    }

    fn read_multipart_completion_snapshot(
        &mut self,
        subject: MultipartCompletionSubject,
    ) -> Result<MultipartCompletionSnapshot, StorageRpcPayloadError> {
        let existing_etag = self.read_optional_string()?;
        let current_object_identity = self.read_optional_multipart_object_identity()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "multipart completion snapshot part count exceeds payload",
        )?;
        let mut part_records = Vec::new();
        for _ in 0..part_count {
            part_records.push(self.read_multipart_part_record()?);
        }
        let selected_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "multipart completion snapshot segment count exceeds payload",
        )?;
        let mut selected_streaming_segments = Vec::new();
        for _ in 0..selected_segment_count {
            selected_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let cleanup = self.read_complete_multipart_commit_cleanup()?;
        Ok(MultipartCompletionSnapshot::from_storage(
            subject,
            existing_etag,
            current_object_identity,
            stale_payload_source,
            part_records,
            selected_streaming_segments,
            cleanup,
        ))
    }

    fn read_list_parts_resp(&mut self) -> Result<ListPartsResp, StorageRpcPayloadError> {
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "multipart parts list count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            parts.push(self.read_multipart_part_record()?);
        }
        let is_truncated = self.read_bool()?;
        let next_part_number_marker = self.read_optional_u32()?;
        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    fn read_listed_multipart_parts(
        &mut self,
    ) -> Result<ListedMultipartParts, StorageRpcPayloadError> {
        let upload = self.read_multipart_upload_record()?;
        let response = self.read_list_parts_resp()?;
        ListedMultipartParts::from_storage(upload, response).map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid multipart parts listing")
        })
    }

    fn read_multipart_upload_management_lookup(
        &mut self,
    ) -> Result<MultipartUploadManagementLookup, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(MultipartUploadManagementLookup::InProgress(Box::new(
                self.read_multipart_upload_record()?,
            ))),
            1 => Ok(MultipartUploadManagementLookup::NonInProgress(Box::new(
                self.read_multipart_upload_record()?,
            ))),
            2 => Ok(MultipartUploadManagementLookup::Replay(Box::new(
                self.read_multipart_completion_replay()?,
            ))),
            3 => Ok(MultipartUploadManagementLookup::Missing),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart management lookup tag",
            )),
        }
    }

    fn read_multipart_completion_replay(
        &mut self,
    ) -> Result<MultipartCompletionReplay, StorageRpcPayloadError> {
        let upload_id = self.read_upload_id()?;
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let fingerprint = MultipartCompletionFingerprint::from_bytes(
            self.read_bytes()?.try_into().map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "multipart completion replay fingerprint must be 32 bytes",
                )
            })?,
        );
        Ok(MultipartCompletionReplay {
            upload_id,
            bucket,
            key,
            fingerprint,
            version_id: VersionId::from_u64(self.read_u64()?),
            etag: self.read_object_etag()?,
            size: self.read_u64()?,
            last_modified: self.read_u64()?,
            tags: self.read_optional_serialized_tag_set()?,
            system_metadata_blob: self.read_optional_serialized_system_metadata_blob()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_stream_upload_command_record(
        &mut self,
    ) -> Result<crate::types::StreamUploadCommandRecord, StorageRpcPayloadError> {
        Ok(crate::types::StreamUploadCommandRecord {
            session_id: SessionId::try_from(self.read_string()?).map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream session id")
            })?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream upload state"),
            )?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_stream_upload_record(&mut self) -> Result<StreamUploadRecord, StorageRpcPayloadError> {
        Ok(StreamUploadRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid stream upload state"),
            )?,
            created_at: self.read_u64()?,
            cleanup_after: self.read_optional_u64()?,
            encryption: self.read_object_encryption()?,
            next_segment_vid: self.read_generation_id()?,
            bucket_write_reservation: self.read_optional_bucket_write_reservation_proof()?,
        })
    }

    fn read_optional_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<Option<BucketWriteReservationProof>, StorageRpcPayloadError> {
        match self.read_bool()? {
            true => Ok(Some(self.read_bucket_write_reservation_proof()?)),
            false => Ok(None),
        }
    }

    fn read_stream_upload_segment_record(
        &mut self,
    ) -> Result<StreamUploadSegmentRecord, StorageRpcPayloadError> {
        Ok(StreamUploadSegmentRecord {
            session_id: self.read_session_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("stream upload segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_terminal_stream_cleanup_record(
        &mut self,
    ) -> Result<TerminalStreamCleanupRecord, StorageRpcPayloadError> {
        Ok(TerminalStreamCleanupRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid stream cleanup state",
                ),
            )?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_stream_put_finalize_storage_snapshot(
        &mut self,
    ) -> Result<StreamPutFinalizeStorageSnapshot, StorageRpcPayloadError> {
        let session = self.read_stream_upload_record()?;
        let existing_etag = self.read_optional_string()?;
        let generation_id = self.read_generation_id()?;
        let stale_payload_source = self.read_optional_stored_object()?;
        let stale_payload = self.read_optional_object_payload_reclaim()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "stream PUT finalize segment count exceeds payload",
        )?;
        let mut staging_segments = Vec::new();
        for _ in 0..segment_count {
            staging_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(StreamPutFinalizeStorageSnapshot {
            session,
            existing_etag,
            generation_id,
            stale_payload_source,
            stale_payload,
            staging_segments,
        })
    }

    fn read_stream_put_commit_input(
        &mut self,
    ) -> Result<StreamPutCommitInput, StorageRpcPayloadError> {
        Ok(StreamPutCommitInput {
            versioning: self.read_bucket_versioning_state()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            etag_crc64: self.read_u64()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_complete_multipart_commit_request(
        &mut self,
    ) -> Result<CompleteMultipartCommitRequest, StorageRpcPayloadError> {
        let bucket = self.read_bucket_name()?;
        let key = self.read_object_key()?;
        let upload_id = self.read_upload_id()?;
        let completion_fingerprint = MultipartCompletionFingerprint::from_bytes(
            self.read_bytes()?.try_into().map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "multipart completion fingerprint must be 32 bytes",
                )
            })?,
        );
        let versioning = self.read_bucket_versioning_state()?;
        let owner = self.read_owner_identity()?;
        let acl_grants = self.read_acl_grants()?;
        let public_read = self.read_bool()?;
        let generation_id = self.read_generation_id()?;
        let size = self.read_u64()?;
        let etag_crc64 = self.read_bytes()?.try_into().map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "complete multipart etag crc64 must be 8 bytes",
            )
        })?;
        let tags = self.read_optional_serialized_tag_set()?;
        let metadata_blob = self.read_optional_serialized_metadata_blob()?;
        let system_metadata_blob = self.read_optional_serialized_system_metadata_blob()?;
        let object_lock = self.read_object_lock_state()?;
        let encryption = self.read_object_encryption()?;
        let expected_stale_payload_source = self.read_optional_stored_object()?;
        let expected_current_object_identity = self.read_optional_multipart_object_identity()?;
        let conditional_completion = self.read_bool()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "complete multipart part count exceeds payload",
        )?;
        let mut part_records = Vec::new();
        for _ in 0..part_count {
            part_records.push(self.read_multipart_part_record()?);
        }
        let selected_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "complete multipart selected segment count exceeds payload",
        )?;
        let mut selected_streaming_segments = Vec::new();
        for _ in 0..selected_segment_count {
            selected_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let expected_cleanup = self.read_complete_multipart_commit_cleanup()?;
        Ok(CompleteMultipartCommitRequest {
            bucket,
            key,
            upload_id,
            completion_fingerprint,
            versioning,
            owner,
            acl_grants,
            public_read,
            generation_id,
            size,
            etag_crc64,
            tags,
            metadata_blob,
            system_metadata_blob,
            object_lock,
            encryption,
            expected_stale_payload_source,
            expected_current_object_identity,
            conditional_completion,
            part_records,
            selected_streaming_segments,
            expected_cleanup,
        })
    }

    fn read_complete_multipart_commit_cleanup(
        &mut self,
    ) -> Result<CompleteMultipartCommitCleanup, StorageRpcPayloadError> {
        let omitted_part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "complete multipart omitted part count exceeds payload",
        )?;
        let mut omitted_parts = Vec::new();
        for _ in 0..omitted_part_count {
            omitted_parts.push(self.read_multipart_part_record()?);
        }
        let omitted_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "complete multipart omitted segment count exceeds payload",
        )?;
        let mut omitted_streaming_segments = Vec::new();
        for _ in 0..omitted_segment_count {
            omitted_streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let stream_upload_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
            "complete multipart stream cleanup count exceeds payload",
        )?;
        let mut stream_uploads = Vec::new();
        for _ in 0..stream_upload_count {
            stream_uploads.push(self.read_terminal_stream_cleanup_record()?);
        }
        let stream_upload_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "complete multipart stream segment cleanup count exceeds payload",
        )?;
        let mut stream_upload_segments = Vec::new();
        for _ in 0..stream_upload_segment_count {
            stream_upload_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(CompleteMultipartCommitCleanup {
            omitted_parts,
            omitted_streaming_segments,
            stream_uploads,
            stream_upload_segments,
        })
    }

    fn read_optional_abort_multipart_upload_cleanup(
        &mut self,
    ) -> Result<Option<AbortMultipartUploadCleanup>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_abort_multipart_upload_cleanup()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional abort multipart cleanup tag",
            )),
        }
    }

    fn read_abort_multipart_upload_cleanup(
        &mut self,
    ) -> Result<AbortMultipartUploadCleanup, StorageRpcPayloadError> {
        let upload = self.read_multipart_upload_record()?;
        let part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
            "abort multipart part cleanup count exceeds payload",
        )?;
        let mut parts = Vec::new();
        for _ in 0..part_count {
            parts.push(self.read_multipart_part_record()?);
        }
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "abort multipart segment cleanup count exceeds payload",
        )?;
        let mut streaming_segments = Vec::new();
        for _ in 0..segment_count {
            streaming_segments.push(self.read_multipart_part_segment_record()?);
        }
        let stream_upload_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
            "abort multipart stream cleanup count exceeds payload",
        )?;
        let mut stream_uploads = Vec::new();
        for _ in 0..stream_upload_count {
            stream_uploads.push(self.read_terminal_stream_cleanup_record()?);
        }
        let stream_upload_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "abort multipart stream segment cleanup count exceeds payload",
        )?;
        let mut stream_upload_segments = Vec::new();
        for _ in 0..stream_upload_segment_count {
            stream_upload_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(AbortMultipartUploadCleanup {
            upload,
            parts,
            streaming_segments,
            stream_uploads,
            stream_upload_segments,
        })
    }

    fn read_stream_upload_part_snapshot(
        &mut self,
    ) -> Result<StreamUploadPartSnapshot, StorageRpcPayloadError> {
        let session = self.read_stream_upload_record()?;
        let upload = self.read_multipart_upload_record()?;
        let existing_part_generation = self.read_optional_u32()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
            "stream part finalize segment count exceeds payload",
        )?;
        let mut staging_segments = Vec::new();
        for _ in 0..segment_count {
            staging_segments.push(self.read_stream_upload_segment_record()?);
        }
        Ok(StreamUploadPartSnapshot {
            session,
            upload,
            existing_part_generation,
            staging_segments,
        })
    }

    fn read_optional_multipart_part_record(
        &mut self,
    ) -> Result<Option<MultipartPartRecord>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_multipart_part_record()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional multipart part tag",
            )),
        }
    }

    fn read_multipart_part_record(
        &mut self,
    ) -> Result<MultipartPartRecord, StorageRpcPayloadError> {
        Ok(MultipartPartRecord {
            upload_id: self.read_upload_id()?,
            part_number: self.read_u32()?,
            generation: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid etag kind"),
            )?,
            part_vid: self.read_generation_id()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            last_modified: self.read_u64()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn read_stream_part_finalize_storage_snapshot(
        &mut self,
    ) -> Result<StreamUploadPartStorageSnapshot, StorageRpcPayloadError> {
        let auth_snapshot = self.read_stream_upload_part_snapshot()?;
        let existing_part = self.read_optional_multipart_part_record()?;
        let segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "stream part finalize displaced segment count exceeds payload",
        )?;
        let mut displaced_segments = Vec::new();
        for _ in 0..segment_count {
            displaced_segments.push(self.read_multipart_part_segment_record()?);
        }
        Ok(StreamUploadPartStorageSnapshot {
            auth_snapshot,
            existing_part,
            displaced_segments,
        })
    }

    fn read_optional_delete_object_version_target(
        &mut self,
    ) -> Result<Option<DeleteObjectVersionTarget>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_delete_object_version_target()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional delete object version target tag",
            )),
        }
    }

    fn read_delete_object_version_target(
        &mut self,
    ) -> Result<DeleteObjectVersionTarget, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(DeleteObjectVersionTarget::DeleteMarker {
                write_sequence: self.read_u64()?,
            }),
            1 => Ok(DeleteObjectVersionTarget::Live {
                generation_id: GenerationId::new(self.read_u64()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "invalid delete target generation id",
                    ),
                )?,
                layout: self.read_object_layout()?,
                payload: self.read_object_payload_reclaim()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid delete object version target tag",
            )),
        }
    }

    fn read_metadata_command_envelope_response_item(
        &mut self,
        authority: &MetadataCommandDecodeAuthority,
    ) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
        let item = self.read_metadata_command_item()?;
        metadata_command_envelope_from_item(&item, authority)
    }

    fn read_live_object_record(&mut self) -> Result<LiveObjectRecord, StorageRpcPayloadError> {
        Ok(LiveObjectRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            generation_id: self.read_generation_id()?,
            size: self.read_u64()?,
            etag: self.read_object_etag()?,
            last_modified: self.read_u64()?,
            became_noncurrent_at: self.read_optional_u64()?,
            storage_class: self.read_storage_class()?,
            ec: self.read_ec_shape()?,
            layout: self.read_object_layout()?,
            tags: self.read_optional_serialized_tag_set()?,
            metadata_blob: self.read_optional_serialized_metadata_blob()?,
            system_metadata_blob: self.read_optional_serialized_system_metadata_blob()?,
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_upload_id(&mut self) -> Result<UploadId, StorageRpcPayloadError> {
        UploadId::try_from(self.read_string_with_limit(
            UPLOAD_ID_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("upload id is too large"),
        )?)
        .map_err(|_| StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid upload id"))
    }

    fn read_optional_upload_id(&mut self) -> Result<Option<UploadId>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_upload_id()?)),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional upload id tag",
            )),
        }
    }

    fn read_object_read_auth_subject(
        &mut self,
    ) -> Result<ObjectReadAuthSubject, StorageRpcPayloadError> {
        let stored = self.read_stored_object()?;
        Ok(ObjectReadAuthSubject {
            identity: ObjectReadAuthSubjectIdentity::for_stored(&stored),
            stored,
        })
    }

    fn read_object_read_snapshot(&mut self) -> Result<ObjectReadSnapshot, StorageRpcPayloadError> {
        let stored = self.read_stored_object()?;
        let object_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN,
            "object read segment count exceeds payload",
        )?;
        let mut object_segments = Vec::new();
        for _ in 0..object_segment_count {
            object_segments.push(self.read_object_segment_record()?);
        }
        let multipart_part_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_OBJECT_PART_RECORD_LEN,
            "object read part count exceeds payload",
        )?;
        let mut multipart_parts = Vec::new();
        for _ in 0..multipart_part_count {
            multipart_parts.push(self.read_object_part_record()?);
        }
        let multipart_part_segment_count = self.read_bounded_remaining_count(
            STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
            "object read multipart segment count exceeds payload",
        )?;
        let mut multipart_part_segments = Vec::new();
        for _ in 0..multipart_part_segment_count {
            multipart_part_segments.push(self.read_multipart_part_segment_record()?);
        }
        ObjectReadSnapshot::from_records(
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        )
        .map_err(StorageRpcPayloadError::InvalidObjectMetadataRequest)
    }

    fn read_object_segment_record(
        &mut self,
    ) -> Result<ObjectSegmentRecord, StorageRpcPayloadError> {
        Ok(ObjectSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("object segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_object_part_record(&mut self) -> Result<ObjectPartRecord, StorageRpcPayloadError> {
        Ok(ObjectPartRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            part_number: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid etag kind"),
            )?,
            part_vid: self.read_generation_id()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            data_pg_id: self.read_u32()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn read_multipart_part_segment_record(
        &mut self,
    ) -> Result<MultipartPartSegmentRecord, StorageRpcPayloadError> {
        Ok(MultipartPartSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            upload_id: self.read_upload_id()?,
            version_id: self.read_u64()?,
            part_number: self.read_u32()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_16_bytes("multipart part segment OKH")?,
            segment_vid: self.read_generation_id()?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn read_optional_checksum_bytes(
        &mut self,
    ) -> Result<Option<ChecksumBytes>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Some(
                ChecksumBytes::new(self.read_bytes()?).map_err(invalid_checksum_metadata_error),
            )
            .transpose(),
            _ => Err(StorageRpcPayloadError::InvalidChecksumMetadata(
                "invalid optional checksum tag",
            )),
        }
    }

    fn read_object_read_snapshot_mode(
        &mut self,
    ) -> Result<ObjectReadSnapshotMode, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectReadSnapshotMode::MetadataOnly),
            1 => Ok(ObjectReadSnapshotMode::StandardSegments),
            2 => Ok(ObjectReadSnapshotMode::MultipartParts),
            3 => Ok(ObjectReadSnapshotMode::FullPayloadLayout),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object read snapshot mode",
            )),
        }
    }

    fn read_delete_marker_record(&mut self) -> Result<DeleteMarkerRecord, StorageRpcPayloadError> {
        Ok(DeleteMarkerRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: VersionId::from_u64(self.read_u64()?),
            owner: self.read_owner_identity()?,
            last_modified: self.read_u64()?,
        })
    }

    fn read_canonical_user_id(&mut self) -> Result<CanonicalUserId, StorageRpcPayloadError> {
        let value = self.read_string_with_limit(
            s3_types::CANONICAL_USER_ID_LEN,
            StorageRpcPayloadError::InvalidBucketMetadataRequest("canonical user id is too large"),
        )?;
        CanonicalUserId::parse_stored(&value).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid canonical user id"),
        )
    }

    fn read_acl_grants(&mut self) -> Result<AclGrants, StorageRpcPayloadError> {
        let value = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_ACL_GRANTS_LEN,
            StorageRpcPayloadError::InvalidBucketMetadataRequest("ACL grants are too large"),
        )?;
        AclGrants::parse_current_storage(&value)
            .map_err(|_| StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid ACL grants"))
    }

    fn read_owner_identity(&mut self) -> Result<OwnerIdentity, StorageRpcPayloadError> {
        let principal = self.read_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_OWNER_PRINCIPAL_LEN,
            StorageRpcPayloadError::InvalidObjectMetadataRequest("owner principal is too large"),
        )?;
        let canonical_id = self.read_canonical_user_id()?;
        Ok(OwnerIdentity {
            principal,
            canonical_id,
        })
    }

    fn read_bool(&mut self) -> Result<bool, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bool tag",
            )),
        }
    }

    fn read_bucket_state(&mut self) -> Result<BucketState, StorageRpcPayloadError> {
        BucketState::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket state"),
        )
    }

    fn read_bucket_versioning_state(
        &mut self,
    ) -> Result<BucketVersioningState, StorageRpcPayloadError> {
        BucketVersioningState::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket versioning state"),
        )
    }

    fn read_bucket_metadata_control_mutation(
        &mut self,
    ) -> Result<StorageRpcBucketMetadataControlMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(StorageRpcBucketMetadataControlMutation::Versioning(
                self.read_bucket_versioning_state()?,
            )),
            1 => Ok(StorageRpcBucketMetadataControlMutation::Acl {
                acl_grants: self.read_acl_grants()?,
                summary: BucketAclSummary {
                    public_read: self.read_bool()?,
                    public_write: self.read_bool()?,
                },
            }),
            2 => Ok(StorageRpcBucketMetadataControlMutation::Property(
                self.read_bucket_property_mutation()?,
            )),
            3 => Ok(StorageRpcBucketMetadataControlMutation::Subresource(
                self.read_bucket_subresource_mutation()?,
            )),
            4 => Ok(StorageRpcBucketMetadataControlMutation::MarkDeleting),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket metadata control mutation tag",
            )),
        }
    }

    fn read_bucket_property_mutation(
        &mut self,
    ) -> Result<BucketPropertyMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(BucketPropertyMutation::ObjectLock(
                self.read_bucket_object_lock_config()?,
            )),
            1 => Ok(BucketPropertyMutation::Encryption(
                self.read_bucket_encryption_config()?,
            )),
            2 => Ok(BucketPropertyMutation::PublicAccessBlock(
                self.read_optional_public_access_block_config()?,
            )),
            3 => Ok(BucketPropertyMutation::OwnershipControls(
                self.read_optional_bucket_ownership_controls()?,
            )),
            4 => Ok(BucketPropertyMutation::AbacEnabled(self.read_bool()?)),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket property mutation tag",
            )),
        }
    }

    fn read_bucket_encryption_config(
        &mut self,
    ) -> Result<BucketEncryptionConfig, StorageRpcPayloadError> {
        let default_encryption = match self.read_u8()? {
            0 => None,
            1 => Some(ManagedEncryptionAlgorithm::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid managed encryption algorithm",
                ),
            )?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid bucket encryption tag",
                ))
            }
        };
        Ok(BucketEncryptionConfig {
            default_encryption,
            sse_c_blocked: self.read_bool()?,
        })
    }

    fn read_bucket_subresource_mutation(
        &mut self,
    ) -> Result<BucketSubresourceMutation, StorageRpcPayloadError> {
        match self.read_u8()? {
            1 => {
                let kind = self.read_bucket_subresource_kind()?;
                let body = self.read_string_with_limit(
                    STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
                    StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "bucket subresource body is too large",
                    ),
                )?;
                let aux = self.read_bucket_subresource_aux(kind)?;
                match (kind, aux) {
                    (BucketSubresourceKind::Cors, BucketSubresourceAux::None) => {
                        Ok(BucketSubresourceMutation::PutCors(body))
                    }
                    (BucketSubresourceKind::Tagging, BucketSubresourceAux::None) => {
                        SerializedBucketTagSet::from_current_xml(body)
                            .map(BucketSubresourceMutation::PutTagging)
                            .map_err(|_| {
                                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                                    "invalid bucket tags",
                                )
                            })
                    }
                    (BucketSubresourceKind::Policy, BucketSubresourceAux::Policy { is_public }) => {
                        Ok(BucketSubresourceMutation::PutPolicy { body, is_public })
                    }
                    (BucketSubresourceKind::Lifecycle, BucketSubresourceAux::None) => {
                        Ok(BucketSubresourceMutation::PutLifecycle(body))
                    }
                    _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "bucket subresource kind and auxiliary data disagree",
                    )),
                }
            }
            2 => Ok(BucketSubresourceMutation::Delete {
                kind: self.read_bucket_subresource_kind()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket subresource mutation tag",
            )),
        }
    }

    fn read_bucket_subresource_kind(
        &mut self,
    ) -> Result<BucketSubresourceKind, StorageRpcPayloadError> {
        BucketSubresourceKind::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid bucket subresource kind"),
        )
    }

    fn read_bucket_subresource_aux(
        &mut self,
        kind: BucketSubresourceKind,
    ) -> Result<BucketSubresourceAux, StorageRpcPayloadError> {
        let aux = match self.read_u8()? {
            0 => BucketSubresourceAux::None,
            1 if kind == BucketSubresourceKind::Policy => {
                BucketSubresourceAux::policy(self.read_bool()?)
            }
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid bucket subresource aux",
                ))
            }
        };
        if !kind.supports_aux(aux) {
            return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "bucket subresource kind does not support aux",
            ));
        }
        Ok(aux)
    }

    fn read_storage_class(&mut self) -> Result<StorageClass, StorageRpcPayloadError> {
        StorageClass::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid storage class"),
        )
    }

    fn read_ec_shape(&mut self) -> Result<EcShape, StorageRpcPayloadError> {
        Ok(EcShape {
            k: self.read_u8()?,
            m: self.read_u8()?,
        })
    }

    fn read_16_bytes(&mut self) -> Result<[u8; 16], StorageRpcPayloadError> {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(self.read_exact(16)?);
        Ok(bytes)
    }

    fn read_scavenger_observation_reason(
        &mut self,
    ) -> Result<ShardScavengerObservationReason, StorageRpcPayloadError> {
        ShardScavengerObservationReason::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid shard scavenger observation reason",
            ),
        )
    }

    fn read_scavenger_observation_key(
        &mut self,
    ) -> Result<ShardScavengerObservationKey, StorageRpcPayloadError> {
        let node_id = self.read_u32()?;
        let data_pg_id = self.read_u32()?;
        let shard_index = ShardIndex::new(self.read_u8()?);
        let shard_key = self.read_shard_key()?;
        if shard_key.shard_index() != shard_index {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "shard scavenger observation key shard index mismatch",
            ));
        }
        Ok(ShardScavengerObservationKey {
            node_id,
            data_pg_id,
            shard_index,
            shard_key,
        })
    }

    fn read_scavenger_observation_record(
        &mut self,
    ) -> Result<ShardScavengerObservationRecord, StorageRpcPayloadError> {
        Ok(ShardScavengerObservationRecord {
            key: self.read_scavenger_observation_key()?,
            data_size: self.read_optional_u64()?,
            crc64: self.read_optional_u64()?,
            file_exists: self.read_bool()?,
            shard_row_exists: self.read_bool()?,
            reason: self.read_scavenger_observation_reason()?,
            last_error: self.read_optional_string_with_limit(
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN + 1,
                    limit: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                },
            )?,
        })
    }

    fn read_scavenger_observation(
        &mut self,
    ) -> Result<ShardScavengerObservation, StorageRpcPayloadError> {
        Ok(ShardScavengerObservation {
            key: self.read_scavenger_observation_key()?,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            data_size: self.read_optional_u64()?,
            crc64: self.read_optional_u64()?,
            file_exists: self.read_bool()?,
            shard_row_exists: self.read_bool()?,
            reason: self.read_scavenger_observation_reason()?,
            last_error: self.read_optional_string_with_limit(
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN + 1,
                    limit: STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_ERROR_LEN,
                },
            )?,
            resolved_at: self.read_optional_u64()?,
        })
    }

    fn read_segment_stored_bytes_request(
        &mut self,
    ) -> Result<SegmentStoredBytesRequest, StorageRpcPayloadError> {
        Ok(SegmentStoredBytesRequest {
            data_pg_id: self.read_u32()?,
            segment_okh: self.read_16_bytes()?,
            segment_vid: self.read_generation_id()?,
            stored_size: self.read_u64()? as usize,
            segment_crc64: self.read_u64()?,
            ec: self.read_ec_shape()?,
        })
    }

    fn read_placed_segment_shard_repair_work_item(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairWorkItem, StorageRpcPayloadError> {
        let request = self.read_segment_stored_bytes_request()?;
        let shard_index = ShardIndex::new(self.read_u8()?);
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        validate_placed_segment_shard_repair_work_item(&work_item)?;
        Ok(work_item)
    }

    fn read_placed_segment_shard_repair_record(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairRecord, StorageRpcPayloadError> {
        Ok(PlacedSegmentShardRepairRecord {
            work_item: self.read_placed_segment_shard_repair_work_item()?,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_repair_claim_record(
        &mut self,
    ) -> Result<PlacedSegmentShardRepairClaimRecord, StorageRpcPayloadError> {
        Ok(PlacedSegmentShardRepairClaimRecord {
            work_item: self.read_placed_segment_shard_repair_work_item()?,
            claim_id: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
            )?,
            owner_token: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken(
                    "owner token exceeds maximum length",
                ),
            )?,
            cluster_epoch: self.read_cluster_epoch()?,
            claimed_at: self.read_u64()?,
            lease_deadline: self.read_optional_u64()?,
            attempt_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_backfill_work_item(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillWorkItem, StorageRpcPayloadError> {
        let request = self.read_segment_stored_bytes_request()?;
        let source_cluster_epoch = self.read_cluster_epoch()?;
        let desired_cluster_epoch = self.read_cluster_epoch()?;
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request,
            source_cluster_epoch,
            desired_cluster_epoch,
        };
        validate_placed_segment_shard_backfill_work_item(&work_item)?;
        Ok(work_item)
    }

    fn read_placed_segment_shard_backfill_record(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillRecord, StorageRpcPayloadError> {
        let work_item = self.read_placed_segment_shard_backfill_work_item()?;
        let remaining_tolerance = self.read_u8()?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &work_item,
            remaining_tolerance,
        )?;
        Ok(PlacedSegmentShardBackfillRecord {
            work_item,
            remaining_tolerance,
            first_seen_at: self.read_u64()?,
            last_seen_at: self.read_u64()?,
            observation_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_placed_segment_shard_backfill_claim_record(
        &mut self,
    ) -> Result<PlacedSegmentShardBackfillClaimRecord, StorageRpcPayloadError> {
        let work_item = self.read_placed_segment_shard_backfill_work_item()?;
        let remaining_tolerance = self.read_u8()?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &work_item,
            remaining_tolerance,
        )?;
        Ok(PlacedSegmentShardBackfillClaimRecord {
            work_item,
            remaining_tolerance,
            claim_id: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
            )?,
            owner_token: self.read_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
                StorageRpcPayloadError::InvalidDurableClaimToken(
                    "owner token exceeds maximum length",
                ),
            )?,
            cluster_epoch: self.read_cluster_epoch()?,
            claimed_at: self.read_u64()?,
            lease_deadline: self.read_optional_u64()?,
            attempt_count: self.read_u64()?,
            last_error: self.read_optional_string_with_limit(
                PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                StorageRpcPayloadError::PayloadTooLarge {
                    len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                },
            )?,
        })
    }

    fn read_scavenger_payload_reference(
        &mut self,
    ) -> Result<ShardScavengerPayloadReference, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ShardScavengerPayloadReference::Placed(
                ShardScavengerPlacedShardSetReference {
                    data_pg_id: self.read_u32()?,
                    okh: self.read_16_bytes()?,
                    generation_id: self.read_generation_id()?,
                    placement_cluster_epoch: self.read_cluster_epoch()?,
                    stored_size: self.read_u64()?,
                    crc64: self.read_u64()?,
                    ec: self.read_ec_shape()?,
                },
            )),
            1 => Ok(ShardScavengerPayloadReference::ReclaimOnly(
                ShardScavengerReclaimShardSetReference {
                    data_pg_id: self.read_u32()?,
                    okh: self.read_16_bytes()?,
                    generation_id: self.read_generation_id()?,
                    ec: self.read_ec_shape()?,
                },
            )),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid shard scavenger payload reference tag",
            )),
        }
    }

    fn read_placed_scavenger_reference(
        &mut self,
    ) -> Result<ShardScavengerPlacedShardSetReference, StorageRpcPayloadError> {
        Ok(ShardScavengerPlacedShardSetReference {
            data_pg_id: self.read_u32()?,
            okh: self.read_16_bytes()?,
            generation_id: self.read_generation_id()?,
            placement_cluster_epoch: self.read_cluster_epoch()?,
            stored_size: self.read_u64()?,
            crc64: self.read_u64()?,
            ec: self.read_ec_shape()?,
        })
    }

    fn read_placed_segment_backfill_reference_cursor(
        &mut self,
    ) -> Result<PlacedSegmentBackfillReferenceCursor, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(PlacedSegmentBackfillReferenceCursor::ObjectSegment {
                bucket: self.read_bucket_name()?,
                key: self.read_object_key()?,
                version_id: self.read_u64()?,
                segment_index: self.read_u32()?,
            }),
            1 => Ok(PlacedSegmentBackfillReferenceCursor::StreamUploadSegment {
                session_id: self.read_session_id()?,
                segment_index: self.read_u32()?,
            }),
            2 => Ok(PlacedSegmentBackfillReferenceCursor::MultipartPartSegment {
                bucket: self.read_bucket_name()?,
                key: self.read_object_key()?,
                upload_id: self.read_upload_id()?,
                part_number: self.read_u32()?,
                segment_index: self.read_u32()?,
            }),
            3 => Ok(PlacedSegmentBackfillReferenceCursor::PendingCommand {
                cluster_epoch: self.read_cluster_epoch()?,
                pg_id: PgId::new(self.read_u32()?),
                log_index: self.read_u64()?,
                command_checksum: self.read_u64()?,
                reference_index: self.read_u32()?,
            }),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid backfill reference cursor tag",
            )),
        }
    }

    fn read_object_etag(&mut self) -> Result<ObjectEtag, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => {
                let mut crc64 = [0u8; 8];
                crc64.copy_from_slice(self.read_exact(8)?);
                Ok(ObjectEtag::SinglePart(crc64))
            }
            1 => {
                let mut crc64 = [0u8; 8];
                crc64.copy_from_slice(self.read_exact(8)?);
                let parts = NonZeroU32::new(self.read_u32()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "multipart etag parts must not be zero",
                    ),
                )?;
                Ok(ObjectEtag::MultipartComposite { crc64, parts })
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object etag tag",
            )),
        }
    }

    fn read_object_layout(&mut self) -> Result<ObjectLayout, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(ObjectLayout::Standard),
            1 => {
                let parts_count = NonZeroU32::new(self.read_u32()?).ok_or(
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "multipart layout parts must not be zero",
                    ),
                )?;
                Ok(ObjectLayout::MultipartManifest { parts_count })
            }
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid object layout tag",
            )),
        }
    }

    fn read_bucket_object_lock_config(
        &mut self,
    ) -> Result<BucketObjectLockConfig, StorageRpcPayloadError> {
        let enabled = self.read_bool()?;
        let default_retention = match self.read_u8()? {
            0 => None,
            1 => Some(ObjectLockDefaultRetention {
                mode: self.read_object_lock_mode()?,
                period: self.read_retention_period()?,
            }),
            _ => {
                return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid object-lock default-retention tag",
                ));
            }
        };
        Ok(BucketObjectLockConfig {
            enabled,
            default_retention,
        })
    }

    fn read_object_lock_mode(&mut self) -> Result<ObjectLockMode, StorageRpcPayloadError> {
        ObjectLockMode::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid object-lock mode"),
        )
    }

    fn read_retention_period(&mut self) -> Result<RetentionPeriod, StorageRpcPayloadError> {
        let value = self.read_u32()?;
        let value =
            NonZeroU32::new(value).ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "retention period must not be zero",
            ))?;
        match self.read_u8()? {
            0 => Ok(RetentionPeriod::Days(value)),
            1 => Ok(RetentionPeriod::Years(value)),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid retention period tag",
            )),
        }
    }

    fn read_object_lock_state(&mut self) -> Result<ObjectLockState, StorageRpcPayloadError> {
        let retention = match self.read_u8()? {
            0 => None,
            1 => Some(ObjectRetention {
                retain_until_unix_seconds: self.read_u64()?,
                mode: self.read_object_lock_mode()?,
            }),
            _ => {
                return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid object-lock retention tag",
                ))
            }
        };
        let legal_hold = StoredLegalHoldStatus::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stored legal hold status",
            ),
        )?;
        Ok(ObjectLockState {
            retention,
            legal_hold,
        })
    }

    fn read_object_encryption(&mut self) -> Result<ObjectEncryption, StorageRpcPayloadError> {
        let encryption_type = ObjectEncryptionType::from_u8(self.read_u8()?).ok_or(
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid object encryption type"),
        )?;
        let state = self.read_optional_bytes_value()?;
        ObjectEncryption::decode(encryption_type, state).map_err(|_| {
            StorageRpcPayloadError::InvalidObjectMetadataRequest("invalid object encryption state")
        })
    }

    fn read_optional_serialized_tag_set(
        &mut self,
    ) -> Result<Option<SerializedTagSet>, StorageRpcPayloadError> {
        self.read_optional_string()?
            .map(SerializedTagSet::from_current_xml)
            .transpose()
            .map_err(|_| {
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "invalid canonical object tags",
                )
            })
    }

    fn read_optional_serialized_metadata_blob(
        &mut self,
    ) -> Result<Option<SerializedMetadataBlob>, StorageRpcPayloadError> {
        Ok(self
            .read_optional_bytes_value()?
            .map(SerializedMetadataBlob::new))
    }

    fn read_optional_serialized_system_metadata_blob(
        &mut self,
    ) -> Result<Option<SerializedSystemMetadataBlob>, StorageRpcPayloadError> {
        Ok(self
            .read_optional_bytes_value()?
            .map(SerializedSystemMetadataBlob::new))
    }

    fn read_optional_bytes_value(&mut self) -> Result<Option<Vec<u8>>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_bytes()?.to_vec())),
            _ => Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional bytes tag",
            )),
        }
    }

    fn read_optional_public_access_block_config(
        &mut self,
    ) -> Result<Option<PublicAccessBlockConfig>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(PublicAccessBlockConfig {
                block_public_acls: self.read_bool()?,
                ignore_public_acls: self.read_bool()?,
                block_public_policy: self.read_bool()?,
                restrict_public_buckets: self.read_bool()?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid public-access-block tag",
            )),
        }
    }

    fn read_optional_bucket_ownership_controls(
        &mut self,
    ) -> Result<Option<BucketOwnershipControls>, StorageRpcPayloadError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::from_u8(self.read_u8()?).ok_or(
                    StorageRpcPayloadError::InvalidBucketMetadataRequest(
                        "invalid object ownership",
                    ),
                )?,
            })),
            _ => Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid ownership-controls tag",
            )),
        }
    }

    fn read_bucket_ownership_controls(
        &mut self,
    ) -> Result<BucketOwnershipControls, StorageRpcPayloadError> {
        Ok(BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest("invalid object ownership"),
            )?,
        })
    }

    fn read_effective_bucket_encryption_config(
        &mut self,
    ) -> Result<EffectiveBucketEncryptionConfig, StorageRpcPayloadError> {
        Ok(EffectiveBucketEncryptionConfig {
            default_encryption: ManagedEncryptionAlgorithm::from_u8(self.read_u8()?).ok_or(
                StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid managed encryption algorithm",
                ),
            )?,
            sse_c_blocked: self.read_bool()?,
        })
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    fn read_u8(&mut self) -> Result<u8, StorageRpcPayloadError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, StorageRpcPayloadError> {
        let mut bytes = [0u8; 2];
        bytes.copy_from_slice(self.read_exact(2)?);
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, StorageRpcPayloadError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, StorageRpcPayloadError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(bytes))
    }
}
