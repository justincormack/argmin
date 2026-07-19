use super::*;

impl ObjectListingMetadataNodeClient for UnixStorageNodeClient {
    fn list_objects_page(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListObjectsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                start_after: req.start_after.clone(),
                start_at: req.start_at.clone(),
                max_keys: req.max_keys,
            },
        };
        let payload = encode_list_objects_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode object list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectListPage,
                payload,
                listing_probe_admission_class(req.max_keys),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_objects_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object list response", error.to_string()),
            )
        })?;
        validate_list_objects_response(self, &response.response, req)?;
        Ok(response.response)
    }

    fn list_object_versions_page(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectVersionsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListObjectVersionsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                key_marker: req.key_marker.clone(),
                version_id_marker: req.version_id_marker,
                start_at: req.start_at.clone(),
                max_keys: req.max_keys,
            },
        };
        let payload = encode_list_object_versions_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode object version list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectVersionListPage,
                payload,
                listing_probe_admission_class(req.max_keys),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_object_versions_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object version list response", error.to_string()),
            )
        })?;
        validate_list_object_versions_response(self, &response.response, req)?;
        Ok(response.response)
    }

    fn list_multipart_uploads_page(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListMultipartUploadsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            request: ListMultipartUploadsReq {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                page_start: req.page_start.clone(),
                max_uploads: req.max_uploads,
            },
        };
        let payload = encode_list_multipart_uploads_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode multipart upload list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectMultipartUploadListPage,
                payload,
                listing_probe_admission_class(req.max_uploads),
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_list_multipart_uploads_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode multipart upload list response", error.to_string()),
            )
        })?;
        validate_list_multipart_uploads_response(self, &response.response, req)?;
        Ok(response.response)
    }
}

pub(super) fn validate_list_objects_response(
    client: &UnixStorageNodeClient,
    response: &ListObjectsResp,
    req: &ListObjectsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.objects.len() > req.max_keys as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate object list response",
            "object list response exceeds requested max keys".to_string(),
        )));
    }
    for object in &response.objects {
        if object.bucket() != &req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "object list response bucket does not match request".to_string(),
            )));
        }
    }
    match (response.is_truncated, response.next_start_after.as_ref()) {
        (false, None) => {}
        (false, Some(_)) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "non-truncated object list response has next marker".to_string(),
            )));
        }
        (true, Some(marker))
            if response
                .objects
                .last()
                .is_some_and(|object| marker == object.key()) => {}
        (true, _) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object list response",
                "truncated object list response marker does not match last object".to_string(),
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_list_object_versions_response(
    client: &UnixStorageNodeClient,
    response: &ListObjectVersionsResp,
    req: &ListObjectVersionsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.versions.len() > req.max_keys as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate object version list response",
            "object version list response exceeds requested max keys".to_string(),
        )));
    }
    for object in &response.versions {
        if object.bucket() != &req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object version list response",
                "object version list response bucket does not match request".to_string(),
            )));
        }
    }
    match (
        response.is_truncated,
        response.next_key_marker.as_ref(),
        response.next_version_id_marker,
    ) {
        (false, None, None) => {}
        (false, _, _) => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate object version list response",
                "non-truncated object version list response has next marker".to_string(),
            )));
        }
        (true, Some(key_marker), Some(version_marker))
            if response.versions.last().is_some_and(|object| {
                key_marker == object.key() && version_marker == object.version_id()
            }) => {}
        (true, _, _) => {
            return Err(BucketSnapshotLoadError::Store(
                client.rpc_payload_error(
                    "validate object version list response",
                    "truncated object version list response marker does not match last version"
                        .to_string(),
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_list_multipart_uploads_response(
    client: &UnixStorageNodeClient,
    response: &ListMultipartUploadsResp,
    req: &ListMultipartUploadsReq,
) -> Result<(), BucketSnapshotLoadError> {
    if response.uploads.len() > req.max_uploads as usize {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate multipart upload list response",
            "multipart upload list response exceeds requested max uploads".to_string(),
        )));
    }
    for upload in &response.uploads {
        if upload.bucket != req.bucket {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate multipart upload list response",
                "multipart upload list response bucket does not match request".to_string(),
            )));
        }
    }
    match (
        response.is_truncated,
        response.next_key_marker.as_ref(),
        response.next_upload_id_marker.as_ref(),
        response.uploads.last(),
    ) {
        (_, Some(key_marker), Some(upload_id_marker), Some(upload))
            if key_marker == &upload.key && upload_id_marker == &upload.upload_id => {}
        (false, None, None, None) => {}
        _ => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate multipart upload list response",
                "multipart upload list response markers do not match the final upload".to_string(),
            )));
        }
    }
    Ok(())
}

fn unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
    client: &UnixStorageNodeClient,
    pg_id: BucketPgId,
    acquire: DurableBucketWriteReservationAcquire<'_>,
    class: UnixStorageNodeRpcAdmissionClass,
) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
    let request = StorageRpcBucketWriteReservationAcquireRequest {
        node_id: client.node_id,
        cluster_epoch: acquire.cluster_epoch,
        pg_id: pg_id.pg_id(),
        bucket: acquire.name.clone(),
        reservation_id: acquire.reservation_id.to_string(),
        owner_token: acquire.owner_token.to_string(),
        operation_kind: acquire.operation_kind.to_string(),
        created_at: acquire.created_at,
        lease_deadline: acquire.lease_deadline,
        target_context: acquire.target_context.map(str::to_string),
    };
    let payload = encode_bucket_write_reservation_acquire_request(&request).map_err(|error| {
        BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "encode bucket write reservation acquire request",
            error.to_string(),
        ))
    })?;
    let response = client
        .rpc_request_with_admission_class(
            StorageRpcMessageKind::BucketWriteReservationAcquire,
            payload,
            class,
        )
        .map_err(BucketSnapshotLoadError::Store)?;
    let response = decode_bucket_write_reservation_record_response(&response).map_err(|error| {
        BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "decode bucket write reservation acquire response",
            error.to_string(),
        ))
    })?;
    let record = match response.outcome {
        StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => record,
        StorageRpcBucketWriteReservationAcquireOutcome::Draining => {
            return Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDraining,
            ));
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { name }
            if name == *acquire.name =>
        {
            return Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound { name },
            ));
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { .. } => {
            return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
                "validate bucket write reservation acquire response",
                "bucket not found response identity does not match request".to_string(),
            )));
        }
    };
    if record.bucket != *acquire.name
        || record.reservation_id != acquire.reservation_id
        || record.owner_token != acquire.owner_token
        || record.cluster_epoch != acquire.cluster_epoch
        || record.operation_kind != acquire.operation_kind
        || record.created_at != acquire.created_at
        || record.lease_deadline != acquire.lease_deadline
        || record.target_context.as_deref() != acquire.target_context
    {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            "validate bucket write reservation acquire response",
            "reservation response identity does not match request".to_string(),
        )));
    }
    Ok(record)
}

impl BucketWriteReservationNodeClient for UnixStorageNodeClient {
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainExists, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_metadata_command_bool_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write drain exists response",
                error.to_string(),
            ))
        })?;
        Ok(response.value)
    }

    fn durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainGet, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_write_drain_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(
                    self.rpc_payload_error(
                        "decode bucket write drain get response",
                        error.to_string(),
                    ),
                )
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket write drain get response",
                    "drain response bucket does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn record_bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            record: record.clone(),
        };
        let payload =
            encode_bucket_delete_attempt_outcome_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket delete attempt outcome record request",
                    error.to_string(),
                ))
            })?;
        self.rpc_request(
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
            payload,
        )
        .map(|_| ())
        .map_err(BucketSnapshotLoadError::Store)
    }

    fn bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_delete_attempt_outcome_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete attempt outcome get response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket delete attempt outcome get response",
                    "outcome response bucket does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
            self,
            pg_id,
            acquire,
            storage_rpc_admission_class(StorageRpcMessageKind::BucketWriteReservationAcquire),
        )
    }

    fn acquire_completion_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
            self,
            pg_id,
            acquire,
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }
    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationProofRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            proof: proof.clone(),
        };
        let payload = encode_bucket_write_reservation_proof_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket write reservation proof request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::BucketWriteReservationValidate,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write reservation validate response",
            &response,
        )
    }

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationHeartbeatRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            proof: proof.clone(),
            lease_deadline,
        };
        let payload =
            encode_bucket_write_reservation_heartbeat_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write reservation heartbeat request",
                    error.to_string(),
                ))
            })?;
        let kind = StorageRpcMessageKind::BucketWriteReservationHeartbeat;
        let response = match self
            .rpc_request_result(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?
        {
            Ok(response) => response,
            Err(error) => return Err(self.bucket_snapshot_rpc_response_error(kind, error)),
        };
        let response =
            decode_bucket_write_reservation_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write reservation heartbeat response",
                    error.to_string(),
                ))
            })?;
        let record = match response.outcome {
            StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => record,
            StorageRpcBucketWriteReservationAcquireOutcome::Draining
            | StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { .. } => {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket write reservation heartbeat response",
                    "heartbeat response returned non-record outcome".to_string(),
                )));
            }
        };
        let mut previous_identity = record.clone();
        previous_identity.lease_deadline = proof.lease_deadline;
        if !proof.matches_record(&previous_identity) || record.lease_deadline != lease_deadline {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservation heartbeat response",
                "heartbeat response identity does not match request".to_string(),
            )));
        }
        Ok(record)
    }

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteReservationRecordRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            record: record.clone(),
        };
        let payload =
            encode_bucket_write_reservation_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write reservation release request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::BucketWriteReservationRelease,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write reservation release response",
            &response,
        )
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcProofReleaseRequest {
            node_id: self.node_id,
            route_cluster_epoch: proof.cluster_epoch,
            pg_id: pg_id.pg_id(),
            proof: proof.clone(),
        };
        let payload = encode_proof_release_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode proof release request", error.to_string()),
            )
        })?;
        let response =
            self.rpc_request_bucket_snapshot(StorageRpcMessageKind::ProofRelease, payload)?;
        self.validate_proof_release_response(&response)?;
        Ok(())
    }

    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainBeginRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id: pg_id.pg_id(),
                bucket: bucket.clone(),
            },
            drain_id: drain_id.to_string(),
            owner_token: owner_token.to_string(),
            created_at,
            lease_deadline,
        };
        let payload =
            encode_bucket_write_drain_begin_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain begin request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainBegin, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_write_drain_begin_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write drain begin response",
                error.to_string(),
            ))
        })?;
        let record = match response.outcome {
            StorageRpcBucketWriteDrainBeginOutcome::Acquired(record) => record,
            StorageRpcBucketWriteDrainBeginOutcome::Conflict => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict {
                        drain_id: drain_id.to_string(),
                    },
                ));
            }
        };
        if record.bucket != *bucket
            || record.drain_id != drain_id
            || record.owner_token != owner_token
            || record.cluster_epoch != cluster_epoch
            || record.created_at != created_at
            || record.lease_deadline != lease_deadline
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain begin response",
                "drain response identity does not match request".to_string(),
            )));
        }
        Ok(record)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainRecordRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            record: record.clone(),
        };
        let payload =
            encode_bucket_write_drain_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain clear request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainClear, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket write drain clear response",
            &response,
        )
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainClearExpiredRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
                bucket: bucket.clone(),
            },
            now,
        };
        let payload =
            encode_bucket_write_drain_clear_expired_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket write drain clear expired request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteDrainClearExpired, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_write_drain_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write drain clear expired response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket write drain clear expired response",
                    "expired drain bucket does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let request = StorageRpcBucketWriteDrainHeartbeatRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            record: record.clone(),
            lease_deadline,
        };
        let payload = encode_bucket_write_drain_heartbeat_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket write drain heartbeat request",
                error.to_string(),
            ))
        })?;
        let kind = StorageRpcMessageKind::BucketWriteDrainHeartbeat;
        let response = self
            .rpc_request_result(kind, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = match response {
            Ok(response) => response,
            Err(error) if error.code == StorageRpcErrorCode::BucketWriteDrainConflict => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict {
                        drain_id: error.message,
                    },
                ));
            }
            Err(error) if error.code == StorageRpcErrorCode::BucketWriteDrainNotFound => {
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainNotFound {
                        drain_id: error.message,
                    },
                ));
            }
            Err(error) => {
                return Err(BucketSnapshotLoadError::Store(
                    self.rpc_response_error(kind, error),
                ));
            }
        };
        let response =
            decode_bucket_write_drain_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket write drain heartbeat response",
                    error.to_string(),
                ))
            })?;
        let Some(renewed) = response.record else {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain heartbeat response",
                "heartbeat response returned no drain record".to_string(),
            )));
        };
        if renewed.bucket != record.bucket
            || renewed.drain_id != record.drain_id
            || renewed.owner_token != record.owner_token
            || renewed.cluster_epoch != record.cluster_epoch
            || renewed.bucket_execution_generation != record.bucket_execution_generation
            || renewed.lease_deadline != lease_deadline
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write drain heartbeat response",
                "heartbeat response identity does not match request".to_string(),
            )));
        }
        Ok(renewed)
    }

    fn durable_bucket_write_reservations(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketWriteReservationsList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = crate::storage_rpc::decode_bucket_write_reservations_list_response(
            &response,
        )
        .map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket write reservations list response",
                error.to_string(),
            ))
        })?;
        if response
            .records
            .iter()
            .any(|record| record.bucket != *bucket)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservations list response",
                "reservation bucket does not match request".to_string(),
            )));
        }
        Ok(response.records)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteFinalizeRootsRequest {
            route: StorageRpcBucketPgRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
            },
            now,
            limit,
        };
        let payload = encode_bucket_delete_finalize_roots_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket delete finalize roots request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketDeleteFinalizeRoots, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_bucket_delete_finalize_roots_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete finalize roots response",
                    error.to_string(),
                ))
            })?;
        if response.roots.len() > limit {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket delete finalize roots response",
                "root count exceeds request limit".to_string(),
            )));
        }
        Ok(response.roots)
    }

    fn get_bucket_delete_begin_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteBeginRootsRequest {
            route: StorageRpcBucketPgRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
            },
            now,
            start_after_bucket: start_after_bucket.cloned(),
            limit,
        };
        let payload = encode_bucket_delete_begin_roots_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode bucket delete begin roots request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketDeleteBeginRoots, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_delete_begin_roots_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode bucket delete begin roots response",
                error.to_string(),
            ))
        })?;
        if response.roots.len() > limit {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket delete begin roots response",
                "root count exceeds request limit".to_string(),
            )));
        }
        Ok(response.roots)
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteFinalizeClaimAcquireRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id: pg_id.pg_id(),
                bucket: bucket.clone(),
            },
            bucket_incarnation_generation,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload =
            encode_bucket_delete_finalize_claim_acquire_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket delete finalize claim acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire,
            payload,
        )?;
        let response = decode_bucket_delete_finalize_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete finalize claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket delete finalize claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcBucketDeleteFinalizeClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            record: claim.clone(),
        };
        let payload =
            encode_bucket_delete_finalize_claim_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode bucket delete finalize claim release request",
                    error.to_string(),
                ))
            })?;
        let kind = StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease;
        let response = self.rpc_request_bucket_snapshot(kind, payload)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode bucket delete finalize claim release response",
            &response,
        )
    }

    fn bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::BucketDeleteFinalizeClaimGet, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_bucket_delete_finalize_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode bucket delete finalize claim get response",
                    error.to_string(),
                ))
            })?;
        Ok(response.record)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepRootsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            now,
            limit,
        };
        let payload = encode_lifecycle_sweep_roots_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode lifecycle sweep roots request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::LifecycleSweepRoots, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_lifecycle_sweep_roots_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode lifecycle sweep roots response", error.to_string()),
            )
        })?;
        if response.roots.len() > limit {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep roots response",
                "root count exceeds request limit".to_string(),
            )));
        }
        Ok(response.roots)
    }

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: BucketPgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError> {
        let request = StorageRpcBucketPgRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
        };
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("encode lifecycle sweep buckets request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::LifecycleSweepBucketsList, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response =
            decode_lifecycle_sweep_buckets_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep buckets response",
                    error.to_string(),
                ))
            })?;
        Ok(response.buckets)
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimAcquireRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id: pg_id.pg_id(),
                bucket: bucket.clone(),
            },
            bucket_incarnation_generation,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload = encode_lifecycle_sweep_claim_acquire_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim acquire request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimAcquire,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_optional_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate lifecycle sweep claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: BucketPgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimHeartbeatRequest {
            record: StorageRpcLifecycleSweepClaimRecordRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
                claim: claim.clone(),
            },
            heartbeat_at,
            lease_deadline,
        };
        let payload =
            encode_lifecycle_sweep_claim_heartbeat_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode lifecycle sweep claim heartbeat request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim heartbeat response",
                    error.to_string(),
                ))
            })?;
        if !lifecycle_sweep_claim_identity_matches(&response.record, claim)
            || response.record.heartbeat_at != heartbeat_at
            || response.record.lease_deadline != lease_deadline
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep claim heartbeat response",
                "claim response identity does not match request".to_string(),
            )));
        }
        Ok(response.record)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: BucketPgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimErrorRequest {
            record: StorageRpcLifecycleSweepClaimRecordRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
                claim: claim.clone(),
            },
            last_error: last_error.to_string(),
        };
        let payload = encode_lifecycle_sweep_claim_error_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim error request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimError,
            payload,
        )?;
        let response =
            decode_lifecycle_sweep_claim_record_response(&response).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode lifecycle sweep claim error response",
                    error.to_string(),
                ))
            })?;
        if !lifecycle_sweep_claim_identity_matches(&response.record, claim)
            || response.record.last_error.as_deref() != Some(last_error)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate lifecycle sweep claim error response",
                "claim response identity does not match request".to_string(),
            )));
        }
        Ok(response.record)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: BucketPgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcLifecycleSweepClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            claim: claim.clone(),
        };
        let payload = encode_lifecycle_sweep_claim_record_request(&request).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "encode lifecycle sweep claim release request",
                error.to_string(),
            ))
        })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::LifecycleSweepClaimRelease,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode lifecycle sweep claim release response",
            &response,
        )
    }
}

impl ObjectGenerationMetadataNodeClient for UnixStorageNodeClient {
    fn object_generation_reservation(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectGenerationReservationRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            reservation_id: reservation_id.clone(),
        };
        let payload = encode_object_generation_reservation_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectGenerationReservation, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_generation_reservation_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object generation reservation response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectGenerationReservationOutcome::Found(generation_id) => Ok(generation_id),
            StorageRpcObjectGenerationReservationOutcome::NotFound { reservation_id } => Err(
                ObjectPgActionError::Metadata(MetadataError::ObjectGenerationReservationNotFound {
                    reservation_id: reservation_id.into_string(),
                }),
            ),
        }
    }

    fn next_object_generation_id(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectGenerationNext, payload)
            .map_err(ObjectPgActionError::Store)?;
        decode_object_generation_response(&response)
            .map(|response| response.generation_id)
            .map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error("decode object generation response", error.to_string()),
                )
            })
    }
}

impl ObjectVersionMetadataNodeClient for UnixStorageNodeClient {
    fn next_object_version_id(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id_with_admission_class(
            pg_id,
            bucket,
            key,
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectVersionNext),
        )
    }

    fn next_completion_object_version_id(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id_with_admission_class(
            pg_id,
            bucket,
            key,
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }
}

impl UnixStorageNodeClient {
    fn next_object_version_id_with_admission_class(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Result<VersionId, ObjectPgActionError> {
        let request = StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectVersionNext,
                payload,
                class,
            )
            .map_err(ObjectPgActionError::Store)?;
        decode_object_version_response(&response)
            .map(|response| response.version_id)
            .map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error("decode object version response", error.to_string()),
                )
            })
    }
}

impl DirectPutMetadataNodeClient for UnixStorageNodeClient {
    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcDirectPutCommitSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            reservation_id: reservation_id.clone(),
            generation_id,
        };
        let payload = encode_direct_put_commit_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::DirectPutCommitSnapshotLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_commit_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode direct PUT commit snapshot response",
                error.to_string(),
            ))
        })?;
        self.validate_direct_put_commit_snapshot_response(&response.snapshot, bucket, key)?;
        Ok(response.snapshot)
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcDirectPutCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: request.cluster_epoch,
                pg_id: request.pg_id,
                bucket: request.request.bucket.clone(),
                key: request.request.key.clone(),
            },
            request: request.request.clone(),
            version_id: request.version_id,
            expected_snapshot: request.expected_snapshot.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_direct_put_command_build_request(&rpc_request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode direct PUT commit command build request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::DirectPutCommitCommandBuild, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_command_build_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode direct PUT commit command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcDirectPutCommandBuildOutcome::Command(command) => {
                self.validate_direct_put_command_build_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleDirectPutCommitSnapshot)
            }
            StorageRpcDirectPutCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != request.pg_id.get() {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode direct PUT commit command build response",
                        "metadata command log conflict route mismatch".to_string(),
                    )));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode direct PUT commit command build response",
                        "metadata command log conflict index must not be zero".to_string(),
                    )));
                }
                Err(ObjectPgActionError::Store(
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
}

impl ObjectMutationMetadataNodeClient for UnixStorageNodeClient {
    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        let request = StorageRpcStreamUploadMatchRequest {
            object: self.object_request(pg_id, &create.bucket, &create.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_stream_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream upload match request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload match response", error.to_string()),
            )
        })?;
        self.validate_stream_upload_match_response(response.exists, expected_command)?;
        Ok(response.exists)
    }

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadMatchRequest {
            object: self.object_request(pg_id, &create.bucket, &create.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_multipart_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode multipart upload match request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode multipart upload match response", error.to_string()),
            )
        })?;
        self.validate_multipart_upload_match_response(response.initiated_at, expected_command)?;
        Ok(response.initiated_at)
    }

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_upload_session_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadSessionLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_session_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload session response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcStreamUploadSessionOutcome::Loaded(session) => {
                self.validate_stream_upload_session_response(
                    &session,
                    bucket,
                    key,
                    session_id,
                    "validate stream upload session response",
                )?;
                Ok(*session)
            }
            StorageRpcStreamUploadSessionOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != *session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream upload session response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartUploadLoad, payload)
            .map_err(BucketSnapshotLoadError::from)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            BucketSnapshotLoadError::from(
                self.rpc_payload_error("decode multipart upload load response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    None,
                    "validate multipart upload load response",
                )
                .map_err(|error| match error {
                    ObjectPgActionError::Store(store) => BucketSnapshotLoadError::Store(store),
                    ObjectPgActionError::Metadata(metadata) => {
                        BucketSnapshotLoadError::Metadata(metadata)
                    }
                    other => BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate multipart upload load response",
                        other.to_string(),
                    )),
                })?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(BucketSnapshotLoadError::from(self.rpc_payload_error(
                        "validate multipart upload load response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    },
                ))
            }
        }
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode in-progress multipart upload load response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    "validate in-progress multipart upload load response",
                )?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate in-progress multipart upload load response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode in-progress multipart upload listing response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.validate_multipart_upload_response(
                    &upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    "validate in-progress multipart upload listing response",
                )?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate in-progress multipart upload listing response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let request = StorageRpcMultipartCompletionSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
            requested_part_numbers: requested_part_numbers.to_vec(),
        };
        let payload = encode_multipart_completion_snapshot_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode multipart completion snapshot request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion snapshot response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcMultipartCompletionSnapshotOutcome::Loaded(snapshot) => {
                self.validate_multipart_completion_snapshot_response(
                    &snapshot,
                    authorized_upload,
                    requested_part_numbers,
                )?;
                Ok(*snapshot)
            }
            StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
            StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: returned_upload_id,
                part_number,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part upload id does not match request".to_string(),
                    )));
                }
                if !requested_part_numbers.contains(&part_number) {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part number does not match request".to_string(),
                    )));
                }
                Err(MetadataError::PartNotFound {
                    upload_id: upload_id.to_string(),
                    part_number,
                }
                .into())
            }
        }
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let request = StorageRpcMultipartCompletionPreflightRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
        };
        let payload = encode_multipart_completion_preflight_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "encode multipart completion preflight request",
                error.to_string(),
            ))
        })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_preflight_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion preflight response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight) => Ok(preflight),
            StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion preflight response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let request = StorageRpcMultipartPartsListRequest {
            object: self.object_request(pg_id, bucket, key),
            authorized_upload: authorized_upload.record().clone(),
            part_number_marker,
            max_parts,
        };
        let payload = encode_multipart_parts_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode multipart parts list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectMultipartPartsList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_parts_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode multipart parts list response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartPartsListOutcome::Loaded(listed) => {
                self.validate_listed_multipart_parts_response(
                    &listed,
                    authorized_upload,
                    part_number_marker,
                    max_parts,
                )?;
                Ok(*listed)
            }
            StorageRpcMultipartPartsListOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart parts list response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartManagementLookup,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_management_lookup_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode multipart management lookup response",
                error.to_string(),
            ))
        })?;
        self.validate_multipart_management_lookup_response(
            &response.lookup,
            bucket,
            key,
            upload_id,
        )?;
        Ok(response.lookup)
    }

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let precondition = match request.precondition {
            CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation,
            } => StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation,
            },
            CreateStreamUploadPrecondition::PutObject {
                expected_current,
                require_generation_reservation,
            } => StorageRpcCreateStreamUploadPrecondition::PutObject {
                expected_current: expected_current.cloned(),
                require_generation_reservation,
            },
            CreateStreamUploadPrecondition::UploadPart { expected_upload } => {
                StorageRpcCreateStreamUploadPrecondition::UploadPart {
                    expected_upload: expected_upload.clone(),
                }
            }
        };
        let rpc_request = StorageRpcCreateStreamUploadCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            precondition,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_create_stream_upload_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream upload command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild,
            request.pg_id,
            payload,
            "decode stream upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_create_stream_upload_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None if matches!(
                request.precondition,
                CreateStreamUploadPrecondition::UploadPart { .. }
            ) =>
            {
                let StreamUploadTarget::UploadPart { upload_id, .. } = &request.request.target
                else {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "decode stream upload command build response",
                        "stream upload command build missing outcome for non-upload-part target"
                            .to_string(),
                    )));
                };
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into())
            }
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode stream upload command build response",
                "stream upload command build cannot return missing".to_string(),
            ))),
        }
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcCreateMultipartUploadCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            expected_current: request.expected_current.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_create_multipart_upload_command_build_request(&rpc_request).map_err(
            |error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode multipart upload command build request",
                    error.to_string(),
                ))
            },
        )?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartUploadCommandBuild,
            request.pg_id,
            payload,
            "decode multipart upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_create_multipart_upload_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode multipart upload command build response",
                "multipart upload command build cannot return missing".to_string(),
            ))),
        }
    }

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_upload_session_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_segments_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream upload segments response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcStreamUploadSegmentsOutcome::Loaded(segments) => {
                self.validate_stream_upload_segments_response(
                    &segments,
                    session_id,
                    "validate stream upload segments response",
                )?;
                Ok(segments)
            }
            StorageRpcStreamUploadSegmentsOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != *session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream upload segments response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream uploads list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectStreamUploadsList,
                payload,
                listing_probe_admission_class(limit),
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream uploads list response", error.to_string()),
            )
        })?;
        if response.uploads.len() > limit as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "response exceeded requested page limit".to_string(),
            )));
        }
        for upload in &response.uploads {
            if &upload.bucket != bucket {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate stream uploads list response",
                    "upload bucket does not match request".to_string(),
                )));
            }
            if session_id_marker.is_some_and(|marker| upload.session_id.as_str() <= marker.as_str())
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate stream uploads list response",
                    "upload is not after requested marker".to_string(),
                )));
            }
        }
        if response
            .uploads
            .windows(2)
            .any(|pair| pair[0].session_id.as_str() >= pair[1].session_id.as_str())
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "uploads are not strictly ordered by session id".to_string(),
            )));
        }
        if response.next_session_id_marker.as_ref()
            != response.uploads.last().map(|upload| &upload.session_id)
            && response.next_session_id_marker.is_some()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads list response",
                "next marker does not match the last returned upload".to_string(),
            )));
        }
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsPgListRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_pg_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("encode stream uploads PG list request", error.to_string()),
            )
        })?;
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadsPgList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode stream uploads PG list response", error.to_string()),
            )
        })?;
        if response.uploads.len() > limit as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "response exceeded requested page limit".to_string(),
            )));
        }
        if response
            .uploads
            .windows(2)
            .any(|pair| pair[0].session_id.as_str() >= pair[1].session_id.as_str())
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "uploads are not strictly ordered by session id".to_string(),
            )));
        }
        if session_id_marker.is_some()
            && response.uploads.iter().any(|upload| {
                session_id_marker
                    .is_some_and(|marker| upload.session_id.as_str() <= marker.as_str())
            })
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "upload is not after requested marker".to_string(),
            )));
        }
        if response.next_session_id_marker.as_ref()
            != response.uploads.last().map(|upload| &upload.session_id)
            && response.next_session_id_marker.is_some()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream uploads PG list response",
                "next marker does not match the last returned upload".to_string(),
            )));
        }
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self.object_request(pg_id, bucket, key),
            generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimExists, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_metadata_command_bool_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode object payload reclaim exists response",
                error.to_string(),
            ))
        })?;
        Ok(response.value)
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode object bucket payload reclaim root response",
                error.to_string(),
            ))
        })?;
        self.validate_bucket_payload_reclaim_root_response(&response, bucket)?;
        Ok(response.root)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimRoot, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode object payload reclaim root response",
                error.to_string(),
            ))
        })?;
        Ok(response.root)
    }

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self.object_request(pg_id, bucket, key),
            generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_object_payload_reclaim_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.rpc_payload_error("decode object payload reclaim response", error.to_string()),
            )
        })?;
        if !reclaim_matches_bucket_key_generation(
            response.reclaim.as_ref(),
            bucket,
            key,
            generation_id,
        ) {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate object payload reclaim response",
                "response reclaim payload does not match request".to_string(),
            )));
        }
        Ok(response.reclaim)
    }

    fn object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimClaimGet, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_object_payload_reclaim_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode object payload reclaim claim get response",
                    error.to_string(),
                ))
            })?;
        if response
            .record
            .as_ref()
            .is_some_and(|record| record.pg_id != pg_id.get())
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate object payload reclaim claim get response",
                "claim response PG does not match request".to_string(),
            )));
        }
        Ok(response.record)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            bucket_incarnation_generation,
            generation_id,
            reclaim_kind,
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            claimed_at,
            lease_deadline,
            now,
        };
        let payload =
            encode_object_payload_reclaim_claim_acquire_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode object payload reclaim claim acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
            payload,
        )?;
        let response = decode_object_payload_reclaim_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "decode object payload reclaim claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != *bucket
                || record.bucket_incarnation_generation != bucket_incarnation_generation
                || record.key != *key
                || record.generation_id != generation_id
                || record.reclaim_kind != reclaim_kind
                || record.claim_id != claim_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.pg_id != pg_id.get()
                || record.claimed_at != claimed_at
                || record.lease_deadline != lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate object payload reclaim claim acquire response",
                    "claim response identity does not match request".to_string(),
                )));
            }
        }
        Ok(response.record)
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimClaimRecordRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            record: claim.clone(),
        };
        let payload =
            encode_object_payload_reclaim_claim_record_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode object payload reclaim claim release request",
                    error.to_string(),
                ))
            })?;
        let response = self.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease,
            payload,
        )?;
        self.validate_empty_bucket_write_reservation_response(
            "decode object payload reclaim claim release response",
            &response,
        )
    }

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let expected_session =
            self.load_stream_upload_session(pg_id, bucket, key, &request.session_id)?;
        let expected_target = expected_session.target;
        let rpc_request = StorageRpcStreamSegmentAppendPrepareRequest {
            object: self.object_request(pg_id, bucket, key),
            request: request.clone(),
        };
        let payload = encode_stream_segment_append_prepare_request(&rpc_request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_segment_append_prepare_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream segment append prepare response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcStreamSegmentAppendPrepareOutcome::Prepared { target, segment } => {
                self.validate_stream_segment_append_prepare_response(
                    &segment,
                    &target,
                    &expected_target,
                    request,
                    "validate stream segment append prepare response",
                )?;
                Ok((target, *segment))
            }
            StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
                session_id: returned_session_id,
            } => {
                if returned_session_id != request.session_id {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate stream segment append prepare response",
                        "missing session id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::StreamSessionNotFound {
                    session_id: request.session_id.as_str().to_string(),
                }
                .into())
            }
        }
    }

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcStreamPutFinalizeSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
        };
        let payload = encode_stream_put_finalize_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_put_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.validate_stream_put_finalize_snapshot_response(
            &response.snapshot,
            bucket,
            key,
            session_id,
        )?;
        Ok(response.snapshot)
    }

    fn update_stream_upload_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
    ) -> Result<(), ObjectPgActionError> {
        let request = StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
            object: self.object_request(pg_id, bucket, key),
            session_id: session_id.clone(),
            current: current.clone(),
            renewed: renewed.clone(),
        };
        let payload = encode_stream_upload_bucket_write_reservation_update_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        self.validate_empty_bucket_write_reservation_response(
            "decode stream upload bucket write reservation update response",
            &response,
        )
        .map_err(|error| match error {
            BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
            BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
        })
    }

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcStreamPutCommitCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            session_id: request.session_id.clone(),
            total_size: request.total_size,
            expected_snapshot: request.expected_snapshot.clone(),
            commit: request.commit.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_stream_put_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream PUT commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT commit command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_stream_put_commit_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream PUT commit command build response",
                    "stream PUT commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode stream PUT commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcStreamPartFinalizeSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
            session_id: session_id.clone(),
            part_number,
        };
        let payload = encode_stream_part_finalize_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_part_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.validate_stream_part_finalize_snapshot_response(
            &response.snapshot,
            bucket,
            key,
            upload_id,
            session_id,
            part_number,
        )?;
        Ok(response.snapshot)
    }

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcStreamPartCommitCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            upload_id: request.upload_id.clone(),
            session_id: request.session_id.clone(),
            part_number: request.part_number,
            expected_snapshot: request.expected_snapshot.clone(),
            part: request.part.clone(),
            segments: request.segments.to_vec(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_stream_part_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode stream part commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part commit command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_stream_part_commit_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode stream part commit command build response",
                    "stream part commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode stream part commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn load_multipart_completion_stale_payload_source(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let request = self.object_request(pg_id, bucket, key);
        let payload = encode_object_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_stale_source_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode multipart completion stale source response",
                    error.to_string(),
                ))
            })?;
        if let Some(source) = response.source.as_ref() {
            match source {
                StoredObject::Live(live)
                    if live.bucket == *bucket && live.key == *key && live.version_id.is_null() => {}
                _ => {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate multipart completion stale source response",
                        "stale source must be null live object for requested object".to_string(),
                    )));
                }
            }
        }
        Ok(response.source)
    }

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcCompleteMultipartCommandBuildRequest {
            object: self.object_request(
                request.pg_id,
                &request.request.bucket,
                &request.request.key,
            ),
            request: request.request.clone(),
            version_id: request.version_id,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_complete_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode complete multipart command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild,
            request.pg_id,
            payload,
            "decode complete multipart command build response",
            ObjectPgActionError::StaleMultipartCompletionSnapshot,
            |command| self.validate_complete_multipart_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode complete multipart command build response",
                "complete multipart command build cannot return missing".to_string(),
            ))),
        }
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let bucket = request.bucket;
        let key = request.key;
        let upload_id = request.upload_id;
        let rpc_request = StorageRpcAbortMultipartCommandBuildRequest {
            object: self.object_request(request.pg_id, bucket, key),
            upload_id: upload_id.clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_abort_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAbortCommandBuild,
            request.pg_id,
            payload,
            "decode abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: request.pg_id,
                        cluster_epoch: request.cluster_epoch,
                        bucket,
                        key,
                        upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: &request.bucket_write_reservation,
                    },
                )
            },
        )
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let bucket = &request.authorized_upload.record().bucket;
        let key = &request.authorized_upload.record().key;
        let upload_id = &request.authorized_upload.record().upload_id;
        let rpc_request = StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
            object: self.object_request(request.pg_id, bucket, key),
            authorized_upload: request.authorized_upload.record().clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_authorized_abort_multipart_command_build_request(&rpc_request)
            .map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode authorized abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild,
            request.pg_id,
            payload,
            "decode authorized abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: request.pg_id,
                        cluster_epoch: request.cluster_epoch,
                        bucket,
                        key,
                        upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: &request.bucket_write_reservation,
                    },
                )
            },
        )
    }

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        let request = StorageRpcAbortMultipartCleanupRequest {
            object: self.object_request(pg_id, bucket, key),
            upload_id: upload_id.clone(),
        };
        let payload = encode_abort_multipart_cleanup_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_abort_multipart_cleanup_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode abort multipart cleanup response",
                    error.to_string(),
                ))
            })?;
        if let Some(cleanup) = response.cleanup.as_ref() {
            self.validate_abort_cleanup_snapshot_response(cleanup, bucket, key, upload_id)?;
        }
        Ok(response.cleanup)
    }

    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let request = StorageRpcPutObjectMetadataSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id,
        };
        let payload = encode_put_object_metadata_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_put_object_metadata_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT snapshot response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(stored) => {
                self.validate_stored_object_response(
                    &stored,
                    bucket,
                    key,
                    version_id,
                    "validate object metadata PUT snapshot response",
                )?;
                Ok(*stored)
            }
            StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound => {
                Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound))
            }
        }
    }

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let rpc_request = StorageRpcPutObjectMetadataCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            requested_version_id: request.requested_version_id,
            expected_stored: request.expected_stored.clone(),
            version_id: request.version_id,
            mutation: request.mutation.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_put_object_metadata_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode object metadata PUT command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT command build response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.validate_put_object_metadata_command_response(&command, &request)?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object metadata PUT command build response",
                    "PUT metadata command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                request.pg_id,
                "decode object metadata PUT command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.load_object_delete_snapshot(
            pg_id,
            bucket,
            key,
            None,
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
        )
    }

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.load_object_delete_snapshot(
            pg_id,
            bucket,
            key,
            Some(version_id),
            StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
        )
    }

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        self.load_object_lifecycle_version_list(pg_id, bucket, key)
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let rpc_request = StorageRpcDeleteSpecificObjectCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            version_id: request.version_id,
            expected_stored: request.expected_stored.cloned(),
            expected_target: request.expected_target.cloned(),
            expected_version_list: request.expected_version_list.map(<[StoredObject]>::to_vec),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_specific_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode delete-specific object command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
            request.pg_id,
            payload,
            "decode delete-specific object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_delete_specific_object_command_response(command, &request),
        )
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let rpc_request = StorageRpcDeleteCurrentObjectCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            expected_current: request.expected_current.cloned(),
            expected_target: request.expected_target.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_current_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode delete-current object command build request",
                    error.to_string(),
                ))
            })?;
        self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
            request.pg_id,
            payload,
            "decode delete-current object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_delete_current_object_command_response(command, &request),
        )
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let stale_payload = match &request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(reclaim) => {
                StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim.clone())
            }
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
                StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: *created_at,
                }
            }
        };
        let rpc_request = StorageRpcInsertDeleteMarkerCommandBuildRequest {
            object: self.object_request(request.pg_id, request.bucket, request.key),
            expected_current: request.expected_current.cloned(),
            expected_stale_payload_source: request.expected_stale_payload_source.cloned(),
            version_id: request.version_id,
            owner: request.owner.clone(),
            stale_payload,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_insert_delete_marker_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "encode insert-delete-marker command build request",
                    error.to_string(),
                ))
            })?;
        match self.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
            request.pg_id,
            payload,
            "decode insert-delete-marker command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| self.validate_insert_delete_marker_command_response(command, &request),
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "decode insert-delete-marker command build response",
                "insert-delete-marker command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl ObjectReadMetadataNodeClient for UnixStorageNodeClient {
    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let request = StorageRpcObjectReadAuthSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
        };
        let payload = encode_object_read_auth_subject_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectReadAuthSubjectLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_read_auth_subject_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode object read auth subject response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectReadAuthSubjectOutcome::Loaded(subject) => {
                self.validate_object_read_subject_response(&subject, bucket, key, version_id)?;
                Ok(*subject)
            }
            StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound => {
                Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound))
            }
        }
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let request = StorageRpcObjectReadSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
            expected_identity: expected_identity.clone(),
            snapshot_mode,
        };
        let payload = encode_object_read_snapshot_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectReadSnapshotLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_read_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode object read snapshot response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcObjectReadSnapshotOutcome::Loaded(snapshot) => {
                self.validate_object_read_snapshot_response(
                    &snapshot,
                    bucket,
                    key,
                    version_id,
                    expected_identity,
                    snapshot_mode,
                )?;
                Ok(*snapshot)
            }
            StorageRpcObjectReadSnapshotOutcome::StaleSubject => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
        }
    }

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: VersionId,
    ) -> Result<Option<String>, ObjectPgActionError> {
        let request = StorageRpcObjectTagsForSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id,
            expected_identity: expected_identity.clone(),
            authorized_version_id,
        };
        let payload = encode_object_tags_for_subject_request(&request);
        let response = self
            .rpc_request(StorageRpcMessageKind::ObjectTagsForSubjectLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_tags_for_subject_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object tags for subject response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcObjectTagsForSubjectOutcome::Loaded(tags) => Ok(tags),
            StorageRpcObjectTagsForSubjectOutcome::StaleSubject => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
        }
    }
}
