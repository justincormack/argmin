use super::*;
use crate::metadata_command::{
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::storage_rpc::StorageRpcStreamUploadsListResponse;

struct UnixObjectReadMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixObjectGenerationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixObjectVersionMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixDirectPutMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixPutObjectMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixObjectDeleteMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixMultipartUploadCreationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixMultipartUploadLookupMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixAuthorizedMultipartUploadMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    _route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    authorized_upload: AuthorizedMultipartUploadRecord,
}

struct UnixMultipartCompletionMutationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixMultipartAbortMutationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
}

struct UnixObjectPayloadReclaimMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

struct UnixObjectMutationScanMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataScanPgId,
    pg_topology: Arc<PgTopology>,
}

struct UnixStreamUploadCreationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

struct UnixStreamUploadSessionMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    session_id: SessionId,
}

struct UnixStreamPutFinalizationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    session_id: SessionId,
}

struct UnixStreamPartFinalizationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
    session_id: SessionId,
    part_number: u32,
}

struct UnixObjectListingMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataScanPgId,
    pg_topology: Arc<PgTopology>,
}

impl UnixStorageNodeClient {
    fn list_objects_page_rpc(
        &self,
        pg_id: ObjectMetadataScanPgId,
        pg_topology: &PgTopology,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
        validate_list_objects_response(self, pg_id, pg_topology, &response.response, req)?;
        Ok(response.response)
    }

    fn list_object_versions_page_rpc(
        &self,
        pg_id: ObjectMetadataScanPgId,
        pg_topology: &PgTopology,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListObjectVersionsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
        validate_list_object_versions_response(self, pg_id, pg_topology, &response.response, req)?;
        Ok(response.response)
    }

    fn list_multipart_uploads_page_rpc(
        &self,
        pg_id: ObjectMetadataScanPgId,
        pg_topology: &PgTopology,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        let request = StorageRpcListMultipartUploadsRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
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
        validate_list_multipart_uploads_response(
            self,
            pg_id,
            pg_topology,
            &response.response,
            req,
        )?;
        Ok(response.response)
    }
}

impl ObjectListingMetadataNodeClient for UnixStorageNodeClient {
    fn open_object_listing_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn ObjectListingMetadataRoute + '_>, BucketSnapshotLoadError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(BucketSnapshotLoadError::Store(
                StoreError::StaleMetadataOperation {
                    pg_id: pg_id.get(),
                    operation_epoch: route_cluster_epoch,
                    current_epoch: self.cluster_epoch,
                },
            ));
        }
        let pg_topology = self.pg_topology.as_ref().ok_or_else(|| {
            BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "open object listing metadata route",
                "object listing client has no installed PG topology".to_string(),
            ))
        })?;
        Ok(Box::new(UnixObjectListingMetadataRoute {
            client: self,
            pg_id,
            pg_topology: Arc::clone(pg_topology),
        }))
    }
}

impl ObjectListingMetadataRoute for UnixObjectListingMetadataRoute<'_> {
    fn list_objects_page(
        &self,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError> {
        self.client
            .list_objects_page_rpc(self.pg_id, &self.pg_topology, req)
    }

    fn list_object_versions_page(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError> {
        self.client
            .list_object_versions_page_rpc(self.pg_id, &self.pg_topology, req)
    }

    fn list_multipart_uploads_page(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError> {
        self.client
            .list_multipart_uploads_page_rpc(self.pg_id, &self.pg_topology, req)
    }
}

fn validate_listing_subject_pg(
    client: &UnixStorageNodeClient,
    pg_id: ObjectMetadataScanPgId,
    pg_topology: &PgTopology,
    bucket: &BucketName,
    key: &ObjectKey,
    operation: &'static str,
) -> Result<(), BucketSnapshotLoadError> {
    if pg_topology.object_pg_for(bucket, key) != pg_id.get() {
        return Err(BucketSnapshotLoadError::Store(client.rpc_payload_error(
            operation,
            "listing response object does not belong to the scoped scan PG".to_string(),
        )));
    }
    Ok(())
}

pub(super) fn validate_list_objects_response(
    client: &UnixStorageNodeClient,
    pg_id: ObjectMetadataScanPgId,
    pg_topology: &PgTopology,
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
        validate_listing_subject_pg(
            client,
            pg_id,
            pg_topology,
            object.bucket(),
            object.key(),
            "validate object list response",
        )?;
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
    pg_id: ObjectMetadataScanPgId,
    pg_topology: &PgTopology,
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
        validate_listing_subject_pg(
            client,
            pg_id,
            pg_topology,
            object.bucket(),
            object.key(),
            "validate object version list response",
        )?;
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
    pg_id: ObjectMetadataScanPgId,
    pg_topology: &PgTopology,
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
        validate_listing_subject_pg(
            client,
            pg_id,
            pg_topology,
            &upload.bucket,
            &upload.key,
            "validate multipart upload list response",
        )?;
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
    effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
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
        effect_deadline,
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

impl UnixStorageNodeClient {
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
            None,
        )
    }

    fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(acquire.cluster_epoch)?;
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
            self,
            pg_id,
            acquire,
            storage_rpc_admission_class(StorageRpcMessageKind::BucketWriteReservationAcquire),
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
        )
    }

    #[cfg(test)]
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
            None,
        )
    }

    fn acquire_completion_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(acquire.cluster_epoch)?;
        unix_storage_node_acquire_durable_bucket_write_reservation_with_admission_class(
            self,
            pg_id,
            acquire,
            UnixStorageNodeRpcAdmissionClass::Completion,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
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

    fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let route_cluster_epoch = effect_fence.cluster_epoch();
        effect_fence.require_valid_for(route_cluster_epoch)?;
        if route_cluster_epoch != self.cluster_epoch {
            return Err(BucketSnapshotLoadError::Store(
                StoreError::RouteAdmissionClusterMismatch {
                    admitted_epoch: route_cluster_epoch,
                    operation_epoch: self.cluster_epoch,
                },
            ));
        }
        let request = StorageRpcBucketWriteReservationHeartbeatRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            pg_id: pg_id.pg_id(),
            proof: proof.clone(),
            lease_deadline,
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_bucket_write_reservation_heartbeat_request(&request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "encode fenced bucket write reservation heartbeat request",
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
                    "decode fenced bucket write reservation heartbeat response",
                    error.to_string(),
                ))
            })?;
        let StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) = response.outcome
        else {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate fenced bucket write reservation heartbeat response",
                "unexpected non-acquired outcome".to_string(),
            )));
        };
        let mut previous_identity = record.clone();
        previous_identity.lease_deadline = proof.lease_deadline;
        if !proof.matches_record(&previous_identity) || record.lease_deadline != lease_deadline {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate fenced bucket write reservation heartbeat response",
                "heartbeat response identity does not match request".to_string(),
            )));
        }
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)] // Private wire adapter mirrors the RPC request envelope.
    fn begin_durable_bucket_write_drain_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(cluster_epoch)?;
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
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
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
        let mut reservation_ids = BTreeSet::new();
        if response
            .records
            .iter()
            .any(|record| !reservation_ids.insert(record.reservation_id.as_str()))
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket write reservations list response",
                "reservation response contains duplicate identities".to_string(),
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

    #[allow(clippy::too_many_arguments)] // Private wire adapter mirrors the RPC request envelope.
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
        if let Some(record) = &response.record {
            if record.bucket != *bucket || record.pg_id != pg_id.get() {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate bucket delete finalize claim get response",
                    "claim response subject does not match request".to_string(),
                )));
            }
        }
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

    fn list_buckets_with_lifecycle(
        &self,
        pg_id: BucketPgId,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
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

    #[allow(clippy::too_many_arguments)] // Private wire adapter mirrors the RPC request envelope.
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
}

impl UnixStorageNodeClient {
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

struct UnixBucketWriteReservationRoute<'a> {
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: BucketName,
}

struct UnixBucketWriteReservationScanRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: BucketPgId,
    pg_topology: Arc<PgTopology>,
}

impl UnixBucketWriteReservationRoute<'_> {
    fn require_bucket_subject(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if bucket != &self.bucket {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "request subject does not match the scoped bucket write route".to_string(),
                ),
            ));
        }
        Ok(())
    }

    fn require_current_subject(
        &self,
        bucket: &BucketName,
        cluster_epoch: ClusterEpoch,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(bucket, operation)?;
        if cluster_epoch != self.route_cluster_epoch {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "request epoch does not match the scoped bucket write route".to_string(),
                ),
            ));
        }
        Ok(())
    }

    fn require_claim_subject(
        &self,
        bucket: &BucketName,
        cluster_epoch: ClusterEpoch,
        pg_id: u32,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_current_subject(bucket, cluster_epoch, operation)?;
        if pg_id != self.pg_id.get() {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "claim PG does not match the scoped bucket write route".to_string(),
                ),
            ));
        }
        Ok(())
    }
}

impl BucketWriteReservationNodeClient for UnixStorageNodeClient {
    fn open_bucket_write_reservation_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        self.validate_bucket_route_subject(
            route_cluster_epoch,
            pg_id,
            bucket,
            "open bucket write reservation route",
        )?;
        Ok(Box::new(UnixBucketWriteReservationRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
        }))
    }

    fn open_bucket_write_reservation_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketWriteReservationScanRoute + '_>, BucketSnapshotLoadError> {
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
                "open bucket write reservation scan route",
                "bucket write reservation client has no installed PG topology".to_string(),
            ))
        })?;
        Ok(Box::new(UnixBucketWriteReservationScanRoute {
            client: self,
            pg_id,
            pg_topology: Arc::clone(pg_topology),
        }))
    }
}

impl UnixBucketWriteReservationScanRoute<'_> {
    fn require_bucket(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if self.pg_topology.bucket_pg_for(bucket) != self.pg_id.get() {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    operation,
                    "response bucket does not belong to the scoped bucket metadata PG".to_string(),
                ),
            ));
        }
        Ok(())
    }
}

impl BucketWriteReservationScanRoute for UnixBucketWriteReservationScanRoute<'_> {
    fn get_bucket_delete_finalize_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let roots = self
            .client
            .get_bucket_delete_finalize_roots(self.pg_id, now, limit)?;
        let mut identities = BTreeSet::new();
        for root in &roots {
            self.require_bucket(
                &root.bucket,
                "validate bucket delete finalize roots response",
            )?;
            if !identities.insert((&root.bucket, root.bucket_incarnation_generation)) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket delete finalize roots response",
                        "response contains a duplicate finalizer root".to_string(),
                    ),
                ));
            }
        }
        Ok(roots)
    }

    fn get_bucket_delete_begin_roots(
        &self,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, BucketSnapshotLoadError> {
        if let Some(bucket) = start_after_bucket {
            self.require_bucket(bucket, "get bucket delete begin roots")?;
        }
        let roots = self.client.get_bucket_delete_begin_roots(
            self.pg_id,
            now,
            start_after_bucket,
            limit,
        )?;
        let mut previous = start_after_bucket;
        for root in &roots {
            self.require_bucket(&root.bucket, "validate bucket delete begin roots response")?;
            if previous.is_some_and(|previous| root.bucket <= *previous) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate bucket delete begin roots response",
                        "response roots are not strictly after the pagination marker".to_string(),
                    ),
                ));
            }
            previous = Some(&root.bucket);
        }
        Ok(roots)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let roots = self
            .client
            .get_lifecycle_sweep_roots(self.pg_id, now, limit)?;
        let mut identities = BTreeSet::new();
        for root in &roots {
            self.require_bucket(&root.bucket, "validate lifecycle sweep roots response")?;
            if !identities.insert((&root.bucket, root.bucket_incarnation_generation)) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate lifecycle sweep roots response",
                        "response contains a duplicate lifecycle root".to_string(),
                    ),
                ));
            }
        }
        Ok(roots)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let buckets = self.client.list_buckets_with_lifecycle(self.pg_id)?;
        let mut lifecycle_names = BTreeSet::new();
        for bucket in &buckets {
            self.require_bucket(&bucket.name, "validate lifecycle sweep buckets response")?;
            if !lifecycle_names.insert(&bucket.name) {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate lifecycle sweep buckets response",
                        "response contains a duplicate lifecycle bucket".to_string(),
                    ),
                ));
            }
        }
        Ok(buckets)
    }
}

impl BucketWriteReservationRoute for UnixBucketWriteReservationRoute<'_> {
    fn durable_bucket_write_drain_exists(&self) -> Result<bool, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_drain_exists(self.pg_id, &self.bucket)
    }

    fn durable_bucket_write_drain(
        &self,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_drain(self.pg_id, &self.bucket)
    }

    fn record_bucket_delete_attempt_outcome(
        &self,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(&record.bucket, "record bucket delete attempt outcome")?;
        self.client
            .record_bucket_delete_attempt_outcome(self.pg_id, record)
    }

    fn bucket_delete_attempt_outcome(
        &self,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketSnapshotLoadError> {
        self.client
            .bucket_delete_attempt_outcome(self.pg_id, &self.bucket)
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire durable bucket write reservation",
        )?;
        self.client
            .acquire_durable_bucket_write_reservation(self.pg_id, acquire)
    }

    fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire durable bucket write reservation",
        )?;
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .acquire_durable_bucket_write_reservation_with_effect_fence(
                self.pg_id,
                acquire,
                effect_fence,
            )
    }

    #[cfg(test)]
    fn acquire_completion_durable_bucket_write_reservation(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire completion bucket write reservation",
        )?;
        self.client
            .acquire_completion_durable_bucket_write_reservation(self.pg_id, acquire)
    }

    fn acquire_completion_durable_bucket_write_reservation_with_effect_fence(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_current_subject(
            acquire.name,
            acquire.cluster_epoch,
            "acquire completion bucket write reservation",
        )?;
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .acquire_completion_durable_bucket_write_reservation_with_effect_fence(
                self.pg_id,
                acquire,
                effect_fence,
            )
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_bucket_subject(&proof.bucket, "validate bucket write reservation proof")?;
        self.client
            .validate_bucket_write_reservation_proof(self.pg_id, proof)
    }

    fn begin_durable_bucket_write_drain(
        &self,
        drain_id: &str,
        owner_token: &str,
        created_at: u64,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        self.begin_durable_bucket_write_drain_with_effect_fence(
            drain_id,
            owner_token,
            created_at,
            lease_deadline,
            AdmittedRouteEffectFence::unbounded(self.route_cluster_epoch),
        )
    }

    fn begin_durable_bucket_write_drain_with_effect_fence(
        &self,
        drain_id: &str,
        owner_token: &str,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .begin_durable_bucket_write_drain_with_effect_fence(
                self.pg_id,
                &self.bucket,
                drain_id,
                owner_token,
                self.route_cluster_epoch,
                created_at,
                lease_deadline,
                effect_fence,
            )
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        self.client
            .clear_expired_durable_bucket_write_drain(self.pg_id, &self.bucket, now)
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        self.require_bucket_subject(&record.bucket, "heartbeat durable bucket write drain")?;
        self.client
            .heartbeat_durable_bucket_write_drain(self.pg_id, record, lease_deadline)
    }

    fn durable_bucket_write_reservations(
        &self,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        self.client
            .durable_bucket_write_reservations(self.pg_id, &self.bucket)
    }

    fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.require_bucket_subject(&proof.bucket, "heartbeat durable bucket write reservation")?;
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.client
            .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                self.pg_id,
                proof,
                lease_deadline,
                effect_fence,
            )
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        self.client.acquire_bucket_delete_finalize_claim(
            self.pg_id,
            &self.bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            self.route_cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn bucket_delete_finalize_claim(
        &self,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        self.client
            .bucket_delete_finalize_claim(self.pg_id, &self.bucket)
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        self.client.acquire_lifecycle_sweep_claim(
            self.pg_id,
            &self.bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            self.route_cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.cluster_epoch,
            claim.pg_id,
            "heartbeat lifecycle sweep claim",
        )?;
        self.client
            .heartbeat_lifecycle_sweep_claim(self.pg_id, claim, heartbeat_at, lease_deadline)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.cluster_epoch,
            claim.pg_id,
            "record lifecycle sweep claim error",
        )?;
        self.client
            .record_lifecycle_sweep_claim_error(self.pg_id, claim, last_error)
    }
}

struct UnixRetainedBucketWriteReservationRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: BucketPgId,
    bucket: BucketName,
}

impl UnixRetainedBucketWriteReservationRoute<'_> {
    fn require_subject(
        &self,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if bucket != &self.bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_claim_subject(
        &self,
        bucket: &BucketName,
        pg_id: u32,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(bucket, operation)?;
        if pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl RetainedBucketWriteReservationNodeClient for UnixStorageNodeClient {
    fn open_retained_bucket_write_reservation_route(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn RetainedBucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        Ok(Box::new(UnixRetainedBucketWriteReservationRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
        }))
    }
}

impl RetainedBucketWriteReservationRoute for UnixRetainedBucketWriteReservationRoute<'_> {
    fn release_durable_bucket_write_reservation(
        &self,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(&record.bucket, "release durable bucket write reservation")?;
        self.client
            .release_durable_bucket_write_reservation(self.pg_id, record)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(
            &proof.bucket,
            "release metadata command bucket write reservation",
        )?;
        self.client
            .release_metadata_command_bucket_write_reservation(self.pg_id, proof)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_subject(&record.bucket, "clear durable bucket write drain")?;
        self.client
            .clear_durable_bucket_write_drain(self.pg_id, record)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(
            &claim.bucket,
            claim.pg_id,
            "release bucket delete finalize claim",
        )?;
        self.client
            .release_bucket_delete_finalize_claim(self.pg_id, claim)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(&claim.bucket, claim.pg_id, "release lifecycle sweep claim")?;
        self.client.release_lifecycle_sweep_claim(self.pg_id, claim)
    }
}

impl ObjectGenerationMetadataNodeClient for UnixStorageNodeClient {
    fn open_object_generation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectGenerationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixObjectGenerationMetadataRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl ObjectGenerationMetadataRoute for UnixObjectGenerationMetadataRoute<'_> {
    fn object_generation_reservation(
        &self,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectGenerationReservationRequest {
            object: StorageRpcObjectRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.client.cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            reservation_id: reservation_id.clone(),
        };
        let payload = encode_object_generation_reservation_request(&request);
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectGenerationReservation, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_generation_reservation_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
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

    fn next_object_generation_id(&self) -> Result<GenerationId, ObjectPgActionError> {
        let request = StorageRpcObjectRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.client.cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
        };
        let payload = encode_object_request(&request);
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectGenerationNext, payload)
            .map_err(ObjectPgActionError::Store)?;
        decode_object_generation_response(&response)
            .map(|response| response.generation_id)
            .map_err(|error| {
                ObjectPgActionError::Store(
                    self.client
                        .rpc_payload_error("decode object generation response", error.to_string()),
                )
            })
    }
}

impl ObjectVersionMetadataNodeClient for UnixStorageNodeClient {
    fn open_object_version_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectVersionMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixObjectVersionMetadataRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl ObjectVersionMetadataRoute for UnixObjectVersionMetadataRoute<'_> {
    fn next_object_version_id(&self) -> Result<VersionId, ObjectPgActionError> {
        self.client.next_object_version_id_with_admission_class(
            self.pg_id,
            &self.bucket,
            &self.key,
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectVersionNext),
        )
    }

    fn next_completion_object_version_id(&self) -> Result<VersionId, ObjectPgActionError> {
        self.client.next_object_version_id_with_admission_class(
            self.pg_id,
            &self.bucket,
            &self.key,
            UnixStorageNodeRpcAdmissionClass::Completion,
        )
    }
}

impl UnixStorageNodeClient {
    fn load_stream_upload_session_rpc(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id.pg_id(), bucket, key),
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

    fn load_stream_upload_segments_rpc(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id.pg_id(), bucket, key),
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

    fn prepare_stream_segment_append_rpc(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let expected_session =
            self.load_stream_upload_session_rpc(pg_id, bucket, key, &request.session_id)?;
        let expected_target = expected_session.target;
        let rpc_request = StorageRpcStreamSegmentAppendPrepareRequest {
            object: self.object_request(pg_id.pg_id(), bucket, key),
            request: request.clone(),
            effect_deadline,
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
    fn open_direct_put_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn DirectPutMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixDirectPutMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl UnixDirectPutMetadataRoute<'_> {
    fn require_request_subject(
        &self,
        request: &BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if request.request.bucket != self.bucket
            || request.request.key != self.key
            || !request
                .request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
            || request.request.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build direct PUT commit command",
            }
            .into());
        }
        Ok(())
    }
}

impl DirectPutMetadataRoute for UnixDirectPutMetadataRoute<'_> {
    fn load_direct_put_commit_snapshot(
        &self,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcDirectPutCommitSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            reservation_id: reservation_id.clone(),
            generation_id,
        };
        let payload = encode_direct_put_commit_snapshot_request(&request);
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::DirectPutCommitSnapshotLoad, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_commit_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode direct PUT commit snapshot response",
                error.to_string(),
            ))
        })?;
        self.client.validate_direct_put_commit_snapshot_response(
            &response.snapshot,
            &self.bucket,
            &self.key,
        )?;
        Ok(response.snapshot)
    }

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_request_subject(&request)?;
        let rpc_request = StorageRpcDirectPutCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            request: request.request.clone(),
            version_id: request.version_id,
            expected_snapshot: request.expected_snapshot.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_direct_put_command_build_request(&rpc_request).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "encode direct PUT commit command build request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::DirectPutCommitCommandBuild, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_direct_put_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode direct PUT commit command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcDirectPutCommandBuildOutcome::Command(command) => {
                self.client.validate_direct_put_command_build_response(
                    &command,
                    self.route_cluster_epoch,
                    self.pg_id,
                    &self.bucket,
                    &self.key,
                    &request,
                )?;
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
                if cluster_epoch != self.route_cluster_epoch || conflict_pg_id != self.pg_id.get() {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "decode direct PUT commit command build response",
                        "metadata command log conflict route mismatch".to_string(),
                    )));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
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

impl UnixStorageNodeClient {
    pub(super) fn validate_retained_stream_upload_abort_prepare_command(
        &self,
        command: MetadataCommandEnvelope,
        pg_id: ObjectMetadataPgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<PreparedRetainedStreamUploadAbort, ObjectPgActionError> {
        PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            bucket,
            key,
            session_id,
            command,
        )
        .ok_or_else(|| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "validate retained stream upload abort prepare response",
                "response is not the requested retained stream abort command".to_string(),
            ))
        })
    }
}

impl UnixStorageNodeClient {
    fn prepare_retained_stream_upload_abort(
        &self,
        pg_id: ObjectMetadataPgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Option<PreparedRetainedStreamUploadAbort>, ObjectPgActionError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(ObjectPgActionError::Store(
                StoreError::StalePayloadOperation {
                    pg_id: pg_id.get(),
                    operation_epoch: cluster_epoch,
                    current_epoch: self.cluster_epoch,
                },
            ));
        }
        let request = StorageRpcStreamUploadSessionRequest {
            object: self.object_request(pg_id.pg_id(), bucket, key),
            session_id: session_id.clone(),
        };
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare,
                encode_stream_upload_session_request(&request),
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_metadata_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "decode retained stream upload abort prepare response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                let prepared = self.validate_retained_stream_upload_abort_prepare_command(
                    *command,
                    pg_id,
                    cluster_epoch,
                    bucket,
                    key,
                    session_id,
                )?;
                Ok(Some(prepared))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => Ok(None),
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                pg_id.pg_id(),
                "decode retained stream upload abort prepare response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode retained stream upload abort prepare response",
                    "retained stream abort prepare cannot return stale snapshot".to_string(),
                )))
            }
        }
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: ObjectMetadataPgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = pg_id.pg_id();
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
}

struct UnixRetainedObjectMutationMetadataRoute<'a> {
    client: &'a UnixStorageNodeClient,
    pg_id: ObjectMetadataPgId,
    cluster_epoch: ClusterEpoch,
    bucket: BucketName,
    key: ObjectKey,
}

impl UnixRetainedObjectMutationMetadataRoute<'_> {
    fn require_claim_subject(
        &self,
        claim: &ObjectPayloadReclaimClaimRecord,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if claim.bucket != self.bucket
            || claim.key != self.key
            || claim.pg_id != self.pg_id.get()
            || claim.cluster_epoch != self.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl RetainedObjectMutationMetadataNodeClient for UnixStorageNodeClient {
    fn open_retained_object_mutation_route(
        &self,
        pg_id: ObjectMetadataPgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn RetainedObjectMutationMetadataRoute + '_>, BucketSnapshotLoadError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixRetainedObjectMutationMetadataRoute {
            client: self,
            pg_id,
            cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl RetainedObjectMutationMetadataRoute for UnixRetainedObjectMutationMetadataRoute<'_> {
    fn prepare_retained_stream_upload_abort(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<PreparedRetainedStreamUploadAbort>, ObjectPgActionError> {
        self.client.prepare_retained_stream_upload_abort(
            self.pg_id,
            self.cluster_epoch,
            &self.bucket,
            &self.key,
            session_id,
        )
    }

    fn release_object_payload_reclaim_claim(
        &self,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_claim_subject(claim, "release object payload reclaim claim")?;
        self.client
            .release_object_payload_reclaim_claim(self.pg_id, claim)
    }
}

impl UnixPutObjectMetadataRoute<'_> {
    fn require_request_subject(
        &self,
        request: &BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if request.expected_stored.bucket() != &self.bucket
            || request.expected_stored.key() != &self.key
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build PUT object metadata command",
            }
            .into());
        }
        Ok(())
    }
}

impl PutObjectMetadataRoute for UnixPutObjectMetadataRoute<'_> {
    fn load_put_object_metadata_snapshot(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let request = StorageRpcPutObjectMetadataSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            version_id,
        };
        let payload = encode_put_object_metadata_snapshot_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_put_object_metadata_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode object metadata PUT snapshot response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(stored) => {
                self.client.validate_stored_object_response(
                    &stored,
                    &self.bucket,
                    &self.key,
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
        self.require_request_subject(&request)?;
        let rpc_request = StorageRpcPutObjectMetadataCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
            },
            requested_version_id: request.requested_version_id,
            expected_stored: request.expected_stored.clone(),
            version_id: request.version_id,
            mutation: request.mutation.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_put_object_metadata_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode object metadata PUT command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_metadata_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode object metadata PUT command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.client.validate_put_object_metadata_command_response(
                    &command,
                    self.route_cluster_epoch,
                    self.pg_id,
                    &request,
                )?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleObjectReadSubject)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode object metadata PUT command build response",
                    "PUT metadata command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.client.metadata_command_log_conflict_error(
                self.pg_id.pg_id(),
                "decode object metadata PUT command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }
}

impl UnixObjectDeleteMetadataRoute<'_> {
    fn object_request(&self) -> StorageRpcObjectRequest {
        StorageRpcObjectRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
        }
    }

    fn require_proof(
        &self,
        proof: &BucketWriteReservationProof,
        operation_kind: &str,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if !proof.matches_exact_mutation_subject(
            self.route_cluster_epoch,
            &self.bucket,
            operation_kind,
            Some(self.key.as_str()),
        ) {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_stored_subject(
        &self,
        stored: Option<&StoredObject>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if stored.is_some_and(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_reclaim_subject(
        &self,
        reclaim: Option<&ObjectPayloadReclaimCommand>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let crossed = match reclaim {
            Some(ObjectPayloadReclaimCommand::Segments(record)) => {
                record.bucket != self.bucket || record.key != self.key
            }
            Some(ObjectPayloadReclaimCommand::Multipart(record)) => {
                record.bucket != self.bucket || record.key != self.key
            }
            None => false,
        };
        if crossed {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_delete_target_subject(
        &self,
        target: Option<&DeleteObjectVersionTarget>,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if let Some(DeleteObjectVersionTarget::Live { payload, .. }) = target {
            self.require_reclaim_subject(Some(payload), operation)?;
        }
        Ok(())
    }
}

impl ObjectDeleteMetadataRoute for UnixObjectDeleteMetadataRoute<'_> {
    fn load_current_object_delete_snapshot(
        &self,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.client.load_object_delete_snapshot(
            self.pg_id.pg_id(),
            &self.bucket,
            &self.key,
            None,
            StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
        )
    }

    fn load_specific_object_delete_snapshot(
        &self,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        self.client.load_object_delete_snapshot(
            self.pg_id.pg_id(),
            &self.bucket,
            &self.key,
            Some(version_id),
            StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
        )
    }

    fn list_object_versions_for_lifecycle(&self) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        self.client
            .load_object_lifecycle_version_list(self.pg_id.pg_id(), &self.bucket, &self.key)
    }

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
            "build delete-specific object command",
        )?;
        self.require_stored_subject(
            request.expected_stored,
            "build delete-specific object command",
        )?;
        self.require_delete_target_subject(
            request.expected_target,
            "build delete-specific object command",
        )?;
        if request.expected_version_list.is_some_and(|versions| {
            versions
                .iter()
                .any(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
        }) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build delete-specific object command",
            }
            .into());
        }
        let rpc_request = StorageRpcDeleteSpecificObjectCommandBuildRequest {
            object: self.object_request(),
            version_id: request.version_id,
            expected_stored: request.expected_stored.cloned(),
            expected_target: request.expected_target.cloned(),
            expected_version_list: request.expected_version_list.map(<[StoredObject]>::to_vec),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_specific_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode delete-specific object command build request",
                    error.to_string(),
                ))
            })?;
        self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode delete-specific object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client
                    .validate_delete_specific_object_command_response(
                        command,
                        self.route_cluster_epoch,
                        self.pg_id,
                        &self.bucket,
                        &self.key,
                        &request,
                    )
            },
        )
    }

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
            "build delete-current object command",
        )?;
        self.require_stored_subject(
            request.expected_current,
            "build delete-current object command",
        )?;
        self.require_delete_target_subject(
            request.expected_target,
            "build delete-current object command",
        )?;
        let rpc_request = StorageRpcDeleteCurrentObjectCommandBuildRequest {
            object: self.object_request(),
            expected_current: request.expected_current.cloned(),
            expected_target: request.expected_target.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_delete_current_object_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode delete-current object command build request",
                    error.to_string(),
                ))
            })?;
        self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode delete-current object command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client.validate_delete_current_object_command_response(
                    command,
                    self.route_cluster_epoch,
                    self.pg_id,
                    &self.bucket,
                    &self.key,
                    &request,
                )
            },
        )
    }

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_proof(
            request.bucket_write_reservation,
            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
            "build insert-delete-marker command",
        )?;
        self.require_stored_subject(
            request.expected_current,
            "build insert-delete-marker command",
        )?;
        self.require_stored_subject(
            request.expected_stale_payload_source,
            "build insert-delete-marker command",
        )?;
        if let InsertDeleteMarkerStalePayload::Explicit(reclaim) = &request.stale_payload {
            self.require_reclaim_subject(reclaim.as_ref(), "build insert-delete-marker command")?;
        }
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
            object: self.object_request(),
            expected_current: request.expected_current.cloned(),
            expected_stale_payload_source: request.expected_stale_payload_source.cloned(),
            version_id: request.version_id,
            owner: request.owner.clone(),
            stale_payload,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_insert_delete_marker_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode insert-delete-marker command build request",
                    error.to_string(),
                ))
            })?;
        match self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode insert-delete-marker command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client.validate_insert_delete_marker_command_response(
                    command,
                    self.route_cluster_epoch,
                    self.pg_id,
                    &self.bucket,
                    &self.key,
                    &request,
                )
            },
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode insert-delete-marker command build response",
                "insert-delete-marker command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl UnixMultipartUploadCreationMetadataRoute<'_> {
    fn require_create_subject(
        &self,
        create: &CreateMultipartUploadReq,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if create.bucket != self.bucket || create.key != self.key {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_expected_command_subject(
        &self,
        create: &CreateMultipartUploadReq,
        command: &CreateMultipartUploadCommand,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if command.upload.bucket != self.bucket
            || command.upload.key != self.key
            || !command.matches_request(create)
            || !command
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }
}

impl MultipartUploadCreationMetadataRoute for UnixMultipartUploadCreationMetadataRoute<'_> {
    fn matching_multipart_upload_initiated_at(
        &self,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        self.require_create_subject(create, "match multipart upload creation")?;
        if let Some(command) = expected_command {
            self.require_expected_command_subject(
                create,
                command,
                "match multipart upload creation",
            )?;
        }
        let request = StorageRpcMultipartUploadMatchRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_multipart_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("encode multipart upload match request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectMultipartUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("decode multipart upload match response", error.to_string()),
            )
        })?;
        self.client
            .validate_multipart_upload_match_response(response.initiated_at, expected_command)?;
        Ok(response.initiated_at)
    }

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_create_subject(request.request, "build create multipart upload command")?;
        if request
            .expected_current
            .is_some_and(|stored| stored.bucket() != &self.bucket || stored.key() != &self.key)
            || !request
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create multipart upload command",
            }
            .into());
        }
        let rpc_request = StorageRpcCreateMultipartUploadCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            request: request.request.clone(),
            expected_current: request.expected_current.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload = encode_create_multipart_upload_command_build_request(&rpc_request).map_err(
            |error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode multipart upload command build request",
                    error.to_string(),
                ))
            },
        )?;
        match self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartUploadCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode multipart upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client
                    .validate_create_multipart_upload_command_response(
                        command,
                        self.route_cluster_epoch,
                        self.pg_id,
                        &self.bucket,
                        &self.key,
                        &request,
                    )
            },
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode multipart upload command build response",
                "multipart upload command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl MultipartCompletionMutationMetadataRoute for UnixMultipartCompletionMutationMetadataRoute<'_> {
    fn load_stale_payload_source(&self) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let request = self
            .client
            .object_request(self.pg_id.pg_id(), &self.bucket, &self.key);
        let payload = encode_object_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_stale_source_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode multipart completion stale source response",
                    error.to_string(),
                ))
            })?;
        if let Some(source) = response.source.as_ref() {
            match source {
                StoredObject::Live(live)
                    if live.bucket == self.bucket
                        && live.key == self.key
                        && live.version_id.is_null() => {}
                _ => {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
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
        require_multipart_completion_mutation_subject(
            self.route_cluster_epoch,
            &self.bucket,
            &self.key,
            &request,
        )?;
        let rpc_request = StorageRpcCompleteMultipartCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            request: request.request.clone(),
            version_id: request.version_id,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_complete_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode complete multipart command build request",
                    error.to_string(),
                ))
            })?;
        match self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode complete multipart command build response",
            ObjectPgActionError::StaleMultipartCompletionSnapshot,
            |command| {
                self.client.validate_complete_multipart_command_response(
                    command,
                    self.route_cluster_epoch,
                    self.pg_id,
                    &request,
                )
            },
        )? {
            Some(command) => Ok(command),
            None => Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode complete multipart command build response",
                "complete multipart command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl MultipartAbortMutationMetadataRoute for UnixMultipartAbortMutationMetadataRoute<'_> {
    fn load_cleanup(&self) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError> {
        let request = StorageRpcAbortMultipartCleanupRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            upload_id: self.upload_id.clone(),
        };
        let payload = encode_abort_multipart_cleanup_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_abort_multipart_cleanup_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode abort multipart cleanup response",
                    error.to_string(),
                ))
            })?;
        if let Some(cleanup) = response.cleanup.as_ref() {
            self.client.validate_abort_cleanup_snapshot_response(
                cleanup,
                &self.bucket,
                &self.key,
                &self.upload_id,
            )?;
        }
        Ok(response.cleanup)
    }

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_multipart_abort_mutation_subject(
            MultipartAbortMutationSubject {
                route_cluster_epoch: self.route_cluster_epoch,
                bucket: &self.bucket,
                key: &self.key,
                upload_id: &self.upload_id,
            },
            None,
            request.expected_cleanup,
            request.bucket_write_reservation,
            "build abort multipart upload command",
        )?;
        let rpc_request = StorageRpcAbortMultipartCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            upload_id: self.upload_id.clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_abort_multipart_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAbortCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: self.pg_id,
                        cluster_epoch: self.route_cluster_epoch,
                        bucket: &self.bucket,
                        key: &self.key,
                        upload_id: &self.upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: request.bucket_write_reservation,
                    },
                )
            },
        )
    }

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_multipart_abort_mutation_subject(
            MultipartAbortMutationSubject {
                route_cluster_epoch: self.route_cluster_epoch,
                bucket: &self.bucket,
                key: &self.key,
                upload_id: &self.upload_id,
            },
            Some(request.authorized_upload.record()),
            request.expected_cleanup,
            request.bucket_write_reservation,
            "build authorized abort multipart upload command",
        )?;
        let rpc_request = StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            authorized_upload: request.authorized_upload.record().clone(),
            expected_cleanup: request.expected_cleanup.cloned(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload = encode_authorized_abort_multipart_command_build_request(&rpc_request)
            .map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode authorized abort multipart command build request",
                    error.to_string(),
                ))
            })?;
        self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode authorized abort multipart command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client.validate_abort_multipart_command_response(
                    command,
                    &AbortMultipartCommandValidation {
                        pg_id: self.pg_id,
                        cluster_epoch: self.route_cluster_epoch,
                        bucket: &self.bucket,
                        key: &self.key,
                        upload_id: &self.upload_id,
                        expected_cleanup: request.expected_cleanup,
                        bucket_write_reservation: request.bucket_write_reservation,
                    },
                )
            },
        )
    }
}

impl ObjectPayloadReclaimMetadataRoute for UnixObjectPayloadReclaimMetadataRoute<'_> {
    fn exists(&self) -> Result<bool, ObjectPgActionError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            generation_id: self.generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimExists, payload)
            .map_err(ObjectPgActionError::Store)?;
        decode_metadata_command_bool_response(&response)
            .map(|response| response.value)
            .map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode object payload reclaim exists response",
                    error.to_string(),
                ))
            })
    }

    fn load_payload(&self) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let request = StorageRpcObjectPayloadReclaimExistsRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            generation_id: self.generation_id,
        };
        let payload = encode_object_payload_reclaim_exists_request(&request);
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimLoad, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_object_payload_reclaim_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(
                self.client
                    .rpc_payload_error("decode object payload reclaim response", error.to_string()),
            )
        })?;
        if !reclaim_matches_bucket_key_generation(
            response.reclaim.as_ref(),
            &self.bucket,
            &self.key,
            self.generation_id,
        ) {
            return Err(BucketSnapshotLoadError::Store(
                self.client.rpc_payload_error(
                    "validate object payload reclaim response",
                    "response reclaim payload does not match route".to_string(),
                ),
            ));
        }
        Ok(response.reclaim)
    }

    fn acquire_claim(
        &self,
        request: AcquireObjectPayloadReclaimClaimReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        let rpc_request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            bucket_incarnation_generation: request.bucket_incarnation_generation,
            generation_id: self.generation_id,
            reclaim_kind: request.reclaim_kind,
            claim_id: request.claim_id.to_string(),
            owner_token: request.owner_token.to_string(),
            claimed_at: request.claimed_at,
            lease_deadline: request.lease_deadline,
            now: request.now,
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_object_payload_reclaim_claim_acquire_request(&rpc_request).map_err(|error| {
                BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                    "encode object payload reclaim claim acquire request",
                    error.to_string(),
                ))
            })?;
        let response = self.client.rpc_request_bucket_snapshot(
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
            payload,
        )?;
        let response = decode_object_payload_reclaim_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                    "decode object payload reclaim claim acquire response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.bucket != self.bucket
                || record.bucket_incarnation_generation != request.bucket_incarnation_generation
                || record.key != self.key
                || record.generation_id != self.generation_id
                || record.reclaim_kind != request.reclaim_kind
                || record.claim_id != request.claim_id
                || record.owner_token != request.owner_token
                || record.cluster_epoch != self.route_cluster_epoch
                || record.pg_id != self.pg_id.get()
                || record.claimed_at != request.claimed_at
                || record.lease_deadline != request.lease_deadline
            {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate object payload reclaim claim acquire response",
                        "claim response identity does not match route request".to_string(),
                    ),
                ));
            }
        }
        Ok(response.record)
    }

    fn build_delete_object_payload_reclaim_command(
        &self,
        request: BuildDeleteObjectPayloadReclaimCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        require_object_payload_reclaim_command_subject(
            self.route_cluster_epoch,
            self.pg_id,
            &self.bucket,
            &self.key,
            self.generation_id,
            &request,
        )?;
        let rpc_request = StorageRpcObjectPayloadReclaimCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            generation_id: self.generation_id,
            payload: request.payload.clone(),
            claim: request.claim.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_object_payload_reclaim_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode object payload reclaim command build request",
                    error.to_string(),
                ))
            })?;
        let command = self
            .client
            .object_metadata_command_build_request(
                StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild,
                self.pg_id.pg_id(),
                payload,
                "decode object payload reclaim command build response",
                ObjectPgActionError::StaleObjectReadSubject,
                |command| {
                    let valid = command.id().cluster_epoch() == self.route_cluster_epoch
                        && command.id().pg_id() == self.pg_id.pg_id()
                        && matches!(
                            command.payload(),
                            MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                                if delete.matches_request(
                                    &self.bucket,
                                    &self.key,
                                    self.generation_id,
                                )
                                    && delete.payload == *request.payload
                                    && delete.reclaim_claim
                                        == ObjectPayloadReclaimClaimProof::from(request.claim)
                        );
                    if valid {
                        Ok(())
                    } else {
                        Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                            "validate object payload reclaim command build response",
                            "response command does not match requested reclaim subject".to_string(),
                        )))
                    }
                },
            )?
            .ok_or_else(|| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode object payload reclaim command build response",
                    "object payload reclaim command build cannot return missing".to_string(),
                ))
            })?;
        Ok(command)
    }
}

impl UnixMultipartUploadLookupMetadataRoute<'_> {
    fn object_request(&self) -> StorageRpcObjectRequest {
        self.client
            .object_request(self.pg_id.pg_id(), &self.bucket, &self.key)
    }

    fn load_multipart_upload_with_kind(
        &self,
        upload_id: &UploadId,
        kind: StorageRpcMessageKind,
        expected_state: Option<UploadState>,
        decode_operation: &'static str,
        validate_operation: &'static str,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .client
            .rpc_request(kind, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_upload_load_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error(decode_operation, error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
                self.client.validate_multipart_upload_response(
                    &upload,
                    &self.bucket,
                    &self.key,
                    upload_id,
                    expected_state,
                    validate_operation,
                )?;
                Ok(*upload)
            }
            StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != *upload_id {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        validate_operation,
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
}

impl MultipartUploadLookupMetadataRoute for UnixMultipartUploadLookupMetadataRoute<'_> {
    fn load_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        self.load_multipart_upload_with_kind(
            upload_id,
            StorageRpcMessageKind::ObjectMultipartUploadLoad,
            None,
            "decode multipart upload load response",
            "validate multipart upload load response",
        )
        .map_err(|error| match error {
            ObjectPgActionError::Store(store) => BucketSnapshotLoadError::Store(store),
            ObjectPgActionError::Metadata(metadata) => BucketSnapshotLoadError::Metadata(metadata),
            other => {
                BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                    "validate multipart upload load response",
                    other.to_string(),
                ))
            }
        })
    }

    fn load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.load_multipart_upload_with_kind(
            upload_id,
            StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad,
            Some(UploadState::InProgress),
            "decode in-progress multipart upload load response",
            "validate in-progress multipart upload load response",
        )
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.load_multipart_upload_with_kind(
            upload_id,
            StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad,
            Some(UploadState::InProgress),
            "decode in-progress multipart upload listing response",
            "validate in-progress multipart upload listing response",
        )
    }

    fn lookup_multipart_upload_management(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let request = StorageRpcMultipartUploadLoadRequest {
            object: self.object_request(),
            upload_id: upload_id.clone(),
        };
        let payload = encode_multipart_upload_load_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartManagementLookup,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_management_lookup_response(&response).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode multipart management lookup response",
                error.to_string(),
            ))
        })?;
        self.client.validate_multipart_management_lookup_response(
            &response.lookup,
            &self.bucket,
            &self.key,
            upload_id,
        )?;
        Ok(response.lookup)
    }
}

impl AuthorizedMultipartUploadMetadataRoute for UnixAuthorizedMultipartUploadMetadataRoute<'_> {
    fn load_multipart_completion_snapshot(
        &self,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let upload = self.authorized_upload.record();
        let request = StorageRpcMultipartCompletionSnapshotRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &upload.bucket, &upload.key),
            authorized_upload: upload.clone(),
            requested_part_numbers: requested_part_numbers.to_vec(),
        };
        let payload = encode_multipart_completion_snapshot_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "encode multipart completion snapshot request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_completion_snapshot_response(
            &response,
            crate::types::MultipartCompletionSubject::from_upload(upload),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode multipart completion snapshot response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcMultipartCompletionSnapshotOutcome::Loaded(snapshot) => {
                self.client
                    .validate_multipart_completion_snapshot_response(
                        &snapshot,
                        &self.authorized_upload,
                        requested_part_numbers,
                    )?;
                Ok(*snapshot)
            }
            StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != upload.upload_id {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload.upload_id.to_string(),
                }
                .into())
            }
            StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: returned_upload_id,
                part_number,
            } => {
                if returned_upload_id != upload.upload_id {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part upload id does not match request".to_string(),
                    )));
                }
                if !requested_part_numbers.contains(&part_number) {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "validate multipart completion snapshot response",
                        "missing part number does not match request".to_string(),
                    )));
                }
                Err(MetadataError::PartNotFound {
                    upload_id: upload.upload_id.to_string(),
                    part_number,
                }
                .into())
            }
        }
    }

    fn load_multipart_completion_preflight(
        &self,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let upload = self.authorized_upload.record();
        let request = StorageRpcMultipartCompletionPreflightRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &upload.bucket, &upload.key),
            authorized_upload: upload.clone(),
        };
        let payload = encode_multipart_completion_preflight_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "encode multipart completion preflight request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_multipart_completion_preflight_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode multipart completion preflight response",
                    error.to_string(),
                ))
            })?;
        match response.outcome {
            StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight) => Ok(preflight),
            StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != upload.upload_id {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "validate multipart completion preflight response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload.upload_id.to_string(),
                }
                .into())
            }
        }
    }

    fn list_multipart_parts(
        &self,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let upload = self.authorized_upload.record();
        let request = StorageRpcMultipartPartsListRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &upload.bucket, &upload.key),
            authorized_upload: upload.clone(),
            part_number_marker,
            max_parts,
        };
        let payload = encode_multipart_parts_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("encode multipart parts list request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectMultipartPartsList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_multipart_parts_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("decode multipart parts list response", error.to_string()),
            )
        })?;
        match response.outcome {
            StorageRpcMultipartPartsListOutcome::Loaded(listed) => {
                self.client.validate_listed_multipart_parts_response(
                    &listed,
                    &self.authorized_upload,
                    part_number_marker,
                    max_parts,
                )?;
                Ok(*listed)
            }
            StorageRpcMultipartPartsListOutcome::NoSuchUpload {
                upload_id: returned_upload_id,
            } => {
                if returned_upload_id != upload.upload_id {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                        "validate multipart parts list response",
                        "missing upload id does not match request".to_string(),
                    )));
                }
                Err(MetadataError::NoSuchUpload {
                    upload_id: upload.upload_id.to_string(),
                }
                .into())
            }
        }
    }
}

impl UnixStreamUploadCreationMetadataRoute<'_> {
    fn require_create_subject(
        &self,
        create: &CreateStreamUploadReq,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if create.bucket != self.bucket || create.key != self.key {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_expected_command_subject(
        &self,
        create: &CreateStreamUploadReq,
        command: &CreateStreamUploadCommand,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let expected_operation_kind = match create.target {
            StreamUploadTarget::PutObject => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            StreamUploadTarget::UploadPart { .. } => {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
        };
        if !command.matches_request(create)
            || !command
                .bucket_write_reservation
                .matches_exact_mutation_subject(
                    self.route_cluster_epoch,
                    &self.bucket,
                    expected_operation_kind,
                    Some(self.key.as_str()),
                )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
        }
        Ok(())
    }

    fn require_build_subject(
        &self,
        request: &BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        self.require_create_subject(request.request, "build create stream upload command")?;
        let expected_operation_kind = match (&request.request.target, &request.precondition) {
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObjectNoCurrentCheck { .. },
            ) => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            (
                StreamUploadTarget::PutObject,
                CreateStreamUploadPrecondition::PutObject {
                    expected_current, ..
                },
            ) if expected_current.is_none_or(|stored| {
                stored.bucket() == &self.bucket && stored.key() == &self.key
            }) =>
            {
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            (
                StreamUploadTarget::UploadPart { upload_id, .. },
                CreateStreamUploadPrecondition::UploadPart { expected_upload },
            ) if expected_upload.bucket == self.bucket
                && expected_upload.key == self.key
                && expected_upload.upload_id == *upload_id =>
            {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            _ => {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "build create stream upload command",
                }
                .into());
            }
        };
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                expected_operation_kind,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create stream upload command",
            }
            .into());
        }
        Ok(())
    }
}

impl StreamUploadCreationMetadataRoute for UnixStreamUploadCreationMetadataRoute<'_> {
    fn matching_stream_upload_exists(
        &self,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        self.require_create_subject(create, "match stream upload creation")?;
        if let Some(command) = expected_command {
            self.require_expected_command_subject(create, command, "match stream upload creation")?;
        }
        let request = StorageRpcStreamUploadMatchRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            request: create.clone(),
            expected_command: expected_command.cloned(),
        };
        let payload = encode_stream_upload_match_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("encode stream upload match request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadMatch, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_upload_match_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("decode stream upload match response", error.to_string()),
            )
        })?;
        self.client
            .validate_stream_upload_match_response(response.exists, expected_command)?;
        Ok(response.exists)
    }

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.require_build_subject(&request)?;
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
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            request: request.request.clone(),
            cleanup_after: request.cleanup_after,
            precondition,
            bucket_write_reservation: request.bucket_write_reservation.clone(),
        };
        let payload =
            encode_create_stream_upload_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode stream upload command build request",
                    error.to_string(),
                ))
            })?;
        match self.client.object_metadata_command_build_request(
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild,
            self.pg_id.pg_id(),
            payload,
            "decode stream upload command build response",
            ObjectPgActionError::StaleObjectReadSubject,
            |command| {
                self.client.validate_create_stream_upload_command_response(
                    command,
                    self.pg_id,
                    self.route_cluster_epoch,
                    &request,
                )
            },
        )? {
            Some(command) => Ok(command),
            None if matches!(
                request.precondition,
                CreateStreamUploadPrecondition::UploadPart { .. }
            ) =>
            {
                let StreamUploadTarget::UploadPart { upload_id, .. } = &request.request.target
                else {
                    return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
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
            None => Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode stream upload command build response",
                "stream upload command build cannot return missing".to_string(),
            ))),
        }
    }
}

impl UnixStreamUploadSessionMetadataRoute<'_> {
    fn require_put_reservation_subject(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), ObjectPgActionError> {
        // The active route separately authorizes this RPC at the current
        // epoch. A durable stream reservation may retain its creation epoch
        // across route transitions, so validate its stable subject here.
        if proof.bucket != self.bucket
            || proof.operation_kind != PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            || proof.target_context.as_deref() != Some(self.key.as_str())
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "update stream upload bucket write reservation",
            }
            .into());
        }
        Ok(())
    }
}

impl StreamUploadSessionMetadataRoute for UnixStreamUploadSessionMetadataRoute<'_> {
    fn load_session(&self) -> Result<StreamUploadRecord, ObjectPgActionError> {
        self.client.load_stream_upload_session_rpc(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.session_id,
        )
    }

    fn load_segments(&self) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        self.client.load_stream_upload_segments_rpc(
            self.pg_id,
            &self.bucket,
            &self.key,
            &self.session_id,
        )
    }

    fn prepare_segment_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if request.session_id != self.session_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "prepare stream segment append",
            }
            .into());
        }
        let effect_deadline =
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                });
        self.client.prepare_stream_segment_append_rpc(
            self.pg_id,
            &self.bucket,
            &self.key,
            request,
            effect_deadline,
        )
    }

    fn update_put_bucket_write_reservation(
        &self,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        self.require_put_reservation_subject(current)?;
        self.require_put_reservation_subject(renewed)?;
        if !current.has_same_stable_identity(renewed) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "update stream upload bucket write reservation",
            }
            .into());
        }
        let request = StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            session_id: self.session_id.clone(),
            current: current.clone(),
            renewed: renewed.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload = encode_stream_upload_bucket_write_reservation_update_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        self.client
            .validate_empty_bucket_write_reservation_response(
                "decode fenced stream upload bucket write reservation update response",
                &response,
            )
            .map_err(|error| match error {
                BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
                BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
            })
    }
}

impl StreamPutFinalizationMetadataRoute for UnixStreamPutFinalizationMetadataRoute<'_> {
    fn load_snapshot(&self) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcStreamPutFinalizeSnapshotRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            session_id: self.session_id.clone(),
        };
        let payload = encode_stream_put_finalize_snapshot_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_put_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode stream PUT finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.client.validate_stream_put_finalize_snapshot_response(
            &response.snapshot,
            &self.bucket,
            &self.key,
            &self.session_id,
        )?;
        Ok(response.snapshot)
    }

    fn build_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build stream PUT commit command",
            }
            .into());
        }
        let rpc_request = StorageRpcStreamPutCommitCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            session_id: self.session_id.clone(),
            total_size: request.total_size,
            expected_snapshot: request.expected_snapshot.clone(),
            commit: request.commit.clone(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_stream_put_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode stream PUT commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_metadata_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode stream PUT commit command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.client.validate_stream_put_commit_command_response(
                    &command,
                    &rpc_request,
                    &request,
                )?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode stream PUT commit command build response",
                    "stream PUT commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.client.metadata_command_log_conflict_error(
                self.pg_id.pg_id(),
                "decode stream PUT commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }
}

impl StreamPartFinalizationMetadataRoute for UnixStreamPartFinalizationMetadataRoute<'_> {
    fn load_snapshot(&self) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcStreamPartFinalizeSnapshotRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            upload_id: self.upload_id.clone(),
            session_id: self.session_id.clone(),
            part_number: self.part_number,
        };
        let payload = encode_stream_part_finalize_snapshot_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_stream_part_finalize_snapshot_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode stream part finalize snapshot response",
                    error.to_string(),
                ))
            })?;
        self.client
            .validate_stream_part_finalize_snapshot_response(
                &response.snapshot,
                &self.bucket,
                &self.key,
                &self.upload_id,
                &self.session_id,
                self.part_number,
            )?;
        Ok(response.snapshot)
    }

    fn build_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        effect_fence.require_valid_for(self.route_cluster_epoch)?;
        if !request
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                self.route_cluster_epoch,
                &self.bucket,
                UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build stream part commit command",
            }
            .into());
        }
        let rpc_request = StorageRpcStreamPartCommitCommandBuildRequest {
            object: self
                .client
                .object_request(self.pg_id.pg_id(), &self.bucket, &self.key),
            upload_id: self.upload_id.clone(),
            session_id: self.session_id.clone(),
            part_number: self.part_number,
            expected_snapshot: request.expected_snapshot.clone(),
            part: request.part.clone(),
            segments: request.segments.to_vec(),
            bucket_write_reservation: request.bucket_write_reservation.clone(),
            effect_deadline: effect_fence.deadline().map(|deadline| {
                StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }
            }),
        };
        let payload =
            encode_stream_part_commit_command_build_request(&rpc_request).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "encode stream part commit command build request",
                    error.to_string(),
                ))
            })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_metadata_command_build_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "decode stream part commit command build response",
                error.to_string(),
            ))
        })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                self.client.validate_stream_part_commit_command_response(
                    &command,
                    &rpc_request,
                    &request,
                )?;
                Ok(*command)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => {
                Err(ObjectPgActionError::StaleStreamFinalizeSnapshot)
            }
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => {
                Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode stream part commit command build response",
                    "stream part commit command build cannot return missing".to_string(),
                )))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.client.metadata_command_log_conflict_error(
                self.pg_id.pg_id(),
                "decode stream part commit command build response",
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }
}

impl ObjectMutationMetadataNodeClient for UnixStorageNodeClient {
    fn open_put_object_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn PutObjectMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixPutObjectMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadCreationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixMultipartUploadCreationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_upload_lookup_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadLookupMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixMultipartUploadLookupMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_authorized_multipart_upload_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Box<dyn AuthorizedMultipartUploadMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixAuthorizedMultipartUploadMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorized_upload: authorized_upload.clone(),
        }))
    }

    fn open_multipart_completion_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartCompletionMutationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixMultipartCompletionMutationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_abort_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Box<dyn MultipartAbortMutationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixMultipartAbortMutationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
        }))
    }

    fn open_object_payload_reclaim_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadReclaimMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixObjectPayloadReclaimMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }))
    }

    fn open_stream_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn StreamUploadCreationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixStreamUploadCreationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_stream_upload_session_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamUploadSessionMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixStreamUploadSessionMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
        }))
    }

    fn open_stream_put_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamPutFinalizationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixStreamPutFinalizationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
        }))
    }

    fn open_stream_part_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<Box<dyn StreamPartFinalizationMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixStreamPartFinalizationMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            session_id: session_id.clone(),
            part_number,
        }))
    }

    fn open_object_mutation_scan_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ObjectMutationScanMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        let pg_topology = self.pg_topology.as_ref().ok_or_else(|| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                "open object mutation scan metadata route",
                "object mutation scan client has no installed PG topology".to_string(),
            ))
        })?;
        Ok(Box::new(UnixObjectMutationScanMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            pg_topology: Arc::clone(pg_topology),
        }))
    }

    fn open_object_delete_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectDeleteMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixObjectDeleteMetadataRoute {
            client: self,
            route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }
}

impl UnixObjectMutationScanMetadataRoute<'_> {
    fn require_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation: &'static str,
    ) -> Result<(), StoreError> {
        if self.pg_topology.object_pg_for(bucket, key) != self.pg_id.get() {
            return Err(self.client.rpc_payload_error(
                operation,
                "response subject does not belong to the scoped object scan PG".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_stream_page(
        &self,
        response: &StorageRpcStreamUploadsListResponse,
        expected_bucket: Option<&BucketName>,
        session_id_marker: Option<&SessionId>,
        limit: u32,
        operation: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if response.uploads.len() > limit as usize {
            return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                operation,
                "response exceeded requested page limit".to_string(),
            )));
        }
        for upload in &response.uploads {
            if expected_bucket.is_some_and(|bucket| bucket != &upload.bucket) {
                return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    operation,
                    "upload bucket does not match request".to_string(),
                )));
            }
            self.require_subject(&upload.bucket, &upload.key, operation)?;
            if session_id_marker.is_some_and(|marker| upload.session_id.as_str() <= marker.as_str())
            {
                return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    operation,
                    "upload is not after requested marker".to_string(),
                )));
            }
        }
        if response
            .uploads
            .windows(2)
            .any(|pair| pair[0].session_id.as_str() >= pair[1].session_id.as_str())
        {
            return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                operation,
                "uploads are not strictly ordered by session id".to_string(),
            )));
        }
        if response.next_session_id_marker.as_ref()
            != response.uploads.last().map(|upload| &upload.session_id)
            && response.next_session_id_marker.is_some()
        {
            return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                operation,
                "next marker does not match the last returned upload".to_string(),
            )));
        }
        Ok(())
    }

    fn validate_root(
        &self,
        root: &PayloadReclaimRoot,
        expected_bucket: Option<&BucketName>,
        operation: &'static str,
    ) -> Result<(), BucketSnapshotLoadError> {
        if expected_bucket.is_some_and(|bucket| bucket != &root.bucket) {
            return Err(BucketSnapshotLoadError::Store(
                self.client
                    .rpc_payload_error(operation, "root bucket does not match request".to_string()),
            ));
        }
        self.require_subject(&root.bucket, &root.key, operation)?;
        Ok(())
    }
}

impl ObjectMutationScanMetadataRoute for UnixObjectMutationScanMetadataRoute<'_> {
    fn list_aborting_multipart_upload_bucket_witnesses(
        &self,
    ) -> Result<Vec<AbortingMultipartUploadBucketWitness>, ObjectPgActionError> {
        let request = StorageRpcBucketPgRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
        };
        let payload = encode_bucket_pg_request(&request).map_err(|error| {
            ObjectPgActionError::Store(self.client.rpc_payload_error(
                "encode aborting multipart upload buckets request",
                error.to_string(),
            ))
        })?;
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_aborting_multipart_upload_buckets_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "decode aborting multipart upload buckets response",
                    error.to_string(),
                ))
            })?;
        let mut buckets = BTreeSet::new();
        for witness in &response.witnesses {
            self.require_subject(
                &witness.bucket,
                &witness.key,
                "validate aborting multipart upload buckets response",
            )?;
            if !buckets.insert(&witness.bucket) {
                return Err(ObjectPgActionError::Store(self.client.rpc_payload_error(
                    "validate aborting multipart upload buckets response",
                    "response contains a duplicate bucket witness".to_string(),
                )));
            }
        }
        Ok(response.witnesses)
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: self.client.node_id,
                cluster_epoch: self.route_cluster_epoch,
                pg_id: self.pg_id.pg_id(),
                bucket: bucket.clone(),
            },
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("encode stream uploads list request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request_with_admission_class(
                StorageRpcMessageKind::ObjectStreamUploadsList,
                payload,
                listing_probe_admission_class(limit),
            )
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("decode stream uploads list response", error.to_string()),
            )
        })?;
        self.validate_stream_page(
            &response,
            Some(bucket),
            session_id_marker,
            limit,
            "validate stream uploads list response",
        )?;
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn list_all_stream_uploads_page(
        &self,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError> {
        let request = StorageRpcStreamUploadsPgListRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            session_id_marker: session_id_marker.cloned(),
            limit,
        };
        let payload = encode_stream_uploads_pg_list_request(&request).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("encode stream uploads PG list request", error.to_string()),
            )
        })?;
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectStreamUploadsPgList, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_stream_uploads_list_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.client
                    .rpc_payload_error("decode stream uploads PG list response", error.to_string()),
            )
        })?;
        self.validate_stream_page(
            &response,
            None,
            session_id_marker,
            limit,
            "validate stream uploads PG list response",
        )?;
        Ok(StreamUploadRecordPage {
            uploads: response.uploads,
            next_session_id_marker: response.next_session_id_marker,
        })
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let request = StorageRpcBucketRequest {
            node_id: self.client.node_id,
            cluster_epoch: self.route_cluster_epoch,
            pg_id: self.pg_id.pg_id(),
            bucket: bucket.clone(),
        };
        let payload = encode_bucket_request(&request);
        let response = self
            .client
            .rpc_request(
                StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
                payload,
            )
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                "decode object bucket payload reclaim root response",
                error.to_string(),
            ))
        })?;
        if let Some(root) = &response.root {
            self.validate_root(root, Some(bucket), "validate bucket payload reclaim root")?;
        }
        Ok(response.root)
    }

    fn get_payload_reclaim_root(
        &self,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let payload = self
            .client
            .encode_metadata_command_state_request(self.pg_id.pg_id());
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimRoot, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_payload_reclaim_root_response(&response).map_err(|error| {
            BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                "decode object payload reclaim root response",
                error.to_string(),
            ))
        })?;
        if let Some(root) = &response.root {
            self.validate_root(root, None, "validate PG payload reclaim root")?;
        }
        Ok(response.root)
    }

    fn object_payload_reclaim_claim(
        &self,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError> {
        let payload = self
            .client
            .encode_metadata_command_state_request(self.pg_id.pg_id());
        let response = self
            .client
            .rpc_request(StorageRpcMessageKind::ObjectPayloadReclaimClaimGet, payload)
            .map_err(BucketSnapshotLoadError::Store)?;
        let response = decode_object_payload_reclaim_claim_optional_record_response(&response)
            .map_err(|error| {
                BucketSnapshotLoadError::Store(self.client.rpc_payload_error(
                    "decode object payload reclaim claim get response",
                    error.to_string(),
                ))
            })?;
        if let Some(record) = &response.record {
            if record.pg_id != self.pg_id.get() {
                return Err(BucketSnapshotLoadError::Store(
                    self.client.rpc_payload_error(
                        "validate object payload reclaim claim get response",
                        "claim response PG does not match route".to_string(),
                    ),
                ));
            }
            self.require_subject(
                &record.bucket,
                &record.key,
                "validate object payload reclaim claim get response",
            )?;
        }
        Ok(response.record)
    }
}

impl UnixStorageNodeClient {
    fn load_object_read_auth_subject(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let request = StorageRpcObjectReadAuthSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: self.node_id,
                cluster_epoch: self.cluster_epoch,
                pg_id: pg_id.pg_id(),
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
        pg_id: ObjectMetadataPgId,
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
                pg_id: pg_id.pg_id(),
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
}

impl ObjectReadMetadataNodeClient for UnixStorageNodeClient {
    fn open_object_read_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn ObjectReadMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixObjectReadMetadataRoute {
            client: self,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_multipart_upload_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn MultipartUploadLookupMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixMultipartUploadLookupMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }))
    }

    fn open_authorized_multipart_upload_read_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        _authorization: MetadataReadAuthorization,
    ) -> Result<Box<dyn AuthorizedMultipartUploadMetadataRoute + '_>, ObjectPgActionError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            }
            .into());
        }
        Ok(Box::new(UnixAuthorizedMultipartUploadMetadataRoute {
            client: self,
            _route_cluster_epoch: route_cluster_epoch,
            pg_id,
            authorized_upload: authorized_upload.clone(),
        }))
    }
}

impl ObjectReadMetadataRoute for UnixObjectReadMetadataRoute<'_> {
    fn load_object_read_auth_subject(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        self.client
            .load_object_read_auth_subject(self.pg_id, &self.bucket, &self.key, version_id)
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        self.client.load_object_read_snapshot_for_subject(
            self.pg_id,
            &self.bucket,
            &self.key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }
}
