use super::*;

impl UnixStorageNodeClient {
    pub(super) fn bucket_pg_request(&self, pg_id: PgId) -> StorageRpcBucketPgRequest {
        StorageRpcBucketPgRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        }
    }

    pub(super) fn object_request(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> StorageRpcObjectRequest {
        StorageRpcObjectRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            bucket: bucket.clone(),
            key: key.clone(),
        }
    }

    pub(super) fn validate_stream_upload_session_response(
        &self,
        session: &StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if session.session_id != *session_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream session id does not match request".to_string(),
            )));
        }
        validate_stream_upload_session_binding(session, bucket, key).map_err(|error| {
            ObjectPgActionError::Store(self.rpc_payload_error(context, error.to_string()))
        })
    }

    pub(super) fn validate_stream_upload_segments_response(
        &self,
        segments: &[StreamUploadSegmentRecord],
        session_id: &SessionId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let mut seen = BTreeSet::new();
        for segment in segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "stream segment session id does not match request".to_string(),
                )));
            }
            if !seen.insert(segment.segment_index) {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "duplicate stream segment index in response".to_string(),
                )));
            }
        }
        if !segments
            .windows(2)
            .all(|pair| pair[0].segment_index < pair[1].segment_index)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segments are not strictly ascending".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_stream_segment_append_prepare_response(
        &self,
        segment: &StreamUploadSegmentRecord,
        returned_target: &StreamUploadTarget,
        expected_target: &StreamUploadTarget,
        request: &PrepareStreamUploadSegmentAppendReq,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if returned_target != expected_target {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segment append target does not match session".to_string(),
            )));
        }
        if segment.session_id != request.session_id
            || segment.segment_index != request.segment_index
            || segment.size != request.size
            || segment.segment_crc64 != request.segment_crc64
            || segment.payload_crc64 != request.payload_crc64
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream segment append response does not match request".to_string(),
            )));
        }
        if matches!(expected_target, StreamUploadTarget::UploadPart { .. })
            && segment.segment_okh != request.segment_okh
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stream upload-part segment OKH does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn load_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        kind: StorageRpcMessageKind,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError> {
        let request = StorageRpcObjectDeleteSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id,
        };
        let payload = encode_object_delete_snapshot_request(&request);
        let response = self
            .rpc_request(kind, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response = decode_object_delete_snapshot_response(&response).map_err(|error| {
            ObjectPgActionError::Store(
                self.rpc_payload_error("decode object delete snapshot response", error.to_string()),
            )
        })?;
        if let Some(stored) = response.stored.as_ref() {
            self.validate_stored_object_response(
                stored,
                bucket,
                key,
                version_id,
                "validate object delete snapshot response",
            )?;
        }
        if let Some(target) = response.target.as_ref() {
            self.validate_delete_target_response(
                target,
                bucket,
                key,
                "validate object delete snapshot response",
            )?;
        }
        if !self.delete_snapshot_target_matches_stored(&response) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object delete snapshot response",
                "delete target does not match snapshot object".to_string(),
            )));
        }
        Ok(ObjectDeleteStorageSnapshot {
            stored: response.stored,
            target: response.target,
        })
    }

    pub(super) fn load_object_lifecycle_version_list(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let request = StorageRpcObjectDeleteSnapshotRequest {
            object: self.object_request(pg_id, bucket, key),
            version_id: None,
        };
        let payload = encode_object_delete_snapshot_request(&request);
        let response = self
            .rpc_request(
                StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
                payload,
            )
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_lifecycle_version_list_response(&response).map_err(|error| {
                ObjectPgActionError::Store(self.rpc_payload_error(
                    "decode object lifecycle version list response",
                    error.to_string(),
                ))
            })?;
        for stored in &response.versions {
            self.validate_stored_object_response(
                stored,
                bucket,
                key,
                None,
                "validate object lifecycle version list response",
            )?;
        }
        Ok(response.versions)
    }

    pub(super) fn validate_delete_target_response(
        &self,
        target: &DeleteObjectVersionTarget,
        bucket: &BucketName,
        key: &ObjectKey,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => Ok(()),
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                payload,
            } => {
                let payload_matches = match (payload, layout) {
                    (ObjectPayloadReclaimCommand::Segments(reclaim), ObjectLayout::Standard) => {
                        reclaim.generation_id == *generation_id
                    }
                    (
                        ObjectPayloadReclaimCommand::Multipart(reclaim),
                        ObjectLayout::MultipartManifest { .. },
                    ) => reclaim.generation_id == *generation_id,
                    _ => false,
                };
                if !reclaim_matches_bucket_key(Some(payload), bucket, key) || !payload_matches {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "delete target payload does not match request".to_string(),
                    )));
                }
                Ok(())
            }
        }
    }

    pub(super) fn delete_snapshot_target_matches_stored(
        &self,
        response: &StorageRpcObjectDeleteSnapshotResponse,
    ) -> bool {
        match (&response.stored, &response.target) {
            (None, None) => true,
            (
                Some(StoredObject::DeleteMarker(_)),
                Some(DeleteObjectVersionTarget::DeleteMarker { .. }),
            ) => true,
            (
                Some(StoredObject::Live(live)),
                Some(DeleteObjectVersionTarget::Live {
                    generation_id,
                    layout,
                    payload,
                }),
            ) => {
                *generation_id == live.generation_id
                    && *layout == live.layout
                    && reclaim_matches_bucket_key(Some(payload), &live.bucket, &live.key)
            }
            _ => false,
        }
    }

    pub(super) fn object_metadata_command_build_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        payload: Vec<u8>,
        decode_context: &'static str,
        stale_snapshot_error: ObjectPgActionError,
        validate: impl FnOnce(&MetadataCommandEnvelope) -> Result<(), ObjectPgActionError>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let response = self
            .rpc_request(kind, payload)
            .map_err(ObjectPgActionError::Store)?;
        let response =
            decode_object_metadata_command_build_response(&response).map_err(|error| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error(decode_context, error.to_string()),
                )
            })?;
        match response.outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
                validate(&command)?;
                Ok(Some(*command))
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => Err(stale_snapshot_error),
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => Ok(None),
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(self.metadata_command_log_conflict_error(
                pg_id,
                decode_context,
                node_id,
                conflict_pg_id,
                cluster_epoch,
                log_index,
            )),
        }
    }

    pub(super) fn metadata_command_log_conflict_error(
        &self,
        pg_id: PgId,
        decode_context: &'static str,
        node_id: u32,
        conflict_pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> ObjectPgActionError {
        if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
            return ObjectPgActionError::Store(self.rpc_payload_error(
                decode_context,
                "metadata command log conflict route mismatch".to_string(),
            ));
        }
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return ObjectPgActionError::Store(self.rpc_payload_error(
                decode_context,
                "metadata command log conflict index must not be zero".to_string(),
            ));
        }
        ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: conflict_pg_id,
            cluster_epoch,
            log_index,
        })
    }

    pub(super) fn validate_stream_upload_match_response(
        &self,
        exists: bool,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<(), ObjectPgActionError> {
        if exists && expected_command.is_none() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate stream upload match response",
                "positive stream upload match requires expected command".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_multipart_upload_match_response(
        &self,
        initiated_at: Option<u64>,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<(), ObjectPgActionError> {
        let Some(initiated_at) = initiated_at else {
            return Ok(());
        };
        let Some(expected_command) = expected_command else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate multipart upload match response",
                "positive multipart upload match requires expected command".to_string(),
            )));
        };
        if initiated_at != expected_command.upload.initiated_at {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate multipart upload match response",
                "multipart upload match timestamp does not match expected command".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_multipart_upload_response(
        &self,
        upload: &MultipartUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_state: Option<UploadState>,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if upload.bucket != *bucket
            || upload.key != *key
            || upload.upload_id != *upload_id
            || expected_state.is_some_and(|state| upload.state != state)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "multipart upload identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_multipart_completion_snapshot_response(
        &self,
        snapshot: &MultipartCompletionSnapshot,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart completion snapshot response";
        let upload = authorized_upload.record();
        let requested = requested_part_numbers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if snapshot.part_records.len() != requested_part_numbers.len() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot selected part count does not match request".to_string(),
            )));
        }
        for (part, requested_part_number) in snapshot
            .part_records
            .iter()
            .zip(requested_part_numbers.iter())
        {
            if part.upload_id != upload.upload_id || part.part_number != *requested_part_number {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot selected part identity does not match request".to_string(),
                )));
            }
        }
        for segment in &snapshot.selected_streaming_segments {
            if segment.bucket != upload.bucket
                || segment.key != upload.key
                || segment.upload_id != upload.upload_id
                || !requested.contains(&segment.part_number)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot selected segment identity does not match request".to_string(),
                )));
            }
        }
        for part in &snapshot.cleanup.omitted_parts {
            if part.upload_id != upload.upload_id || requested.contains(&part.part_number) {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot omitted part identity does not match request".to_string(),
                )));
            }
        }
        for segment in &snapshot.cleanup.omitted_streaming_segments {
            if segment.bucket != upload.bucket
                || segment.key != upload.key
                || segment.upload_id != upload.upload_id
                || requested.contains(&segment.part_number)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot omitted segment identity does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &snapshot.cleanup.stream_uploads,
            &snapshot.cleanup.stream_upload_segments,
            &upload.bucket,
            &upload.key,
            &upload.upload_id,
            context,
        )?;
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            let Some(live) = source.as_live() else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot stale payload source is not live".to_string(),
                )));
            };
            if live.bucket != upload.bucket || live.key != upload.key || !live.version_id.is_null()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot stale payload source identity does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_listed_multipart_parts_response(
        &self,
        listed: &ListedMultipartParts,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart parts list response";
        if listed.upload != *authorized_upload.record()
            || listed.response.parts.len() > max_parts as usize
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "listed multipart parts response shape does not match request".to_string(),
            )));
        }
        let mut previous_part_number = part_number_marker;
        for part in &listed.response.parts {
            if part.upload_id != authorized_upload.upload_id
                || previous_part_number.is_some_and(|marker| part.part_number <= marker)
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "listed multipart part identity does not match request".to_string(),
                )));
            }
            previous_part_number = Some(part.part_number);
        }
        match (
            listed.response.is_truncated,
            listed.response.next_part_number_marker,
            listed.response.parts.last(),
        ) {
            (false, None, _) => {}
            (false, Some(next_marker), None)
                if max_parts == 0 && next_marker == part_number_marker.unwrap_or(0) => {}
            (false, Some(_), _) => {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "non-truncated multipart parts response has next marker".to_string(),
                )));
            }
            (true, Some(next_marker), Some(last)) if next_marker == last.part_number => {}
            (true, _, _) => {
                return Err(ObjectPgActionError::Store(
                    self.rpc_payload_error(
                        context,
                        "truncated multipart parts response marker does not match last part"
                            .to_string(),
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_multipart_management_lookup_response(
        &self,
        lookup: &MultipartUploadManagementLookup,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart management lookup response";
        match lookup {
            MultipartUploadManagementLookup::InProgress(upload) => self
                .validate_multipart_upload_response(
                    upload,
                    bucket,
                    key,
                    upload_id,
                    Some(UploadState::InProgress),
                    context,
                ),
            MultipartUploadManagementLookup::NonInProgress(upload) => {
                if upload.bucket != *bucket
                    || upload.key != *key
                    || upload.upload_id != *upload_id
                    || upload.state == UploadState::InProgress
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "non-in-progress lookup identity does not match request".to_string(),
                    )));
                }
                Ok(())
            }
            MultipartUploadManagementLookup::Completed(completed) => {
                if completed.bucket != *bucket
                    || completed.key != *key
                    || completed.upload_id != *upload_id
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "completed lookup identity does not match request".to_string(),
                    )));
                }
                Ok(())
            }
            MultipartUploadManagementLookup::Missing => Ok(()),
        }
    }

    pub(super) fn validate_stored_object_response(
        &self,
        stored: &StoredObject,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if stored.bucket() != bucket
            || stored.key() != key
            || version_id.is_some_and(|version_id| stored.version_id() != version_id)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stored object identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_object_metadata_command_route(
        &self,
        command: &MetadataCommandEnvelope,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if command.id().cluster_epoch() != cluster_epoch || command.id().pg_id() != pg_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "command id route does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_put_object_metadata_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate object metadata PUT command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not object metadata PUT".to_string(),
            )));
        };
        let live = request
            .expected_stored
            .as_live()
            .ok_or(MetadataError::MethodNotAllowedOnDeleteMarker)?;
        let expected = PutObjectMetadataCommand::from_live_object_and_mutation(
            live.clone(),
            request.mutation.clone(),
            request.bucket_write_reservation.clone(),
        );
        if update.as_ref() != &expected || request.version_id != live.version_id {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_delete_specific_object_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate delete-specific object command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not object version delete".to_string(),
            )));
        };
        if delete.bucket != *request.bucket
            || delete.key != *request.key
            || delete.version_id != request.version_id
            || delete.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        self.validate_delete_target_response(&delete.target, request.bucket, request.key, context)?;
        if !delete_target_matches_expected(Some(&delete.target), request.expected_target) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response delete target does not match request snapshot".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_delete_current_object_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let Some(StoredObject::Live(expected)) = request.expected_current else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate delete-current object command build response",
                "missing/delete-marker current must not return delete command".to_string(),
            )));
        };
        let specific = BuildDeleteSpecificObjectVersionCommandReq {
            pg_id: request.pg_id,
            cluster_epoch: request.cluster_epoch,
            bucket: request.bucket,
            key: request.key,
            version_id: expected.version_id,
            expected_stored: request.expected_current,
            expected_target: request.expected_target,
            expected_version_list: None,
            bucket_write_reservation: request.bucket_write_reservation,
        };
        self.validate_delete_specific_object_command_response(command, &specific)
    }

    pub(super) fn validate_insert_delete_marker_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate insert-delete-marker command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not insert delete marker".to_string(),
            )));
        };
        if marker.bucket != *request.bucket
            || marker.key != *request.key
            || marker.version_id != request.version_id
            || marker.owner != *request.owner
            || marker.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        match &request.stale_payload {
            InsertDeleteMarkerStalePayload::Explicit(expected) => {
                let mut actual = marker.stale_payload.clone();
                normalize_reclaim_created_at(&mut actual);
                if &actual != expected {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "response stale payload does not match request".to_string(),
                    )));
                }
            }
            InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { .. } => {
                if let Some(payload) = marker.stale_payload.as_ref() {
                    self.validate_reclaim_payload_response(
                        payload,
                        request.bucket,
                        request.key,
                        context,
                    )?;
                }
                if !reclaim_matches_snapshot_live_object(
                    marker.stale_payload.as_ref(),
                    &request.expected_stale_payload_source.cloned(),
                ) {
                    return Err(ObjectPgActionError::Store(
                        self.rpc_payload_error(
                            context,
                            "response stale payload does not match current null live snapshot"
                                .to_string(),
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate_create_stream_upload_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream upload command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not create stream upload".to_string(),
            )));
        };
        if create.session.session_id != request.request.session_id
            || create.session.bucket != request.request.bucket
            || create.session.key != request.request.key
            || create.session.target != request.request.target
            || create.session.state != StreamUploadState::InProgress
            || create.session.encryption != request.request.encryption
            || create.initial_next_segment_vid != GenerationId::MIN
            || create.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_create_multipart_upload_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate multipart upload command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CreateMultipartUpload(create) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not create multipart upload".to_string(),
            )));
        };
        let upload = &create.upload;
        if upload.upload_id != request.request.upload_id
            || upload.bucket != request.request.bucket
            || upload.key != request.request.key
            || upload.state != UploadState::InProgress
            || upload.tags != request.request.tags
            || upload.metadata_blob != request.request.metadata_blob
            || upload.system_metadata_blob != request.request.system_metadata_blob
            || upload.initiator != request.request.initiator
            || upload.owner != request.request.owner
            || upload.acl_grants != request.request.acl_grants
            || upload.public_read != request.request.public_read
            || upload.object_lock != request.request.object_lock
            || upload.checksum != request.request.checksum
            || upload.encryption != request.request.encryption
            || create.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_stream_put_finalize_snapshot_response(
        &self,
        snapshot: &StreamPutFinalizeStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream PUT finalize snapshot response";
        if snapshot.session.session_id != *session_id
            || snapshot.session.bucket != *bucket
            || snapshot.session.key != *key
            || snapshot.session.target != StreamUploadTarget::PutObject
            || snapshot.session.state != StreamUploadState::InProgress
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot session identity does not match request".to_string(),
            )));
        }
        for segment in &snapshot.staging_segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot segment identity does not match request".to_string(),
                )));
            }
        }
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            if source.bucket() != bucket || source.key() != key {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "stale payload source identity does not match request".to_string(),
                )));
            }
        }
        if !reclaim_matches_bucket_key(snapshot.stale_payload.as_ref(), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stale payload identity does not match request".to_string(),
            )));
        }
        if !reclaim_matches_snapshot_live_object(
            snapshot.stale_payload.as_ref(),
            &snapshot.stale_payload_source,
        ) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "stale payload shape does not match source live object".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_stream_part_finalize_snapshot_response(
        &self,
        snapshot: &StreamUploadPartStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream part finalize snapshot response";
        let auth = &snapshot.auth_snapshot;
        if auth.session.session_id != *session_id
            || auth.session.bucket != *bucket
            || auth.session.key != *key
            || auth.session.target
                != (StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                })
            || auth.session.state != StreamUploadState::InProgress
            || auth.upload.upload_id != *upload_id
            || auth.upload.bucket != *bucket
            || auth.upload.key != *key
            || auth.upload.state != UploadState::InProgress
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "snapshot identity does not match request".to_string(),
            )));
        }
        for segment in &auth.staging_segments {
            if segment.session_id != *session_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "snapshot segment identity does not match request".to_string(),
                )));
            }
        }
        if snapshot
            .existing_part
            .as_ref()
            .is_some_and(|part| part.upload_id != *upload_id || part.part_number != part_number)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "existing part identity does not match request".to_string(),
            )));
        }
        for segment in &snapshot.displaced_segments {
            if segment.bucket != *bucket
                || segment.key != *key
                || segment.upload_id != *upload_id
                || segment.part_number != part_number
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "displaced segment identity does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_stream_put_commit_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream PUT commit command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not stream PUT commit".to_string(),
            )));
        };
        if !commit.matches_request(
            request.bucket,
            request.key,
            request.session_id,
            request.expected_snapshot.generation_id,
        ) || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        let object = &commit.object;
        let expected_etag = ObjectEtag::single_part(request.commit.etag_crc64);
        if object.generation_id != request.expected_snapshot.generation_id
            || object.version_id != request.commit.version_id
            || object.owner != request.commit.owner
            || object.acl_grants != request.commit.acl_grants
            || object.public_read != request.commit.public_read
            || object.size != request.commit.size
            || object.etag != expected_etag
            || object.layout != ObjectLayout::Standard
            || object.tags != request.commit.tags
            || object.metadata_blob.as_ref() != Some(&request.commit.metadata_blob)
            || object.system_metadata_blob.as_ref() != Some(&request.commit.system_metadata_blob)
            || object.object_lock != request.commit.object_lock
            || object.encryption != request.commit.encryption
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command object does not match request".to_string(),
            )));
        }
        let segments_total: u64 = request
            .expected_snapshot
            .staging_segments
            .iter()
            .map(|segment| segment.size)
            .sum();
        if segments_total != request.total_size
            || commit.segments.len() != request.expected_snapshot.staging_segments.len()
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command segment shape does not match request".to_string(),
            )));
        }
        for (actual, expected) in commit
            .segments
            .iter()
            .zip(request.expected_snapshot.staging_segments.iter())
        {
            if actual.bucket != *request.bucket
                || actual.key != *request.key
                || actual.version_id != request.commit.version_id
                || actual.segment_index != expected.segment_index
                || actual.size != expected.size
                || actual.segment_crc64 != expected.segment_crc64
                || actual.segment_okh != expected.segment_okh
                || actual.segment_vid != expected.segment_vid
                || actual.data_pg_id != expected.data_pg_id
                || actual.placement_cluster_epoch != expected.placement_cluster_epoch
                || actual.ec_k != expected.ec_k
                || actual.ec_m != expected.ec_m
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response command segment does not match snapshot".to_string(),
                )));
            }
        }
        if request.commit.version_id.is_null() {
            let mut actual_stale_payload = commit.stale_payload.clone();
            normalize_reclaim_created_at(&mut actual_stale_payload);
            if actual_stale_payload != request.expected_snapshot.stale_payload {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response command stale payload does not match expected snapshot".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "versioned stream PUT response must not reclaim stale null payload".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_stream_part_commit_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate stream part commit command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not stream part commit".to_string(),
            )));
        };
        if !commit.matches_request(
            request.bucket,
            request.key,
            request.upload_id,
            request.session_id,
            request.part_number,
        ) || commit.upload != request.expected_snapshot.auth_snapshot.upload
            || commit.part != *request.part
            || commit.segments != request.segments
            || commit.existing_part != request.expected_snapshot.existing_part
            || commit.displaced_segments != request.expected_snapshot.displaced_segments
            || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_complete_multipart_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate complete multipart command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not complete multipart".to_string(),
            )));
        };
        let parts_count = std::num::NonZeroU32::new(
            u32::try_from(request.request.part_records.len()).map_err(|_| {
                ObjectPgActionError::Store(
                    self.rpc_payload_error(context, "request part count exceeds u32".to_string()),
                )
            })?,
        )
        .ok_or_else(|| {
            ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "complete multipart response requires at least one part".to_string(),
            ))
        })?;
        let expected_etag = ObjectEtag::MultipartComposite {
            crc64: request.request.etag_crc64,
            parts: parts_count,
        };
        if !commit.matches_request(
            &request.request.bucket,
            &request.request.key,
            &request.request.upload_id,
            request.request.generation_id,
            &request.request.part_records,
        ) || commit.object.version_id != request.version_id
            || commit.object.owner != request.request.owner
            || commit.object.acl_grants != request.request.acl_grants
            || commit.object.public_read != request.request.public_read
            || commit.object.size != request.request.size
            || commit.object.etag != expected_etag
            || commit.object.layout != (ObjectLayout::MultipartManifest { parts_count })
            || commit.object.tags != request.request.tags
            || commit.object.metadata_blob != request.request.metadata_blob
            || commit.object.system_metadata_blob != request.request.system_metadata_blob
            || commit.object.object_lock != request.request.object_lock
            || commit.object.encryption != request.request.encryption
            || commit.completion_order != request.completion_order
            || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command does not match request".to_string(),
            )));
        }
        for part in &commit.parts {
            if part.bucket != request.request.bucket
                || part.key != request.request.key
                || part.version_id != request.version_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response object part identity does not match request".to_string(),
                )));
            }
        }
        if commit.parts != request.expected_object_parts {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response object part placement does not match expected topology".to_string(),
            )));
        }
        for segment in commit
            .selected_streaming_segments
            .iter()
            .chain(commit.omitted_streaming_segments.iter())
        {
            if segment.bucket != request.request.bucket
                || segment.key != request.request.key
                || segment.upload_id != request.request.upload_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response multipart segment identity does not match request".to_string(),
                )));
            }
        }
        let mut selected_part_numbers = BTreeMap::new();
        for part in &request.request.part_records {
            if selected_part_numbers
                .insert(part.part_number, part)
                .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "request contains duplicate multipart part numbers".to_string(),
                )));
            }
        }
        let mut omitted_part_numbers = BTreeMap::new();
        for part in &commit.omitted_parts {
            if part.upload_id != request.request.upload_id
                || selected_part_numbers.contains_key(&part.part_number)
                || omitted_part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response omitted part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut expected_selected_streaming_segments =
            request.request.selected_streaming_segments.clone();
        for segment in &mut expected_selected_streaming_segments {
            segment.version_id = request.version_id.to_u64();
        }
        if commit.selected_streaming_segments != expected_selected_streaming_segments {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response selected streaming segment cleanup does not match request".to_string(),
            )));
        }
        let mut omitted_segment_ids = BTreeMap::new();
        for segment in &commit.omitted_streaming_segments {
            if selected_part_numbers.contains_key(&segment.part_number)
                || omitted_segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response omitted streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        if commit.omitted_parts != request.request.expected_cleanup.omitted_parts
            || commit.omitted_streaming_segments
                != request.request.expected_cleanup.omitted_streaming_segments
            || !terminal_stream_cleanup_rows_match(
                &commit.stream_uploads,
                &request.request.expected_cleanup.stream_uploads,
                &commit.stream_upload_segments,
                &request.request.expected_cleanup.stream_upload_segments,
            )
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response cleanup does not match expected snapshot".to_string(),
            )));
        }
        self.validate_terminal_stream_cleanup_response(
            &commit.stream_uploads,
            &commit.stream_upload_segments,
            &request.request.bucket,
            &request.request.key,
            &request.request.upload_id,
            context,
        )?;
        if request.version_id.is_null() {
            if let Some(payload) = commit.stale_payload.as_ref() {
                self.validate_reclaim_payload_response(
                    payload,
                    &request.request.bucket,
                    &request.request.key,
                    context,
                )?;
                if !reclaim_matches_snapshot_live_object(
                    Some(payload),
                    &request.request.expected_stale_payload_source,
                ) {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        context,
                        "response stale payload does not match expected source".to_string(),
                    )));
                }
            } else if request.request.expected_stale_payload_source.is_some() {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response missing expected stale payload".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(
                self.rpc_payload_error(
                    context,
                    "versioned complete multipart response must not reclaim stale null payload"
                        .to_string(),
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_abort_multipart_command_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &AbortMultipartCommandValidation<'_>,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate abort multipart command build response";
        self.validate_object_metadata_command_route(
            command,
            request.pg_id,
            request.cluster_epoch,
            context,
        )?;
        let MetadataCommandPayload::AbortMultipartUpload(abort) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command payload is not abort multipart".to_string(),
            )));
        };
        if abort.bucket != *request.bucket
            || abort.key != *request.key
            || abort.upload_id != *request.upload_id
            || abort.bucket_write_reservation != *request.bucket_write_reservation
            || abort.cleanup.upload.bucket != *request.bucket
            || abort.cleanup.upload.key != *request.key
            || abort.cleanup.upload.upload_id != *request.upload_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response command identity does not match request".to_string(),
            )));
        }
        if request.expected_cleanup != Some(&abort.cleanup) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response cleanup does not match expected snapshot".to_string(),
            )));
        }
        let mut part_numbers = BTreeMap::new();
        for part in &abort.cleanup.parts {
            if part.upload_id != *request.upload_id
                || part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in &abort.cleanup.streaming_segments {
            if segment.bucket != *request.bucket
                || segment.key != *request.key
                || segment.upload_id != *request.upload_id
                || segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &abort.cleanup.stream_uploads,
            &abort.cleanup.stream_upload_segments,
            request.bucket,
            request.key,
            request.upload_id,
            context,
        )?;
        Ok(())
    }

    pub(super) fn validate_abort_cleanup_snapshot_response(
        &self,
        cleanup: &AbortMultipartUploadCleanup,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        let context = "validate abort multipart cleanup response";
        if cleanup.upload.bucket != *bucket
            || cleanup.upload.key != *key
            || cleanup.upload.upload_id != *upload_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "cleanup upload does not match request".to_string(),
            )));
        }
        let mut part_numbers = BTreeMap::new();
        for part in &cleanup.parts {
            if part.upload_id != *upload_id
                || part_numbers
                    .insert(part.part_number, part.generation)
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "part cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in &cleanup.streaming_segments {
            if segment.bucket != *bucket
                || segment.key != *key
                || segment.upload_id != *upload_id
                || segment_ids
                    .insert(
                        (segment.part_number, segment.segment_index),
                        segment.version_id,
                    )
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "streaming segment cleanup does not match request".to_string(),
                )));
            }
        }
        self.validate_terminal_stream_cleanup_response(
            &cleanup.stream_uploads,
            &cleanup.stream_upload_segments,
            bucket,
            key,
            upload_id,
            context,
        )
    }

    pub(super) fn validate_terminal_stream_cleanup_response(
        &self,
        stream_uploads: &[crate::types::TerminalStreamCleanupRecord],
        stream_upload_segments: &[StreamUploadSegmentRecord],
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let mut sessions = BTreeMap::new();
        for stream in stream_uploads {
            let StreamUploadTarget::UploadPart {
                upload_id: stream_upload_id,
                ..
            } = &stream.target
            else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream cleanup target is not upload-part".to_string(),
                )));
            };
            if stream.bucket != *bucket
                || stream.key != *key
                || stream_upload_id != upload_id
                || sessions.insert(stream.session_id.clone(), stream).is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream cleanup does not match request".to_string(),
                )));
            }
        }
        let mut segment_ids = BTreeMap::new();
        for segment in stream_upload_segments {
            if !sessions.contains_key(&segment.session_id)
                || segment_ids
                    .insert((segment.session_id.clone(), segment.segment_index), ())
                    .is_some()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    context,
                    "response stream segment cleanup does not match request".to_string(),
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_reclaim_payload_response(
        &self,
        payload: &ObjectPayloadReclaimCommand,
        bucket: &BucketName,
        key: &ObjectKey,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        if !reclaim_matches_bucket_key(Some(payload), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                context,
                "response reclaim payload does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_object_read_subject_response(
        &self,
        subject: &ObjectReadAuthSubject,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<(), ObjectPgActionError> {
        if subject.stored.bucket() != bucket
            || subject.stored.key() != key
            || version_id.is_some_and(|version_id| subject.stored.version_id() != version_id)
            || !subject.identity.matches_stored(&subject.stored)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read auth subject response",
                "subject identity does not match request".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_object_read_snapshot_response(
        &self,
        snapshot: &ObjectReadSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<(), ObjectPgActionError> {
        if snapshot.stored.bucket() != bucket
            || snapshot.stored.key() != key
            || version_id.is_some_and(|version_id| snapshot.stored.version_id() != version_id)
            || !expected_identity.matches_stored(&snapshot.stored)
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read snapshot response",
                "snapshot identity does not match request".to_string(),
            )));
        }

        let stored_version_id = snapshot.stored.version_id();
        for segment in &snapshot.object_segments {
            if &segment.bucket != bucket
                || &segment.key != key
                || segment.version_id != stored_version_id
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "object segment identity does not match snapshot".to_string(),
                )));
            }
        }
        for part in &snapshot.multipart_parts {
            if &part.bucket != bucket || &part.key != key || part.version_id != stored_version_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart part identity does not match snapshot".to_string(),
                )));
            }
        }
        for segment in &snapshot.multipart_part_segments {
            if &segment.bucket != bucket
                || &segment.key != key
                || segment.version_id != stored_version_id.to_u64()
            {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment identity does not match snapshot".to_string(),
                )));
            }
        }

        match &snapshot.stored {
            StoredObject::DeleteMarker(_) => {
                if !snapshot.object_segments.is_empty()
                    || !snapshot.multipart_parts.is_empty()
                    || !snapshot.multipart_part_segments.is_empty()
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate object read snapshot response",
                        "delete-marker snapshot must not include payload layout".to_string(),
                    )));
                }
            }
            StoredObject::Live(record) => match (record.layout, snapshot_mode) {
                (_, ObjectReadSnapshotMode::MetadataOnly)
                | (ObjectLayout::Standard, ObjectReadSnapshotMode::MultipartParts)
                | (
                    ObjectLayout::MultipartManifest { .. },
                    ObjectReadSnapshotMode::StandardSegments,
                ) => {
                    if !snapshot.object_segments.is_empty()
                        || !snapshot.multipart_parts.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "snapshot mode must not include payload layout".to_string(),
                        )));
                    }
                }
                (ObjectLayout::Standard, ObjectReadSnapshotMode::StandardSegments)
                | (ObjectLayout::Standard, ObjectReadSnapshotMode::FullPayloadLayout) => {
                    if !snapshot.multipart_parts.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(
                            self.rpc_payload_error(
                                "validate object read snapshot response",
                                "standard object snapshot must not include multipart layout"
                                    .to_string(),
                            ),
                        ));
                    }
                }
                (
                    ObjectLayout::MultipartManifest { parts_count },
                    ObjectReadSnapshotMode::MultipartParts,
                ) => {
                    if !snapshot.object_segments.is_empty()
                        || !snapshot.multipart_part_segments.is_empty()
                    {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "multipart-parts snapshot must not include segment layout".to_string(),
                        )));
                    }
                    self.validate_object_read_multipart_manifest_snapshot(
                        snapshot,
                        parts_count,
                        false,
                    )?;
                }
                (
                    ObjectLayout::MultipartManifest { parts_count },
                    ObjectReadSnapshotMode::FullPayloadLayout,
                ) => {
                    if !snapshot.object_segments.is_empty() {
                        return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                            "validate object read snapshot response",
                            "multipart snapshot must not include standard segments".to_string(),
                        )));
                    }
                    self.validate_object_read_multipart_manifest_snapshot(
                        snapshot,
                        parts_count,
                        true,
                    )?;
                }
            },
        }
        Ok(())
    }

    pub(super) fn validate_object_read_multipart_manifest_snapshot(
        &self,
        snapshot: &ObjectReadSnapshot,
        parts_count: std::num::NonZeroU32,
        require_segment_layout: bool,
    ) -> Result<(), ObjectPgActionError> {
        if snapshot.multipart_parts.len() != parts_count.get() as usize {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate object read snapshot response",
                "multipart snapshot part count does not match manifest".to_string(),
            )));
        }

        let mut parts_by_number = BTreeMap::new();
        for part in &snapshot.multipart_parts {
            if parts_by_number.insert(part.part_number, part).is_some() {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart snapshot contains duplicate part numbers".to_string(),
                )));
            }
        }

        let mut segment_counts_by_part = BTreeMap::<u32, usize>::new();
        for segment in &snapshot.multipart_part_segments {
            let Some(part) = parts_by_number.get(&segment.part_number) else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment has no matching part row".to_string(),
                )));
            };
            if part.part_okh != [0u8; 16] {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate object read snapshot response",
                    "multipart segment belongs to shard-set part row".to_string(),
                )));
            }
            *segment_counts_by_part
                .entry(segment.part_number)
                .or_default() += 1;
        }

        if require_segment_layout {
            for part in &snapshot.multipart_parts {
                if part.part_okh == [0u8; 16]
                    && part.size > 0
                    && !segment_counts_by_part.contains_key(&part.part_number)
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate object read snapshot response",
                        "segmented multipart part has no segment rows".to_string(),
                    )));
                }
            }
        }

        Ok(())
    }

    pub(super) fn validate_direct_put_command_build_response(
        &self,
        command: &MetadataCommandEnvelope,
        request: &BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<(), ObjectPgActionError> {
        if command.id().cluster_epoch() != request.cluster_epoch
            || command.id().pg_id() != request.pg_id
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "command id route does not match request".to_string(),
            )));
        }
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command payload is not direct PUT commit".to_string(),
            )));
        };
        if !commit.matches_request(
            &request.request.bucket,
            &request.request.key,
            &request.request.generation_reservation_id,
            request.request.generation_id,
        ) || commit.bucket_write_reservation != *request.bucket_write_reservation
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command identity does not match request".to_string(),
            )));
        }
        let object = &commit.object;
        let expected_etag = ObjectEtag::single_part(request.request.etag_crc64);
        if object.version_id != request.version_id
            || object.owner != request.request.owner
            || object.acl_grants != request.request.acl_grants
            || object.public_read != request.request.public_read
            || object.size != request.request.size
            || object.etag != expected_etag
            || object.ec != request.request.ec
            || object.layout != ObjectLayout::Standard
            || object.tags != request.request.tags
            || object.metadata_blob.as_ref() != Some(&request.request.metadata_blob)
            || object.system_metadata_blob.as_ref() != Some(&request.request.system_metadata_blob)
            || object.object_lock != request.request.object_lock
            || object.encryption != request.request.encryption
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command object does not match request".to_string(),
            )));
        }
        let [segment] = commit.segments.as_slice() else {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command must contain one segment".to_string(),
            )));
        };
        if segment.bucket != request.request.bucket
            || segment.key != request.request.key
            || segment.version_id != request.version_id
            || segment.segment_index != request.request.segment_index
            || segment.size != request.request.size
            || segment.segment_crc64 != request.request.segment_crc64
            || segment.segment_okh != request.request.segment_okh
            || segment.segment_vid != request.request.segment_vid
            || segment.data_pg_id != request.request.data_pg_id
            || segment.placement_cluster_epoch != request.cluster_epoch
            || segment.ec_k != request.request.ec.k
            || segment.ec_m != request.request.ec.m
        {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "response command segment does not match request".to_string(),
            )));
        }
        if request.version_id.is_null() {
            let mut actual_stale_payload = commit.stale_payload.clone();
            normalize_reclaim_created_at(&mut actual_stale_payload);
            if actual_stale_payload != request.expected_snapshot.stale_payload {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit command build response",
                    "response command stale payload does not match expected snapshot".to_string(),
                )));
            }
        } else if commit.stale_payload.is_some() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit command build response",
                "versioned direct PUT response must not reclaim stale null payload".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_direct_put_commit_snapshot_response(
        &self,
        snapshot: &DirectPutCommitStorageSnapshot,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), ObjectPgActionError> {
        let expected_etag = match snapshot.current.as_ref() {
            Some(current) => {
                if current.bucket() != bucket || current.key() != key {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate direct PUT commit snapshot response",
                        "current object identity does not match request".to_string(),
                    )));
                }
                current.as_live().map(|record| record.etag.format())
            }
            None => None,
        };
        if snapshot.auth_snapshot.existing_etag != expected_etag {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "auth snapshot etag does not match current object".to_string(),
            )));
        }
        if let Some(segments) = snapshot.committed_segments.as_ref() {
            let Some(current) = snapshot.current.as_ref() else {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit snapshot response",
                    "committed segments require current object".to_string(),
                )));
            };
            if current.as_live().is_none() {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit snapshot response",
                    "committed segments require live current object".to_string(),
                )));
            }
            for segment in segments {
                if segment.bucket != *bucket
                    || segment.key != *key
                    || segment.version_id != current.version_id()
                {
                    return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                        "validate direct PUT commit snapshot response",
                        "committed segment identity does not match current object".to_string(),
                    )));
                }
            }
        } else if snapshot.committed_stale_generation_id.is_some() {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "committed stale generation requires committed segments".to_string(),
            )));
        }
        if let (Some(stale_generation_id), Some(live)) = (
            snapshot.committed_stale_generation_id,
            snapshot
                .current
                .as_ref()
                .and_then(crate::StoredObject::as_live),
        ) {
            if stale_generation_id == live.generation_id {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit snapshot response",
                    "committed stale generation must not match current live generation".to_string(),
                )));
            }
        }
        if !reclaim_matches_bucket_key(snapshot.stale_payload.as_ref(), bucket, key) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "stale payload identity does not match request".to_string(),
            )));
        }
        if let Some(source) = snapshot.stale_payload_source.as_ref() {
            if source.bucket() != bucket || source.key() != key {
                return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                    "validate direct PUT commit snapshot response",
                    "stale payload source identity does not match request".to_string(),
                )));
            }
        }
        if !reclaim_matches_snapshot_live_object(
            snapshot.stale_payload.as_ref(),
            &snapshot.stale_payload_source,
        ) {
            return Err(ObjectPgActionError::Store(self.rpc_payload_error(
                "validate direct PUT commit snapshot response",
                "stale payload shape does not match source live object".to_string(),
            )));
        }
        Ok(())
    }

    pub(super) fn validate_empty_bucket_write_reservation_response(
        &self,
        context: &'static str,
        response: &[u8],
    ) -> Result<(), BucketSnapshotLoadError> {
        if response.is_empty() {
            Ok(())
        } else {
            Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                context,
                "bucket write reservation response payload must be empty".to_string(),
            )))
        }
    }

    pub(super) fn validate_bucket_delete_finalized_response(
        &self,
        response: StorageRpcBucketDeleteFinalizedResponse,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        match response.outcome {
            StorageRpcBucketDeleteFinalizedOutcome::Deleted => Ok(()),
            StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { name } if name == *bucket => {
                Err(BucketWriteDrainError::Metadata(
                    MetadataError::BucketNotFound { name },
                ))
            }
            StorageRpcBucketDeleteFinalizedOutcome::BucketNotFound { .. } => {
                Err(BucketWriteDrainError::Store(self.rpc_payload_error(
                    "validate bucket delete finalized response",
                    "bucket not found response name does not match request".to_string(),
                )))
            }
        }
    }

    pub(super) fn validate_proof_release_response(
        &self,
        response: &[u8],
    ) -> Result<(), BucketSnapshotLoadError> {
        if response.is_empty() {
            Ok(())
        } else {
            Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "decode proof release response",
                "proof release response payload must be empty".to_string(),
            )))
        }
    }

    pub(super) fn validate_create_bucket_command_build_outcome(
        &self,
        outcome: StorageRpcCreateBucketCommandBuildOutcome,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError> {
        match outcome {
            StorageRpcCreateBucketCommandBuildOutcome::Exists(info) => {
                if info.name != *bucket {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate create-bucket command build response",
                        "exists response bucket name does not match request".to_string(),
                    )));
                }
                Ok(CreateBucketCommandBuild::Exists(info))
            }
            StorageRpcCreateBucketCommandBuildOutcome::Command(command) => {
                if command.id() != command_id {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate create-bucket command build response",
                        "response command id does not match request".to_string(),
                    )));
                }
                match command.payload() {
                    MetadataCommandPayload::CreateBucket(create)
                        if create.matches_create_config(config) => {}
                    _ => {
                        return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                            "validate create-bucket command build response",
                            "response command payload does not match request".to_string(),
                        )));
                    }
                }
                Ok(CreateBucketCommandBuild::Command(command))
            }
        }
    }

    pub(super) fn validate_completed_multipart_order_command_build_response(
        &self,
        completion_order: u64,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart order command build response",
                "response command id does not match request".to_string(),
            )));
        }
        if completion_order == 0 {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate completed multipart order command build response",
                "response completion order must not be zero".to_string(),
            )));
        }
        match command.payload() {
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                if advance.bucket == *bucket && advance.completion_order == completion_order => {}
            _ => {
                return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                    "validate completed multipart order command build response",
                    "response command payload does not match request".to_string(),
                )))
            }
        }
        Ok((completion_order, command))
    }

    pub(super) fn validate_bucket_metadata_control_command_build_response(
        &self,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &StorageRpcBucketMetadataControlMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket metadata control command build response",
                "response command id does not match request".to_string(),
            )));
        }
        let matches = match (command.payload(), mutation) {
            (
                MetadataCommandPayload::PutBucketVersioning(versioning),
                StorageRpcBucketMetadataControlMutation::Versioning(state),
            ) => versioning.bucket.name == *bucket && versioning.bucket.versioning == *state,
            (
                MetadataCommandPayload::PutBucketAcl(acl),
                StorageRpcBucketMetadataControlMutation::Acl {
                    acl_grants,
                    public_read,
                    public_write,
                },
            ) => {
                acl.bucket.name == *bucket
                    && acl.bucket.acl_grants == *acl_grants
                    && acl.bucket.public_read == *public_read
                    && acl.bucket.public_write == *public_write
            }
            (
                MetadataCommandPayload::PutBucketProperty(property),
                StorageRpcBucketMetadataControlMutation::Property(mutation),
            ) => bucket_property_command_matches_mutation(property, bucket, mutation),
            (
                MetadataCommandPayload::PutBucketSubresource(subresource),
                StorageRpcBucketMetadataControlMutation::Subresource(mutation),
            ) => subresource.matches_mutation(bucket, mutation),
            _ => false,
        };
        if !matches {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket metadata control command build response",
                "response command payload does not match request".to_string(),
            )));
        }
        Ok(command)
    }

    pub(super) fn validate_mark_bucket_deleting_command_build_response(
        &self,
        command: MetadataCommandEnvelope,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        if command.id() != command_id {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket mark-deleting command build response",
                "response command id does not match request".to_string(),
            )));
        }
        let valid = matches!(
            command.payload(),
            MetadataCommandPayload::MarkBucketDeleting(mark)
                if mark.bucket.name == *bucket && mark.bucket.state == BucketState::Deleting
        );
        if !valid {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate bucket mark-deleting command build response",
                "response command payload does not match request".to_string(),
            )));
        }
        Ok(command)
    }

    pub(super) fn validate_mark_bucket_deleting_already_deleting_response(
        &self,
        info: &BucketInfo,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        if info.name == *bucket && info.state == BucketState::Deleting {
            return Ok(());
        }
        Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
            "validate bucket mark-deleting command build response",
            "already-deleting response identity does not match request".to_string(),
        )))
    }

    pub(super) fn validate_bucket_snapshot_pair_response(
        &self,
        pair: &BucketSnapshotPair,
        source: (&BucketName, BucketSnapshotRequest),
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<(), BucketSnapshotLoadError> {
        match pair {
            BucketSnapshotPair::Same { bucket } => {
                if source.0 != destination.0 {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "same-bucket response for distinct bucket request".to_string(),
                    )));
                }
                let expected_request = source.1.union(destination.1);
                if bucket.bucket.name != *source.0 || bucket.request != expected_request {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "same-bucket snapshot identity does not match request".to_string(),
                    )));
                }
            }
            BucketSnapshotPair::Distinct {
                source: source_snapshot,
                destination: destination_snapshot,
            } => {
                if source.0 == destination.0 {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "distinct-bucket response for same bucket request".to_string(),
                    )));
                }
                if source_snapshot.bucket.name != *source.0
                    || source_snapshot.request != source.1
                    || destination_snapshot.bucket.name != *destination.0
                    || destination_snapshot.request != destination.1
                {
                    return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                        "validate bucket snapshot pair response",
                        "distinct-bucket snapshot identity does not match request".to_string(),
                    )));
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate_bucket_payload_reclaim_root_response(
        &self,
        response: &StorageRpcPayloadReclaimRootResponse,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        if response
            .root
            .as_ref()
            .is_some_and(|root| &root.bucket != bucket)
        {
            return Err(BucketSnapshotLoadError::Store(self.rpc_payload_error(
                "validate object bucket payload reclaim root response",
                "payload reclaim root bucket does not match request".to_string(),
            )));
        }
        Ok(())
    }
}
