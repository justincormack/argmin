// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("storage RPC byte slice length must fit in u32");
    put_u32(out, len);
    out.extend_from_slice(bytes);
}

fn checked_u32_len(len: usize) -> Result<u32, StorageRpcPayloadError> {
    u32::try_from(len).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
        len,
        limit: u32::MAX as usize,
    })
}

fn put_string_vec(out: &mut Vec<u8>, values: &[String]) -> Result<(), StorageRpcPayloadError> {
    put_u32(out, checked_u32_len(values.len())?);
    for value in values {
        put_string(out, value);
    }
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_acl_grants(out: &mut Vec<u8>, grants: &AclGrants) {
    let stored = StoredAclGrants::from_grants(grants);
    put_string(out, stored.as_storage_str());
}

fn put_shard_location(out: &mut Vec<u8>, location: StorageRpcShardLocation) {
    put_u64(out, location.cluster_epoch.get());
    put_u32(out, location.pg_id.get());
    put_u8(out, location.shard_index.get());
    put_u32(out, location.node_id.as_u32());
}

fn put_claim_token(out: &mut Vec<u8>, token: &StorageRpcDurableClaimToken) {
    match token {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(token) => {
            put_u8(out, 0);
            put_string(out, token.bucket.as_str());
            put_u64(out, token.bucket_incarnation_generation);
            put_string(out, token.key.as_str());
            put_u64(out, token.generation_id.get());
            put_u8(out, token.reclaim_kind as u8);
            put_string(out, &token.claim_id);
            put_string(out, &token.owner_token);
            put_u64(out, token.cluster_epoch.get());
            put_u32(out, token.pg_id);
        }
        StorageRpcDurableClaimToken::BucketDeleteFinalize(token) => {
            put_u8(out, 1);
            put_bucket_claim_token(out, token);
        }
        StorageRpcDurableClaimToken::LifecycleSweep(token) => {
            put_u8(out, 2);
            put_bucket_claim_token(out, token);
        }
    }
}

fn put_bucket_claim_token(out: &mut Vec<u8>, token: &StorageRpcBucketClaimToken) {
    put_string(out, token.bucket.as_str());
    put_u64(out, token.bucket_incarnation_generation);
    put_string(out, &token.claim_id);
    put_string(out, &token.owner_token);
    put_u64(out, token.cluster_epoch.get());
    put_u32(out, token.pg_id);
}

fn put_bucket_write_reservation_proof(out: &mut Vec<u8>, proof: &BucketWriteReservationProof) {
    put_string(out, proof.bucket.as_str());
    put_string(out, &proof.reservation_id);
    put_string(out, &proof.owner_token);
    put_u64(out, proof.cluster_epoch.get());
    put_u64(out, proof.bucket_execution_generation);
    put_u64(out, proof.bucket_incarnation_generation);
    put_string(out, &proof.operation_kind);
    put_u64(out, proof.created_at);
    put_u64(out, proof.lease_deadline);
    put_optional_string(out, proof.target_context.as_deref());
}

fn put_bucket_write_reservation_record(out: &mut Vec<u8>, record: &BucketWriteReservationRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.reservation_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.operation_kind);
    put_u64(out, record.created_at);
    put_u64(out, record.lease_deadline);
    put_optional_string(out, record.target_context.as_deref());
}

fn put_bucket_write_drain_record(out: &mut Vec<u8>, record: &BucketWriteDrainRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.drain_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u8(
        out,
        match record.state {
            BucketWriteDrainState::Draining => 0,
        },
    );
    put_u64(out, record.created_at);
    put_u64(out, record.lease_deadline);
}

fn put_bucket_delete_attempt_outcome_record(
    out: &mut Vec<u8>,
    record: &BucketDeleteAttemptOutcomeRecord,
) {
    put_string(out, record.bucket.as_str());
    put_string(out, &record.drain_id);
    put_u64(out, record.cluster_epoch.get());
    put_u64(out, record.bucket_execution_generation);
    put_u8(out, record.outcome as u8);
    put_u8(out, record.phase as u8);
    put_string(out, &record.detail);
    put_optional_u32(out, record.post_reservation_next_object_pg_id);
    put_optional_u32(out, record.stream_cleanup_next_object_pg_id);
    put_optional_string(
        out,
        record
            .stream_cleanup_next_session_id_marker
            .as_ref()
            .map(SessionId::as_str),
    );
    put_bool(out, record.stream_cleanup_aborted_uploads);
    put_optional_u32(out, record.final_visibility_next_object_pg_id);
    put_optional_u32(out, record.finalizer_next_object_pg_id);
    put_u64(out, record.updated_at);
}

fn put_bucket_delete_finalize_root(out: &mut Vec<u8>, root: &BucketDeleteFinalizeRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_incarnation_generation);
}

fn put_bucket_delete_begin_root(out: &mut Vec<u8>, root: &BucketDeleteBeginRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_execution_generation);
    put_u64(out, root.bucket_incarnation_generation);
}

fn put_bucket_delete_finalize_claim_record(
    out: &mut Vec<u8>,
    record: &BucketDeleteFinalizeClaimRecord,
) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_object_payload_reclaim_claim_record(
    out: &mut Vec<u8>,
    record: &ObjectPayloadReclaimClaimRecord,
) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, record.key.as_str());
    put_u64(out, record.generation_id.get());
    put_u8(out, record.reclaim_kind as u8);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_lifecycle_sweep_root(out: &mut Vec<u8>, root: &LifecycleSweepRoot) {
    put_string(out, root.bucket.as_str());
    put_u64(out, root.bucket_incarnation_generation);
    put_u8(
        out,
        match root.source {
            LifecycleSweepRootSource::ExpiredClaim => 0,
            LifecycleSweepRootSource::BusyClaim => 1,
            LifecycleSweepRootSource::LifecycleConfig => 2,
            LifecycleSweepRootSource::AbortingMultipartUpload => 3,
        },
    );
}

fn put_lifecycle_sweep_claim_record(out: &mut Vec<u8>, record: &LifecycleSweepClaimRecord) {
    put_string(out, record.bucket.as_str());
    put_u64(out, record.bucket_incarnation_generation);
    put_string(out, &record.claim_id);
    put_string(out, &record.owner_token);
    put_u64(out, record.cluster_epoch.get());
    put_u32(out, record.pg_id);
    put_u64(out, record.claimed_at);
    put_u64(out, record.heartbeat_at);
    put_optional_u64(out, record.lease_deadline);
    put_u64(out, record.attempt_count);
    put_optional_string(out, record.last_error.as_deref());
}

fn put_create_bucket_config(out: &mut Vec<u8>, config: &StorageRpcCreateBucketConfig) {
    put_string(out, config.name.as_str());
    put_string(out, &config.owner_principal);
    put_string(out, config.owner_canonical_id.as_str());
    put_acl_grants(out, &config.acl_grants);
    put_bool(out, config.public_read);
    put_bool(out, config.public_write);
    put_u8(out, config.versioning as u8);
    put_bucket_object_lock_config(out, &config.object_lock);
    put_u8(out, config.ownership_controls.object_ownership as u8);
}

fn put_bucket_info(out: &mut Vec<u8>, info: &BucketInfo) {
    put_string(out, info.name.as_str());
    put_string(out, &info.owner_principal);
    put_string(out, info.owner_canonical_id.as_str());
    put_u64(out, info.created_at);
    put_u16(out, info.region);
    put_u8(out, info.state as u8);
    put_u8(out, info.versioning as u8);
    put_bucket_object_lock_config(out, &info.object_lock);
    put_acl_grants(out, &info.acl_grants);
    put_bool(out, info.public_read);
    put_bool(out, info.public_write);
    put_optional_public_access_block_config(out, info.public_access_block);
    put_optional_bucket_ownership_controls(out, info.ownership_controls);
    put_bool(out, info.bucket_policy_present);
    put_bool(out, info.bucket_policy_public);
    put_u64(out, info.bucket_policy_generation);
    put_bool(out, info.bucket_lifecycle_present);
    put_u64(out, info.bucket_lifecycle_generation);
    put_u64(out, info.bucket_execution_generation);
    put_u64(out, info.bucket_incarnation_generation);
    put_bytes(out, info.multipart_upload_id_key.as_bytes());
    put_bool(out, info.bucket_abac_enabled);
    put_u8(out, info.encryption.default_encryption as u8);
    put_bool(out, info.encryption.sse_c_blocked);
}

fn put_bucket_fast_path_identity(out: &mut Vec<u8>, identity: BucketFastPathIdentity) {
    put_u64(out, identity.bucket_execution_generation);
    put_u64(out, identity.bucket_incarnation_generation);
}

fn put_bucket_snapshot_request(out: &mut Vec<u8>, request: BucketSnapshotRequest) {
    put_bool(out, request.policy);
    put_u8(
        out,
        match request.tags {
            BucketSnapshotTagsRequest::NotRequested => 0,
            BucketSnapshotTagsRequest::IfBucketAbacEnabled => 1,
            BucketSnapshotTagsRequest::Always => 2,
        },
    );
    put_bool(out, request.lifecycle);
    put_bool(out, request.cors);
}

fn put_bucket_snapshot(out: &mut Vec<u8>, snapshot: &BucketSnapshot) {
    put_bucket_info(out, &snapshot.bucket);
    put_bucket_snapshot_request(out, snapshot.request);
    put_loaded_bucket_subresource(out, &snapshot.policy);
    put_loaded_bucket_tags(out, &snapshot.tags);
    put_loaded_bucket_subresource(out, &snapshot.lifecycle);
    put_loaded_bucket_subresource(out, &snapshot.cors);
}

fn put_bucket_metadata_control_mutation(
    out: &mut Vec<u8>,
    mutation: &StorageRpcBucketMetadataControlMutation,
) {
    match mutation {
        StorageRpcBucketMetadataControlMutation::Versioning(state) => {
            put_u8(out, 0);
            put_u8(out, *state as u8);
        }
        StorageRpcBucketMetadataControlMutation::Acl {
            acl_grants,
            summary,
        } => {
            put_u8(out, 1);
            put_acl_grants(out, acl_grants);
            put_bool(out, summary.public_read);
            put_bool(out, summary.public_write);
        }
        StorageRpcBucketMetadataControlMutation::Property(mutation) => {
            put_u8(out, 2);
            put_bucket_property_mutation(out, mutation);
        }
        StorageRpcBucketMetadataControlMutation::Subresource(mutation) => {
            put_u8(out, 3);
            put_bucket_subresource_mutation(out, mutation);
        }
        StorageRpcBucketMetadataControlMutation::MarkDeleting => {
            put_u8(out, 4);
        }
    }
}

fn put_bucket_property_mutation(out: &mut Vec<u8>, mutation: &BucketPropertyMutation) {
    match mutation {
        BucketPropertyMutation::ObjectLock(config) => {
            put_u8(out, 0);
            put_bucket_object_lock_config(out, config);
        }
        BucketPropertyMutation::Encryption(config) => {
            put_u8(out, 1);
            put_bucket_encryption_config(out, *config);
        }
        BucketPropertyMutation::PublicAccessBlock(config) => {
            put_u8(out, 2);
            put_optional_public_access_block_config(out, *config);
        }
        BucketPropertyMutation::OwnershipControls(config) => {
            put_u8(out, 3);
            put_optional_bucket_ownership_controls(out, *config);
        }
        BucketPropertyMutation::AbacEnabled(enabled) => {
            put_u8(out, 4);
            put_bool(out, *enabled);
        }
    }
}

fn put_bucket_encryption_config(out: &mut Vec<u8>, config: BucketEncryptionConfig) {
    match config.default_encryption {
        None => put_u8(out, 0),
        Some(algorithm) => {
            put_u8(out, 1);
            put_u8(out, algorithm as u8);
        }
    }
    put_bool(out, config.sse_c_blocked);
}

fn put_bucket_subresource_mutation(out: &mut Vec<u8>, mutation: &BucketSubresourceMutation) {
    match mutation {
        BucketSubresourceMutation::PutCors(body) => {
            put_u8(out, 1);
            put_bucket_subresource_kind(out, BucketSubresourceKind::Cors);
            put_string(out, body);
            put_bucket_subresource_aux(out, BucketSubresourceAux::None);
        }
        BucketSubresourceMutation::PutTagging(tags) => {
            put_u8(out, 1);
            put_bucket_subresource_kind(out, BucketSubresourceKind::Tagging);
            put_string(out, tags.as_str());
            put_bucket_subresource_aux(out, BucketSubresourceAux::None);
        }
        BucketSubresourceMutation::PutPolicy { body, is_public } => {
            put_u8(out, 1);
            put_bucket_subresource_kind(out, BucketSubresourceKind::Policy);
            put_string(out, body);
            put_bucket_subresource_aux(out, BucketSubresourceAux::policy(*is_public));
        }
        BucketSubresourceMutation::PutLifecycle(body) => {
            put_u8(out, 1);
            put_bucket_subresource_kind(out, BucketSubresourceKind::Lifecycle);
            put_string(out, body);
            put_bucket_subresource_aux(out, BucketSubresourceAux::None);
        }
        BucketSubresourceMutation::Delete { kind } => {
            put_u8(out, 2);
            put_bucket_subresource_kind(out, *kind);
        }
    }
}

fn put_bucket_subresource_kind(out: &mut Vec<u8>, kind: BucketSubresourceKind) {
    put_u8(out, kind as u8);
}

fn put_bucket_subresource_aux(out: &mut Vec<u8>, aux: BucketSubresourceAux) {
    match aux {
        BucketSubresourceAux::None => put_u8(out, 0),
        BucketSubresourceAux::Policy { is_public } => {
            put_u8(out, 1);
            put_bool(out, is_public);
        }
    }
}

fn put_loaded_bucket_subresource(out: &mut Vec<u8>, subresource: &LoadedBucketSubresource<String>) {
    match subresource {
        LoadedBucketSubresource::NotRequested => put_u8(out, 0),
        LoadedBucketSubresource::Missing => put_u8(out, 1),
        LoadedBucketSubresource::Loaded(value) => {
            put_u8(out, 2);
            put_string(out, value);
        }
    }
}

fn put_loaded_bucket_tags(
    out: &mut Vec<u8>,
    tags: &LoadedBucketSubresource<SerializedBucketTagSet>,
) {
    match tags {
        LoadedBucketSubresource::NotRequested => put_u8(out, 0),
        LoadedBucketSubresource::Missing => put_u8(out, 1),
        LoadedBucketSubresource::Loaded(tags) => {
            put_u8(out, 2);
            put_string(out, tags.as_str());
        }
    }
}

fn put_direct_put_commit_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &DirectPutCommitStorageSnapshot,
) {
    put_optional_string(out, snapshot.auth_snapshot.existing_etag.as_deref());
    put_optional_stored_object(out, snapshot.current.as_ref());
    put_optional_object_segments(out, snapshot.committed_segments.as_deref());
    put_optional_generation_id(out, snapshot.committed_stale_generation_id);
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_optional_object_payload_reclaim(out, snapshot.stale_payload.as_ref());
}

fn put_optional_generation_id(out: &mut Vec<u8>, generation_id: Option<GenerationId>) {
    match generation_id {
        None => put_u8(out, 0),
        Some(generation_id) => {
            put_u8(out, 1);
            put_u64(out, generation_id.get());
        }
    }
}

fn put_optional_object_segments(out: &mut Vec<u8>, segments: Option<&[ObjectSegmentRecord]>) {
    match segments {
        None => put_u8(out, 0),
        Some(segments) => {
            put_u8(out, 1);
            put_u32(
                out,
                u32::try_from(segments.len()).expect("object segment count must fit in u32"),
            );
            for segment in segments {
                put_object_segment_record(out, segment);
            }
        }
    }
}

fn put_optional_object_payload_reclaim(
    out: &mut Vec<u8>,
    reclaim: Option<&ObjectPayloadReclaimCommand>,
) {
    match reclaim {
        None => put_u8(out, 0),
        Some(reclaim) => {
            put_u8(out, 1);
            put_object_payload_reclaim(out, reclaim);
        }
    }
}

fn put_optional_payload_reclaim_root(out: &mut Vec<u8>, root: Option<&PayloadReclaimRoot>) {
    match root {
        Some(root) => {
            put_u8(out, 1);
            put_string(out, root.bucket.as_str());
            put_string(out, root.key.as_str());
            put_u64(out, root.generation_id.get());
        }
        None => put_u8(out, 0),
    }
}

fn put_object_payload_reclaim(out: &mut Vec<u8>, reclaim: &ObjectPayloadReclaimCommand) {
    match reclaim {
        ObjectPayloadReclaimCommand::Segments(reclaim) => {
            put_u8(out, 0);
            put_object_segments_reclaim_record(out, reclaim);
        }
        ObjectPayloadReclaimCommand::Multipart(reclaim) => {
            put_u8(out, 1);
            put_multipart_reclaim_record(out, reclaim);
        }
    }
}

fn put_object_segments_reclaim_record(out: &mut Vec<u8>, reclaim: &ObjectSegmentsReclaimRecord) {
    put_string(out, reclaim.bucket.as_str());
    put_string(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.segments.len() as u32);
    for segment in &reclaim.segments {
        put_u32(out, segment.segment_index);
        put_bytes(out, &segment.segment_okh);
        put_u64(out, segment.segment_vid.get());
        put_u32(out, segment.data_pg_id);
        put_ec_shape(out, segment.ec);
    }
}

fn put_multipart_reclaim_record(out: &mut Vec<u8>, reclaim: &MultipartReclaimRecord) {
    put_string(out, reclaim.bucket.as_str());
    put_string(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.parts.len() as u32);
    for part in &reclaim.parts {
        put_u32(out, part.part_number);
        put_u32(out, part.segments.len() as u32);
        for segment in &part.segments {
            put_u32(out, segment.segment_index);
            put_bytes(out, &segment.segment_okh);
            put_u64(out, segment.segment_vid.get());
            put_u32(out, segment.data_pg_id);
            put_ec_shape(out, segment.ec);
        }
    }
}

fn put_commit_direct_put_object_req(out: &mut Vec<u8>, request: &CommitDirectPutObjectReq) {
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_string(out, request.generation_reservation_id.as_str());
    put_u8(out, request.versioning as u8);
    put_owner_identity(out, &request.owner);
    put_acl_grants(out, &request.acl_grants);
    put_bool(out, request.public_read);
    put_u64(out, request.generation_id.get());
    put_u64(out, request.size);
    put_u64(out, request.etag_crc64);
    put_ec_shape(out, request.ec);
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, request.metadata_blob.as_slice());
    put_bytes(out, request.system_metadata_blob.as_slice());
    put_object_lock_state(out, request.object_lock);
    put_object_encryption(out, &request.encryption);
    put_u32(out, request.segment_index);
    put_u64(out, request.segment_crc64);
    put_bytes(out, &request.segment_okh);
    put_u64(out, request.segment_vid.get());
    put_u32(out, request.data_pg_id);
    put_bucket_write_reservation_proof(out, &request.bucket_write_reservation);
}

fn put_optional_stored_object(out: &mut Vec<u8>, stored: Option<&StoredObject>) {
    match stored {
        None => put_u8(out, 0),
        Some(stored) => {
            put_u8(out, 1);
            put_stored_object(out, stored);
        }
    }
}

fn put_optional_stored_object_list(out: &mut Vec<u8>, stored: Option<&[StoredObject]>) {
    match stored {
        None => put_u8(out, 0),
        Some(stored) => {
            put_u8(out, 1);
            put_stored_object_list(out, stored);
        }
    }
}

fn put_stored_object_list(out: &mut Vec<u8>, stored: &[StoredObject]) {
    put_u32(
        out,
        u32::try_from(stored.len()).expect("stored object list count must fit in u32"),
    );
    for stored in stored {
        put_stored_object(out, stored);
    }
}

fn put_stored_object(out: &mut Vec<u8>, stored: &StoredObject) {
    match stored {
        StoredObject::Live(record) => {
            put_u8(out, 0);
            put_live_object_record(out, record);
        }
        StoredObject::DeleteMarker(record) => {
            put_u8(out, 1);
            put_delete_marker_record(out, record);
        }
    }
}

fn put_put_object_metadata_mutation(out: &mut Vec<u8>, mutation: &PutObjectMetadataMutation) {
    match mutation {
        PutObjectMetadataMutation::PutTags(tags) => {
            put_u8(out, 0);
            put_string(out, tags.as_str());
        }
        PutObjectMetadataMutation::DeleteTags => put_u8(out, 1),
        PutObjectMetadataMutation::PutRetention(retention) => {
            put_u8(out, 2);
            put_u64(out, retention.retain_until_unix_seconds);
            put_u8(out, retention.mode as u8);
        }
        PutObjectMetadataMutation::PutLegalHold(legal_hold) => {
            put_u8(out, 3);
            put_u8(out, *legal_hold as u8);
        }
        PutObjectMetadataMutation::PutAcl {
            acl_grants,
            public_read,
        } => {
            put_u8(out, 4);
            put_acl_grants(out, acl_grants);
            put_bool(out, *public_read);
        }
    }
}

fn put_insert_delete_marker_stale_payload(
    out: &mut Vec<u8>,
    stale_payload: &StorageRpcInsertDeleteMarkerStalePayload,
) {
    match stale_payload {
        StorageRpcInsertDeleteMarkerStalePayload::Explicit(reclaim) => {
            put_u8(out, 0);
            put_optional_object_payload_reclaim(out, reclaim.as_ref());
        }
        StorageRpcInsertDeleteMarkerStalePayload::SnapshotCurrentNullLive { created_at } => {
            put_u8(out, 1);
            put_u64(out, *created_at);
        }
    }
}

fn put_create_stream_upload_req(out: &mut Vec<u8>, request: &CreateStreamUploadReq) {
    put_string(out, request.session_id.as_str());
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_stream_upload_target(out, &request.target);
    put_object_encryption(out, &request.encryption);
}

fn put_prepare_stream_segment_append_req(
    out: &mut Vec<u8>,
    request: &PrepareStreamUploadSegmentAppendReq,
) {
    put_string(out, request.session_id.as_str());
    put_u32(out, request.segment_index);
    put_u64(out, request.size);
    put_u64(out, request.segment_crc64);
    put_u64(out, request.payload_crc64);
}

fn put_stream_upload_target(out: &mut Vec<u8>, target: &StreamUploadTarget) {
    match target {
        StreamUploadTarget::PutObject => put_u8(out, 0),
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number,
        } => {
            put_u8(out, 1);
            put_string(out, upload_id.as_str());
            put_u32(out, *part_number);
        }
    }
}

fn put_create_stream_upload_precondition(
    out: &mut Vec<u8>,
    precondition: &StorageRpcCreateStreamUploadPrecondition,
) {
    match precondition {
        StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
            require_generation_reservation,
        } => {
            put_u8(out, 0);
            put_bool(out, *require_generation_reservation);
        }
        StorageRpcCreateStreamUploadPrecondition::PutObject {
            expected_current,
            require_generation_reservation,
        } => {
            put_u8(out, 1);
            put_optional_stored_object(out, expected_current.as_ref());
            put_bool(out, *require_generation_reservation);
        }
        StorageRpcCreateStreamUploadPrecondition::UploadPart { expected_upload } => {
            put_u8(out, 2);
            put_multipart_upload_record(out, expected_upload);
        }
    }
}

fn put_optional_create_stream_upload_command(
    out: &mut Vec<u8>,
    command: Option<&CreateStreamUploadCommand>,
) {
    match command {
        None => put_u8(out, 0),
        Some(command) => {
            put_u8(out, 1);
            put_create_stream_upload_command(out, command);
        }
    }
}

fn put_create_stream_upload_command(out: &mut Vec<u8>, command: &CreateStreamUploadCommand) {
    put_stream_upload_command_record(out, &command.session);
    put_u64(out, command.initial_next_segment_vid.get());
    put_optional_u64(out, command.cleanup_after);
    put_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn put_stream_upload_command_record(
    out: &mut Vec<u8>,
    record: &crate::types::StreamUploadCommandRecord,
) {
    put_string(out, record.session_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_stream_upload_target(out, &record.target);
    put_u8(out, record.state as u8);
    put_u64(out, record.created_at);
    put_object_encryption(out, &record.encryption);
}

fn put_stream_upload_record(out: &mut Vec<u8>, record: &StreamUploadRecord) {
    put_string(out, record.session_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_stream_upload_target(out, &record.target);
    put_u8(out, record.state as u8);
    put_u64(out, record.created_at);
    put_optional_u64(out, record.cleanup_after);
    put_object_encryption(out, &record.encryption);
    put_u64(out, record.next_segment_vid.get());
    put_optional_bucket_write_reservation_proof(out, record.bucket_write_reservation.as_ref());
}

fn put_optional_bucket_write_reservation_proof(
    out: &mut Vec<u8>,
    proof: Option<&BucketWriteReservationProof>,
) {
    match proof {
        Some(proof) => {
            put_bool(out, true);
            put_bucket_write_reservation_proof(out, proof);
        }
        None => put_bool(out, false),
    }
}

fn put_stream_upload_segment_record(out: &mut Vec<u8>, segment: &StreamUploadSegmentRecord) {
    put_string(out, segment.session_id.as_str());
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_u64(out, segment.payload_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_terminal_stream_cleanup_record(out: &mut Vec<u8>, stream: &TerminalStreamCleanupRecord) {
    put_string(out, stream.session_id.as_str());
    put_string(out, stream.bucket.as_str());
    put_string(out, stream.key.as_str());
    put_stream_upload_target(out, &stream.target);
    put_u8(out, stream.state as u8);
    put_u64(out, stream.created_at);
    put_object_encryption(out, &stream.encryption);
}

fn put_stream_put_finalize_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &StreamPutFinalizeStorageSnapshot,
) {
    put_stream_upload_record(out, &snapshot.session);
    put_optional_string(out, snapshot.existing_etag.as_deref());
    put_u64(out, snapshot.generation_id.get());
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_optional_object_payload_reclaim(out, snapshot.stale_payload.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.staging_segments.len())
            .expect("stream segment count must fit in u32"),
    );
    for segment in &snapshot.staging_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_stream_put_commit_input(out: &mut Vec<u8>, commit: &StreamPutCommitInput) {
    put_u8(out, commit.versioning as u8);
    put_u64(out, commit.version_id.to_u64());
    put_owner_identity(out, &commit.owner);
    put_acl_grants(out, &commit.acl_grants);
    put_bool(out, commit.public_read);
    put_u64(out, commit.etag_crc64);
    put_optional_string(out, commit.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, commit.metadata_blob.as_slice());
    put_bytes(out, commit.system_metadata_blob.as_slice());
    put_object_lock_state(out, commit.object_lock);
    put_object_encryption(out, &commit.encryption);
}

fn put_complete_multipart_commit_request(
    out: &mut Vec<u8>,
    request: &CompleteMultipartCommitRequest,
) {
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_string(out, request.upload_id.as_str());
    put_bytes(out, request.completion_fingerprint.as_bytes());
    put_u8(out, request.versioning as u8);
    put_owner_identity(out, &request.owner);
    put_acl_grants(out, &request.acl_grants);
    put_bool(out, request.public_read);
    put_u64(out, request.generation_id.get());
    put_u64(out, request.size);
    put_bytes(out, &request.etag_crc64);
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_optional_bytes(
        out,
        request.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    put_optional_bytes(
        out,
        request
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    put_object_lock_state(out, request.object_lock);
    put_object_encryption(out, &request.encryption);
    put_optional_stored_object(out, request.expected_stale_payload_source.as_ref());
    put_optional_multipart_object_identity(out, request.expected_current_object_identity);
    put_bool(out, request.conditional_completion);
    put_u32(
        out,
        u32::try_from(request.part_records.len())
            .expect("complete multipart part count must fit in u32"),
    );
    for part in &request.part_records {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(request.selected_streaming_segments.len())
            .expect("complete multipart selected segment count must fit in u32"),
    );
    for segment in &request.selected_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_complete_multipart_commit_cleanup(out, &request.expected_cleanup);
}

fn put_complete_multipart_commit_cleanup(
    out: &mut Vec<u8>,
    cleanup: &CompleteMultipartCommitCleanup,
) {
    put_u32(
        out,
        u32::try_from(cleanup.omitted_parts.len())
            .expect("complete multipart omitted part count must fit in u32"),
    );
    for part in &cleanup.omitted_parts {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(cleanup.omitted_streaming_segments.len())
            .expect("complete multipart omitted segment count must fit in u32"),
    );
    for segment in &cleanup.omitted_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_uploads.len())
            .expect("complete multipart stream cleanup count must fit in u32"),
    );
    for stream in &cleanup.stream_uploads {
        put_terminal_stream_cleanup_record(out, stream);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_upload_segments.len())
            .expect("complete multipart stream segment cleanup count must fit in u32"),
    );
    for segment in &cleanup.stream_upload_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_optional_abort_multipart_upload_cleanup(
    out: &mut Vec<u8>,
    cleanup: Option<&AbortMultipartUploadCleanup>,
) {
    match cleanup {
        None => put_u8(out, 0),
        Some(cleanup) => {
            put_u8(out, 1);
            put_abort_multipart_upload_cleanup(out, cleanup);
        }
    }
}

fn put_abort_multipart_upload_cleanup(out: &mut Vec<u8>, cleanup: &AbortMultipartUploadCleanup) {
    put_multipart_upload_record(out, &cleanup.upload);
    put_u32(
        out,
        u32::try_from(cleanup.parts.len()).expect("abort multipart part count must fit in u32"),
    );
    for part in &cleanup.parts {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(cleanup.streaming_segments.len())
            .expect("abort multipart segment count must fit in u32"),
    );
    for segment in &cleanup.streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_uploads.len())
            .expect("abort multipart stream cleanup count must fit in u32"),
    );
    for stream in &cleanup.stream_uploads {
        put_terminal_stream_cleanup_record(out, stream);
    }
    put_u32(
        out,
        u32::try_from(cleanup.stream_upload_segments.len())
            .expect("abort multipart stream segment cleanup count must fit in u32"),
    );
    for segment in &cleanup.stream_upload_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_stream_upload_part_snapshot(out: &mut Vec<u8>, snapshot: &StreamUploadPartSnapshot) {
    put_stream_upload_record(out, &snapshot.session);
    put_multipart_upload_record(out, &snapshot.upload);
    put_optional_u32(out, snapshot.existing_part_generation);
    put_u32(
        out,
        u32::try_from(snapshot.staging_segments.len())
            .expect("stream part staging segment count must fit in u32"),
    );
    for segment in &snapshot.staging_segments {
        put_stream_upload_segment_record(out, segment);
    }
}

fn put_optional_multipart_part_record(out: &mut Vec<u8>, part: Option<&MultipartPartRecord>) {
    match part {
        None => put_u8(out, 0),
        Some(part) => {
            put_u8(out, 1);
            put_multipart_part_record(out, part);
        }
    }
}

fn put_multipart_part_record(out: &mut Vec<u8>, part: &MultipartPartRecord) {
    put_string(out, part.upload_id.as_str());
    put_u32(out, part.part_number);
    put_u32(out, part.generation);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u64(out, part.last_modified);
    put_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn put_stream_part_finalize_storage_snapshot(
    out: &mut Vec<u8>,
    snapshot: &StreamUploadPartStorageSnapshot,
) {
    put_stream_upload_part_snapshot(out, &snapshot.auth_snapshot);
    put_optional_multipart_part_record(out, snapshot.existing_part.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.displaced_segments.len())
            .expect("stream part displaced segment count must fit in u32"),
    );
    for segment in &snapshot.displaced_segments {
        put_multipart_part_segment_record(out, segment);
    }
}

fn put_create_multipart_upload_req(out: &mut Vec<u8>, request: &CreateMultipartUploadReq) {
    put_string(out, request.upload_id.as_str());
    put_string(out, request.bucket.as_str());
    put_string(out, request.key.as_str());
    put_optional_string(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, request.metadata_blob.as_slice());
    put_bytes(out, request.system_metadata_blob.as_slice());
    put_owner_identity(out, &request.initiator);
    put_owner_identity(out, &request.owner);
    put_acl_grants(out, &request.acl_grants);
    put_bool(out, request.public_read);
    put_object_lock_state(out, request.object_lock);
    put_optional_multipart_checksum_config(out, request.checksum);
    put_object_encryption(out, &request.encryption);
}

fn put_optional_multipart_checksum_config(
    out: &mut Vec<u8>,
    checksum: Option<MultipartChecksumConfig>,
) {
    match checksum {
        None => put_u8(out, 0),
        Some(checksum) => {
            put_u8(out, 1);
            put_u8(out, checksum.algorithm().wire_tag());
            put_u8(out, checksum.checksum_type().wire_tag());
        }
    }
}

fn put_optional_create_multipart_upload_command(
    out: &mut Vec<u8>,
    command: Option<&CreateMultipartUploadCommand>,
) {
    match command {
        None => put_u8(out, 0),
        Some(command) => {
            put_u8(out, 1);
            put_create_multipart_upload_command(out, command);
        }
    }
}

fn put_create_multipart_upload_command(out: &mut Vec<u8>, command: &CreateMultipartUploadCommand) {
    put_multipart_upload_record(out, command.upload());
    put_bucket_write_reservation_proof(out, command.bucket_write_reservation());
}

fn put_multipart_upload_record(out: &mut Vec<u8>, record: &MultipartUploadRecord) {
    put_string(out, record.upload_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.initiated_at);
    put_u8(out, record.state as u8);
    put_optional_string(out, record.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, record.metadata_blob.as_slice());
    put_bytes(out, record.system_metadata_blob.as_slice());
    put_owner_identity(out, &record.initiator);
    put_owner_identity(out, &record.owner);
    put_acl_grants(out, &record.acl_grants);
    put_bool(out, record.public_read);
    put_u64(out, record.object_generation_id.get());
    put_optional_multipart_object_identity(out, record.initiated_object_identity);
    put_object_lock_state(out, record.object_lock);
    put_optional_multipart_checksum_config(out, record.checksum);
    put_object_encryption(out, &record.encryption);
}

fn put_optional_multipart_object_identity(
    out: &mut Vec<u8>,
    identity: Option<MultipartObjectIdentity>,
) {
    match identity {
        None => put_u8(out, 0),
        Some(MultipartObjectIdentity::Live {
            version_id,
            generation_id,
        }) => {
            put_u8(out, 1);
            put_u64(out, version_id.to_u64());
            put_u64(out, generation_id.get());
        }
        Some(MultipartObjectIdentity::DeleteMarker {
            version_id,
            write_sequence,
        }) => {
            put_u8(out, 2);
            put_u64(out, version_id.to_u64());
            put_u64(out, write_sequence);
        }
    }
}

fn put_multipart_completion_replay(out: &mut Vec<u8>, record: &MultipartCompletionReplay) {
    put_string(out, record.upload_id.as_str());
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_bytes(out, record.fingerprint.as_bytes());
    put_u64(out, record.version_id.to_u64());
    put_object_etag(out, record.etag);
    put_u64(out, record.size);
    put_u64(out, record.last_modified);
    put_optional_string(out, record.tags.as_ref().map(|tags| tags.as_str()));
    put_optional_bytes(
        out,
        record
            .system_metadata_blob
            .as_ref()
            .map(|metadata| metadata.as_slice()),
    );
    put_object_encryption(out, &record.encryption);
}

fn put_multipart_completion_snapshot(out: &mut Vec<u8>, snapshot: &MultipartCompletionSnapshot) {
    put_optional_string(out, snapshot.existing_etag.as_deref());
    put_optional_multipart_object_identity(out, snapshot.current_object_identity);
    put_optional_stored_object(out, snapshot.stale_payload_source.as_ref());
    put_u32(
        out,
        u32::try_from(snapshot.part_records.len())
            .expect("multipart completion snapshot part count must fit in u32"),
    );
    for part in &snapshot.part_records {
        put_multipart_part_record(out, part);
    }
    put_u32(
        out,
        u32::try_from(snapshot.selected_streaming_segments.len())
            .expect("multipart completion snapshot segment count must fit in u32"),
    );
    for segment in &snapshot.selected_streaming_segments {
        put_multipart_part_segment_record(out, segment);
    }
    put_complete_multipart_commit_cleanup(out, &snapshot.cleanup);
}

fn put_list_parts_resp(out: &mut Vec<u8>, response: &ListPartsResp) {
    put_u32(
        out,
        u32::try_from(response.parts.len()).expect("multipart parts count must fit in u32"),
    );
    for part in &response.parts {
        put_multipart_part_record(out, part);
    }
    put_bool(out, response.is_truncated);
    put_optional_u32(out, response.next_part_number_marker);
}

fn put_listed_multipart_parts(out: &mut Vec<u8>, listed: &ListedMultipartParts) {
    put_multipart_upload_record(out, &listed.upload);
    put_list_parts_resp(out, &listed.response);
}

fn put_multipart_upload_management_lookup(
    out: &mut Vec<u8>,
    lookup: &MultipartUploadManagementLookup,
) {
    match lookup {
        MultipartUploadManagementLookup::InProgress(upload) => {
            put_u8(out, 0);
            put_multipart_upload_record(out, upload);
        }
        MultipartUploadManagementLookup::NonInProgress(upload) => {
            put_u8(out, 1);
            put_multipart_upload_record(out, upload);
        }
        MultipartUploadManagementLookup::Replay(replay) => {
            put_u8(out, 2);
            put_multipart_completion_replay(out, replay);
        }
        MultipartUploadManagementLookup::Missing => put_u8(out, 3),
    }
}

fn put_optional_delete_object_version_target(
    out: &mut Vec<u8>,
    target: Option<&DeleteObjectVersionTarget>,
) {
    match target {
        None => put_u8(out, 0),
        Some(target) => {
            put_u8(out, 1);
            put_delete_object_version_target(out, target);
        }
    }
}

fn put_delete_object_version_target(out: &mut Vec<u8>, target: &DeleteObjectVersionTarget) {
    match target {
        DeleteObjectVersionTarget::DeleteMarker { write_sequence } => {
            put_u8(out, 0);
            put_u64(out, *write_sequence);
        }
        DeleteObjectVersionTarget::Live {
            generation_id,
            layout,
            payload,
        } => {
            put_u8(out, 1);
            put_u64(out, generation_id.get());
            put_object_layout(out, *layout);
            put_object_payload_reclaim(out, payload);
        }
    }
}

fn put_metadata_command_envelope_response_item(
    out: &mut Vec<u8>,
    command: &crate::metadata_command::MetadataCommandEnvelope,
) {
    put_u64(out, command.checksum_crc64());
    put_bytes(out, &command.command_bytes());
}

fn put_object_read_auth_subject(out: &mut Vec<u8>, subject: &ObjectReadAuthSubject) {
    put_stored_object(out, &subject.stored);
}

fn put_object_read_snapshot(out: &mut Vec<u8>, snapshot: &ObjectReadSnapshot) {
    put_stored_object(out, &snapshot.stored);
    put_u32(
        out,
        u32::try_from(snapshot.object_segments.len())
            .expect("object segment count must fit in u32"),
    );
    for segment in &snapshot.object_segments {
        put_object_segment_record(
            out,
            segment
                .object_record()
                .expect("object snapshot segment must retain its object record"),
        );
    }
    put_u32(
        out,
        u32::try_from(snapshot.multipart_parts.len()).expect("object part count must fit in u32"),
    );
    for part in &snapshot.multipart_parts {
        put_object_part_record(out, part.record());
    }
    put_u32(
        out,
        u32::try_from(snapshot.multipart_part_segments.len())
            .expect("multipart part segment count must fit in u32"),
    );
    for segment in &snapshot.multipart_part_segments {
        put_multipart_part_segment_record(
            out,
            segment
                .multipart_record()
                .expect("multipart snapshot segment must retain its multipart record"),
        );
    }
}

fn put_object_segment_record(out: &mut Vec<u8>, segment: &ObjectSegmentRecord) {
    put_string(out, segment.bucket.as_str());
    put_string(out, segment.key.as_str());
    put_u64(out, segment.version_id.to_u64());
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_object_part_record(out: &mut Vec<u8>, part: &ObjectPartRecord) {
    put_string(out, part.bucket.as_str());
    put_string(out, part.key.as_str());
    put_u64(out, part.version_id.to_u64());
    put_u32(out, part.part_number);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u32(out, part.data_pg_id);
    put_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn put_multipart_part_segment_record(out: &mut Vec<u8>, segment: &MultipartPartSegmentRecord) {
    put_string(out, segment.bucket.as_str());
    put_string(out, segment.key.as_str());
    put_string(out, segment.upload_id.as_str());
    put_u64(out, segment.version_id);
    put_u32(out, segment.part_number);
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn put_optional_version_id(out: &mut Vec<u8>, version_id: Option<VersionId>) {
    match version_id {
        None => put_u8(out, 0),
        Some(version_id) => {
            put_u8(out, 1);
            put_u64(out, version_id.to_u64());
        }
    }
}

fn put_object_read_snapshot_mode(out: &mut Vec<u8>, mode: ObjectReadSnapshotMode) {
    put_u8(
        out,
        match mode {
            ObjectReadSnapshotMode::MetadataOnly => 0,
            ObjectReadSnapshotMode::StandardSegments => 1,
            ObjectReadSnapshotMode::MultipartParts => 2,
            ObjectReadSnapshotMode::FullPayloadLayout => 3,
        },
    );
}

fn put_live_object_record(out: &mut Vec<u8>, record: &LiveObjectRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.version_id.to_u64());
    put_owner_identity(out, &record.owner);
    put_acl_grants(out, &record.acl_grants);
    put_bool(out, record.public_read);
    put_u64(out, record.generation_id.get());
    put_u64(out, record.size);
    put_object_etag(out, record.etag);
    put_u64(out, record.last_modified);
    put_optional_u64(out, record.became_noncurrent_at);
    put_u8(out, record.storage_class as u8);
    put_ec_shape(out, record.ec);
    put_object_layout(out, record.layout);
    put_optional_string(out, record.tags.as_ref().map(|tags| tags.as_str()));
    put_optional_bytes(
        out,
        record.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    put_optional_bytes(
        out,
        record
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    put_object_lock_state(out, record.object_lock);
    put_object_encryption(out, &record.encryption);
}

fn put_delete_marker_record(out: &mut Vec<u8>, record: &DeleteMarkerRecord) {
    put_string(out, record.bucket.as_str());
    put_string(out, record.key.as_str());
    put_u64(out, record.version_id.to_u64());
    put_owner_identity(out, &record.owner);
    put_u64(out, record.last_modified);
}

fn put_owner_identity(out: &mut Vec<u8>, owner: &OwnerIdentity) {
    put_string(out, &owner.principal);
    put_string(out, owner.canonical_id.as_str());
}

fn put_ec_shape(out: &mut Vec<u8>, ec: EcShape) {
    put_u8(out, ec.k);
    put_u8(out, ec.m);
}

fn put_scavenger_observation_key(out: &mut Vec<u8>, key: &ShardScavengerObservationKey) {
    put_u32(out, key.node_id);
    put_u32(out, key.data_pg_id);
    put_u8(out, key.shard_index.get());
    put_bytes(out, key.shard_key.as_bytes());
}

fn put_scavenger_observation_record(
    out: &mut Vec<u8>,
    observation: &ShardScavengerObservationRecord,
) {
    put_scavenger_observation_key(out, &observation.key);
    put_optional_u64(out, observation.data_size);
    put_optional_u64(out, observation.crc64);
    put_bool(out, observation.file_exists);
    put_bool(out, observation.shard_row_exists);
    put_u8(out, observation.reason as u8);
    put_optional_string(out, observation.last_error.as_deref());
}

fn put_scavenger_observation(out: &mut Vec<u8>, observation: &ShardScavengerObservation) {
    put_scavenger_observation_key(out, &observation.key);
    put_u64(out, observation.first_seen_at);
    put_u64(out, observation.last_seen_at);
    put_u64(out, observation.observation_count);
    put_optional_u64(out, observation.data_size);
    put_optional_u64(out, observation.crc64);
    put_bool(out, observation.file_exists);
    put_bool(out, observation.shard_row_exists);
    put_u8(out, observation.reason as u8);
    put_optional_string(out, observation.last_error.as_deref());
    put_optional_u64(out, observation.resolved_at);
}

fn put_segment_stored_bytes_request(out: &mut Vec<u8>, request: &SegmentStoredBytesRequest) {
    put_u32(out, request.data_pg_id);
    out.extend_from_slice(&request.segment_okh);
    put_u64(out, request.segment_vid.get());
    put_u64(out, request.stored_size as u64);
    put_u64(out, request.segment_crc64);
    put_ec_shape(out, request.ec);
}

fn put_placed_segment_shard_repair_work_item(
    out: &mut Vec<u8>,
    work_item: &PlacedSegmentShardRepairWorkItem,
) {
    put_segment_stored_bytes_request(out, &work_item.request);
    put_u8(out, work_item.shard_index.get());
}

fn put_placed_segment_shard_repair_record(
    out: &mut Vec<u8>,
    repair: &PlacedSegmentShardRepairRecord,
) {
    put_placed_segment_shard_repair_work_item(out, &repair.work_item);
    put_u64(out, repair.first_seen_at);
    put_u64(out, repair.last_seen_at);
    put_u64(out, repair.observation_count);
    put_optional_string(out, repair.last_error.as_deref());
}

fn put_placed_segment_shard_repair_claim_record(
    out: &mut Vec<u8>,
    claim: &PlacedSegmentShardRepairClaimRecord,
) {
    put_placed_segment_shard_repair_work_item(out, &claim.work_item);
    put_string(out, &claim.claim_id);
    put_string(out, &claim.owner_token);
    put_u64(out, claim.cluster_epoch.get());
    put_u64(out, claim.claimed_at);
    put_optional_u64(out, claim.lease_deadline);
    put_u64(out, claim.attempt_count);
    put_optional_string(out, claim.last_error.as_deref());
}

fn put_placed_segment_shard_backfill_work_item(
    out: &mut Vec<u8>,
    work_item: &PlacedSegmentShardBackfillWorkItem,
) {
    put_segment_stored_bytes_request(out, &work_item.request);
    put_u64(out, work_item.source_cluster_epoch.get());
    put_u64(out, work_item.desired_cluster_epoch.get());
}

fn put_placed_segment_shard_backfill_record(
    out: &mut Vec<u8>,
    backfill: &PlacedSegmentShardBackfillRecord,
) {
    put_placed_segment_shard_backfill_work_item(out, &backfill.work_item);
    put_u8(out, backfill.remaining_tolerance);
    put_u64(out, backfill.first_seen_at);
    put_u64(out, backfill.last_seen_at);
    put_u64(out, backfill.observation_count);
    put_optional_string(out, backfill.last_error.as_deref());
}

fn put_placed_segment_shard_backfill_claim_record(
    out: &mut Vec<u8>,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) {
    put_placed_segment_shard_backfill_work_item(out, &claim.work_item);
    put_u8(out, claim.remaining_tolerance);
    put_string(out, &claim.claim_id);
    put_string(out, &claim.owner_token);
    put_u64(out, claim.cluster_epoch.get());
    put_u64(out, claim.claimed_at);
    put_optional_u64(out, claim.lease_deadline);
    put_u64(out, claim.attempt_count);
    put_optional_string(out, claim.last_error.as_deref());
}

fn put_scavenger_payload_reference(out: &mut Vec<u8>, reference: &ShardScavengerPayloadReference) {
    match reference {
        ShardScavengerPayloadReference::Placed(reference) => {
            put_u8(out, 0);
            put_u32(out, reference.data_pg_id);
            out.extend_from_slice(&reference.okh);
            put_u64(out, reference.generation_id.get());
            put_u64(out, reference.placement_cluster_epoch.get());
            put_u64(out, reference.stored_size);
            put_u64(out, reference.crc64);
            put_ec_shape(out, reference.ec);
        }
        ShardScavengerPayloadReference::ReclaimOnly(reference) => {
            put_u8(out, 1);
            put_u32(out, reference.data_pg_id);
            out.extend_from_slice(&reference.okh);
            put_u64(out, reference.generation_id.get());
            put_ec_shape(out, reference.ec);
        }
    }
}

fn put_placed_scavenger_reference(
    out: &mut Vec<u8>,
    reference: &ShardScavengerPlacedShardSetReference,
) {
    put_u32(out, reference.data_pg_id);
    out.extend_from_slice(&reference.okh);
    put_u64(out, reference.generation_id.get());
    put_u64(out, reference.placement_cluster_epoch.get());
    put_u64(out, reference.stored_size);
    put_u64(out, reference.crc64);
    put_ec_shape(out, reference.ec);
}

fn put_placed_segment_backfill_reference_cursor(
    out: &mut Vec<u8>,
    cursor: &PlacedSegmentBackfillReferenceCursor,
) {
    match cursor {
        PlacedSegmentBackfillReferenceCursor::ObjectSegment {
            bucket,
            key,
            version_id,
            segment_index,
        } => {
            put_u8(out, 0);
            put_string(out, bucket.as_str());
            put_string(out, key.as_str());
            put_u64(out, *version_id);
            put_u32(out, *segment_index);
        }
        PlacedSegmentBackfillReferenceCursor::StreamUploadSegment {
            session_id,
            segment_index,
        } => {
            put_u8(out, 1);
            put_string(out, session_id.as_str());
            put_u32(out, *segment_index);
        }
        PlacedSegmentBackfillReferenceCursor::MultipartPartSegment {
            bucket,
            key,
            upload_id,
            part_number,
            segment_index,
        } => {
            put_u8(out, 2);
            put_string(out, bucket.as_str());
            put_string(out, key.as_str());
            put_string(out, upload_id.as_str());
            put_u32(out, *part_number);
            put_u32(out, *segment_index);
        }
        PlacedSegmentBackfillReferenceCursor::PendingCommand {
            cluster_epoch,
            pg_id,
            log_index,
            command_checksum,
            reference_index,
        } => {
            put_u8(out, 3);
            put_u64(out, cluster_epoch.get());
            put_u32(out, pg_id.get());
            put_u64(out, *log_index);
            put_u64(out, *command_checksum);
            put_u32(out, *reference_index);
        }
    }
}

fn put_object_etag(out: &mut Vec<u8>, etag: ObjectEtag) {
    match etag {
        ObjectEtag::SinglePart(crc64) => {
            put_u8(out, 0);
            out.extend_from_slice(&crc64);
        }
        ObjectEtag::MultipartComposite { crc64, parts } => {
            put_u8(out, 1);
            out.extend_from_slice(&crc64);
            put_u32(out, parts.get());
        }
    }
}

fn put_object_layout(out: &mut Vec<u8>, layout: ObjectLayout) {
    match layout {
        ObjectLayout::Standard => put_u8(out, 0),
        ObjectLayout::MultipartManifest { parts_count } => {
            put_u8(out, 1);
            put_u32(out, parts_count.get());
        }
    }
}

fn put_object_lock_state(out: &mut Vec<u8>, object_lock: ObjectLockState) {
    match object_lock.retention {
        None => put_u8(out, 0),
        Some(retention) => {
            put_u8(out, 1);
            put_u64(out, retention.retain_until_unix_seconds);
            put_u8(out, retention.mode as u8);
        }
    }
    put_u8(out, object_lock.legal_hold as u8);
}

fn put_object_encryption(out: &mut Vec<u8>, encryption: &ObjectEncryption) {
    put_u8(out, encryption.encryption_type() as u8);
    put_optional_bytes(out, encryption.encode_state().as_deref());
}

fn put_bucket_object_lock_config(out: &mut Vec<u8>, config: &BucketObjectLockConfig) {
    put_bool(out, config.enabled);
    match config.default_retention {
        None => put_u8(out, 0),
        Some(retention) => {
            put_u8(out, 1);
            put_u8(out, retention.mode as u8);
            match retention.period {
                RetentionPeriod::Days(days) => {
                    put_u32(out, days.get());
                    put_u8(out, 0);
                }
                RetentionPeriod::Years(years) => {
                    put_u32(out, years.get());
                    put_u8(out, 1);
                }
            }
        }
    }
}

fn put_optional_public_access_block_config(
    out: &mut Vec<u8>,
    config: Option<PublicAccessBlockConfig>,
) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_bool(out, config.block_public_acls);
            put_bool(out, config.ignore_public_acls);
            put_bool(out, config.block_public_policy);
            put_bool(out, config.restrict_public_buckets);
        }
    }
}

fn put_optional_bucket_ownership_controls(
    out: &mut Vec<u8>,
    controls: Option<BucketOwnershipControls>,
) {
    match controls {
        None => put_u8(out, 0),
        Some(controls) => {
            put_u8(out, 1);
            put_u8(out, controls.object_ownership as u8);
        }
    }
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    put_u8(out, u8::from(value));
}

fn put_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
    }
}

fn put_optional_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u32(out, value);
        }
    }
}

fn put_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_string(out, value);
        }
    }
}

fn put_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_bytes(out, value);
        }
    }
}

fn read_u16_from<R: Read>(reader: &mut R) -> Result<u16, std::io::Error> {
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32_from<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_from<R: Read>(reader: &mut R) -> Result<u64, std::io::Error> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
