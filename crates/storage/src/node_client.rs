use std::collections::HashMap;
use std::sync::Arc;

use placement::NodeId;
use s3_types::{AclGrants, BucketVersioningState};

use crate::error::{BucketSnapshotLoadError, MetadataError, ObjectPgActionError, StoreError};
use crate::metadata_command::{
    BucketPropertyMutation, BucketRecord, BucketSubresourceMutation, MarkBucketDeletingCommand,
    MetadataCommandAcceptance, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload,
    MetadataCommandReplicaState, ObjectPayloadReclaimCommand, PutBucketAclCommand,
    PutBucketPropertyCommand, PutBucketSubresourceCommand, PutBucketVersioningCommand,
};
use crate::node::SharedStorageNode;
use crate::pg_store::ScavengerShardFileScan;
use crate::traits::PgMetadataStore;
use crate::types::{
    AuthorizedMultipartUploadRecord, BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot,
    BucketFastPathIdentity, BucketInfo, BucketName, BucketSnapshot, BucketSnapshotPair,
    BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState, BucketSubresourceKind,
    BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch, DataPgId, GenerationId,
    LifecycleSweepBuckets, LifecycleSweepClaimRecord, LifecycleSweepRoot, ListPartsReq,
    ListedMultipartParts, MultipartCompletionPreflight, MultipartCompletionSnapshot,
    MultipartUploadManagementLookup, MultipartUploadRecord, ObjectKey,
    ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind, ObjectReadAuthSubject,
    ObjectReadAuthSubjectIdentity, ObjectReadSnapshot, ObjectReadSnapshotMode, PayloadReclaimRoot,
    PgId, PrepareStreamUploadSegmentAppendReq, SessionId, ShardKey, StoredObject,
    StreamUploadRecord, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId,
    UploadState, WriteAck,
};

fn merge_bucket_snapshot_pair_request(
    source: BucketSnapshotRequest,
    destination: BucketSnapshotRequest,
) -> BucketSnapshotRequest {
    BucketSnapshotRequest {
        policy: source.policy || destination.policy,
        tags: match (source.tags, destination.tags) {
            (BucketSnapshotTagsRequest::Always, _) | (_, BucketSnapshotTagsRequest::Always) => {
                BucketSnapshotTagsRequest::Always
            }
            (BucketSnapshotTagsRequest::IfBucketAbacEnabled, _)
            | (_, BucketSnapshotTagsRequest::IfBucketAbacEnabled) => {
                BucketSnapshotTagsRequest::IfBucketAbacEnabled
            }
            (BucketSnapshotTagsRequest::NotRequested, BucketSnapshotTagsRequest::NotRequested) => {
                BucketSnapshotTagsRequest::NotRequested
            }
        },
        lifecycle: source.lifecycle || destination.lifecycle,
        cors: source.cors || destination.cors,
    }
}

fn load_multipart_upload_from_pg(
    pg: &crate::PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
) -> Result<MultipartUploadRecord, MetadataError> {
    let upload = pg.get_multipart_upload(upload_id)?;
    if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
        return Err(MetadataError::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    Ok(upload)
}

fn load_in_progress_multipart_upload_from_pg(
    pg: &crate::PgStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
) -> Result<MultipartUploadRecord, MetadataError> {
    let upload = load_multipart_upload_from_pg(pg, bucket, key, upload_id)?;
    if upload.state != UploadState::InProgress {
        return Err(MetadataError::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    Ok(upload)
}

fn validate_stream_upload_session_binding(
    session: &StreamUploadRecord,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<(), ObjectPgActionError> {
    if session.state != StreamUploadState::InProgress {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "stream session is not in progress".to_string(),
        });
    }
    if session.bucket != bucket.as_str() || session.key != key.as_str() {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "session bucket/key mismatch".to_string(),
        });
    }
    Ok(())
}

fn reject_duplicate_stream_segment_index(
    pg: &crate::PgStore,
    session_id: &SessionId,
    segment_index: u32,
) -> Result<(), ObjectPgActionError> {
    let existing_segments = pg.list_stream_segments(session_id)?;
    if existing_segments
        .iter()
        .any(|segment| segment.segment_index == segment_index)
    {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: format!("duplicate segment_index {segment_index}"),
        });
    }
    Ok(())
}

fn pending_bucket_command_matches_current(
    current: BucketRecord,
    target: &BucketRecord,
    build_expected: impl FnOnce(BucketRecord) -> Result<BucketRecord, BucketSnapshotLoadError>,
) -> Result<bool, BucketSnapshotLoadError> {
    if current.bucket_execution_generation == target.bucket_execution_generation {
        return Ok(current.command_metadata_eq(target));
    }
    if current.bucket_execution_generation > target.bucket_execution_generation {
        return Ok(false);
    }
    Ok(build_expected(current)?.command_metadata_eq(target))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarkBucketDeletingCommandBuild {
    AlreadyDeleting,
    Command(Box<MetadataCommandEnvelope>),
}

pub(crate) trait StorageNodeClient: Send + Sync {
    fn node_id(&self) -> NodeId;

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError>;

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Vec<u8>, StoreError>;

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError>;

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError>;

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError>;

    fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool;

    fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize;

    fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool;

    fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    );

    fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    );

    fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize;

    #[cfg(any(test, feature = "test-hooks"))]
    fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize;

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError>;

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError>;

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError>;

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError>;

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError>;

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError>;

    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError>;

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError>;

    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_record_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketRecord, BucketSnapshotLoadError>;

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError>;

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_acl_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError>;

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError>;

    fn load_existing_live_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError>;

    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError>;

    fn load_object_tag_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: s3_types::VersionId,
    ) -> Result<Option<String>, ObjectPgActionError>;

    fn load_object_legal_hold_read_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_retention_read_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError>;

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_multipart_completion_snapshot(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError>;

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError>;

    fn list_multipart_parts_for_authorized_upload(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError>;

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError>;

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError>;

    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<s3_types::VersionId, ObjectPgActionError>;

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError>;

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn load_bucket_execution_generations(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError>;

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError>;

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError>;

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
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError>;

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError>;

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError>;

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError>;

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError>;

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError>;

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError>;
}

pub(crate) struct LocalStorageNodeClient {
    node_id: NodeId,
    storage_node: Arc<SharedStorageNode>,
}

impl LocalStorageNodeClient {
    pub(crate) fn new(node_id: NodeId, storage_node: Arc<SharedStorageNode>) -> Self {
        Self {
            node_id,
            storage_node,
        }
    }
}

impl StorageNodeClient for LocalStorageNodeClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.storage_node
            .write_shard_file(data_pg_id.get(), key, data)
    }

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Vec<u8>, StoreError> {
        self.storage_node.read_shard_file(data_pg_id.get(), key)
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.storage_node
            .read_shard_file_into(data_pg_id.get(), key, dst)
    }

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError> {
        self.storage_node.delete_shard_file(data_pg_id.get(), key)
    }

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        self.storage_node
            .list_scavenger_shard_files(data_pg_id.get())
    }

    fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.storage_node
            .try_acquire_object_payload_lease(bucket, key, generation_id)
    }

    fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.storage_node
            .release_object_payload_lease(bucket, key, generation_id)
    }

    fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.storage_node
            .try_begin_object_payload_reclaim(bucket, key, generation_id)
    }

    fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    ) {
        self.storage_node
            .finish_object_payload_reclaim(bucket, key, generation_id, keep_fence);
    }

    fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.storage_node
            .clear_object_payload_reclaim_fence(bucket, key, generation_id);
    }

    fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.storage_node
            .object_payload_lease_count(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        self.storage_node.bucket_object_payload_lease_count(bucket)
    }

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.max_metadata_command_log_index(cluster_epoch)
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.pending_metadata_command_envelope(self.node_id.as_u32(), cluster_epoch)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.try_insert_pending_metadata_command_slot(self.node_id.as_u32(), command, bucket)
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.try_insert_bucket_control_pending_metadata_command_slot(
            self.node_id.as_u32(),
            command,
            bucket,
        )
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.remove_pending_metadata_command_slot(self.node_id.as_u32(), command)
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        pg_id: PgId,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.replace_pending_metadata_command_slot_for_reissue(
            self.node_id.as_u32(),
            previous,
            replacement,
            bucket,
        )
    }

    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_drain(&*pg, bucket)?.is_some())
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        )?)
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &proof.bucket,
            &proof.reservation_id,
        )?
        else {
            return Err(MetadataError::BucketWriteReservationNotFound {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        };
        if !proof.matches_record(&record) {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        let current_bucket = PgMetadataStore::head_bucket_raw(&*pg, &proof.bucket)?;
        if current_bucket.state == BucketState::Active
            && current_bucket.bucket_incarnation_generation == proof.bucket_incarnation_generation
        {
            Ok(())
        } else {
            Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into())
        }
    }

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_durable_bucket_write_reservation(
            &*pg,
            &record.bucket,
            &record.reservation_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
            record.bucket_incarnation_generation,
        )?)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        if let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*pg,
            &proof.bucket,
            &proof.reservation_id,
        )? {
            if !proof.matches_record(&record) {
                return Err(MetadataError::BucketWriteReservationConflict {
                    reservation_id: proof.reservation_id.clone(),
                }
                .into());
            }
        }
        Ok(
            PgMetadataStore::release_metadata_command_bucket_write_reservation(
                &*pg,
                &proof.bucket,
                &proof.reservation_id,
                &proof.owner_token,
                proof.cluster_epoch,
                proof.bucket_execution_generation,
                proof.bucket_incarnation_generation,
            )?,
        )
    }

    fn begin_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            created_at,
            lease_deadline,
        )?)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::clear_durable_bucket_write_drain(
            &*pg,
            &record.bucket,
            &record.drain_id,
            &record.owner_token,
            record.cluster_epoch,
            record.bucket_execution_generation,
        )?)
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::clear_expired_durable_bucket_write_drain(
            &*pg, bucket, now,
        )?)
    }

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::durable_bucket_write_reservations(
            &*pg, bucket,
        )?)
    }

    fn load_bucket_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_bucket_snapshot_from_pg(&pg, bucket, request)
    }

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: PgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: PgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        if source.0 == destination.0 {
            let merged_request = merge_bucket_snapshot_pair_request(source.1, destination.1);
            let pg = self.storage_node.get_pg(source_pg_id.get())?;
            let bucket =
                SharedStorageNode::load_bucket_snapshot_from_pg(&pg, source.0, merged_request)?;
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let guards = self
            .storage_node
            .lock_bucket_pair_pgs(source_pg_id.get(), destination_pg_id.get())?;
        match guards {
            crate::node::BucketPairPgGuards::Same { bucket } => Ok(BucketSnapshotPair::Distinct {
                source: Box::new(SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket, source.0, source.1,
                )?),
                destination: Box::new(SharedStorageNode::load_bucket_snapshot_from_pg(
                    &bucket,
                    destination.0,
                    destination.1,
                )?),
            }),
            crate::node::BucketPairPgGuards::Distinct {
                source: source_pg,
                destination: destination_pg,
            } => Ok(BucketSnapshotPair::Distinct {
                source: Box::new(SharedStorageNode::load_bucket_snapshot_from_pg(
                    &source_pg, source.0, source.1,
                )?),
                destination: Box::new(SharedStorageNode::load_bucket_snapshot_from_pg(
                    &destination_pg,
                    destination.0,
                    destination.1,
                )?),
            }),
        }
    }

    fn head_bucket_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket_raw(&*pg, bucket)?)
    }

    fn head_bucket_info(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket(&*pg, bucket)?)
    }

    fn head_bucket_record_raw(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<BucketRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?)
    }

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(MarkBucketDeletingCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
            )
            .bucket)
        })
    }

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        if current.state == BucketState::Deleting {
            return Ok(MarkBucketDeletingCommandBuild::AlreadyDeleting);
        }
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MarkBucketDeletingCommandBuild::Command(Box::new(
            MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                    current.with_execution_generation(bucket_execution_generation),
                )),
            ),
        )))
    }

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            if state == BucketVersioningState::Disabled
                && record.versioning != BucketVersioningState::Disabled
            {
                return Err(MetadataError::InvalidVersioningTransition {
                    from: record.versioning,
                    to: state,
                }
                .into());
            }
            Ok(PutBucketVersioningCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                state,
            )
            .bucket)
        })
    }

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        if state == BucketVersioningState::Disabled
            && current.versioning != BucketVersioningState::Disabled
        {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current.versioning,
                to: state,
            }
            .into());
        }
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current.with_execution_generation(bucket_execution_generation),
                state,
            )),
        ))
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
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(PutBucketAclCommand::from_bucket(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                acl_grants.clone(),
                public_read,
                public_write,
            )
            .bucket)
        })
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
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                current.with_execution_generation(bucket_execution_generation),
                acl_grants.clone(),
                public_read,
                public_write,
            )),
        ))
    }

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        pending_bucket_command_matches_current(current, &command.bucket, |record| {
            Ok(PutBucketPropertyCommand::from_bucket_and_mutation(
                record.with_execution_generation(command.bucket.bucket_execution_generation),
                mutation.clone(),
            )
            .bucket)
        })
    }

    fn build_put_bucket_property_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let current = PgMetadataStore::head_bucket_record_raw(&*pg, bucket)?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketProperty(
                PutBucketPropertyCommand::from_bucket_and_mutation(
                    current.with_execution_generation(bucket_execution_generation),
                    mutation.clone(),
                ),
            ),
        ))
    }

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let bucket_execution_generation = pg.next_bucket_execution_generation_candidate()?;
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                mutation.clone(),
                bucket_execution_generation,
            )),
        ))
    }

    fn get_bucket_subresource(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_subresource(&*pg, bucket, kind)?.map(|stored| stored.body))
    }

    fn list_buckets(
        &self,
        pg_id: PgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::list_buckets(&*pg, owner_canonical_id)?)
    }

    fn load_existing_live_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(SharedStorageNode::load_existing_live_object_from_object_pg(
            &pg, bucket, key,
        )?)
    }

    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_read_auth_subject_from_object_pg(
            &pg, bucket, key, version_id,
        )
    }

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_read_snapshot_for_subject_from_object_pg(
            &pg,
            bucket,
            key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }

    fn load_object_tag_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_tag_read_auth_subject_from_object_pg(
            &pg, bucket, key, version_id,
        )
    }

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: s3_types::VersionId,
    ) -> Result<Option<String>, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::get_object_tags_for_subject_from_object_pg(
            &pg,
            bucket,
            key,
            version_id,
            expected_identity,
            authorized_version_id,
        )
    }

    fn load_object_legal_hold_read_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_legal_hold_read_subject_from_object_pg(
            &pg, bucket, key, version_id,
        )
    }

    fn load_object_retention_read_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        SharedStorageNode::load_object_retention_read_subject_from_object_pg(
            &pg, bucket, key, version_id,
        )
    }

    fn load_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(load_multipart_upload_from_pg(&pg, bucket, key, upload_id)?)
    }

    fn load_in_progress_multipart_upload(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(load_in_progress_multipart_upload_from_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.load_in_progress_multipart_upload(pg_id, bucket, key, upload_id)
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
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let mut part_records = Vec::with_capacity(requested_part_numbers.len());
        for &part_number in requested_part_numbers {
            part_records.push(pg.get_multipart_part(upload_id, part_number)?);
        }
        Ok(MultipartCompletionSnapshot {
            existing_etag,
            part_records,
        })
    }

    fn load_multipart_completion_preflight(
        &self,
        pg_id: PgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        Ok(MultipartCompletionPreflight { existing_etag })
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
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let upload = load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let response = pg.list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })?;
        Ok(ListedMultipartParts { upload, response })
    }

    fn lookup_multipart_upload_management(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        match load_multipart_upload_from_pg(&pg, bucket, key, upload_id) {
            Ok(upload) if upload.state == UploadState::InProgress => {
                return Ok(MultipartUploadManagementLookup::InProgress(Box::new(
                    upload,
                )));
            }
            Ok(upload) => {
                return Ok(MultipartUploadManagementLookup::NonInProgress(Box::new(
                    upload,
                )));
            }
            Err(MetadataError::NoSuchUpload { .. }) => {}
            Err(error) => return Err(error.into()),
        }

        if let Some(completed) = pg.get_completed_multipart_upload(upload_id)? {
            if completed.bucket == *bucket && completed.key == *key {
                return Ok(MultipartUploadManagementLookup::Completed(completed));
            }
        }
        Ok(MultipartUploadManagementLookup::Missing)
    }

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::payload_reclaim_exists(
            &*pg,
            bucket,
            key,
            generation_id,
        )?)
    }

    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<s3_types::VersionId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::next_version_id(&*pg, bucket, key)?)
    }

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::next_generation_id(&*pg, bucket, key)?)
    }

    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_object_generation_reservation(
            &*pg,
            bucket,
            key,
            reservation_id,
        )?)
    }

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(session_id)?;
        validate_stream_upload_session_binding(&session, bucket, key)?;
        Ok(session)
    }

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        let session = pg.get_stream_upload(&request.session_id)?;
        validate_stream_upload_session_binding(&session, bucket, key)?;
        reject_duplicate_stream_segment_index(&pg, &request.session_id, request.segment_index)?;
        let pg_topology = self.storage_node.pg_topology();
        let (segment_okh, segment_vid, data_pg_id) = match session.target {
            StreamUploadTarget::PutObject => {
                let generation_id =
                    pg.get_object_generation_reservation(bucket, key, &request.session_id)?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    crate::segment_key_hash(
                        bucket.as_str(),
                        key.as_str(),
                        generation_id,
                        request.segment_index,
                    ),
                    segment_vid,
                    pg_topology
                        .object_generation_segment_data_pg(
                            bucket,
                            key,
                            generation_id,
                            request.segment_index,
                        )
                        .get(),
                )
            }
            StreamUploadTarget::UploadPart {
                ref upload_id,
                part_number,
            } => {
                let upload =
                    load_in_progress_multipart_upload_from_pg(&pg, bucket, key, upload_id)?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    request.segment_okh,
                    segment_vid,
                    pg_topology
                        .object_generation_multipart_part_segment_data_pg(
                            bucket,
                            key,
                            upload.object_generation_id,
                            part_number,
                            request.segment_index,
                        )
                        .get(),
                )
            }
        };
        let ec = self.storage_node.default_ec_shape();
        let segment_record = StreamUploadSegmentRecord {
            session_id: request.session_id.clone(),
            segment_index: request.segment_index,
            size: request.size,
            segment_crc64: request.segment_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            ec_k: ec.k,
            ec_m: ec.m,
        };
        Ok((session.target, segment_record))
    }

    fn load_bucket_execution_generations(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.load_bucket_execution_generations(buckets)?)
    }

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: PgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(pg.load_bucket_fast_path_identities(buckets)?)
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_payload_reclaim_root(
            &*pg, bucket,
        )?)
    }

    fn get_payload_reclaim_root(
        &self,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_payload_reclaim_root(&*pg)?)
    }

    fn get_object_payload_reclaim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        if let Some(reclaim) =
            PgMetadataStore::get_object_segments_reclaim(&*pg, bucket, key, generation_id)?
        {
            Ok(Some(ObjectPayloadReclaimCommand::Segments(reclaim)))
        } else {
            Ok(
                PgMetadataStore::get_multipart_reclaim(&*pg, bucket, key, generation_id)?
                    .map(ObjectPayloadReclaimCommand::Multipart),
            )
        }
    }

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
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_object_payload_reclaim_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            key,
            generation_id,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn release_object_payload_reclaim_claim(
        &self,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_object_payload_reclaim_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.key,
            claim.generation_id,
            claim.reclaim_kind,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_bucket_delete_finalize_roots(
            &*pg, now, limit,
        )?)
    }

    fn acquire_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_bucket_delete_finalize_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        pg_id: PgId,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_bucket_delete_finalize_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: PgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::get_lifecycle_sweep_roots(
            &*pg, now, limit,
        )?)
    }

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: PgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(LifecycleSweepBuckets {
            lifecycle_buckets: PgMetadataStore::list_buckets_with_lifecycle(&*pg)?,
            aborting_buckets: PgMetadataStore::list_buckets_with_aborting_multipart_uploads(&*pg)?,
        })
    }

    fn acquire_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::acquire_lifecycle_sweep_claim(
            &*pg,
            bucket,
            bucket_incarnation_generation,
            claim_id,
            owner_token,
            cluster_epoch,
            claimed_at,
            lease_deadline,
            now,
        )?)
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::heartbeat_lifecycle_sweep_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
            heartbeat_at,
            lease_deadline,
        )?)
    }

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::record_lifecycle_sweep_claim_error(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
            last_error,
        )?)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        pg_id: PgId,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        Ok(PgMetadataStore::release_lifecycle_sweep_claim(
            &*pg,
            &claim.bucket,
            claim.bucket_incarnation_generation,
            &claim.claim_id,
            &claim.owner_token,
            claim.cluster_epoch,
        )?)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_replica_state()
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_acceptance(self.node_id.as_u32(), command)
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_abandon_acceptance(self.node_id.as_u32(), command)
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.applied_metadata_command_log_entry_hashes(self.node_id.as_u32(), command)
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.has_matching_applied_metadata_command_log_entry(
            self.node_id.as_u32(),
            command,
            expected_previous_log_hash,
        )
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.apply_metadata_command_and_record(self.node_id.as_u32(), command)
    }

    fn record_metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.record_metadata_command_abandoned(self.node_id.as_u32(), command)
    }

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let pg = self.storage_node.get_pg(pg_id.get())?;
        pg.metadata_command_abandoned(self.node_id.as_u32(), command)
    }
}
