use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarkBucketDeletingCommandBuild {
    AlreadyDeleting,
    Command(Box<MetadataCommandEnvelope>),
}

#[derive(Debug, Clone)]
pub(crate) enum CreateBucketCommandBuild {
    Exists(BucketInfo),
    Command(Box<MetadataCommandEnvelope>),
}

pub(crate) trait BucketMetadataNodeClient: Send + Sync {
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

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError>;

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError>;

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
}

pub(crate) trait BucketWriteReservationNodeClient: Send + Sync {
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

    #[allow(clippy::too_many_arguments)]
    fn acquire_completion_durable_bucket_write_reservation(
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
        self.acquire_durable_bucket_write_reservation(
            pg_id,
            bucket,
            reservation_id,
            owner_token,
            cluster_epoch,
            operation_kind,
            created_at,
            lease_deadline,
            target_context,
        )
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_metadata_command_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
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

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError>;

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
}

pub(crate) trait ObjectGenerationMetadataNodeClient: Send + Sync {
    fn object_generation_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn next_object_generation_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError>;
}

pub(crate) trait ObjectVersionMetadataNodeClient: Send + Sync {
    fn next_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError>;

    fn next_completion_object_version_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id(pg_id, bucket, key)
    }
}

pub(crate) trait DirectPutMetadataNodeClient: Send + Sync {
    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError>;

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait ObjectListingMetadataNodeClient: Send + Sync {
    fn list_objects_page(
        &self,
        pg_id: PgId,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError>;

    fn list_object_versions_page(
        &self,
        pg_id: PgId,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError>;

    fn list_multipart_uploads_page(
        &self,
        pg_id: PgId,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError>;
}

pub(crate) trait ObjectMutationMetadataNodeClient: Send + Sync {
    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError>;

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError>;

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError>;

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError>;

    fn load_stream_upload_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError>;

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

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError>;

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError>;

    fn payload_reclaim_exists(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError>;

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

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError>;

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError>;

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_multipart_completion_stale_payload_source(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError>;

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError>;

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;
}

pub(crate) trait ObjectReadMetadataNodeClient: Send + Sync {
    fn load_object_read_auth_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_read_snapshot_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError>;

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: VersionId,
    ) -> Result<Option<String>, ObjectPgActionError>;
}

pub(crate) struct BuildStreamPutCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) session_id: &'a SessionId,
    pub(crate) total_size: u64,
    pub(crate) expected_snapshot: &'a StreamPutFinalizeStorageSnapshot,
    pub(crate) commit: &'a StreamPutCommitInput,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDirectPutCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CommitDirectPutObjectReq,
    pub(crate) version_id: VersionId,
    pub(crate) expected_snapshot: &'a DirectPutCommitStorageSnapshot,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) enum CreateStreamUploadPrecondition<'a> {
    PutObjectNoCurrentCheck {
        require_generation_reservation: bool,
    },
    PutObject {
        expected_current: Option<&'a StoredObject>,
        require_generation_reservation: bool,
    },
    UploadPart {
        expected_upload: &'a MultipartUploadRecord,
    },
}

pub(crate) struct BuildCreateStreamUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CreateStreamUploadReq,
    pub(crate) precondition: CreateStreamUploadPrecondition<'a>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCreateMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CreateMultipartUploadReq,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildStreamPartCommitCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) session_id: &'a SessionId,
    pub(crate) part_number: u32,
    pub(crate) expected_snapshot: &'a StreamUploadPartStorageSnapshot,
    pub(crate) part: &'a MultipartPartRecord,
    pub(crate) segments: &'a [MultipartPartSegmentRecord],
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCompleteMultipartObjectCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) request: &'a CompleteMultipartCommitRequest,
    pub(crate) version_id: VersionId,
    pub(crate) expected_object_parts: &'a [ObjectPartRecord],
    pub(crate) completion_order: u64,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct AbortMultipartCommandValidation<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildAbortMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

pub(crate) struct BuildAuthorizedAbortMultipartUploadCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) authorized_upload: &'a AuthorizedMultipartUploadRecord,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

pub(crate) struct BuildPutObjectMetadataCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) requested_version_id: Option<VersionId>,
    pub(crate) expected_stored: &'a StoredObject,
    pub(crate) version_id: VersionId,
    pub(crate) mutation: PutObjectMetadataMutation,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteSpecificObjectVersionCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) version_id: VersionId,
    pub(crate) expected_stored: Option<&'a StoredObject>,
    pub(crate) expected_target: Option<&'a DeleteObjectVersionTarget>,
    pub(crate) expected_version_list: Option<&'a [StoredObject]>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteCurrentObjectCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) expected_target: Option<&'a DeleteObjectVersionTarget>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectDeleteStorageSnapshot {
    pub(crate) stored: Option<StoredObject>,
    pub(crate) target: Option<DeleteObjectVersionTarget>,
}

pub(crate) struct BuildInsertDeleteMarkerCommandReq<'a> {
    pub(crate) pg_id: PgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) version_id: VersionId,
    pub(crate) owner: &'a OwnerIdentity,
    pub(crate) stale_payload: InsertDeleteMarkerStalePayload,
    pub(crate) expected_stale_payload_source: Option<&'a StoredObject>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) enum InsertDeleteMarkerStalePayload {
    Explicit(Option<ObjectPayloadReclaimCommand>),
    SnapshotCurrentNullLive { created_at: u64 },
}

pub(crate) trait PlacedShardNodeClient: Send + Sync {
    fn node_id(&self) -> NodeId;

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError>;

    fn repair_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError>;

    fn read_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError>;

    /// Non-serving read used by repair/backfill to inspect reconstructed
    /// historical placements without treating them as current route authority.
    fn read_placed_shard_for_historical_inspection(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError>;

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError>;

    fn delete_placed_shard(&self, data_pg_id: DataPgId, key: &ShardKey) -> Result<(), StoreError>;

    fn delete_placed_shard_for_historical_cleanup(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        self.delete_placed_shard(location.data_pg_id(), key)
    }
}

pub(crate) trait ShardReadHandleLease: Send {
    fn release(&mut self) -> Result<(), StoreError>;
}

pub(crate) trait ShardReadHandleNodeClient: Send + Sync {
    fn acquire_read_handles(
        &self,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleLease>, StoreError>;
}

pub(crate) trait ShardAckNodeClient: Send + Sync {
    fn register_written_shard_acks(
        &self,
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError>;

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError>;

    fn load_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<WriteAck, StoreError>;

    fn load_written_shard_ack_for_historical_inspection(
        &self,
        pg_id: PgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError>;

    fn delete_written_shard_ack(&self, pg_id: PgId, key: &ShardKey) -> Result<(), StoreError>;

    fn delete_written_shard_ack_at_retained_epoch(
        &self,
        _cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        key: &ShardKey,
    ) -> Result<(), StoreError> {
        self.delete_written_shard_ack(pg_id, key)
    }

    fn record_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError>;

    fn list_placed_segment_shard_repairs(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError>;

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError>;

    fn complete_placed_segment_shard_repair_claim(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError>;

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError>;

    fn resolve_placed_segment_shard_repair(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError>;

    fn record_placed_segment_shard_backfill(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError>;

    fn list_placed_segment_shard_backfills(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError>;

    fn count_placed_segment_shard_backfills(&self, pg_id: PgId) -> Result<usize, StoreError>;

    fn placed_segment_shard_backfill_exists(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError>;

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        pg_id: PgId,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError>;

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError>;

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError>;

    fn resolve_placed_segment_shard_backfill(
        &self,
        pg_id: PgId,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError>;
}

pub(crate) trait ShardScavengerNodeClient: Send + Sync {
    fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<crate::PgClusterMapHistoryReferenceSummary, StoreError>;

    fn list_scavenger_shard_files(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<ScavengerShardFileScan, StoreError>;

    fn list_scavenger_shard_rows(&self, pg_id: PgId) -> Result<Vec<ScavengerShardRow>, StoreError>;

    fn list_shard_scavenger_payload_references(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError>;

    fn record_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError>;

    fn list_shard_scavenger_observations(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError>;

    fn resolve_shard_scavenger_observation(
        &self,
        pg_id: PgId,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError>;
}

pub(crate) trait MetadataCommandNodeClient: Send + Sync {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandNodeClient>, StoreError>;

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError>;

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError>;

    fn next_completion_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least(pg_id, cluster_epoch, min_log_index)
    }

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

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError>;

    fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError>;

    fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError>;

    fn validate_metadata_command_replay_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_replica_state_can_initialize(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<bool, StoreError>;

    fn initialize_metadata_transfer_empty_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn initialize_metadata_transfer_matching_state(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        commands: &[MetadataTransferCommand],
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    #[allow(dead_code)]
    fn install_metadata_transfer_checkpoint_base(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        checkpoint: &MetadataCommandCheckpoint,
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

    #[allow(dead_code)]
    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError>;

    #[allow(dead_code)]
    fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError>;

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

    fn replay_metadata_command_for_peering(
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

pub(crate) trait StorageNodeClient:
    ShardScavengerNodeClient + MetadataCommandNodeClient
{
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

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        pg_id: PgId,
        proof: &crate::BucketWriteReservationProof,
        lease_deadline: u64,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

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

    fn delete_finalized_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError>;

    fn build_create_bucket_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError>;

    fn build_advance_completed_multipart_upload_sequence_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError>;

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

    fn load_put_object_metadata_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError>;

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_current_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn load_specific_object_delete_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn list_object_versions_for_lifecycle(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError>;

    fn build_delete_specific_object_version_command(
        &self,
        request: BuildDeleteSpecificObjectVersionCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_delete_current_object_command(
        &self,
        request: BuildDeleteCurrentObjectCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_insert_delete_marker_command(
        &self,
        request: BuildInsertDeleteMarkerCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

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

    fn get_object_tags_for_subject(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<s3_types::VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: s3_types::VersionId,
    ) -> Result<Option<String>, ObjectPgActionError>;

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

    fn load_abort_multipart_upload_cleanup(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError>;

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

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

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError>;

    fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError>;

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_upload_segments(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError>;

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_all_stream_uploads_page(
        &self,
        pg_id: PgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn prepare_stream_segment_append(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn load_direct_put_commit_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError>;

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_put_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError>;

    fn build_stream_put_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn load_stream_part_finalize_snapshot(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError>;

    fn build_stream_part_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;

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

    fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, BucketSnapshotLoadError>;

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
}
