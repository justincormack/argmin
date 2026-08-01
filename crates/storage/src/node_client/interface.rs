use super::*;
use crate::BucketAclSummary;
use crate::SerializedBucketTagSet;

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
    fn head_bucket_replica_for_delete(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_raw(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn head_bucket_info(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError>;

    fn load_bucket_snapshot(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError>;

    fn load_bucket_snapshot_pair(
        &self,
        source_pg_id: BucketPgId,
        source: (&BucketName, BucketSnapshotRequest),
        destination_pg_id: BucketPgId,
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError>;

    fn build_create_bucket_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        config: &CreateBucketConfig<'_>,
    ) -> Result<CreateBucketCommandBuild, BucketSnapshotLoadError>;

    fn build_advance_multipart_completion_barrier_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<(u64, MetadataCommandEnvelope), BucketSnapshotLoadError>;

    fn pending_mark_bucket_deleting_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &MarkBucketDeletingCommand,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_mark_bucket_deleting_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
    ) -> Result<MarkBucketDeletingCommandBuild, BucketSnapshotLoadError>;

    fn pending_put_bucket_versioning_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketVersioningCommand,
        state: BucketVersioningState,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_versioning_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        state: BucketVersioningState,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_acl_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketAclCommand,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_acl_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn pending_put_bucket_property_command_matches_current(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command: &PutBucketPropertyCommand,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn build_put_bucket_property_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketPropertyMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn build_put_bucket_subresource_command(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        command_id: MetadataCommandId,
        mutation: &BucketSubresourceMutation,
    ) -> Result<MetadataCommandEnvelope, BucketSnapshotLoadError>;

    fn get_bucket_subresource(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError>;

    fn get_bucket_tags(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<SerializedBucketTagSet>, BucketSnapshotLoadError> {
        self.get_bucket_subresource(pg_id, bucket, BucketSubresourceKind::Tagging)?
            .map(SerializedBucketTagSet::from_current_xml)
            .transpose()
            .map_err(|error| {
                MetadataError::InvariantViolation {
                    context: "get bucket tags",
                    reason: format!("stored bucket tags are invalid: {error}"),
                }
                .into()
            })
    }

    fn list_buckets(
        &self,
        pg_id: BucketPgId,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, BucketSnapshotLoadError>;

    fn load_bucket_execution_generations(
        &self,
        pg_id: BucketPgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError>;

    fn load_bucket_fast_path_identities(
        &self,
        pg_id: BucketPgId,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError>;
}

pub(crate) trait BucketWriteReservationNodeClient: Send + Sync {
    fn durable_bucket_write_drain_exists(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError>;

    fn durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError>;

    fn record_bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn bucket_delete_attempt_outcome(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketSnapshotLoadError>;

    fn acquire_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    #[cfg(test)]
    fn acquire_completion_durable_bucket_write_reservation(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.acquire_durable_bucket_write_reservation(pg_id, acquire)
    }

    fn acquire_completion_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        acquire: DurableBucketWriteReservationAcquire<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError> {
        self.acquire_durable_bucket_write_reservation_with_effect_fence(
            pg_id,
            acquire,
            effect_fence,
        )
    }

    fn validate_bucket_write_reservation_proof(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
        self.begin_durable_bucket_write_drain_with_effect_fence(
            pg_id,
            bucket,
            drain_id,
            owner_token,
            cluster_epoch,
            created_at,
            lease_deadline,
            AdmittedRouteEffectFence::unbounded(cluster_epoch),
        )
    }

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn clear_expired_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_drain(
        &self,
        pg_id: BucketPgId,
        record: &BucketWriteDrainRecord,
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, BucketSnapshotLoadError>;

    fn durable_bucket_write_reservations(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, BucketSnapshotLoadError>;

    fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        pg_id: BucketPgId,
        proof: &BucketWriteReservationProof,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, BucketSnapshotLoadError>;

    fn get_bucket_delete_finalize_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, BucketSnapshotLoadError>;

    fn get_bucket_delete_begin_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<BucketDeleteBeginRoot>, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError>;

    fn bucket_delete_finalize_claim(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, BucketSnapshotLoadError>;

    fn get_lifecycle_sweep_roots(
        &self,
        pg_id: BucketPgId,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, BucketSnapshotLoadError>;

    fn list_lifecycle_sweep_buckets(
        &self,
        pg_id: BucketPgId,
    ) -> Result<LifecycleSweepBuckets, BucketSnapshotLoadError>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Option<LifecycleSweepClaimRecord>, BucketSnapshotLoadError>;

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        pg_id: BucketPgId,
        claim: &LifecycleSweepClaimRecord,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;

    fn record_lifecycle_sweep_claim_error(
        &self,
        pg_id: BucketPgId,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, BucketSnapshotLoadError>;
}

/// Cleanup authority for exact durable bucket-write subjects which may need
/// to outlive the active route that created them.
///
/// Keeping these operations separate prevents an active acquisition client
/// from being used implicitly for retained proof, drain, or worker-claim
/// cleanup.
pub(crate) trait RetainedBucketWriteReservationNodeClient: Send + Sync {
    /// Bind retained bucket-write cleanup to one bucket metadata PG and exact
    /// bucket subject selected by the retained cluster route.
    fn open_retained_bucket_write_reservation_route(
        &self,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn RetainedBucketWriteReservationRoute + '_>, BucketSnapshotLoadError>;
}

pub(crate) trait RetainedBucketWriteReservationRoute: Send {
    fn release_durable_bucket_write_reservation(
        &self,
        record: &BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_metadata_command_bucket_write_reservation(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn clear_durable_bucket_write_drain(
        &self,
        record: &BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_bucket_delete_finalize_claim(
        &self,
        claim: &BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;

    fn release_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;
}

pub(crate) trait ObjectGenerationMetadataNodeClient: Send + Sync {
    fn open_object_generation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectGenerationMetadataRoute + '_>, ObjectPgActionError>;
}

pub(crate) trait ObjectGenerationMetadataRoute: Send {
    fn object_generation_reservation(
        &self,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError>;

    fn next_object_generation_id(&self) -> Result<GenerationId, ObjectPgActionError>;
}

pub(crate) trait ObjectVersionMetadataNodeClient: Send + Sync {
    fn open_object_version_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectVersionMetadataRoute + '_>, ObjectPgActionError>;
}

pub(crate) trait ObjectVersionMetadataRoute: Send {
    fn next_object_version_id(&self) -> Result<VersionId, ObjectPgActionError>;

    fn next_completion_object_version_id(&self) -> Result<VersionId, ObjectPgActionError> {
        self.next_object_version_id()
    }
}

pub(crate) trait DirectPutMetadataNodeClient: Send + Sync {
    fn open_direct_put_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn DirectPutMetadataRoute + '_>, ObjectPgActionError>;
}

pub(crate) trait DirectPutMetadataRoute: Send {
    fn load_direct_put_commit_snapshot(
        &self,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> Result<DirectPutCommitStorageSnapshot, ObjectPgActionError>;

    fn build_direct_put_commit_command(
        &self,
        request: BuildDirectPutCommitCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait ObjectListingMetadataNodeClient: Send + Sync {
    fn open_object_listing_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ObjectListingMetadataRoute + '_>, BucketSnapshotLoadError>;
}

pub(crate) trait ObjectListingMetadataRoute: Send {
    fn list_objects_page(
        &self,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, BucketSnapshotLoadError>;

    fn list_object_versions_page(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, BucketSnapshotLoadError>;

    fn list_multipart_uploads_page(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, BucketSnapshotLoadError>;
}

pub(crate) trait ObjectMutationMetadataNodeClient: Send + Sync {
    fn open_put_object_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn PutObjectMetadataRoute + '_>, ObjectPgActionError>;

    fn open_object_delete_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectDeleteMetadataRoute + '_>, ObjectPgActionError>;

    fn open_multipart_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadCreationMetadataRoute + '_>, ObjectPgActionError>;

    fn open_multipart_upload_lookup_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartUploadLookupMetadataRoute + '_>, ObjectPgActionError>;

    fn open_authorized_multipart_upload_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Box<dyn AuthorizedMultipartUploadMetadataRoute + '_>, ObjectPgActionError>;

    fn open_multipart_completion_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn MultipartCompletionMutationMetadataRoute + '_>, ObjectPgActionError>;

    fn open_multipart_abort_mutation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Box<dyn MultipartAbortMutationMetadataRoute + '_>, ObjectPgActionError>;

    fn open_object_payload_reclaim_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadReclaimMetadataRoute + '_>, ObjectPgActionError>;

    fn open_stream_upload_creation_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn StreamUploadCreationMetadataRoute + '_>, ObjectPgActionError>;

    fn open_stream_upload_session_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamUploadSessionMetadataRoute + '_>, ObjectPgActionError>;

    fn open_stream_put_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Box<dyn StreamPutFinalizationMetadataRoute + '_>, ObjectPgActionError>;

    #[allow(clippy::too_many_arguments)]
    fn open_stream_part_finalization_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> Result<Box<dyn StreamPartFinalizationMetadataRoute + '_>, ObjectPgActionError>;

    fn list_stream_uploads_for_bucket_page(
        &self,
        pg_id: ObjectMetadataScanPgId,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn list_all_stream_uploads_page(
        &self,
        pg_id: ObjectMetadataScanPgId,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, ObjectPgActionError>;

    fn payload_reclaim_exists(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError>;

    fn get_bucket_payload_reclaim_root(
        &self,
        pg_id: ObjectMetadataScanPgId,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn get_payload_reclaim_root(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketSnapshotLoadError>;

    fn object_payload_reclaim_claim(
        &self,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError>;
}

pub(crate) trait PutObjectMetadataRoute: Send {
    fn load_put_object_metadata_snapshot(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError>;

    fn build_put_object_metadata_command(
        &self,
        request: BuildPutObjectMetadataCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait MultipartCompletionMutationMetadataRoute: Send {
    fn load_stale_payload_source(&self) -> Result<Option<StoredObject>, ObjectPgActionError>;

    fn build_complete_multipart_object_command(
        &self,
        request: BuildCompleteMultipartObjectCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait MultipartAbortMutationMetadataRoute: Send {
    fn load_cleanup(&self) -> Result<Option<AbortMultipartUploadCleanup>, ObjectPgActionError>;

    fn build_abort_multipart_upload_command(
        &self,
        request: BuildAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;

    fn build_authorized_abort_multipart_upload_command(
        &self,
        request: BuildAuthorizedAbortMultipartUploadCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError>;
}

pub(crate) trait ObjectPayloadReclaimMetadataRoute: Send {
    fn load_payload(&self) -> Result<Option<ObjectPayloadReclaimCommand>, BucketSnapshotLoadError>;

    fn acquire_claim(
        &self,
        request: AcquireObjectPayloadReclaimClaimReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, BucketSnapshotLoadError>;

    fn build_delete_object_payload_reclaim_command(
        &self,
        request: BuildDeleteObjectPayloadReclaimCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait StreamUploadCreationMetadataRoute: Send {
    fn matching_stream_upload_exists(
        &self,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError>;

    fn build_create_stream_upload_command(
        &self,
        request: BuildCreateStreamUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait StreamUploadSessionMetadataRoute: Send {
    fn load_session(&self) -> Result<StreamUploadRecord, ObjectPgActionError>;

    fn load_segments(&self) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError>;

    fn prepare_segment_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError>;

    fn update_put_bucket_write_reservation(
        &self,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), ObjectPgActionError>;
}

pub(crate) trait StreamPutFinalizationMetadataRoute: Send {
    fn load_snapshot(&self) -> Result<StreamPutFinalizeStorageSnapshot, ObjectPgActionError>;

    fn build_commit_command(
        &self,
        request: BuildStreamPutCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait StreamPartFinalizationMetadataRoute: Send {
    fn load_snapshot(&self) -> Result<StreamUploadPartStorageSnapshot, ObjectPgActionError>;

    fn build_commit_command(
        &self,
        request: BuildStreamPartCommitCommandReq<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait ObjectDeleteMetadataRoute: Send {
    fn load_current_object_delete_snapshot(
        &self,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn load_specific_object_delete_snapshot(
        &self,
        version_id: VersionId,
    ) -> Result<ObjectDeleteStorageSnapshot, ObjectPgActionError>;

    fn list_object_versions_for_lifecycle(&self) -> Result<Vec<StoredObject>, ObjectPgActionError>;

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
}

pub(crate) trait MultipartUploadCreationMetadataRoute: Send {
    fn matching_multipart_upload_initiated_at(
        &self,
        create: &CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError>;

    fn build_create_multipart_upload_command(
        &self,
        request: BuildCreateMultipartUploadCommandReq<'_>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError>;
}

pub(crate) trait MultipartUploadLookupMetadataRoute: Send {
    fn load_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError>;

    fn load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn load_in_progress_multipart_upload_for_listing(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError>;

    fn lookup_multipart_upload_management(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError>;
}

pub(crate) trait AuthorizedMultipartUploadMetadataRoute: Send {
    fn load_multipart_completion_snapshot(
        &self,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError>;

    fn load_multipart_completion_preflight(
        &self,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError>;

    fn list_multipart_parts(
        &self,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError>;
}

/// Cleanup authority for exact object-metadata subjects which may need to
/// outlive the active route that created them.
pub(crate) trait RetainedObjectMutationMetadataNodeClient: Send + Sync {
    /// Bind retained object cleanup to one object metadata PG and exact
    /// bucket/key subject selected by the retained cluster route.
    fn open_retained_object_mutation_route(
        &self,
        pg_id: ObjectMetadataPgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn RetainedObjectMutationMetadataRoute + '_>, BucketSnapshotLoadError>;
}

pub(crate) trait RetainedObjectMutationMetadataRoute: Send {
    /// Prepare the exact state-reducing abort command used by a retained
    /// stream-cleanup capability. Implementations must serialize session and
    /// segment loading with pending-slot allocation.
    fn prepare_retained_stream_upload_abort(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<PreparedRetainedStreamUploadAbort>, ObjectPgActionError>;

    fn release_object_payload_reclaim_claim(
        &self,
        claim: &ObjectPayloadReclaimClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError>;
}

pub(crate) trait ObjectReadMetadataNodeClient: Send + Sync {
    fn open_object_read_metadata_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectReadMetadataRoute + '_>, ObjectPgActionError>;
}

pub(crate) trait ObjectReadMetadataRoute: Send {
    fn load_object_read_auth_subject(
        &self,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError>;

    fn load_object_read_snapshot_for_subject(
        &self,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError>;

    fn get_object_tags_for_subject(
        &self,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        authorized_version_id: VersionId,
    ) -> Result<Option<crate::SerializedTagSet>, ObjectPgActionError>;
}

pub(crate) struct BuildStreamPutCommitCommandReq<'a> {
    pub(crate) total_size: u64,
    pub(crate) expected_snapshot: &'a StreamPutFinalizeStorageSnapshot,
    pub(crate) commit: &'a StreamPutCommitInput,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDirectPutCommitCommandReq<'a> {
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
    pub(crate) request: &'a CreateStreamUploadReq,
    pub(crate) cleanup_after: Option<u64>,
    pub(crate) precondition: CreateStreamUploadPrecondition<'a>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCreateMultipartUploadCommandReq<'a> {
    pub(crate) request: &'a CreateMultipartUploadReq,
    pub(crate) expected_current: Option<&'a StoredObject>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildStreamPartCommitCommandReq<'a> {
    pub(crate) expected_snapshot: &'a StreamUploadPartStorageSnapshot,
    pub(crate) part: &'a MultipartPartRecord,
    pub(crate) segments: &'a [MultipartPartSegmentRecord],
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildCompleteMultipartObjectCommandReq<'a> {
    pub(crate) request: &'a CompleteMultipartCommitRequest,
    pub(crate) version_id: VersionId,
    pub(crate) expected_object_parts: &'a [ObjectPartRecord],
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) fn require_multipart_completion_mutation_subject(
    route_cluster_epoch: ClusterEpoch,
    bucket: &BucketName,
    key: &ObjectKey,
    build: &BuildCompleteMultipartObjectCommandReq<'_>,
) -> Result<(), ObjectPgActionError> {
    let request = build.request;
    let cleanup = &request.expected_cleanup;
    let invalid = request.bucket != *bucket
        || request.key != *key
        || !build
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                route_cluster_epoch,
                bucket,
                crate::metadata_command::COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            )
        || request
            .part_records
            .iter()
            .chain(cleanup.omitted_parts.iter())
            .any(|part| part.upload_id != request.upload_id)
        || request
            .selected_streaming_segments
            .iter()
            .chain(cleanup.omitted_streaming_segments.iter())
            .any(|segment| {
                segment.bucket != *bucket
                    || segment.key != *key
                    || segment.upload_id != request.upload_id
            })
        || request
            .expected_stale_payload_source
            .as_ref()
            .is_some_and(|stored| {
                !stored.version_id().is_null()
                    || stored.as_live().is_none()
                    || stored.bucket() != bucket
                    || stored.key() != key
            })
        || cleanup.stream_uploads.iter().any(|stream| {
            stream.bucket != *bucket
                || stream.key != *key
                || !matches!(
                    &stream.target,
                    StreamUploadTarget::UploadPart { upload_id, .. }
                        if upload_id == &request.upload_id
                )
        })
        || cleanup.stream_upload_segments.iter().any(|segment| {
            !cleanup
                .stream_uploads
                .iter()
                .any(|stream| stream.session_id == segment.session_id)
        });
    if invalid {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "build complete multipart object command",
        }
        .into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct MultipartAbortMutationSubject<'a> {
    pub(crate) route_cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
}

pub(crate) fn require_multipart_abort_mutation_subject(
    subject: MultipartAbortMutationSubject<'_>,
    authorized_record: Option<&crate::MultipartUploadRecord>,
    expected_cleanup: Option<&AbortMultipartUploadCleanup>,
    bucket_write_reservation: &BucketWriteReservationProof,
    operation: &'static str,
) -> Result<(), ObjectPgActionError> {
    let MultipartAbortMutationSubject {
        route_cluster_epoch,
        bucket,
        key,
        upload_id,
    } = subject;
    let invalid_authorized_upload = authorized_record.is_some_and(|upload| {
        upload.bucket != *bucket || upload.key != *key || upload.upload_id != *upload_id
    });
    let invalid_cleanup = expected_cleanup.is_some_and(|cleanup| {
        !cleanup.matches_upload_subject(bucket, key, upload_id)
            || authorized_record.is_some_and(|upload| cleanup.upload != *upload)
    });
    if invalid_authorized_upload
        || invalid_cleanup
        || !bucket_write_reservation.matches_exact_mutation_subject(
            route_cluster_epoch,
            bucket,
            crate::metadata_command::ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
    {
        return Err(StoreError::RouteCapabilitySubjectMismatch { operation }.into());
    }
    Ok(())
}

pub(crate) struct AbortMultipartCommandValidation<'a> {
    pub(crate) pg_id: ObjectMetadataPgId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket: &'a BucketName,
    pub(crate) key: &'a ObjectKey,
    pub(crate) upload_id: &'a UploadId,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildAbortMultipartUploadCommandReq<'a> {
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildAuthorizedAbortMultipartUploadCommandReq<'a> {
    pub(crate) authorized_upload: &'a crate::AuthorizedMultipartUploadAbort,
    pub(crate) expected_cleanup: Option<&'a AbortMultipartUploadCleanup>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteObjectPayloadReclaimCommandReq<'a> {
    pub(crate) payload: &'a ObjectPayloadReclaimCommand,
    pub(crate) claim: &'a ObjectPayloadReclaimClaimRecord,
}

pub(crate) struct AcquireObjectPayloadReclaimClaimReq<'a> {
    pub(crate) reclaim_kind: ObjectPayloadReclaimKind,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) claim_id: &'a str,
    pub(crate) owner_token: &'a str,
    pub(crate) claimed_at: u64,
    pub(crate) lease_deadline: Option<u64>,
    pub(crate) now: u64,
}

pub(crate) fn require_object_payload_reclaim_subject(
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
    payload: &ObjectPayloadReclaimCommand,
    operation: &'static str,
) -> Result<(), StoreError> {
    let matches = match payload {
        ObjectPayloadReclaimCommand::Segments(record) => {
            record.bucket == *bucket && record.key == *key && record.generation_id == generation_id
        }
        ObjectPayloadReclaimCommand::Multipart(record) => {
            record.bucket == *bucket && record.key == *key && record.generation_id == generation_id
        }
    };
    if !matches {
        return Err(StoreError::RouteCapabilitySubjectMismatch { operation });
    }
    Ok(())
}

pub(crate) fn require_object_payload_reclaim_command_subject(
    route_cluster_epoch: ClusterEpoch,
    pg_id: ObjectMetadataPgId,
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
    request: &BuildDeleteObjectPayloadReclaimCommandReq<'_>,
) -> Result<(), ObjectPgActionError> {
    let payload_matches = require_object_payload_reclaim_subject(
        bucket,
        key,
        generation_id,
        request.payload,
        "build delete object payload reclaim command",
    )
    .is_ok();
    let claim = request.claim;
    if !payload_matches
        || claim.pg_id != pg_id.get()
        || claim.cluster_epoch != route_cluster_epoch
        || claim.bucket != *bucket
        || claim.key != *key
        || claim.generation_id != generation_id
        || claim.reclaim_kind != request.payload.kind()
    {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "build delete object payload reclaim command",
        }
        .into());
    }
    Ok(())
}

pub(crate) struct BuildPutObjectMetadataCommandReq<'a> {
    pub(crate) requested_version_id: Option<VersionId>,
    pub(crate) expected_stored: &'a StoredObject,
    pub(crate) version_id: VersionId,
    pub(crate) mutation: PutObjectMetadataMutation,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteSpecificObjectVersionCommandReq<'a> {
    pub(crate) version_id: VersionId,
    pub(crate) expected_stored: Option<&'a StoredObject>,
    pub(crate) expected_target: Option<&'a DeleteObjectVersionTarget>,
    pub(crate) expected_version_list: Option<&'a [StoredObject]>,
    pub(crate) bucket_write_reservation: &'a BucketWriteReservationProof,
}

pub(crate) struct BuildDeleteCurrentObjectCommandReq<'a> {
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

    fn open_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn PlacedShardRoute + '_>, StoreError>;
}

pub(crate) trait PlacedShardRoute: Send {
    fn write_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError>;

    fn write_placed_shard_with_effect_fence(
        &self,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, StoreError>;

    fn repair_placed_shard(&self, data: &[u8]) -> Result<WriteAck, StoreError>;

    fn read_placed_shard(&self, expected_ack: WriteAck) -> Result<Vec<u8>, StoreError>;

    fn read_placed_shard_into(
        &self,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError>;

    fn delete_placed_shard(&self) -> Result<(), StoreError>;
}

/// Retained access to exact shard placements which may outlive the active
/// data route that originally wrote them.
pub(crate) trait RetainedPlacedShardNodeClient: Send + Sync {
    fn open_retained_placed_shard_route(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedPlacedShardRoute + '_>, StoreError>;
}

pub(crate) trait RetainedPlacedShardRoute: Send {
    fn read_placed_shard_for_historical_inspection(
        &self,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError>;

    fn delete_placed_shard_for_historical_cleanup(&self) -> Result<(), StoreError>;
}

pub(crate) trait ShardReadHandleLease: Send {
    fn release(&mut self) -> Result<(), StoreError>;
}

pub(crate) trait ShardReadHandleNodeClient: Send + Sync {
    fn open_shard_read_handle_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleRoute + '_>, StoreError>;
}

pub(crate) trait ShardReadHandleRoute: Send {
    fn acquire(self: Box<Self>) -> Result<Box<dyn ShardReadHandleLease>, StoreError>;
}

pub(crate) trait ObjectPayloadLeaseNodeLease: Send {
    fn release(&mut self) -> Result<usize, StoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectPayloadLeaseKind {
    BroadSnapshot,
    ShardLocations,
}

pub(crate) trait ObjectPayloadLeaseNodeClient: Send + Sync {
    fn open_object_payload_lease_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadLeaseRoute + '_>, StoreError>;
}

pub(crate) trait ObjectPayloadLeaseRoute: Send {
    fn acquire_object_payload_lease(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError>;

    fn try_begin_object_payload_reclaim(
        &self,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError>;

    fn object_payload_lease_count(&self) -> Result<usize, StoreError>;
}

/// Retained cleanup authority for an exact object-payload reclaim subject.
/// Reclaim completion and fence release can outlive the active route which
/// admitted the reclaim attempt.
pub(crate) trait RetainedObjectPayloadReclaimNodeClient: Send + Sync {
    fn open_retained_object_payload_reclaim_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<Box<dyn RetainedObjectPayloadReclaimRoute + '_>, StoreError>;
}

pub(crate) trait RetainedObjectPayloadReclaimRoute: Send {
    fn finish_object_payload_reclaim(&self, keep_fence: bool) -> Result<(), StoreError>;

    fn clear_object_payload_reclaim_fence(&self) -> Result<(), StoreError>;
}

pub(crate) trait ShardAckNodeClient: Send + Sync {
    fn open_shard_ack_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardAckRoute + '_>, StoreError>;
}

pub(crate) trait ShardAckRoute: Send {
    fn register_shard_acks(&self, shard_batch: &[(&ShardKey, WriteAck)]) -> Result<(), StoreError>;

    fn validate_shard_ack(&self, key: &ShardKey, ack: WriteAck) -> Result<(), StoreError>;

    fn load_shard_ack(&self, key: &ShardKey) -> Result<WriteAck, StoreError>;

    fn delete_shard_ack(&self, key: &ShardKey) -> Result<(), StoreError>;

    fn record_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError>;

    fn list_placed_segment_shard_repairs(
        &self,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError>;

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError>;

    fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError>;

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError>;

    fn resolve_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError>;

    fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError>;

    fn list_placed_segment_shard_backfills(
        &self,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError>;

    fn count_placed_segment_shard_backfills(&self) -> Result<usize, StoreError>;

    fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError>;

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError>;

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError>;

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError>;

    fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError>;
}

/// Retained access to acknowledgement rows for exact historical placements.
pub(crate) trait RetainedShardAckNodeClient: Send + Sync {
    fn open_retained_shard_ack_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedShardAckRoute + '_>, StoreError>;
}

pub(crate) trait RetainedShardAckRoute: Send {
    fn load_written_shard_ack_for_historical_inspection(&self) -> Result<WriteAck, StoreError>;

    fn delete_retained_shard_ack(&self) -> Result<(), StoreError>;
}

pub(crate) trait ShardScavengerNodeClient: Send + Sync {
    fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError>;

    fn open_shard_scavenger_data_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerDataRoute + '_>, StoreError>;

    fn open_shard_scavenger_object_scan_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        pg_id: ObjectMetadataScanPgId,
    ) -> Result<Box<dyn ShardScavengerObjectScanRoute + '_>, StoreError>;
}

pub(crate) trait ShardScavengerDataRoute: Send {
    fn list_scavenger_shard_files(&self) -> Result<ScavengerShardFileScan, StoreError>;

    fn list_scavenger_shard_rows(&self) -> Result<Vec<ScavengerShardRow>, StoreError>;
}

pub(crate) trait ShardScavengerObjectScanRoute: Send {
    fn list_placed_segment_backfill_reference_page(
        &self,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<PlacedSegmentBackfillReferencePage, StoreError>;

    fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError>;
}

/// Primary-only durable state for shard-scavenger findings. Keeping this
/// separate from scan access prevents an inventory client from publishing or
/// resolving observations.
pub(crate) trait ShardScavengerObservationNodeClient: Send + Sync {
    /// Bind primary-only scavenger observation state to one routed data PG.
    ///
    /// The returned interface omits a PG argument so record/list/resolve calls
    /// cannot redirect authority selected for another observation partition.
    fn open_shard_scavenger_observation_route(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardScavengerObservationRoute + '_>, StoreError>;
}

pub(crate) trait ShardScavengerObservationRoute: Send {
    fn record_shard_scavenger_observation(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError>;

    fn list_shard_scavenger_observations(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError>;

    fn resolve_shard_scavenger_observation(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError>;
}

/// Exact retained-route authority for applying and finishing a previously
/// prepared stream-upload abort. Keeping this separate prevents ordinary
/// metadata-command publishers from invoking retained cleanup operations.
pub(crate) trait RetainedMetadataCommandNodeClient: Send + Sync {
    fn apply_retained_stream_upload_abort(
        &self,
        prepared: &PreparedRetainedStreamUploadAbort,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;

    fn finish_retained_stream_upload_abort(
        &self,
        prepared: &PreparedRetainedStreamUploadAbort,
    ) -> Result<bool, StoreError>;
}

/// Read-only metadata-command state shared by active publication and peering.
/// Implementations may validate stored invariants but must not publish,
/// replay, compact, or otherwise mutate command state.
pub(crate) trait MetadataCommandInspectionNodeClient: Send + Sync {
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

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError>;

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError>;

    fn metadata_command_replica_state_can_initialize(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<bool, StoreError>;

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

    fn metadata_command_abandoned(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError>;
}

/// Quiesced-PG replay and metadata-transfer mutations. This interface cannot
/// allocate or publish ordinary request-path metadata commands.
pub(crate) trait MetadataCommandPeeringNodeClient:
    MetadataCommandInspectionNodeClient + Send + Sync
{
    /// Bind peering mutation to one metadata PG and epoch selected by an
    /// already-quiesced cluster workflow.
    ///
    /// The returned interface deliberately omits PG and epoch parameters so a
    /// caller cannot redirect authority selected for one peering route to a
    /// different metadata command subject.
    fn open_metadata_command_peering_route(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandPeeringRoute + '_>, StoreError>;
}

pub(crate) trait MetadataCommandPeeringRoute: Send {
    fn validate_metadata_command_replay_state(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn initialize_metadata_transfer_empty_state(
        &self,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn initialize_metadata_transfer_matching_state(
        &self,
        applied_log_index: u64,
        applied_log_hash: u64,
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        commands: &[MetadataTransferCommand],
        expected_state_digest: u64,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    #[allow(dead_code)]
    fn install_metadata_transfer_checkpoint_base(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError>;

    fn replay_metadata_command_for_peering(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;
}

/// Opens a recovery critical section bound to one metadata PG and epoch.
///
/// The returned interface deliberately omits PG and epoch parameters. This
/// prevents recovery code from acquiring serialization for one subject and
/// then applying that authority to another.
pub(crate) trait MetadataCommandRecoveryNodeClient: Send + Sync {
    fn open_metadata_command_recovery_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandRecoveryCriticalSection>, StoreError>;

    /// Apply one authorized recovery command on a historical replica.
    ///
    /// The server serializes this single mutation internally. Unlike the
    /// primary critical section, this capability does not authorize pending
    /// slot inspection or replacement on the replica.
    fn apply_metadata_command_and_record_on_recovery_replica(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;

    /// Record one certificate-bound recovery tombstone on a historical replica.
    ///
    /// The server serializes only this mutation internally, without granting
    /// the reporting-primary inspection and replacement capability.
    fn record_metadata_command_abandoned_on_recovery_replica(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError>;
}

/// Pending-command convergence and explicitly authorized recovery mutation
/// while holding the critical section for one metadata PG and epoch.
/// Ordinary publishers cannot replace an existing pending slot, apply a
/// recovery command, or record abandonment through their active interface.
pub(crate) trait MetadataCommandRecoveryCriticalSection: Send {
    fn max_metadata_command_log_index(&self) -> Result<u64, StoreError>;

    fn pending_metadata_command_envelope(
        &self,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError>;

    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn metadata_command_abandon_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError>;

    fn replace_pending_metadata_command_slot_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, StoreError>;

    fn apply_metadata_command_and_record_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;

    fn record_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError>;
}

pub(crate) trait MetadataCommandNodeClient: Send + Sync {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandCriticalSection>, StoreError>;

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

    fn try_insert_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), StoreError>;

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError>;

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<bool, StoreError>;

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
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

    /// Record one tombstone on a current-route replica without granting the
    /// primary-only metadata-command critical-section capability.
    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError>;
}

/// Active publisher authority serialized for one metadata PG and epoch.
///
/// PG and epoch are selected when the section is opened and cannot be
/// replaced on individual operations. The interface exposes only operations
/// which currently require the cross-process critical section.
pub(crate) trait MetadataCommandCriticalSection: Send {
    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError>;

    fn apply_metadata_command_and_record(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError>;
}
