use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io;
use std::num::{NonZeroU16, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ec::{EcConfig, ErasureCodec};
use placement::NodeId;

pub use crate::node_client::LocalUnixStorageNodeClientAdmissionSettings;
pub use local::{
    LocalClusterMap, LocalNodeStoreConfig, LocalPgRoute, LocalUnixMetadataCommandNodeClientConfig,
    LocalUnixShardNodeClientConfig, LocalUnixStorageNodeClientConfig,
};
use local::{
    LocalClusterRuntimeState, LocalRouteMapLeaseSnapshot, MetadataCommandRecoveryAdmission,
    MetadataCommandRecoveryGuard,
};
pub use request_ops::BucketIdentityGenerations;
pub(crate) use request_ops::{DurableReclaimScanBatch, DurableReclaimScanOutcome};

use crate::control_plane::{
    ClusterRuntimeMapSnapshot, ControlPlaneError, ControlPlaneRuntimeMapSource,
    ControlPlaneRuntimeMapStatus, PendingMetadataCommandObservation, PgMetadataProof,
    PgRouteSnapshot, RuntimeMapContentDigest, RuntimeMapFreshnessProof,
};
use crate::control_plane_lease::BoundRouteMapLease;
use crate::error::{
    ClusterBuildError, ObjectMetadataMutationFailure, ObjectReadFailure, PgMetadataTransferError,
    ShardIoError, StoreError,
};
use crate::metadata_command::{
    metadata_command_log_hash, AbortStreamUploadCommand, AppendStreamSegmentCommand,
    BucketPropertyMutation, BucketWriteReservationProof, CommitDirectPutObjectCommand,
    CreateMultipartUploadCommand, CreateStreamUploadCommand, DeleteObjectVersionMode,
    DeleteObjectVersionTarget, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
    MetadataCommandPayload, MetadataCommandReplicaState, MetadataTransferCommand,
    ObjectPayloadReclaimCommand, PutObjectMetadataMutation, ReleaseObjectGenerationCommand,
    ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::node::SharedStorageNode;
use crate::node_client::{
    BucketMetadataRoute, BuildCreateStreamUploadCommandReq, BuildDirectPutCommitCommandReq,
    CreateStreamUploadPrecondition, MetadataCommandInspectionNodeClient, MetadataCommandNodeClient,
    MetadataCommandPeeringNodeClient, MetadataReadAuthorization, ObjectListingMetadataRoute,
    ObjectPayloadLeaseNodeLease, RetainedShardAckNodeClient, ShardAckRoute,
};
pub(crate) use crate::peering::PgMetadataTransferArtifact;
use crate::peering::{
    build_pg_metadata_transfer_artifact_from_retained_log_entries,
    build_pg_peering_replay_plan_from_retained_log_entries,
    rebase_pg_metadata_transfer_artifact_commands,
    reconstruct_pg_peering_from_primary_retained_log, PgMetadataTransferBaseKind,
    PgPeeringReconstructionDecision, PgPeeringReconstructionError, PgPeeringReconstructionFailure,
    PgPeeringReplicaReconstructionInput,
};
#[cfg(test)]
use crate::pg_store::PgClusterMapHistoryReferenceSummary;
use crate::pg_store::{MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus};
use crate::storage_rpc::{
    StorageRpcErrorCode, STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES,
    STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES, STORAGE_RPC_MAX_PAYLOAD_LEN,
};
#[cfg(test)]
use crate::traits::PgMetadataStore;
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::MultipartUploadRecord;
#[cfg(test)]
use crate::types::PlacedSegmentShardBackfillRecord;
use crate::types::{
    AclGrants, AdmittedRouteEffectFence, AuthorizedMultipartUploadRecord, BucketAclSummary,
    BucketEncryptionConfig, BucketInfo, BucketName, BucketObjectLockConfig,
    BucketOwnershipControls, BucketSnapshot, BucketSnapshotRequest, BucketSubresourceKind,
    BucketVersioningState, BucketWriteDrainRecord, BucketWriteReservationRecord, CanonicalUserId,
    ClusterEpoch, CommitDirectPutObjectReq, CompleteMultipartCommitOutcome,
    CompleteMultipartCommitRequest, CreateStreamUploadReq, DeleteCurrentObjectOutcome,
    DeleteSpecificObjectVersionOutcome, DirectPutCommitSnapshot, DirectPutPayloadWrite,
    DirectPutWrittenSegment, EcShape, FinalizeDirectPutObjectOutcome, FinalizeStreamPartOutcome,
    FinalizeStreamPutOutcome, GenerationId, InsertCurrentDeleteMarkerOutcome,
    ListedBucketMultipartUploads, ListedBucketObjectVersions, ListedBucketObjects,
    ListedMultipartParts, ObjectEncryption, ObjectKey, ObjectLayout, ObjectPayloadSegment,
    ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectReadSnapshotOutcome, ObjectRetention,
    ObjectSegmentRecord, OwnerIdentity, PgId, PgState, PlacedSegmentBackfillReferenceCursor,
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimAcquireParams,
    PlacedSegmentShardBackfillClaimRecord, PlacedSegmentShardBackfillWorkItem,
    PlacedSegmentShardRepairClaimAcquire, PlacedSegmentShardRepairClaimAcquireParams,
    PlacedSegmentShardRepairClaimRecord, PlacedSegmentShardRepairRecord,
    PlacedSegmentShardRepairWorkItem, PrepareStreamUploadSegmentAppendReq,
    PreparedDirectPutObjectCommit, PreparedStreamPartCommit, PreparedStreamPutCommit,
    PublicAccessBlockConfig, RouteMapValidity, SegmentStoredBytesRequest, SerializedBucketTagSet,
    SerializedTagSet, SessionId, ShardIndex, ShardKey, ShardScavengerObservation,
    ShardScavengerObservationKey, ShardScavengerObservationReason, ShardScavengerObservationRecord,
    ShardScavengerPayloadReference, ShardScavengerPlacedShardSetReference, StoredLegalHoldStatus,
    StoredObject, StreamPartFinalizeInput, StreamPartFinalizeSnapshot, StreamPutFinalizeSnapshot,
    StreamSegmentAppendInput, StreamSegmentAppendOutcome, StreamUploadCommandRecord,
    StreamUploadRecord, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId,
    VersionId, WriteAck, WrittenShardAck, PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT,
};
#[cfg(test)]
use crate::types::{
    MultipartReclaimRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    PutLiveObjectReq,
};
use crate::DataPgId;
use crate::ObjectEtag;
use crate::PutBucketSubresource;
use crate::{BucketPgId, ObjectMetadataPgId, ObjectMetadataScanPgId};
use crate::{BucketSnapshotLoadError, MetadataError, ObjectPgActionError};

mod local;
mod request_ops;

include!("cluster/core.rs");

include!("cluster/route_admission.rs");

include!("cluster/operation_routes.rs");

include!("cluster/runtime_map.rs");

include!("cluster/runtime_map_tests.rs");

include!("cluster/metadata_support.rs");

include!("cluster/metadata_operations.rs");

include!("cluster/payload_operations.rs");

include!("cluster/shard_maintenance.rs");

include!("cluster/repair_helpers_tests.rs");
