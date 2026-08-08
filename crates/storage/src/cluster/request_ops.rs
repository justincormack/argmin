use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::MutexGuard;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Mutex, OnceLock};

use placement::NodeId;

use super::{
    LocalClusterRuntimeState, MetadataCommandCheckpointRecordSummary,
    MetadataCommandExecutionRoute, MetadataCommandRecoveryProof, MetadataCommandRouteMode,
};
#[cfg(any(test, feature = "test-hooks"))]
use super::{
    MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
    MetadataCommandApplyTestContext, MetadataCommandApplyTestKind, ShardLocation,
};
#[cfg(test)]
use super::{
    MetadataCommandDrainAuthority, MetadataCommandRecoveryAdmission,
    MetadataCommandRecoveryDrainAuthority, RequestWorkBudget, BUCKET_WRITE_DRAIN_RETRY_BUDGET,
};
use crate::metadata_command::{
    BucketPropertyMutation, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitMultipartObjectCommand, CommitStreamPartCommand, DeleteFinalizedBucketCommand,
    DeleteObjectVersionTarget, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandPayload, ObjectPayloadReclaimClaimProof,
    ObjectPayloadReclaimCommand, PutObjectMetadataCommand, PutObjectMetadataMutation,
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node::ReclaimQueueInsert;
use crate::node_client::{
    complete_multipart_expected_object_parts, AcquireObjectPayloadReclaimClaimReq,
    BucketWriteReservationNodeClient, BucketWriteReservationRoute, BucketWriteReservationScanRoute,
    BuildCompleteMultipartObjectCommandReq, BuildCreateMultipartUploadCommandReq,
    BuildCreateStreamUploadCommandReq, BuildDeleteCurrentObjectCommandReq,
    BuildDeleteSpecificObjectVersionCommandReq, BuildInsertDeleteMarkerCommandReq,
    BuildPutObjectMetadataCommandReq, BuildStreamPartCommitCommandReq,
    BuildStreamPutCommitCommandReq, CreateBucketCommandBuild, CreateStreamUploadPrecondition,
    InsertDeleteMarkerStalePayload, MarkBucketDeletingCommandBuild, MetadataReadAuthorization,
};
use crate::storage_rpc::StorageRpcErrorCode;
use crate::traits::DurableBucketWriteReservationAcquire;
#[cfg(any(test, feature = "test-hooks"))]
use crate::traits::PgMetadataStore;
use crate::types::{
    AdmittedRouteEffectFence, BucketDeleteDebugBucketRow, BucketDeleteDebugDrain,
    BucketDeleteDebugFinalizeClaim, BucketDeleteDebugObjectVersionKind,
    BucketDeleteDebugObjectVersionSample, BucketDeleteDebugObjectVersionSampleError,
    BucketDeleteDebugPayloadReclaimClaim, BucketDeleteDebugPayloadReclaimClaimError,
    BucketDeleteDebugPayloadReclaimRoot, BucketDeleteDebugPayloadReclaimRootError,
    BucketDeleteDebugPendingCommand, BucketDeleteDebugSnapshot,
    ObjectPayloadPlacementDiagnosticError,
};
#[cfg(test)]
use crate::types::{
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::{
    ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    PlacedSegmentShardRepairWorkItem, SegmentStoredBytesRequest,
};
use crate::*;

include!("request_ops/support.rs");
include!("request_ops/metadata_commands.rs");
include!("request_ops/bucket_delete.rs");
include!("request_ops/bucket_operations.rs");
include!("request_ops/object_operations.rs");
include!("request_ops/multipart_operations.rs");
include!("request_ops/diagnostics_test_support.rs");
include!("request_ops/tests.rs");
