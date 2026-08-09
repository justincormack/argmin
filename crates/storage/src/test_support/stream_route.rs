// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::TestStorageFailure;
use crate::{
    ActiveMultipartObjectRoute, ActivePutObjectRoute, BucketName, ObjectEncryption, ObjectKey,
    SessionId, StorageCluster, StreamSegmentAppendInput, StreamSegmentAppendOutcome,
    StreamUploadFailure,
};

/// Logical stream-session mutation support for cross-crate tests.
///
/// The raw abort operation and its storage implementation error remain
/// storage-private. This capability exists only so deterministic higher-layer
/// race tests can end a known session and observe the same opaque failure
/// boundary used by production admitted routes.
pub trait StorageClusterStreamSessionTestSupport {
    fn test_create_put_object_stream_session_with_cleanup_deadline(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        cleanup_after: Option<u64>,
    ) -> Result<(), TestStorageFailure>;

    fn test_abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), StreamUploadFailure>;
}

impl StorageClusterStreamSessionTestSupport for StorageCluster {
    fn test_create_put_object_stream_session_with_cleanup_deadline(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        cleanup_after: Option<u64>,
    ) -> Result<(), TestStorageFailure> {
        StorageCluster::create_put_object_stream_session_record_with_cleanup_deadline(
            self,
            bucket,
            key,
            session_id,
            encryption,
            cleanup_after,
        )
        .map_err(TestStorageFailure::from_object_pg_action)
    }

    fn test_abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), StreamUploadFailure> {
        StorageCluster::abort_stream_upload_session(self, bucket, key, session_id)
            .map_err(StreamUploadFailure::from_object_pg_action)
    }
}

/// Deterministic scheduling support for admitted stream-mutation routes.
///
/// The callback runs after storage has prepared the segment and before it
/// writes payload shards. The route retains the production admission,
/// deadline, placement, and mutation behavior; callers cannot construct an
/// unbounded storage effect through this interface.
pub trait ActiveStreamRouteTestSupport {
    fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, StreamUploadFailure>;
}

impl ActiveStreamRouteTestSupport for ActivePutObjectRoute<'_> {
    fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, StreamUploadFailure> {
        ActivePutObjectRoute::test_append_stream_segment_with_after_prepare(
            self,
            input,
            after_prepare,
        )
    }
}

impl ActiveStreamRouteTestSupport for ActiveMultipartObjectRoute<'_> {
    fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, StreamUploadFailure> {
        ActiveMultipartObjectRoute::test_append_stream_segment_with_after_prepare(
            self,
            input,
            after_prepare,
        )
    }
}

/// PUT-specific admitted-route scheduling support which preserves the
/// caller-owned reservation heartbeat during the injected pause.
pub trait ActivePutObjectRouteTestSupport {
    fn test_append_stream_segment_with_after_prepare_and_lease_maintenance(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
        maintain_lease: impl FnMut() -> Result<(), StreamUploadFailure>,
    ) -> Result<StreamSegmentAppendOutcome, StreamUploadFailure>;
}

impl ActivePutObjectRouteTestSupport for ActivePutObjectRoute<'_> {
    fn test_append_stream_segment_with_after_prepare_and_lease_maintenance(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
        maintain_lease: impl FnMut() -> Result<(), StreamUploadFailure>,
    ) -> Result<StreamSegmentAppendOutcome, StreamUploadFailure> {
        ActivePutObjectRoute::test_append_stream_segment_with_after_prepare_and_lease_maintenance(
            self,
            input,
            after_prepare,
            maintain_lease,
        )
    }
}
