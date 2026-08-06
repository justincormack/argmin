use crate::{
    ActiveMultipartObjectRoute, ActivePutObjectRoute, ObjectPgActionError,
    StreamSegmentAppendInput, StreamSegmentAppendOutcome,
};

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
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError>;
}

impl ActiveStreamRouteTestSupport for ActivePutObjectRoute<'_> {
    fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError> {
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
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError> {
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
        maintain_lease: impl FnMut() -> Result<(), ObjectPgActionError>,
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError>;
}

impl ActivePutObjectRouteTestSupport for ActivePutObjectRoute<'_> {
    fn test_append_stream_segment_with_after_prepare_and_lease_maintenance(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
        maintain_lease: impl FnMut() -> Result<(), ObjectPgActionError>,
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError> {
        ActivePutObjectRoute::test_append_stream_segment_with_after_prepare_and_lease_maintenance(
            self,
            input,
            after_prepare,
            maintain_lease,
        )
    }
}
