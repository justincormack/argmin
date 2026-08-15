// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

static NEXT_PROCESS_LOCAL_REGISTRY_KEY: AtomicU64 = AtomicU64::new(1);

/// Opaque identity used only to share process-local state for one storage node.
///
/// The key intentionally exposes neither its value nor `Display` or
/// serialization support; its `Debug` output is redacted. It must never cross
/// an RPC or durable boundary.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessLocalRegistryKey(NonZeroU64);

impl ProcessLocalRegistryKey {
    pub(crate) fn allocate() -> Option<Self> {
        NEXT_PROCESS_LOCAL_REGISTRY_KEY
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .ok()
            .and_then(NonZeroU64::new)
            .map(Self)
    }
}

impl fmt::Debug for ProcessLocalRegistryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessLocalRegistryKey([redacted])")
    }
}

const DIRECT_PUT_METADATA_RETRY_BUDGET: Duration = Duration::from_secs(10);
const STREAM_PUT_FINALIZE_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OBJECT_GENERATION_RESERVATION_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OBJECT_VERSION_RESERVATION_RETRY_BUDGET: Duration = Duration::from_secs(10);
pub(super) const BUCKET_WRITE_DRAIN_RETRY_BUDGET: Duration = Duration::from_secs(10);
const PUT_OBJECT_STREAM_CREATE_RETRY_BUDGET: Duration = Duration::from_secs(10);
const STREAM_SEGMENT_APPEND_RETRY_BUDGET: Duration = Duration::from_secs(10);
const STREAM_UPLOAD_ABORT_RETRY_BUDGET: Duration = Duration::from_secs(10);
#[cfg(test)]
const METADATA_COMMAND_REISSUE_BUDGET: Duration = Duration::from_secs(1);
const METADATA_CONTENTION_BACKOFF_INITIAL: Duration = Duration::from_millis(1);
const METADATA_CONTENTION_BACKOFF_MAX: Duration = Duration::from_millis(25);
const PLACED_SEGMENT_SHARD_BACKFILL_CANDIDATE_SCAN_LIMIT: usize = 256;
const PLACED_SEGMENT_SHARD_BACKFILL_REFERENCE_SCAN_LIMIT: usize = 256;
const PLACED_SEGMENT_SHARD_BACKFILL_PG_SCAN_LIMIT: usize = 8;
const PLACED_SEGMENT_SHARD_BACKFILL_SCAN_TIME_BUDGET: Duration = Duration::from_millis(50);
pub(crate) const METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT: usize = 4;
const METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE: u64 = 64;
const METADATA_COMMAND_CHECKPOINT_FRAME_RISK_BYTES: usize = STORAGE_RPC_MAX_PAYLOAD_LEN * 3 / 4;

#[derive(Debug)]
pub(super) struct RequestWorkBudget {
    started: Instant,
    budget: Duration,
    attempts: usize,
    contention_retries: usize,
    max_attempts: Option<usize>,
    operation: &'static str,
    pg_id: Option<PgId>,
}

mod metadata_command_drain_authority {
    use super::{
        MetadataCommandEnvelope, MetadataCommandRecoveryGuard, PgId, RequestWorkBudget, StoreError,
    };

    #[derive(Debug)]
    struct LeaderSeal;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct RecoveryCommandSubject {
        pg_id: PgId,
        log_index: u64,
        checksum_crc64: u64,
    }

    impl RecoveryCommandSubject {
        fn new(pg_id: PgId, command: &MetadataCommandEnvelope) -> Self {
            Self {
                pg_id,
                log_index: command.id().log_index().get(),
                checksum_crc64: command.checksum_crc64(),
            }
        }

        fn matches(self, pg_id: PgId, command: &MetadataCommandEnvelope) -> bool {
            self == Self::new(pg_id, command)
        }
    }

    /// Bounded authority for storage-owned pending-command recovery.
    ///
    /// Publisher paths use their registry token instead. Recovery paths must
    /// keep one of these alive across repeated drains so every contender
    /// consumes the same finite work budget rather than silently starting a
    /// fresh budget. Its state is private to this child module so code in the
    /// publisher module cannot construct either authority with a struct
    /// literal.
    pub(super) struct Recovery<'a> {
        work_budget: &'a mut RequestWorkBudget,
    }

    impl<'a> Recovery<'a> {
        pub(super) fn new(work_budget: &'a mut RequestWorkBudget) -> Self {
            Self { work_budget }
        }

        pub(super) fn check(&mut self, context: &'static str) -> Result<(), StoreError> {
            self.work_budget.check(context)
        }

        pub(super) fn sleep_after_contention(
            &mut self,
            context: &'static str,
        ) -> Result<(), StoreError> {
            self.work_budget.sleep_after_contention(context)
        }
    }

    /// One invocation's compiler-visible authority to drain a pending
    /// command.
    ///
    /// The primitive drain accepts only this type. It can be derived either
    /// from a registered publisher token or from storage-owned recovery
    /// authority, so there is no raw optional-budget or struct-literal path
    /// which an unclassified wrapper can call.
    pub(super) struct Invocation<'a> {
        work_budget: &'a mut RequestWorkBudget,
    }

    /// Joined recovery-leader authority for one pending command.
    ///
    /// This owns the process-local leader guard. Lower historical mutation
    /// boundaries accept only a proof borrowed from this value, so authority
    /// cannot outlive the joined recovery section or be constructed by a
    /// sibling module. The proof starts bound to the guard's exact recovery
    /// key and can advance only through a validated command-reissue chain.
    pub(super) struct Leader<'a> {
        _guard: MetadataCommandRecoveryGuard,
        work_budget: &'a mut RequestWorkBudget,
        seal: LeaderSeal,
        subject: RecoveryCommandSubject,
    }

    #[derive(Clone, Copy, Debug)]
    pub(super) struct LeaderProof<'a> {
        _seal: &'a LeaderSeal,
        root_subject: RecoveryCommandSubject,
        subject: RecoveryCommandSubject,
        predecessor_subject: Option<RecoveryCommandSubject>,
    }

    impl<'a> Invocation<'a> {
        pub(super) fn for_publisher(
            _publisher: impl crate::metadata_command::MetadataCommandPublisher,
            work_budget: &'a mut RequestWorkBudget,
        ) -> Self {
            Self { work_budget }
        }

        pub(super) fn for_recovery(recovery: &'a mut Recovery<'_>) -> Self {
            Self {
                work_budget: &mut *recovery.work_budget,
            }
        }

        pub(super) fn work_budget(&mut self) -> &mut RequestWorkBudget {
            self.work_budget
        }

        pub(super) fn admit_leader(
            &mut self,
            guard: MetadataCommandRecoveryGuard,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
        ) -> Result<Leader<'_>, StoreError> {
            if !guard.matches_command(pg_id, command) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-leader",
                });
            }
            Ok(Leader {
                _guard: guard,
                work_budget: &mut *self.work_budget,
                seal: LeaderSeal,
                subject: RecoveryCommandSubject::new(pg_id, command),
            })
        }
    }

    impl Leader<'_> {
        #[cfg(test)]
        pub(super) fn proof(&self) -> LeaderProof<'_> {
            LeaderProof {
                _seal: &self.seal,
                root_subject: self.subject,
                subject: self.subject,
                predecessor_subject: None,
            }
        }

        pub(super) fn parts_with_guard(
            &mut self,
        ) -> (
            &mut RequestWorkBudget,
            LeaderProof<'_>,
            &MetadataCommandRecoveryGuard,
        ) {
            (
                &mut *self.work_budget,
                LeaderProof {
                    _seal: &self.seal,
                    root_subject: self.subject,
                    subject: self.subject,
                    predecessor_subject: None,
                },
                &self._guard,
            )
        }
    }

    impl LeaderProof<'_> {
        pub(super) fn require_command(
            self,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
        ) -> Result<(), StoreError> {
            if !self.subject.matches(pg_id, command) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-subject",
                });
            }
            Ok(())
        }

        pub(super) fn derive_reissue(
            self,
            pg_id: PgId,
            source: &MetadataCommandEnvelope,
            replacement: &MetadataCommandEnvelope,
        ) -> Result<Self, StoreError> {
            self.require_command(pg_id, source)?;
            let source_id = source.id();
            let replacement_id = replacement.id();
            let same_payload = replacement.payload() == source.payload();
            let valid_chain = source_id.pg_id() == pg_id
                && replacement_id.pg_id() == pg_id
                && replacement_id.cluster_epoch() == source_id.cluster_epoch()
                && replacement_id.log_index().get() > source_id.log_index().get()
                && replacement
                    .payload()
                    .is_authorized_recovery_derivative_of(source.payload());
            if !valid_chain {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-reissue",
                });
            }
            Ok(Self {
                _seal: self._seal,
                root_subject: self.root_subject,
                subject: RecoveryCommandSubject::new(pg_id, replacement),
                predecessor_subject: if same_payload {
                    self.predecessor_subject
                } else {
                    Some(self.subject)
                },
            })
        }

        pub(super) fn require_root_command(
            self,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
        ) -> Result<(), StoreError> {
            if !self.root_subject.matches(pg_id, command) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-root-subject",
                });
            }
            Ok(())
        }

        pub(super) fn require_predecessor_command(
            self,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
        ) -> Result<(), StoreError> {
            if !self
                .predecessor_subject
                .is_some_and(|subject| subject.matches(pg_id, command))
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-predecessor-subject",
                });
            }
            Ok(())
        }

        pub(super) fn require_predecessor_context(
            self,
            pg_id: PgId,
            command: Option<&MetadataCommandEnvelope>,
        ) -> Result<(), StoreError> {
            match command {
                None if self.predecessor_subject.is_none() => Ok(()),
                Some(command) => self.require_predecessor_command(pg_id, command),
                None => Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata-command-recovery-predecessor-context",
                }),
            }
        }

        pub(super) fn has_predecessor(self) -> bool {
            self.predecessor_subject.is_some()
        }
    }
}

use metadata_command_drain_authority::{
    Invocation as MetadataCommandDrainAuthority, Leader as MetadataCommandRecoveryLeader,
    LeaderProof as MetadataCommandRecoveryProof, Recovery as MetadataCommandRecoveryDrainAuthority,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DurablePlacedSegmentShardRepairEnqueueSummary {
    pub(crate) scanned: usize,
    pub(crate) enqueued: usize,
}

#[cfg(test)]
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TestDirectPutWrittenSegment {
    pub data_pg_id: u32,
    pub ec: EcShape,
    pub written_shards: Vec<WrittenShardAck>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MetadataCommandCheckpointRecordSummary {
    pub(crate) scanned: usize,
    pub(crate) recorded: usize,
    pub(crate) already_current: usize,
    pub(crate) skipped_cadence: usize,
    pub(crate) skipped_inactive: usize,
    pub(crate) skipped_empty: usize,
    pub(crate) skipped_stale_epoch: usize,
    pub(crate) compacted: usize,
    pub(crate) compaction_deleted_entries: u64,
    pub(crate) compaction_noop: usize,
    pub(crate) compaction_no_checkpoint: usize,
    pub(crate) compaction_pending: usize,
    pub(crate) compaction_failed: usize,
    pub(crate) failed: usize,
    pub(crate) limit_reached: bool,
}

/// Resume position for bounded routine metadata-command checkpoint scans.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MetadataCommandCheckpointScanCursor {
    after_pg_id: Option<PgId>,
}

impl MetadataCommandCheckpointRecordSummary {
    fn mutations(&self) -> usize {
        self.recorded + self.compacted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataCommandCheckpointRecordDecision {
    Record,
    AlreadyCurrent,
    SkipCadence,
}

impl RequestWorkBudget {
    fn new(budget: Duration, max_attempts: Option<usize>) -> Self {
        Self {
            started: Instant::now(),
            budget,
            attempts: 0,
            contention_retries: 0,
            max_attempts,
            operation: "unknown",
            pg_id: None,
        }
    }

    fn for_operation(mut self, operation: &'static str) -> Self {
        self.operation = operation;
        self
    }

    fn for_pg(mut self, pg_id: PgId) -> Self {
        self.pg_id = Some(pg_id);
        self
    }

    fn deadline(&self) -> Instant {
        self.started.checked_add(self.budget).unwrap_or(self.started)
    }

    #[cfg(test)]
    pub(super) fn expire_for_test(&mut self) {
        self.started = Instant::now()
            .checked_sub(self.budget)
            .unwrap_or(self.started);
    }

    #[cfg(test)]
    pub(super) fn reset_for_test(&mut self, budget: Duration) {
        self.started = Instant::now();
        self.budget = budget;
    }

    fn check(&mut self, context: &'static str) -> Result<(), StoreError> {
        if self.started.elapsed() >= self.budget
            || self
                .max_attempts
                .is_some_and(|max_attempts| self.attempts >= max_attempts)
        {
            self.emit_budget_exhausted(context);
            return Err(StoreError::MetadataCommandContention { context });
        }
        self.attempts += 1;
        Ok(())
    }

    fn sleep_after_contention(&mut self, context: &'static str) -> Result<(), StoreError> {
        if self.started.elapsed() >= self.budget
            || self
                .max_attempts
                .is_some_and(|max_attempts| self.attempts >= max_attempts)
        {
            self.emit_budget_exhausted(context);
            return Err(StoreError::MetadataCommandContention { context });
        }
        self.contention_retries = self.contention_retries.saturating_add(1);
        let cap = metadata_contention_backoff_cap(self.contention_retries);
        let remaining = self
            .budget
            .checked_sub(self.started.elapsed())
            .unwrap_or(Duration::ZERO);
        let cap = cap.min(remaining);
        let delay = sleep_for_metadata_contention_cap(cap);
        emit_metadata_contention_backoff(self.operation, self.pg_id, context, delay);
        Ok(())
    }

    fn emit_budget_exhausted(&self, context: &'static str) {
        let _ = observability::emit_metadata_command_budget_exhausted(
            TRACE_TARGET,
            observability::MetadataCommandBudgetExhaustedSummary {
                pg_id: self.pg_id.map(|pg_id| pg_id.get()),
                operation: self.operation,
                context,
                elapsed_us: self.started.elapsed().as_micros(),
                budget_us: self.budget.as_micros(),
                attempts: self.attempts,
                max_attempts: self.max_attempts,
            },
        );
    }
}

pub(super) fn sleep_after_metadata_contention_retry_for(
    operation: &'static str,
    pg_id: Option<PgId>,
    context: &'static str,
    contention_retries: &mut usize,
) {
    *contention_retries = (*contention_retries).saturating_add(1);
    let delay =
        sleep_for_metadata_contention_cap(metadata_contention_backoff_cap(*contention_retries));
    emit_metadata_contention_backoff(operation, pg_id, context, delay);
}

pub(super) fn sleep_after_metadata_contention_retry_until(
    operation: &'static str,
    pg_id: Option<PgId>,
    context: &'static str,
    contention_retries: &mut usize,
    deadline: Instant,
) -> bool {
    *contention_retries = (*contention_retries).saturating_add(1);
    let cap = metadata_contention_backoff_cap(*contention_retries);
    let requested_delay = jittered_metadata_contention_backoff_delay(cap);
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        emit_metadata_contention_backoff(operation, pg_id, context, Duration::ZERO);
        return false;
    };
    let delay = requested_delay.min(remaining);
    if !delay.is_zero() {
        std::thread::sleep(delay);
    }
    emit_metadata_contention_backoff(operation, pg_id, context, delay);
    Instant::now() < deadline
}

fn sleep_for_metadata_contention_cap(cap: Duration) -> Duration {
    let delay = jittered_metadata_contention_backoff_delay(cap);
    if delay > Duration::ZERO {
        std::thread::sleep(delay);
    }
    delay
}

fn emit_metadata_contention_backoff(
    operation: &'static str,
    pg_id: Option<PgId>,
    context: &'static str,
    delay: Duration,
) {
    let _ = observability::emit_metadata_command_backoff(
        TRACE_TARGET,
        observability::MetadataCommandBackoffSummary {
            pg_id: pg_id.map(|pg_id| pg_id.get()),
            operation,
            context,
            sleep_us: delay.as_micros(),
        },
    );
}

fn jittered_metadata_contention_backoff_delay(cap: Duration) -> Duration {
    let max_nanos = cap.as_nanos().min(u128::from(u64::MAX)) as u64;
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    let mut bytes = [0u8; 8];
    if argmin_crypto::random::fill(&mut bytes).is_err() {
        return Duration::from_nanos((max_nanos / 2).max(1));
    }
    Duration::from_nanos((u64::from_le_bytes(bytes) % max_nanos.saturating_add(1)).max(1))
}

fn random_hex_identifier(prefix: &str) -> Result<String, argmin_crypto::CryptoError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut id_bytes = [0u8; 16];
    argmin_crypto::random::fill(&mut id_bytes)?;

    let mut encoded = String::with_capacity(prefix.len() + id_bytes.len() * 2);
    encoded.push_str(prefix);
    for byte in id_bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn metadata_contention_backoff_cap(contention_retries: usize) -> Duration {
    let multiplier = 1u32 << contention_retries.saturating_sub(1).min(8);
    METADATA_CONTENTION_BACKOFF_INITIAL
        .saturating_mul(multiplier)
        .min(METADATA_CONTENTION_BACKOFF_MAX)
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCommandApplyTestKind {
    CreateBucket,
    PutBucketVersioning,
    PutBucketAcl,
    PutBucketProperty,
    PutBucketSubresource,
    MarkBucketDeleting,
    DeleteFinalizedBucket,
    AdvanceMultipartCompletionBarrier,
    ReserveObjectGeneration,
    ReleaseObjectGeneration,
    ReserveObjectVersion,
    CommitDirectPutObject,
    CommitMultipartObject,
    DeleteObjectVersion,
    InsertDeleteMarker,
    PutObjectMetadata,
    CreateStreamUpload,
    AppendStreamSegment,
    AbortStreamUpload,
    CommitStreamPart,
    CreateMultipartUpload,
    AbortMultipartUpload,
    DeleteObjectPayloadReclaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectPayloadReclaimAttempt {
    Completed,
    Deferred,
    MissingRoot,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCommandApplyTestContext {
    pub node_id: NodeId,
    pub kind: MetadataCommandApplyTestKind,
    pub bucket: Option<BucketName>,
    pub key: Option<ObjectKey>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type MetadataCommandApplyContextTestHook =
    Arc<dyn Fn(MetadataCommandApplyTestContext) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub struct MetadataCommandApplyContextTestHookGuard {
    pub(super) scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandRecoveryTestGuard {
    _guard: Box<dyn Send>,
}

const TRACE_TARGET: &str = "storage";

type ShardScavengerLocationIdentity = (u32, u32, ShardKey);
type ShardScavengerRepairIdentity = (u32, ShardKey);

#[derive(Debug, Default)]
struct ShardScavengerReferenceScan {
    locations: HashSet<ShardScavengerLocationIdentity>,
    repair_work_by_shard: HashMap<ShardScavengerRepairIdentity, PlacedSegmentShardRepairWorkItem>,
}

fn conflicting_pending_object_metadata_command(context: &'static str) -> ObjectPgActionError {
    ObjectPgActionError::Store(StoreError::MetadataCommandContention { context })
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingMetadataCommandOutcome {
    Applied,
    Abandoned,
    RetryPartialExactConflict,
}

impl PendingMetadataCommandOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Abandoned => "abandoned",
            Self::RetryPartialExactConflict => "retry_partial_exact_conflict",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataCommandRouteMode {
    Normal,
    Recovery,
}

#[derive(Debug, Clone, Copy)]
struct MetadataCommandExecutionRoute<'a> {
    mode: MetadataCommandRouteMode,
    recovery_proof: Option<MetadataCommandRecoveryProof<'a>>,
    recovery_authorized_source: Option<&'a MetadataCommandEnvelope>,
    recovery_abandoned_source: Option<&'a MetadataCommandEnvelope>,
}

impl<'a> MetadataCommandExecutionRoute<'a> {
    fn normal() -> Self {
        Self {
            mode: MetadataCommandRouteMode::Normal,
            recovery_proof: None,
            recovery_authorized_source: None,
            recovery_abandoned_source: None,
        }
    }

    fn recovery(
        recovery_proof: MetadataCommandRecoveryProof<'a>,
        recovery_authorized_source: Option<&'a MetadataCommandEnvelope>,
        recovery_abandoned_source: Option<&'a MetadataCommandEnvelope>,
    ) -> Self {
        Self {
            mode: MetadataCommandRouteMode::Recovery,
            recovery_proof: Some(recovery_proof),
            recovery_authorized_source,
            recovery_abandoned_source,
        }
    }

    fn recovery_proof(self) -> MetadataCommandRecoveryProof<'a> {
        self.recovery_proof
            .expect("recovery execution route must carry joined leader authority")
    }

    fn require_command(
        self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        if self.mode == MetadataCommandRouteMode::Recovery {
            let proof = self.recovery_proof();
            proof.require_command(pg_id, command)?;
            if let Some(authorized_source) = self.recovery_authorized_source {
                proof.require_root_command(pg_id, authorized_source)?;
            }
        }
        Ok(())
    }

    fn require_recovery_predecessor(self, pg_id: PgId) -> Result<(), StoreError> {
        if self.mode == MetadataCommandRouteMode::Recovery {
            self.recovery_proof()
                .require_predecessor_context(pg_id, self.recovery_abandoned_source)?;
        }
        Ok(())
    }

    fn require_reissue_source(
        self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        replacement_payload: &MetadataCommandPayload,
    ) -> Result<(), StoreError> {
        self.require_command(pg_id, command)?;
        if self.mode == MetadataCommandRouteMode::Recovery {
            let proof = self.recovery_proof();
            if proof.has_predecessor() {
                proof.require_predecessor_context(pg_id, self.recovery_abandoned_source)?;
            } else if let Some(abandoned_source) = self.recovery_abandoned_source {
                proof.require_command(pg_id, abandoned_source)?;
                if replacement_payload == command.payload()
                    || !replacement_payload.is_authorized_recovery_derivative_of(command.payload())
                {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "metadata-command-recovery-derivative-source",
                    });
                }
            }
        }
        Ok(())
    }

    fn for_reissued_command(
        self,
        pg_id: PgId,
        source: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
    ) -> Result<Self, StoreError> {
        if self.mode == MetadataCommandRouteMode::Normal {
            return Ok(self);
        }
        Ok(Self {
            recovery_proof: Some(self.recovery_proof().derive_reissue(
                pg_id,
                source,
                replacement,
            )?),
            ..self
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataCommandRecoveryWaiterOutcome {
    StillPending,
    Applied,
    MissingNotApplied,
    ReplacedNotApplied,
}

impl MetadataCommandRecoveryWaiterOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::StillPending => "waiter_still_pending",
            Self::Applied => "waiter_applied",
            Self::MissingNotApplied => "waiter_missing_not_applied",
            Self::ReplacedNotApplied => "waiter_replaced_not_applied",
        }
    }

    #[cfg(test)]
    fn pending_outcome(self) -> Option<PendingMetadataCommandOutcome> {
        match self {
            Self::StillPending => None,
            Self::Applied => Some(PendingMetadataCommandOutcome::Applied),
            Self::MissingNotApplied | Self::ReplacedNotApplied => {
                Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExactPendingObjectMetadataCommand<'a> {
    command: &'a MetadataCommandEnvelope,
}

impl<'a> ExactPendingObjectMetadataCommand<'a> {
    /// Caller has already matched this PG-slot command to the request whose
    /// result will be returned to the client.
    pub(super) fn for_checked_request(command: &'a MetadataCommandEnvelope) -> Self {
        Self { command }
    }
}

fn object_payload_reclaim_generation(
    stale_payload: &Option<ObjectPayloadReclaimCommand>,
) -> Option<GenerationId> {
    match stale_payload {
        None => None,
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => Some(reclaim.generation_id),
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => Some(reclaim.generation_id),
    }
}

fn delete_object_version_reclaim_generation(
    target: &DeleteObjectVersionTarget,
) -> Option<GenerationId> {
    match target {
        DeleteObjectVersionTarget::DeleteMarker { .. } => None,
        DeleteObjectVersionTarget::Live { generation_id, .. } => Some(*generation_id),
    }
}

fn bucket_snapshot_error_to_object_pg_action_error(
    error: BucketSnapshotLoadError,
) -> ObjectPgActionError {
    match error {
        BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
        BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
    }
}

fn object_pg_action_error_to_bucket_snapshot_error(
    error: ObjectPgActionError,
) -> BucketSnapshotLoadError {
    match error {
        ObjectPgActionError::Store(error) => BucketSnapshotLoadError::Store(error),
        ObjectPgActionError::Metadata(error) => BucketSnapshotLoadError::Metadata(error),
        ObjectPgActionError::InvalidRequest { reason } => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other(reason),
            })
        }
        ObjectPgActionError::StaleObjectReadSubject => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale object read subject"),
            })
        }
        ObjectPgActionError::StaleDirectPutCommitSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale direct PUT commit snapshot"),
            })
        }
        ObjectPgActionError::StaleStreamFinalizeSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale stream finalize snapshot"),
            })
        }
        ObjectPgActionError::SnapshotReinspectionConflict => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("snapshot reinspection conflict"),
            })
        }
        ObjectPgActionError::StaleMultipartCompletionSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale multipart completion snapshot"),
            })
        }
        ObjectPgActionError::MultipartConditionalRequestConflict => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("multipart conditional request conflict"),
            })
        }
    }
}

fn stream_create_request_matches_session(
    session: &StreamUploadCommandRecord,
    create: &CreateStreamUploadReq,
) -> bool {
    session.session_id == create.session_id
        && session.bucket == create.bucket
        && session.key == create.key
        && session.target == create.target
        && session.state == StreamUploadState::InProgress
        && session.encryption == create.encryption
}

fn applied_stream_create_command<'a>(
    applied_commands: &'a [MetadataCommandEnvelope],
    create: &CreateStreamUploadReq,
    cleanup_after: Option<u64>,
) -> Option<&'a CreateStreamUploadCommand> {
    applied_commands.iter().rev().find_map(|command| {
        let MetadataCommandPayload::CreateStreamUpload(create_command) = command.payload() else {
            return None;
        };
        (stream_create_request_matches_session(&create_command.session, create)
            && create_command.cleanup_after == cleanup_after)
            .then_some(create_command.as_ref())
    })
}

fn applied_multipart_create_command<'a>(
    applied_commands: &'a [MetadataCommandEnvelope],
    create: &crate::CreateMultipartUploadReq,
) -> Option<&'a CreateMultipartUploadCommand> {
    applied_commands.iter().rev().find_map(|command| {
        let MetadataCommandPayload::CreateMultipartUpload(create_command) = command.payload()
        else {
            return None;
        };
        create_command
            .matches_request(create)
            .then_some(create_command.as_ref())
    })
}

fn pending_command_completes_stream_session(
    command: &MetadataCommandEnvelope,
    bucket: &BucketName,
    key: &ObjectKey,
    session_id: &SessionId,
) -> bool {
    match command.payload() {
        MetadataCommandPayload::CommitDirectPutObject(commit) => {
            commit.matches_stream_session(bucket, key, session_id)
        }
        MetadataCommandPayload::AbortStreamUpload(abort) => {
            abort.bucket == *bucket && abort.key == *key && abort.session_id == *session_id
        }
        MetadataCommandPayload::CommitStreamPart(commit) => {
            commit.bucket == *bucket && commit.key == *key && commit.session_id == *session_id
        }
        MetadataCommandPayload::CommitMultipartObject(commit) => {
            commit.object.bucket == *bucket
                && commit.object.key == *key
                && commit
                    .stream_uploads
                    .iter()
                    .any(|session| session.session_id == *session_id)
        }
        _ => false,
    }
}

#[cfg(any(test, feature = "test-hooks"))]
type StreamAbortHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type RetainedStreamAbortHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type MetadataCommandPendingInstallHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type MetadataCommandDrainedTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type MultipartCreateUploadIdPreparedTestHook = Arc<dyn Fn(&UploadId) + Send + Sync>;

#[cfg(test)]
type MultipartCreateCommandInstallTestHook = Arc<dyn Fn(&MetadataCommandEnvelope) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type DirectPutCommandIdHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(test)]
type DirectPutDeadlineExpiryHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ObjectGenerationCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ObjectVersionCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type StreamAppendCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamAppendCommandIdAllocatedHook = Arc<dyn Fn(MetadataCommandId) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ObjectMetadataReservationAcquiredHook =
    Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(test)]
type DirectPutAbandonedLogInspectionHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> Result<(), BucketSnapshotLoadError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type MetadataListingPgCompleteHook = Arc<dyn Fn(u32) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ReclaimCoordinationTestHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type PayloadShardCleanupTestHook =
    Arc<dyn Fn(&ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type PayloadShardReadTestHook =
    Arc<dyn Fn(&ShardLocation, &ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type PayloadShardWriteTestHook =
    Arc<dyn Fn(&ShardLocation, &ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type PayloadCleanupErrorTestHook = Arc<dyn Fn(&'static str, &StoreError) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
struct StorageClusterTestHooks {
    before_stream_abort_storage: Option<StreamAbortHook>,
    before_retained_stream_cleanup_capability: Option<StreamAbortHook>,
    after_retained_stream_cleanup_capability: Option<StreamAbortHook>,
    before_retained_stream_abort: Option<RetainedStreamAbortHook>,
    before_metadata_command_pending_install: Option<MetadataCommandPendingInstallHook>,
    #[cfg(test)]
    after_metadata_command_drain: Option<MetadataCommandDrainedTestHook>,
    #[cfg(test)]
    after_multipart_create_upload_id_prepared: Option<MultipartCreateUploadIdPreparedTestHook>,
    #[cfg(test)]
    before_multipart_create_command_install: Option<MultipartCreateCommandInstallTestHook>,
    before_direct_put_command_id: Option<DirectPutCommandIdHook>,
    #[cfg(test)]
    after_direct_put_snapshot_loaded: Option<DirectPutDeadlineExpiryHook>,
    #[cfg(test)]
    after_direct_put_action: Option<DirectPutDeadlineExpiryHook>,
    #[cfg(test)]
    before_direct_put_abandoned_log_inspection: Option<DirectPutAbandonedLogInspectionHook>,
    before_object_generation_command_id: Option<ObjectGenerationCommandIdHook>,
    before_object_version_command_id: Option<ObjectVersionCommandIdHook>,
    before_stream_append_command_id: Option<StreamAppendCommandIdHook>,
    #[cfg(test)]
    after_stream_append_command_id_allocated: Option<StreamAppendCommandIdAllocatedHook>,
    after_object_metadata_reservation_acquired: Option<ObjectMetadataReservationAcquiredHook>,
    after_metadata_listing_pg_complete: Option<MetadataListingPgCompleteHook>,
    after_reclaim_claim_acquired: Option<ReclaimCoordinationTestHook>,
    before_reclaim_ownership_lookup: Option<ReclaimCoordinationTestHook>,
    before_reclaim_claim_release: Option<ReclaimCoordinationTestHook>,
    before_placed_payload_shard_write: Option<PayloadShardWriteTestHook>,
    before_placed_payload_shard_read: Option<PayloadShardReadTestHook>,
    before_placed_payload_shard_delete: Option<PayloadShardCleanupTestHook>,
    before_metadata_primary_payload_ack_delete: Option<PayloadShardCleanupTestHook>,
    best_effort_payload_cleanup_error: Option<PayloadCleanupErrorTestHook>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct StreamAbortTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct RetainedStreamAbortTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct RetainedStreamCleanupCapabilityTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct AfterRetainedStreamCleanupCapabilityTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct MetadataCommandPendingInstallHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct MetadataCommandDrainedTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct MultipartCreateUploadIdPreparedTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct MultipartCreateCommandInstallTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct DirectPutCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct DirectPutSnapshotLoadedHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct DirectPutActionHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub struct DirectPutAbandonedLogInspectionHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct ObjectGenerationCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct ObjectVersionCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct StreamAppendCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub struct StreamAppendCommandIdAllocatedHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct ObjectMetadataReservationAcquiredHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(test)]
pub(crate) struct MetadataListingPgCompleteHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct ReclaimOwnershipLookupTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct ReclaimClaimAcquiredTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct ReclaimClaimReleaseTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct PayloadShardReadTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub(crate) struct PayloadShardWriteTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
enum PayloadCleanupTestHookKind {
    PlacedShardDelete,
    MetadataPrimaryAckDelete,
    BestEffortError,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct PayloadCleanupTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
    kind: PayloadCleanupTestHookKind,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamAbortTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_stream_abort_storage = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for RetainedStreamAbortTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_retained_stream_abort = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for RetainedStreamCleanupCapabilityTestHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_retained_stream_cleanup_capability = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for AfterRetainedStreamCleanupCapabilityTestHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_retained_stream_cleanup_capability = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for MetadataCommandPendingInstallHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install = None;
    }
}

#[cfg(test)]
impl Drop for MetadataCommandDrainedTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().after_metadata_command_drain = None;
    }
}

#[cfg(test)]
impl Drop for MultipartCreateUploadIdPreparedTestHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_multipart_create_upload_id_prepared = None;
    }
}

#[cfg(test)]
impl Drop for MultipartCreateCommandInstallTestHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_multipart_create_command_install = None;
    }
}

#[cfg(test)]
impl Drop for DirectPutCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_direct_put_command_id = None;
    }
}

#[cfg(test)]
impl Drop for DirectPutSnapshotLoadedHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().after_direct_put_snapshot_loaded = None;
    }
}

#[cfg(test)]
impl Drop for DirectPutActionHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().after_direct_put_action = None;
    }
}

#[cfg(test)]
impl Drop for DirectPutAbandonedLogInspectionHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_direct_put_abandoned_log_inspection = None;
    }
}

#[cfg(test)]
impl Drop for ObjectGenerationCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id = None;
    }
}

#[cfg(test)]
impl Drop for ObjectVersionCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_object_version_command_id = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamAppendCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_stream_append_command_id = None;
    }
}

#[cfg(test)]
impl Drop for StreamAppendCommandIdAllocatedHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_stream_append_command_id_allocated = None;
    }
}

#[cfg(test)]
impl Drop for ObjectMetadataReservationAcquiredHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired = None;
    }
}

#[cfg(test)]
impl Drop for MetadataListingPgCompleteHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_metadata_listing_pg_complete = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ReclaimOwnershipLookupTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_reclaim_ownership_lookup = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ReclaimClaimAcquiredTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().after_reclaim_claim_acquired = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ReclaimClaimReleaseTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_reclaim_claim_release = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PayloadShardReadTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_placed_payload_shard_read = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PayloadShardWriteTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_placed_payload_shard_write = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PayloadCleanupTestHookGuard {
    fn drop(&mut self) {
        let mut hooks = self.hooks.lock().unwrap();
        match self.kind {
            PayloadCleanupTestHookKind::PlacedShardDelete => {
                hooks.before_placed_payload_shard_delete = None;
            }
            PayloadCleanupTestHookKind::MetadataPrimaryAckDelete => {
                hooks.before_metadata_primary_payload_ack_delete = None;
            }
            PayloadCleanupTestHookKind::BestEffortError => {
                hooks.best_effort_payload_cleanup_error = None;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShardLocation {
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    shard_index: ShardIndex,
    node_id: NodeId,
}

impl ShardLocation {
    pub(crate) fn new(
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        node_id: NodeId,
    ) -> Self {
        Self {
            cluster_epoch,
            data_pg_id,
            shard_index,
            node_id,
        }
    }

    pub(crate) fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub(crate) fn data_pg_id(&self) -> DataPgId {
        self.data_pg_id
    }

    pub(crate) fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlacedSegmentShardValidation {
    Valid,
    MissingAck,
    WrongSize { expected: u64, actual: u64 },
    Unreadable { reason: String },
}

impl PlacedSegmentShardValidation {
    #[must_use]
    pub(crate) fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardHealth {
    pub(crate) shard_index: ShardIndex,
    pub(crate) shard_key: ShardKey,
    pub(crate) location: ShardLocation,
    pub(crate) validation: PlacedSegmentShardValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlacedSegmentShardSetRisk {
    Healthy,
    Degraded { tolerance_remaining: usize },
    Unrecoverable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardSetHealth {
    pub(crate) total_shards: usize,
    pub(crate) required_shards: usize,
    pub(crate) valid_shards: usize,
    pub(crate) risk: PlacedSegmentShardSetRisk,
    pub(crate) shards: Vec<PlacedSegmentShardHealth>,
}

impl PlacedSegmentShardSetHealth {
    #[must_use]
    pub(crate) fn repair_targets(&self) -> Vec<ShardIndex> {
        self.shards
            .iter()
            .filter(|shard| !shard.validation.is_valid())
            .map(|shard| shard.shard_index)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillCopyTarget {
    pub shard_index: ShardIndex,
    pub shard_key: ShardKey,
    pub source: ShardLocation,
    pub destination: ShardLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillPlan {
    pub source_health: PlacedSegmentShardSetHealth,
    pub desired_health: PlacedSegmentShardSetHealth,
    pub already_present: Vec<ShardIndex>,
    pub copy_targets: Vec<PlacedSegmentShardBackfillCopyTarget>,
    pub reconstruction_targets: Vec<ShardIndex>,
    pub unrecoverable_targets: Vec<ShardIndex>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillCandidateEnqueueSummary {
    pub scanned: usize,
    pub current_epoch: usize,
    pub already_queued: usize,
    pub already_complete: usize,
    pub enqueued: usize,
    pub unrecoverable: usize,
    pub deferred: usize,
    pub failed: usize,
    pub limit_reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PlacedSegmentShardBackfillCandidateKey {
    source_cluster_epoch: ClusterEpoch,
    data_pg_id: u32,
    segment_vid: u64,
    segment_okh: [u8; 16],
    stored_size: usize,
    segment_crc64: u64,
    ec_k: u8,
    ec_m: u8,
}

impl PlacedSegmentShardBackfillCandidateKey {
    fn new(request: SegmentStoredBytesRequest, source_cluster_epoch: ClusterEpoch) -> Self {
        Self {
            source_cluster_epoch,
            data_pg_id: request.data_pg_id,
            segment_vid: request.segment_vid.get(),
            segment_okh: request.segment_okh,
            stored_size: request.stored_size,
            segment_crc64: request.segment_crc64,
            ec_k: request.ec.k,
            ec_m: request.ec.m,
        }
    }
}

/// Resume position for bounded placed-segment backfill candidate verification.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PlacedSegmentShardBackfillCandidateScanCursor {
    after_pg_id: Option<PgId>,
    active_pg_id: Option<PgId>,
    reference_after: Option<PlacedSegmentBackfillReferenceCursor>,
}

impl PlacedSegmentShardBackfillPlan {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.copy_targets.is_empty()
            && self.reconstruction_targets.is_empty()
            && self.unrecoverable_targets.is_empty()
    }

    #[must_use]
    pub fn source_remaining_tolerance(&self) -> u8 {
        let tolerance = match self.source_health.risk {
            PlacedSegmentShardSetRisk::Healthy => self
                .source_health
                .total_shards
                .saturating_sub(self.source_health.required_shards),
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining,
            } => tolerance_remaining,
            PlacedSegmentShardSetRisk::Unrecoverable => 0,
        };
        u8::try_from(tolerance).unwrap_or(u8::MAX)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlacedSegmentShardHealthReadMode {
    CurrentRoute,
    HistoricalInspection,
}

pub(crate) struct AcquiredObjectPayloadNodeLeases {
    pub(crate) node_leases: Vec<Box<dyn ObjectPayloadLeaseNodeLease>>,
    pub(crate) leased_node_ids: BTreeSet<NodeId>,
}

pub struct ObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    node_leases: Vec<Box<dyn ObjectPayloadLeaseNodeLease>>,
    leased_node_ids: BTreeSet<NodeId>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    pg_id: u32,
    released: bool,
}

/// Subject-bound authority for reading an active object's selected payload
/// segments while deletion exclusion remains held.
///
/// The capability owns both the exact segment descriptors and their narrow
/// storage-node leases. Callers cannot use an independently retained lease to
/// read another segment through a raw cluster handle.
pub struct ActiveObjectPayloadRead {
    cluster: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    segments: Vec<ObjectPayloadSegment>,
    lease: Mutex<Option<ObjectPayloadLease>>,
}

impl ActiveObjectPayloadRead {
    fn matches_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.generation_id == generation_id
    }

    fn contains_segment(&self, segment: &ObjectPayloadSegment) -> bool {
        self.segments.contains(segment)
    }

    /// Verifies that every segment selected for a response belongs to this
    /// lease-bound payload authority.
    pub fn contains_object_payload_segments<'a>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a ObjectPayloadSegment>,
    ) -> bool {
        self.matches_subject(bucket, key, generation_id)
            && segments
                .into_iter()
                .all(|segment| self.contains_segment(segment))
    }

    pub fn read_segment_payload_stored_bytes_into(
        &self,
        segment: &ObjectPayloadSegment,
        dst: &mut Vec<u8>,
    ) -> Result<(), ObjectReadFailure> {
        if !self.contains_segment(segment) {
            return Err(ObjectReadFailure::from_store(
                StoreError::PayloadShardSetMismatch {
                    reason: "payload read is outside the active lease-bound segment set"
                        .to_string(),
                },
            ));
        }
        self.cluster
            .read_object_payload_segment_stored_bytes_into(segment, dst)
    }
}

impl Drop for ActiveObjectPayloadRead {
    fn drop(&mut self) {
        let Some(lease) = self
            .lease
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        lease.release_and_schedule_reclaim_if_needed();
    }
}

/// Subject-bound payload-read authority retained by a streaming response.
///
/// This capability deliberately does not retain request route admission. It
/// owns the deletion-exclusion lease acquired while that admission was valid
/// and permits reads only for segment descriptors present in the admitted
/// object snapshot. Read recovery may briefly reacquire admission solely to
/// record repair work, and only while the originating publication generation
/// is still current.
pub struct RetainedObjectPayloadRead {
    cluster: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    segments: Vec<ObjectPayloadSegment>,
    leased_node_ids: BTreeSet<NodeId>,
    lease: Mutex<Option<ObjectPayloadLease>>,
    repair_fence: Option<RetainedActiveRouteRepairFence>,
}

impl RetainedObjectPayloadRead {
    fn matches_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.generation_id == generation_id
    }

    fn contains_segment(&self, segment: &ObjectPayloadSegment) -> bool {
        self.segments.contains(segment)
    }

    /// Verifies that a logical read layout is wholly covered by this retained
    /// subject-bound snapshot authority.
    pub fn covers_complete_object_payload_layout<'a>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a ObjectPayloadSegment>,
    ) -> bool {
        self.matches_subject(bucket, key, generation_id) && self.segments.iter().eq(segments)
    }

    /// Verifies that every segment selected for a partial response belongs to
    /// this retained subject-bound snapshot.
    pub fn contains_object_payload_segments<'a>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a ObjectPayloadSegment>,
    ) -> bool {
        self.matches_subject(bucket, key, generation_id)
            && segments
                .into_iter()
                .all(|segment| self.contains_segment(segment))
    }

    pub fn read_segment_payload_stored_bytes_into(
        &self,
        segment: &ObjectPayloadSegment,
        dst: &mut Vec<u8>,
    ) -> Result<(), ObjectReadFailure> {
        self.read_segment_payload_stored_bytes_into_raw(segment, dst)
            .map_err(ObjectReadFailure::from_store)
    }

    fn read_segment_payload_stored_bytes_into_raw(
        &self,
        segment: &ObjectPayloadSegment,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        if !self.contains_segment(segment) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "payload read is outside the retained object snapshot".to_string(),
            });
        }
        self.cluster
            .read_retained_segment_payload_stored_bytes_at_placement_epoch_into(
                segment.placement_cluster_epoch(),
                segment.stored_bytes_request(),
                dst,
                &self.leased_node_ids,
                self.repair_fence.as_ref(),
            )
    }
}

impl Drop for RetainedObjectPayloadRead {
    fn drop(&mut self) {
        let Some(lease) = self
            .lease
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        lease.release_and_schedule_reclaim_if_needed();
    }
}

impl ObjectPayloadLease {
    fn new(
        cluster: Weak<StorageCluster>,
        acquired: AcquiredObjectPayloadNodeLeases,
        runtime_state: Arc<LocalClusterRuntimeState>,
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        pg_id: u32,
    ) -> Self {
        let AcquiredObjectPayloadNodeLeases {
            node_leases,
            leased_node_ids,
        } = acquired;
        Self {
            cluster,
            node_leases,
            leased_node_ids,
            runtime_state,
            bucket,
            key,
            generation_id,
            pg_id,
            released: false,
        }
    }

    pub fn release(mut self) -> ReleasedObjectPayloadLease {
        let remaining = release_object_payload_node_leases(&mut self.node_leases);
        self.released = true;
        ReleasedObjectPayloadLease {
            cluster: self.cluster.clone(),
            runtime_state: Arc::clone(&self.runtime_state),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation_id: self.generation_id,
            pg_id: self.pg_id,
            remaining,
        }
    }

    /// Releases this deletion-exclusion lease and schedules payload reclaim
    /// when it was the final lease for a generation that still has a durable
    /// reclaim root.
    pub fn release_and_schedule_reclaim_if_needed(self) {
        let released = self.release();
        if released.remaining() == 0 && released.payload_reclaim_exists().unwrap_or(true) {
            released.enqueue_object_payload_reclaim();
        }
    }

    fn leased_node_ids(&self) -> &BTreeSet<NodeId> {
        &self.leased_node_ids
    }
}

impl Drop for ObjectPayloadLease {
    fn drop(&mut self) {
        if !self.released {
            let _ = release_object_payload_node_leases(&mut self.node_leases);
        }
    }
}

fn release_object_payload_node_leases(
    node_leases: &mut [Box<dyn ObjectPayloadLeaseNodeLease>],
) -> usize {
    node_leases
        .iter_mut()
        .filter_map(|lease| lease.release().ok())
        .max()
        .unwrap_or(0)
}

pub struct ReleasedObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    pg_id: u32,
    remaining: usize,
}

impl ReleasedObjectPayloadLease {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn payload_reclaim_exists(&self) -> Result<bool, ObjectPgActionError> {
        let Some(cluster) = self.cluster.upgrade() else {
            // The already-acquired lease has been released; if its original
            // cluster handle is gone, conservatively let the caller enqueue a
            // reclaim retry. A worker will drop the item if no reclaim row
            // exists.
            return Ok(true);
        };
        cluster.payload_reclaim_exists(&self.bucket, &self.key, self.generation_id)
    }

    pub fn enqueue_object_payload_reclaim(&self) {
        if let Some(cluster) = self.cluster.upgrade() {
            cluster.enqueue_object_payload_reclaim(&self.bucket, &self.key, self.generation_id);
            return;
        }

        // The cluster handle can be gone after shutdown; keep the existing
        // conservative retry behavior for any worker still draining the queue.
        let _ = self.runtime_state.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            self.generation_id,
            self.pg_id,
        );
    }
}

/// Cluster-shaped storage handle.
#[derive(Debug, thiserror::Error)]
pub enum StorageClusterRuntimeMapRefreshError {
    #[error("control-plane runtime map refresh failed: {0}")]
    ControlPlane(#[from] ControlPlaneError),
    #[error("refreshed runtime map did not build a storage cluster: {0}")]
    Build(#[from] ClusterBuildError),
    #[error(
        "refreshed runtime map would downgrade storage cluster epoch from {current} to {candidate}"
    )]
    EpochDowngrade {
        current: ClusterEpoch,
        candidate: ClusterEpoch,
    },
    #[error("refreshed runtime map for epoch {candidate} has unbounded route-map validity")]
    UnboundedRouteMapValidity { candidate: ClusterEpoch },
    #[error("static route authority cannot publish or refresh a runtime map")]
    StaticRouteAuthorityRefresh,
    #[error(
        "refreshed runtime map for epoch {candidate} expired before publication (valid until {valid_until_ms}, now {now_ms})"
    )]
    ExpiredRouteMapValidity {
        candidate: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },
    #[error("storage cluster runtime-map refresh loop interval must be non-zero")]
    RefreshLoopZeroInterval,
    #[error("spawn storage cluster runtime-map refresh loop")]
    RefreshLoopSpawn {
        #[source]
        source: io::Error,
    },
}

impl StorageClusterRuntimeMapRefreshError {
    fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::ControlPlane(error) => control_plane_refresh_error_diagnostic_kind(error),
            Self::Build(_) => "cluster_build",
            Self::EpochDowngrade { .. } => "epoch_downgrade",
            Self::UnboundedRouteMapValidity { .. } => "unbounded_route_map_validity",
            Self::StaticRouteAuthorityRefresh => "static_route_authority_refresh",
            Self::ExpiredRouteMapValidity { .. } => "expired_route_map_validity",
            Self::RefreshLoopZeroInterval => "refresh_loop_zero_interval",
            Self::RefreshLoopSpawn { .. } => "refresh_loop_spawn",
        }
    }
}

fn control_plane_refresh_error_diagnostic_kind(error: &ControlPlaneError) -> &'static str {
    match error {
        ControlPlaneError::Io { diagnostic: source } => match source.kind() {
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => "control_plane_io_timeout",
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => {
                "control_plane_unavailable"
            }
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof => "control_plane_disconnected",
            _ => "control_plane_io",
        },
        ControlPlaneError::RpcProtocol { .. } => "control_plane_protocol",
        ControlPlaneError::RpcRemote { .. } => "control_plane_remote",
        ControlPlaneError::RpcUnconfirmed { .. } => "control_plane_unconfirmed",
        ControlPlaneError::AuthorityClockCheckpoint { .. }
        | ControlPlaneError::CommittedTimestampRegression { .. }
        | ControlPlaneError::CommittedTimestampTooFarAhead { .. }
        | ControlPlaneError::AuthorityClockLeadershipChanged { .. }
        | ControlPlaneError::AuthorityClockSourceUnavailable
        | ControlPlaneError::AuthorityClockNotEstablished { .. }
        | ControlPlaneError::AuthorityClockSampleWindowTooWide { .. }
        | ControlPlaneError::AuthorityClockAlreadyEstablished
        | ControlPlaneError::AuthorityClockGenerationMismatch { .. }
        | ControlPlaneError::AuthorityClockCommittedTimestampMismatch { .. }
        | ControlPlaneError::AuthorityClockRaftTermMismatch { .. }
        | ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority
        | ControlPlaneError::AuthorityClockWallBehindCommittedTimestamp { .. }
        | ControlPlaneError::AuthorityClockGenerationOverflow => "control_plane_clock",
        ControlPlaneError::PgPeeringPendingMetadataCommand { .. } => "pending_metadata_command",
        _ => "control_plane_state",
    }
}

#[derive(Debug, thiserror::Error)]
enum PendingMetadataCommandRefreshRecoveryError {
    #[error("load PG-scoped runtime map: {0}")]
    ControlPlane(#[from] ControlPlaneError),
    #[error("build historical recovery cluster: {0}")]
    Build(#[from] ClusterBuildError),
    #[error("load pending metadata command: {0}")]
    Store(#[from] StoreError),
    #[error("recover pending metadata command: {0}")]
    Recover(#[from] ObjectPgActionError),
    #[error(
        "pending metadata command recovery discovery failed for PG {pg_id} ({kind:?}): {detail}"
    )]
    DiscoveryFailure {
        pg_id: u32,
        kind: crate::control_plane::PendingMetadataCommandRecoveryDiscoveryFailureKind,
        detail: String,
    },
    #[error(
        "pending metadata command recovery authorization changed for PG {pg_id}: expected {expected:?}, current route state {actual_state}, authorization {actual:?}"
    )]
    AuthorizationChanged {
        pg_id: u32,
        expected: crate::control_plane::PendingMetadataCommandRecovery,
        actual_state: PgState,
        actual: Option<crate::control_plane::PendingMetadataCommandRecovery>,
    },
    #[error(
        "reported pending metadata command PG {pg_id} epoch {pending_epoch} route primary is node {actual_primary}, not reporting node {reporting_node}"
    )]
    ReportingNodeNotHistoricalPrimary {
        pg_id: u32,
        pending_epoch: ClusterEpoch,
        reporting_node: u32,
        actual_primary: u32,
    },
    #[error(
        "reported pending metadata command PG {pg_id} epoch {pending_epoch} historical route is {state}, not active"
    )]
    HistoricalRouteNotActive {
        pg_id: u32,
        pending_epoch: ClusterEpoch,
        state: PgState,
    },
    #[error(
        "reported pending metadata command identity changed for PG {pg_id}: expected epoch {expected_epoch} index {expected_index} checksum {expected_checksum}, got epoch {actual_epoch} index {actual_index} checksum {actual_checksum}"
    )]
    IdentityChanged {
        pg_id: u32,
        expected_epoch: ClusterEpoch,
        expected_index: u64,
        expected_checksum: u64,
        actual_epoch: ClusterEpoch,
        actual_index: u64,
        actual_checksum: u64,
    },
}

impl PendingMetadataCommandRefreshRecoveryError {
    fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::ControlPlane(error) => control_plane_refresh_error_diagnostic_kind(error),
            Self::Build(_) => "pending_recovery_build",
            Self::Store(_) => "pending_recovery_store",
            Self::Recover(_) => "pending_recovery_command",
            Self::DiscoveryFailure { .. } => "pending_recovery_discovery",
            Self::AuthorizationChanged { .. } => "pending_recovery_authorization_changed",
            Self::ReportingNodeNotHistoricalPrimary { .. } => "pending_recovery_reporter_changed",
            Self::HistoricalRouteNotActive { .. } => "pending_recovery_route_not_active",
            Self::IdentityChanged { .. } => "pending_recovery_identity_changed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StaticRouteMapContentDigest([u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StaticRouteAuthorityProof {
    content_digest: StaticRouteMapContentDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DynamicRouteAuthorityProof {
    content_digest: RuntimeMapContentDigest,
    freshness_proof: RuntimeMapFreshnessProof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageClusterRouteAuthority {
    Static(StaticRouteAuthorityProof),
    Dynamic(DynamicRouteAuthorityProof),
}

/// Storage-owned preparation for one exact embedded standalone topology.
///
/// The logical configuration is consumed once, so callers bind and open the
/// same topology rather than reconstructing it on opposite sides of the
/// durable identity boundary.
pub struct PreparedStandaloneEmbeddedTopology {
    metadata_primary_node_id: NodeId,
    configs: Vec<LocalNodeStoreConfig>,
    pg_ids: Box<[u32]>,
    default_ec_shape: EcShape,
    cluster_epoch: ClusterEpoch,
    route_identity: crate::StandaloneRouteIdentity,
}

impl PreparedStandaloneEmbeddedTopology {
    #[must_use]
    pub fn route_identity(&self) -> crate::StandaloneRouteIdentity {
        self.route_identity
    }

    pub fn open(self) -> Result<Arc<StorageCluster>, ClusterBuildError> {
        let local_map = LocalClusterMap::open_with_configs_and_epoch(
            self.metadata_primary_node_id,
            self.configs,
            &self.pg_ids,
            self.default_ec_shape,
            self.cluster_epoch,
        )?;
        let cluster = StorageCluster::from_static_local_map(Arc::new(local_map))?;
        if cluster.route_authority.require_static()?.content_digest.0 != self.route_identity.0 {
            return Err(ClusterBuildError::StandaloneEmbeddedRouteIdentityChanged);
        }
        Ok(cluster)
    }
}

impl StorageClusterRouteAuthority {
    fn static_for(local_map: &LocalClusterMap) -> Result<Self, ClusterBuildError> {
        if local_map.route_map_validity() != RouteMapValidity::Forever {
            return Err(ClusterBuildError::StaticRouteAuthorityBoundedValidity);
        }
        Ok(Self::Static(StaticRouteAuthorityProof {
            content_digest: StaticRouteMapContentDigest(
                local_map.static_route_map_content_digest(),
            ),
        }))
    }

    fn dynamic_for(
        local_map: &LocalClusterMap,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Self, ClusterBuildError> {
        if runtime_map.valid_until_ms().is_none() || local_map.route_map_valid_until_ms().is_none()
        {
            return Err(ClusterBuildError::DynamicRouteAuthorityUnboundedValidity {
                epoch: runtime_map.cluster_epoch(),
            });
        }
        if local_map.epoch() != runtime_map.cluster_epoch() {
            return Err(ClusterBuildError::DynamicRouteAuthorityEpochMismatch {
                local: local_map.epoch(),
                authority: runtime_map.cluster_epoch(),
            });
        }
        if local_map.route_map_validity() != runtime_map.validity() {
            return Err(ClusterBuildError::DynamicRouteAuthorityValidityMismatch {
                local: local_map.route_map_validity(),
                authority: runtime_map.validity(),
            });
        }
        local_map.validate_dynamic_route_map_content(runtime_map)?;
        Ok(Self::Dynamic(DynamicRouteAuthorityProof {
            content_digest: runtime_map.content_digest(),
            freshness_proof: *runtime_map.freshness_proof(),
        }))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_dynamic_for(local_map: &LocalClusterMap) -> Result<Self, ClusterBuildError> {
        if local_map.route_map_valid_until_ms().is_none() {
            return Err(ClusterBuildError::DynamicRouteAuthorityUnboundedValidity {
                epoch: local_map.epoch(),
            });
        }
        Ok(Self::Dynamic(DynamicRouteAuthorityProof {
            content_digest: RuntimeMapContentDigest::from_bytes(
                local_map.static_route_map_content_digest(),
            ),
            freshness_proof: RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation: crate::control_plane::AuthorityIncarnation::INITIAL,
            },
        }))
    }

    fn dynamic_proof(
        self,
    ) -> Result<DynamicRouteAuthorityProof, StorageClusterRuntimeMapRefreshError> {
        match self {
            Self::Static(_) => {
                Err(StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh)
            }
            Self::Dynamic(proof) => Ok(proof),
        }
    }

    fn require_static(self) -> Result<StaticRouteAuthorityProof, ClusterBuildError> {
        match self {
            Self::Static(proof) => Ok(proof),
            Self::Dynamic(_) => {
                Err(ClusterBuildError::DynamicRouteAuthorityRequiresRuntimeMapHandle)
            }
        }
    }
}
