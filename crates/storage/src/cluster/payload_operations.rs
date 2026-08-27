// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

fn object_version_allocator_command_contention(error: &ObjectPgActionError) -> bool {
    matches!(
        error,
        ObjectPgActionError::Store(error)
            if request_ops::store_error_is_metadata_command_contention(error)
    )
}

fn direct_put_uninstalled_pending_drain_error(
    error: ObjectPgActionError,
) -> ObjectPgActionError {
    pending_object_metadata_convergence_as_contention(
        error,
        "direct PUT blocked by pending command convergence",
    )
}

fn unrelated_pending_object_metadata_drain_error(
    error: ObjectPgActionError,
) -> ObjectPgActionError {
    pending_object_metadata_convergence_as_contention(
        error,
        "request blocked by unrelated object metadata command convergence",
    )
}

fn pending_object_metadata_convergence_as_contention(
    error: ObjectPgActionError,
    context: &'static str,
) -> ObjectPgActionError {
    match error {
        ObjectPgActionError::Store(
            StoreError::MetadataCommandOutcomeUnconfirmed { .. }
            | StoreError::MetadataCommandIrrevocableConvergencePending { .. }
            | StoreError::MetadataCommandDependencyConvergencePending { .. },
        ) => ObjectPgActionError::Store(StoreError::MetadataCommandContention { context }),
        error => error,
    }
}

fn metadata_command_irreversible_resolution(
    error: &ObjectPgActionError,
) -> Option<MetadataCommandRecoveryResolution> {
    match error {
        ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed { .. }) => {
            Some(MetadataCommandRecoveryResolution::OutcomeUnconfirmed)
        }
        ObjectPgActionError::Store(
            StoreError::MetadataCommandIrrevocableConvergencePending { .. },
        ) => Some(MetadataCommandRecoveryResolution::IrrevocableConvergencePending),
        _ => None,
    }
}

fn metadata_command_recovery_resolution_error(
    command: &MetadataCommandEnvelope,
    resolution: MetadataCommandRecoveryResolution,
) -> Option<ObjectPgActionError> {
    let id = command.id();
    let error = match resolution {
        MetadataCommandRecoveryResolution::Outcome(_) => return None,
        MetadataCommandRecoveryResolution::OutcomeUnconfirmed => {
            StoreError::MetadataCommandOutcomeUnconfirmed {
                pg_id: id.pg_id().get(),
                cluster_epoch: id.cluster_epoch(),
                log_index: id.log_index().get(),
            }
        }
        MetadataCommandRecoveryResolution::IrrevocableConvergencePending => {
            StoreError::MetadataCommandIrrevocableConvergencePending {
                pg_id: id.pg_id().get(),
                cluster_epoch: id.cluster_epoch(),
                log_index: id.log_index().get(),
            }
        }
    };
    Some(ObjectPgActionError::Store(error))
}

#[derive(Debug)]
enum PendingObjectMetadataCommandCompletion {
    Applied,
    PublishedPendingRecovery,
    Abandoned,
    TerminalCleanupPending { applied: bool },
    RetryPartialExactConflict(Box<MetadataCommandEnvelope>),
}

impl PendingObjectMetadataCommandCompletion {
    fn into_outcome(self) -> PendingMetadataCommandOutcome {
        match self {
            Self::Applied => PendingMetadataCommandOutcome::Applied,
            Self::PublishedPendingRecovery => {
                PendingMetadataCommandOutcome::PublishedPendingRecovery
            }
            Self::Abandoned => PendingMetadataCommandOutcome::Abandoned,
            Self::TerminalCleanupPending { applied } => {
                PendingMetadataCommandOutcome::TerminalCleanupPending { applied }
            }
            Self::RetryPartialExactConflict(_) => {
                PendingMetadataCommandOutcome::RetryPartialExactConflict
            }
        }
    }
}

fn metadata_command_apply_failure_is_definitive_terminal_stream_outcome(
    command: &MetadataCommandEnvelope,
    failure: &request_ops::MetadataCommandApplyFailure,
) -> bool {
    if !failure.progress.is_abortable()
        || failure.applied_nodes != 0
        || failure.may_have_applied
    {
        return false;
    }
    match &failure.source {
        BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload { upload_id }) => command
            .payload()
            .stream_upload_no_such_upload_subject()
            .is_some_and(|(_, expected_upload_id)| expected_upload_id.as_str() == upload_id),
        BucketSnapshotLoadError::Metadata(MetadataError::StreamSessionNotFound {
            session_id,
        }) => command
            .payload()
            .stream_upload_terminal_session_subject()
            .is_some_and(|(expected_session_id, _)| expected_session_id.as_str() == session_id),
        BucketSnapshotLoadError::Metadata(MetadataError::StreamSessionNotInProgress { .. }) => {
            command
                .payload()
                .stream_upload_terminal_session_subject()
                .is_some()
        }
        _ => false,
    }
}

#[cfg(test)]
pub(in crate::cluster) type TerminalStreamApplyFailureTestHook = Arc<
    dyn Fn(&MetadataCommandEnvelope, &mut request_ops::MetadataCommandApplyFailure) + Send + Sync,
>;

#[cfg(test)]
static TERMINAL_STREAM_APPLY_FAILURE_TEST_HOOKS: std::sync::OnceLock<
    Mutex<HashMap<usize, TerminalStreamApplyFailureTestHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(in crate::cluster) struct TerminalStreamApplyFailureTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
impl Drop for TerminalStreamApplyFailureTestHookGuard {
    fn drop(&mut self) {
        TERMINAL_STREAM_APPLY_FAILURE_TEST_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
fn maybe_run_terminal_stream_apply_failure_test_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    failure: &mut request_ops::MetadataCommandApplyFailure,
) {
    let hook = TERMINAL_STREAM_APPLY_FAILURE_TEST_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(command, failure);
    }
}

enum CollectedPendingObjectMetadataCommands {
    Drained(Vec<MetadataCommandEnvelope>),
    PendingRecovery(Vec<MetadataCommandEnvelope>),
}

#[derive(Clone, Copy)]
struct PendingMetadataCommandDrainContext<'a> {
    route_mode: MetadataCommandRouteMode,
    recovery_authorized_source: Option<&'a MetadataCommandEnvelope>,
    convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
    admission_policy: PendingMetadataCommandDrainAdmissionPolicy,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingMetadataCommandDrainAdmissionPolicy {
    ExactCommandOwner,
    UnrelatedDrainer,
    UnrelatedDrainerProjectOutcome,
}

impl PendingMetadataCommandDrainAdmissionPolicy {
    fn requests_authorized_recovery_handoff(self) -> bool {
        !matches!(self, Self::ExactCommandOwner)
    }

    fn projects_retained_outcome(self) -> bool {
        !matches!(self, Self::UnrelatedDrainer)
    }
}

fn observed_pending_metadata_command_outcome_for_drain_policy(
    outcome: PendingMetadataCommandOutcome,
    admission_policy: PendingMetadataCommandDrainAdmissionPolicy,
) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
    if outcome.retains_pending_slot() && !admission_policy.projects_retained_outcome() {
        Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery)
    } else {
        Ok(outcome)
    }
}

fn relinquished_pending_metadata_command_outcome_for_drain_policy(
    outcome: PendingMetadataCommandOutcome,
    admission_policy: PendingMetadataCommandDrainAdmissionPolicy,
) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
    if outcome.retains_pending_slot() && !admission_policy.projects_retained_outcome() {
        Err(ObjectPgActionError::MetadataCommandRecoveryTransferred)
    } else {
        Ok(outcome)
    }
}

impl<'a> PendingMetadataCommandDrainContext<'a> {
    fn unrelated(
        convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
        admission_policy: PendingMetadataCommandDrainAdmissionPolicy,
    ) -> Self {
        debug_assert!(admission_policy.requests_authorized_recovery_handoff());
        Self {
            route_mode: MetadataCommandRouteMode::Normal,
            recovery_authorized_source: None,
            convergence_requirement,
            admission_policy,
        }
    }

    fn exact(
        convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
    ) -> Self {
        Self {
            route_mode: MetadataCommandRouteMode::Normal,
            recovery_authorized_source: None,
            convergence_requirement,
            admission_policy: PendingMetadataCommandDrainAdmissionPolicy::ExactCommandOwner,
        }
    }

    fn recovery(
        recovery_authorized_source: Option<&'a MetadataCommandEnvelope>,
        convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
    ) -> Self {
        Self {
            route_mode: MetadataCommandRouteMode::Recovery,
            recovery_authorized_source,
            convergence_requirement,
            admission_policy: PendingMetadataCommandDrainAdmissionPolicy::ExactCommandOwner,
        }
    }
}

impl CollectedPendingObjectMetadataCommands {
    fn empty_drained() -> Self {
        Self::Drained(Vec::new())
    }

    fn commands(&self) -> &[MetadataCommandEnvelope] {
        match self {
            Self::Drained(commands) | Self::PendingRecovery(commands) => commands,
        }
    }

    fn require_drained_for_unmatched_request(&self) -> Result<(), ObjectPgActionError> {
        let Self::PendingRecovery(_) = self else {
            return Ok(());
        };
        Err(ObjectPgActionError::Store(
            StoreError::MetadataCommandContention {
                context: "unrelated terminal metadata command awaiting recovery cleanup",
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExactPendingObjectMetadataCommandOutcome {
    Applied,
    Reinspect,
}

#[derive(Clone, Copy)]
enum ObjectPendingCommandFinishPolicy {
    Standard,
    AbandonZeroApplyStaleReservation,
    AllocatorReinspectContention,
}

struct ObjectPendingCommandFinishContext<'a> {
    execution_route: MetadataCommandExecutionRoute<'a>,
    recovery_guard: Option<&'a MetadataCommandRecoveryGuard>,
    finish_policy: ObjectPendingCommandFinishPolicy,
    convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
}

#[derive(Clone, Copy)]
struct ObjectPendingCommandCleanupContext<'a> {
    execution_route: MetadataCommandExecutionRoute<'a>,
    recovery_guard: Option<&'a MetadataCommandRecoveryGuard>,
    convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
}

impl ObjectPendingCommandFinishPolicy {
    fn abandons_zero_apply_stale_reservation(self) -> bool {
        matches!(self, Self::AbandonZeroApplyStaleReservation)
    }

    fn returns_metadata_command_contention(self) -> bool {
        matches!(self, Self::AllocatorReinspectContention)
    }
}

impl StorageCluster {
    #[cfg(test)]
    pub(in crate::cluster) fn test_install_terminal_stream_apply_failure_hook(
        &self,
        hook: TerminalStreamApplyFailureTestHook,
    ) -> TerminalStreamApplyFailureTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        TERMINAL_STREAM_APPLY_FAILURE_TEST_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(scope_id, hook);
        TerminalStreamApplyFailureTestHookGuard { scope_id }
    }

    pub(crate) fn place_payload_shards(
        &self,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        self.local_map.place_payload_shards(
            self.operation_epoch(),
            data_pg_id,
            ec_shape,
            stable_placement_key,
        )
    }

    pub(crate) fn place_payload_shards_for_pg_route(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
        acting_set: &[NodeId],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        LocalClusterMap::place_payload_shards_for_pg_route(
            cluster_epoch,
            data_pg_id,
            ec_shape,
            stable_placement_key,
            acting_set,
        )
    }

    pub(crate) fn place_payload_shards_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        if route.pg_id() != data_pg_id.pg_id() {
            return Err(ClusterBuildError::InvalidLocalPlacement {
                reason: format!(
                    "route PG {} does not match data PG {}",
                    route.pg_id().get(),
                    data_pg_id.pg_id().get()
                ),
            });
        }
        self.place_payload_shards_for_pg_route(
            route.cluster_epoch(),
            data_pg_id,
            ec_shape,
            stable_placement_key,
            route.acting_set(),
        )
    }

    pub(crate) fn write_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.maybe_run_before_placed_payload_shard_write_hook(location, key)?;
        self.local_map
            .write_payload_shard(self.operation_epoch(), location, key, data)
    }

    fn write_payload_shard_with_effect_fence(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, ShardIoError> {
        self.maybe_run_before_placed_payload_shard_write_hook(location, key)?;
        self.local_map.write_payload_shard_with_effect_fence(
            self.operation_epoch(),
            location,
            key,
            data,
            effect_fence,
        )
    }

    pub(crate) fn repair_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.local_map
            .repair_payload_shard(self.operation_epoch(), location, key, data)
    }

    #[cfg(test)]
    pub(crate) fn read_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.local_map
            .test_read_payload_shard(self.operation_epoch(), location, key, expected)
    }

    fn read_payload_shard_for_historical_inspection_self_validating(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(Vec<u8>, WriteAck), ShardIoError> {
        self.local_map
            .read_payload_shard_for_historical_inspection(location, key)
    }

    fn read_payload_shard_for_historical_inspection(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        let (payload, actual) =
            self.read_payload_shard_for_historical_inspection_self_validating(location, key)?;
        if actual != expected {
            return Err(ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::ShardAckMismatch {
                    shard: key.clone(),
                    expected_size: expected.stored_size,
                    expected_crc: expected.crc64,
                    actual_size: actual.stored_size,
                    actual_crc: actual.crc64,
                },
            });
        }
        Ok(payload)
    }

    #[cfg(test)]
    pub(crate) fn read_payload_shard_into(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.local_map
            .read_payload_shard_into(self.operation_epoch(), location, key, expected, dst)
    }

    #[cfg(test)]
    pub(crate) fn delete_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        self.local_map
            .delete_payload_shard(self.operation_epoch(), location, key)
    }

    #[cfg(test)]
    pub(crate) fn write_direct_put_segment_payload_shards(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
        segment_okh: &[u8; 16],
        data: &[u8],
    ) -> Result<DirectPutWrittenSegment, StoreError> {
        self.write_direct_put_segment_payload_shards_with_route_validation(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            generation_id,
            segment_index,
            segment_okh,
            data,
            || Ok(()),
        )
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_write_direct_put_segment_payload_shards(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
        segment_okh: &[u8; 16],
        data: &[u8],
    ) -> Result<TestDirectPutWrittenSegment, StoreError> {
        let written = self.write_direct_put_segment_payload_shards(
            bucket,
            key,
            generation_id,
            segment_index,
            segment_okh,
            data,
        )?;
        Ok(TestDirectPutWrittenSegment {
            data_pg_id: written.data_pg_id,
            ec: written.ec,
            written_shards: written.written_shards,
        })
    }

    fn write_direct_put_segment_payload_shards_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        generation_id: GenerationId,
        segment_index: u32,
        segment_okh: &[u8; 16],
        data: &[u8],
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<DirectPutWrittenSegment, StoreError> {
        let PutObjectMutationEffectRoute {
            bucket,
            key,
            object_pg_id,
            effect_fence,
            ..
        } = route;
        debug_assert_eq!(object_pg_id, self.object_metadata_pg(bucket, key));
        require_valid_route()?;
        let ec = self.default_payload_ec_shape();
        let data_pg_id = self
            .local_map
            .object_generation_segment_data_pg(bucket, key, generation_id, segment_index)
            .get();
        let data_pg = self.validated_data_pg(PgId::new(data_pg_id))?;
        let segment_vid = generation_id;
        let written_shards = self.write_placed_segment_payload_shards_with_route_validation(
            data_pg,
            ec,
            PlacedSegmentPayloadWrite {
                segment_okh,
                segment_vid,
                data,
            },
            Some(effect_fence),
            &mut require_valid_route,
            &mut || Ok::<(), StoreError>(()),
        )?;

        Ok(DirectPutWrittenSegment {
            data_pg_id,
            ec,
            written_shards,
        })
    }

    #[cfg(test)]
    fn write_placed_segment_payload_shards(
        &self,
        data_pg: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        self.write_placed_segment_payload_shards_with_route_validation(
            data_pg,
            ec,
            PlacedSegmentPayloadWrite {
                segment_okh,
                segment_vid,
                data,
            },
            None,
            &mut || Ok(()),
            &mut || Ok(()),
        )
    }

    fn write_placed_segment_payload_shards_with_route_validation<E>(
        &self,
        data_pg: DataPgId,
        ec: EcShape,
        write: PlacedSegmentPayloadWrite<'_>,
        effect_fence: Option<AdmittedRouteEffectFence>,
        require_valid_route: &mut impl FnMut() -> Result<(), StoreError>,
        maintain_lease: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Vec<WrittenShardAck>, E>
    where
        E: From<StoreError>,
    {
        let PlacedSegmentPayloadWrite {
            segment_okh,
            segment_vid,
            data,
        } = write;
        require_valid_route().map_err(E::from)?;
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)
            .map_err(E::from)?;
        self.local_map.write_erasure_coded_segment_shards_with(
            segment_okh,
            segment_vid,
            data,
            ec,
            |shard_batch| {
                let mut written_acks = Vec::with_capacity(shard_batch.len());
                let mut written_for_cleanup: Vec<WrittenShardAck> =
                    Vec::with_capacity(shard_batch.len());
                for (location, (shard_key, shard_payload)) in
                    locations.iter().zip(shard_batch.iter())
                {
                    if let Err(error) = maintain_lease() {
                        self.delete_payload_shard_keys_best_effort(
                            data_pg.get(),
                            ec,
                            segment_okh,
                            segment_vid,
                            written_for_cleanup
                                .iter()
                                .map(|written| written.key.clone()),
                        );
                        return Err(error);
                    }
                    if let Err(error) = require_valid_route() {
                        self.delete_payload_shard_keys_best_effort(
                            data_pg.get(),
                            ec,
                            segment_okh,
                            segment_vid,
                            written_for_cleanup
                                .iter()
                                .map(|written| written.key.clone()),
                        );
                        return Err(E::from(error));
                    }
                    let write_result = effect_fence.map_or_else(
                        || self.write_payload_shard(*location, shard_key, shard_payload),
                        |effect_fence| {
                            self.write_payload_shard_with_effect_fence(
                                *location,
                                shard_key,
                                shard_payload,
                                effect_fence,
                            )
                        },
                    );
                    match write_result {
                        Ok(ack) => {
                            written_acks.push((shard_key.clone(), ack));
                            written_for_cleanup.push(WrittenShardAck {
                                key: shard_key.clone(),
                                ack,
                            });
                        }
                        Err(error) => {
                            self.delete_payload_shard_keys_best_effort(
                                data_pg.get(),
                                ec,
                                segment_okh,
                                segment_vid,
                                written_for_cleanup
                                    .iter()
                                    .map(|written| written.key.clone()),
                            );
                            return Err(E::from(shard_io_error_to_store(error)));
                        }
                    }
                }
                Ok(written_acks)
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn reserve_put_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        self.reserve_put_object_generation_with_route_validation(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            reservation_id,
            || Ok(()),
        )
    }

    fn reserve_put_object_generation_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        reservation_id: &SessionId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(ReservePutObjectGeneration);
        let PutObjectMutationEffectRoute {
            object_pg_id,
            bucket,
            key,
            effect_fence,
            ..
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mut work_budget =
            RequestWorkBudget::new(OBJECT_GENERATION_RESERVATION_RETRY_BUDGET, None)
                .for_operation("reserve_object_generation")
                .for_pg(pg_id);
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("object generation reservation retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                // Same-object requests must reobserve a published direct PUT so their
                // conditions see its result. An unrelated direct PUT remains an authorized
                // recovery handoff that this foreground request must not take over.
                let unrelated_direct_put = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket != *bucket || commit.object.key != *key
                );
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let generation_id = reservation.generation_id;
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied
                            | PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            } => {
                                return Ok(generation_id);
                            }
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation reservation command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: false,
                            } => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation reservation abandoned pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
                #[cfg(test)]
                request_ops::maybe_run_object_generation_pending_drain_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    &command,
                    &mut work_budget,
                );
                match self
                    .drain_pending_object_metadata_command_with_work_budget(
                        publisher,
                        pg_id,
                        &command,
                        &mut work_budget,
                    )
                    .map(|_| ())
                {
                    Ok(()) => {}
                    Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery)
                        if !unrelated_direct_put =>
                    {
                        #[cfg(test)]
                        request_ops::maybe_run_pending_object_metadata_command_recovery_transferred_hook(
                            self.metadata_command_apply_test_hook_scope_id(),
                            &command,
                        );
                        work_budget
                            .sleep_after_contention(
                                "object generation reservation authorized recovery budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                work_budget
                    .sleep_after_contention(
                        "object generation reservation pending drain retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }

            let generation_client = self.object_generation_metadata_primary_client(bucket, key)?;
            let generation_route = generation_client.open_object_generation_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
            )?;
            match generation_route.object_generation_reservation(reservation_id) {
                Ok(generation_id) => return Ok(generation_id),
                Err(ObjectPgActionError::Metadata(
                    MetadataError::ObjectGenerationReservationNotFound { .. },
                )) => {}
                Err(error) => return Err(error),
            }
            let generation_id = generation_route.next_object_generation_id()?;
            self.maybe_run_before_object_generation_command_id_hook();
            if generation_route.next_object_generation_id()? != generation_id {
                work_budget
                    .sleep_after_contention(
                        "object generation reservation stale generation retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }
            self.maybe_run_before_metadata_command_pending_install_hook();
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let command = match self.install_allocator_cleanup_metadata_command_with_fresh_id(
                publisher,
                pg_id,
                bucket,
                false,
                Some(effect_fence),
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReserveObjectGeneration(
                            ReserveObjectGenerationCommand::new(
                                bucket.clone(),
                                key.clone(),
                                reservation_id.clone(),
                                generation_id,
                                crate::clock::current_time_millis(),
                            ),
                        ),
                    )
                },
            )? {
                AllocatorCleanupFreshInstallOutcome::Installed(command) => *command,
                AllocatorCleanupFreshInstallOutcome::PendingContenderDrained => {
                    work_budget
                        .sleep_after_contention(
                            "object generation reservation pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                AllocatorCleanupFreshInstallOutcome::LogConflictHandled => {
                    work_budget
                        .sleep_after_contention(
                            "object generation reservation log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(outcome) => {
                        if outcome == request_ops::MetadataCommandApplyOutcome::Converged {
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                        }
                        match command.payload() {
                            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                                return Ok(reservation.generation_id);
                            }
                            _ => {
                                unreachable!(
                                    "reserve object generation pending command kind changed"
                                )
                            }
                        }
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        match command.payload() {
                            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                                return Ok(reservation.generation_id);
                            }
                            _ => {
                                unreachable!(
                                    "reserve object generation pending command kind changed"
                                )
                            }
                        }
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial reserve object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.progress.is_abortable()
                            && error.applied_nodes == 0
                            && Self::reserve_object_generation_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        if let Err(abandon_error) =
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                        {
                            if !Self::metadata_command_log_conflict_matches(
                                &command,
                                &abandon_error.source,
                            ) {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(
                                    abandon_error.source,
                                ));
                            }
                        }
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        work_budget
                            .sleep_after_contention(
                                "object generation reservation conflict cleanup retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                    Err(error)
                        if error.progress.is_abortable()
                            && error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let reissued = match self.reissue_pending_metadata_command_until(
                            pg_id,
                            &command,
                            work_budget.deadline(),
                        ) {
                            Ok(Some(reissued)) => reissued,
                            Ok(None) => break,
                            Err(BucketSnapshotLoadError::Store(
                                StoreError::MetadataCommandLogConflict { .. },
                            )) => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable generation reservation reissue conflict",
                                ));
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                            }
                        };
                        command = reissued;
                        if let Err(error) = work_budget.sleep_after_contention(
                            "object generation reservation reissue retry budget exhausted",
                        ) {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            return Err(ObjectPgActionError::Store(error));
                        }
                    }
                    Err(error) => {
                        if error.progress.is_abortable() && error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    fn reserve_next_object_version(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version_with_completion_admission(
            pg_id,
            bucket,
            key,
            false,
            None,
            || Ok(()),
        )
    }

    fn reserve_next_object_version_with_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        effect_fence: AdmittedRouteEffectFence,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version_with_completion_admission(
            pg_id,
            bucket,
            key,
            false,
            Some(effect_fence),
            require_valid_route,
        )
    }

    fn reserve_next_object_version_for_completion_with_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        effect_fence: AdmittedRouteEffectFence,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version_with_completion_admission(
            pg_id,
            bucket,
            key,
            true,
            Some(effect_fence),
            require_valid_route,
        )
    }

    fn reserve_next_object_version_with_completion_admission(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        completion_admission: bool,
        effect_fence: Option<AdmittedRouteEffectFence>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<VersionId, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(ReserveNextObjectVersion);
        let mut work_budget = RequestWorkBudget::new(OBJECT_VERSION_RESERVATION_RETRY_BUDGET, None)
            .for_operation("reserve_object_version")
            .for_pg(pg_id);
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("object version reservation retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::ReserveObjectVersion(reservation) = command.payload()
                {
                    let reserved_version_id = reservation.version_id;
                    let exact = ExactPendingObjectMetadataCommand::for_checked_request(&command);
                    require_valid_route().map_err(ObjectPgActionError::Store)?;
                    let outcome = match self
                        .finish_exact_pending_object_metadata_command_for_allocator(
                            pg_id,
                            exact,
                            &mut work_budget,
                        )
                    {
                        Ok(outcome) => outcome,
                        Err(ObjectPgActionError::Metadata(
                            MetadataError::ObjectVersionReservationConflict { version_id },
                        )) if version_id == reserved_version_id => {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            let pending =
                                self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                            if pending.as_ref() == Some(&command) {
                                self.remove_pending_metadata_command_for_bucket(
                                    pg_id, bucket, &command,
                                )
                                .map_err(ObjectPgActionError::from)?;
                            }
                            // Another helper may already have removed or replaced the abandoned
                            // allocator command. In either case the current slot, not this stale
                            // observation, determines the next version allocation attempt.
                            work_budget
                                .sleep_after_contention(
                                    "object version reservation stale cleanup retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                        Err(error) if object_version_allocator_command_contention(&error) => {
                            // A concurrent helper can abandon or replace this identity-less
                            // allocator command between observation and recovery reissue. The
                            // changed slot is the authoritative state; inspect it again instead
                            // of exposing the internal recovery race to the S3 operation.
                            work_budget
                                .sleep_after_contention(
                                    "object version reservation recovery race retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    match outcome {
                        PendingMetadataCommandOutcome::Applied
                        | PendingMetadataCommandOutcome::PublishedPendingRecovery
                        | PendingMetadataCommandOutcome::Abandoned
                        | PendingMetadataCommandOutcome::TerminalCleanupPending { .. }
                        | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            // A version reservation has no caller identity. Even when it targets
                            // the same key, it may belong to a concurrent write. A partial retry
                            // can also mean another helper displaced the allocator command while
                            // reissuing it. Reinspect the slot and allocate a fresh version instead
                            // of adopting its result or exposing internal command contention.
                            work_budget
                                .sleep_after_contention(
                                    "object version reservation pending completion retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                    }
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                work_budget
                    .sleep_after_contention(
                        "object version reservation unrelated pending retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let version_id = self.max_next_object_version_id_on_acting_set(
                self.object_metadata_pg(bucket, key),
                bucket,
                key,
                completion_admission,
            )?;
            self.maybe_run_before_object_version_command_id_hook();
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let install = match self.install_allocator_cleanup_metadata_command_with_fresh_id(
                publisher,
                pg_id,
                bucket,
                completion_admission,
                effect_fence,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReserveObjectVersion(
                            ReserveObjectVersionCommand::new(
                                bucket.clone(),
                                key.clone(),
                                version_id,
                            ),
                        ),
                    )
                },
            ) {
                Ok(install) => install,
                Err(error) if object_version_allocator_command_contention(&error) => {
                    work_budget
                        .sleep_after_contention(
                            "object version reservation install race retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let command = match install {
                AllocatorCleanupFreshInstallOutcome::Installed(command) => *command,
                AllocatorCleanupFreshInstallOutcome::PendingContenderDrained => {
                    work_budget
                        .sleep_after_contention(
                            "object version reservation pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                AllocatorCleanupFreshInstallOutcome::LogConflictHandled => {
                    work_budget
                        .sleep_after_contention(
                            "object version reservation log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            match self.apply_new_object_metadata_command_for_bucket_allocator(
                pg_id,
                bucket,
                &command,
                &mut work_budget,
            ) {
                Ok(()) => {}
                Err(ObjectPgActionError::Metadata(
                    MetadataError::ObjectVersionReservationConflict {
                        version_id: stale_version,
                    },
                )) if stale_version == version_id => {
                    work_budget
                        .sleep_after_contention(
                            "object version reservation stale version retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) if object_version_allocator_command_contention(&error) => {
                    // The command remains recoverable from the pending slot when apply loses a
                    // concurrent command-log race. Reinspect it instead of leaking that internal
                    // allocator contention through the enclosing object mutation.
                    work_budget
                        .sleep_after_contention(
                            "object version reservation apply race retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
            return Ok(version_id);
        }
    }

    fn max_next_object_version_id_on_acting_set(
        &self,
        object_pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        completion_admission: bool,
    ) -> Result<VersionId, ObjectPgActionError> {
        let mut version_id = VersionId::from_u64(1);
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), object_pg_id.pg_id())?
        {
            let object_version_client = node.object_version_metadata_client();
            let object_version_route = object_version_client.open_object_version_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
            )?;
            let candidate = if completion_admission {
                object_version_route.next_completion_object_version_id()?
            } else {
                object_version_route.next_object_version_id()?
            };
            if candidate.to_u64() > version_id.to_u64() {
                version_id = candidate;
            }
        }
        Ok(version_id)
    }

    fn finish_exact_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(
            Duration::from_millis(request_ops::METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("exact_pending_object_metadata_command_apply")
        .for_pg(pg_id);
        self.finish_exact_pending_object_metadata_command_with_work_budget(
            pg_id,
            command,
            &mut work_budget,
        )
        .map(PendingObjectMetadataCommandCompletion::into_outcome)
    }

    fn finish_exact_pending_object_metadata_command_with_work_budget(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingObjectMetadataCommandCompletion, ObjectPgActionError> {
        self.finish_object_pg_pending_slot_inner(
            pg_id,
            command.command,
            work_budget,
            self,
            ObjectPendingCommandFinishContext {
                execution_route: MetadataCommandExecutionRoute::normal(),
                recovery_guard: None,
                finish_policy: ObjectPendingCommandFinishPolicy::Standard,
                convergence_requirement:
                    request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            },
        )
    }

    fn finish_exact_pending_object_metadata_command_for_allocator(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_object_pg_pending_slot_inner(
            pg_id,
            command.command,
            work_budget,
            self,
            ObjectPendingCommandFinishContext {
                execution_route: MetadataCommandExecutionRoute::normal(),
                recovery_guard: None,
                finish_policy: ObjectPendingCommandFinishPolicy::AllocatorReinspectContention,
                convergence_requirement:
                    request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            },
        )
        .map(PendingObjectMetadataCommandCompletion::into_outcome)
    }

    #[cfg(test)]
    pub(crate) fn test_reserve_next_object_version(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version(pg_id, bucket, key)
    }

    fn apply_exact_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<(), ObjectPgActionError> {
        match self.apply_exact_pending_object_metadata_command_or_reinspect(pg_id, command)? {
            ExactPendingObjectMetadataCommandOutcome::Applied => Ok(()),
            ExactPendingObjectMetadataCommandOutcome::Reinspect => {
                Err(conflicting_pending_object_metadata_command(
                "abandoned pending object metadata command",
                ))
            }
        }
    }

    fn apply_exact_pending_object_metadata_command_or_reinspect(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<ExactPendingObjectMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(
            Duration::from_millis(request_ops::METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("exact_pending_object_metadata_command_apply")
        .for_pg(pg_id);
        self.apply_exact_pending_object_metadata_command_or_reinspect_with_work_budget(
            pg_id,
            command,
            &mut work_budget,
        )
    }

    pub(super) fn apply_exact_pending_object_metadata_command_or_reinspect_with_work_budget(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<ExactPendingObjectMetadataCommandOutcome, ObjectPgActionError> {
        let mut command = command.command.clone();
        loop {
            match self.finish_exact_pending_object_metadata_command_with_work_budget(
                pg_id,
                ExactPendingObjectMetadataCommand::for_checked_request(&command),
                work_budget,
            )? {
                PendingObjectMetadataCommandCompletion::Applied
                | PendingObjectMetadataCommandCompletion::PublishedPendingRecovery
                | PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                    applied: true,
                } => {
                    return Ok(ExactPendingObjectMetadataCommandOutcome::Applied);
                }
                PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(
                    current_command,
                ) => {
                    command = *current_command;
                    if let Err(error) = work_budget.sleep_after_contention(
                        "exact pending object metadata convergence budget exhausted",
                    ) {
                        match self.classify_pending_metadata_command_budget_exhaustion(
                            pg_id,
                            &command,
                            MetadataCommandRouteMode::Normal,
                            error,
                        )? {
                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                return Ok(ExactPendingObjectMetadataCommandOutcome::Applied);
                            }
                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                return Err(ObjectPgActionError::Store(error));
                            }
                        }
                    }
                }
                PendingObjectMetadataCommandCompletion::Abandoned
                | PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                    applied: false,
                } => {
                    return Ok(ExactPendingObjectMetadataCommandOutcome::Reinspect);
                }
            }
        }
    }

    fn drain_pending_object_metadata_command(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_publisher_drain")
            .for_pg(pg_id);
        self.drain_pending_object_metadata_command_with_work_budget(
            publisher,
            pg_id,
            command,
            &mut work_budget,
        )
        .map(|_| ())
    }

    fn drain_pending_object_metadata_command_with_work_budget(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        loop {
            #[cfg(test)]
            let force_partial_conflict =
                request_ops::maybe_force_pending_object_metadata_partial_conflict_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    command,
                );
            #[cfg(not(test))]
            let force_partial_conflict = false;
            let outcome = if force_partial_conflict {
                #[cfg(test)]
                work_budget.expire_for_test();
                PendingMetadataCommandOutcome::RetryPartialExactConflict
            } else {
                self.drain_pending_object_metadata_command_outcome_with_work_budget(
                    publisher,
                    pg_id,
                    command,
                    work_budget,
                )?
            };
            match outcome {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::PublishedPendingRecovery
                | PendingMetadataCommandOutcome::Abandoned
                | PendingMetadataCommandOutcome::TerminalCleanupPending { .. } => {
                    return Ok(outcome);
                }
                PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    // The exact command has crossed its publication boundary. A helper for a
                    // later request must keep converging that command rather than exposing its
                    // internal log race as contention on the later S3 operation.
                    if let Err(error) = work_budget.sleep_after_contention(
                        "partial pending object metadata convergence budget exhausted",
                    ) {
                        return Err(unrelated_pending_object_metadata_drain_error(match error {
                            StoreError::MetadataCommandContention { .. } => {
                                let id = command.id();
                                ObjectPgActionError::Store(
                                    StoreError::MetadataCommandIrrevocableConvergencePending {
                                        pg_id: id.pg_id().get(),
                                        cluster_epoch: id.cluster_epoch(),
                                        log_index: id.log_index().get(),
                                    },
                                )
                            }
                            error => ObjectPgActionError::Store(error),
                        }));
                    }
                }
            }
        }
    }

    fn drain_pending_object_metadata_command_outcome_with_work_budget(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.drain_pending_object_metadata_command_outcome_with_work_budget_and_policy(
            publisher,
            pg_id,
            command,
            work_budget,
            PendingMetadataCommandDrainAdmissionPolicy::UnrelatedDrainer,
        )
    }

    fn drain_pending_object_metadata_command_outcome_for_semantic_projection_with_work_budget(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.drain_pending_object_metadata_command_outcome_with_work_budget_and_policy(
            publisher,
            pg_id,
            command,
            work_budget,
            PendingMetadataCommandDrainAdmissionPolicy::UnrelatedDrainerProjectOutcome,
        )
    }

    fn drain_pending_object_metadata_command_outcome_with_work_budget_and_policy(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
        admission_policy: PendingMetadataCommandDrainAdmissionPolicy,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut authority = MetadataCommandDrainAuthority::for_publisher(publisher, work_budget);
        let context = match admission_policy {
            PendingMetadataCommandDrainAdmissionPolicy::ExactCommandOwner => {
                PendingMetadataCommandDrainContext::exact(
                    request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                )
            }
            PendingMetadataCommandDrainAdmissionPolicy::UnrelatedDrainer
            | PendingMetadataCommandDrainAdmissionPolicy::UnrelatedDrainerProjectOutcome => {
                PendingMetadataCommandDrainContext::unrelated(
                    request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                    admission_policy,
                )
            }
        };
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            context,
        )
        .map_err(unrelated_pending_object_metadata_drain_error)?;
        self.maybe_run_after_metadata_command_drain_hook();
        Ok(outcome)
    }

    #[cfg(test)]
    fn drain_pending_metadata_command_with_recovery_gate(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_test_recovery")
            .for_pg(pg_id);
        self.drain_pending_metadata_command_with_recovery_gate_and_work_budget(
            pg_id,
            command,
            &mut work_budget,
        )
    }

    #[cfg(test)]
    fn drain_pending_metadata_command_with_recovery_gate_and_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            PendingMetadataCommandDrainContext::exact(
                request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            ),
        )?;
        self.maybe_run_after_metadata_command_drain_hook();
        Ok(outcome)
    }

    fn drain_pending_metadata_command_with_recovery_authority(
        &self,
        authority: &mut MetadataCommandRecoveryDrainAuthority<'_>,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.drain_pending_metadata_command_with_recovery_authority_and_requirement(
            authority,
            pg_id,
            command,
            request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
        )
    }

    fn drain_pending_metadata_command_with_recovery_authority_and_requirement(
        &self,
        authority: &mut MetadataCommandRecoveryDrainAuthority<'_>,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut drain_authority = MetadataCommandDrainAuthority::for_recovery(authority);
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut drain_authority,
            self,
            PendingMetadataCommandDrainContext::exact(convergence_requirement),
        )?;
        self.maybe_run_after_metadata_command_drain_hook();
        Ok(outcome)
    }

    fn drain_pending_metadata_command_with_local_recovery_route(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_local_recovery")
            .for_pg(pg_id);
        let mut recovery_authority =
            MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            PendingMetadataCommandDrainContext::recovery(
                None,
                request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            ),
        )
    }

    #[cfg(test)]
    fn drain_pending_metadata_command_with_authorized_recovery_route(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_authorized_recovery")
            .for_pg(pg_id);
        self.drain_pending_metadata_command_with_authorized_recovery_source_and_work_budget(
            pg_id,
            command,
            command,
            reservation_authority,
            &mut work_budget,
        )
    }

    fn drain_pending_metadata_command_with_authorized_recovery_source(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        authorized_source: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_authorized_recovery")
            .for_pg(pg_id);
        self.drain_pending_metadata_command_with_authorized_recovery_source_and_work_budget(
            pg_id,
            command,
            authorized_source,
            reservation_authority,
            &mut work_budget,
        )
    }

    fn drain_pending_metadata_command_with_authorized_recovery_source_and_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        authorized_source: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            reservation_authority,
            PendingMetadataCommandDrainContext::recovery(
                Some(authorized_source),
                request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            ),
        )
    }

    fn drain_pending_metadata_command_with_authority_inner(
        &self,
        pg_id: PgId,
        initial_command: &MetadataCommandEnvelope,
        authority: &mut MetadataCommandDrainAuthority<'_>,
        reservation_authority: &StorageCluster,
        context: PendingMetadataCommandDrainContext<'_>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let route_mode = context.route_mode;
        let convergence_requirement = context.convergence_requirement;
        let admission_policy = context.admission_policy;
        let recovery_authorized_source = context.recovery_authorized_source.cloned();
        let mut command = initial_command.clone();
        loop {
            let runtime_state = self.local_map.runtime_state();
            let recovery = match (route_mode, recovery_authorized_source.as_ref()) {
                (MetadataCommandRouteMode::Recovery, Some(authorized_source)) => runtime_state
                    .join_authorized_metadata_command_recovery_until(
                        pg_id,
                        &command,
                        authorized_source,
                        authority.work_budget().deadline(),
                    ),
                (MetadataCommandRouteMode::Normal, _)
                    if admission_policy.requests_authorized_recovery_handoff() =>
                {
                    runtime_state.join_metadata_command_recovery_as_unrelated_drainer_until(
                        pg_id,
                        &command,
                        authority.work_budget().deadline(),
                    )
                }
                _ => runtime_state.join_metadata_command_recovery_until(
                    pg_id,
                    &command,
                    authority.work_budget().deadline(),
                ),
            };
            let recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(
                        pg_id,
                        &command,
                        "drain_awaiting_authorized_recovery",
                    );
                    if admission_policy.projects_retained_outcome() {
                        if let Some(MetadataCommandRecoveryResolution::Outcome(outcome)) =
                            resolution
                        {
                            return observed_pending_metadata_command_outcome_for_drain_policy(
                                outcome,
                                admission_policy,
                            );
                        }
                    }
                    return Err(
                        ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery,
                    );
                }
                MetadataCommandRecoveryAdmission::Waited {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_wait");
                    if let Some(resolution) = resolution {
                        if let MetadataCommandRecoveryResolution::Outcome(outcome) = resolution {
                            return observed_pending_metadata_command_outcome_for_drain_policy(
                                outcome,
                                admission_policy,
                            );
                        }
                        return Err(metadata_command_recovery_resolution_error(
                            &command,
                            resolution,
                        )
                        .expect("irreversible recovery resolution must produce an error"));
                    }
                    if let Err(error) = authority
                        .work_budget()
                        .check("pending command recovery wait budget exhausted")
                    {
                        match self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
                            pg_id,
                            &command,
                            route_mode,
                            convergence_requirement,
                            error,
                        )? {
                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                return observed_pending_metadata_command_outcome_for_drain_policy(
                                    PendingMetadataCommandOutcome::PublishedPendingRecovery,
                                    admission_policy,
                                );
                            }
                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                return Err(ObjectPgActionError::Store(error));
                            }
                        }
                    }
                    let waiter_outcome = loop {
                        match self.pending_command_recovery_waiter_outcome_with_route_mode_until(
                            pg_id,
                            &command,
                            route_mode,
                            authority.work_budget().deadline(),
                        ) {
                            Ok(outcome) => break outcome,
                            Err(waiter_error) => {
                                if let Err(error) = authority
                                    .work_budget()
                                    .check("pending command recovery wait budget exhausted")
                                {
                                    match self
                                        .classify_pending_metadata_command_budget_exhaustion_with_requirement(
                                            pg_id,
                                            &command,
                                            route_mode,
                                            convergence_requirement,
                                            error,
                                        )?
                                    {
                                        request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                            return observed_pending_metadata_command_outcome_for_drain_policy(
                                                PendingMetadataCommandOutcome::PublishedPendingRecovery,
                                                admission_policy,
                                            );
                                        }
                                        request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                            return Err(ObjectPgActionError::Store(error));
                                        }
                                    }
                                }
                                if request_ops::object_pg_action_error_is_retryable_command_observation(
                                    &waiter_error,
                                ) {
                                    if let Err(error) = authority
                                        .work_budget()
                                        .sleep_after_contention(
                                            "pending command recovery waiter observation budget exhausted",
                                        )
                                    {
                                        match self
                                            .classify_pending_metadata_command_budget_exhaustion_with_requirement(
                                                pg_id,
                                                &command,
                                                route_mode,
                                                convergence_requirement,
                                                error,
                                            )?
                                        {
                                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                                return observed_pending_metadata_command_outcome_for_drain_policy(
                                                    PendingMetadataCommandOutcome::PublishedPendingRecovery,
                                                    admission_policy,
                                                );
                                            }
                                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                                return Err(ObjectPgActionError::Store(error));
                                            }
                                        }
                                    }
                                    continue;
                                }
                                return Err(waiter_error);
                            }
                        }
                    };
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        waiter_outcome.metric_label(),
                    );
                    match waiter_outcome {
                        MetadataCommandRecoveryWaiterOutcome::StillPending => continue,
                        MetadataCommandRecoveryWaiterOutcome::Applied => {
                            return Ok(PendingMetadataCommandOutcome::Applied);
                        }
                        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
                        | MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
                            return Ok(PendingMetadataCommandOutcome::Abandoned);
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::TimedOut,
                        wait_us,
                    );
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        "timed_out",
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_timeout");
                    #[cfg(test)]
                    request_ops::maybe_run_pending_command_recovery_timeout_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        &command,
                        authority.work_budget(),
                    );
                    if let Some(resolution) = resolution {
                        if let MetadataCommandRecoveryResolution::Outcome(outcome) = resolution {
                            return observed_pending_metadata_command_outcome_for_drain_policy(
                                outcome,
                                admission_policy,
                            );
                        }
                        let error = metadata_command_recovery_resolution_error(
                            &command,
                            resolution,
                        )
                        .expect("irreversible recovery resolution must produce an error");
                        if authority
                            .work_budget()
                            .sleep_after_contention(
                                "authorized metadata command recovery handoff budget exhausted",
                            )
                            .is_ok()
                        {
                            continue;
                        }
                        return Err(error);
                    }
                    if let Err(error) = authority
                        .work_budget()
                        .sleep_after_contention("pending command recovery retry budget exhausted")
                    {
                        match self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
                            pg_id,
                            &command,
                            route_mode,
                            convergence_requirement,
                            error,
                        )? {
                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                return observed_pending_metadata_command_outcome_for_drain_policy(
                                    PendingMetadataCommandOutcome::PublishedPendingRecovery,
                                    admission_policy,
                                );
                            }
                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                return Err(ObjectPgActionError::Store(error));
                            }
                        }
                    }
                    continue;
                }
            };
            self.emit_pending_slot_action_for_command(pg_id, &command, "drain_attempt");
            #[cfg(test)]
            request_ops::maybe_run_pending_object_metadata_command_drain_attempt_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                &command,
                authority.work_budget(),
            )?;
            let mut leader = authority
                .admit_leader(recovery_guard, pg_id, &command)
                .map_err(ObjectPgActionError::Store)?;
            let finish_context = match route_mode {
                MetadataCommandRouteMode::Normal => PendingMetadataCommandDrainContext {
                    route_mode,
                    recovery_authorized_source: None,
                    convergence_requirement,
                    admission_policy,
                },
                MetadataCommandRouteMode::Recovery => PendingMetadataCommandDrainContext::recovery(
                    recovery_authorized_source.as_ref(),
                    convergence_requirement,
                ),
            };
            let outcome = self.finish_pending_metadata_command_with_recovery_leader(
                pg_id,
                &command,
                &mut leader,
                reservation_authority,
                finish_context,
            );
            match outcome {
                Ok(outcome) => {
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        outcome.metric_label(),
                    );
                    let relinquished =
                        leader.complete_with_outcome_and_relinquish_if_requested(outcome);
                    if outcome.retains_pending_slot()
                        && admission_policy.requests_authorized_recovery_handoff()
                    {
                        debug_assert!(
                            relinquished,
                            "unrelated nonterminal object command must retain its recovery flight"
                        );
                    }
                    return relinquished_pending_metadata_command_outcome_for_drain_policy(
                        outcome,
                        admission_policy,
                    );
                }
                Err(error)
                    if matches!(route_mode, MetadataCommandRouteMode::Normal)
                        && metadata_command_irreversible_resolution(&error).is_some() =>
                {
                    // A publisher draining someone else's command cannot claim its exact
                    // outcome. Preserve the flight for authority-backed recovery and let the
                    // publisher boundary expose ordinary retryable contention.
                    leader.mark_irreversible_handoff(
                        metadata_command_irreversible_resolution(&error)
                            .expect("guard requires typed irreversible uncertainty"),
                    );
                    leader.relinquish_for_authorized_recovery();
                    return Err(ObjectPgActionError::MetadataCommandRecoveryTransferred);
                }
                Err(error) if leader.lineage_advanced_from(&command) => {
                    leader.relinquish_for_authorized_recovery();
                    return Err(error);
                }
                Err(error)
                    if matches!(route_mode, MetadataCommandRouteMode::Normal)
                        && request_ops::object_pg_action_error_requires_metadata_command_recovery_route(
                            &error,
                        ) =>
                {
                    leader.relinquish_for_authorized_recovery();
                    return Err(ObjectPgActionError::MetadataCommandRecoveryTransferred);
                }
                Err(error)
                    if matches!(route_mode, MetadataCommandRouteMode::Recovery)
                        && recovery_authorized_source.is_some()
                        && (request_ops::object_pg_action_error_is_retryable_command_observation(
                            &error,
                        ) || metadata_command_irreversible_resolution(&error).is_some()) =>
                {
                    leader.relinquish_for_authorized_recovery();
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn classify_pending_metadata_command_budget_exhaustion(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        budget_error: StoreError,
    ) -> Result<request_ops::MetadataCommandBudgetExhaustionOutcome, ObjectPgActionError> {
        self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
            pg_id,
            command,
            route_mode,
            request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
            budget_error,
        )
    }

    fn classify_pending_metadata_command_budget_exhaustion_with_requirement(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        convergence_requirement: request_ops::MetadataCommandConvergenceRequirement,
        budget_error: StoreError,
    ) -> Result<request_ops::MetadataCommandBudgetExhaustionOutcome, ObjectPgActionError> {
        self.classify_metadata_command_budget_exhaustion_with_route_mode(
            pg_id,
            command,
            route_mode,
            convergence_requirement,
            budget_error,
        )
        .map_err(bucket_snapshot_error_to_object_pg_action_error)
    }

    fn finish_direct_put_after_pending_command_uncertainty(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        fallback_error: ObjectPgActionError,
    ) -> Result<request_ops::NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.finish_new_object_metadata_command_after_uncertainty(
            pg_id,
            bucket,
            command,
            fallback_error,
        )
    }

    fn finish_direct_put_after_pending_install_uncertainty(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &mut MetadataCommandEnvelope,
        fallback_error: ObjectPgActionError,
    ) -> Result<
        (
            request_ops::NewObjectMetadataCommandApplyOutcome,
            bool,
        ),
        ObjectPgActionError,
    > {
        let confirmation_deadline =
            Instant::now() + request_ops::METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
        let mut observation_retries = 0;
        let publication = loop {
            let publication = match self.metadata_command_publication_state_on_acting_set_until(
                pg_id,
                command,
                MetadataCommandRouteMode::Normal,
                confirmation_deadline,
            ) {
                Ok(publication) => publication,
                Err(error) => {
                    return Err(Self::direct_put_pending_install_confirmation_error(
                        command,
                        bucket_snapshot_error_to_object_pg_action_error(error),
                    ));
                }
            };
            if publication != MetadataCommandPublicationState::IrrevocableUnconfirmed {
                break publication;
            }
            if !sleep_after_metadata_contention_retry_until(
                "confirm_direct_put_pending_install",
                Some(pg_id),
                "direct PUT pending install publication observation blocked",
                &mut observation_retries,
                confirmation_deadline,
            ) {
                let id = command.id();
                return Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandOutcomeUnconfirmed {
                        pg_id: id.pg_id().get(),
                        cluster_epoch: id.cluster_epoch(),
                        log_index: id.log_index().get(),
                    },
                ));
            }
        };
        match publication {
            MetadataCommandPublicationState::Published => Ok((
                request_ops::NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery,
                true,
            )),
            MetadataCommandPublicationState::PublicationStarted
            | MetadataCommandPublicationState::Witnessed
            | MetadataCommandPublicationState::PublicationUnconfirmed => {
                let id = command.id();
                Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandIrrevocableConvergencePending {
                        pg_id: id.pg_id().get(),
                        cluster_epoch: id.cluster_epoch(),
                        log_index: id.log_index().get(),
                    },
                ))
            }
            MetadataCommandPublicationState::IrrevocableUnconfirmed => {
                let id = command.id();
                Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandOutcomeUnconfirmed {
                        pg_id: id.pg_id().get(),
                        cluster_epoch: id.cluster_epoch(),
                        log_index: id.log_index().get(),
                    },
                ))
            }
            MetadataCommandPublicationState::NotPublished => {
                // Re-read the slot after publication inspection. The inspection
                // serializes with pending installation on the primary; an insert
                // that was already in flight must therefore be visible here.
                let pending = self
                    .pending_metadata_command_for_bucket_with_route_mode_until(
                        pg_id,
                        bucket,
                        MetadataCommandRouteMode::Normal,
                        command.id().cluster_epoch(),
                        confirmation_deadline,
                    )
                    .map_err(|error| {
                        Self::direct_put_pending_install_confirmation_error(
                            command,
                            ObjectPgActionError::Store(error),
                        )
                    })?;
                if pending.as_ref() == Some(&*command) {
                    let outcome = self
                        .finish_new_object_metadata_command_after_uncertainty_until(
                            pg_id,
                            bucket,
                            command,
                            fallback_error,
                            confirmation_deadline,
                        )
                        .map_err(|error| {
                            Self::direct_put_pending_install_confirmation_error(command, error)
                        })?;
                    return Ok((outcome, true));
                }
                let Some(current) = pending else {
                    return Ok((
                        request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(
                            fallback_error,
                        ),
                        false,
                    ));
                };
                let certified_reissue = self
                    .pending_metadata_command_is_certified_reissue_until(
                        pg_id,
                        command,
                        &current,
                        confirmation_deadline,
                    )
                    .map_err(|error| {
                        Self::direct_put_pending_install_confirmation_error(
                            command,
                            bucket_snapshot_error_to_object_pg_action_error(error),
                        )
                    })?;
                if !certified_reissue {
                    return Ok((
                        request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(
                            fallback_error,
                        ),
                        false,
                    ));
                }
                *command = current;
                let outcome = self
                    .finish_new_object_metadata_command_after_uncertainty_until(
                        pg_id,
                        bucket,
                        command,
                        fallback_error,
                        confirmation_deadline,
                    )
                    .map_err(|error| {
                        Self::direct_put_pending_install_confirmation_error(command, error)
                    })?;
                Ok((outcome, true))
            }
        }
    }

    fn direct_put_pending_install_confirmation_error(
        command: &MetadataCommandEnvelope,
        error: ObjectPgActionError,
    ) -> ObjectPgActionError {
        if !request_ops::object_pg_action_error_is_retryable_command_observation(&error) {
            return error;
        }
        let id = command.id();
        ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed {
            pg_id: id.pg_id().get(),
            cluster_epoch: id.cluster_epoch(),
            log_index: id.log_index().get(),
        })
    }

    fn finish_pending_metadata_command_with_recovery_leader(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        leader: &mut MetadataCommandRecoveryLeader<'_>,
        reservation_authority: &StorageCluster,
        context: PendingMetadataCommandDrainContext<'_>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let PendingMetadataCommandDrainContext {
            route_mode,
            recovery_authorized_source,
            convergence_requirement,
            admission_policy: _,
        } = context;
        let abandoned_predecessor = leader.abandoned_predecessor();
        let root_is_abandoned = leader.root_is_abandoned();
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = match route_mode {
                    MetadataCommandRouteMode::Normal => self
                        .finish_pending_metadata_command_to_acting_set_under_recovery_leader_with_work_budget(
                            pg_id,
                            command,
                            false,
                            leader,
                            convergence_requirement,
                        ),
                    MetadataCommandRouteMode::Recovery => {
                        self.finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
                            pg_id,
                            command,
                            false,
                            recovery_authorized_source,
                            leader,
                            convergence_requirement,
                        )
                    }
                }
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
            Ok(match outcome {
                request_ops::FinishPendingMetadataCommandResult::Applied => {
                    PendingMetadataCommandOutcome::Applied
                }
                request_ops::FinishPendingMetadataCommandResult::PublishedPendingRecovery => {
                    PendingMetadataCommandOutcome::PublishedPendingRecovery
                }
                request_ops::FinishPendingMetadataCommandResult::Abandoned => {
                    PendingMetadataCommandOutcome::Abandoned
                }
                request_ops::FinishPendingMetadataCommandResult::TerminalCleanupPending {
                    applied,
                } => PendingMetadataCommandOutcome::TerminalCleanupPending { applied },
                request_ops::FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    PendingMetadataCommandOutcome::RetryPartialExactConflict
                }
            })
        } else {
            let (work_budget, proof, recovery_guard) = leader.parts_with_guard();
            let completion = self.finish_object_pg_pending_slot_inner(
                pg_id,
                command,
                work_budget,
                reservation_authority,
                ObjectPendingCommandFinishContext {
                    execution_route: match route_mode {
                        MetadataCommandRouteMode::Normal => {
                            MetadataCommandExecutionRoute::normal()
                        }
                        MetadataCommandRouteMode::Recovery => {
                            MetadataCommandExecutionRoute::recovery(
                                proof,
                                recovery_authorized_source,
                                abandoned_predecessor.as_ref(),
                            )
                        }
                    },
                    recovery_guard: Some(recovery_guard),
                    finish_policy:
                        ObjectPendingCommandFinishPolicy::AbandonZeroApplyStaleReservation,
                    convergence_requirement,
                },
            )?;
            if root_is_abandoned {
                let predecessor = abandoned_predecessor.as_ref().ok_or({
                    ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "metadata-command-recovery-abandoned-root-context",
                    })
                })?;
                return match completion {
                    PendingObjectMetadataCommandCompletion::Applied => {
                        self.after_object_metadata_command_abandoned_payload_cleanup(predecessor);
                        Ok(PendingMetadataCommandOutcome::Abandoned)
                    }
                    PendingObjectMetadataCommandCompletion::PublishedPendingRecovery
                    | PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                        applied: true,
                    } => {
                        self.after_object_metadata_command_abandoned_payload_cleanup(predecessor);
                        Ok(PendingMetadataCommandOutcome::TerminalCleanupPending {
                            applied: false,
                        })
                    }
                    PendingObjectMetadataCommandCompletion::Abandoned
                    | PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                        applied: false,
                    }
                    | PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(_) => {
                        Err(conflicting_pending_object_metadata_command(
                            "certified abandoned-command cleanup did not apply",
                        ))
                    }
                };
            }
            Ok(completion.into_outcome())
        }
    }

    #[cfg(test)]
    fn pending_command_recovery_waiter_outcome(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandRecoveryWaiterOutcome, ObjectPgActionError> {
        self.pending_command_recovery_waiter_outcome_with_route_mode(
            pg_id,
            command,
            MetadataCommandRouteMode::Normal,
        )
    }

    #[cfg(test)]
    fn pending_command_recovery_waiter_outcome_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<MetadataCommandRecoveryWaiterOutcome, ObjectPgActionError> {
        let deadline = Instant::now()
            .checked_add(request_ops::METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET)
            .unwrap_or_else(Instant::now);
        self.pending_command_recovery_waiter_outcome_with_route_mode_until(
            pg_id, command, route_mode, deadline,
        )
    }

    fn pending_command_recovery_waiter_outcome_with_route_mode_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        deadline: Instant,
    ) -> Result<MetadataCommandRecoveryWaiterOutcome, ObjectPgActionError> {
        let pending = self.pending_metadata_command_for_bucket_with_route_mode_until(
            pg_id,
            command.bucket_name(),
            route_mode,
            command.id().cluster_epoch(),
            deadline,
        )?;
        if pending.as_ref() == Some(command) {
            return Ok(MetadataCommandRecoveryWaiterOutcome::StillPending);
        }
        if self
            .metadata_command_is_applied_on_all_acting_nodes_with_route_mode_and_deadline(
                pg_id,
                command,
                route_mode,
                Some(deadline),
            )
            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
        {
            return Ok(MetadataCommandRecoveryWaiterOutcome::Applied);
        }
        if pending.is_some() {
            Ok(MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied)
        } else {
            Ok(MetadataCommandRecoveryWaiterOutcome::MissingNotApplied)
        }
    }

    fn metadata_command_recovery_applied_collectable_object_command(
        command: &MetadataCommandEnvelope,
        outcome: PendingMetadataCommandOutcome,
    ) -> bool {
        outcome.is_logically_applied() && !Self::metadata_command_is_bucket_pg_command(command)
    }

    fn finish_object_pg_pending_slot_requiring_convergence_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_object_pg_pending_slot_inner(
            pg_id,
            command,
            work_budget,
            self,
            ObjectPendingCommandFinishContext {
                execution_route: MetadataCommandExecutionRoute::normal(),
                recovery_guard: None,
                finish_policy: ObjectPendingCommandFinishPolicy::Standard,
                convergence_requirement:
                    request_ops::MetadataCommandConvergenceRequirement::RequireAllReplicas,
            },
        )
        .map(PendingObjectMetadataCommandCompletion::into_outcome)
    }

    fn finish_object_pg_pending_slot_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
        reservation_authority: &StorageCluster,
        finish_context: ObjectPendingCommandFinishContext<'_>,
    ) -> Result<PendingObjectMetadataCommandCompletion, ObjectPgActionError> {
        let ObjectPendingCommandFinishContext {
            mut execution_route,
            recovery_guard,
            finish_policy,
            convergence_requirement,
        } = finish_context;
        execution_route
            .require_command(pg_id, command)
            .map_err(ObjectPgActionError::Store)?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source.cloned();
        let recovery_abandoned_source = execution_route.recovery_abandoned_source.cloned();
        let mut command = command.clone();
        let mut apply_progress = request_ops::MetadataCommandApplyProgress::Abortable;
        loop {
            if let Err(error) =
                work_budget.check("object metadata pending command apply budget exhausted")
            {
                match self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
                    pg_id,
                    &command,
                    route_mode,
                    convergence_requirement,
                    error,
                )? {
                    request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                        return Ok(
                            PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                        );
                    }
                    request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                        return Err(ObjectPgActionError::Store(error));
                    }
                }
            }
            let command_bucket = command.bucket_name();
            let abandoned_on_acting_set = if apply_progress.is_abortable() {
                match route_mode {
                    MetadataCommandRouteMode::Normal => {
                        self.metadata_command_has_abandoned_log_on_acting_set_until(
                            &command,
                            work_budget.deadline(),
                        )
                    }
                    MetadataCommandRouteMode::Recovery => self
                        .metadata_command_has_abandoned_log_on_acting_set_for_recovery_until(
                            execution_route.recovery_proof(),
                            &command,
                            recovery_authorized_source.as_ref(),
                            recovery_abandoned_source.as_ref(),
                            work_budget.deadline(),
                        ),
                }
            } else {
                Ok(false)
            };
            let abandoned_on_acting_set = match self
                .resolve_metadata_command_abandonment_observation(
                    pg_id,
                    &command,
                    route_mode,
                    convergence_requirement,
                    abandoned_on_acting_set,
                    work_budget,
                )
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?
            {
                request_ops::MetadataCommandAbandonmentObservation::Observed(abandoned) => {
                    abandoned
                }
                request_ops::MetadataCommandAbandonmentObservation::Retry => continue,
                request_ops::MetadataCommandAbandonmentObservation::PublishedPendingRecovery => {
                    return Ok(
                        PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                    );
                }
            };
            if abandoned_on_acting_set {
                let record_result = match route_mode {
                    MetadataCommandRouteMode::Normal => self
                        .record_abandoned_metadata_command_to_acting_set_until(
                            &command,
                            work_budget.deadline(),
                        ),
                    MetadataCommandRouteMode::Recovery => self
                        .record_abandoned_metadata_command_to_acting_set_for_recovery_until(
                            execution_route.recovery_proof(),
                            &command,
                            recovery_authorized_source.as_ref(),
                            recovery_abandoned_source.as_ref(),
                            work_budget.deadline(),
                        ),
                };
                record_result.map_err(|error| {
                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                })?;
                let cleanup = self.complete_abandoned_object_metadata_command(
                    pg_id,
                    &command,
                    reservation_authority,
                    work_budget,
                    ObjectPendingCommandCleanupContext {
                        execution_route,
                        recovery_guard,
                        convergence_requirement,
                    },
                )?;
                return Ok(if cleanup == request_ops::PendingMetadataCommandTerminalCleanup::Deferred {
                    PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                        applied: false,
                    }
                } else {
                    PendingObjectMetadataCommandCompletion::Abandoned
                });
            }
            let apply_result = self.apply_metadata_command_to_acting_set_with_route_mode_until(
                &command,
                execution_route,
                reservation_authority,
                apply_progress,
                work_budget.deadline(),
            );
            if let Err(error) = &apply_result {
                apply_progress = apply_progress.merge(error.progress);
            }
            match apply_result {
                Ok(outcome) => {
                    if outcome == request_ops::MetadataCommandApplyOutcome::PublishedPendingRecovery
                        && convergence_requirement
                            == request_ops::MetadataCommandConvergenceRequirement::RequireAllReplicas
                    {
                        let id = command.id();
                        return Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandDependencyConvergencePending {
                                pg_id: id.pg_id().get(),
                                cluster_epoch: id.cluster_epoch(),
                                log_index: id.log_index().get(),
                            },
                        ));
                    }
                    if outcome == request_ops::MetadataCommandApplyOutcome::Converged {
                        if let Some(recovery_guard) = recovery_guard {
                            recovery_guard.record_outcome(
                                PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        if !reservation_authority
                            .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                pg_id, &command,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        {
                            self.after_object_metadata_command_applied(&command);
                            return Ok(
                                PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        let cleanup = match route_mode {
                                MetadataCommandRouteMode::Normal => self
                                    .remove_pending_metadata_command_for_bucket(
                                        pg_id,
                                        command_bucket,
                                        &command,
                                    ),
                                MetadataCommandRouteMode::Recovery => self
                                    .remove_pending_metadata_command_for_bucket_recovery(
                                        execution_route,
                                        pg_id,
                                        command_bucket,
                                        &command,
                                        work_budget,
                                    ),
                        }
                        .map_err(ObjectPgActionError::from)?;
                        if cleanup == request_ops::PendingMetadataCommandTerminalCleanup::Deferred {
                            self.after_object_metadata_command_applied(&command);
                            return Ok(
                                PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        self.after_object_metadata_command_applied(&command);
                        return Ok(PendingObjectMetadataCommandCompletion::Applied);
                    }
                    return Ok(
                        PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                    );
                }
                Err(error)
                    if error.progress.is_abortable()
                        && finish_policy.returns_metadata_command_contention()
                        && request_ops::metadata_command_apply_error_is_contention(
                            &error.source,
                        ) =>
                {
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
                Err(error)
                    if error.progress.is_abortable()
                        && (request_ops::metadata_command_apply_transport_error_is_retryable(
                            &error.source,
                        ) || request_ops::metadata_command_apply_error_is_contention(
                            &error.source,
                        )) =>
                {
                    if let Err(error) = work_budget.sleep_after_contention(
                        "pending metadata command transport retry budget exhausted",
                    ) {
                        match self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
                            pg_id,
                            &command,
                            route_mode,
                            convergence_requirement,
                            error,
                        )? {
                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                return Ok(
                                    PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                                );
                            }
                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                return Err(ObjectPgActionError::Store(error));
                            }
                        }
                    }
                    continue;
                }
                Err(error)
                    if error.progress.is_abortable()
                        && Self::metadata_command_log_conflict_matches(&command, &error.source)
                        && self
                            .metadata_command_log_conflict_actor_has_exact_abandonment(
                                pg_id,
                                &command,
                                &error.source,
                                route_mode,
                                work_budget.deadline(),
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)? =>
                {
                    let cleanup = self.propagate_and_complete_abandoned_object_metadata_command(
                        pg_id,
                        &command,
                        reservation_authority,
                        work_budget,
                        ObjectPendingCommandCleanupContext {
                            execution_route,
                            recovery_guard,
                            convergence_requirement,
                        },
                    )?;
                    return Ok(if cleanup == request_ops::PendingMetadataCommandTerminalCleanup::Deferred {
                        PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                            applied: false,
                        }
                    } else {
                        PendingObjectMetadataCommandCompletion::Abandoned
                    });
                }
                Err(error)
                    if Self::metadata_command_log_conflict_matches(&command, &error.source)
                        && (self
                            .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                                pg_id, &command, route_mode,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                            || self
                                .metadata_command_log_conflict_actor_has_exact_entry(
                                    pg_id,
                                    &command,
                                    &error.source,
                                    route_mode,
                                    Some(work_budget.deadline()),
                                )
                                .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                            || (!error.progress.is_abortable()
                                && self
                                    .partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                                route_mode,
                                    )
                                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?)) =>
                {
                    if self
                        .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                            pg_id, &command, route_mode,
                        )
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        if let Some(recovery_guard) = recovery_guard {
                            recovery_guard.record_outcome(
                                PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        if !reservation_authority
                            .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                pg_id, &command,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        {
                            self.after_object_metadata_command_applied(&command);
                            return Ok(
                                PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        let cleanup = match route_mode {
                                MetadataCommandRouteMode::Normal => self
                                    .remove_pending_metadata_command_for_bucket(
                                        pg_id,
                                        command_bucket,
                                        &command,
                                    ),
                                MetadataCommandRouteMode::Recovery => self
                                    .remove_pending_metadata_command_for_bucket_recovery(
                                        execution_route,
                                        pg_id,
                                        command_bucket,
                                        &command,
                                        work_budget,
                                    ),
                        }
                        .map_err(ObjectPgActionError::from)?;
                        if cleanup == request_ops::PendingMetadataCommandTerminalCleanup::Deferred {
                            self.after_object_metadata_command_applied(&command);
                            return Ok(
                                PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        self.after_object_metadata_command_applied(&command);
                        return Ok(PendingObjectMetadataCommandCompletion::Applied);
                    }
                    apply_progress = apply_progress
                        .merge(request_ops::MetadataCommandApplyProgress::PublicationUnconfirmed);
                    #[cfg(test)]
                    if request_ops::maybe_force_pending_object_metadata_partial_conflict_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        &command,
                    ) {
                        work_budget.expire_for_test();
                    }
                    if let Err(error) = work_budget.sleep_after_contention(
                        "partial pending object metadata convergence budget exhausted",
                    ) {
                        match self.classify_pending_metadata_command_budget_exhaustion_with_requirement(
                            pg_id,
                            &command,
                            route_mode,
                            convergence_requirement,
                            error,
                        )? {
                            request_ops::MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                return Ok(
                                    PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                                );
                            }
                            request_ops::MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                return Err(ObjectPgActionError::Store(error));
                            }
                        }
                    }
                    continue;
                }
                Err(error)
                    if error.progress.is_abortable()
                        && error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let reissued = match self
                        .reissue_pending_metadata_command_outcome_with_route_mode_until(
                            pg_id,
                            &command,
                            execution_route,
                            command.payload(),
                            recovery_guard,
                            work_budget.deadline(),
                        ) {
                        Ok(ReissuePendingMetadataCommandOutcome::Reissued(reissued)
                        | ReissuePendingMetadataCommandOutcome::MatchingCurrent(reissued)) => {
                            reissued
                        }
                        Ok(ReissuePendingMetadataCommandOutcome::Missing) => {
                            return Ok(PendingObjectMetadataCommandCompletion::Abandoned);
                        }
                        Ok(ReissuePendingMetadataCommandOutcome::MatchingCurrentConflict {
                            command: current,
                            ..
                        }) => {
                            if finish_policy.abandons_zero_apply_stale_reservation() {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending object metadata drain",
                                ));
                            }
                            #[cfg(test)]
                            if request_ops::maybe_force_pending_object_metadata_partial_conflict_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                                &current,
                            ) {
                                work_budget.expire_for_test();
                            }
                            return Ok(
                                PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(
                                    Box::new(current),
                                ),
                            );
                        }
                        Err(BucketSnapshotLoadError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            if finish_policy.abandons_zero_apply_stale_reservation() {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending object metadata drain",
                                ));
                            }
                            return Ok(
                                PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(
                                    Box::new(command),
                                ),
                            );
                        }
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                        }
                    };
                    execution_route = execution_route
                        .for_reissued_command(pg_id, &command, &reissued)
                        .map_err(ObjectPgActionError::Store)?;
                    command = reissued;
                    apply_progress = request_ops::MetadataCommandApplyProgress::Abortable;
                    #[cfg(test)]
                    if request_ops::maybe_force_pending_object_metadata_partial_conflict_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        &command,
                    ) {
                        work_budget.expire_for_test();
                    }
                }
                Err(error)
                    if finish_policy.abandons_zero_apply_stale_reservation()
                        && error.progress.is_abortable()
                        && error.applied_nodes == 0
                        && (Self::reserve_object_generation_conflict_matches(
                            &command,
                            &error.source,
                        ) || Self::reserve_object_version_conflict_matches(
                            &command,
                            &error.source,
                        ) || Self::bucket_write_reservation_rejection_matches(
                            &command,
                            &error.source,
                        )) =>
                {
                    match route_mode {
                        MetadataCommandRouteMode::Normal => self
                            .record_abandoned_metadata_command_to_acting_set_until(
                                &command,
                                work_budget.deadline(),
                            ),
                        MetadataCommandRouteMode::Recovery => self
                            .record_abandoned_metadata_command_to_acting_set_for_recovery_until(
                                execution_route.recovery_proof(),
                                &command,
                                recovery_authorized_source.as_ref(),
                                recovery_abandoned_source.as_ref(),
                                work_budget.deadline(),
                            ),
                    }
                    .map_err(|error| {
                        bucket_snapshot_error_to_object_pg_action_error(error.source)
                    })?;
                    let cleanup = self.complete_abandoned_object_metadata_command(
                        pg_id,
                        &command,
                        reservation_authority,
                        work_budget,
                        ObjectPendingCommandCleanupContext {
                            execution_route,
                            recovery_guard,
                            convergence_requirement,
                        },
                    )?;
                    return Ok(if cleanup == request_ops::PendingMetadataCommandTerminalCleanup::Deferred {
                        PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                            applied: false,
                        }
                    } else {
                        PendingObjectMetadataCommandCompletion::Abandoned
                    });
                }
                Err(error) => {
                    #[cfg(test)]
                    let error = {
                        let mut error = error;
                        maybe_run_terminal_stream_apply_failure_test_hook(
                            self.metadata_command_apply_test_hook_scope_id(),
                            &command,
                            &mut error,
                        );
                        error
                    };
                    if metadata_command_apply_failure_is_definitive_terminal_stream_outcome(
                        &command, &error,
                    ) {
                        let cleanup =
                            self.propagate_and_complete_abandoned_object_metadata_command(
                                pg_id,
                                &command,
                                reservation_authority,
                                work_budget,
                                ObjectPendingCommandCleanupContext {
                                    execution_route,
                                    recovery_guard,
                                    convergence_requirement,
                                },
                            )?;
                        return Ok(if cleanup
                            == request_ops::PendingMetadataCommandTerminalCleanup::Deferred
                        {
                            PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                applied: false,
                            }
                        } else {
                            PendingObjectMetadataCommandCompletion::Abandoned
                        });
                    }
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
            }
        }
    }

    fn propagate_and_complete_abandoned_object_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
        work_budget: &mut RequestWorkBudget,
        cleanup_context: ObjectPendingCommandCleanupContext<'_>,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, ObjectPgActionError> {
        let execution_route = cleanup_context.execution_route;
        let record_result = match execution_route.mode {
            MetadataCommandRouteMode::Normal => self
                .record_abandoned_metadata_command_to_acting_set_until(
                    command,
                    work_budget.deadline(),
                ),
            MetadataCommandRouteMode::Recovery => self
                .record_abandoned_metadata_command_to_acting_set_for_recovery_until(
                    execution_route.recovery_proof(),
                    command,
                    execution_route.recovery_authorized_source,
                    execution_route.recovery_abandoned_source,
                    work_budget.deadline(),
                ),
        };
        record_result
            .map_err(|error| bucket_snapshot_error_to_object_pg_action_error(error.source))?;
        self.complete_abandoned_object_metadata_command(
            pg_id,
            command,
            reservation_authority,
            work_budget,
            cleanup_context,
        )
    }

    fn complete_abandoned_object_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
        work_budget: &mut RequestWorkBudget,
        cleanup_context: ObjectPendingCommandCleanupContext<'_>,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, ObjectPgActionError> {
        let ObjectPendingCommandCleanupContext {
            execution_route,
            recovery_guard,
            convergence_requirement,
        } = cleanup_context;
        if let Some(recovery_guard) = recovery_guard {
            recovery_guard.record_outcome(
                PendingMetadataCommandOutcome::TerminalCleanupPending { applied: false },
            );
        }
        reservation_authority
            .release_metadata_command_bucket_write_reservation(command)
            .map_err(bucket_snapshot_error_to_object_pg_action_error)?;

        if let (
            MetadataCommandRouteMode::Recovery,
            Some(authorized_source),
            Some(follow_up_payload),
        ) = (
            execution_route.mode,
            execution_route.recovery_authorized_source,
            command.payload().abandoned_recovery_follow_up(),
        ) {
            let Some(follow_up) = self
                .reissue_pending_metadata_command_with_route_mode_and_recovery_guard_until(
                    pg_id,
                    command,
                    MetadataCommandExecutionRoute::recovery(
                        execution_route.recovery_proof(),
                        Some(authorized_source),
                        Some(command),
                    ),
                    &follow_up_payload,
                    recovery_guard,
                    work_budget.deadline(),
                )
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?
            else {
                return Err(conflicting_pending_object_metadata_command(
                    "abandoned recovery command was displaced before generation cleanup",
                ));
            };
            let follow_up_route = MetadataCommandExecutionRoute::recovery(
                execution_route.recovery_proof(),
                Some(authorized_source),
                Some(command),
            )
            .for_reissued_command(pg_id, command, &follow_up)
            .map_err(ObjectPgActionError::Store)?;
            match self.finish_object_pg_pending_slot_inner(
                pg_id,
                &follow_up,
                work_budget,
                reservation_authority,
                ObjectPendingCommandFinishContext {
                    execution_route: follow_up_route,
                    recovery_guard,
                    finish_policy: ObjectPendingCommandFinishPolicy::Standard,
                    convergence_requirement,
                },
            )? {
                PendingObjectMetadataCommandCompletion::Applied => {
                    self.after_object_metadata_command_abandoned_payload_cleanup(command);
                    return Ok(request_ops::PendingMetadataCommandTerminalCleanup::Removed);
                }
                PendingObjectMetadataCommandCompletion::PublishedPendingRecovery
                | PendingObjectMetadataCommandCompletion::TerminalCleanupPending { .. } => {
                    self.after_object_metadata_command_abandoned_payload_cleanup(command);
                    return Ok(request_ops::PendingMetadataCommandTerminalCleanup::Deferred);
                }
                PendingObjectMetadataCommandCompletion::Abandoned
                | PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(_) => {
                    return Err(conflicting_pending_object_metadata_command(
                        "certified generation cleanup did not apply",
                    ));
                }
            }
        }

        let command_bucket = command.bucket_name();
        let cleanup = match execution_route.mode {
            MetadataCommandRouteMode::Normal => self.remove_pending_metadata_command_for_bucket(
                pg_id,
                command_bucket,
                command,
            ),
            MetadataCommandRouteMode::Recovery => self
                .remove_pending_metadata_command_for_bucket_recovery(
                    execution_route,
                    pg_id,
                    command_bucket,
                    command,
                    work_budget,
                ),
        }
        .map_err(ObjectPgActionError::from)?;
        self.after_object_metadata_command_abandoned(command, reservation_authority)?;
        Ok(cleanup)
    }

    fn after_object_metadata_command_abandoned_payload_cleanup(
        &self,
        command: &MetadataCommandEnvelope,
    ) {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                for segment in &commit.segments {
                    self.delete_object_segment_payload_shards_best_effort(segment);
                }
            }
            MetadataCommandPayload::AppendStreamSegment(append) => {
                self.delete_stream_segment_payload_shards_best_effort(&append.segment);
            }
            _ => {}
        }
    }

    fn after_object_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), ObjectPgActionError> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                let release_result = reservation_authority
                    .release_object_generation_reservation_command_required(
                        command.id().pg_id(),
                        &commit.object.bucket,
                        &commit.object.key,
                        &commit.generation_reservation_id,
                    );
                self.after_object_metadata_command_abandoned_payload_cleanup(command);
                release_result?;
            }
            MetadataCommandPayload::AppendStreamSegment(_) => {
                self.after_object_metadata_command_abandoned_payload_cleanup(command);
            }
            MetadataCommandPayload::CreateStreamUpload(create)
                if create.session.target == StreamUploadTarget::PutObject =>
            {
                reservation_authority.release_object_generation_reservation_command_required(
                    command.id().pg_id(),
                    &create.session.bucket,
                    &create.session.key,
                    &create.session.session_id,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn release_object_generation_reservation_command_required(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            ReleaseObjectGenerationReservationCommandRequired
        );
        let mut work_budget =
            RequestWorkBudget::new(OBJECT_GENERATION_RESERVATION_RETRY_BUDGET, None)
                .for_operation("release_object_generation")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("object generation release retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::ReleaseObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied
                            | PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            } => {
                                return Ok(());
                            }
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation release partial pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: false,
                            } => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation release abandoned pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                        work_budget
                            .sleep_after_contention(
                                "object generation release pending drain retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                }
            }

            let command = match self.install_allocator_cleanup_metadata_command_with_fresh_id(
                publisher,
                pg_id,
                bucket,
                false,
                None,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReleaseObjectGeneration(
                            ReleaseObjectGenerationCommand::new(
                                bucket.clone(),
                                key.clone(),
                                reservation_id.clone(),
                            ),
                        ),
                    )
                },
            )? {
                AllocatorCleanupFreshInstallOutcome::Installed(command) => *command,
                AllocatorCleanupFreshInstallOutcome::PendingContenderDrained => {
                    work_budget
                        .sleep_after_contention(
                            "object generation release pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                AllocatorCleanupFreshInstallOutcome::LogConflictHandled => {
                    work_budget
                        .sleep_after_contention(
                            "object generation release log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(outcome) => {
                        if outcome == request_ops::MetadataCommandApplyOutcome::Converged {
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                        }
                        return Ok(());
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        return Ok(());
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial release object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.progress.is_abortable()
                            && error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command_until(
                                pg_id,
                                &command,
                                work_budget.deadline(),
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        command = reissued;
                    }
                    Err(error) => {
                        if error.progress.is_abortable() && error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            break;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn drain_pending_object_metadata_commands_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("test_metadata_command_recovery")
            .for_pg(pg_id);
        let mut authority = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            match self.drain_pending_metadata_command_with_recovery_authority(
                &mut authority,
                pg_id,
                &command,
            )? {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::Abandoned => {}
                PendingMetadataCommandOutcome::PublishedPendingRecovery => return Ok(()),
                PendingMetadataCommandOutcome::TerminalCleanupPending { .. } => return Ok(()),
                PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    return Err(conflicting_pending_object_metadata_command(
                        "retryable partial pending object metadata drain",
                    ));
                }
            }
        }
        Ok(())
    }

    fn drain_one_pending_object_metadata_command(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
            return Ok(());
        };
        self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
        Ok(())
    }

    fn drain_one_pending_object_metadata_command_with_work_budget(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
            return Ok(());
        };
        let _ = self.drain_pending_object_metadata_command_with_work_budget(
            publisher,
            pg_id,
            &command,
            work_budget,
        )?;
        Ok(())
    }

    fn drain_pending_object_metadata_commands_for_publisher_collect(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<CollectedPendingObjectMetadataCommands, ObjectPgActionError> {
        let mut applied = Vec::new();
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_publisher_collect")
            .for_pg(pg_id);
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let outcome = self.drain_pending_object_metadata_command_with_work_budget(
                publisher,
                pg_id,
                &command,
                &mut work_budget,
            )?;
            if Self::metadata_command_recovery_applied_collectable_object_command(&command, outcome)
            {
                applied.push(command);
            }
            if outcome.retains_pending_slot() {
                return Ok(CollectedPendingObjectMetadataCommands::PendingRecovery(applied));
            }
        }
        Ok(CollectedPendingObjectMetadataCommands::Drained(applied))
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        self.drain_pending_object_metadata_commands_for_exact_bucket_inner(
            pg_id,
            bucket,
            work_budget,
        )
    }

    fn emit_exact_bucket_object_drain_step(
        bucket: &BucketName,
        pg_id: PgId,
        step: &'static str,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(" {detail}")
        };
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_exact_object_drain_step",
            format!(
                "bucket={:?} object_pg_id={} step={}{}",
                bucket,
                pg_id.get(),
                step,
                suffix
            ),
        );
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(work_budget);
        let mut drain_iteration = 0u64;
        loop {
            drain_iteration += 1;
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "pending_lookup_start",
                format!("iteration={drain_iteration}"),
            );
            let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
                Self::emit_exact_bucket_object_drain_step(
                    bucket,
                    pg_id,
                    "pending_lookup_done",
                    format!("iteration={drain_iteration} has_pending=false"),
                );
                break;
            };
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "pending_lookup_done",
                format!(
                    "iteration={} has_pending=true command_kind={} command_bucket={:?}",
                    drain_iteration,
                    command.payload().kind_name(),
                    command.bucket_name()
                ),
            );
            recovery_authority.check("exact bucket object command drain budget exhausted")?;
            if command.bucket_name() != bucket {
                Self::emit_exact_bucket_object_drain_step(
                    bucket,
                    pg_id,
                    "stop_foreign_bucket",
                    format!(
                        "iteration={} command_kind={} command_bucket={:?}",
                        drain_iteration,
                        command.payload().kind_name(),
                        command.bucket_name()
                    ),
                );
                return Ok(());
            }
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "apply_start",
                format!(
                    "iteration={} command_kind={}",
                    drain_iteration,
                    command.payload().kind_name()
                ),
            );
            let outcome = match self
                .drain_pending_metadata_command_with_recovery_authority_and_requirement(
                    &mut recovery_authority,
                    pg_id,
                    &command,
                    request_ops::MetadataCommandConvergenceRequirement::RequireAllReplicas,
                ) {
                Ok(outcome) => outcome,
                Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandDependencyConvergencePending { .. }
                    | StoreError::MetadataCommandIrrevocableConvergencePending { .. },
                )) => {
                    recovery_authority.sleep_after_contention(
                        "exact bucket object command convergence budget exhausted",
                    )?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "apply_done",
                format!("iteration={} outcome={outcome:?}", drain_iteration),
            );
            match outcome {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::Abandoned => {}
                PendingMetadataCommandOutcome::PublishedPendingRecovery
                | PendingMetadataCommandOutcome::TerminalCleanupPending { .. } => {
                    recovery_authority.sleep_after_contention(
                        "exact bucket object command terminal cleanup budget exhausted",
                    )?;
                    continue;
                }
                PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    return Err(conflicting_pending_object_metadata_command(
                        "retryable partial pending object metadata drain",
                    ));
                }
            }
        }
        Ok(())
    }

    fn pending_command_completes_stream_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .is_some_and(|command| {
                pending_command_completes_stream_session(&command, bucket, key, session_id)
            }))
    }

    fn next_object_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        self.next_object_metadata_command_id_with_completion_admission(pg_id, false)
    }

    fn next_object_metadata_command_id_with_completion_admission(
        &self,
        pg_id: PgId,
        completion_admission: bool,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        if completion_admission {
            return self
                .next_completion_metadata_command_id(pg_id)
                .map_err(ObjectPgActionError::from);
        }
        self.next_metadata_command_id(pg_id)
            .map_err(ObjectPgActionError::from)
    }

    fn next_object_metadata_command_id_or_drain(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandId>, ObjectPgActionError> {
        self.next_object_metadata_command_id_or_drain_with_completion_admission(
            publisher, pg_id, bucket, false,
        )
    }

    fn next_object_metadata_command_id_or_drain_with_completion_admission(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
    ) -> Result<Option<MetadataCommandId>, ObjectPgActionError> {
        match self
            .next_object_metadata_command_id_with_completion_admission(pg_id, completion_admission)
        {
            Ok(command_id) => Ok(Some(command_id)),
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn next_object_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        self.next_metadata_command_id_from_locked_pg(pg_id, pg)
            .map_err(ObjectPgActionError::from)
    }

    fn next_bucket_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, BucketSnapshotLoadError> {
        self.next_metadata_command_id(pg_id)
            .map_err(BucketSnapshotLoadError::from)
    }

    fn next_completion_bucket_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, BucketSnapshotLoadError> {
        self.next_completion_metadata_command_id(pg_id)
            .map_err(BucketSnapshotLoadError::from)
    }

    fn apply_new_stream_append_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<StreamAppendCommandApplyOutcome, ObjectPgActionError> {
        match self.apply_new_object_metadata_command_for_bucket_or_reinspect(
            pg_id,
            bucket,
            command,
            work_budget,
        )? {
            request_ops::NewObjectMetadataCommandApplyOutcome::Applied
            | request_ops::NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
            | request_ops::NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => {
                Ok(StreamAppendCommandApplyOutcome::Applied)
            }
            request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(_) => {
                Ok(StreamAppendCommandApplyOutcome::RetryFromFreshSnapshot)
            }
            request_ops::NewObjectMetadataCommandApplyOutcome::Abandoned(error) => Err(error),
        }
    }

    fn after_object_metadata_command_applied(&self, command: &MetadataCommandEnvelope) {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&commit.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &commit.object.bucket,
                        &commit.object.key,
                        stale_generation_id,
                    );
                }
            }
            MetadataCommandPayload::CommitMultipartObject(commit) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&commit.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &commit.object.bucket,
                        &commit.object.key,
                        stale_generation_id,
                    );
                }
                self.delete_complete_multipart_cleanup_best_effort(
                    &Self::complete_multipart_command_cleanup(commit),
                );
            }
            MetadataCommandPayload::DeleteObjectVersion(delete) => {
                if let Some(reclaim_generation_id) =
                    delete_object_version_reclaim_generation(&delete.target)
                {
                    self.enqueue_object_payload_reclaim(
                        &delete.bucket,
                        &delete.key,
                        reclaim_generation_id,
                    );
                }
            }
            MetadataCommandPayload::InsertDeleteMarker(marker) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&marker.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &marker.bucket,
                        &marker.key,
                        stale_generation_id,
                    );
                }
            }
            MetadataCommandPayload::CreateStreamUpload(_) => {}
            MetadataCommandPayload::AbortStreamUpload(abort) => {
                self.delete_staged_stream_segment_payload_shards_best_effort(
                    &abort.staged_segments,
                );
            }
            MetadataCommandPayload::CommitStreamPart(commit) => {
                self.delete_finalize_upload_part_cleanup_best_effort(
                    &crate::FinalizeStreamPartCleanup {
                        displaced_segments: commit.displaced_segments.clone(),
                    },
                );
            }
            MetadataCommandPayload::AbortMultipartUpload(abort) => {
                self.delete_abort_multipart_cleanup_best_effort(&abort.cleanup);
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(delete) => {
                self.enqueue_bucket_delete_finalize(crate::BucketDeleteFinalizeRoot {
                    bucket: delete.bucket.clone(),
                    bucket_incarnation_generation: delete
                        .reclaim_claim
                        .bucket_incarnation_generation,
                });
            }
            _ => {}
        }
    }

    fn release_object_generation_reservation_best_effort(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) {
        let _ = self.release_object_generation_reservation(bucket, key, reservation_id);
    }

    fn release_object_generation_reservation_after_same_object_projection(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        loop {
            match self.release_object_generation_reservation_with_work_budget(
                bucket,
                key,
                reservation_id,
                work_budget,
            ) {
                Ok(()) => return Ok(()),
                Err(error)
                    if matches!(
                        error,
                        ObjectPgActionError::MetadataCommandRecoveryTransferred
                    ) || request_ops::object_pg_action_error_is_retryable_pending_drain(&error) =>
                {
                    if work_budget
                        .sleep_after_contention(
                            "same-object direct PUT loser cleanup retry budget exhausted",
                        )
                        .is_ok()
                    {
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn release_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mut work_budget =
            RequestWorkBudget::new(OBJECT_GENERATION_RESERVATION_RETRY_BUDGET, None)
                .for_operation("release_object_generation")
                .for_pg(pg_id);
        self.release_object_generation_reservation_with_work_budget(
            bucket,
            key,
            reservation_id,
            &mut work_budget,
        )
    }

    fn release_object_generation_reservation_with_work_budget(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            ReleaseObjectGenerationReservation
        );
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        loop {
            work_budget
                .check("object generation release retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::ReleaseObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied
                            | PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            } => {
                                return Ok(());
                            }
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation release command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: false,
                            } => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation release abandoned pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                        work_budget
                            .sleep_after_contention(
                                "object generation release pending drain retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                }
            }
            let Some(command_id) =
                self.next_object_metadata_command_id_or_drain(publisher, pg_id, bucket)?
            else {
                work_budget
                    .sleep_after_contention(
                        "object generation release command id retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        bucket.clone(),
                        key.clone(),
                        reservation_id.clone(),
                    ),
                ),
            );
            match self.install_allocator_cleanup_pending_command_or_drain(
                publisher, pg_id, bucket, &command, None,
            )? {
                AllocatorCleanupPendingInstallOutcome::Installed => {}
                AllocatorCleanupPendingInstallOutcome::RetryAfterContention => {
                    work_budget
                        .sleep_after_contention(
                            "object generation release pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            }
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(outcome) => {
                        if outcome == request_ops::MetadataCommandApplyOutcome::Converged {
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                        }
                        return Ok(());
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        return Ok(());
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial release object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.progress.is_abortable()
                            && error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command_until(
                                pg_id,
                                &command,
                                work_budget.deadline(),
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        command = reissued;
                        if let Err(error) = work_budget.sleep_after_contention(
                            "object generation release reissue retry budget exhausted",
                        ) {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            return Err(ObjectPgActionError::Store(error));
                        }
                    }
                    Err(error) => {
                        if error.progress.is_abortable() && error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            work_budget
                                .sleep_after_contention(
                                    "object generation release abandoned command retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            break;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn commit_direct_put_object_from_payload_shards<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        action: impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        self.commit_direct_put_object_from_payload_shards_with_route_validation(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(&req.bucket),
                object_pg_id: self.object_metadata_pg(&req.bucket, &req.key),
                bucket: &req.bucket,
                key: &req.key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            req,
            written_shards,
            || {},
            || Ok(()),
            action,
        )
    }

    fn commit_direct_put_object_from_payload_shards_with_route_validation<E>(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        mut disarm_payload_cleanup: impl FnMut(),
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            CommitDirectPutObjectFromPayloadShards
        );
        let PutObjectMutationEffectRoute {
            object_pg_id,
            bucket,
            key,
            effect_fence,
            ..
        } = route;
        if &req.bucket != bucket || &req.key != key {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT request does not match admitted object route".to_string(),
            });
        }
        let pg_id = object_pg_id.pg_id();
        let effective_bucket_write_reservation = req.bucket_write_reservation.clone();
        let mut bucket_write_proof_command_owned = false;
        #[derive(Clone, Copy)]
        enum DirectPutPayloadOwnership {
            Caller,
            DurableCommand,
        }
        let mut payload_ownership = DirectPutPayloadOwnership::Caller;
        macro_rules! release_caller_bucket_write_proof_if_unowned {
            () => {{
                if !bucket_write_proof_command_owned {
                    self.release_bucket_write_reservation_proof(&effective_bucket_write_reservation)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)
                } else {
                    Ok(())
                }
            }};
        }
        macro_rules! cleanup_direct_put_attempt_before_command_ownership {
            () => {{
                let release_result = release_caller_bucket_write_proof_if_unowned!();
                match payload_ownership {
                    DirectPutPayloadOwnership::Caller => {
                        self.release_object_generation_reservation_best_effort(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        );
                        self.delete_direct_put_segment_payload_shards_at_epoch(
                            effective_bucket_write_reservation.cluster_epoch,
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                    }
                    DirectPutPayloadOwnership::DurableCommand => {}
                }
                release_result?;
            }};
        }
        let mut work_budget = RequestWorkBudget::new(DIRECT_PUT_METADATA_RETRY_BUDGET, None)
            .for_operation("commit_direct_put_metadata")
            .for_pg(pg_id);
        macro_rules! check_direct_put_work_before_command_ownership {
            ($context:literal) => {{
                if let Err(error) = work_budget.check($context) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
            }};
        }
        macro_rules! reject_expired_direct_put_snapshot_before_command_ownership {
            () => {{
                if Instant::now() >= work_budget.deadline() {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::SnapshotReinspectionConflict);
                }
            }};
        }
        macro_rules! sleep_direct_put_before_command_ownership_after_contention {
            ($context:literal) => {{
                if let Err(error) = work_budget.sleep_after_contention($context) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
            }};
        }
        macro_rules! retry_direct_put_observation_before_command_ownership {
            ($error:expr, $context:literal) => {{
                let error = $error;
                let pending_subject_conflict = matches!(
                    &error,
                    ObjectPgActionError::Store(StoreError::MetadataCommandPendingConflict { .. })
                );
                if !pending_subject_conflict
                    && request_ops::object_pg_action_error_is_retryable_command_observation(&error)
                    && work_budget.sleep_after_contention($context).is_ok()
                {
                    continue;
                }
                cleanup_direct_put_attempt_before_command_ownership!();
                return Err(error);
            }};
        }
        macro_rules! retry_direct_put_pending_drain_error {
            ($error:ident, $context:literal) => {{
                if work_budget.sleep_after_contention($context).is_err() {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(direct_put_uninstalled_pending_drain_error($error));
                }
                continue;
            }};
        }
        macro_rules! require_direct_put_route_before_command_ownership {
            () => {{
                if let Err(error) = require_valid_route() {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
            }};
        }
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        require_direct_put_route_before_command_ownership!();
        let direct_put_metadata_client = match self.direct_put_metadata_primary_client(bucket, key)
        {
            Ok(client) => client,
            Err(error) => {
                cleanup_direct_put_attempt_before_command_ownership!();
                return Err(error.into());
            }
        };
        let direct_put_metadata_route = match direct_put_metadata_client
            .open_direct_put_metadata_route(self.operation_epoch(), object_pg_id, bucket, key)
        {
            Ok(route) => route,
            Err(error) => {
                cleanup_direct_put_attempt_before_command_ownership!();
                return Err(error);
            }
        };
        macro_rules! finish_direct_put_after_safe_abandonment {
            () => {{
                #[cfg(test)]
                request_ops::maybe_run_snapshot_reinspection_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    &mut work_budget,
                );
                let reinspection_deadline = work_budget.deadline();
                if work_budget
                    .check("direct PUT snapshot reinspection budget exhausted")
                    .is_err()
                {
                    self.release_object_generation_reservation_best_effort(
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                    );
                    self.delete_direct_put_segment_payload_shards(
                        req.data_pg_id,
                        req.ec,
                        &req.segment_okh,
                        req.segment_vid,
                        written_shards,
                    );
                    return Err(ObjectPgActionError::SnapshotReinspectionConflict);
                }
                let snapshot = match direct_put_metadata_route
                    .load_direct_put_commit_snapshot_until(
                        &req.generation_reservation_id,
                        req.generation_id,
                        reinspection_deadline,
                    ) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.release_object_generation_reservation_best_effort(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        );
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        if error.is_operation_deadline_exhaustion() {
                            return Err(ObjectPgActionError::SnapshotReinspectionConflict);
                        }
                        return Err(error);
                    }
                };
                if let Some(outcome) = Self::committed_direct_put_retry_outcome(req, &snapshot)? {
                    return Ok(Ok(outcome));
                }
                #[cfg(test)]
                request_ops::maybe_run_before_snapshot_reinspection_action_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if Instant::now() >= reinspection_deadline {
                    self.release_object_generation_reservation_best_effort(
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                    );
                    self.delete_direct_put_segment_payload_shards(
                        req.data_pg_id,
                        req.ec,
                        &req.segment_okh,
                        req.segment_vid,
                        written_shards,
                    );
                    return Err(ObjectPgActionError::SnapshotReinspectionConflict);
                }
                let action_result = action(snapshot.auth_snapshot);
                self.release_object_generation_reservation_best_effort(
                    &req.bucket,
                    &req.key,
                    &req.generation_reservation_id,
                );
                self.delete_direct_put_segment_payload_shards(
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                    written_shards,
                );
                match action_result {
                    Err(error) => return Ok(Err(error)),
                    Ok(()) => return Err(ObjectPgActionError::SnapshotReinspectionConflict),
                }
            }};
        }
        macro_rules! finish_direct_put_after_pending_uncertainty {
            ($command:ident, $error:expr) => {{
                match self.finish_direct_put_after_pending_command_uncertainty(
                    pg_id,
                    &req.bucket,
                    &$command,
                    $error,
                )? {
                    request_ops::NewObjectMetadataCommandApplyOutcome::Applied
                    | request_ops::NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
                    | request_ops::NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => break $command,
                    request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(_) => {
                        finish_direct_put_after_safe_abandonment!();
                    }
                    request_ops::NewObjectMetadataCommandApplyOutcome::Abandoned(error) => {
                        self.release_object_generation_reservation(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        )?;
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        return Err(error);
                    }
                }
            }};
        }

        let mut snapshot_retry_phase = SnapshotSensitiveRetryPhase::default();
        let mut same_object_publication_observed = false;
        let (command, new_pending_command) = loop {
            require_direct_put_route_before_command_ownership!();
            check_direct_put_work_before_command_ownership!(
                "direct PUT metadata retry budget exhausted"
            );
            let (
                mut command,
                new_pending_command,
                payload_acks_registered,
                mut evaluated_attempt,
            ) = loop {
                require_direct_put_route_before_command_ownership!();
                check_direct_put_work_before_command_ownership!(
                    "direct PUT metadata pending retry budget exhausted"
                );
                let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                    match self.pending_metadata_command_for_bucket_until(
                        pg_id,
                        &req.bucket,
                        work_budget.deadline(),
                    ) {
                        Ok(command) => command,
                        Err(error) => {
                            retry_direct_put_observation_before_command_ownership!(
                                ObjectPgActionError::Store(error),
                                "direct PUT pending command observation retry budget exhausted"
                            );
                        }
                    }
                } else {
                    None
                };
                let Some(command) = pending_command
                else {
                    #[cfg(test)]
                    if let Err(error) = request_ops::maybe_run_direct_put_snapshot_read_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    ) {
                        retry_direct_put_observation_before_command_ownership!(
                            error,
                            "direct PUT commit snapshot retry budget exhausted"
                        );
                    }
                    let snapshot = match direct_put_metadata_route
                        .load_direct_put_commit_snapshot_until(
                            &req.generation_reservation_id,
                            req.generation_id,
                            work_budget.deadline(),
                        ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            retry_direct_put_observation_before_command_ownership!(
                                error,
                                "direct PUT commit snapshot retry budget exhausted"
                            );
                        }
                    };
                    if snapshot.committed_segments.is_some() {
                        // A durable object may already own these exact staging keys even if
                        // corruption makes the retry snapshot fail validation below. Preserve
                        // payload in that ambiguous durable state and fail closed at metadata.
                        payload_ownership = DirectPutPayloadOwnership::DurableCommand;
                        disarm_payload_cleanup();
                    }
                    if let Some(outcome) = Self::committed_direct_put_retry_outcome(req, &snapshot)?
                    {
                        return Ok(Ok(outcome));
                    }
                    #[cfg(test)]
                    self.maybe_expire_direct_put_budget_after_snapshot_load(&mut work_budget);
                    reject_expired_direct_put_snapshot_before_command_ownership!();
                    match action(snapshot.auth_snapshot.clone()) {
                        Ok(()) => {
                            #[cfg(test)]
                            self.maybe_expire_direct_put_budget_after_action(&mut work_budget);
                            reject_expired_direct_put_snapshot_before_command_ownership!();
                        }
                        Err(error) => {
                            if same_object_publication_observed {
                                if let Err(cleanup_error) = self
                                    .release_object_generation_reservation_after_same_object_projection(
                                        &req.bucket,
                                        &req.key,
                                        &req.generation_reservation_id,
                                        &mut work_budget,
                                    )
                                {
                                    cleanup_direct_put_attempt_before_command_ownership!();
                                    return Err(cleanup_error);
                                }
                            }
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Ok(Err(error));
                        }
                    }
                    let evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();

                    let version_id = if req.versioning == crate::BucketVersioningState::Enabled {
                        match self.reserve_next_object_version_for_completion_with_effect_fence(
                            pg_id,
                            &req.bucket,
                            &req.key,
                            effect_fence,
                            &mut require_valid_route,
                        ) {
                            Ok(version_id) => version_id,
                            Err(error) => {
                                cleanup_direct_put_attempt_before_command_ownership!();
                                return Err(error);
                            }
                        }
                    } else {
                        VersionId::Null
                    };
                    reject_expired_direct_put_snapshot_before_command_ownership!();
                    if let Err(error) = self.maybe_run_before_direct_put_command_id_hook() {
                        retry_direct_put_observation_before_command_ownership!(
                            error,
                            "direct PUT pre-command observation retry budget exhausted"
                        );
                    }
                    if let Err(error) = require_valid_route() {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    reject_expired_direct_put_snapshot_before_command_ownership!();
                    if let Err(error) =
                        self.register_payload_shard_acks(req.data_pg_id, &shard_batch)
                    {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    let command = match direct_put_metadata_route
                        .build_direct_put_commit_command_until(
                            BuildDirectPutCommitCommandReq {
                                request: req,
                                version_id,
                                expected_snapshot: &snapshot,
                                bucket_write_reservation: &effective_bucket_write_reservation,
                            },
                            work_budget.deadline(),
                        ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::StaleDirectPutCommitSnapshot) => {
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT stale snapshot retry budget exhausted"
                            );
                            continue;
                        }
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            let pending_visible = match self
                                .pending_metadata_command_for_bucket_until(
                                    pg_id,
                                    &req.bucket,
                                    work_budget.deadline(),
                                )
                            {
                                Ok(pending) => pending.is_some(),
                                Err(error) => {
                                    retry_direct_put_observation_before_command_ownership!(
                                        ObjectPgActionError::Store(error),
                                        "direct PUT log conflict observation retry budget exhausted"
                                    );
                                }
                            };
                            #[cfg(test)]
                            request_ops::maybe_run_direct_put_pending_drain_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                                &mut work_budget,
                            );
                            let drain_result =
                                self.drain_after_object_pg_log_conflict_with_work_budget(
                                    publisher,
                                    pg_id,
                                    &req.bucket,
                                    pending_visible,
                                    &mut work_budget,
                                );
                            if let Err(error) = drain_result {
                                if request_ops::object_pg_action_error_is_retryable_pending_drain(
                                    &error,
                                ) {
                                    retry_direct_put_pending_drain_error!(
                                        error,
                                        "direct PUT command log conflict drain retry budget exhausted"
                                    );
                                }
                                cleanup_direct_put_attempt_before_command_ownership!();
                                return Err(error);
                            }
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT command log conflict retry budget exhausted"
                            );
                            continue;
                        }
                        Err(error) => {
                            retry_direct_put_observation_before_command_ownership!(
                                error,
                                "direct PUT command build retry budget exhausted"
                            );
                        }
                    };
                    break (command, true, true, Some(evaluated_attempt));
                };

                let matching_direct_put = match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.matches_request(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                            req.generation_id,
                        ) && commit.bucket_write_reservation
                            == effective_bucket_write_reservation =>
                    {
                        Some(commit.as_ref())
                    }
                    _ => None,
                };
                let is_matching_direct_put = matching_direct_put.is_some();
                let is_same_object_direct_put = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket == req.bucket && commit.object.key == req.key
                );
                if let Some(commit) = matching_direct_put {
                    bucket_write_proof_command_owned = true;
                    if Self::direct_put_command_owns_request_payload(commit, req) {
                        payload_ownership = DirectPutPayloadOwnership::DurableCommand;
                        disarm_payload_cleanup();
                    } else {
                        // The pending command owns the logical reservation and write proof, but
                        // it does not authorize recovery with a different physical payload. Take
                        // responsibility away from the RAII guard before returning: its generic
                        // cleanup would release command-owned state. Disjoint caller staging can
                        // be removed; overlapping keys may be the command's live recovery input
                        // and must remain untouched.
                        disarm_payload_cleanup();
                        let caller_owned_shards =
                            Self::direct_put_written_shards_not_owned_by_command(
                                commit,
                                req.data_pg_id,
                                written_shards,
                            );
                        if !caller_owned_shards.is_empty() {
                            self.delete_direct_put_segment_payload_shards_at_epoch(
                                effective_bucket_write_reservation.cluster_epoch,
                                req.data_pg_id,
                                req.ec,
                                &req.segment_okh,
                                req.segment_vid,
                                &caller_owned_shards,
                            );
                        }
                        return Err(conflicting_pending_object_metadata_command(
                            "pending direct PUT command payload differs from request",
                        ));
                    }
                }
                let abandonment_observation = self
                    .metadata_command_has_abandoned_log_on_acting_set_until(
                        &command,
                        work_budget.deadline(),
                    );
                #[cfg(test)]
                self.maybe_expire_direct_put_budget_after_abandonment_observation(
                    &command,
                    &mut work_budget,
                );
                let has_abandoned_log = match self
                    .resolve_metadata_command_abandonment_observation(
                        pg_id,
                        &command,
                        MetadataCommandRouteMode::Normal,
                        request_ops::MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                        abandonment_observation,
                        &mut work_budget,
                    )
                {
                    Ok(request_ops::MetadataCommandAbandonmentObservation::Observed(
                        has_abandoned_log,
                    )) => has_abandoned_log,
                    Ok(request_ops::MetadataCommandAbandonmentObservation::Retry) => continue,
                    Ok(
                        request_ops::MetadataCommandAbandonmentObservation::PublishedPendingRecovery,
                    ) if is_matching_direct_put => {
                        return self
                            .direct_put_outcome_from_published_command(&command)
                            .map(Ok);
                    }
                    Ok(
                        request_ops::MetadataCommandAbandonmentObservation::PublishedPendingRecovery,
                    ) if is_same_object_direct_put => {
                        // A different direct PUT for this object has crossed its publication
                        // boundary. Reinspect the object even while that command retains the
                        // bucket's pending slot so conditional requests project the winner's
                        // state instead of exposing an internal recovery handoff as SlowDown.
                        same_object_publication_observed = true;
                        snapshot_retry_phase.require_snapshot_reinspection();
                        continue;
                    }
                    Ok(
                        request_ops::MetadataCommandAbandonmentObservation::PublishedPendingRecovery,
                    ) => {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(conflicting_pending_object_metadata_command(
                            "unrelated published metadata command awaiting recovery",
                        ));
                    }
                    Err(error) => {
                        let error = bucket_snapshot_error_to_object_pg_action_error(error);
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                };
                if has_abandoned_log {
                    if is_matching_direct_put {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        let error =
                            match self.finish_exact_pending_object_metadata_command_with_work_budget(
                                pg_id,
                                exact,
                                &mut work_budget,
                            ) {
                                Ok(
                                    PendingObjectMetadataCommandCompletion::Applied
                                    | PendingObjectMetadataCommandCompletion::PublishedPendingRecovery,
                                ) => {
                                    unreachable!(
                                        "already-classified abandoned metadata command was applied"
                                    )
                                }
                                Ok(PendingObjectMetadataCommandCompletion::Abandoned) => {
                                    finish_direct_put_after_safe_abandonment!();
                                }
                                Ok(PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: false,
                                }) => {
                                    finish_direct_put_after_safe_abandonment!();
                                }
                                Ok(PendingObjectMetadataCommandCompletion::TerminalCleanupPending {
                                    applied: true,
                                }) => {
                                    unreachable!(
                                        "already-classified abandoned metadata command was applied"
                                    )
                                }
                                Ok(
                                    PendingObjectMetadataCommandCompletion::RetryPartialExactConflict(
                                        _,
                                    ),
                                ) => {
                                    conflicting_pending_object_metadata_command(
                                        "retryable partial pending command for direct put commit",
                                    )
                                }
                                Err(error) => error,
                            };
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    if let Err(error) = self.drain_pending_object_metadata_command_with_work_budget(
                        publisher,
                        pg_id,
                        &command,
                        &mut work_budget,
                    ) {
                        if request_ops::object_pg_action_error_is_retryable_pending_drain(&error) {
                            retry_direct_put_pending_drain_error!(
                                error,
                                "direct PUT abandoned pending drain retry budget exhausted"
                            );
                        }
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    sleep_direct_put_before_command_ownership_after_contention!(
                        "direct PUT abandoned pending drain retry budget exhausted"
                    );
                    continue;
                }
                if is_matching_direct_put {
                    break (command, false, false, None);
                }
                if is_same_object_direct_put {
                    match self
                        .drain_pending_object_metadata_command_outcome_for_semantic_projection_with_work_budget(
                            publisher,
                            pg_id,
                            &command,
                            &mut work_budget,
                        )
                    {
                        Ok(
                            PendingMetadataCommandOutcome::Applied
                            | PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            },
                        )
                        | Err(ObjectPgActionError::MetadataCommandRecoveryTransferred) => {
                            same_object_publication_observed = true;
                            snapshot_retry_phase.require_snapshot_reinspection();
                            continue;
                        }
                        Ok(PendingMetadataCommandOutcome::Abandoned) => {}
                        Ok(PendingMetadataCommandOutcome::TerminalCleanupPending {
                            applied: false,
                        }) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(ObjectPgActionError::MetadataCommandRecoveryTransferred);
                        }
                        Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict) => {
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "same-object direct PUT partial command retry budget exhausted"
                            );
                            continue;
                        }
                        Err(error) => {
                            if request_ops::object_pg_action_error_is_retryable_pending_drain(
                                &error,
                            ) {
                                retry_direct_put_pending_drain_error!(
                                    error,
                                    "same-object direct PUT pending drain retry budget exhausted"
                                );
                            }
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error);
                        }
                    }
                } else if let Err(error) = self
                    .drain_pending_object_metadata_command_with_work_budget(
                        publisher,
                        pg_id,
                        &command,
                        &mut work_budget,
                    )
                {
                    if request_ops::object_pg_action_error_is_retryable_pending_drain(&error) {
                        retry_direct_put_pending_drain_error!(
                            error,
                            "direct PUT unrelated pending drain retry budget exhausted"
                        );
                    }
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(error);
                }
                sleep_direct_put_before_command_ownership_after_contention!(
                    "direct PUT unrelated pending drain retry budget exhausted"
                );
            };

            if !payload_acks_registered {
                #[cfg(test)]
                self.maybe_run_before_direct_put_payload_ack_registration_hook()?;
                if let Err(error) = self.register_payload_shard_acks(req.data_pg_id, &shard_batch) {
                    if new_pending_command {
                        let release_result =
                            self.release_metadata_command_bucket_write_reservation(&command);
                        self.release_object_generation_reservation_best_effort(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        );
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        release_result.map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    }
                    return Err(error);
                }
            }
            if let Err(error) = self.validate_payload_shard_acks(
                req.data_pg_id,
                req.ec,
                &req.segment_okh,
                req.segment_vid,
                &shard_batch,
            ) {
                if new_pending_command {
                    let release_result =
                        self.release_metadata_command_bucket_write_reservation(&command);
                    self.release_object_generation_reservation_best_effort(
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                    );
                    self.delete_direct_put_segment_payload_shards(
                        req.data_pg_id,
                        req.ec,
                        &req.segment_okh,
                        req.segment_vid,
                        written_shards,
                    );
                    release_result.map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                }
                return Err(error);
            }
            if new_pending_command {
                check_direct_put_work_before_command_ownership!(
                    "direct PUT command install retry budget exhausted"
                );
                self.maybe_run_before_metadata_command_pending_install_hook();
                let mut install_may_have_applied = false;
                let mut install_confirmation_found_applied = false;
                let install = loop {
                    match self
                        .install_snapshot_sensitive_metadata_command_or_drain_with_work_budget_classified(
                            publisher,
                            pg_id,
                            &req.bucket,
                            &command,
                            Some(effect_fence),
                            &mut work_budget,
                            &mut install_may_have_applied,
                            evaluated_attempt.as_mut().expect(
                                "new direct PUT command requires an evaluated snapshot attempt",
                            ),
                        ) {
                        Ok(install) => break install,
                        Err(error) => {
                            if install_may_have_applied {
                                bucket_write_proof_command_owned = true;
                                payload_ownership = DirectPutPayloadOwnership::DurableCommand;
                                disarm_payload_cleanup();
                            }
                            let retryable = if install_may_have_applied {
                                request_ops::object_pg_action_error_is_retryable_command_observation(
                                    &error,
                                )
                            } else {
                                request_ops::object_pg_action_error_is_retryable_pending_drain(
                                    &error,
                                )
                            };
                            if retryable {
                                match work_budget.sleep_after_contention(
                                    "direct PUT pending install drain retry budget exhausted",
                                ) {
                                    Ok(()) => continue,
                                    Err(budget_error) if install_may_have_applied => {
                                        let (outcome, command_owned) = self
                                            .finish_direct_put_after_pending_install_uncertainty(
                                                pg_id,
                                                &req.bucket,
                                                &mut command,
                                                ObjectPgActionError::Store(budget_error),
                                            )?;
                                        match outcome {
                                            request_ops::NewObjectMetadataCommandApplyOutcome::Applied
                                            | request_ops::NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
                                            | request_ops::NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => {
                                                install_confirmation_found_applied = true;
                                                break SnapshotSensitiveInstallOutcome::Installed;
                                            }
                                            request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(_) => {
                                                if !command_owned {
                                                    bucket_write_proof_command_owned = false;
                                                    payload_ownership =
                                                        DirectPutPayloadOwnership::Caller;
                                                    cleanup_direct_put_attempt_before_command_ownership!();
                                                    return Err(ObjectPgActionError::SnapshotReinspectionConflict);
                                                }
                                                finish_direct_put_after_safe_abandonment!();
                                            }
                                            request_ops::NewObjectMetadataCommandApplyOutcome::Abandoned(error) => {
                                                if !command_owned {
                                                    bucket_write_proof_command_owned = false;
                                                    payload_ownership =
                                                        DirectPutPayloadOwnership::Caller;
                                                    cleanup_direct_put_attempt_before_command_ownership!();
                                                    return Err(error);
                                                }
                                                self.release_object_generation_reservation(
                                                    &req.bucket,
                                                    &req.key,
                                                    &req.generation_reservation_id,
                                                )?;
                                                self.delete_direct_put_segment_payload_shards(
                                                    req.data_pg_id,
                                                    req.ec,
                                                    &req.segment_okh,
                                                    req.segment_vid,
                                                    written_shards,
                                                );
                                                return Err(error);
                                            }
                                        }
                                    }
                                    Err(_) => {}
                                }
                            }
                            if install_may_have_applied {
                                return Err(error);
                            }
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(direct_put_uninstalled_pending_drain_error(error));
                        }
                    }
                };
                match install {
                    SnapshotSensitiveInstallOutcome::Installed => {
                        payload_ownership = DirectPutPayloadOwnership::DurableCommand;
                        disarm_payload_cleanup();
                        #[cfg(test)]
                        if !install_confirmation_found_applied {
                            request_ops::maybe_run_direct_put_pending_installed_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                                &command,
                                &mut work_budget,
                            );
                        }
                    }
                    SnapshotSensitiveInstallOutcome::ReinspectSnapshot => {
                        continue;
                    }
                    SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        // Draining the predecessor is forward progress. Re-enter the FIFO PG
                        // admission path immediately so maintenance arriving afterward cannot
                        // repeatedly overtake this request during voluntary backoff. The outer
                        // loop checks the same absolute work deadline before doing more work.
                        continue;
                    }
                }
                if install_confirmation_found_applied {
                    break (command, false);
                }
            }
            break (command, new_pending_command);
        };

        let mut command = command;
        let mut apply_as_new = new_pending_command;
        let command = 'direct_recovery: loop {
            let recovery = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery_and_wait_for_authorized_handoff_until(
                    pg_id,
                    &command,
                    work_budget.deadline(),
                );
            let recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    match resolution {
                        Some(MetadataCommandRecoveryResolution::Outcome(
                            PendingMetadataCommandOutcome::Applied
                            | PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            },
                        )) => break command,
                        Some(MetadataCommandRecoveryResolution::Outcome(
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: false,
                            },
                        )) => finish_direct_put_after_safe_abandonment!(),
                        _ => {}
                    }
                    return Err(
                        ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery,
                    );
                }
                MetadataCommandRecoveryAdmission::Waited {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    apply_as_new = false;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_wait");
                    if let Some(resolution) = resolution {
                        match resolution {
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::Applied
                                | PendingMetadataCommandOutcome::PublishedPendingRecovery
                                | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: true,
                                },
                            ) => break command,
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::Abandoned
                                | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: false,
                                },
                            ) => finish_direct_put_after_safe_abandonment!(),
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::RetryPartialExactConflict,
                            ) => {
                                return Err(ObjectPgActionError::Store(
                                    StoreError::MetadataCommandIrrevocableConvergencePending {
                                        pg_id: command.id().pg_id().get(),
                                        cluster_epoch: command.id().cluster_epoch(),
                                        log_index: command.id().log_index().get(),
                                    },
                                ));
                            }
                            resolution => {
                                return Err(metadata_command_recovery_resolution_error(
                                    &command,
                                    resolution,
                                )
                                .expect(
                                    "irreversible recovery resolution must produce an error",
                                ));
                            }
                        }
                    }
                    if let Err(error) =
                        work_budget.check("direct PUT pending recovery wait budget exhausted")
                    {
                        finish_direct_put_after_pending_uncertainty!(
                            command,
                            ObjectPgActionError::Store(error)
                        );
                    }
                    let waiter_outcome = self
                        .pending_command_recovery_waiter_outcome_with_route_mode_until(
                            pg_id,
                            &command,
                            MetadataCommandRouteMode::Normal,
                            work_budget.deadline(),
                        )?;
                    match waiter_outcome {
                        MetadataCommandRecoveryWaiterOutcome::Applied => {
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id,
                                &command,
                                waiter_outcome.metric_label(),
                            );
                            break command;
                        }
                        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
                        => {
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id,
                                &command,
                                waiter_outcome.metric_label(),
                            );
                            finish_direct_put_after_safe_abandonment!();
                        }
                        MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
                            let outcome = match (new_pending_command, waiter_outcome) {
                                (true, MetadataCommandRecoveryWaiterOutcome::MissingNotApplied) => {
                                    "cleanup_suppressed_waiter_missing_not_applied"
                                }
                                (
                                    true,
                                    MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied,
                                ) => "cleanup_suppressed_waiter_replaced_not_applied",
                                _ => waiter_outcome.metric_label(),
                            };
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id, &command, outcome,
                            );
                            // The recovery leader may have reissued and applied a matching
                            // command, so the owner cannot safely tear down payload state here.
                            return Err(conflicting_pending_object_metadata_command(
                                "retryable partial pending command for direct put commit",
                            ));
                        }
                        MetadataCommandRecoveryWaiterOutcome::StillPending => {
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id,
                                &command,
                                waiter_outcome.metric_label(),
                            );
                            if let Err(error) = work_budget.sleep_after_contention(
                                "direct PUT pending recovery retry budget exhausted",
                            ) {
                                finish_direct_put_after_pending_uncertainty!(
                                    command,
                                    ObjectPgActionError::Store(error)
                                );
                            }
                            continue;
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut {
                    wait_us,
                    lineage_tip,
                    resolution,
                } => {
                    command = lineage_tip;
                    apply_as_new = false;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::TimedOut,
                        wait_us,
                    );
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        "timed_out",
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_timeout");
                    #[cfg(test)]
                    request_ops::maybe_run_pending_command_recovery_timeout_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        &command,
                        &mut work_budget,
                    );
                    if let Some(resolution) = resolution {
                        match resolution {
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::Applied
                                | PendingMetadataCommandOutcome::PublishedPendingRecovery
                                | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: true,
                                },
                            ) => break command,
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::Abandoned
                                | PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: false,
                                },
                            ) => finish_direct_put_after_safe_abandonment!(),
                            MetadataCommandRecoveryResolution::Outcome(
                                PendingMetadataCommandOutcome::RetryPartialExactConflict,
                            ) => {
                                return Err(ObjectPgActionError::Store(
                                    StoreError::MetadataCommandIrrevocableConvergencePending {
                                        pg_id: command.id().pg_id().get(),
                                        cluster_epoch: command.id().cluster_epoch(),
                                        log_index: command.id().log_index().get(),
                                    },
                                ));
                            }
                            resolution => {
                                let error = metadata_command_recovery_resolution_error(
                                    &command,
                                    resolution,
                                )
                                .expect("irreversible recovery resolution must produce an error");
                                if work_budget
                                    .sleep_after_contention(
                                        "direct PUT authorized recovery handoff budget exhausted",
                                    )
                                    .is_ok()
                                {
                                    continue;
                                }
                                return Err(error);
                            }
                        }
                    }
                    if let Err(error) = work_budget.sleep_after_contention(
                        "direct PUT pending recovery retry budget exhausted",
                    ) {
                        finish_direct_put_after_pending_uncertainty!(
                            command,
                            ObjectPgActionError::Store(error)
                        );
                    }
                    continue;
                }
            };

            #[cfg(test)]
            let injected_uncertainty = match
                request_ops::maybe_force_direct_put_metadata_apply_uncertainty(
                    self.metadata_command_apply_test_hook_scope_id(),
                    &command,
                ) {
                    request_ops::DirectPutMetadataApplyUncertaintyTestAction::None => false,
                    request_ops::DirectPutMetadataApplyUncertaintyTestAction::Inject => true,
                    request_ops::DirectPutMetadataApplyUncertaintyTestAction::InjectAfterBudgetExpiry => {
                        work_budget.expire_for_test();
                        true
                    }
                };
            #[cfg(not(test))]
            let injected_uncertainty = false;
            let mut apply = if injected_uncertainty {
                let id = command.id();
                Err(ObjectPgActionError::Store(
                    StoreError::MetadataCommandOutcomeUnconfirmed {
                        pg_id: id.pg_id().get(),
                        cluster_epoch: id.cluster_epoch(),
                        log_index: id.log_index().get(),
                    },
                ))
            } else if apply_as_new {
                self.apply_new_object_metadata_command_for_bucket_or_reinspect_with_recovery_guard(
                    pg_id,
                    &req.bucket,
                    &command,
                    &mut work_budget,
                    &recovery_guard,
                )
            } else {
                self.apply_recovered_pending_object_metadata_command_for_bucket_or_reinspect_with_recovery_guard(
                    pg_id,
                    &req.bucket,
                    &command,
                    &mut work_budget,
                    &recovery_guard,
                )
            };
            let mut unresolved_irreversible = None;
            let apply = loop {
                match apply {
                    Ok(apply) => break apply,
                    Err(error) if metadata_command_irreversible_resolution(&error).is_some() => {
                        let resolution = metadata_command_irreversible_resolution(&error)
                            .expect("guard requires typed irreversible uncertainty");
                        command = recovery_guard.lineage_tip();
                        apply_as_new = false;
                        unresolved_irreversible = Some(resolution);
                        // The request still owns this exact flight and its active route. Retry
                        // there before depending on a later heartbeat to authorize recovery.
                        if work_budget
                            .sleep_after_contention(
                                "direct PUT same-route convergence budget exhausted",
                            )
                            .is_err()
                        {
                            recovery_guard.mark_irreversible_handoff(resolution);
                            recovery_guard.relinquish_for_authorized_recovery();
                            continue 'direct_recovery;
                        }
                        apply = self
                            .apply_recovered_pending_object_metadata_command_for_bucket_or_reinspect_with_recovery_guard(
                                pg_id,
                                &req.bucket,
                                &command,
                                &mut work_budget,
                                &recovery_guard,
                            );
                    }
                    Err(error) if unresolved_irreversible.is_some() => {
                        recovery_guard.mark_irreversible_handoff(
                            unresolved_irreversible
                                .expect("same-route retry requires irreversible progress"),
                        );
                        if request_ops::object_pg_action_error_is_retryable_command_observation(
                            &error,
                        ) {
                            recovery_guard.relinquish_for_authorized_recovery();
                            continue 'direct_recovery;
                        }
                        recovery_guard.relinquish_for_authorized_recovery();
                        return Err(error);
                    }
                    Err(error) if recovery_guard.lineage_advanced_from(&command) => {
                        recovery_guard.relinquish_for_authorized_recovery();
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                }
            };
            match apply {
                outcome @ (request_ops::NewObjectMetadataCommandApplyOutcome::Applied
                | request_ops::NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
                | request_ops::NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending) => {
                    let outcome = outcome
                        .pending_metadata_command_outcome()
                        .expect("logically applied command must produce a recovery outcome");
                    recovery_guard.record_outcome(outcome);
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        outcome.metric_label(),
                    );
                    if outcome.retains_pending_slot() {
                        recovery_guard.relinquish_for_authorized_recovery();
                    }
                    break command;
                }
                request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(_error) => {
                    recovery_guard.record_outcome(PendingMetadataCommandOutcome::Abandoned);
                    finish_direct_put_after_safe_abandonment!();
                }
                request_ops::NewObjectMetadataCommandApplyOutcome::Abandoned(error) => {
                    recovery_guard.record_outcome(PendingMetadataCommandOutcome::Abandoned);
                    self.release_object_generation_reservation(
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                    )?;
                    self.delete_direct_put_segment_payload_shards(
                        req.data_pg_id,
                        req.ec,
                        &req.segment_okh,
                        req.segment_vid,
                        written_shards,
                    );
                    return Err(error);
                }
            }
        };

        debug_assert!(matches!(
            payload_ownership,
            DirectPutPayloadOwnership::DurableCommand
        ));

        self.direct_put_outcome_from_published_command(&command)
            .map(Ok)
    }

    fn direct_put_outcome_from_published_command(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<FinalizeDirectPutObjectOutcome, ObjectPgActionError> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                #[cfg(any(test, feature = "test-hooks"))]
                crate::node::maybe_run_after_direct_put_metadata_publish_hook(
                    self.metadata_primary_test_hook_node().test_hook_scope_id(),
                    &commit.object.bucket,
                    &commit.object.key,
                )?;
                Ok(FinalizeDirectPutObjectOutcome {
                    version_id: commit.object.version_id,
                    encryption: commit.object.encryption.clone(),
                    live_tags: commit.object.tags.clone(),
                    live_size: commit.object.size,
                    live_last_modified: commit.last_modified_millis,
                    stale_generation_id: commit.stale_payload.as_ref().map(
                        |payload| match payload {
                            ObjectPayloadReclaimCommand::Segments(reclaim) => reclaim.generation_id,
                            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                                reclaim.generation_id
                            }
                        },
                    ),
                })
            }
            _ => unreachable!("direct put commit pending command kind changed"),
        }
    }

    fn committed_direct_put_retry_outcome(
        req: &CommitDirectPutObjectReq,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<Option<FinalizeDirectPutObjectOutcome>, ObjectPgActionError> {
        let Some(segments) = snapshot.committed_segments.as_ref() else {
            return Ok(None);
        };
        let Some(live) = snapshot
            .current
            .as_ref()
            .and_then(crate::StoredObject::as_live)
        else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry snapshot has segments but no live object"
                    .to_string(),
            });
        };
        let version_shape_matches = match req.versioning {
            crate::BucketVersioningState::Enabled => !live.version_id.is_null(),
            crate::BucketVersioningState::Disabled | crate::BucketVersioningState::Suspended => {
                live.version_id.is_null()
            }
        };
        let expected_etag = ObjectEtag::single_part(req.etag_crc64);
        if live.bucket != req.bucket
            || live.key != req.key
            || !version_shape_matches
            || live.owner != req.owner
            || live.acl_grants != req.acl_grants
            || live.public_read != req.public_read
            || live.generation_id != req.generation_id
            || live.size != req.size
            || live.etag != expected_etag
            || live.ec != req.ec
            || live.layout != ObjectLayout::Standard
            || live.tags != req.tags
            || live.metadata_blob.as_ref() != Some(&req.metadata_blob)
            || live.system_metadata_blob.as_ref() != Some(&req.system_metadata_blob)
            || live.object_lock != req.object_lock
            || live.encryption != req.encryption
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry live object does not match request".to_string(),
            });
        }
        if segments.len() != 1 {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry must have exactly one committed segment"
                    .to_string(),
            });
        }
        let segment = &segments[0];
        if segment.bucket != req.bucket
            || segment.key != req.key
            || segment.version_id != live.version_id
            || segment.segment_index != req.segment_index
            || segment.size != req.size
            || segment.segment_crc64 != req.segment_crc64
            || segment.segment_okh != req.segment_okh
            || segment.segment_vid != req.segment_vid
            || segment.data_pg_id != req.data_pg_id
            || segment.placement_cluster_epoch != req.bucket_write_reservation.cluster_epoch
            || segment.ec_k != req.ec.k
            || segment.ec_m != req.ec.m
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry segment does not match request".to_string(),
            });
        }
        Ok(Some(FinalizeDirectPutObjectOutcome {
            version_id: live.version_id,
            encryption: live.encryption.clone(),
            live_tags: live.tags.clone(),
            live_size: live.size,
            live_last_modified: live.last_modified,
            stale_generation_id: snapshot.committed_stale_generation_id,
        }))
    }

    fn direct_put_command_owns_request_payload(
        command: &CommitDirectPutObjectCommand,
        req: &CommitDirectPutObjectReq,
    ) -> bool {
        let [segment] = command.segments.as_slice() else {
            return false;
        };
        segment.bucket == req.bucket
            && segment.key == req.key
            && segment.segment_index == req.segment_index
            && segment.size == req.size
            && segment.segment_crc64 == req.segment_crc64
            && segment.segment_okh == req.segment_okh
            && segment.segment_vid == req.segment_vid
            && segment.data_pg_id == req.data_pg_id
            && segment.placement_cluster_epoch == req.bucket_write_reservation.cluster_epoch
            && segment.ec_k == req.ec.k
            && segment.ec_m == req.ec.m
    }

    fn direct_put_written_shards_not_owned_by_command(
        command: &CommitDirectPutObjectCommand,
        request_data_pg_id: u32,
        written_shards: &[WrittenShardAck],
    ) -> Vec<WrittenShardAck> {
        let command_owned_keys: HashSet<ShardKey> = command
            .segments
            .iter()
            .filter(|segment| segment.data_pg_id == request_data_pg_id)
            .flat_map(|segment| {
                Self::payload_shard_set_keys(
                    &segment.segment_okh,
                    segment.segment_vid,
                    EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                )
            })
            .collect();
        written_shards
            .iter()
            .filter(|written| !command_owned_keys.contains(&written.key))
            .cloned()
            .collect()
    }

    #[cfg(test)]
    fn prepare_commit_direct_put_object_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        req: &CommitDirectPutObjectReq,
        version_id: VersionId,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let reserved_generation = object_pg.get_object_generation_reservation(
            &req.bucket,
            &req.key,
            &req.generation_reservation_id,
        )?;
        if reserved_generation != req.generation_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "object generation reservation mismatch: reserved {} but commit requested {}",
                    reserved_generation.get(),
                    req.generation_id.get()
                ),
            });
        }

        let last_modified_millis = crate::clock::current_time_millis();
        let write_sequence =
            object_pg.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
        let stale_payload = if version_id.is_null() {
            self.snapshot_direct_put_stale_payload_command(
                object_pg,
                &req.bucket,
                &req.key,
                last_modified_millis,
            )?
        } else {
            None
        };

        let segment_record = ObjectSegmentRecord {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            segment_index: req.segment_index,
            size: req.size,
            segment_crc64: req.segment_crc64,
            segment_okh: req.segment_okh,
            segment_vid: req.segment_vid,
            data_pg_id: req.data_pg_id,
            placement_cluster_epoch: self.operation_epoch(),
            ec_k: req.ec.k,
            ec_m: req.ec.m,
        };
        let object = PutLiveObjectReq {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            owner: req.owner.clone(),
            acl_grants: req.acl_grants.clone(),
            public_read: req.public_read,
            generation_id: req.generation_id,
            size: req.size,
            etag: ObjectEtag::single_part(req.etag_crc64),
            ec: req.ec,
            layout: ObjectLayout::Standard,
            tags: req.tags.clone(),
            metadata_blob: Some(req.metadata_blob.clone()),
            system_metadata_blob: Some(req.system_metadata_blob.clone()),
            object_lock: req.object_lock,
            encryption: req.encryption.clone(),
        };
        let command_id = self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?;
        let command = CommitDirectPutObjectCommand {
            object,
            segments: vec![segment_record],
            generation_reservation_id: req.generation_reservation_id.clone(),
            write_sequence,
            last_modified_millis,
            stale_payload,
            bucket_write_reservation,
        };
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
        ))
    }

    #[cfg(test)]
    fn snapshot_direct_put_stale_payload_command(
        &self,
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        created_at: u64,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, MetadataError> {
        let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
            Ok(stored) => stored,
            Err(MetadataError::ObjectNotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let record = match stored {
            crate::StoredObject::Live(record) => record,
            crate::StoredObject::DeleteMarker(_) => return Ok(None),
        };

        Ok(Some(Self::snapshot_live_object_payload_reclaim_command(
            pg, bucket, key, &record, created_at,
        )?))
    }

    #[cfg(test)]
    fn snapshot_live_object_payload_reclaim_command(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &crate::LiveObjectRecord,
        created_at: u64,
    ) -> Result<ObjectPayloadReclaimCommand, MetadataError> {
        match record.layout {
            ObjectLayout::Standard => {
                let segments =
                    PgMetadataStore::get_object_segments(pg, bucket, key, record.version_id)?;
                Ok(ObjectPayloadReclaimCommand::Segments(
                    ObjectSegmentsReclaimRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        generation_id: record.generation_id,
                        created_at,
                        segments: segments
                            .into_iter()
                            .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                                segment_index: segment.segment_index,
                                segment_okh: segment.segment_okh,
                                segment_vid: segment.segment_vid,
                                data_pg_id: segment.data_pg_id,
                                ec: EcShape {
                                    k: segment.ec_k,
                                    m: segment.ec_m,
                                },
                            })
                            .collect(),
                    },
                ))
            }
            ObjectLayout::MultipartManifest { .. } => {
                let parts = PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                        pg,
                        bucket,
                        key,
                        record.version_id,
                        part.part_number,
                    )?);
                }
                Ok(ObjectPayloadReclaimCommand::Multipart(
                    MultipartReclaimRecord::from_object_parts(
                        bucket,
                        key,
                        record.generation_id,
                        created_at,
                        &parts,
                        &streaming_segments,
                    ),
                ))
            }
        }
    }

    pub(crate) fn delete_direct_put_segment_payload_shards(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        written_shards: &[WrittenShardAck],
    ) {
        self.delete_payload_shard_keys_best_effort(
            data_pg_id,
            ec,
            segment_okh,
            segment_vid,
            written_shards.iter().map(|written| written.key.clone()),
        );
    }

    fn delete_direct_put_segment_payload_shards_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        written_shards: &[WrittenShardAck],
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            segment_okh,
            segment_vid,
            written_shards.iter().map(|written| written.key.clone()),
        );
    }

    #[cfg(test)]
    pub(crate) fn create_put_object_stream_session_record(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        self.create_put_object_stream_session_record_with_cleanup_deadline(
            bucket, key, session_id, encryption, None,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn create_put_object_stream_session_record_with_cleanup_deadline(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        cleanup_after: Option<u64>,
    ) -> Result<(), ObjectPgActionError> {
        self.create_put_object_stream_session_record_with_route_validation(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            session_id,
            encryption,
            cleanup_after,
            || Ok(()),
        )
    }

    fn create_put_object_stream_session_record_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        cleanup_after: Option<u64>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        let PutObjectMutationEffectRoute {
            bucket,
            key,
            effect_fence,
            object_pg_id,
            ..
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mut work_budget = RequestWorkBudget::new(PUT_OBJECT_STREAM_CREATE_RETRY_BUDGET, None)
            .for_operation("create_put_object_stream_session")
            .for_pg(pg_id);
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("put object stream create retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => return Err(bucket_snapshot_error_to_object_pg_action_error(error)),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let request = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::PutObject,
                encryption,
            };
            let result = self.create_put_object_stream_session_record_under_reservation(
                request,
                cleanup_after,
                proof.clone(),
                route,
                &mut require_valid_route,
                &mut work_budget,
            );
            let release_result = match &result {
                Ok(BucketWriteReservationDisposition::TransferredToCommand) => Ok(()),
                Ok(BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure) => Ok(()),
                Ok(BucketWriteReservationDisposition::ReleaseByCaller) => self
                    .release_durable_bucket_write_reservation(reservation)
                    .map_err(bucket_snapshot_error_to_object_pg_action_error),
                Err(_) => {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => Ok(()),
                        Ok(false) => self
                            .release_durable_bucket_write_reservation(reservation)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error),
                        Err(error) => Err(error),
                    }
                }
            };
            return match (result, release_result) {
                (Ok(_), Ok(())) => Ok(()),
                (Ok(_), Err(error)) => Err(error),
                (Err(error), Ok(())) | (Err(error), Err(_)) => Err(error),
            };
        }
    }

    fn create_put_object_stream_session_record_under_reservation(
        &self,
        request: CreateStreamUploadReq,
        cleanup_after: Option<u64>,
        bucket_write_reservation: BucketWriteReservationProof,
        route: PutObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<BucketWriteReservationDisposition, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            CreatePutObjectStreamSessionRecordUnderReservation
        );
        let bucket = &request.bucket;
        let key = &request.key;
        let session_id = &request.session_id;
        if request.bucket != *route.bucket || request.key != *route.key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "create put object stream session record",
                },
            ));
        }
        let object_pg_id = route.object_pg_id;
        let pg_id = object_pg_id.pg_id();
        debug_assert_eq!(request.target, StreamUploadTarget::PutObject);
        let mut snapshot_retry_phase = SnapshotSensitiveRetryPhase::default();
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("put object stream create retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let applied_commands = if snapshot_retry_phase.pending_drain_allowed() {
                self.drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, bucket,
                )?
            } else {
                CollectedPendingObjectMetadataCommands::empty_drained()
            };
            let expected_command = applied_stream_create_command(
                applied_commands.commands(),
                &request,
                cleanup_after,
            );
            let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
            let stream_creation_route = mutation_client
                .open_stream_upload_creation_metadata_route(
                    self.operation_epoch(),
                    object_pg_id,
                    bucket,
                    key,
                )?;
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            if stream_creation_route.matching_stream_upload_exists(&request, expected_command)? {
                return Ok(BucketWriteReservationDisposition::ReleaseByCaller);
            }
            applied_commands.require_drained_for_unmatched_request()?;
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            self.reserve_put_object_generation_with_route_validation(
                route,
                session_id,
                &mut require_valid_route,
            )?;
            if let Err(error) = require_valid_route() {
                let _ = self.release_object_generation_reservation(bucket, key, session_id);
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match stream_creation_route.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    request: &request,
                    cleanup_after,
                    precondition: CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                        require_generation_reservation: true,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    work_budget
                        .sleep_after_contention(
                            "put object stream create stale read retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    work_budget
                        .sleep_after_contention(
                            "put object stream create log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => {
                    let _ = self.release_object_generation_reservation(bucket, key, session_id);
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                let _ = self.release_object_generation_reservation(bucket, key, session_id);
                return Err(ObjectPgActionError::Store(error));
            }
            self.maybe_run_before_metadata_command_pending_install_hook();
            match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(route.effect_fence),
                &mut evaluated_attempt,
            )? {
                SnapshotSensitiveInstallOutcome::Installed => {}
                SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    work_budget
                        .sleep_after_contention(
                            "put object stream create pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                if self
                    .pending_metadata_command_for_bucket(pg_id, bucket)?
                    .is_none()
                {
                    let _ = self.release_object_generation_reservation(bucket, key, session_id);
                }
                return Err(error);
            }
            return Ok(BucketWriteReservationDisposition::TransferredToCommand);
        }
    }

    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, crate::StreamUploadFailure> {
        (|| {
            let object_pg_id = self.object_metadata_pg(bucket, key);
            let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
            mutation_client
                .open_stream_upload_session_metadata_route(
                    self.operation_epoch(),
                    object_pg_id,
                    bucket,
                    key,
                    session_id,
                )?
                .load_session()
        })()
        .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    fn load_stream_upload_session_on_route(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        self.object_mutation_metadata_primary_client(route.bucket, route.key)?
            .open_stream_upload_session_metadata_route(
                self.operation_epoch(),
                route.object_pg_id,
                route.bucket,
                route.key,
                session_id,
            )?
            .load_session()
    }

    #[cfg(test)]
    pub(crate) fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let (target, mut segment_record) = mutation_client
            .open_stream_upload_session_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                &request.session_id,
            )?
            .prepare_segment_append(
                request,
                AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            )?;
        segment_record.placement_cluster_epoch = self.operation_epoch();
        Ok((target, segment_record))
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_append_stream_segment_with_after_prepare(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, ObjectPgActionError> {
        self.append_stream_segment_with_route_validation(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            input,
            || Ok(()),
            after_prepare,
            || Ok(()),
        )
    }

    fn append_stream_segment_with_route_validation<E>(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        input: StreamSegmentAppendInput<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut after_prepare: impl FnMut(),
        mut maintain_lease: impl FnMut() -> Result<(), E>,
    ) -> Result<StreamSegmentAppendOutcome, E>
    where
        E: From<ObjectPgActionError> + From<StoreError>,
    {
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let session = self
            .load_stream_upload_session_on_route(route, input.session_id)
            .map_err(E::from)?;
        let logical_size = if input.storage_bytes.is_empty() {
            0
        } else {
            input
                .storage_bytes
                .len()
                .checked_sub(session.encryption.segment_ciphertext_extra_len())
                .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                    reason: "encrypted stream segment shorter than authentication tag".to_string(),
                })
                .map_err(E::from)? as u64
        };
        let (target, segment_record) = self
            .prepare_stream_segment_append_with_route_validation(
                route,
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: input.session_id.clone(),
                    segment_index: input.segment_index,
                    size: logical_size,
                    segment_crc64: checksum::crc64::checksum(input.storage_bytes),
                    payload_crc64: input.payload_crc64,
                },
                &mut require_valid_route,
            )
            .map_err(E::from)?;
        after_prepare();
        maintain_lease()?;
        let written_shards = self.write_stream_segment_payload_shards_with_route_validation(
            &segment_record,
            input.storage_bytes,
            route.effect_fence,
            &mut require_valid_route,
            &mut maintain_lease,
        )?;
        if let Err(error) = maintain_lease() {
            self.delete_stream_segment_payload_shard_keys_best_effort(
                &segment_record,
                written_shards.iter().map(|written| written.key.clone()),
            );
            return Err(error);
        }
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        let request = StreamAppendCommitRequest {
            bucket: route.bucket,
            key: route.key,
            session_id: input.session_id,
            segment_index: input.segment_index,
            segment_record: &segment_record,
            shard_batch: &shard_batch,
        };
        self.commit_stream_segment_append_with_route_validation(
            route,
            request,
            &target,
            require_valid_route,
        )
        .map_err(E::from)?;
        Ok(StreamSegmentAppendOutcome {
            target,
            logical_size,
        })
    }

    fn prepare_stream_segment_append_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        request: &PrepareStreamUploadSegmentAppendReq,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let pg_id = route.object_pg_id.pg_id();
        let mut work_budget = RequestWorkBudget::new(STREAM_SEGMENT_APPEND_RETRY_BUDGET, None)
            .for_operation("prepare_stream_segment_append")
            .for_pg(pg_id);
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            recovery_authority.check("stream append preparation retry budget exhausted")?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, route.bucket)? {
                let outcome = match self.drain_pending_metadata_command_with_recovery_authority(
                    &mut recovery_authority,
                    pg_id,
                    &command,
                ) {
                    Ok(outcome) => outcome,
                    Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery) => {
                        recovery_authority.sleep_after_contention(
                            "stream append preparation pending recovery retry budget exhausted",
                        )?;
                        continue;
                    }
                    Err(error) => {
                        return Err(unrelated_pending_object_metadata_drain_error(error));
                    }
                };
                match outcome {
                    PendingMetadataCommandOutcome::Applied
                    | PendingMetadataCommandOutcome::Abandoned => {}
                    PendingMetadataCommandOutcome::PublishedPendingRecovery => {}
                    PendingMetadataCommandOutcome::TerminalCleanupPending { .. } => {}
                    PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial pending stream append preparation command",
                        ));
                    }
                }
                if !outcome.retains_pending_slot() {
                    recovery_authority.sleep_after_contention(
                        "stream append preparation contention retry budget exhausted",
                    )?;
                    continue;
                }
            }
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let mutation_client =
                self.object_mutation_metadata_primary_client(route.bucket, route.key)?;
            let (target, mut segment_record) = mutation_client
                .open_stream_upload_session_metadata_route(
                    self.operation_epoch(),
                    route.object_pg_id,
                    route.bucket,
                    route.key,
                    &request.session_id,
                )?
                .prepare_segment_append(request, route.effect_fence)?;
            segment_record.placement_cluster_epoch = self.operation_epoch();
            return Ok((target, segment_record));
        }
    }

    #[cfg(test)]
    pub(crate) fn write_stream_segment_payload_shards(
        &self,
        segment_record: &StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let ec = EcShape {
            k: segment_record.ec_k,
            m: segment_record.ec_m,
        };
        self.write_placed_segment_payload_shards(
            self.validated_data_pg(PgId::new(segment_record.data_pg_id))?,
            ec,
            &segment_record.segment_okh,
            segment_record.segment_vid,
            data,
        )
    }

    fn write_stream_segment_payload_shards_with_route_validation<E>(
        &self,
        segment_record: &StreamUploadSegmentRecord,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        maintain_lease: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Vec<WrittenShardAck>, E>
    where
        E: From<StoreError>,
    {
        if segment_record.placement_cluster_epoch != self.operation_epoch() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "write put object stream segment from another placement epoch",
            }
            .into());
        }
        let ec = EcShape {
            k: segment_record.ec_k,
            m: segment_record.ec_m,
        };
        self.write_placed_segment_payload_shards_with_route_validation(
            self.validated_data_pg(PgId::new(segment_record.data_pg_id))
                .map_err(E::from)?,
            ec,
            PlacedSegmentPayloadWrite {
                segment_okh: &segment_record.segment_okh,
                segment_vid: segment_record.segment_vid,
                data,
            },
            Some(effect_fence),
            &mut require_valid_route,
            maintain_lease,
        )
    }

    #[cfg(test)]
    pub(crate) fn commit_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let request = StreamAppendCommitRequest {
            bucket,
            key,
            session_id,
            segment_index,
            segment_record,
            shard_batch,
        };
        self.commit_stream_segment_append_with_work_budget(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            request,
            None,
            || Ok(()),
            RequestWorkBudget::new(STREAM_SEGMENT_APPEND_RETRY_BUDGET, None)
                .for_operation("commit_stream_segment_append")
                .for_pg(PgId::new(self.object_metadata_pg_id(bucket, key))),
        )
    }

    fn commit_stream_segment_append_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        request: StreamAppendCommitRequest<'_>,
        target: &StreamUploadTarget,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        self.commit_stream_segment_append_with_work_budget(
            route,
            request,
            Some(target),
            require_valid_route,
            RequestWorkBudget::new(STREAM_SEGMENT_APPEND_RETRY_BUDGET, None)
                .for_operation("commit_stream_segment_append")
                .for_pg(route.object_pg_id.pg_id()),
        )
    }

    #[cfg(test)]
    fn test_commit_stream_segment_append_with_max_attempts(
        &self,
        request: StreamAppendCommitRequest<'_>,
        max_attempts: usize,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(request.bucket, request.key));
        self.commit_stream_segment_append_with_work_budget(
            PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(request.bucket),
                object_pg_id: self.object_metadata_pg(request.bucket, request.key),
                bucket: request.bucket,
                key: request.key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            request,
            None,
            || Ok(()),
            RequestWorkBudget::new(STREAM_SEGMENT_APPEND_RETRY_BUDGET, Some(max_attempts))
                .for_operation("commit_stream_segment_append")
                .for_pg(pg_id),
        )
    }

    fn commit_stream_segment_append_with_work_budget(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        request: StreamAppendCommitRequest<'_>,
        target: Option<&StreamUploadTarget>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut work_budget: RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(CommitStreamSegmentAppend);
        let StreamAppendCommitRequest {
            bucket,
            key,
            session_id,
            segment_index,
            segment_record,
            shard_batch,
        } = request;
        if bucket != route.bucket || key != route.key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "commit put object stream segment append",
                },
            ));
        }
        let object_pg_id = route.object_pg_id;
        let pg_id = object_pg_id.pg_id();
        let ec = EcShape {
            k: segment_record.ec_k,
            m: segment_record.ec_m,
        };
        let mut payload_cleanup = StreamAppendPayloadCleanup::EagerAllowed;
        macro_rules! cleanup_stream_append_payload {
            () => {
                match payload_cleanup {
                    StreamAppendPayloadCleanup::EagerAllowed => self
                        .delete_stream_segment_payload_shard_keys_best_effort(
                            segment_record,
                            shard_batch.iter().map(|(key, _)| (*key).clone()),
                        ),
                    StreamAppendPayloadCleanup::ReferenceCheckRequired => self
                        .delete_stream_append_payload_if_unreferenced_best_effort(
                            pg_id,
                            segment_record,
                            shard_batch.iter().map(|(key, _)| (*key).clone()),
                        ),
                }
            };
        }
        let mutation_client = match self.object_mutation_metadata_primary_client(bucket, key) {
            Ok(client) => client,
            Err(error) => {
                cleanup_stream_append_payload!();
                return Err(error.into());
            }
        };
        let stream_route = match mutation_client.open_stream_upload_session_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
            session_id,
        ) {
            Ok(route) => route,
            Err(error) => {
                cleanup_stream_append_payload!();
                return Err(error);
            }
        };
        let loaded_target;
        let target = match target {
            Some(target) => target,
            None => match stream_route.load_session() {
                Ok(session) => {
                    loaded_target = session.target;
                    &loaded_target
                }
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            },
        };
        loop {
            if let Err(error) = require_valid_route() {
                cleanup_stream_append_payload!();
                return Err(ObjectPgActionError::Store(error));
            }
            if let Err(error) = work_budget.check("stream append metadata retry budget exhausted") {
                cleanup_stream_append_payload!();
                return Err(ObjectPgActionError::Store(error));
            }
            let pending = match self.pending_metadata_command_for_bucket(pg_id, bucket) {
                Ok(pending) => pending,
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(ObjectPgActionError::Store(error));
                }
            };
            if let Some(command) = pending.filter(|command| command.bucket_name() == bucket) {
                // Once another command is visible, it may be an idempotent
                // reissue of this logical segment and may publish these exact
                // shard keys. Any later cleanup must first resolve whether the
                // payload is now referenced.
                payload_cleanup = StreamAppendPayloadCleanup::ReferenceCheckRequired;
                match self.drain_pending_object_metadata_command(publisher, pg_id, &command) {
                    Ok(_) => {}
                    Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery) => {
                        if let Err(error) = work_budget.sleep_after_contention(
                            "stream append pending recovery retry budget exhausted",
                        ) {
                            cleanup_stream_append_payload!();
                            return Err(ObjectPgActionError::Store(error));
                        }
                        continue;
                    }
                    Err(error) => {
                        cleanup_stream_append_payload!();
                        return Err(error);
                    }
                }
                if let Err(error) = work_budget
                    .sleep_after_contention("stream append pending drain retry budget exhausted")
                {
                    cleanup_stream_append_payload!();
                    return Err(ObjectPgActionError::Store(error));
                }
                continue;
            }
            let existing_stream_segment = match stream_route.load_segments() {
                Ok(segments) => segments
                    .into_iter()
                    .find(|segment| segment.segment_index == segment_index),
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            };
            match existing_stream_segment {
                Some(existing) if existing == *segment_record => return Ok(()),
                Some(_) => {
                    cleanup_stream_append_payload!();
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: format!("duplicate segment_index {segment_index}"),
                    });
                }
                None => {}
            }

            self.maybe_run_before_stream_append_command_id_hook();
            if let Err(error) = require_valid_route() {
                cleanup_stream_append_payload!();
                return Err(ObjectPgActionError::Store(error));
            }
            if let Err(error) =
                self.register_payload_shard_acks(segment_record.data_pg_id, shard_batch)
            {
                cleanup_stream_append_payload!();
                return Err(error);
            }
            if let Err(error) = self.validate_payload_shard_acks(
                segment_record.data_pg_id,
                ec,
                &segment_record.segment_okh,
                segment_record.segment_vid,
                shard_batch,
            ) {
                cleanup_stream_append_payload!();
                return Err(error);
            }

            // From this point another caller can consume the selected log
            // index, publish this exact logical segment, and clear its pending
            // slot before our install result is visible. No install outcome
            // can prove that these shard keys remain exclusively ours.
            payload_cleanup = StreamAppendPayloadCleanup::ReferenceCheckRequired;
            self.maybe_run_before_metadata_command_pending_install_hook();
            if let Err(error) = require_valid_route() {
                cleanup_stream_append_payload!();
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match self.install_apply_validated_metadata_command_with_fresh_id(
                publisher,
                pg_id,
                bucket,
                false,
                Some(route.effect_fence),
                |command_id| {
                    self.maybe_run_after_stream_append_command_id_allocated_hook(command_id);
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::AppendStreamSegment(Box::new(
                            AppendStreamSegmentCommand {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                target: target.clone(),
                                segment: segment_record.clone(),
                            },
                        )),
                    )
                },
            ) {
                Ok(ApplyValidatedFreshInstallOutcome::Installed(command)) => *command,
                Ok(ApplyValidatedFreshInstallOutcome::PendingContenderDrained) => {
                    if let Err(error) = work_budget
                        .sleep_after_contention("stream append pending retry budget exhausted")
                    {
                        cleanup_stream_append_payload!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    continue;
                }
                Ok(ApplyValidatedFreshInstallOutcome::LogConflictHandled) => {
                    if let Err(error) = work_budget
                        .sleep_after_contention("stream append log conflict retry budget exhausted")
                    {
                        cleanup_stream_append_payload!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    continue;
                }
                Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery) => {
                    if let Err(error) = work_budget.sleep_after_contention(
                        "stream append install pending recovery retry budget exhausted",
                    ) {
                        cleanup_stream_append_payload!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    continue;
                }
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            };
            let apply_outcome = match self.apply_new_stream_append_command(
                pg_id,
                bucket,
                &command,
                &mut work_budget,
            ) {
                Ok(outcome) => outcome,
                Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery) => {
                    if let Err(error) = work_budget.sleep_after_contention(
                        "stream append apply pending recovery retry budget exhausted",
                    ) {
                        cleanup_stream_append_payload!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    continue;
                }
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            };
            match apply_outcome {
                StreamAppendCommandApplyOutcome::Applied => return Ok(()),
                StreamAppendCommandApplyOutcome::RetryFromFreshSnapshot => {
                    payload_cleanup = StreamAppendPayloadCleanup::ReferenceCheckRequired;
                    if let Err(error) = work_budget.sleep_after_contention(
                        "stream append fresh snapshot retry budget exhausted",
                    ) {
                        cleanup_stream_append_payload!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    continue;
                }
            }
        }
    }

    fn register_payload_shard_acks(
        &self,
        data_pg_id: u32,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .register_shard_acks(shard_batch)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_register_payload_shard_acks(
        &self,
        data_pg_id: u32,
        written_shards: &[WrittenShardAck],
    ) -> Result<(), ObjectPgActionError> {
        let shard_batch: Vec<_> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(data_pg_id, &shard_batch)
    }

    fn validate_payload_shard_acks(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let expected_keys = Self::payload_shard_set_keys(segment_okh, segment_vid, ec);
        if shard_batch.len() != expected_keys.len() {
            return Err(ObjectPgActionError::Store(
                StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "expected {} shards for EC {}+{}, got {}",
                        expected_keys.len(),
                        ec.k,
                        ec.m,
                        shard_batch.len()
                    ),
                },
            ));
        }
        for (expected_key, (actual_key, _)) in expected_keys.iter().zip(shard_batch.iter()) {
            if expected_key != *actual_key {
                return Err(ObjectPgActionError::Store(
                    StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "expected shard {} at index {}, got {}",
                            expected_key,
                            expected_key.shard_index().get(),
                            actual_key
                        ),
                    },
                ));
            }
        }

        let data_pg = self.validated_data_pg(PgId::new(data_pg_id))?;
        let shard_ack_route = self.metadata_pg_primary_shard_ack_route(data_pg)?;
        for (key, ack) in shard_batch {
            shard_ack_route.validate_shard_ack(key, *ack)?;
        }

        let reader = self
            .current_placed_segment_shard_reader(
                data_pg_id,
                ec,
                segment_okh,
                segment_vid,
            )
            .map_err(ObjectPgActionError::Store)?;
        for (key, ack) in shard_batch {
            reader
                .read(usize::from(key.shard_index().get()), *ack)
                .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
        }
        Ok(())
    }

}
