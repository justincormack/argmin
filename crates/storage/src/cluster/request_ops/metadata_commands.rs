// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl super::StorageCluster {
    fn open_bucket_write_reservation_route<'a>(
        &self,
        client: &'a dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn BucketWriteReservationRoute + 'a>, BucketSnapshotLoadError> {
        client.open_bucket_write_reservation_route(self.operation_epoch(), pg_id, bucket)
    }

    fn open_bucket_write_reservation_scan_route<'a>(
        &self,
        client: &'a dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
    ) -> Result<Box<dyn BucketWriteReservationScanRoute + 'a>, BucketSnapshotLoadError> {
        client.open_bucket_write_reservation_scan_route(self.operation_epoch(), pg_id)
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_metadata_command_apply_hook(
        &self,
        hook: MetadataCommandApplyTestHook,
    ) -> MetadataCommandApplyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_METADATA_COMMAND_APPLY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandApplyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_abort_multipart_pending_install_hook(
        &self,
        hook: AbortMultipartPendingInstallTestHook,
    ) -> AbortMultipartPendingInstallTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        AbortMultipartPendingInstallTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_stream_put_create_pending_install_hook(
        &self,
        hook: StreamPutCreatePendingInstallTestHook,
    ) -> StreamPutCreatePendingInstallTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutCreatePendingInstallTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_stream_put_create_command_id_hook(
        &self,
        hook: StreamPutCreateCommandIdTestHook,
    ) -> StreamPutCreateCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutCreateCommandIdTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_stream_put_finalize_command_id_hook(
        &self,
        hook: StreamPutFinalizeCommandIdTestHook,
    ) -> StreamPutFinalizeCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutFinalizeCommandIdTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_bucket_delete_command_id_hook(
        &self,
        hook: BucketDeleteCommandIdTestHook,
    ) -> BucketDeleteCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteCommandIdTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_bucket_delete_final_visibility_hook(
        &self,
        hook: BucketDeleteFinalVisibilityStartTestHook,
    ) -> BucketDeleteFinalVisibilityStartTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_BUCKET_DELETE_FINAL_VISIBILITY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteFinalVisibilityStartTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_bucket_delete_final_visibility_proven_hook(
        &self,
        hook: BucketDeleteFinalVisibilityProvenTestHook,
    ) -> BucketDeleteFinalVisibilityProvenTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROVEN_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteFinalVisibilityProvenTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_bucket_delete_reservation_wait_ready_hook(
        &self,
        hook: BucketDeleteReservationWaitReadyTestHook,
    ) -> BucketDeleteReservationWaitReadyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_RESERVATION_WAIT_READY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteReservationWaitReadyTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_bucket_delete_post_reservation_progress_hook(
        &self,
        hook: BucketDeletePostReservationProgressTestHook,
    ) -> BucketDeletePostReservationProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_POST_RESERVATION_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeletePostReservationProgressTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_bucket_delete_semantic_post_reservation_progress_hook(
        &self,
        hook: BucketDeleteSemanticPostReservationProgressTestHook,
    ) -> BucketDeletePostReservationProgressTestHookGuard {
        let last_object_pg_id = self.metadata_pg_ids().into_iter().max();
        self.test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                let more_frontiers_remain = last_object_pg_id
                    .is_some_and(|last_object_pg_id| next_object_pg_id <= last_object_pg_id);
                hook(more_frontiers_remain)
            },
        ))
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_bucket_delete_stream_cleanup_progress_hook(
        &self,
        hook: BucketDeleteStreamCleanupProgressTestHook,
    ) -> BucketDeleteStreamCleanupProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_STREAM_CLEANUP_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteStreamCleanupProgressTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_bucket_delete_final_visibility_progress_hook(
        &self,
        hook: BucketDeleteFinalVisibilityProgressTestHook,
    ) -> BucketDeleteFinalVisibilityProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteFinalVisibilityProgressTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_bucket_delete_exact_drain_progress_hook(
        &self,
        hook: BucketDeleteExactDrainProgressTestHook,
    ) -> BucketDeleteExactDrainProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_EXACT_DRAIN_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteExactDrainProgressTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_bucket_delete_exact_drain_hook(
        &self,
        hook: BucketDeleteExactDrainStartTestHook,
    ) -> BucketDeleteExactDrainStartTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_BUCKET_DELETE_EXACT_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteExactDrainStartTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_multipart_completion_barrier_command_id_hook(
        &self,
        hook: MultipartCompletionBarrierCommandIdTestHook,
    ) -> MultipartCompletionBarrierCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_MULTIPART_COMPLETION_BARRIER_COMMAND_ID_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionBarrierCommandIdTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_multipart_completion_stale_retry_hook(
        &self,
        hook: MultipartCompletionStaleRetryTestHook,
    ) -> MultipartCompletionStaleRetryTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            MULTIPART_COMPLETION_STALE_RETRY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionStaleRetryTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_metadata_command_apply_context_hook(
        &self,
        hook: MetadataCommandApplyContextTestHook,
    ) -> MetadataCommandApplyContextTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandApplyContextTestHookGuard { scope_id }
    }

    fn metadata_command_apply_test_hook_scope_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.local_map) as usize
    }

    pub(crate) fn metadata_pg_ids(&self) -> Vec<u32> {
        let mut pg_ids = self.local_map.pg_ids().to_vec();
        pg_ids.sort_unstable();
        pg_ids
    }

    fn terminal_bucket_delete_post_reservation_next_object_pg_id(&self) -> u32 {
        self.metadata_pg_ids()
            .into_iter()
            .max()
            .map_or(0, |pg_id| pg_id.saturating_add(1))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.test_node().get_pg(pg_id)
    }

    fn list_objects_page(
        &self,
        pg_id: u32,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_read_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_objects_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn list_object_versions_page(
        &self,
        pg_id: u32,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_read_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_object_versions_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn list_multipart_uploads_page(
        &self,
        pg_id: u32,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_read_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_multipart_uploads_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn try_probe_bucket_pg_available(
        &self,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_probe_bucket_pg_available(bucket)
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn try_probe_object_pg_available(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_probe_object_pg_available(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_bucket_with_config_and_load_info(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, crate::BucketSnapshotLoadFailure> {
        self.create_bucket_with_config_and_load_info_raw(config)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn create_bucket_with_config_and_load_info_raw(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        self.create_bucket_with_config_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(&bucket),
                bucket: &bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            config,
        )
    }

    pub(super) fn create_bucket_with_config_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(CreateBucket);
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket: routed_bucket,
            effect_fence,
        } = route;
        if bucket != *routed_bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "create bucket",
            }
            .into());
        }
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = primary_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, &bucket)?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("create_bucket_metadata")
        .for_pg(pg_id);
        loop {
            work_budget.check("create bucket metadata command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = match self
                .pending_metadata_command_for_bucket(pg_id, &bucket)?
            {
                Some(command) => {
                    if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        &bucket,
                        &command,
                        &mut work_budget,
                    )? {
                        continue;
                    }
                    match command.payload() {
                        MetadataCommandPayload::CreateBucket(create)
                            if create.matches_create_config(config) =>
                        {
                            (command, false)
                        }
                        _ => {
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                &bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                    }
                }
                None => {
                    require_valid_route()?;
                    match metadata_route.head_bucket_raw() {
                        Ok(info) => {
                            return Ok(BucketCreateAttemptOutcome::Exists(info));
                        }
                        Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                            ..
                        })) => {}
                        Err(other) => return Err(other),
                    }
                    let Some(command_id) = self
                        .next_bucket_metadata_command_id_or_drain_with_work_budget(
                            pg_id,
                            &bucket,
                            &mut work_budget,
                        )?
                    else {
                        continue;
                    };
                    require_valid_route()?;
                    let command =
                        match metadata_route.build_create_bucket_command(command_id, config)? {
                            CreateBucketCommandBuild::Exists(info) => {
                                return Ok(BucketCreateAttemptOutcome::Exists(info));
                            }
                            CreateBucketCommandBuild::Command(command) => *command,
                        };
                    match self.install_apply_validated_bucket_pg_command_or_retry(
                        publisher,
                        pg_id,
                        &bucket,
                        &command,
                        Some(effect_fence),
                        &mut work_budget,
                    )? {
                        super::ApplyValidatedPendingInstallOutcome::Installed => {}
                        super::ApplyValidatedPendingInstallOutcome::RetryAfterContention => {
                            continue;
                        }
                    }
                    (command, true)
                }
            };
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut work_budget,
                )?;
            match outcome {
                FinishPendingMetadataCommandResult::Applied => {}
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    return Err(conflicting_pending_metadata_command(
                        "retryable partial pending create bucket command",
                    ));
                }
                FinishPendingMetadataCommandResult::Abandoned => continue,
            }

            let info = metadata_route.head_bucket_info()?;
            return Ok(BucketCreateAttemptOutcome::Created(info));
        }
    }

    pub(super) fn apply_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_with_reservation_authority(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
            reservation_authority,
        )
    }

    pub(super) fn apply_reissued_metadata_command_to_acting_set_for_recovery(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                Some(authorized_source),
                abandoned_source,
            ),
            reservation_authority,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_for_recovery(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(recovery_proof, None, None),
            reservation_authority,
        )
    }

    #[cfg(test)]
    pub(super) fn test_apply_metadata_command_to_acting_set_for_recovery(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.test_apply_metadata_command_to_acting_set_for_recovery_under_leader(
            command,
            command,
            reservation_authority,
        )
    }

    #[cfg(test)]
    pub(super) fn test_apply_metadata_command_to_acting_set_for_recovery_under_leader(
        &self,
        leader_command: &MetadataCommandEnvelope,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let guard = match self
            .local_map
            .runtime_state()
            .join_metadata_command_recovery(leader_command.id().pg_id(), leader_command)
        {
            MetadataCommandRecoveryAdmission::Leader(guard) => guard,
            admission => panic!("test recovery leader must be uncontended: {admission:?}"),
        };
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("test_recovery_apply")
            .for_pg(leader_command.id().pg_id());
        let mut recovery = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery);
        let leader = authority
            .admit_leader(guard, leader_command.id().pg_id(), leader_command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        self.apply_metadata_command_to_acting_set_for_recovery(
            leader.proof(),
            command,
            reservation_authority,
        )
    }

    #[cfg(test)]
    pub(super) fn test_apply_recovery_derivative_without_predecessor(
        &self,
        leader_command: &MetadataCommandEnvelope,
        derivative: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = leader_command.id().pg_id();
        let guard = match self
            .local_map
            .runtime_state()
            .join_metadata_command_recovery(pg_id, leader_command)
        {
            MetadataCommandRecoveryAdmission::Leader(guard) => guard,
            admission => panic!("test recovery leader must be uncontended: {admission:?}"),
        };
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("test_recovery_derivative")
            .for_pg(pg_id);
        let mut recovery = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery);
        let leader = authority
            .admit_leader(guard, pg_id, leader_command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let derivative_proof = leader
            .proof()
            .derive_reissue(pg_id, leader_command, derivative)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        self.apply_metadata_command_to_acting_set_with_route_mode(
            derivative,
            MetadataCommandExecutionRoute::recovery(derivative_proof, Some(leader_command), None),
            reservation_authority,
        )
    }

    fn apply_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let route_mode = execution_route.mode;
        let pg_id = command.id().pg_id();
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            primary_node_id,
            command,
            execution_route,
            reservation_authority,
        )
    }

    fn apply_metadata_command_to_acting_set_from_origin_with_route_mode(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let route_mode = execution_route.mode;
        let authorized_source = execution_route.recovery_authorized_source;
        let abandoned_source = execution_route.recovery_abandoned_source;
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        if origin_node_id != primary_node_id {
            return Err(MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: StoreError::MetadataCommandFromNonPrimary {
                    node_id: primary_node_id.as_u32(),
                    pg_id: pg_id.get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    origin_node_id: origin_node_id.as_u32(),
                    primary_node_id: primary_node_id.as_u32(),
                }
                .into(),
            });
        }
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        // Fanout is primary-first. Once the primary has durably applied this
        // exact command, that log entry is the admission witness for replica
        // completion even if the original reservation expires meanwhile.
        let mut admission_witnessed = false;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                if let Some(source) = authorized_source {
                    let primary_critical_section = node
                        .metadata_command_recovery_client()
                        .open_metadata_command_recovery_critical_section(
                            pg_id,
                            command.id().cluster_epoch(),
                        )
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    let metadata_client = primary_critical_section.as_ref();
                    let acceptance = metadata_client
                        .metadata_command_acceptance(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                        metadata_client
                            .apply_metadata_command_and_record_for_recovery(
                                source,
                                abandoned_source,
                                command,
                            )
                            .map_err(|source| MetadataCommandApplyFailure {
                                applied_nodes,
                                source,
                            })?;
                        admission_witnessed = true;
                        continue;
                    }
                    reservation_authority
                        .validate_metadata_command_bucket_write_reservation(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                    maybe_run_before_metadata_command_apply_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        node.node_id(),
                        command,
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    })?;
                    metadata_client
                        .apply_metadata_command_and_record_for_recovery(
                            source,
                            abandoned_source,
                            command,
                        )
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                } else {
                    let primary_critical_section = node
                        .metadata_command_client()
                        .open_metadata_command_critical_section(pg_id, command.id().cluster_epoch())
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    let metadata_client = primary_critical_section.as_ref();
                    let acceptance = metadata_client
                        .metadata_command_acceptance(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                        metadata_client
                            .apply_metadata_command_and_record(command)
                            .map_err(|source| MetadataCommandApplyFailure {
                                applied_nodes,
                                source,
                            })?;
                        admission_witnessed = true;
                        continue;
                    }
                    reservation_authority
                        .validate_metadata_command_bucket_write_reservation(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                    maybe_run_before_metadata_command_apply_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        node.node_id(),
                        command,
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    })?;
                    metadata_client
                        .apply_metadata_command_and_record(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                }
                admission_witnessed = true;
                continue;
            }

            debug_assert!(
                admission_witnessed,
                "metadata primary must be visited first"
            );
            let acceptance = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.local_map.validate_metadata_command_for_replica(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                    )
                }
                MetadataCommandRouteMode::Recovery => self
                    .local_map
                    .validate_metadata_command_for_replica_for_metadata_command_recovery(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                    ),
            }
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                let apply = match authorized_source {
                    Some(source) => node
                        .metadata_command_recovery_client()
                        .open_metadata_command_recovery_replica_apply_route(
                            pg_id,
                            command.id().cluster_epoch(),
                            source,
                            abandoned_source,
                            command,
                        )
                        .map_err(BucketSnapshotLoadError::Store)
                        .and_then(|route| route.apply()),
                    None => node
                        .metadata_command_client()
                        .apply_metadata_command_and_record(pg_id, command),
                };
                apply.map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source,
                })?;
                continue;
            }
            maybe_run_before_metadata_command_apply_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                node.node_id(),
                command,
            )
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
            let apply = match authorized_source {
                Some(source) => node
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_replica_apply_route(
                        pg_id,
                        command.id().cluster_epoch(),
                        source,
                        abandoned_source,
                        command,
                    )
                    .map_err(BucketSnapshotLoadError::Store)
                    .and_then(|route| route.apply()),
                None => node
                    .metadata_command_client()
                    .apply_metadata_command_and_record(pg_id, command),
            };
            apply.map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source,
            })?;
        }
        Ok(())
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
        )
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set_for_recovery(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                authorized_source,
                abandoned_source,
            ),
        )
    }

    fn record_abandoned_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source;
        let recovery_abandoned_source = execution_route.recovery_abandoned_source;
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        let mut nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                let primary_critical_section = node
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_critical_section(
                        pg_id,
                        command.id().cluster_epoch(),
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: BucketSnapshotLoadError::Store(source),
                    })?;
                let metadata_client = primary_critical_section.as_ref();
                let acceptance = metadata_client
                    .metadata_command_abandon_acceptance(command)
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: BucketSnapshotLoadError::Store(source),
                    })?;
                if acceptance != MetadataCommandAcceptance::AlreadyApplied {
                    metadata_client
                        .record_metadata_command_abandoned(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: source.into(),
                        })?;
                }
                continue;
            }
            let acceptance = self
                .local_map
                .validate_metadata_command_abandon_for_replica(
                    primary_node_id,
                    node.node_id(),
                    pg_id,
                    command,
                )
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                continue;
            }
            let result = match route_mode {
                MetadataCommandRouteMode::Normal => node
                    .metadata_command_client()
                    .record_metadata_command_abandoned_on_replica(pg_id, command),
                MetadataCommandRouteMode::Recovery => node
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_replica_abandon_route(
                        pg_id,
                        command.id().cluster_epoch(),
                        recovery_authorized_source.unwrap_or(command),
                        recovery_abandoned_source,
                        command,
                    )
                    .and_then(|route| route.record_abandoned()),
            };
            result.map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
        }
        Ok(())
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
        )
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set_for_recovery(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                authorized_source,
                abandoned_source,
            ),
        )
    }

    fn metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: source.into(),
            })?;
        let route_mode = execution_route.mode;
        self.maybe_run_before_direct_put_abandoned_log_inspection_hook(command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                source,
            })?;
        let pg_id = command.id().pg_id();
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node
                .metadata_command_inspection_client()
                .metadata_command_abandoned(pg_id, command)
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                })?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn test_apply_metadata_command_to_acting_set_from_origin(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            origin_node_id,
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
        )
        .map_err(|error| error.source)
    }

    #[cfg(test)]
    pub(super) fn finish_pending_metadata_command_to_acting_set(
        &self,
        pg_id: PgId,
        _bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("metadata_command_apply")
        .for_pg(pg_id);
        self.finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            &mut work_budget,
        )
    }

    fn finish_pending_metadata_command_to_acting_set_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        match self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            false,
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )? {
            FinishPendingMetadataCommandResult::Applied => {
                Ok(super::PendingMetadataCommandOutcome::Applied)
            }
            FinishPendingMetadataCommandResult::Abandoned => {
                Ok(super::PendingMetadataCommandOutcome::Abandoned)
            }
            FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                unreachable!("partial exact conflict retry is disabled for this caller")
            }
        }
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            true,
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            true,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                recovery_authorized_source,
                None,
            ),
            work_budget,
        )
    }

    fn finish_pending_metadata_command_to_acting_set_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        retry_partial_exact_conflict: bool,
        mut execution_route: MetadataCommandExecutionRoute<'_>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        execution_route.require_command(pg_id, command)?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source.cloned();
        let recovery_abandoned_source = execution_route.recovery_abandoned_source.cloned();
        let mut command = command.clone();
        loop {
            work_budget.check("metadata command apply retry budget exhausted")?;
            let command_bucket = command.bucket_name();
            let abandoned_on_acting_set = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.metadata_command_has_abandoned_log_on_acting_set(&command)
                }
                MetadataCommandRouteMode::Recovery => self
                    .metadata_command_has_abandoned_log_on_acting_set_for_recovery(
                        execution_route.recovery_proof(),
                        &command,
                        recovery_authorized_source.as_ref(),
                        recovery_abandoned_source.as_ref(),
                    ),
            }
            .map_err(|error| error.source)?;
            if abandoned_on_acting_set {
                match route_mode {
                    MetadataCommandRouteMode::Normal => {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                    }
                    MetadataCommandRouteMode::Recovery => self
                        .record_abandoned_metadata_command_to_acting_set_for_recovery(
                            execution_route.recovery_proof(),
                            &command,
                            recovery_authorized_source.as_ref(),
                            recovery_abandoned_source.as_ref(),
                        ),
                }
                .map_err(|error| error.source)?;
                self.release_metadata_command_bucket_write_reservation(&command)?;
                match route_mode {
                    MetadataCommandRouteMode::Normal => self
                        .remove_pending_metadata_command_for_bucket_with_work_budget(
                            pg_id,
                            command_bucket,
                            &command,
                            work_budget,
                        ),
                    MetadataCommandRouteMode::Recovery => self
                        .remove_pending_metadata_command_for_bucket_recovery(
                            execution_route,
                            pg_id,
                            command_bucket,
                            &command,
                            work_budget,
                        ),
                }?;
                return Ok(FinishPendingMetadataCommandResult::Abandoned);
            }
            let apply_result = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.apply_metadata_command_to_acting_set(&command)
                }
                MetadataCommandRouteMode::Recovery => match recovery_authorized_source.as_ref() {
                    Some(authorized_source) => self
                        .apply_reissued_metadata_command_to_acting_set_for_recovery(
                            execution_route.recovery_proof(),
                            authorized_source,
                            None,
                            &command,
                            self,
                        ),
                    None => self.apply_metadata_command_to_acting_set_for_recovery(
                        execution_route.recovery_proof(),
                        &command,
                        self,
                    ),
                },
            };
            match apply_result {
                Ok(()) => {
                    if self
                        .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                            pg_id, &command,
                        )?
                    {
                        match route_mode {
                            MetadataCommandRouteMode::Normal => self
                                .remove_pending_metadata_command_for_bucket_with_work_budget(
                                    pg_id,
                                    command_bucket,
                                    &command,
                                    work_budget,
                                ),
                            MetadataCommandRouteMode::Recovery => self
                                .remove_pending_metadata_command_for_bucket_recovery(
                                    execution_route,
                                    pg_id,
                                    command_bucket,
                                    &command,
                                    work_budget,
                                ),
                        }?;
                    }
                    return Ok(FinishPendingMetadataCommandResult::Applied);
                }
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        source,
                    } = error;
                    if metadata_command_apply_transport_error_is_retryable(&source) {
                        work_budget.sleep_after_contention(
                            "metadata command transport retry budget exhausted",
                        )?;
                        continue;
                    }
                    if retry_partial_exact_conflict
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let exact_conflict_retryable = applied_nodes == 0
                            || self
                                .partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
                                    pg_id,
                                    &command,
                                    applied_nodes,
                                    &source,
                                    route_mode,
                                )?;
                        if exact_conflict_retryable
                            && self
                                .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                                    pg_id, &command, route_mode,
                                )?
                        {
                            if self
                                .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                    pg_id, &command,
                                )?
                            {
                                match route_mode {
                                    MetadataCommandRouteMode::Normal => self
                                        .remove_pending_metadata_command_for_bucket_with_work_budget(
                                            pg_id,
                                            command_bucket,
                                            &command,
                                            work_budget,
                                        ),
                                    MetadataCommandRouteMode::Recovery => self
                                        .remove_pending_metadata_command_for_bucket_recovery(
                                            execution_route,
                                            pg_id,
                                            command_bucket,
                                            &command,
                                            work_budget,
                                        ),
                                }?;
                            }
                            return Ok(FinishPendingMetadataCommandResult::Applied);
                        }
                        if exact_conflict_retryable && applied_nodes > 0 {
                            return Ok(
                                FinishPendingMetadataCommandResult::RetryPartialExactConflict,
                            );
                        }
                    }
                    if applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command_with_route_mode(
                                pg_id,
                                &command,
                                execution_route,
                                command.payload(),
                            )?
                        else {
                            return Ok(FinishPendingMetadataCommandResult::Abandoned);
                        };
                        execution_route =
                            execution_route.for_reissued_command(pg_id, &command, &reissued)?;
                        command = reissued;
                        continue;
                    }
                    if clear_pending_on_zero_apply && applied_nodes == 0 {
                        match route_mode {
                            MetadataCommandRouteMode::Normal => {
                                self.record_abandoned_metadata_command_to_acting_set(&command)
                            }
                            MetadataCommandRouteMode::Recovery => self
                                .record_abandoned_metadata_command_to_acting_set_for_recovery(
                                    execution_route.recovery_proof(),
                                    &command,
                                    recovery_authorized_source.as_ref(),
                                    recovery_abandoned_source.as_ref(),
                                ),
                        }
                        .map_err(|error| error.source)?;
                        match route_mode {
                            MetadataCommandRouteMode::Normal => self
                                .remove_pending_metadata_command_for_bucket_with_work_budget(
                                    pg_id,
                                    command_bucket,
                                    &command,
                                    work_budget,
                                ),
                            MetadataCommandRouteMode::Recovery => self
                                .remove_pending_metadata_command_for_bucket_recovery(
                                    execution_route,
                                    pg_id,
                                    command_bucket,
                                    &command,
                                    work_budget,
                                ),
                        }?;
                    }
                    return Err(source);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn drain_bucket_pg_pending_metadata_command(
        &self,
        pg_id: PgId,
        _bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("metadata_command_pg_slot_drain")
        .for_pg(pg_id);
        self.drain_bucket_pg_pending_metadata_command_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            &mut work_budget,
        )
    }

    fn drain_bucket_pg_pending_metadata_command_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        self.emit_pending_slot_action_for_command(pg_id, command, "drain_attempt");
        self.finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            work_budget,
        )
    }

    fn metadata_command_bucket_name(command: &MetadataCommandEnvelope) -> &BucketName {
        command.bucket_name()
    }

    fn drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pending_bucket = Self::metadata_command_bucket_name(command).clone();
        if pending_bucket == *bucket {
            return Ok(false);
        }
        self.drain_pending_metadata_command_pg_slot_with_work_budget(
            pg_id,
            &pending_bucket,
            command,
            work_budget,
        )?;
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn drain_pending_metadata_command_pg_slot(
        &self,
        pg_id: PgId,
        _pending_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let _ = self
            .drain_pending_metadata_command_with_recovery_gate(pg_id, command)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        Ok(())
    }

    fn drain_pending_metadata_command_pg_slot_with_work_budget(
        &self,
        pg_id: PgId,
        _pending_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketSnapshotLoadError> {
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    command,
                    false,
                    work_budget,
                )?;
            return match outcome {
                FinishPendingMetadataCommandResult::Applied
                | FinishPendingMetadataCommandResult::Abandoned => Ok(()),
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    Err(conflicting_pending_metadata_command(
                        "retryable partial pending metadata command drain",
                    ))
                }
            };
        }

        let mut recovery_authority = super::MetadataCommandRecoveryDrainAuthority::new(work_budget);
        let _ = self
            .drain_pending_metadata_command_with_recovery_authority(
                &mut recovery_authority,
                pg_id,
                command,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        Ok(())
    }

    fn drain_pending_multipart_completion_barrier_command_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketSnapshotLoadError> {
        let _ = self.drain_bucket_pg_pending_metadata_command_with_work_budget(
            pg_id,
            command,
            false,
            work_budget,
        )?;
        Ok(())
    }

    fn next_bucket_metadata_command_id_or_drain_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        self.next_bucket_metadata_command_id_or_drain_inner(pg_id, bucket, false, work_budget)
    }

    fn next_completion_bucket_metadata_command_id_or_drain_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        self.next_bucket_metadata_command_id_or_drain_inner(pg_id, bucket, true, work_budget)
    }

    fn next_bucket_metadata_command_id_or_drain_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        let command_id_result = if completion_admission {
            self.next_completion_bucket_metadata_command_id(pg_id)
        } else {
            self.next_bucket_metadata_command_id(pg_id)
        };
        match command_id_result {
            Ok(command_id) => Ok(Some(command_id)),
            Err(BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                ..
            })) => {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&command).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &command,
                        work_budget,
                    )?;
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        if effect_fence.is_some() {
            self.maybe_run_before_metadata_command_pending_install_hook();
        }
        match self.try_set_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(Some(())) => Ok(true),
            Ok(None) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn install_allocator_cleanup_bucket_pg_command_or_retry(
        &self,
        _publisher: impl crate::metadata_command::AllocatorCleanupMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::AllocatorCleanupPendingInstallOutcome, BucketSnapshotLoadError> {
        if self.try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
            work_budget,
        )? {
            Ok(super::AllocatorCleanupPendingInstallOutcome::Installed)
        } else {
            Ok(super::AllocatorCleanupPendingInstallOutcome::RetryAfterContention)
        }
    }

    fn install_snapshot_sensitive_bucket_pg_command_or_drain(
        &self,
        _publisher: impl crate::metadata_command::SnapshotSensitiveMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::SnapshotSensitiveInstallOutcome, BucketSnapshotLoadError> {
        if self.try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
            work_budget,
        )? {
            Ok(super::SnapshotSensitiveInstallOutcome::Installed)
        } else {
            Ok(super::SnapshotSensitiveInstallOutcome::ContenderDrained)
        }
    }

    fn install_apply_validated_bucket_pg_command_or_retry(
        &self,
        _publisher: impl crate::metadata_command::ApplyValidatedMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::ApplyValidatedPendingInstallOutcome, BucketSnapshotLoadError> {
        if self.try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
            work_budget,
        )? {
            Ok(super::ApplyValidatedPendingInstallOutcome::Installed)
        } else {
            Ok(super::ApplyValidatedPendingInstallOutcome::RetryAfterContention)
        }
    }

    #[cfg(test)]
    pub(super) fn try_set_bucket_control_pending_command_or_retry(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        match primary
            .metadata_command_client()
            .try_insert_bucket_control_pending_metadata_command_slot(pg_id, command, bucket)
        {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, &pending)?;
                    return Ok(false);
                }

                if self
                    .open_bucket_write_reservation_route(
                        primary.bucket_write_reservation_client().as_ref(),
                        self.validated_bucket_metadata_pg(pg_id),
                        bucket,
                    )?
                    .durable_bucket_write_drain_exists()?
                {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    return Ok(false);
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, &pending)?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn try_set_bucket_control_pending_command_or_retry_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.maybe_run_before_metadata_command_pending_install_hook();
        let insert = match effect_fence {
            Some(effect_fence) => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
                    pg_id,
                    command,
                    bucket,
                    effect_fence,
                ),
            None => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot(pg_id, command, bucket),
        };
        match insert {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                    return Ok(false);
                }

                if self
                    .open_bucket_write_reservation_route(
                        primary.bucket_write_reservation_client().as_ref(),
                        self.validated_bucket_metadata_pg(pg_id),
                        bucket,
                    )?
                    .durable_bucket_write_drain_exists()?
                {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    return Ok(false);
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn install_snapshot_sensitive_bucket_control_command_or_drain(
        &self,
        _publisher: impl crate::metadata_command::SnapshotSensitiveMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::SnapshotSensitiveInstallOutcome, BucketSnapshotLoadError> {
        if self.try_set_bucket_control_pending_command_or_retry_with_work_budget(
            pg_id,
            bucket,
            command,
            effect_fence,
            work_budget,
        )? {
            Ok(super::SnapshotSensitiveInstallOutcome::Installed)
        } else {
            Ok(super::SnapshotSensitiveInstallOutcome::ContenderDrained)
        }
    }

    fn finish_pending_command_for_multipart_completion_barrier(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, ObjectPgActionError> {
        match command.payload() {
            MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::ReleaseObjectGeneration(_)
            | MetadataCommandPayload::ReserveObjectVersion(_)
            | MetadataCommandPayload::CommitDirectPutObject(_)
            | MetadataCommandPayload::CommitMultipartObject(_)
            | MetadataCommandPayload::DeleteObjectVersion(_)
            | MetadataCommandPayload::InsertDeleteMarker(_)
            | MetadataCommandPayload::PutObjectMetadata(_)
            | MetadataCommandPayload::CreateStreamUpload(_)
            | MetadataCommandPayload::AppendStreamSegment(_)
            | MetadataCommandPayload::AbortStreamUpload(_)
            | MetadataCommandPayload::CommitStreamPart(_)
            | MetadataCommandPayload::CreateMultipartUpload(_)
            | MetadataCommandPayload::AbortMultipartUpload(_)
            | MetadataCommandPayload::DeleteObjectPayloadReclaim(_) => {
                self.finish_object_pg_pending_slot(pg_id, command)
            }
            MetadataCommandPayload::CreateBucket(_)
            | MetadataCommandPayload::PutBucketVersioning(_)
            | MetadataCommandPayload::PutBucketAcl(_)
            | MetadataCommandPayload::PutBucketProperty(_)
            | MetadataCommandPayload::PutBucketSubresource(_)
            | MetadataCommandPayload::MarkBucketDeleting(_)
            | MetadataCommandPayload::DeleteFinalizedBucket(_)
            | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => self
                .drain_bucket_pg_pending_metadata_command_with_work_budget(
                    pg_id,
                    command,
                    false,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error),
        }
    }


}
