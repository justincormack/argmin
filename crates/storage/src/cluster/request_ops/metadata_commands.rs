// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[test]
    fn expired_metadata_command_pg_lock_deadline_does_not_acquire_available_lock() {
        let lock = std::sync::Mutex::new(());

        assert!(lock_metadata_command_pg_until(&lock, Instant::now()).is_none());
        assert!(lock.try_lock().is_ok());
    }
}

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

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_metadata_command_apply_hook(
        &self,
        hook: MetadataCommandAfterApplyTestHook,
    ) -> MetadataCommandAfterApplyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_METADATA_COMMAND_APPLY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandAfterApplyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_apply_attempt_hook(
        &self,
        hook: MetadataCommandApplyAttemptTestHook,
    ) -> MetadataCommandApplyAttemptTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            METADATA_COMMAND_APPLY_ATTEMPT_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandApplyAttemptTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_progress_reconstruction_hook(
        &self,
        hook: MetadataCommandProgressReconstructionTestHook,
    ) -> MetadataCommandProgressReconstructionTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = METADATA_COMMAND_PROGRESS_RECONSTRUCTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandProgressReconstructionTestHookGuard { scope_id }
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
    pub(crate) fn test_install_stream_put_pending_drain_hook(
        &self,
        hook: StreamPutPendingDrainTestHook,
    ) -> StreamPutPendingDrainTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = STREAM_PUT_PENDING_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutPendingDrainTestHookGuard { scope_id }
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
    pub(crate) fn test_install_multipart_completion_pending_barrier_observed_hook(
        &self,
        hook: MultipartCompletionPendingBarrierObservedTestHook,
    ) -> MultipartCompletionPendingBarrierObservedTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = MULTIPART_COMPLETION_PENDING_BARRIER_OBSERVED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionPendingBarrierObservedTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_pending_object_metadata_partial_conflict_hook(
        &self,
        hook: PendingObjectMetadataPartialConflictTestHook,
    ) -> PendingObjectMetadataPartialConflictTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = PENDING_OBJECT_METADATA_PARTIAL_CONFLICT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PendingObjectMetadataPartialConflictTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_post_budget_metadata_command_inspection_hook(
        &self,
        hook: PostBudgetMetadataCommandInspectionTestHook,
    ) -> PostBudgetMetadataCommandInspectionTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = POST_BUDGET_METADATA_COMMAND_INSPECTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PostBudgetMetadataCommandInspectionTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_pending_command_recovery_timeout_hook(
        &self,
        hook: PendingCommandRecoveryTimeoutTestHook,
    ) -> PendingCommandRecoveryTimeoutTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = PENDING_COMMAND_RECOVERY_TIMEOUT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PendingCommandRecoveryTimeoutTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_pending_command_recovery_waited_hook(
        &self,
        hook: PendingCommandRecoveryWaitedTestHook,
    ) -> PendingCommandRecoveryWaitedTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = PENDING_COMMAND_RECOVERY_WAITED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PendingCommandRecoveryWaitedTestHookGuard { scope_id }
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

    #[cfg(test)]
    pub(crate) fn test_install_multipart_completion_auxiliary_reservation_hook(
        &self,
        hook: MultipartCompletionAuxiliaryReservationTestHook,
    ) -> MultipartCompletionAuxiliaryReservationTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = MULTIPART_COMPLETION_AUXILIARY_RESERVATION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionAuxiliaryReservationTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_terminal_reservation_release_hook(
        &self,
        hook: MetadataCommandTerminalReservationReleaseTestHook,
    ) -> MetadataCommandTerminalReservationReleaseTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = METADATA_COMMAND_TERMINAL_RESERVATION_RELEASE_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandTerminalReservationReleaseTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_global_metadata_command_terminal_reservation_release_hook(
        &self,
        hook: MetadataCommandTerminalReservationReleaseTestHook,
    ) -> MetadataCommandTerminalReservationReleaseTestHookGuard {
        let slot = METADATA_COMMAND_TERMINAL_RESERVATION_RELEASE_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID, hook);
        MetadataCommandTerminalReservationReleaseTestHookGuard {
            scope_id: GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_global_metadata_command_terminal_slot_removal_hook(
        &self,
        hook: MetadataCommandTerminalSlotRemovalTestHook,
    ) -> MetadataCommandTerminalSlotRemovalTestHookGuard {
        let slot = METADATA_COMMAND_TERMINAL_SLOT_REMOVAL_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID, hook);
        MetadataCommandTerminalSlotRemovalTestHookGuard {
            scope_id: GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID,
        }
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

    pub(super) fn metadata_command_apply_test_hook_scope_id(&self) -> usize {
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
                FinishPendingMetadataCommandResult::Applied
                | FinishPendingMetadataCommandResult::PublishedPendingRecovery => {}
                FinishPendingMetadataCommandResult::TerminalCleanupPending {
                    applied: true,
                } => {}
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    return Err(conflicting_pending_metadata_command(
                        "retryable partial pending create bucket command",
                    ));
                }
                FinishPendingMetadataCommandResult::Abandoned
                | FinishPendingMetadataCommandResult::TerminalCleanupPending {
                    applied: false,
                } => continue,
            }

            let MetadataCommandPayload::CreateBucket(create) = command.payload() else {
                unreachable!("create bucket completed a different command kind")
            };
            return Ok(BucketCreateAttemptOutcome::Created(
                BucketMutationReceipt::new(
                    create.bucket().name.clone(),
                    create.bucket().bucket_execution_generation,
                ),
            ));
        }
    }

    pub(super) fn apply_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_initial_progress(
            command,
            MetadataCommandApplyProgress::Abortable,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_with_initial_progress(
        &self,
        command: &MetadataCommandEnvelope,
        initial_progress: MetadataCommandApplyProgress,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
            initial_progress,
        )
    }

    pub(super) fn apply_recovered_pending_metadata_command_to_acting_set_with_initial_progress(
        &self,
        command: &MetadataCommandEnvelope,
        initial_progress: MetadataCommandApplyProgress,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_recovered_pending_metadata_command_to_acting_set_with_initial_progress_until(
            command,
            initial_progress,
            Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET,
        )
    }

    pub(super) fn apply_recovered_pending_metadata_command_to_acting_set_with_initial_progress_until(
        &self,
        command: &MetadataCommandEnvelope,
        initial_progress: MetadataCommandApplyProgress,
        deadline: Instant,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode_until_inner(
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
            initial_progress,
            deadline,
            MetadataCommandApplyProgressProvenance::RecoveredPending,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_with_initial_progress_until(
        &self,
        command: &MetadataCommandEnvelope,
        initial_progress: MetadataCommandApplyProgress,
        deadline: Instant,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode_until_inner(
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
            initial_progress,
            deadline,
            MetadataCommandApplyProgressProvenance::Authoritative,
        )
    }

    fn reconstruct_pending_metadata_command_apply_progress_with_held_primary_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        primary_observation: HeldPrimaryMetadataCommandObservation,
        deadline: Instant,
    ) -> Result<Option<MetadataCommandApplyProgress>, BucketSnapshotLoadError> {
        let state = self.reconstruct_metadata_command_publication_state_with_held_primary_until(
            pg_id,
            command,
            route_mode,
            primary_observation,
            deadline,
        )?;
        match state {
            MetadataCommandPublicationState::NotPublished => {
                Ok(Some(MetadataCommandApplyProgress::Abortable))
            }
            MetadataCommandPublicationState::PublicationStarted => {
                Ok(Some(MetadataCommandApplyProgress::PublicationStarted))
            }
            MetadataCommandPublicationState::Witnessed => {
                Ok(Some(MetadataCommandApplyProgress::Witnessed))
            }
            MetadataCommandPublicationState::PublicationUnconfirmed => {
                Ok(Some(MetadataCommandApplyProgress::PublicationUnconfirmed))
            }
            MetadataCommandPublicationState::Published => {
                Ok(Some(MetadataCommandApplyProgress::Published))
            }
            MetadataCommandPublicationState::IrrevocableUnconfirmed => Ok(None),
        }
    }

    #[cfg(test)]
    pub(super) fn test_apply_metadata_command_to_acting_set_for_recovery(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
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
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
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
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(leader.proof(), None, None),
            reservation_authority,
            MetadataCommandApplyProgress::Abortable,
        )
    }

    #[cfg(test)]
    pub(super) fn test_apply_recovery_derivative_without_predecessor(
        &self,
        leader_command: &MetadataCommandEnvelope,
        derivative: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
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
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        let derivative_proof = leader
            .proof()
            .derive_reissue(pg_id, leader_command, derivative)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        self.apply_metadata_command_to_acting_set_with_route_mode(
            derivative,
            MetadataCommandExecutionRoute::recovery(derivative_proof, Some(leader_command), None),
            reservation_authority,
            MetadataCommandApplyProgress::Abortable,
        )
    }

    fn apply_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
        initial_progress: MetadataCommandApplyProgress,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode_until_inner(
            command,
            execution_route,
            reservation_authority,
            initial_progress,
            Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET,
            MetadataCommandApplyProgressProvenance::Authoritative,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_with_route_mode_until(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
        initial_progress: MetadataCommandApplyProgress,
        deadline: Instant,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode_until_inner(
            command,
            execution_route,
            reservation_authority,
            initial_progress,
            deadline,
            MetadataCommandApplyProgressProvenance::RecoveredPending,
        )
    }

    fn apply_metadata_command_to_acting_set_with_route_mode_until_inner(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
        initial_progress: MetadataCommandApplyProgress,
        deadline: Instant,
        progress_provenance: MetadataCommandApplyProgressProvenance,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: initial_progress,
                may_have_applied: false,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: initial_progress,
                may_have_applied: false,
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
            progress: initial_progress,
            may_have_applied: false,
            source: source.into(),
        })?
        .node_id();
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            primary_node_id,
            command,
            execution_route,
            reservation_authority,
            MetadataCommandApplyAttemptContext {
                progress: initial_progress,
                deadline,
                provenance: progress_provenance,
                publication_start: MetadataCommandPublicationStartPolicy::Required,
            },
        )
    }

    fn apply_metadata_command_to_acting_set_from_origin_with_route_mode(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
        attempt_context: MetadataCommandApplyAttemptContext,
    ) -> Result<MetadataCommandApplyOutcome, MetadataCommandApplyFailure> {
        let mut progress = attempt_context.progress;
        let deadline = attempt_context.deadline;
        let mut publication_may_have_applied = false;
        let mut applied_nodes = 0usize;
        let mut retries = 0usize;
        loop {
            let attempt = maybe_run_metadata_command_apply_attempt_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                command,
            )
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    0, progress, source,
                )
            })
            .and_then(|()| {
                self.apply_metadata_command_to_acting_set_once(
                    origin_node_id,
                    command,
                    execution_route,
                    reservation_authority,
                    MetadataCommandApplyAttemptContext {
                        progress,
                        deadline,
                        provenance: attempt_context.provenance,
                        publication_start: attempt_context.publication_start,
                    },
                )
            });
            match attempt {
                Ok(()) => return Ok(MetadataCommandApplyOutcome::Converged),
                Err(mut error) => {
                    publication_may_have_applied |= error.failure.may_have_applied;
                    let apply_error_kind = error.apply_error_kind;
                    let attempt_applied_nodes = error.failure.applied_nodes;
                    let observed_progress = self
                        .observe_metadata_command_apply_progress(
                            command,
                            execution_route,
                            deadline,
                        );
                    applied_nodes = applied_nodes.max(error.failure.applied_nodes);
                    progress = progress
                        .merge(error.failure.progress)
                        .merge(observed_progress);
                    error.failure.applied_nodes = applied_nodes;
                    error.failure.progress = progress;
                    error.failure.may_have_applied = publication_may_have_applied;
                    let can_handoff = metadata_command_apply_error_can_handoff_to_recovery(
                        &error.failure.source,
                    );
                    let exact_conflict_retryable = !progress.is_abortable()
                        && Self::metadata_command_log_conflict_matches(
                            command,
                            &error.failure.source,
                        )
                        && self
                            .partial_exact_metadata_command_conflict_is_retryable_with_route_mode_until(
                                command.id().pg_id(),
                                command,
                                attempt_applied_nodes,
                                &error.failure.source,
                                execution_route.mode,
                                deadline,
                            )
                            .unwrap_or(false);
                    let exact_dependency_gap = progress
                        == MetadataCommandApplyProgress::Published
                        && Self::metadata_command_log_gap_matches(
                            command,
                            &error.failure.source,
                        );
                    let definitive_apply_failure =
                        apply_error_kind == Some(MetadataCommandApplyErrorKind::Definitive)
                            && !can_handoff
                            && !exact_conflict_retryable
                            && !exact_dependency_gap;
                    if progress == MetadataCommandApplyProgress::Published
                        && (can_handoff
                            || publication_may_have_applied
                            || exact_dependency_gap)
                        && !definitive_apply_failure
                    {
                        self.emit_metadata_command_recovery_outcome_for_command(
                            command.id().pg_id(),
                            command,
                            "published_pending_recovery",
                        );
                        return Ok(MetadataCommandApplyOutcome::PublishedPendingRecovery);
                    }
                    if progress.is_abortable() {
                        return Err(error.failure);
                    }
                    if (can_handoff
                        || exact_conflict_retryable
                        || publication_may_have_applied)
                        && !definitive_apply_failure
                        && super::sleep_after_metadata_contention_retry_until(
                            "metadata_command_publication_confirmation",
                            Some(command.id().pg_id()),
                            "confirm exact witnessed metadata command publication",
                            &mut retries,
                            deadline,
                        )
                    {
                        continue;
                    }
                    if progress == MetadataCommandApplyProgress::Published
                        && exact_conflict_retryable
                    {
                        self.emit_metadata_command_recovery_outcome_for_command(
                            command.id().pg_id(),
                            command,
                            "published_pending_recovery",
                        );
                        return Ok(MetadataCommandApplyOutcome::PublishedPendingRecovery);
                    }
                    if matches!(
                        progress,
                        MetadataCommandApplyProgress::PublicationStarted
                            | MetadataCommandApplyProgress::Witnessed
                            | MetadataCommandApplyProgress::PublicationUnconfirmed
                    ) && publication_may_have_applied
                        && !definitive_apply_failure
                    {
                        let id = command.id();
                        self.emit_metadata_command_recovery_outcome_for_command(
                            id.pg_id(),
                            command,
                            "publication_unconfirmed",
                        );
                        error.failure.source = StoreError::MetadataCommandOutcomeUnconfirmed {
                            pg_id: id.pg_id().get(),
                            cluster_epoch: id.cluster_epoch(),
                            log_index: id.log_index().get(),
                        }
                        .into();
                    } else if matches!(
                        progress,
                        MetadataCommandApplyProgress::PublicationStarted
                            | MetadataCommandApplyProgress::Witnessed
                            | MetadataCommandApplyProgress::PublicationUnconfirmed
                    ) && (can_handoff || exact_conflict_retryable)
                    {
                        let id = command.id();
                        error.failure.source =
                            StoreError::MetadataCommandIrrevocableConvergencePending {
                                pg_id: id.pg_id().get(),
                                cluster_epoch: id.cluster_epoch(),
                                log_index: id.log_index().get(),
                            }
                            .into();
                    }
                    return Err(error.failure);
                }
            }
        }
    }

    fn observe_metadata_command_apply_progress(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        deadline: Instant,
    ) -> MetadataCommandApplyProgress {
        if Instant::now() >= deadline {
            return MetadataCommandApplyProgress::Abortable;
        }
        let pg_id = command.id().pg_id();
        let nodes = match execution_route.mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        };
        let Ok(nodes) = nodes else {
            return MetadataCommandApplyProgress::Abortable;
        };
        let primary = match execution_route.mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        };
        let Ok(primary) = primary else {
            return MetadataCommandApplyProgress::Abortable;
        };
        let primary_node_id = primary.node_id();
        if primary
            .metadata_command_inspection_client()
            .metadata_command_acceptance_until(pg_id, command, deadline)
            .is_ok_and(|acceptance| acceptance == MetadataCommandAcceptance::AlreadyApplied)
        {
            return MetadataCommandApplyProgress::Published;
        }
        let witness = nodes
            .into_iter()
            .filter(|node| node.node_id() != primary_node_id)
            .min_by_key(|node| node.node_id());
        if witness.is_some_and(|witness| {
            witness
                .metadata_command_inspection_client()
                .metadata_command_acceptance_until(pg_id, command, deadline)
                .is_ok_and(|acceptance| acceptance == MetadataCommandAcceptance::AlreadyApplied)
        }) {
            MetadataCommandApplyProgress::Witnessed
        } else {
            MetadataCommandApplyProgress::Abortable
        }
    }

    fn apply_metadata_command_to_acting_set_once(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        reservation_authority: &StorageCluster,
        attempt_context: MetadataCommandApplyAttemptContext,
    ) -> Result<(), MetadataCommandApplyAttemptFailure> {
        let initial_progress = attempt_context.progress;
        let deadline = attempt_context.deadline;
        if Instant::now() >= deadline {
            return Err(MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: command.id().pg_id().get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                },
            ));
        }
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    0,
                    initial_progress,
                    source,
                )
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    0,
                    initial_progress,
                    source,
                )
            })?;
        let route_mode = execution_route.mode;
        let authorized_source = execution_route.recovery_authorized_source;
        let abandoned_source = execution_route.recovery_abandoned_source;
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = lock_metadata_command_pg_until(&pg_lock, deadline).ok_or_else(|| {
            MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: command.id().pg_id().get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                },
            )
        })?;
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
        .map_err(|source| {
            MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                source,
            )
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
        .map_err(|source| {
            MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                source,
            )
        })?
        .node_id();
        if origin_node_id != primary_node_id {
            return Err(MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                StoreError::MetadataCommandFromNonPrimary {
                    node_id: primary_node_id.as_u32(),
                    pg_id: pg_id.get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    origin_node_id: origin_node_id.as_u32(),
                    primary_node_id: primary_node_id.as_u32(),
                },
            ));
        }
        let primary = nodes
            .iter()
            .find(|node| node.node_id() == primary_node_id)
            .expect("validated metadata primary must be in the acting set");
        let primary_section = match execution_route.recovery_authorized_source {
            Some(_) => HeldPrimaryMetadataCommandSection::Recovery(
                primary
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_critical_section_until(
                        pg_id,
                        command.id().cluster_epoch(),
                        deadline,
                    )
                    .map_err(|source| {
                        MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                            0,
                            initial_progress,
                            source,
                        )
                    })?,
            ),
            None => HeldPrimaryMetadataCommandSection::Active(
                primary
                    .metadata_command_client()
                    .open_metadata_command_critical_section_until(
                        pg_id,
                        command.id().cluster_epoch(),
                        deadline,
                    )
                    .map_err(|source| {
                        MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                            0,
                            initial_progress,
                            source,
                        )
                    })?,
            ),
        };
        // A recovered pending command has no in-process progress owner. Rebuild
        // that progress only after acquiring the primary's cross-process
        // section, which excludes fanout and abandonment by other frontends.
        let initial_progress = if initial_progress.is_abortable()
            && attempt_context.provenance
                == MetadataCommandApplyProgressProvenance::RecoveredPending
        {
            maybe_run_metadata_command_progress_reconstruction_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                command,
            )
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    0,
                    MetadataCommandApplyProgress::PublicationUnconfirmed,
                    source,
                )
            })?;
            let primary_observation = primary_section
                .applied_metadata_command_log_entry_hashes_until(command, deadline);
            let primary_abandonment =
                primary_section.abandonment_acceptance_until(command, deadline);
            let primary_publication_started = primary_section
                .pending_metadata_command_publication_started_until(command, deadline);
            match self
                .reconstruct_pending_metadata_command_apply_progress_with_held_primary_until(
                    pg_id,
                    command,
                    route_mode,
                    HeldPrimaryMetadataCommandObservation {
                        node_id: primary_node_id,
                        result: primary_observation,
                        abandonment: primary_abandonment,
                        publication_started: primary_publication_started,
                    },
                    deadline,
                )
                .map_err(|source| {
                    MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                        0,
                        MetadataCommandApplyProgress::PublicationUnconfirmed,
                        source,
                    )
                })? {
                Some(progress) => progress,
                None => {
                    return Err(MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                        0,
                        MetadataCommandApplyProgress::PublicationUnconfirmed,
                        StoreError::MetadataCommandOutcomeUnconfirmed {
                            pg_id: pg_id.get(),
                            cluster_epoch: command.id().cluster_epoch(),
                            log_index: command.id().log_index().get(),
                        },
                    ));
                }
            }
        } else {
            initial_progress
        };
        let primary_acceptance = primary_section
            .acceptance_until(command, deadline)
            .map_err(|source| {
            MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                0,
                initial_progress,
                source,
            )
        })?;
        let mut attempt_progress = if primary_acceptance == MetadataCommandAcceptance::AlreadyApplied
        {
            initial_progress.merge(MetadataCommandApplyProgress::Published)
        } else {
            initial_progress
        };
        if primary_acceptance == MetadataCommandAcceptance::Apply
            && attempt_progress.is_abortable()
        {
            reservation_authority
                .validate_metadata_command_bucket_write_reservation_until(command, deadline)
                .map_err(|source| {
                    MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                        0,
                        attempt_progress,
                        source,
                    )
                })?;
        }

        let witness_node_id = nodes
            .iter()
            .map(|node| node.node_id())
            .filter(|node_id| *node_id != primary_node_id)
            .min();
        nodes.sort_by_key(|node| {
            Self::metadata_command_publication_order_key(
                node.node_id(),
                primary_node_id,
                witness_node_id,
            )
        });
        // A redundant acting set durably applies one deterministic replica
        // before the primary publication boundary. That exact command-log
        // row is the cross-failure-domain publication witness.
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                if primary_acceptance == MetadataCommandAcceptance::AlreadyApplied {
                    attempt_progress =
                        attempt_progress.merge(MetadataCommandApplyProgress::Published);
                } else {
                    maybe_run_before_metadata_command_apply_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        node.node_id(),
                        command,
                    )
                    .map_err(|source| {
                        MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                            applied_nodes,
                            attempt_progress,
                            source,
                        )
                    })?;
                    if attempt_progress.is_abortable()
                        && attempt_context.publication_start
                            == MetadataCommandPublicationStartPolicy::Required
                    {
                        primary_section
                            .mark_pending_metadata_command_publication_started_until(
                                command, deadline,
                            )
                            .map_err(|error| {
                                let progress = if error.kind()
                                    == MetadataCommandApplyErrorKind::MayHaveApplied
                                {
                                    MetadataCommandApplyProgress::PublicationUnconfirmed
                                } else {
                                    attempt_progress
                                };
                                MetadataCommandApplyAttemptFailure::from_apply_call_error_with_progress(
                                    applied_nodes,
                                    progress,
                                    false,
                                    error,
                                )
                            })?;
                        attempt_progress = attempt_progress
                            .merge(MetadataCommandApplyProgress::PublicationStarted);
                    }
                }
                primary_section
                    .apply_until(
                        authorized_source,
                        abandoned_source,
                        command,
                        deadline,
                    )
                    .map_err(|source| {
                        MetadataCommandApplyAttemptFailure::from_apply_call_error_with_progress(
                            applied_nodes,
                            attempt_progress,
                            true,
                            source,
                        )
                    })?;
                maybe_run_after_metadata_command_apply_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    node.node_id(),
                    command,
                )
                .map_err(|source| {
                    MetadataCommandApplyAttemptFailure::after_apply_dispatch_with_progress(
                        applied_nodes,
                        attempt_progress,
                        true,
                        source,
                    )
                })?;
                attempt_progress =
                    attempt_progress.merge(MetadataCommandApplyProgress::Published);
                continue;
            }

            let acceptance = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.local_map.validate_metadata_command_for_replica_until(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                        deadline,
                    )
                }
                MetadataCommandRouteMode::Recovery => self
                    .local_map
                    .validate_metadata_command_for_replica_for_metadata_command_recovery_until(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                        deadline,
                    ),
            }
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    applied_nodes,
                    attempt_progress,
                    source,
                )
            })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                if Some(node.node_id()) == witness_node_id {
                    attempt_progress =
                        attempt_progress.merge(MetadataCommandApplyProgress::Witnessed);
                }
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
                        .map_err(MetadataCommandApplyError::not_sent)
                        .and_then(|route| route.apply_until(deadline)),
                    None => node
                        .metadata_command_client()
                        .apply_metadata_command_and_record_until(pg_id, command, deadline),
                };
                apply.map_err(|source| {
                    MetadataCommandApplyAttemptFailure::from_apply_call_error_with_progress(
                        applied_nodes,
                        attempt_progress,
                        false,
                        source,
                    )
                })?;
                maybe_run_after_metadata_command_apply_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    node.node_id(),
                    command,
                )
                .map_err(|source| {
                    MetadataCommandApplyAttemptFailure::after_apply_dispatch_with_progress(
                        applied_nodes,
                        attempt_progress,
                        false,
                        source,
                    )
                })?;
                continue;
            }
            maybe_run_before_metadata_command_apply_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                node.node_id(),
                command,
            )
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::before_apply_with_progress(
                    applied_nodes,
                    attempt_progress,
                    source,
                )
            })?;
            if attempt_progress.is_abortable()
                && attempt_context.publication_start
                    == MetadataCommandPublicationStartPolicy::Required
            {
                primary_section
                    .mark_pending_metadata_command_publication_started_until(command, deadline)
                    .map_err(|error| {
                        let progress = if error.kind()
                            == MetadataCommandApplyErrorKind::MayHaveApplied
                        {
                            MetadataCommandApplyProgress::PublicationUnconfirmed
                        } else {
                            attempt_progress
                        };
                        MetadataCommandApplyAttemptFailure::from_apply_call_error_with_progress(
                            applied_nodes,
                            progress,
                            false,
                            error,
                        )
                    })?;
                attempt_progress =
                    attempt_progress.merge(MetadataCommandApplyProgress::PublicationStarted);
            }
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
                    .map_err(MetadataCommandApplyError::not_sent)
                    .and_then(|route| route.apply_until(deadline)),
                None => node
                    .metadata_command_client()
                    .apply_metadata_command_and_record_until(pg_id, command, deadline),
            };
            apply.map_err(|source| {
                MetadataCommandApplyAttemptFailure::from_apply_call_error_with_progress(
                    applied_nodes,
                    attempt_progress,
                    false,
                    source,
                )
            })?;
            maybe_run_after_metadata_command_apply_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                node.node_id(),
                command,
            )
            .map_err(|source| {
                MetadataCommandApplyAttemptFailure::after_apply_dispatch_with_progress(
                    applied_nodes,
                    attempt_progress,
                    false,
                    source,
                )
            })?;
            if Some(node.node_id()) == witness_node_id {
                attempt_progress =
                    attempt_progress.merge(MetadataCommandApplyProgress::Witnessed);
            }
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
            Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET,
        )
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::normal(),
            deadline,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_record_abandoned_metadata_command_to_acting_set_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.record_abandoned_metadata_command_to_acting_set_until(command, deadline)
            .map_err(|error| error.source)
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set_for_recovery_until(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                authorized_source,
                abandoned_source,
            ),
            deadline,
        )
    }

    fn record_abandoned_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
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
        let _pg_guard = lock_metadata_command_pg_until(&pg_lock, deadline).ok_or_else(|| {
            MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::PublicationUnconfirmed,
                may_have_applied: false,
                source: StoreError::MetadataCommandOutcomeUnconfirmed {
                    pg_id: pg_id.get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                }
                .into(),
            }
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
            progress: MetadataCommandApplyProgress::Abortable,
            may_have_applied: false,
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
            progress: MetadataCommandApplyProgress::Abortable,
            may_have_applied: false,
            source: source.into(),
        })?;
        let primary = nodes
            .iter()
            .find(|node| node.node_id() == primary_node_id)
            .expect("validated metadata primary must be in the acting set");
        let primary_critical_section = primary
            .metadata_command_recovery_client()
            .open_metadata_command_recovery_critical_section_until(
                pg_id,
                command.id().cluster_epoch(),
                deadline,
            )
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::PublicationUnconfirmed,
                may_have_applied: false,
                source: source.into(),
            })?;
        let reconstructed = self
            .reconstruct_pending_metadata_command_apply_progress_with_held_primary_until(
                pg_id,
                command,
                route_mode,
                HeldPrimaryMetadataCommandObservation {
                    node_id: primary_node_id,
                    result: primary_critical_section
                        .applied_metadata_command_log_entry_hashes_until(command, deadline),
                    abandonment: primary_critical_section
                        .metadata_command_abandon_acceptance_until(command, deadline),
                    publication_started: primary_critical_section
                        .pending_metadata_command_publication_started_until(command, deadline),
                },
                deadline,
            )
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::PublicationUnconfirmed,
                may_have_applied: false,
                source,
            })?;
        match reconstructed {
            Some(MetadataCommandApplyProgress::Abortable) => {}
            Some(progress) => {
                return Err(MetadataCommandApplyFailure {
                    applied_nodes: 0,
                    progress,
                    may_have_applied: false,
                    source: StoreError::MetadataCommandIrrevocableConvergencePending {
                        pg_id: pg_id.get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    }
                    .into(),
                });
            }
            None => {
                return Err(MetadataCommandApplyFailure {
                    applied_nodes: 0,
                    progress: MetadataCommandApplyProgress::PublicationUnconfirmed,
                    may_have_applied: false,
                    source: StoreError::MetadataCommandOutcomeUnconfirmed {
                        pg_id: pg_id.get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    }
                    .into(),
                });
            }
        }
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                let metadata_client = primary_critical_section.as_ref();
                let acceptance = metadata_client
                    .metadata_command_abandon_acceptance_until(command, deadline)
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        progress: MetadataCommandApplyProgress::Abortable,
                        may_have_applied: false,
                        source: BucketSnapshotLoadError::Store(source),
                    })?;
                if acceptance != MetadataCommandAcceptance::AlreadyApplied {
                    metadata_client
                        .record_metadata_command_abandoned_until(command, deadline)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            progress: MetadataCommandApplyProgress::Abortable,
                            may_have_applied: false,
                            source: source.into(),
                        })?;
                }
                continue;
            }
            let acceptance = self
                .local_map
                .validate_metadata_command_abandon_for_replica_until(
                    primary_node_id,
                    node.node_id(),
                    pg_id,
                    command,
                    deadline,
                )
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    progress: MetadataCommandApplyProgress::Abortable,
                    may_have_applied: false,
                    source: source.into(),
                })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                continue;
            }
            let result = match route_mode {
                MetadataCommandRouteMode::Normal => node
                    .metadata_command_client()
                    .record_metadata_command_abandoned_on_replica_until(
                        pg_id, command, deadline,
                    ),
                MetadataCommandRouteMode::Recovery => node
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_replica_abandon_route(
                        pg_id,
                        command.id().cluster_epoch(),
                        recovery_authorized_source.unwrap_or(command),
                        recovery_abandoned_source,
                        command,
                    )
                    .and_then(|route| route.record_abandoned_until(deadline)),
            };
            result.map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        }
        Ok(())
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode_until(
            command,
            MetadataCommandExecutionRoute::normal(),
            deadline,
        )
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set_for_recovery_until(
        &self,
        recovery_proof: MetadataCommandRecoveryProof<'_>,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode_until(
            command,
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                authorized_source,
                abandoned_source,
            ),
            deadline,
        )
    }

    fn metadata_command_has_abandoned_log_on_acting_set_with_route_mode_until(
        &self,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        execution_route
            .require_command(command.id().pg_id(), command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        execution_route
            .require_recovery_predecessor(command.id().pg_id())
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
                source: source.into(),
            })?;
        let route_mode = execution_route.mode;
        self.maybe_run_before_direct_put_abandoned_log_inspection_hook(command)
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes: 0,
                progress: MetadataCommandApplyProgress::Abortable,
                may_have_applied: false,
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
            progress: MetadataCommandApplyProgress::Abortable,
            may_have_applied: false,
            source: source.into(),
        })?;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node
                .metadata_command_inspection_client()
                .metadata_command_abandoned_until(pg_id, command, deadline)
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    progress: MetadataCommandApplyProgress::Abortable,
                    may_have_applied: false,
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
            MetadataCommandApplyAttemptContext {
                progress: MetadataCommandApplyProgress::Abortable,
                deadline: Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET,
                provenance: MetadataCommandApplyProgressProvenance::Authoritative,
                publication_start: MetadataCommandPublicationStartPolicy::RawFanoutTestBypass,
            },
        )
        .map(|_| ())
        .map_err(|error| error.source)
    }

    #[cfg(test)]
    pub(crate) fn test_apply_pending_metadata_command_to_acting_set_from_origin(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            origin_node_id,
            command,
            MetadataCommandExecutionRoute::normal(),
            self,
            MetadataCommandApplyAttemptContext {
                progress: MetadataCommandApplyProgress::Abortable,
                deadline: Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET,
                provenance: MetadataCommandApplyProgressProvenance::Authoritative,
                publication_start: MetadataCommandPublicationStartPolicy::Required,
            },
        )
        .map(|_| ())
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

    pub(super) fn finish_pending_metadata_command_to_acting_set_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        match self.finish_pending_metadata_command_to_acting_set_coordinated_inner(
            pg_id,
            command,
            MetadataCommandFinishPolicy {
                clear_pending_on_zero_apply,
                retry_partial_exact_conflict: false,
                convergence_requirement:
                    MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                progress_provenance: if clear_pending_on_zero_apply {
                    MetadataCommandApplyProgressProvenance::Authoritative
                } else {
                    MetadataCommandApplyProgressProvenance::RecoveredPending
                },
            },
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )? {
            FinishPendingMetadataCommandResult::Applied => {
                Ok(super::PendingMetadataCommandOutcome::Applied)
            }
            FinishPendingMetadataCommandResult::PublishedPendingRecovery => Ok(
                super::PendingMetadataCommandOutcome::PublishedPendingRecovery,
            ),
            FinishPendingMetadataCommandResult::Abandoned => {
                Ok(super::PendingMetadataCommandOutcome::Abandoned)
            }
            FinishPendingMetadataCommandResult::TerminalCleanupPending { applied } => Ok(
                super::PendingMetadataCommandOutcome::TerminalCleanupPending { applied },
            ),
            FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                unreachable!("partial exact conflict retry is disabled for this caller")
            }
        }
    }

    fn finish_pending_metadata_command_to_acting_set_requiring_convergence_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        match self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            MetadataCommandFinishPolicy {
                clear_pending_on_zero_apply,
                retry_partial_exact_conflict: true,
                convergence_requirement: MetadataCommandConvergenceRequirement::RequireAllReplicas,
                progress_provenance: if clear_pending_on_zero_apply {
                    MetadataCommandApplyProgressProvenance::Authoritative
                } else {
                    MetadataCommandApplyProgressProvenance::RecoveredPending
                },
            },
            MetadataCommandExecutionRoute::normal(),
            work_budget,
            None,
        )? {
            FinishPendingMetadataCommandResult::Applied => {
                Ok(super::PendingMetadataCommandOutcome::Applied)
            }
            FinishPendingMetadataCommandResult::PublishedPendingRecovery => Ok(
                super::PendingMetadataCommandOutcome::PublishedPendingRecovery,
            ),
            FinishPendingMetadataCommandResult::Abandoned => {
                Ok(super::PendingMetadataCommandOutcome::Abandoned)
            }
            FinishPendingMetadataCommandResult::TerminalCleanupPending { applied } => Ok(
                super::PendingMetadataCommandOutcome::TerminalCleanupPending { applied },
            ),
            FinishPendingMetadataCommandResult::RetryPartialExactConflict => Ok(
                super::PendingMetadataCommandOutcome::RetryPartialExactConflict,
            ),
        }
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_coordinated_inner(
            pg_id,
            command,
            MetadataCommandFinishPolicy {
                clear_pending_on_zero_apply,
                retry_partial_exact_conflict: true,
                convergence_requirement:
                    MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                progress_provenance: if clear_pending_on_zero_apply {
                    MetadataCommandApplyProgressProvenance::Authoritative
                } else {
                    MetadataCommandApplyProgressProvenance::RecoveredPending
                },
            },
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_under_recovery_leader_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        leader: &mut MetadataCommandRecoveryLeader<'_>,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        let (work_budget, _proof, recovery_guard) = leader.parts_with_guard();
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            MetadataCommandFinishPolicy {
                clear_pending_on_zero_apply,
                retry_partial_exact_conflict: true,
                convergence_requirement:
                    MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                progress_provenance: MetadataCommandApplyProgressProvenance::RecoveredPending,
            },
            MetadataCommandExecutionRoute::normal(),
            work_budget,
            Some(recovery_guard),
        )
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
        leader: &mut MetadataCommandRecoveryLeader<'_>,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        let (work_budget, recovery_proof, recovery_guard) = leader.parts_with_guard();
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            MetadataCommandFinishPolicy {
                clear_pending_on_zero_apply,
                retry_partial_exact_conflict: true,
                convergence_requirement:
                    MetadataCommandConvergenceRequirement::AllowRecoveryHandoff,
                progress_provenance: MetadataCommandApplyProgressProvenance::RecoveredPending,
            },
            MetadataCommandExecutionRoute::recovery(
                recovery_proof,
                recovery_authorized_source,
                None,
            ),
            work_budget,
            Some(recovery_guard),
        )
    }

    fn finish_pending_metadata_command_to_acting_set_coordinated_inner(
        &self,
        pg_id: PgId,
        initial_command: &MetadataCommandEnvelope,
        mut policy: MetadataCommandFinishPolicy,
        execution_route: MetadataCommandExecutionRoute<'_>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        debug_assert_eq!(execution_route.mode, MetadataCommandRouteMode::Normal);
        let mut command = initial_command.clone();
        loop {
            match self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery_until(pg_id, &command, work_budget.deadline())
            {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    let outcome = self.finish_pending_metadata_command_to_acting_set_inner(
                        pg_id,
                        &command,
                        policy,
                        execution_route,
                        work_budget,
                        Some(&guard),
                    )?;
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        match outcome {
                            FinishPendingMetadataCommandResult::Applied => "applied",
                            FinishPendingMetadataCommandResult::PublishedPendingRecovery => {
                                "published_pending_recovery"
                            }
                            FinishPendingMetadataCommandResult::Abandoned => "abandoned",
                            FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                applied: true,
                            } => "applied_cleanup_pending",
                            FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                applied: false,
                            } => "abandoned_cleanup_pending",
                            FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                                "retry_partial_exact_conflict"
                            }
                        },
                    );
                    return Ok(outcome);
                }
                MetadataCommandRecoveryAdmission::Waited {
                    wait_us,
                    lineage_tip,
                } => {
                    command = lineage_tip;
                    policy.progress_provenance =
                        MetadataCommandApplyProgressProvenance::RecoveredPending;
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    if let Err(error) =
                        work_budget.check("metadata command recovery waiter budget exhausted")
                    {
                        return match self
                            .classify_bucket_metadata_command_budget_exhaustion(
                                pg_id,
                                &command,
                                policy.convergence_requirement,
                                error,
                            )?
                        {
                            MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                Ok(FinishPendingMetadataCommandResult::PublishedPendingRecovery)
                            }
                            MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                Err(error.into())
                            }
                        };
                    }
                    let waiter_outcome = self
                        .pending_command_recovery_waiter_outcome_with_route_mode_until(
                            pg_id,
                            &command,
                            MetadataCommandRouteMode::Normal,
                            work_budget.deadline(),
                        )
                        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        waiter_outcome.metric_label(),
                    );
                    match waiter_outcome {
                        MetadataCommandRecoveryWaiterOutcome::StillPending => continue,
                        MetadataCommandRecoveryWaiterOutcome::Applied => {
                            return Ok(FinishPendingMetadataCommandResult::Applied);
                        }
                        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
                        | MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
                            return Ok(FinishPendingMetadataCommandResult::Abandoned);
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut {
                    wait_us,
                    lineage_tip,
                } => {
                    command = lineage_tip;
                    policy.progress_provenance =
                        MetadataCommandApplyProgressProvenance::RecoveredPending;
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
                    if let Err(error) = work_budget.sleep_after_contention(
                        "metadata command recovery gate retry budget exhausted",
                    ) {
                        return match self
                            .classify_bucket_metadata_command_budget_exhaustion(
                                pg_id,
                                &command,
                                policy.convergence_requirement,
                                error,
                            )?
                        {
                            MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                                Ok(FinishPendingMetadataCommandResult::PublishedPendingRecovery)
                            }
                            MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                                Err(error.into())
                            }
                        };
                    }
                }
            }
        }
    }

    fn classify_bucket_metadata_command_budget_exhaustion(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        convergence_requirement: MetadataCommandConvergenceRequirement,
        budget_error: StoreError,
    ) -> Result<MetadataCommandBudgetExhaustionOutcome, BucketSnapshotLoadError> {
        self.classify_metadata_command_budget_exhaustion_with_route_mode(
            pg_id,
            command,
            MetadataCommandRouteMode::Normal,
            convergence_requirement,
            budget_error,
        )
    }

    pub(super) fn classify_metadata_command_budget_exhaustion_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        convergence_requirement: MetadataCommandConvergenceRequirement,
        budget_error: StoreError,
    ) -> Result<MetadataCommandBudgetExhaustionOutcome, BucketSnapshotLoadError> {
        let confirmation_deadline =
            Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
        let state = self.metadata_command_publication_state_on_acting_set_until(
            pg_id,
            command,
            route_mode,
            confirmation_deadline,
        )?;
        Ok(Self::metadata_command_budget_exhaustion_outcome(
            command,
            convergence_requirement,
            state,
            budget_error,
        ))
    }

    pub(super) fn resolve_metadata_command_abandonment_observation(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        convergence_requirement: MetadataCommandConvergenceRequirement,
        observation: Result<bool, MetadataCommandApplyFailure>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<MetadataCommandAbandonmentObservation, BucketSnapshotLoadError> {
        match observation {
            Ok(abandoned) => Ok(MetadataCommandAbandonmentObservation::Observed(abandoned)),
            Err(error)
                if metadata_command_abandonment_observation_error_is_retryable(&error.source) =>
            {
                let Err(budget_error) = work_budget.sleep_after_contention(
                    "metadata command abandonment observation retry budget exhausted",
                ) else {
                    return Ok(MetadataCommandAbandonmentObservation::Retry);
                };
                match self.classify_metadata_command_budget_exhaustion_with_route_mode(
                    pg_id,
                    command,
                    route_mode,
                    convergence_requirement,
                    budget_error,
                )? {
                    MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => Ok(
                        MetadataCommandAbandonmentObservation::PublishedPendingRecovery,
                    ),
                    MetadataCommandBudgetExhaustionOutcome::Error(error) => Err(error.into()),
                }
            }
            Err(error) => Err(error.source),
        }
    }

    fn metadata_command_budget_exhaustion_outcome(
        command: &MetadataCommandEnvelope,
        convergence_requirement: MetadataCommandConvergenceRequirement,
        state: MetadataCommandPublicationState,
        budget_error: StoreError,
    ) -> MetadataCommandBudgetExhaustionOutcome {
        if state == MetadataCommandPublicationState::NotPublished {
            return MetadataCommandBudgetExhaustionOutcome::Error(budget_error);
        }
        if state == MetadataCommandPublicationState::Published
            && convergence_requirement == MetadataCommandConvergenceRequirement::AllowRecoveryHandoff
        {
            return MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery;
        }
        let id = command.id();
        MetadataCommandBudgetExhaustionOutcome::Error(match convergence_requirement {
            MetadataCommandConvergenceRequirement::AllowRecoveryHandoff => {
                StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: id.pg_id().get(),
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                }
            }
            MetadataCommandConvergenceRequirement::RequireAllReplicas => {
                StoreError::MetadataCommandDependencyConvergencePending {
                    pg_id: id.pg_id().get(),
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                }
            }
        })
    }

    fn finish_pending_metadata_command_to_acting_set_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        policy: MetadataCommandFinishPolicy,
        mut execution_route: MetadataCommandExecutionRoute<'_>,
        work_budget: &mut super::RequestWorkBudget,
        recovery_guard: Option<&super::MetadataCommandRecoveryGuard>,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        execution_route.require_command(pg_id, command)?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source.cloned();
        let recovery_abandoned_source = execution_route.recovery_abandoned_source.cloned();
        let mut command = command.clone();
        let mut apply_progress = MetadataCommandApplyProgress::Abortable;
        let mut progress_provenance = policy.progress_provenance;
        loop {
            if let Err(error) = work_budget.check("metadata command apply retry budget exhausted") {
                let confirmation_deadline =
                    Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
                let state = self
                    .metadata_command_publication_state_on_acting_set_until(
                        pg_id,
                        &command,
                        route_mode,
                        confirmation_deadline,
                    )?;
                match Self::metadata_command_budget_exhaustion_outcome(
                    &command,
                    policy.convergence_requirement,
                    state,
                    error,
                ) {
                    MetadataCommandBudgetExhaustionOutcome::PublishedPendingRecovery => {
                        return Ok(FinishPendingMetadataCommandResult::PublishedPendingRecovery);
                    }
                    MetadataCommandBudgetExhaustionOutcome::Error(error) => {
                        return Err(error.into());
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
                    policy.convergence_requirement,
                    abandoned_on_acting_set,
                    work_budget,
                )?
            {
                MetadataCommandAbandonmentObservation::Observed(abandoned) => abandoned,
                MetadataCommandAbandonmentObservation::Retry => continue,
                MetadataCommandAbandonmentObservation::PublishedPendingRecovery => {
                    return Ok(FinishPendingMetadataCommandResult::PublishedPendingRecovery);
                }
            };
            if abandoned_on_acting_set {
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
                .map_err(|error| error.source)?;
                self.release_metadata_command_bucket_write_reservation(&command)?;
                let cleanup = match route_mode {
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
                return Ok(if cleanup == PendingMetadataCommandTerminalCleanup::Deferred {
                    FinishPendingMetadataCommandResult::TerminalCleanupPending { applied: false }
                } else {
                    FinishPendingMetadataCommandResult::Abandoned
                });
            }
            let apply_result = self.apply_metadata_command_to_acting_set_with_route_mode_until_inner(
                &command,
                execution_route,
                self,
                apply_progress,
                work_budget.deadline(),
                progress_provenance,
            );
            if let Err(error) = &apply_result {
                apply_progress = apply_progress.merge(error.progress);
            }
            match apply_result {
                Ok(outcome) => {
                    if outcome == MetadataCommandApplyOutcome::PublishedPendingRecovery
                        && policy.convergence_requirement
                            == MetadataCommandConvergenceRequirement::RequireAllReplicas
                    {
                        let id = command.id();
                        return Err(StoreError::MetadataCommandDependencyConvergencePending {
                            pg_id: id.pg_id().get(),
                            cluster_epoch: id.cluster_epoch(),
                            log_index: id.log_index().get(),
                        }
                        .into());
                    }
                    if outcome == MetadataCommandApplyOutcome::PublishedPendingRecovery {
                        return Ok(
                            FinishPendingMetadataCommandResult::PublishedPendingRecovery,
                        );
                    }
                    if outcome == MetadataCommandApplyOutcome::Converged {
                        if !self
                            .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                pg_id, &command,
                            )?
                        {
                            return Ok(
                                FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        let cleanup = match route_mode {
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
                        if cleanup == PendingMetadataCommandTerminalCleanup::Deferred {
                            return Ok(
                                FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                    }
                    return Ok(FinishPendingMetadataCommandResult::Applied);
                }
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        progress,
                        source,
                        ..
                    } = error;
                    if progress.is_abortable()
                        && metadata_command_apply_transport_error_is_retryable(&source)
                    {
                        work_budget.sleep_after_contention(
                            "metadata command transport retry budget exhausted",
                        )?;
                        continue;
                    }
                    if policy.retry_partial_exact_conflict
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let applied_on_all = self
                            .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                                pg_id, &command, route_mode,
                            )?;
                        if applied_on_all {
                            if !self
                                .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                    pg_id, &command,
                                )?
                            {
                                return Ok(
                                    FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                        applied: true,
                                    },
                                );
                            }
                            let cleanup = match route_mode {
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
                            if cleanup == PendingMetadataCommandTerminalCleanup::Deferred {
                                return Ok(
                                    FinishPendingMetadataCommandResult::TerminalCleanupPending {
                                        applied: true,
                                    },
                                );
                            }
                            return Ok(FinishPendingMetadataCommandResult::Applied);
                        }
                        let exact_conflict_retryable = self
                            .partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
                                pg_id,
                                &command,
                                applied_nodes,
                                &source,
                                route_mode,
                            )?;
                        if exact_conflict_retryable && !progress.is_abortable() {
                            return Ok(
                                FinishPendingMetadataCommandResult::RetryPartialExactConflict,
                            );
                        }
                    }
                    if progress.is_abortable()
                        && applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command_with_route_mode_and_recovery_guard_until(
                                pg_id,
                                &command,
                                execution_route,
                                command.payload(),
                                recovery_guard,
                                work_budget.deadline(),
                            )?
                        else {
                            return Ok(FinishPendingMetadataCommandResult::Abandoned);
                        };
                        execution_route =
                            execution_route.for_reissued_command(pg_id, &command, &reissued)?;
                        command = reissued;
                        apply_progress = MetadataCommandApplyProgress::Abortable;
                        progress_provenance =
                            MetadataCommandApplyProgressProvenance::Authoritative;
                        continue;
                    }
                    if policy.clear_pending_on_zero_apply
                        && progress.is_abortable()
                        && applied_nodes == 0
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

    fn drain_bucket_pg_pending_metadata_command_requiring_convergence_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        self.emit_pending_slot_action_for_command(pg_id, command, "drain_dependency_attempt");
        self.finish_pending_metadata_command_to_acting_set_requiring_convergence_with_work_budget(
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
                | FinishPendingMetadataCommandResult::PublishedPendingRecovery
                | FinishPendingMetadataCommandResult::Abandoned
                | FinishPendingMetadataCommandResult::TerminalCleanupPending { .. } => Ok(()),
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
        work_budget.check("install bucket-control pending metadata command")?;
        let deadline = work_budget.deadline();
        self.maybe_run_before_metadata_command_pending_install_hook();
        let insert = match effect_fence {
            Some(effect_fence) => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
                    pg_id,
                    command,
                    bucket,
                    effect_fence,
                    deadline,
                ),
            None => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot_until(
                    pg_id, command, bucket, deadline,
                ),
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
                self.finish_object_pg_pending_slot_requiring_convergence_with_work_budget(
                    pg_id,
                    command,
                    work_budget,
                )
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

    #[cfg(test)]
    pub(crate) fn test_finish_pending_command_for_multipart_completion_barrier(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_pending_command_for_multipart_completion_barrier(
            pg_id,
            command,
            work_budget,
        )
    }


}
