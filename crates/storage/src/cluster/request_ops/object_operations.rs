// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug)]
pub(super) enum NewObjectMetadataCommandApplyOutcome {
    Applied,
    PublishedPendingRecovery,
    TerminalCleanupPending,
    Reinspect(ObjectPgActionError),
    Abandoned(ObjectPgActionError),
}

impl NewObjectMetadataCommandApplyOutcome {
    pub(super) fn pending_metadata_command_outcome(&self) -> Option<PendingMetadataCommandOutcome> {
        match self {
            Self::Applied => Some(PendingMetadataCommandOutcome::Applied),
            Self::PublishedPendingRecovery => {
                Some(PendingMetadataCommandOutcome::PublishedPendingRecovery)
            }
            Self::TerminalCleanupPending => {
                Some(PendingMetadataCommandOutcome::TerminalCleanupPending { applied: true })
            }
            Self::Reinspect(_) | Self::Abandoned(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
enum ObjectMetadataCommandApplyProvenance {
    New,
    RecoveredPending,
}

#[derive(Clone, Copy)]
struct ObjectMetadataCommandApplyContext<'a> {
    return_metadata_command_contention: bool,
    provenance: ObjectMetadataCommandApplyProvenance,
    recovery_guard: Option<&'a MetadataCommandRecoveryGuard>,
}

impl super::StorageCluster {
    #[cfg(test)]
    pub(crate) fn test_install_object_metadata_command_definitive_retry_hook(
        &self,
        hook: ObjectMetadataCommandDefinitiveRetryTestHook,
    ) -> ObjectMetadataCommandDefinitiveRetryTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = OBJECT_METADATA_COMMAND_DEFINITIVE_RETRY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        ObjectMetadataCommandDefinitiveRetryTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_object_metadata_command_apply_hook(
        &self,
        hook: BeforeObjectMetadataCommandApplyTestHook,
    ) -> BeforeObjectMetadataCommandApplyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_OBJECT_METADATA_COMMAND_APPLY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BeforeObjectMetadataCommandApplyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_direct_put_metadata_apply_uncertainty_hook(
        &self,
        hook: DirectPutMetadataApplyUncertaintyTestHook,
    ) -> DirectPutMetadataApplyUncertaintyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = DIRECT_PUT_METADATA_APPLY_UNCERTAINTY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        DirectPutMetadataApplyUncertaintyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_direct_put_pending_installed_hook(
        &self,
        hook: DirectPutPendingInstalledTestHook,
    ) -> DirectPutPendingInstalledTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = DIRECT_PUT_PENDING_INSTALLED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        DirectPutPendingInstalledTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_snapshot_reinspection_hook(
        &self,
        hook: SnapshotReinspectionTestHook,
    ) -> SnapshotReinspectionTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = SNAPSHOT_REINSPECTION_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        SnapshotReinspectionTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_snapshot_reinspection_action_hook(
        &self,
        hook: BeforeSnapshotReinspectionActionTestHook,
    ) -> BeforeSnapshotReinspectionActionTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_SNAPSHOT_REINSPECTION_ACTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BeforeSnapshotReinspectionActionTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_direct_put_pending_drain_hook(
        &self,
        hook: DirectPutPendingDrainTestHook,
    ) -> DirectPutPendingDrainTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = DIRECT_PUT_PENDING_DRAIN_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        DirectPutPendingDrainTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_direct_put_snapshot_read_hook(
        &self,
        hook: DirectPutSnapshotReadTestHook,
    ) -> DirectPutSnapshotReadTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = DIRECT_PUT_SNAPSHOT_READ_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        DirectPutSnapshotReadTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_direct_put_pending_install_uncertainty_hook(
        &self,
        hook: DirectPutPendingInstallUncertaintyTestHook,
    ) -> DirectPutPendingInstallUncertaintyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = DIRECT_PUT_PENDING_INSTALL_UNCERTAINTY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        DirectPutPendingInstallUncertaintyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_pending_object_metadata_command_drain_attempt_hook(
        &self,
        hook: PendingObjectMetadataCommandDrainAttemptTestHook,
    ) -> PendingObjectMetadataCommandDrainAttemptTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = PENDING_OBJECT_METADATA_COMMAND_DRAIN_ATTEMPT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PendingObjectMetadataCommandDrainAttemptTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_pending_object_metadata_command_recovery_transferred_hook(
        &self,
        hook: PendingObjectMetadataCommandRecoveryTransferredTestHook,
    ) -> PendingObjectMetadataCommandRecoveryTransferredTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = PENDING_OBJECT_METADATA_COMMAND_RECOVERY_TRANSFERRED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        PendingObjectMetadataCommandRecoveryTransferredTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_object_generation_pending_drain_hook(
        &self,
        hook: ObjectGenerationPendingDrainTestHook,
    ) -> ObjectGenerationPendingDrainTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            OBJECT_GENERATION_PENDING_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        ObjectGenerationPendingDrainTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_object_metadata_command_reissue_hook(
        &self,
        hook: BeforeObjectMetadataCommandReissueTestHook,
    ) -> BeforeObjectMetadataCommandReissueTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_OBJECT_METADATA_COMMAND_REISSUE_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(scope_id, hook);
        BeforeObjectMetadataCommandReissueTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_object_metadata_command_abandoned_hook(
        &self,
        hook: ObjectMetadataCommandAbandonedTestHook,
    ) -> ObjectMetadataCommandAbandonedTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = OBJECT_METADATA_COMMAND_ABANDONED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        ObjectMetadataCommandAbandonedTestHookGuard { scope_id }
    }

    pub(crate) fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_mutation_metadata_client();
        client
            .open_object_payload_reclaim_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                generation_id,
            )?
            .exists()
    }

    fn put_object_metadata_command_from_stored(
        stored: &StoredObject,
        version_id: VersionId,
        mutation: PutObjectMetadataMutation,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<PutObjectMetadataCommand, ObjectPgActionError> {
        if stored.version_id() != version_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "object metadata action returned version {:?} for stored version {:?}",
                    version_id,
                    stored.version_id()
                ),
            });
        }
        let live = stored
            .as_live()
            .ok_or(MetadataError::MethodNotAllowedOnDeleteMarker)?;
        Ok(PutObjectMetadataCommand::from_live_object_and_mutation(
            live.clone(),
            mutation,
            bucket_write_reservation,
        ))
    }

    pub(super) fn put_object_metadata_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(PutObjectMetadataIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
                .object_mutation_metadata_client();
            let put_object_metadata_route = storage_client.open_put_object_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
            )?;
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() {
                    if update.object.bucket == *bucket && update.object.key == *key {
                        let snapshot_version_id = match requested_version_id {
                            Some(version_id) => {
                                if version_id != update.object.version_id {
                                    self.drain_pending_object_metadata_command(
                                        publisher, pg_id, &command,
                                    )?;
                                    continue;
                                }
                                Some(version_id)
                            }
                            None => None,
                        };
                        let stored = put_object_metadata_route
                            .load_put_object_metadata_snapshot(snapshot_version_id)?;
                        if stored.version_id() != update.object.version_id {
                            self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                            continue;
                        }
                        let (value, version_id, mutation) = match action(&stored) {
                            Ok(command) => command,
                            Err(error) => return Ok(Err(error)),
                        };
                        let expected = Self::put_object_metadata_command_from_stored(
                            &stored,
                            version_id,
                            mutation,
                            update.bucket_write_reservation.clone(),
                        )?;
                        if update.as_ref() != &expected {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "conflicting pending command for object metadata update",
                            ));
                        }
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(value));
                    }
                }

                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ));
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_bucket_write_proof {
                () => {{
                    self.release_durable_bucket_write_reservation(reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }

            if snapshot_retry_phase.pending_drain_allowed()
                && self
                    .pending_metadata_command_for_bucket(pg_id, bucket)?
                    .is_some()
            {
                release_bucket_write_proof!()?;
                continue;
            }

            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let stored = match put_object_metadata_route
                .load_put_object_metadata_snapshot(requested_version_id)
            {
                Ok(stored) => stored,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            let (value, version_id, mutation) = match action(&stored) {
                Ok(command) => command,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Ok(Err(error));
                }
            };
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match put_object_metadata_route.build_put_object_metadata_command(
                BuildPutObjectMetadataCommandReq {
                    requested_version_id,
                    expected_stored: &stored,
                    version_id,
                    mutation,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    release_bucket_write_proof!()?;
                    continue;
                }
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    release_bucket_write_proof!()?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
                &mut evaluated_attempt,
            ) {
                Ok(install) => install,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    release_bucket_write_proof!()?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(value));
        }
    }

    #[cfg(test)]
    fn put_object_metadata_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        requested_version_id: Option<VersionId>,
        action: impl FnMut(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        self.put_object_metadata_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            action,
        )
    }

    #[cfg(test)]
    pub(crate) fn put_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        tags: &str,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutTags(crate::tests::object_tags(tags)),
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn delete_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok(((), version_id, PutObjectMetadataMutation::DeleteTags))
        })
    }

    /// Returns the version id the retention was applied to.
    #[cfg(test)]
    pub(crate) fn put_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        retention: ObjectRetention,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutRetention(retention),
            ))
        })
    }

    /// Returns the version id the legal hold was applied to.
    #[cfg(test)]
    pub(crate) fn put_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        legal_hold: StoredLegalHoldStatus,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutLegalHold(legal_hold),
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn put_object_acl_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let (version_id, acl_grants, public_read) = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutAcl {
                    acl_grants,
                    public_read,
                },
            ))
        })
    }

    fn load_bucket_lifecycle_context(
        &self,
        bucket: &BucketName,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<Option<BucketLifecycleContext>, ObjectPgActionError> {
        crate::node::maybe_run_before_lifecycle_context_load_hook(bucket);
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = bucket_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let bucket_info = match metadata_route.head_bucket_info() {
            Ok(info) => info,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Ok(None)
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };
        let bucket_incarnation_generation = bucket_info.bucket_incarnation_generation;
        if bucket_incarnation_generation != expected_bucket_incarnation_generation {
            return Ok(None);
        }
        let raw_lifecycle = if bucket_info.bucket_lifecycle_present {
            metadata_route
                .get_bucket_subresource(BucketSubresourceKind::Lifecycle)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        } else {
            None
        };
        Ok(Some(BucketLifecycleContext {
            bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        }))
    }

    pub(super) fn apply_new_object_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("new_object_metadata_command_apply")
        .for_pg(pg_id);
        match self.apply_new_object_metadata_command_for_bucket_with_owned_recovery(
            pg_id,
            bucket,
            command,
            &mut work_budget,
            false,
        )? {
            NewObjectMetadataCommandApplyOutcome::Applied
            | NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
            | NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => Ok(()),
            NewObjectMetadataCommandApplyOutcome::Reinspect(error)
            | NewObjectMetadataCommandApplyOutcome::Abandoned(error) => Err(error),
        }
    }

    pub(super) fn apply_new_object_metadata_command_for_bucket_or_reinspect(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.apply_new_object_metadata_command_for_bucket_with_owned_recovery(
            pg_id,
            bucket,
            command,
            work_budget,
            false,
        )
    }

    pub(super) fn apply_new_object_metadata_command_for_bucket_or_reinspect_with_recovery_guard(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
        recovery_guard: &MetadataCommandRecoveryGuard,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.apply_new_object_metadata_command_for_bucket_inner(
            pg_id,
            bucket,
            command,
            work_budget,
            ObjectMetadataCommandApplyContext {
                return_metadata_command_contention: false,
                provenance: ObjectMetadataCommandApplyProvenance::New,
                recovery_guard: Some(recovery_guard),
            },
        )
    }

    pub(super) fn apply_recovered_pending_object_metadata_command_for_bucket_or_reinspect_with_recovery_guard(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
        recovery_guard: &MetadataCommandRecoveryGuard,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.apply_new_object_metadata_command_for_bucket_inner(
            pg_id,
            bucket,
            command,
            work_budget,
            ObjectMetadataCommandApplyContext {
                return_metadata_command_contention: false,
                provenance: ObjectMetadataCommandApplyProvenance::RecoveredPending,
                recovery_guard: Some(recovery_guard),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn test_apply_new_object_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, command)
    }

    #[cfg(test)]
    pub(crate) fn test_apply_new_object_metadata_command_for_bucket_with_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        retry_budget: Duration,
    ) -> Result<(), ObjectPgActionError> {
        let mut work_budget = super::RequestWorkBudget::new(retry_budget, None)
            .for_operation("test_new_object_metadata_command_apply")
            .for_pg(pg_id);
        match self.apply_new_object_metadata_command_for_bucket_with_owned_recovery(
            pg_id,
            bucket,
            command,
            &mut work_budget,
            false,
        )? {
            NewObjectMetadataCommandApplyOutcome::Applied
            | NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
            | NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => Ok(()),
            NewObjectMetadataCommandApplyOutcome::Reinspect(error)
            | NewObjectMetadataCommandApplyOutcome::Abandoned(error) => Err(error),
        }
    }

    pub(super) fn apply_new_object_metadata_command_for_bucket_allocator(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        match self.apply_new_object_metadata_command_for_bucket_with_owned_recovery(
            pg_id,
            bucket,
            command,
            work_budget,
            true,
        )? {
            NewObjectMetadataCommandApplyOutcome::Applied
            | NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
            | NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => Ok(()),
            NewObjectMetadataCommandApplyOutcome::Reinspect(error)
            | NewObjectMetadataCommandApplyOutcome::Abandoned(error) => Err(error),
        }
    }

    fn apply_new_object_metadata_command_for_bucket_with_owned_recovery(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
        return_metadata_command_contention: bool,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        let mut recovery_command = command.clone();
        let mut provenance = ObjectMetadataCommandApplyProvenance::New;
        loop {
            work_budget
                .check("new object metadata command recovery admission budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let admission = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery_until(
                    pg_id,
                    &recovery_command,
                    work_budget.deadline(),
                );
            let recovery_guard = match admission {
                MetadataCommandRecoveryAdmission::Leader(guard) => guard,
                MetadataCommandRecoveryAdmission::Waited {
                    lineage_tip,
                    root_disposition,
                    resolution,
                    ..
                } => {
                    recovery_command = lineage_tip;
                    provenance = ObjectMetadataCommandApplyProvenance::RecoveredPending;
                    if let Some(outcome) = Self::new_object_metadata_command_outcome_from_recovery(
                        &recovery_command,
                        &root_disposition,
                        resolution,
                    )? {
                        return Ok(outcome);
                    }
                    continue;
                }
                MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                    lineage_tip,
                    root_disposition,
                    resolution,
                    ..
                }
                | MetadataCommandRecoveryAdmission::TimedOut {
                    lineage_tip,
                    root_disposition,
                    resolution,
                    ..
                } => {
                    if let Some(outcome) = Self::new_object_metadata_command_outcome_from_recovery(
                        &lineage_tip,
                        &root_disposition,
                        resolution,
                    )? {
                        return Ok(outcome);
                    }
                    return Err(
                        ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery,
                    );
                }
            };
            let result = self.apply_new_object_metadata_command_for_bucket_inner(
                pg_id,
                bucket,
                &recovery_command,
                work_budget,
                ObjectMetadataCommandApplyContext {
                    return_metadata_command_contention,
                    provenance,
                    recovery_guard: Some(&recovery_guard),
                },
            );
            match result {
                Ok(outcome) => {
                    let recovery_outcome = outcome
                        .pending_metadata_command_outcome()
                        .unwrap_or(PendingMetadataCommandOutcome::Abandoned);
                    self.complete_metadata_command_recovery_guard(
                        recovery_guard,
                        recovery_outcome,
                    );
                    return Ok(outcome);
                }
                Err(error) => {
                    let resolution = match &error {
                        ObjectPgActionError::Store(
                            StoreError::MetadataCommandOutcomeUnconfirmed { .. },
                        ) => Some(MetadataCommandRecoveryResolution::OutcomeUnconfirmed),
                        ObjectPgActionError::Store(
                            StoreError::MetadataCommandIrrevocableConvergencePending { .. },
                        ) => Some(
                            MetadataCommandRecoveryResolution::IrrevocableConvergencePending,
                        ),
                        _ => None,
                    };
                    if let Some(resolution) = resolution {
                        recovery_guard.mark_irreversible_handoff(resolution);
                        self.relinquish_metadata_command_recovery_guard(recovery_guard);
                    }
                    return Err(error);
                }
            }
        }
    }

    fn new_object_metadata_command_outcome_from_recovery(
        command: &MetadataCommandEnvelope,
        root_disposition: &MetadataCommandRecoveryRootDisposition,
        resolution: Option<MetadataCommandRecoveryResolution>,
    ) -> Result<Option<NewObjectMetadataCommandApplyOutcome>, ObjectPgActionError> {
        if matches!(
            root_disposition,
            MetadataCommandRecoveryRootDisposition::Abandoned { .. }
        ) {
            return match resolution {
                None | Some(MetadataCommandRecoveryResolution::Outcome(_)) => {
                    Ok(Some(NewObjectMetadataCommandApplyOutcome::Reinspect(
                        ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                            context: "new object metadata command was abandoned by a recovery derivative",
                        }),
                    )))
                }
                Some(MetadataCommandRecoveryResolution::OutcomeUnconfirmed) => {
                    Err(Self::object_metadata_command_outcome_unconfirmed_error(command))
                }
                Some(MetadataCommandRecoveryResolution::IrrevocableConvergencePending) => {
                    Err(Self::object_metadata_command_irrevocable_error(command))
                }
            };
        }
        let Some(resolution) = resolution else {
            return Ok(None);
        };
        let outcome = match resolution {
            MetadataCommandRecoveryResolution::Outcome(PendingMetadataCommandOutcome::Applied) => {
                NewObjectMetadataCommandApplyOutcome::Applied
            }
            MetadataCommandRecoveryResolution::Outcome(
                PendingMetadataCommandOutcome::PublishedPendingRecovery,
            ) => NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery,
            MetadataCommandRecoveryResolution::Outcome(
                PendingMetadataCommandOutcome::TerminalCleanupPending { applied: true },
            ) => NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending,
            MetadataCommandRecoveryResolution::Outcome(
                PendingMetadataCommandOutcome::Abandoned,
            ) => NewObjectMetadataCommandApplyOutcome::Reinspect(ObjectPgActionError::Store(
                StoreError::MetadataCommandContention {
                    context: "new object metadata command was abandoned by recovery",
                },
            )),
            MetadataCommandRecoveryResolution::Outcome(
                PendingMetadataCommandOutcome::TerminalCleanupPending { applied: false },
            ) => {
                return Err(ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery);
            }
            MetadataCommandRecoveryResolution::Outcome(
                PendingMetadataCommandOutcome::RetryPartialExactConflict,
            ) => return Ok(None),
            MetadataCommandRecoveryResolution::OutcomeUnconfirmed => {
                return Err(Self::object_metadata_command_outcome_unconfirmed_error(
                    command,
                ));
            }
            MetadataCommandRecoveryResolution::IrrevocableConvergencePending => {
                return Err(Self::object_metadata_command_irrevocable_error(command));
            }
        };
        Ok(Some(outcome))
    }

    fn abandon_definitively_unapplied_object_metadata_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
        work_budget: Option<&mut super::RequestWorkBudget>,
    ) -> Result<(), ObjectPgActionError> {
        let owned_recovery_guard = if let Some(recovery_guard) = recovery_guard {
            if !recovery_guard.owns_lineage_command(pg_id, command) {
                return Err(ObjectPgActionError::Store(
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation: "abandon-object-metadata-command",
                    },
                ));
            }
            None
        } else {
            let deadline = Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
            match self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery_until(pg_id, command, deadline)
            {
                MetadataCommandRecoveryAdmission::Leader(recovery_guard) => {
                    Some(recovery_guard)
                }
                MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery { .. }
                | MetadataCommandRecoveryAdmission::Waited { .. }
                | MetadataCommandRecoveryAdmission::TimedOut { .. } => {
                    return Err(
                        ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery,
                    );
                }
            }
        };
        self.record_abandoned_metadata_command_to_acting_set(command)
            .map_err(|error| {
                super::bucket_snapshot_error_to_object_pg_action_error(error.source)
            })?;
        let pending = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
        if pending.as_ref() != Some(command) {
            return Err(super::conflicting_pending_object_metadata_command(
                "pending object metadata command changed before abandoned cleanup",
            ));
        }
        self.release_metadata_command_bucket_write_reservation(command)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        match work_budget {
            Some(work_budget) => self
                .remove_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    command,
                    work_budget,
                )
                .map_err(ObjectPgActionError::from)?,
            None => self
                .remove_pending_metadata_command_for_bucket(pg_id, bucket, command)
                .map_err(ObjectPgActionError::from)?,
        };
        if let Some(recovery_guard) = owned_recovery_guard {
            let relinquished = self.complete_metadata_command_recovery_guard(
                recovery_guard,
                PendingMetadataCommandOutcome::Abandoned,
            );
            debug_assert!(!relinquished, "abandoned command cannot retain its pending slot");
        }
        Ok(())
    }

    fn abandon_definitively_unapplied_object_metadata_command_until(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
        deadline: Instant,
    ) -> Result<(), ObjectPgActionError> {
        let owned_recovery_guard = if let Some(recovery_guard) = recovery_guard {
            if !recovery_guard.owns_lineage_command(pg_id, command) {
                return Err(ObjectPgActionError::Store(
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation: "abandon-object-metadata-command-until",
                    },
                ));
            }
            None
        } else {
            let recovery_admission_deadline =
                Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
            match self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery_until(
                    pg_id,
                    command,
                    recovery_admission_deadline,
                )
            {
                MetadataCommandRecoveryAdmission::Leader(recovery_guard) => {
                    Some(recovery_guard)
                }
                MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery { .. }
                | MetadataCommandRecoveryAdmission::Waited { .. }
                | MetadataCommandRecoveryAdmission::TimedOut { .. } => {
                    return Err(
                        ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery,
                    );
                }
            }
        };
        self.record_abandoned_metadata_command_to_acting_set_until(command, deadline)
            .map_err(|error| {
                super::bucket_snapshot_error_to_object_pg_action_error(error.source)
            })?;
        let pending = self
            .pending_metadata_command_for_bucket_with_route_mode_until(
                pg_id,
                bucket,
                MetadataCommandRouteMode::Normal,
                command.id().cluster_epoch(),
                deadline,
            )
            .map_err(ObjectPgActionError::Store)?;
        if pending.as_ref() != Some(command) {
            return Err(super::conflicting_pending_object_metadata_command(
                "pending object metadata command changed before abandoned cleanup",
            ));
        }
        self.release_metadata_command_bucket_write_reservation_until(command, deadline)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        crate::node_client::require_metadata_command_operation_deadline(deadline)
            .map_err(ObjectPgActionError::Store)?;
        let cleanup = self
            .remove_pending_metadata_command_for_bucket_until(pg_id, bucket, command, deadline)
            .map_err(ObjectPgActionError::Store)?;
        if cleanup == PendingMetadataCommandTerminalCleanup::Deferred {
            return Err(ObjectPgActionError::Store(
                StoreError::OperationDeadlineExceeded {
                    context: "remove abandoned object metadata command pending slot",
                },
            ));
        }
        if let Some(recovery_guard) = owned_recovery_guard {
            let relinquished = self.complete_metadata_command_recovery_guard(
                recovery_guard,
                PendingMetadataCommandOutcome::Abandoned,
            );
            debug_assert!(!relinquished, "abandoned command cannot retain its pending slot");
        }
        Ok(())
    }

    fn finish_definitively_not_reissued_object_metadata_command_until(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        source: ObjectPgActionError,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
        deadline: Instant,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        let can_reinspect = object_pg_action_error_is_retryable_command_observation(&source);
        match self.abandon_definitively_unapplied_object_metadata_command_until(
            pg_id,
            bucket,
            command,
            recovery_guard,
            deadline,
        ) {
            Ok(()) if can_reinspect => {
                Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(source))
            }
            Ok(()) => Ok(NewObjectMetadataCommandApplyOutcome::Abandoned(source)),
            Err(error) if object_pg_action_error_is_retryable_command_observation(&error) => {
                if can_reinspect {
                    Err(Self::object_metadata_command_irrevocable_error(command))
                } else {
                    Err(source)
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(super) fn test_finish_definitively_not_reissued_object_metadata_command_until(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        source: ObjectPgActionError,
        deadline: Instant,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.finish_definitively_not_reissued_object_metadata_command_until(
            pg_id, bucket, command, source, None, deadline,
        )
    }

    fn apply_new_object_metadata_command_for_bucket_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
        context: ObjectMetadataCommandApplyContext<'_>,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        let ObjectMetadataCommandApplyContext {
            return_metadata_command_contention,
            provenance,
            recovery_guard,
        } = context;
        let mut command = command.clone();
        let mut retrying_definitively_unapplied_contention = false;
        let mut apply_progress = MetadataCommandApplyProgress::Abortable;
        let mut publication_may_have_applied = false;
        #[cfg(test)]
        maybe_run_before_object_metadata_command_apply_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            &command,
            work_budget,
        );
        loop {
            if let Err(error) =
                work_budget.check("object metadata command apply retry budget exhausted")
            {
                if retrying_definitively_unapplied_contention {
                    self.abandon_definitively_unapplied_object_metadata_command(
                        pg_id,
                        bucket,
                        &command,
                        recovery_guard,
                        None,
                    )?;
                    return Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                        ObjectPgActionError::Store(error),
                    ));
                }
                return self.finish_new_object_metadata_command_after_budget_exhaustion(
                    pg_id,
                    bucket,
                    &command,
                    error,
                    recovery_guard,
                );
            }
            retrying_definitively_unapplied_contention = false;
            let apply = match provenance {
                ObjectMetadataCommandApplyProvenance::New => self
                    .apply_metadata_command_to_acting_set_with_initial_progress(
                        &command,
                        apply_progress,
                    ),
                ObjectMetadataCommandApplyProvenance::RecoveredPending => self
                    .apply_recovered_pending_metadata_command_to_acting_set_with_initial_progress(
                        &command,
                        apply_progress,
                    ),
            };
            match apply {
                Ok(outcome) => {
                    if outcome == MetadataCommandApplyOutcome::Converged {
                        if let Some(recovery_guard) = recovery_guard {
                            recovery_guard.record_outcome(
                                PendingMetadataCommandOutcome::TerminalCleanupPending {
                                    applied: true,
                                },
                            );
                        }
                        let completion = if !self
                            .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                pg_id, &command,
                            )
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                        {
                            NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending
                        } else {
                            let cleanup = self
                                .remove_pending_metadata_command_for_bucket(
                                    pg_id,
                                    command.bucket_name(),
                                    &command,
                                )
                                .map_err(ObjectPgActionError::from)?;
                            if cleanup == PendingMetadataCommandTerminalCleanup::Deferred {
                                NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending
                            } else {
                                NewObjectMetadataCommandApplyOutcome::Applied
                            }
                        };
                        self.after_object_metadata_command_applied(&command);
                        #[cfg(test)]
                        crate::node::maybe_run_after_object_metadata_command_publish_hook(
                            self.metadata_primary_test_hook_node().test_hook_scope_id(),
                        )?;
                        return Ok(completion);
                    }
                    #[cfg(test)]
                    crate::node::maybe_run_after_object_metadata_command_publish_hook(
                        self.metadata_primary_test_hook_node().test_hook_scope_id(),
                    )?;
                    return Ok(NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery);
                }
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        progress: observed_progress,
                        may_have_applied,
                        source,
                    } = error;
                    apply_progress = apply_progress.merge(observed_progress);
                    publication_may_have_applied |= may_have_applied;
                    let command_is_irrevocable =
                        publication_may_have_applied || !apply_progress.is_abortable();
                    if apply_progress.is_abortable()
                        && return_metadata_command_contention
                        && metadata_command_apply_error_is_contention(&source)
                    {
                        return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                            source,
                        ));
                    }
                    if apply_progress.is_abortable()
                        && metadata_command_apply_error_is_contention(&source)
                    {
                        #[cfg(test)]
                        if !command_is_irrevocable {
                            maybe_run_object_metadata_command_definitive_retry_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                                &command,
                                &source,
                                work_budget,
                            );
                        }
                        if let Err(error) = work_budget.sleep_after_contention(
                            "object metadata command contention retry budget exhausted",
                        ) {
                            if !command_is_irrevocable {
                                self.abandon_definitively_unapplied_object_metadata_command(
                                    pg_id,
                                    bucket,
                                    &command,
                                    recovery_guard,
                                    None,
                                )?;
                                return Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                                    ObjectPgActionError::Store(error),
                                ));
                            }
                            return self.finish_new_object_metadata_command_after_budget_exhaustion(
                                pg_id,
                                bucket,
                                &command,
                                error,
                                recovery_guard,
                            );
                        }
                        retrying_definitively_unapplied_contention = !command_is_irrevocable;
                        continue;
                    }
                    if apply_progress.is_abortable()
                        && metadata_command_apply_transport_error_is_retryable(&source)
                    {
                        #[cfg(test)]
                        if !command_is_irrevocable {
                            maybe_run_object_metadata_command_definitive_retry_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                                &command,
                                &source,
                                work_budget,
                            );
                        }
                        if let Err(error) = work_budget.sleep_after_contention(
                            "object metadata command transport retry budget exhausted",
                        ) {
                            if !command_is_irrevocable {
                                self.abandon_definitively_unapplied_object_metadata_command(
                                    pg_id,
                                    bucket,
                                    &command,
                                    recovery_guard,
                                    None,
                                )?;
                                return Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                                    ObjectPgActionError::Store(error),
                                ));
                            }
                            return self.finish_new_object_metadata_command_after_budget_exhaustion(
                                pg_id,
                                bucket,
                                &command,
                                error,
                                recovery_guard,
                            );
                        }
                        retrying_definitively_unapplied_contention = !command_is_irrevocable;
                        continue;
                    }
                    if !apply_progress.is_abortable()
                        && (metadata_command_apply_error_can_handoff_to_recovery(&source)
                            || metadata_command_apply_error_requires_exact_confirmation(&source))
                    {
                        if let Err(error) = work_budget.sleep_after_contention(
                            "irrevocable object metadata command convergence budget exhausted",
                        ) {
                            return self.finish_new_object_metadata_command_after_budget_exhaustion(
                                pg_id,
                                bucket,
                                &command,
                                error,
                                recovery_guard,
                            );
                        }
                        continue;
                    }
                    match self
                        .retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            applied_nodes,
                            &source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        Some(true) => {
                            if let Some(recovery_guard) = recovery_guard {
                                recovery_guard.record_outcome(
                                    PendingMetadataCommandOutcome::TerminalCleanupPending {
                                        applied: true,
                                    },
                                );
                            }
                            if !self
                                .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                    pg_id, &command,
                                )
                                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                            {
                                self.after_object_metadata_command_applied(&command);
                                return Ok(
                                    NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending,
                                );
                            }
                            let cleanup = self
                                .remove_pending_metadata_command_for_bucket(
                                    pg_id,
                                    command.bucket_name(),
                                    &command,
                                )
                                .map_err(ObjectPgActionError::from)?;
                            self.after_object_metadata_command_applied(&command);
                            return Ok(if cleanup
                                == PendingMetadataCommandTerminalCleanup::Deferred
                            {
                                NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending
                            } else {
                                NewObjectMetadataCommandApplyOutcome::Applied
                            });
                        }
                        Some(false) => {
                            apply_progress = apply_progress
                                .merge(MetadataCommandApplyProgress::PublicationUnconfirmed);
                            if let Err(error) = work_budget.sleep_after_contention(
                                "partial exact object metadata command convergence budget exhausted",
                            ) {
                                return self
                                    .finish_new_object_metadata_command_after_budget_exhaustion(
                                        pg_id,
                                        bucket,
                                        &command,
                                        error,
                                        recovery_guard,
                                    );
                            }
                            continue;
                        }
                        None => {}
                    }
                    if apply_progress.is_abortable()
                        && applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        #[cfg(test)]
                        maybe_run_before_object_metadata_command_reissue_hook(
                            self.metadata_command_apply_test_hook_scope_id(),
                            &command,
                        );
                        let reissued = match self
                            .reissue_pending_metadata_command_outcome_with_route_mode_classified_until_with_recovery_guard(
                                pg_id,
                                &command,
                                MetadataCommandExecutionRoute::normal(),
                                command.payload(),
                                work_budget.deadline(),
                                recovery_guard,
                            )
                        {
                            Ok(super::ReissuePendingMetadataCommandOutcome::Reissued(reissued)) => {
                                retrying_definitively_unapplied_contention =
                                    !command_is_irrevocable;
                                reissued
                            }
                            Ok(super::ReissuePendingMetadataCommandOutcome::MatchingCurrent(
                                current,
                            )) => {
                                apply_progress = apply_progress
                                    .merge(MetadataCommandApplyProgress::PublicationUnconfirmed);
                                current
                            }
                            Ok(super::ReissuePendingMetadataCommandOutcome::Missing) => {
                                return Err(super::conflicting_pending_object_metadata_command(
                                    "pending object metadata command was displaced during reissue",
                                ));
                            }
                            Ok(super::ReissuePendingMetadataCommandOutcome::MatchingCurrentConflict {
                                command: current,
                                ..
                            }) => {
                                apply_progress = apply_progress
                                    .merge(MetadataCommandApplyProgress::PublicationUnconfirmed);
                                current
                            }
                            Err(super::ReissuePendingMetadataCommandFailure::DefinitelyNotReissued(
                                source,
                            )) if !command_is_irrevocable => {
                                let source =
                                    super::bucket_snapshot_error_to_object_pg_action_error(source);
                                return self
                                    .finish_definitively_not_reissued_object_metadata_command_until(
                                        pg_id,
                                        bucket,
                                        &command,
                                        source,
                                        recovery_guard,
                                        work_budget.deadline(),
                                    );
                            }
                            Err(super::ReissuePendingMetadataCommandFailure::DefinitelyNotReissued(
                                source,
                            )) => {
                                let lineage_tip = recovery_guard
                                    .map(MetadataCommandRecoveryGuard::lineage_tip)
                                    .unwrap_or_else(|| command.clone());
                                return self.finish_new_object_metadata_command_after_uncertainty(
                                    pg_id,
                                    bucket,
                                    &lineage_tip,
                                    super::bucket_snapshot_error_to_object_pg_action_error(source),
                                    recovery_guard,
                                );
                            }
                            Err(super::ReissuePendingMetadataCommandFailure::MayHaveReissued {
                                source,
                                lineage_tip,
                            }) => {
                                return self.finish_new_object_metadata_command_after_uncertainty(
                                    pg_id,
                                    bucket,
                                    &lineage_tip,
                                    super::bucket_snapshot_error_to_object_pg_action_error(source),
                                    recovery_guard,
                                );
                            }
                        };
                        command = reissued;
                        #[cfg(test)]
                        if maybe_force_pending_object_metadata_partial_conflict_hook(
                            self.metadata_command_apply_test_hook_scope_id(),
                            &command,
                        ) {
                            work_budget.expire_for_test();
                        }
                        continue;
                    }
                    if apply_progress.is_abortable()
                        && applied_nodes == 0
                        && super::StorageCluster::reserve_object_version_conflict_matches(
                            &command, &source,
                        )
                    {
                        self.abandon_definitively_unapplied_object_metadata_command(
                            pg_id,
                            bucket,
                            &command,
                            recovery_guard,
                            Some(work_budget),
                        )?;
                        return Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                            super::bucket_snapshot_error_to_object_pg_action_error(source),
                        ));
                    }
                    if apply_progress.is_abortable() && applied_nodes == 0 {
                        let can_reinspect =
                            metadata_command_apply_error_can_reinspect_after_abandonment(&source);
                        self.abandon_definitively_unapplied_object_metadata_command(
                            pg_id,
                            bucket,
                            &command,
                            recovery_guard,
                            Some(work_budget),
                        )?;
                        if can_reinspect {
                            return Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                                super::bucket_snapshot_error_to_object_pg_action_error(source),
                            ));
                        }
                        return Ok(NewObjectMetadataCommandApplyOutcome::Abandoned(
                            super::bucket_snapshot_error_to_object_pg_action_error(source),
                        ));
                    }
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        source,
                    ));
                }
            }
        }
    }

    fn finish_new_object_metadata_command_after_budget_exhaustion(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        budget_error: StoreError,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        self.finish_new_object_metadata_command_after_uncertainty(
            pg_id,
            bucket,
            command,
            ObjectPgActionError::Store(budget_error),
            recovery_guard,
        )
    }

    pub(super) fn finish_new_object_metadata_command_after_uncertainty(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        fallback_error: ObjectPgActionError,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        let confirmation_deadline =
            Instant::now() + METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET;
        self.finish_new_object_metadata_command_after_uncertainty_until(
            pg_id,
            bucket,
            command,
            fallback_error,
            recovery_guard,
            confirmation_deadline,
        )
    }

    pub(super) fn finish_new_object_metadata_command_after_uncertainty_until(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        fallback_error: ObjectPgActionError,
        recovery_guard: Option<&MetadataCommandRecoveryGuard>,
        confirmation_deadline: Instant,
    ) -> Result<NewObjectMetadataCommandApplyOutcome, ObjectPgActionError> {
        let can_reinspect =
            object_pg_action_error_is_retryable_command_observation(&fallback_error);
        match self
            .metadata_command_publication_state_on_acting_set_until(
                pg_id,
                command,
                MetadataCommandRouteMode::Normal,
                confirmation_deadline,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        {
            MetadataCommandPublicationState::Published => {
                #[cfg(test)]
                crate::node::maybe_run_after_object_metadata_command_publish_hook(
                    self.metadata_primary_test_hook_node().test_hook_scope_id(),
                )?;
                if can_reinspect {
                    Ok(NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery)
                } else {
                    Err(fallback_error)
                }
            }
            MetadataCommandPublicationState::PublicationStarted
            | MetadataCommandPublicationState::Witnessed
            | MetadataCommandPublicationState::PublicationUnconfirmed
            | MetadataCommandPublicationState::IrrevocableUnconfirmed => {
                if can_reinspect {
                    Err(Self::object_metadata_command_irrevocable_error(command))
                } else {
                    Err(fallback_error)
                }
            }
            MetadataCommandPublicationState::NotPublished => {
                if let Err(error) = self
                    .abandon_definitively_unapplied_object_metadata_command_until(
                        pg_id,
                        bucket,
                        command,
                        recovery_guard,
                        confirmation_deadline,
                    )
                {
                    if object_pg_action_error_is_retryable_command_observation(&error) {
                        return if can_reinspect {
                            Err(Self::object_metadata_command_outcome_unconfirmed_error(
                                command,
                            ))
                        } else {
                            Err(fallback_error)
                        };
                    }
                    return Err(error);
                }
                if can_reinspect {
                    Ok(NewObjectMetadataCommandApplyOutcome::Reinspect(
                        fallback_error,
                    ))
                } else {
                    Ok(NewObjectMetadataCommandApplyOutcome::Abandoned(
                        fallback_error,
                    ))
                }
            }
        }
    }

    fn object_metadata_command_irrevocable_error(
        command: &MetadataCommandEnvelope,
    ) -> ObjectPgActionError {
        let id = command.id();
        ObjectPgActionError::Store(StoreError::MetadataCommandIrrevocableConvergencePending {
            pg_id: id.pg_id().get(),
            cluster_epoch: id.cluster_epoch(),
            log_index: id.log_index().get(),
        })
    }

    fn object_metadata_command_outcome_unconfirmed_error(
        command: &MetadataCommandEnvelope,
    ) -> ObjectPgActionError {
        let id = command.id();
        ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed {
            pg_id: id.pg_id().get(),
            cluster_epoch: id.cluster_epoch(),
            log_index: id.log_index().get(),
        })
    }

    fn acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        match self.acquire_durable_bucket_write_reservation_with_effect_fence(
            bucket,
            operation_kind,
            Some(key.as_str()),
            Some(effect_fence),
        ) {
            Ok(reservation) => Ok(Some(BucketWriteReservationProof::from(&reservation.record))),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                self.wait_for_durable_bucket_write_drain(bucket)
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                Ok(None)
            }
            Err(error) => Err(super::bucket_snapshot_error_to_object_pg_action_error(
                error,
            )),
        }
    }

    fn try_acquire_bucket_write_proof_for_object_metadata_command(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        wait_for_drain: bool,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        match self.acquire_durable_bucket_write_reservation(
            bucket,
            operation_kind,
            Some(key.as_str()),
        ) {
            Ok(reservation) => Ok(Some(BucketWriteReservationProof::from(&reservation.record))),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                if wait_for_drain {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                }
                Ok(None)
            }
            Err(error) => Err(super::bucket_snapshot_error_to_object_pg_action_error(
                error,
            )),
        }
    }

    fn try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        crate::node::maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(bucket);
        let Some(proof) = self.try_acquire_bucket_write_proof_for_object_metadata_command(
            bucket,
            key,
            operation_kind,
            false,
        )?
        else {
            return Ok(None);
        };
        if proof.bucket_incarnation_generation == expected_bucket_incarnation_generation {
            return Ok(Some(proof));
        }
        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
        Ok(None)
    }

    fn release_bucket_write_proof_for_object_metadata_command(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), ObjectPgActionError> {
        self.release_bucket_write_reservation_proof(proof)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn deleted_specific_from_command_target(
        target: &DeleteObjectVersionTarget,
    ) -> DeletedSpecificObjectVersion {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => {
                DeletedSpecificObjectVersion::DeleteMarker
            }
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                ..
            } => DeletedSpecificObjectVersion::Live {
                generation_id: *generation_id,
                layout: *layout,
            },
        }
    }

    fn deleted_current_from_command_target(
        target: &DeleteObjectVersionTarget,
    ) -> DeletedCurrentObject {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => DeletedCurrentObject::DeleteMarker,
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                ..
            } => DeletedCurrentObject::Live {
                generation_id: *generation_id,
                layout: *layout,
            },
        }
    }

    pub(super) fn delete_specific_object_version_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(DeleteSpecificObjectVersionIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        let Some(version_id) = requested_version_id else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "specific-version delete route is missing its version id".to_string(),
            });
        };
        let pg_id = object_pg_id.pg_id();
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, version_id) {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot =
                            storage_client.load_specific_object_delete_snapshot(version_id)?;
                        let value = match action(snapshot.stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                            value,
                            deleted: Self::deleted_specific_from_command_target(&delete.target),
                        }));
                    }
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_specific_object_delete_snapshot(version_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if snapshot.stored.is_none() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                    value,
                    deleted: DeletedSpecificObjectVersion::Missing,
                }));
            }
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = storage_client.build_delete_specific_object_version_command(
                BuildDeleteSpecificObjectVersionCommandReq {
                    version_id,
                    expected_stored: snapshot.stored.as_ref(),
                    expected_target: snapshot.target.as_ref(),
                    expected_version_list: None,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                        value,
                        deleted: DeletedSpecificObjectVersion::Missing,
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
                &mut evaluated_attempt,
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
                unreachable!("new delete object command changed payload kind");
            };
            return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                value,
                deleted: Self::deleted_specific_from_command_target(&delete.target),
            }));
        }
    }

    pub(super) fn delete_current_object_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(DeleteCurrentObjectIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        if requested_version_id.is_some() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "current-object delete route unexpectedly contains a version id"
                    .to_string(),
            });
        }
        let pg_id = object_pg_id.pg_id();
        let mut work_budget = super::RequestWorkBudget::new(
            super::BUCKET_WRITE_DRAIN_RETRY_BUDGET,
            None,
        )
        .for_operation("delete_current_object")
        .for_pg(pg_id);
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();

        loop {
            work_budget
                .check("delete current object retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        if snapshot
                            .stored
                            .as_ref()
                            .is_some_and(|stored| stored.version_id() == delete.version_id)
                        {
                            let value = match action(snapshot.stored.as_ref()) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            require_valid_route().map_err(ObjectPgActionError::Store)?;
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(DeleteCurrentObjectOutcome {
                                value,
                                deleted: Self::deleted_current_from_command_target(&delete.target),
                            }));
                        }
                    }
                }
                let _ = self.drain_pending_object_metadata_command_with_work_budget(
                    publisher,
                    pg_id,
                    &command,
                    &mut work_budget,
                )?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = self.maybe_run_after_object_metadata_reservation_acquired_hook() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(error);
            }
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            let Some(stored) = snapshot.stored.as_ref() else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::Missing,
                }));
            };
            let StoredObject::Live(_) = stored else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::DeleteMarker,
                }));
            };
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = storage_client.build_delete_current_object_command(
                BuildDeleteCurrentObjectCommandReq {
                    expected_current: Some(stored),
                    expected_target: snapshot.target.as_ref(),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(DeleteCurrentObjectOutcome {
                        value,
                        deleted: DeletedCurrentObject::Missing,
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_one_pending_object_metadata_command_with_work_budget(
                        publisher,
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self
                .install_snapshot_sensitive_metadata_command_or_drain_with_work_budget(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                    &mut evaluated_attempt,
                )
            {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            match self.apply_new_object_metadata_command_for_bucket_or_reinspect(
                pg_id,
                bucket,
                &command,
                &mut work_budget,
            )? {
                NewObjectMetadataCommandApplyOutcome::Applied
                | NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
                | NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => {}
                NewObjectMetadataCommandApplyOutcome::Reinspect(_) => continue,
                NewObjectMetadataCommandApplyOutcome::Abandoned(error) => return Err(error),
            }
            let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
                unreachable!("new delete object command changed payload kind");
            };
            return Ok(Ok(DeleteCurrentObjectOutcome {
                value,
                deleted: Self::deleted_current_from_command_target(&delete.target),
            }));
        }
    }

    fn retry_insert_delete_marker_after_command_observation_error(
        &self,
        error: ObjectPgActionError,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        if !object_pg_action_error_is_retryable_command_observation(&error) {
            return Err(error);
        }
        work_budget
            .sleep_after_contention(
                "insert delete marker contender observation retry budget exhausted",
            )
            .map_err(ObjectPgActionError::Store)
    }

    pub(super) fn insert_current_delete_marker_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(InsertCurrentDeleteMarkerIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        if requested_version_id.is_some() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "delete-marker insertion route unexpectedly contains a version id"
                    .to_string(),
            });
        }
        let pg_id = object_pg_id.pg_id();
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("insert_current_delete_marker")
        .for_pg(pg_id);
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();
        #[cfg(test)]
        let mut abandoned_hook_command = None;

        loop {
            #[cfg(test)]
            if let Some(command) = abandoned_hook_command.take() {
                maybe_run_object_metadata_command_abandoned_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    &command,
                );
            }
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() {
                    if marker.matches_request(bucket, key) {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        let value = match action(snapshot.stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        if self
                            .apply_exact_pending_object_metadata_command_or_reinspect_with_work_budget(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                                &mut work_budget,
                            )?
                            == super::ExactPendingObjectMetadataCommandOutcome::Reinspect
                        {
                            continue;
                        }
                        return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                            value,
                            version_id: marker.version_id,
                        }));
                    }
                }
                if let Err(error) = self.drain_pending_object_metadata_command_with_work_budget(
                    publisher,
                    pg_id,
                    &command,
                    &mut work_budget,
                ) {
                    self.retry_insert_delete_marker_after_command_observation_error(
                        error,
                        &mut work_budget,
                    )?;
                }
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if let Err(error) =
                work_budget.check("insert delete marker retry budget exhausted")
            {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let null_snapshot = if versioning == BucketVersioningState::Suspended {
                if let Err(error) = require_valid_route() {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(ObjectPgActionError::Store(error));
                }
                match storage_client.load_specific_object_delete_snapshot(VersionId::Null) {
                    Ok(snapshot) => Some(snapshot),
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            let marker_vid = match versioning {
                BucketVersioningState::Enabled => {
                    match self.reserve_next_object_version_with_effect_fence(
                        pg_id,
                        bucket,
                        key,
                        effect_fence,
                        &mut require_valid_route,
                    ) {
                        Ok(marker_vid) => marker_vid,
                        Err(error) => {
                            self.release_bucket_write_proof_for_object_metadata_command(
                                &bucket_write_reservation,
                            )?;
                            return Err(error);
                        }
                    }
                }
                BucketVersioningState::Suspended => VersionId::Null,
                BucketVersioningState::Disabled => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(MetadataError::InvariantViolation {
                        context: "insert delete marker with disabled versioning",
                        reason: "delete markers require enabled or suspended versioning".into(),
                    }
                    .into());
                }
            };
            let stale_payload = if versioning == BucketVersioningState::Suspended {
                InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: crate::clock::current_time_millis(),
                }
            } else {
                InsertDeleteMarkerStalePayload::Explicit(None)
            };
            let command = storage_client.build_insert_delete_marker_command(
                BuildInsertDeleteMarkerCommandReq {
                    version_id: marker_vid,
                    owner: &owner,
                    expected_current: snapshot.stored.as_ref(),
                    stale_payload,
                    expected_stale_payload_source: null_snapshot.as_ref().and_then(|snapshot| {
                        match snapshot.stored.as_ref() {
                            Some(stored @ StoredObject::Live(_)) => Some(stored),
                            Some(StoredObject::DeleteMarker(_)) | None => None,
                        }
                    }),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    if let Err(error) = self
                        .drain_one_pending_object_metadata_command_with_work_budget(
                        publisher,
                        pg_id,
                        bucket,
                        &mut work_budget,
                    ) {
                        self.retry_insert_delete_marker_after_command_observation_error(
                            error,
                            &mut work_budget,
                        )?;
                    }
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.retry_insert_delete_marker_after_command_observation_error(
                        error,
                        &mut work_budget,
                    )?;
                    continue;
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let mut install_may_have_applied = false;
            let install = loop {
                match self
                    .install_snapshot_sensitive_metadata_command_or_drain_with_work_budget_classified(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                    &mut install_may_have_applied,
                    &mut evaluated_attempt,
                ) {
                    Ok(install) => break install,
                    Err(error)
                        if object_pg_action_error_is_retryable_command_observation(&error) =>
                    {
                        if let Err(retry_error) = self
                            .retry_insert_delete_marker_after_command_observation_error(
                            error,
                            &mut work_budget,
                        ) {
                            if install_may_have_applied {
                                return Err(ObjectPgActionError::Store(
                                    StoreError::MetadataCommandOutcomeUnconfirmed {
                                        pg_id: command.id().pg_id().get(),
                                        cluster_epoch: command.id().cluster_epoch(),
                                        log_index: command.id().log_index().get(),
                                    },
                                ));
                            }
                            self.release_bucket_write_proof_for_object_metadata_command(
                                &bucket_write_reservation,
                            )?;
                            return Err(retry_error);
                        }
                    }
                    Err(error) => {
                        if install_may_have_applied {
                            return Err(error);
                        }
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            match self.apply_new_object_metadata_command_for_bucket_or_reinspect(
                pg_id,
                bucket,
                &command,
                &mut work_budget,
            )? {
                NewObjectMetadataCommandApplyOutcome::Applied
                | NewObjectMetadataCommandApplyOutcome::PublishedPendingRecovery
                | NewObjectMetadataCommandApplyOutcome::TerminalCleanupPending => {}
                NewObjectMetadataCommandApplyOutcome::Reinspect(_) => {
                    #[cfg(test)]
                    {
                        abandoned_hook_command = Some(command.clone());
                    }
                    continue;
                }
                NewObjectMetadataCommandApplyOutcome::Abandoned(error) => return Err(error),
            }
            return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                value,
                version_id: marker_vid,
            }));
        }
    }

    #[cfg(test)]
    pub(crate) fn delete_current_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        self.delete_current_object_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id: None,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            action,
        )
    }

    #[cfg(test)]
    pub(crate) fn insert_current_delete_marker_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        self.insert_current_delete_marker_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id: None,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            versioning,
            owner,
            action,
        )
    }

    pub fn expire_current_object_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        should_expire: impl FnMut(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, crate::LifecycleMutationFailure>
    {
        self.expire_current_object_if_due_raw(
            bucket,
            key,
            expected_version_id,
            expected_bucket_incarnation_generation,
            should_expire,
        )
        .map_err(crate::LifecycleMutationFailure::from_object_pg_action)
    }

    pub(crate) fn expire_current_object_if_due_raw<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        mut should_expire: impl FnMut(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(ExpireCurrentObjectIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(None));
        };
        let BucketLifecycleContext {
            bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(None));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
        let owner = OwnerIdentity::new(
            bucket_info.owner_principal.clone(),
            bucket_info.owner_canonical_id.clone(),
        );
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();
        loop {
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                match command.payload() {
                    MetadataCommandPayload::DeleteObjectVersion(delete)
                        if delete.matches_request(bucket, key, expected_version_id) =>
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(None));
                        }
                        let snapshot = storage_client
                            .load_specific_object_delete_snapshot(expected_version_id)?;
                        let Some(StoredObject::Live(record)) = snapshot.stored else {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        };
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due.then_some(ExpireCurrentObjectOutcome {
                            reclaim_generation_id: super::delete_object_version_reclaim_generation(
                                &delete.target,
                            ),
                        })));
                    }
                    MetadataCommandPayload::InsertDeleteMarker(marker)
                        if marker.matches_request(bucket, key) =>
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(None));
                        }
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        let Some(StoredObject::Live(record)) = snapshot.stored else {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        };
                        if record.version_id != expected_version_id {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        }
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        let reclaim_generation_id =
                            super::object_payload_reclaim_generation(&marker.stale_payload);
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due.then_some(ExpireCurrentObjectOutcome {
                            reclaim_generation_id,
                        })));
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                        continue;
                    }
                }
            }

            let bucket_write_reservation = match self
                .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    match bucket_info.versioning {
                        BucketVersioningState::Disabled => {
                            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND
                        }
                        BucketVersioningState::Enabled | BucketVersioningState::Suspended => {
                            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND
                        }
                    },
                    bucket_incarnation_generation,
                )? {
                Some(proof) => proof,
                None => return Ok(Ok(None)),
            };
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let Some(StoredObject::Live(record)) = snapshot.stored.as_ref() else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            };
            if record.version_id != expected_version_id {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            }
            let due = match should_expire(raw_lifecycle.as_deref(), record) {
                Ok(due) => due,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if !due {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            }
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();

            let command = match bucket_info.versioning {
                BucketVersioningState::Disabled => storage_client
                    .build_delete_current_object_command(BuildDeleteCurrentObjectCommandReq {
                        expected_current: snapshot.stored.as_ref(),
                        expected_target: snapshot.target.as_ref(),
                        bucket_write_reservation: &bucket_write_reservation,
                    })
                    .and_then(|command| command.ok_or(ObjectPgActionError::StaleObjectReadSubject)),
                BucketVersioningState::Enabled => {
                    let marker_vid = match self.reserve_next_object_version(pg_id, bucket, key) {
                        Ok(marker_vid) => marker_vid,
                        Err(error) => {
                            self.release_bucket_write_proof_for_object_metadata_command(
                                &bucket_write_reservation,
                            )?;
                            return Err(error);
                        }
                    };
                    storage_client.build_insert_delete_marker_command(
                        BuildInsertDeleteMarkerCommandReq {
                            expected_current: snapshot.stored.as_ref(),
                            version_id: marker_vid,
                            owner: &owner,
                            stale_payload: InsertDeleteMarkerStalePayload::Explicit(None),
                            expected_stale_payload_source: None,
                            bucket_write_reservation: &bucket_write_reservation,
                        },
                    )
                }
                BucketVersioningState::Suspended => {
                    match storage_client.load_specific_object_delete_snapshot(VersionId::Null) {
                        Ok(null_snapshot) => storage_client.build_insert_delete_marker_command(
                            BuildInsertDeleteMarkerCommandReq {
                                expected_current: snapshot.stored.as_ref(),
                                version_id: VersionId::Null,
                                owner: &owner,
                                stale_payload:
                                    InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                                        created_at: crate::clock::current_time_millis(),
                                    },
                                expected_stale_payload_source: null_snapshot.stored.as_ref(),
                                bucket_write_reservation: &bucket_write_reservation,
                            },
                        ),
                        Err(error) => Err(error),
                    }
                }
            };
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(None));
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let reclaim_generation_id = match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete) => {
                    super::delete_object_version_reclaim_generation(&delete.target)
                }
                MetadataCommandPayload::InsertDeleteMarker(marker) => {
                    super::object_payload_reclaim_generation(&marker.stale_payload)
                }
                _ => unreachable!("lifecycle current expiry command changed payload kind"),
            };
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                None,
                &mut evaluated_attempt,
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(Some(ExpireCurrentObjectOutcome {
                reclaim_generation_id,
            })));
        }
    }

    pub fn delete_noncurrent_live_versions_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_bucket_incarnation_generation: u64,
        select_versions: impl FnMut(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, crate::LifecycleMutationFailure> {
        self.delete_noncurrent_live_versions_if_due_raw(
            bucket,
            key,
            expected_bucket_incarnation_generation,
            select_versions,
        )
        .map_err(crate::LifecycleMutationFailure::from_object_pg_action)
    }

    pub(crate) fn delete_noncurrent_live_versions_if_due_raw<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_bucket_incarnation_generation: u64,
        mut select_versions: impl FnMut(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(DeleteNoncurrentLiveVersionsIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(Vec::new()));
        };
        let BucketLifecycleContext {
            bucket_info: _bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(Vec::new()));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
        let mut completed_reclaimed_generation_ids = Vec::new();
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();

        'retry: loop {
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(completed_reclaimed_generation_ids));
                        }
                        let versions = storage_client.list_object_versions_for_lifecycle()?;
                        if versions.is_empty() {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(Vec::new()));
                        }
                        let due_version_ids =
                            match select_versions(raw_lifecycle.as_deref(), &versions) {
                                Ok(version_ids) => version_ids,
                                Err(error) => return Ok(Err(error)),
                            };
                        let reclaim_generation_id = due_version_ids
                            .contains(&delete.version_id)
                            .then(|| {
                                super::delete_object_version_reclaim_generation(&delete.target)
                            })
                            .flatten();
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(reclaim_generation_id.into_iter().collect()));
                    }
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                continue;
            }

            let versions = storage_client.list_object_versions_for_lifecycle()?;
            if versions.is_empty() {
                return Ok(Ok(Vec::new()));
            }
            let due_version_ids = match select_versions(raw_lifecycle.as_deref(), &versions) {
                Ok(version_ids) => version_ids,
                Err(error) => return Ok(Err(error)),
            };
            if due_version_ids.is_empty() {
                return Ok(Ok(completed_reclaimed_generation_ids));
            }
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();

            let mut delete_targets = Vec::new();
            for stored in &versions {
                let Some(record) = stored.as_live() else {
                    continue;
                };
                if !due_version_ids.contains(&record.version_id) {
                    continue;
                }
                delete_targets.push(stored.clone());
            }

            for stored in delete_targets {
                let version_id = stored.version_id();
                let snapshot = match storage_client.load_specific_object_delete_snapshot(version_id)
                {
                    Ok(snapshot) if snapshot.stored.as_ref() == Some(&stored) => snapshot,
                    Ok(_) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        return Ok(Ok(completed_reclaimed_generation_ids));
                    }
                    Err(error) => return Err(error),
                };
                let bucket_write_reservation = match self
                    .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                        bucket,
                        key,
                        DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                        bucket_incarnation_generation,
                    )? {
                    Some(proof) => proof,
                    None => return Ok(Ok(completed_reclaimed_generation_ids)),
                };
                let command = storage_client.build_delete_specific_object_version_command(
                    BuildDeleteSpecificObjectVersionCommandReq {
                        version_id,
                        expected_stored: snapshot.stored.as_ref(),
                        expected_target: snapshot.target.as_ref(),
                        expected_version_list: Some(&versions),
                        bucket_write_reservation: &bucket_write_reservation,
                    },
                );
                let command = match command {
                    Ok(Some(command)) => command,
                    Ok(None) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Ok(Ok(completed_reclaimed_generation_ids));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                        continue 'retry;
                    }
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
                let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    None,
                    &mut evaluated_attempt,
                ) {
                    Ok(install) => install,
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
                match install {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                    | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        continue 'retry;
                    }
                }
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if let Some(generation_id) =
                        super::delete_object_version_reclaim_generation(&delete.target)
                    {
                        completed_reclaimed_generation_ids.push(generation_id);
                    }
                }
            }
            return Ok(Ok(completed_reclaimed_generation_ids));
        }
    }

    pub fn delete_expired_delete_marker_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        should_delete: impl FnMut(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, crate::LifecycleMutationFailure> {
        self.delete_expired_delete_marker_if_due_raw(
            bucket,
            key,
            expected_version_id,
            expected_bucket_incarnation_generation,
            should_delete,
        )
        .map_err(crate::LifecycleMutationFailure::from_object_pg_action)
    }

    pub(crate) fn delete_expired_delete_marker_if_due_raw<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        mut should_delete: impl FnMut(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(DeleteExpiredDeleteMarkerIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_info: _bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();

        loop {
            let pending_command = if snapshot_retry_phase.pending_drain_allowed() {
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            } else {
                None
            };
            if let Some(command) = pending_command {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, expected_version_id)
                        && matches!(
                            delete.target,
                            DeleteObjectVersionTarget::DeleteMarker { .. }
                        )
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(false));
                        }
                        let versions = storage_client.list_object_versions_for_lifecycle()?;
                        if versions.is_empty() {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(false));
                        }
                        let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due));
                    }
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                continue;
            }

            let bucket_write_reservation = match self
                .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                    bucket_incarnation_generation,
                )? {
                Some(proof) => proof,
                None => return Ok(Ok(false)),
            };
            let versions = match storage_client.list_object_versions_for_lifecycle() {
                Ok(versions) => versions,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if versions.is_empty() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            }
            let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                Ok(due) => due,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if !due {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            }
            let Some(expected_marker) = versions
                .iter()
                .find(|stored| stored.version_id() == expected_version_id)
                .filter(|stored| matches!(stored, StoredObject::DeleteMarker(_)))
            else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            };
            let snapshot =
                match storage_client.load_specific_object_delete_snapshot(expected_version_id) {
                    Ok(snapshot) if snapshot.stored.as_ref() == Some(expected_marker) => snapshot,
                    Ok(_) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Ok(Ok(false));
                    }
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
            let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
            let command = storage_client.build_delete_specific_object_version_command(
                BuildDeleteSpecificObjectVersionCommandReq {
                    version_id: expected_version_id,
                    expected_stored: snapshot.stored.as_ref(),
                    expected_target: snapshot.target.as_ref(),
                    expected_version_list: Some(&versions),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(false));
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                None,
                &mut evaluated_attempt,
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveInstallOutcome::Installed => {}
                super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot
                | super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(true));
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn acquire_object_payload_lease(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<ObjectPayloadLease, StoreError> {
        let runtime_state = self.ensure_object_payload_lease_allowed(bucket, key, generation_id)?;
        let mut acquired =
            self.local_map
                .try_acquire_object_payload_lease(bucket, key, generation_id)?;
        if acquired.node_leases.is_empty() {
            if let Some(error) = acquired.unavailable_error.take() {
                return Err(error);
            }
            return Err(StoreError::NotFound);
        }
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            acquired,
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
            self.object_metadata_pg_id(bucket, key),
        ))
    }

    pub(crate) fn acquire_object_payload_lease_for_shard_locations(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        locations: &[super::ShardLocation],
    ) -> Result<ObjectPayloadLease, StoreError> {
        let runtime_state = self.ensure_object_payload_lease_allowed(bucket, key, generation_id)?;
        let node_leases = self
            .local_map
            .try_acquire_object_payload_lease_on_locations(bucket, key, generation_id, locations)?;
        if !locations.is_empty() && node_leases.is_empty() {
            return Err(StoreError::NotFound);
        }
        let acquired = super::AcquiredObjectPayloadNodeLeases {
            node_leases,
            leased_node_ids: locations
                .iter()
                .map(super::ShardLocation::node_id)
                .collect(),
            unavailable_error: None,
            reclaim_fenced: false,
        };
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            acquired,
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
            self.object_metadata_pg_id(bucket, key),
        ))
    }

    /// Acquires subject-bound read authority for exactly the opaque payload
    /// segments a caller intends to read. Storage owns placement,
    /// historical-route expansion, and the deletion-exclusion lease.
    pub fn acquire_object_payload_read<'a>(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a ObjectPayloadSegment>,
    ) -> Result<ActiveObjectPayloadRead, ObjectReadFailure> {
        let segments = segments.into_iter().cloned().collect::<Vec<_>>();
        let lease = self
            .acquire_object_payload_read_lease_inner(
                bucket,
                key,
                generation_id,
                segments.iter(),
            )
            .map_err(ObjectReadFailure::from_store)?;
        Ok(ActiveObjectPayloadRead {
            cluster: std::sync::Arc::clone(self),
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            segments,
            lease: std::sync::Mutex::new(Some(lease)),
        })
    }

    pub(crate) fn acquire_object_payload_read_lease_inner<'a>(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a ObjectPayloadSegment>,
    ) -> Result<ObjectPayloadLease, StoreError> {
        let mut locations = Vec::new();
        for segment in segments {
            if !segment.matches_subject(bucket, key, generation_id) {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: "payload segment does not match lease subject".to_string(),
                });
            }
            let request = segment.stored_bytes_request();
            if request.stored_size == 0 {
                continue;
            }
            locations.extend(self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch(),
                &request,
            )?);
        }
        self.acquire_object_payload_lease_for_shard_locations(
            bucket,
            key,
            generation_id,
            &locations,
        )
    }

    fn ensure_object_payload_lease_allowed(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<std::sync::Arc<LocalClusterRuntimeState>, StoreError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        let runtime_state = self.local_map.runtime_state();
        let route_state = self
            .local_map
            .pg_route(pg_id)
            .expect("validated metadata read route must exist")
            .state();
        if route_state == PgState::Active
            && self
                .pending_metadata_command_for_bucket(pg_id, bucket)?
                .is_some_and(|command| {
                    matches!(
                        command.payload(),
                        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                            if delete.matches_request(bucket, key, generation_id)
                    )
                })
        {
            return Err(StoreError::NotFound);
        }
        Ok(runtime_state)
    }

    #[cfg(test)]
    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map
            .object_payload_lease_count(bucket, key, generation_id)
            .expect("test payload lease count should be readable")
    }

    #[cfg(test)]
    pub(crate) fn object_payload_lease_holder_node_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map
            .object_payload_lease_holder_node_count(bucket, key, generation_id)
    }

    #[cfg(test)]
    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map.bucket_object_payload_lease_count(bucket)
    }

    pub fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let _ = self.enqueue_object_payload_reclaim_for_pg(bucket, key, generation_id);
    }

    fn enqueue_object_payload_reclaim_for_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Option<ReclaimQueueInsert> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let outcome = self
            .local_map
            .runtime_state()
            .enqueue_object_payload_reclaim(bucket, key, generation_id, pg_id.get());
        let _ = observability::emit_object_payload_reclaim_event(
            super::TRACE_TARGET,
            observability::ObjectPayloadReclaimEventSummary {
                pg_id: pg_id.get(),
                event: match outcome {
                    ReclaimQueueInsert::Queued => "queued",
                    ReclaimQueueInsert::Deduplicated => "deduplicated",
                    ReclaimQueueInsert::PgCapacityDeferred => "pg_capacity_deferred",
                },
            },
        );
        Some(outcome)
    }

    pub(crate) fn finish_object_payload_reclaim_work(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map
            .runtime_state()
            .finish_object_payload_reclaim_work(bucket, key, generation_id);
    }

    pub(crate) fn enqueue_bucket_delete_finalize(&self, root: BucketDeleteFinalizeRoot) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        let _ = self
            .local_map
            .runtime_state()
            .enqueue_bucket_delete_finalize(root);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_reenqueue_bucket_delete_finalize(
        &self,
        root: &crate::TestBucketDeleteFinalizeRoot,
    ) {
        self.enqueue_bucket_delete_finalize(root.to_root());
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_enqueue_current_bucket_delete_finalize(
        &self,
        bucket: &BucketName,
    ) -> Result<crate::TestBucketDeleteFinalizeRoot, BucketWriteDrainError> {
        let info = self
            .test_head_bucket_raw(bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: info.bucket_incarnation_generation,
        };
        self.enqueue_bucket_delete_finalize(root.clone());
        Ok(crate::TestBucketDeleteFinalizeRoot::from_root(root))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_enqueue_missing_bucket_delete_finalize(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        if !self.bucket_name_absent_on_acting_set(pg_id, bucket)? {
            return Err(BucketWriteDrainError::Metadata(
                MetadataError::BucketNotFinalizedForDelete {
                    state: BucketState::Deleting,
                },
            ));
        }
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: MISSING_BUCKET_DELETE_FINALIZE_INCARNATION,
        };
        self.enqueue_bucket_delete_finalize(root);
        Ok(())
    }

    pub(crate) fn enqueue_bucket_delete_begin(
        &self,
        bucket: &BucketName,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        let root = crate::BucketDeleteBeginRoot {
            bucket: bucket.clone(),
            bucket_execution_generation,
            bucket_incarnation_generation,
        };
        let _ = self
            .local_map
            .runtime_state()
            .enqueue_bucket_delete_begin(root);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_enqueue_bucket_delete_begin(
        &self,
        bucket: &BucketName,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) {
        self.enqueue_bucket_delete_begin(
            bucket,
            bucket_execution_generation,
            bucket_incarnation_generation,
        );
    }

    pub(crate) fn finish_bucket_delete_finalize_work(&self, root: &BucketDeleteFinalizeRoot) {
        self.local_map
            .runtime_state()
            .finish_bucket_delete_finalize_work(root);
    }

    pub(crate) fn finish_bucket_delete_begin_work(&self, root: &BucketDeleteBeginRoot) {
        self.local_map
            .runtime_state()
            .finish_bucket_delete_begin_work(root);
    }

    pub(crate) fn promote_bucket_delete_begin_to_finalize(&self, root: &BucketDeleteBeginRoot) {
        self.local_map
            .runtime_state()
            .promote_bucket_delete_begin_to_finalize(root);
    }

    pub(crate) fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map.runtime_state().try_take_reclaim_work()
    }

    pub(crate) fn enqueue_durable_reclaim_work_batch_excluding(
        &self,
        next_pg_id: Option<u32>,
        max_pgs: usize,
        excluded_object_payload_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
        excluded_bucket_delete_begin_roots: &HashSet<BucketDeleteBeginRoot>,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableReclaimScanBatch {
        if self.operation_epoch() != self.cluster_epoch()
            || self.require_route_map_valid_now().is_err()
        {
            return DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                next_pg_id,
                scanned_pgs: 0,
                retry_pass_required: false,
            };
        }

        let pg_ids = self.metadata_pg_ids();
        let window = bounded_pg_scan_window(&pg_ids, next_pg_id, max_pgs);
        let mut scanned_pgs = 0usize;
        let mut retry_pass_required = false;
        for &raw_pg_id in &pg_ids[window.start..window.end] {
            let pg_id = PgId::new(raw_pg_id);
            let object_payload = self.enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
                pg_id,
                excluded_object_payload_roots,
            );
            if object_payload.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= object_payload.retry_required;
            let bucket_begin = self.enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
                pg_id,
                excluded_bucket_delete_begin_roots,
            );
            if bucket_begin.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= bucket_begin.retry_required;
            let bucket_finalize = self
                .enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
                    pg_id,
                    excluded_bucket_delete_finalize_roots,
                );
            if bucket_finalize.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= bucket_finalize.retry_required;
            scanned_pgs += 1;
        }

        DurableReclaimScanBatch {
            outcome: DurableReclaimScanOutcome::Complete,
            next_pg_id: window.next_pg_id,
            scanned_pgs,
            retry_pass_required,
        }
    }

    /// Poll for work already present in the process-local reclaim queue.
    ///
    /// Durable discovery is owned by the caller's explicit scan cadence and is
    /// never performed by this queue wait.
    pub(crate) fn wait_for_queued_reclaim_work_poll(
        &self,
        stop: &AtomicBool,
    ) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map
            .runtime_state()
            .wait_for_reclaim_work_poll(stop)
    }

    pub(crate) fn wake_reclaim_workers(&self) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map.runtime_state().wake_reclaim_workers();
    }

    pub(crate) fn reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(matches!(
            self.reclaim_object_payload_if_unleased_with_outcome(bucket, key, generation_id)?,
            super::ObjectPayloadReclaimAttempt::Completed
        ))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        self.reclaim_object_payload_if_unleased(bucket, key, generation_id)
    }

    pub(crate) fn reclaim_object_payload_if_unleased_with_outcome(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<super::ObjectPayloadReclaimAttempt, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(ReclaimObjectPayloadIfUnleased);
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let emit_outcome = |outcome: &'static str| {
            let _ = observability::emit_object_payload_reclaim_event(
                super::TRACE_TARGET,
                observability::ObjectPayloadReclaimEventSummary {
                    pg_id: pg_id.get(),
                    event: outcome,
                },
            );
        };
        emit_outcome("started");
        let runtime_state = self.local_map.runtime_state();
        let Some(_execution) = runtime_state
            .try_acquire_object_payload_reclaim_execution(bucket, key, generation_id)
        else {
            emit_outcome("deferred_active");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        };
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let retained_mutation_client =
            self.retained_object_mutation_metadata_primary_client(bucket, key)?;
        if self
            .local_map
            .object_payload_lease_count(bucket, key, generation_id)?
            != 0
        {
            emit_outcome("deferred_lease");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        }

        if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_reclaim_delete = matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                    if delete.matches_request(bucket, key, generation_id)
            );
            if matching_reclaim_delete {
                let reclaim_authority = match command.payload() {
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(delete) => {
                        &delete.reclaim_claim
                    }
                    _ => unreachable!("matching payload reclaim command was already selected"),
                };
                match self.finish_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )? {
                    super::PendingMetadataCommandOutcome::Applied
                    | super::PendingMetadataCommandOutcome::PublishedPendingRecovery
                    | super::PendingMetadataCommandOutcome::TerminalCleanupPending {
                        applied: true,
                    } => {
                        self.local_map.clear_object_payload_reclaim_fence(
                            bucket,
                            key,
                            generation_id,
                            reclaim_authority,
                        )?;
                        emit_outcome("completed_existing_pending");
                        return Ok(super::ObjectPayloadReclaimAttempt::Completed);
                    }
                    super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        emit_outcome("error");
                        return Err(super::conflicting_pending_object_metadata_command(
                            "retryable partial pending payload reclaim command",
                        ));
                    }
                    super::PendingMetadataCommandOutcome::Abandoned
                    | super::PendingMetadataCommandOutcome::TerminalCleanupPending {
                        applied: false,
                    } => {
                        emit_outcome("deferred_abandoned_pending");
                        return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
                    }
                }
            }
            self.emit_pending_slot_action_for_command(pg_id, &command, "reclaim_defer");
            emit_outcome("deferred_pending_command");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        }

        // Claim acquisition is the first durable mutation in this attempt. Capture one
        // immutable request fence before discovery and carry it through claim insertion,
        // command construction, and pending-slot installation so a same-epoch renewal
        // cannot extend any stage independently.
        let reclaim_effect_fence = self.current_route_effect_fence();
        let reclaim_route = mutation_client.open_object_payload_reclaim_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
            generation_id,
        )?;
        let reclaim = {
            if self
                .local_map
                .object_payload_lease_count(bucket, key, generation_id)?
                != 0
            {
                emit_outcome("deferred_lease");
                return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
            }

            reclaim_route
                .load_payload()
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        };

        let Some(reclaim) = reclaim else {
            emit_outcome("missing_root");
            return Ok(super::ObjectPayloadReclaimAttempt::MissingRoot);
        };

        let bucket_incarnation_generation = {
            let bucket_pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
            let bucket_store = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), bucket_pg_id)?;
            let metadata_route = bucket_store
                .bucket_metadata_client()
                .open_bucket_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(bucket_pg_id),
                    bucket,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            match metadata_route
                .head_bucket_raw()
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
            {
                Ok(bucket) => bucket.bucket_incarnation_generation,
                Err(ObjectPgActionError::Metadata(MetadataError::BucketNotFound { .. })) => {
                    ORPHAN_OBJECT_PAYLOAD_RECLAIM_BUCKET_INCARNATION
                }
                Err(error) => return Err(error),
            }
        };
        let claim_id = self.next_object_payload_reclaim_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let claimed_at = crate::clock::current_time_millis();
        let claim = reclaim_route
            .acquire_claim(
                AcquireObjectPayloadReclaimClaimReq {
                    reclaim_kind: reclaim.kind(),
                    bucket_incarnation_generation,
                    claim_id: &claim_id,
                    owner_token: &owner_token,
                    claimed_at,
                    lease_deadline: claimed_at.checked_add(60_000),
                    now: claimed_at,
                },
                reclaim_effect_fence,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let Some(claim) = claim else {
            emit_outcome("deferred_claim_busy");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        };
        let reclaim_authority = ObjectPayloadReclaimClaimProof::from(&claim);
        let retained_reclaim_route = retained_mutation_client
            .open_retained_object_mutation_route(object_pg_id, self.operation_epoch(), bucket, key)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let release_reclaim_claim = || -> Result<(), ObjectPgActionError> {
            self.maybe_run_before_reclaim_claim_release_hook()?;
            retained_reclaim_route
                .release_object_payload_reclaim_claim(&claim)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
        };

        let mut reclaim_fence_started = false;
        let mut payload_delete_started = false;
        let mut command_owns_reclaim_claim = false;
        let mut snapshot_retry_phase = super::SnapshotSensitiveRetryPhase::default();
        let result = (|| -> Result<super::ObjectPayloadReclaimAttempt, ObjectPgActionError> {
            self.maybe_run_after_reclaim_claim_acquired_hook()?;
            if !self.local_map.try_begin_object_payload_reclaim(
                bucket,
                key,
                generation_id,
                &reclaim_authority,
            )? {
                return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
            }
            reclaim_fence_started = true;
            match &reclaim {
                ObjectPayloadReclaimCommand::Segments(reclaim) => {
                    for segment in &reclaim.segments {
                        payload_delete_started = true;
                        self.delete_payload_shard_set(
                            segment.data_pg_id,
                            segment.ec,
                            &segment.segment_okh,
                            segment.segment_vid,
                        )?;
                    }
                }
                ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                    for part in &reclaim.parts {
                        for segment in &part.segments {
                            payload_delete_started = true;
                            self.delete_payload_shard_set(
                                segment.data_pg_id,
                                segment.ec,
                                &segment.segment_okh,
                                segment.segment_vid,
                            )?;
                        }
                    }
                }
            }
            loop {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let matching_reclaim_delete = matches!(
                        command.payload(),
                        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                            if delete.matches_request(bucket, key, generation_id)
                    );
                    if matching_reclaim_delete {
                        match self.finish_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )? {
                            super::PendingMetadataCommandOutcome::Applied
                            | super::PendingMetadataCommandOutcome::PublishedPendingRecovery
                            | super::PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: true,
                            } => {
                                return Ok(super::ObjectPayloadReclaimAttempt::Completed);
                            }
                            super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(super::conflicting_pending_object_metadata_command(
                                    "retryable partial pending payload reclaim command",
                                ));
                            }
                            super::PendingMetadataCommandOutcome::Abandoned
                            | super::PendingMetadataCommandOutcome::TerminalCleanupPending {
                                applied: false,
                            } => continue,
                        }
                    }
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                    continue;
                }

                let mut evaluated_attempt = snapshot_retry_phase.snapshot_evaluated();
                let command = match reclaim_route.build_delete_object_payload_reclaim_command(
                    crate::node_client::BuildDeleteObjectPayloadReclaimCommandReq {
                        payload: &reclaim,
                        claim: &claim,
                    },
                    reclaim_effect_fence,
                ) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                match self.install_snapshot_sensitive_metadata_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(reclaim_effect_fence),
                    &mut evaluated_attempt,
                )? {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ReinspectSnapshot => {
                        return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
                    }
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                command_owns_reclaim_claim = true;
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                return Ok(super::ObjectPayloadReclaimAttempt::Completed);
            }
        })();
        let (pending_command_owns_reclaim_claim, reclaim_ownership_unknown) =
            if result.is_err() && command_owns_reclaim_claim {
                match self
                    .maybe_run_before_reclaim_ownership_lookup_hook()
                    .and_then(|()| {
                        self.pending_metadata_command_for_bucket(pg_id, bucket)
                            .map_err(Into::into)
                    }) {
                    Ok(command) => (
                        command.is_some_and(|command| {
                            matches!(
                                command.payload(),
                                MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                                    if delete.matches_request(bucket, key, generation_id)
                                        && delete.reclaim_claim == reclaim_authority
                            )
                        }),
                        false,
                    ),
                    Err(_) => (true, true),
                }
            } else {
                (command_owns_reclaim_claim, false)
            };
        let mut reclaim_release_unknown = false;
        let release_claim = (result.is_err() && !pending_command_owns_reclaim_claim)
            || matches!(
                &result,
                Ok(super::ObjectPayloadReclaimAttempt::Deferred) if !reclaim_fence_started
            );
        let result = if release_claim {
            match release_reclaim_claim() {
                Ok(()) => result,
                Err(release_error) => {
                    reclaim_release_unknown = true;
                    Err(release_error)
                }
            }
        } else {
            result
        };
        match &result {
            Ok(super::ObjectPayloadReclaimAttempt::Completed) => emit_outcome("completed"),
            Ok(super::ObjectPayloadReclaimAttempt::Deferred) => emit_outcome("deferred_active"),
            Ok(super::ObjectPayloadReclaimAttempt::MissingRoot) => emit_outcome("missing_root"),
            Err(_) => emit_outcome("error"),
        }
        let keep_reclaim_fence = result.is_err()
            && (payload_delete_started || reclaim_ownership_unknown || reclaim_release_unknown);
        if reclaim_fence_started {
            self.local_map.finish_object_payload_reclaim(
                bucket,
                key,
                generation_id,
                &reclaim_authority,
                keep_reclaim_fence,
            )?;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_object_payload_reclaim_roots_excluding(
        &self,
        excluded_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
    ) -> DurableObjectPayloadReclaimScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableObjectPayloadReclaimScan::default();
        }

        let mut scan = DurableObjectPayloadReclaimScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
                PgId::new(pg_id),
                excluded_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
    ) -> DurableObjectPayloadReclaimScan {
        let mut scan = DurableObjectPayloadReclaimScan::default();
        let scan_pg_id = self.object_metadata_scan_pg(pg_id);
        let emit_scan = |outcome: &'static str| {
            let _ = observability::emit_object_payload_reclaim_durable_scan(
                super::TRACE_TARGET,
                observability::ObjectPayloadReclaimEventSummary {
                    pg_id: pg_id.get(),
                    event: outcome,
                },
            );
        };
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let scan_route = match node
            .object_mutation_metadata_client()
            .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
        {
            Ok(route) => route,
            Err(error) => {
                let error = super::object_pg_action_error_to_bucket_snapshot_error(error);
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let root = match scan_route.get_payload_reclaim_root() {
            Ok(root) => root,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let Some(root) = root else {
            return scan;
        };
        // The metadata query returns one row, not an exhaustion proof. A
        // follow-up pass is required even when this row is queued successfully.
        scan.retry_required = true;
        if excluded_roots.contains(&(root.bucket.clone(), root.key.clone(), root.generation_id)) {
            emit_scan("deferred_locally");
            return scan;
        }
        if self.object_metadata_pg_id(&root.bucket, &root.key) != pg_id.get() {
            scan.errors += 1;
            emit_scan("wrong_pg");
            let _ = observability::event(
                super::TRACE_TARGET,
                "object_reclaim_durable_scan_wrong_pg_root",
                Some(format_args!(
                    "pg_id={} root_bucket={} root_key={}",
                    pg_id.get(),
                    root.bucket,
                    root.key
                )),
            );
            scan.retry_required = true;
            return scan;
        }
        let lease_count = match self.local_map.object_payload_lease_count(
            &root.bucket,
            &root.key,
            root.generation_id,
        ) {
            Ok(count) => count,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_lease_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        if lease_count != 0 {
            emit_scan("leased");
            return scan;
        }
        match self.enqueue_object_payload_reclaim_for_pg(
            &root.bucket,
            &root.key,
            root.generation_id,
        ) {
            Some(ReclaimQueueInsert::Queued) => {
                emit_scan("queued");
                scan.queued += 1;
            }
            Some(ReclaimQueueInsert::Deduplicated) => emit_scan("deduplicated"),
            Some(ReclaimQueueInsert::PgCapacityDeferred) => {
                emit_scan("pg_capacity_deferred");
            }
            None => {}
        }
        scan
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_begin_roots_excluding(
        &self,
        excluded_roots: &HashSet<BucketDeleteBeginRoot>,
    ) -> DurableBucketDeleteBeginScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableBucketDeleteBeginScan::default();
        }

        let mut scan = DurableBucketDeleteBeginScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
                PgId::new(pg_id),
                excluded_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_roots: &HashSet<BucketDeleteBeginRoot>,
    ) -> DurableBucketDeleteBeginScan {
        let mut scan = DurableBucketDeleteBeginScan::default();
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_begin_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let client = node.bucket_write_reservation_client();
        let route = match self.open_bucket_write_reservation_scan_route(
            client.as_ref(),
            self.validated_bucket_metadata_pg(pg_id),
        ) {
            Ok(route) => route,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_begin_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let now = crate::clock::current_time_millis();
        let mut start_after_bucket = None;
        let mut queued_for_pg = 0usize;
        loop {
            let roots = match route.get_bucket_delete_begin_roots(
                now,
                start_after_bucket.as_ref(),
                BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG,
            ) {
                Ok(roots) => roots,
                Err(error) => {
                    scan.errors += 1;
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_begin_durable_scan_pg_error",
                        Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                    );
                    scan.route_refresh_required =
                        durable_reclaim_bucket_scan_requires_route_refresh(&error);
                    scan.retry_required = !scan.route_refresh_required;
                    return scan;
                }
            };
            if roots.is_empty() {
                break;
            }
            let page_len = roots.len();
            for root in roots {
                start_after_bucket = Some(root.bucket.clone());
                if excluded_roots.contains(&root) {
                    continue;
                }
                self.local_map
                    .runtime_state()
                    .enqueue_bucket_delete_begin(root);
                scan.queued += 1;
                queued_for_pg += 1;
                if queued_for_pg >= BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG {
                    scan.retry_required = true;
                    break;
                }
            }
            if queued_for_pg >= BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG
                || page_len < BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG
            {
                break;
            }
        }
        scan
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_finalize_roots(
        &self,
    ) -> DurableBucketDeleteFinalizeScan {
        self.enqueue_durable_bucket_delete_finalize_roots_excluding(&HashSet::new())
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_finalize_roots_excluding(
        &self,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableBucketDeleteFinalizeScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableBucketDeleteFinalizeScan::default();
        }

        let mut scan = DurableBucketDeleteFinalizeScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
                PgId::new(pg_id),
                excluded_bucket_delete_finalize_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableBucketDeleteFinalizeScan {
        let mut scan = DurableBucketDeleteFinalizeScan::default();
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let client = node.bucket_write_reservation_client();
        let route = match self.open_bucket_write_reservation_scan_route(
            client.as_ref(),
            self.validated_bucket_metadata_pg(pg_id),
        ) {
            Ok(route) => route,
            Err(error) => {
                scan.errors += 1;
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let roots = match route.get_bucket_delete_finalize_roots(
            crate::clock::current_time_millis(),
            BUCKET_DELETE_FINALIZE_SCAN_LIMIT_PER_PG,
        ) {
            Ok(roots) => roots,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        if roots.len() >= BUCKET_DELETE_FINALIZE_SCAN_LIMIT_PER_PG {
            scan.retry_required = true;
        }
        for root in roots {
            if excluded_bucket_delete_finalize_roots.contains(&root.bucket) {
                continue;
            }
            self.enqueue_bucket_delete_finalize(root);
            scan.queued += 1;
        }
        scan
    }

    pub(super) fn delete_complete_multipart_cleanup_best_effort(
        &self,
        cleanup: &CompleteMultipartCommitCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.omitted_streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    pub(super) fn delete_finalize_upload_part_cleanup_best_effort(
        &self,
        cleanup: &FinalizeStreamPartCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.displaced_segments);
    }

    pub(super) fn delete_abort_multipart_cleanup_best_effort(
        &self,
        cleanup: &AbortMultipartUploadCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    fn delete_multipart_part_segments_best_effort(&self, segments: &[MultipartPartSegmentRecord]) {
        for segment in segments {
            self.delete_multipart_shard_set_best_effort(
                segment.placement_cluster_epoch,
                segment.data_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn delete_multipart_shard_set_best_effort(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            okh,
            generation_id,
        );
    }

}
