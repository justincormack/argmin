// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl super::StorageCluster {
    pub fn head_bucket_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.head_bucket_info_internal(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub(crate) fn head_bucket_info_internal(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let route = node.bucket_metadata_client().open_bucket_metadata_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        route.head_bucket_info()
    }

    pub fn get_bucket_subresource(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<Option<String>, crate::BucketSnapshotLoadFailure> {
        self.get_bucket_subresource_internal(bucket, kind)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn get_bucket_subresource_internal(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let route = node.bucket_metadata_client().open_bucket_metadata_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        route.get_bucket_subresource(kind.stored_kind())
    }

    pub fn get_bucket_tags(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<SerializedBucketTagSet>, crate::BucketSnapshotLoadFailure> {
        self.get_bucket_tags_internal(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn get_bucket_tags_internal(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<SerializedBucketTagSet>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let route = node.bucket_metadata_client().open_bucket_metadata_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        route.get_bucket_tags()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_versioning_and_load_info(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_versioning_and_load_info_raw(bucket, state)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_versioning_and_load_info_raw(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_versioning_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            state,
        )?;
        self.head_bucket_info_internal(bucket)
    }

    pub(super) fn put_bucket_versioning_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        state: BucketVersioningState,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(PutBucketVersioning);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = primary_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)?;
        {
            require_valid_route()?;
            let info = metadata_route.head_bucket_raw()?;
            if state == BucketVersioningState::Disabled
                && info.versioning != BucketVersioningState::Disabled
            {
                return Err(MetadataError::InvalidVersioningTransition {
                    from: info.versioning,
                    to: state,
                }
                .into());
            }
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_versioning")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket versioning command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.bucket_name() == bucket =>
                    {
                        let same_request = versioning.bucket.versioning == state;
                        if !metadata_route.pending_put_bucket_versioning_command_matches_current(
                            versioning, state,
                        )? {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket versioning command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command =
                    metadata_route.build_put_bucket_versioning_command(command_id, state)?;
                require_valid_route()?;
                match self.install_snapshot_sensitive_bucket_control_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let MetadataCommandPayload::PutBucketVersioning(versioning) = command.payload() else {
                unreachable!("versioning mutation completed a different command kind")
            };
            return Ok(BucketMutationReceipt::new(
                versioning.bucket.name.clone(),
                versioning.bucket.bucket_execution_generation,
            ));
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_object_lock_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_object_lock_and_load_info_raw(bucket, config)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_object_lock_and_load_info_raw(
        &self,
        bucket: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::ObjectLock(config),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_encryption_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_encryption_and_load_info_raw(bucket, config)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_encryption_and_load_info_raw(
        &self,
        bucket: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::Encryption(config),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_public_access_block_and_load_info_raw(bucket, config)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_public_access_block_and_load_info_raw(
        &self,
        bucket: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::PublicAccessBlock(Some(config)),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.delete_bucket_public_access_block_and_load_info_raw(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn delete_bucket_public_access_block_and_load_info_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::PublicAccessBlock(None),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_ownership_controls_and_load_info_raw(bucket, config)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_ownership_controls_and_load_info_raw(
        &self,
        bucket: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::OwnershipControls(Some(config)),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.delete_bucket_ownership_controls_and_load_info_raw(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn delete_bucket_ownership_controls_and_load_info_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::OwnershipControls(None),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_abac_enabled_and_load_info(
        &self,
        bucket: &BucketName,
        enabled: bool,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_abac_enabled_and_load_info_raw(bucket, enabled)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_abac_enabled_and_load_info_raw(
        &self,
        bucket: &BucketName,
        enabled: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::AbacEnabled(enabled),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_acl_and_load_info(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_acl_and_load_info_raw(bucket, acl_grants, summary)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_acl_and_load_info_raw(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_acl_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            acl_grants,
            summary,
        )?;
        self.head_bucket_info_internal(bucket)
    }

    pub(super) fn put_bucket_acl_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(PutBucketAcl);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = primary_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)?;
        {
            require_valid_route()?;
            metadata_route.head_bucket_raw()?;
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_acl")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket acl command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketAcl(acl) if acl.bucket_name() == bucket => {
                        let same_request = acl.bucket.acl_grants == *acl_grants
                            && acl.bucket.public_read == summary.public_read
                            && acl.bucket.public_write == summary.public_write;
                        if !metadata_route.pending_put_bucket_acl_command_matches_current(
                            acl, acl_grants, summary,
                        )? {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket acl command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command =
                    metadata_route.build_put_bucket_acl_command(command_id, acl_grants, summary)?;
                require_valid_route()?;
                match self.install_snapshot_sensitive_bucket_control_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let MetadataCommandPayload::PutBucketAcl(acl) = command.payload() else {
                unreachable!("ACL mutation completed a different command kind")
            };
            return Ok(BucketMutationReceipt::new(
                acl.bucket.name.clone(),
                acl.bucket.bucket_execution_generation,
            ));
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_bucket_property_command_and_load_info(
        &self,
        bucket: &BucketName,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            mutation,
        )?;
        self.head_bucket_info_internal(bucket)
    }

    pub(super) fn put_bucket_property_command_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(PutBucketProperty);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = primary_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)?;
        {
            require_valid_route()?;
            metadata_route.head_bucket_raw()?;
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_property")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket property command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketProperty(property)
                        if property.bucket_name() == bucket
                            && property.effect == mutation.effect() =>
                    {
                        if !metadata_route.pending_put_bucket_property_command_matches_current(
                            property, &mutation,
                        )? {
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command =
                    metadata_route.build_put_bucket_property_command(command_id, &mutation)?;
                require_valid_route()?;
                match self.install_snapshot_sensitive_bucket_control_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let MetadataCommandPayload::PutBucketProperty(property) = command.payload() else {
                unreachable!("bucket property mutation completed a different command kind")
            };
            return Ok(BucketMutationReceipt::new(
                property.bucket.name.clone(),
                property.bucket.bucket_execution_generation,
            ));
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_subresource_and_load_info_raw(bucket, req)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn put_bucket_subresource_and_load_info_raw(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_subresource_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            req,
        )?;
        self.head_bucket_info_internal(bucket)
    }

    pub(super) fn put_bucket_subresource_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        let mutation = BucketSubresourceMutation::from_put_request(req).map_err(|error| {
            MetadataError::InvariantViolation {
                context: "put bucket subresource",
                reason: format!("bucket subresource request is invalid: {error}"),
            }
        })?;
        self.put_bucket_subresource_command_with_route_validation(
            route,
            require_valid_route,
            mutation,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.delete_bucket_subresource_and_load_info_raw(bucket, kind)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn delete_bucket_subresource_and_load_info_raw(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.delete_bucket_subresource_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            kind.stored_kind(),
        )?;
        self.head_bucket_info_internal(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_tags_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.delete_bucket_tags_and_load_info_raw(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn delete_bucket_tags_and_load_info_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.delete_bucket_subresource_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            BucketSubresourceKind::Tagging,
        )?;
        self.head_bucket_info_internal(bucket)
    }

    pub(super) fn delete_bucket_subresource_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        kind: BucketSubresourceKind,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        self.put_bucket_subresource_command_with_route_validation(
            route,
            require_valid_route,
            BucketSubresourceMutation::Delete { kind },
        )
    }

    fn put_bucket_subresource_command_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mutation: BucketSubresourceMutation,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(PutBucketSubresource);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = primary_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)?;
        {
            require_valid_route()?;
            metadata_route.head_bucket_raw()?;
        }
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_subresource")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket subresource command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketSubresource(subresource)
                        if subresource.matches_mutation(bucket, &mutation) =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command =
                    metadata_route.build_put_bucket_subresource_command(command_id, &mutation)?;
                require_valid_route()?;
                match self.install_snapshot_sensitive_bucket_control_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let MetadataCommandPayload::PutBucketSubresource(subresource) = command.payload()
            else {
                unreachable!("bucket subresource mutation completed a different command kind")
            };
            return Ok(BucketMutationReceipt::new(
                subresource.name.clone(),
                subresource.bucket_execution_generation,
            ));
        }
    }

    pub(super) fn list_buckets_for_owner_with_route_validation(
        &self,
        owner_canonical_id: &str,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        let mut buckets = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let pg_id = PgId::new(pg_id);
            let node = self
                .local_map
                .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
            let client = node.bucket_metadata_client();
            let route = client
                .open_bucket_metadata_read_scan_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(pg_id),
                    node.authorization(),
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            let mut page = route
                .list_buckets(owner_canonical_id)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            self.validate_bucket_list_page_for_pg(pg_id, node.node_id(), &page)?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id.get());
            buckets.append(&mut page);
        }
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    #[cfg(test)]
    pub(crate) fn list_buckets_for_owner(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        self.list_buckets_for_owner_with_route_validation(owner_canonical_id, || Ok(()))
    }

    pub(crate) fn validate_bucket_list_page_for_pg(
        &self,
        pg_id: PgId,
        node_id: NodeId,
        page: &[BucketInfo],
    ) -> Result<(), ObjectPgActionError> {
        for bucket in page {
            let expected_pg_id = self.bucket_metadata_pg_id(&bucket.name);
            if expected_pg_id != pg_id.get() {
                return Err(ObjectPgActionError::Store(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "validate bucket list response",
                    failure: StorageRpcErrorCode::Internal,
                    detail: crate::StorageNodeFailureDetail::new(format!(
                        "bucket {} belongs to bucket PG {}, not response PG {}",
                        bucket.name.as_str(),
                        expected_pg_id,
                        pg_id.get()
                    )),
                }));
            }
        }
        Ok(())
    }

    pub(crate) fn list_lifecycle_sweep_buckets(
        &self,
    ) -> Result<LifecycleSweepBuckets, ObjectPgActionError> {
        let mut lifecycle_buckets = Vec::new();
        let mut aborting_buckets = Vec::new();
        for raw_pg_id in self.metadata_pg_ids() {
            let pg_id = PgId::new(raw_pg_id);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            let bucket_client = node.bucket_write_reservation_client();
            lifecycle_buckets.extend(
                self.open_bucket_write_reservation_scan_route(
                    bucket_client.as_ref(),
                    self.validated_bucket_metadata_pg(pg_id),
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                .list_buckets_with_lifecycle()
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
            );
            let object_client = node.object_mutation_metadata_client();
            let witnesses = object_client
                .open_object_mutation_scan_metadata_route(
                    self.operation_epoch(),
                    self.object_metadata_scan_pg(pg_id),
                )?
                .list_aborting_multipart_upload_bucket_witnesses()?;
            aborting_buckets.extend(witnesses.into_iter().map(|witness| witness.bucket));
        }
        lifecycle_buckets.sort_by(|a, b| a.name.cmp(&b.name));
        aborting_buckets.sort();
        aborting_buckets.dedup();
        Ok(LifecycleSweepBuckets {
            lifecycle_buckets,
            aborting_buckets,
        })
    }

    pub fn list_lifecycle_sweep_roots(
        &self,
        now: u64,
    ) -> Result<Vec<LifecycleSweepRoot>, crate::LifecycleMaintenanceFailure> {
        self.list_lifecycle_sweep_roots_raw(now)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn list_lifecycle_sweep_roots_raw(
        &self,
        now: u64,
    ) -> Result<Vec<LifecycleSweepRoot>, ObjectPgActionError> {
        let mut roots = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let client = node.bucket_write_reservation_client();
            roots.extend(
                self.open_bucket_write_reservation_scan_route(
                    client.as_ref(),
                    self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                .get_lifecycle_sweep_roots(now, LIFECYCLE_SWEEP_ROOT_SCAN_LIMIT_PER_PG)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
            );
        }
        for bucket in self.list_lifecycle_sweep_buckets()?.aborting_buckets {
            match self.head_bucket_info_internal(&bucket) {
                Ok(bucket_info) => roots.push(LifecycleSweepRoot {
                    bucket,
                    bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
                    source: LifecycleSweepRootSource::AbortingMultipartUpload,
                }),
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                    ..
                })) => {}
                Err(BucketSnapshotLoadError::Metadata(error)) => return Err(error.into()),
                Err(BucketSnapshotLoadError::Store(error)) => return Err(error.into()),
            }
        }
        roots.sort_by(|left, right| {
            lifecycle_sweep_root_source_rank(left.source)
                .cmp(&lifecycle_sweep_root_source_rank(right.source))
                .then_with(|| left.bucket.cmp(&right.bucket))
                .then_with(|| {
                    left.bucket_incarnation_generation
                        .cmp(&right.bucket_incarnation_generation)
                })
        });
        roots.dedup_by(|left, right| {
            left.bucket == right.bucket
                && left.bucket_incarnation_generation == right.bucket_incarnation_generation
        });
        Ok(roots)
    }

    pub fn acquire_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, crate::LifecycleMaintenanceFailure> {
        self.acquire_lifecycle_sweep_claim_raw(bucket, bucket_incarnation_generation, now)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn acquire_lifecycle_sweep_claim_raw(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, ObjectPgActionError> {
        let claim_id = self.next_lifecycle_sweep_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_client = node.bucket_write_reservation_client();
        self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
            bucket,
        )
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        .acquire_lifecycle_sweep_claim(
            bucket_incarnation_generation,
            &claim_id,
            &owner_token,
            now,
            now.checked_add(LIFECYCLE_SWEEP_CLAIM_LEASE_MILLIS),
            now,
        )
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
        now: u64,
    ) -> Result<LifecycleSweepClaimRecord, crate::LifecycleMaintenanceFailure> {
        self.heartbeat_lifecycle_sweep_claim_raw(claim, now)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn heartbeat_lifecycle_sweep_claim_raw(
        &self,
        claim: &LifecycleSweepClaimRecord,
        now: u64,
    ) -> Result<LifecycleSweepClaimRecord, ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_client = node.bucket_write_reservation_client();
        self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
            &claim.bucket,
        )
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        .heartbeat_lifecycle_sweep_claim(
            claim,
            now,
            now.checked_add(LIFECYCLE_SWEEP_CLAIM_LEASE_MILLIS),
        )
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn record_lifecycle_sweep_claim_error(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, crate::LifecycleMaintenanceFailure> {
        self.record_lifecycle_sweep_claim_error_raw(claim, last_error)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn record_lifecycle_sweep_claim_error_raw(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_client = node.bucket_write_reservation_client();
        self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
            &claim.bucket,
        )
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        .record_lifecycle_sweep_claim_error(claim, last_error)
        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn release_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), crate::LifecycleMaintenanceFailure> {
        self.release_lifecycle_sweep_claim_raw(claim)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn release_lifecycle_sweep_claim_raw(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                &claim.bucket,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            .release_lifecycle_sweep_claim(claim)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn list_all_objects_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, crate::LifecycleMaintenanceFailure> {
        self.list_all_objects_for_bucket_raw(bucket)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn list_all_objects_for_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let mut all_objects = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut start_after = None;
            loop {
                let resp = self.list_objects_page(
                    pg_id,
                    &ListObjectsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        start_after: start_after.clone(),
                        start_at: None,
                        max_keys: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                all_objects.extend(resp.objects);
                if !resp.is_truncated {
                    break;
                }
                start_after = resp.next_start_after;
            }
        }
        all_objects.sort_by(|a, b| a.key().cmp(b.key()));
        all_objects.dedup_by(|a, b| a.key() == b.key());
        Ok(all_objects)
    }

    pub fn list_all_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, crate::LifecycleMaintenanceFailure> {
        self.list_all_object_versions_for_bucket_raw(bucket)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn list_all_object_versions_for_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut key_marker = None;
            let mut version_id_marker = None;
            let mut versions = Vec::new();
            loop {
                let resp = self.list_object_versions_page(
                    pg_id,
                    &ListObjectVersionsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        key_marker: key_marker.clone(),
                        version_id_marker,
                        start_at: None,
                        max_keys: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                versions.extend(resp.versions);
                if !resp.is_truncated {
                    break;
                }
                key_marker = resp.next_key_marker;
                version_id_marker = resp.next_version_id_marker;
            }
            cursors.push(VersionCursor {
                pg_id,
                versions,
                next_index: 0,
                next_page_start: None,
            });
        }

        let mut merged_versions = Vec::new();
        while let Some((cursor_index, _)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor.current().map(|version| (cursor_index, version))
            })
            .min_by(|(left_index, left), (right_index, right)| {
                left.key()
                    .cmp(right.key())
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            merged_versions.push(cursors[cursor_index].pop_current());
        }

        Ok(merged_versions)
    }

    pub fn list_all_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<crate::MultipartLifecycleUpload>, crate::LifecycleMaintenanceFailure> {
        self.list_all_multipart_uploads_for_bucket_raw(bucket)
            .map_err(crate::LifecycleMaintenanceFailure::from_object_pg_action)
    }

    fn list_all_multipart_uploads_for_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<crate::MultipartLifecycleUpload>, ObjectPgActionError> {
        let mut uploads = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut key_marker = None;
            let mut upload_id_marker = None;
            loop {
                let resp = self.list_multipart_uploads_page(
                    pg_id,
                    &ListMultipartUploadsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        page_start: key_marker.clone().map(|key_marker| {
                            ListMultipartUploadsPageStart::After {
                                key_marker,
                                upload_id_marker: upload_id_marker.clone(),
                            }
                        }),
                        max_uploads: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                uploads.extend(resp.uploads);
                if !resp.is_truncated {
                    break;
                }
                key_marker = resp.next_key_marker;
                upload_id_marker = resp.next_upload_id_marker;
            }
        }
        uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.upload_id.cmp(&b.upload_id))
        });
        Ok(uploads
            .into_iter()
            .map(crate::MultipartLifecycleUpload::from_record)
            .collect())
    }

    pub(super) fn list_objects_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_keys == 0 {
            return Ok(ListedBucketObjects {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
            });
        }

        let fetch_limit = max_keys.saturating_add(1);
        let prefix = prefix.cloned();
        let continuation_token = continuation_token.cloned();

        if delimiter.is_none() {
            let max = max_keys as usize;
            let mut smallest = BoundedSmallestRecords::new(max.saturating_add(1));
            for pg_id in self.metadata_pg_ids() {
                scan.require_valid().map_err(ObjectPgActionError::Store)?;
                let resp = self.list_objects_page(
                    pg_id,
                    &ListObjectsReq {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        start_after: continuation_token.clone(),
                        start_at: None,
                        max_keys: fetch_limit,
                    },
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id);
                for object in resp.objects {
                    smallest.insert(object.key().clone(), object);
                }
            }

            let mut objects = smallest.into_values();
            let is_truncated = objects.len() > max;
            objects.truncate(max);
            let next_continuation_token = is_truncated
                .then(|| objects.last().map(|object| object.key().clone()))
                .flatten();
            return Ok(ListedBucketObjects {
                objects,
                common_prefixes: Vec::new(),
                is_truncated,
                next_continuation_token,
            });
        }

        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let delimiter = delimiter.expect("checked above");
        let initial_start = match continuation_token {
            Some(token) => {
                let token_str = token.as_str();
                if let Some(after_prefix) = token_str.strip_prefix(prefix_str) {
                    if after_prefix.ends_with(delimiter) {
                        if let Some(upper_bound) = crate::object_key_prefix_upper_bound(&token) {
                            Some(ListObjectsPageStart::At(upper_bound))
                        } else {
                            Some(ListObjectsPageStart::After(token))
                        }
                    } else {
                        Some(ListObjectsPageStart::After(token))
                    }
                } else {
                    Some(ListObjectsPageStart::After(token))
                }
            }
            None => None,
        };

        let fetch_objects_page = |cursor: &mut ObjectCursor,
                                  start: Option<ListObjectsPageStart>|
         -> Result<(), ObjectPgActionError> {
            let (start_after, start_at) = match start {
                Some(ListObjectsPageStart::After(key)) => (Some(key), None),
                Some(ListObjectsPageStart::At(key)) => (None, Some(key)),
                None => (None, None),
            };
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_objects_page(
                cursor.pg_id,
                &ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    start_after,
                    start_at,
                    max_keys: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.objects = resp.objects;
            cursor.next_index = 0;
            cursor.next_page_start = resp.next_start_after.map(ListObjectsPageStart::After);
            Ok(())
        };

        let refill_cursor = |cursor: &mut ObjectCursor| -> Result<(), ObjectPgActionError> {
            while cursor.current().is_none() {
                let Some(next_start) = cursor.next_page_start.clone() else {
                    break;
                };
                fetch_objects_page(cursor, Some(next_start))?;
            }
            Ok(())
        };

        let jump_cursor_to = |cursor: &mut ObjectCursor,
                              start: ListObjectsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.objects.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix =
            |cursor: &mut ObjectCursor, common_prefix: &str| -> Result<(), ObjectPgActionError> {
                while cursor
                    .current()
                    .is_some_and(|obj| obj.key().as_str().starts_with(common_prefix))
                {
                    cursor.next_index += 1;
                    refill_cursor(cursor)?;
                }
                Ok(())
            };

        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = ObjectCursor {
                pg_id,
                objects: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            fetch_objects_page(&mut cursor, initial_start.clone())?;
            cursors.push(cursor);
        }

        let max = max_keys as usize;
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut next_continuation_token = None;
        let mut is_truncated = false;
        let mut active_common_prefix: Option<(ObjectKey, Option<ObjectKey>)> = None;

        while let Some((cursor_index, current_key)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor
                    .current()
                    .map(|object| (cursor_index, object.key().clone()))
            })
            .min_by(|(left_index, left_key), (right_index, right_key)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListObjectsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current object")
                .clone();
            if let Some(common_prefix_key) =
                crate::object_key_common_prefix(current.key(), prefix_str, delimiter)
            {
                let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix_key);
                active_common_prefix = Some((common_prefix_key.clone(), upper_bound));
                if objects.len() + common_prefixes.len() >= max {
                    is_truncated = true;
                    break;
                }
                next_continuation_token = Some(common_prefix_key.clone());
                common_prefixes.push(common_prefix_key);
                continue;
            }

            if objects.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }

            next_continuation_token = Some(current.key().clone());
            objects.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketObjects {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: if is_truncated {
                next_continuation_token
            } else {
                None
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn list_objects_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_objects_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            continuation_token,
            max_keys,
        )
    }

    pub(super) fn list_object_versions_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_keys == 0 {
            return Ok(ListedBucketObjectVersions {
                versions: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        }

        let fetch_limit = max_keys.saturating_add(1);
        let prefix = prefix.cloned();
        let key_marker = key_marker.cloned();
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());
        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let fetch_versions_page = |cursor: &mut VersionCursor,
                                   start: Option<ListVersionsPageStart>|
         -> Result<(), ObjectPgActionError> {
            let (key_marker, version_id_marker, start_at) = match start {
                Some(ListVersionsPageStart::After {
                    key_marker,
                    version_id_marker,
                }) => (Some(key_marker), version_id_marker, None),
                Some(ListVersionsPageStart::At(key)) => (None, None, Some(key)),
                None => (None, None, None),
            };
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_object_versions_page(
                cursor.pg_id,
                &ListObjectVersionsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    key_marker,
                    version_id_marker,
                    start_at,
                    max_keys: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.versions = resp.versions;
            cursor.next_index = 0;
            cursor.next_page_start =
                resp.next_key_marker
                    .map(|key_marker| ListVersionsPageStart::After {
                        key_marker,
                        version_id_marker: resp.next_version_id_marker,
                    });
            Ok(())
        };

        let refill_cursor = |cursor: &mut VersionCursor| -> Result<(), ObjectPgActionError> {
            while cursor.current().is_none() {
                let Some(next_start) = cursor.next_page_start.clone() else {
                    break;
                };
                fetch_versions_page(cursor, Some(next_start))?;
            }
            Ok(())
        };

        let jump_cursor_to = |cursor: &mut VersionCursor,
                              start: ListVersionsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.versions.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix =
            |cursor: &mut VersionCursor, common_prefix: &str| -> Result<(), ObjectPgActionError> {
                while cursor
                    .current()
                    .is_some_and(|obj| obj.key().as_str().starts_with(common_prefix))
                {
                    cursor.next_index += 1;
                    refill_cursor(cursor)?;
                }
                Ok(())
            };

        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = VersionCursor {
                pg_id,
                versions: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            let initial_start = key_marker
                .clone()
                .map(|key_marker| ListVersionsPageStart::After {
                    key_marker,
                    version_id_marker,
                });
            fetch_versions_page(&mut cursor, initial_start)?;
            cursors.push(cursor);
        }

        let max = max_keys as usize;
        let mut versions = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut is_truncated = false;
        let mut next_key_marker = None;
        let mut next_version_id_marker = None;
        let mut active_common_prefix = key_marker.as_ref().and_then(|marker| {
            let delimiter = delimiter?;
            let after_prefix = marker.as_str().strip_prefix(prefix_str)?;
            after_prefix
                .ends_with(delimiter)
                .then(|| (marker.clone(), crate::object_key_prefix_upper_bound(marker)))
        });

        while let Some((cursor_index, current_key)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor
                    .current()
                    .map(|version| (cursor_index, version.key().clone()))
            })
            .min_by(|(left_index, left_key), (right_index, right_key)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListVersionsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current object")
                .clone();
            if let Some(delimiter) = delimiter {
                if let Some(common_prefix_key) =
                    crate::object_key_common_prefix(current.key(), prefix_str, delimiter)
                {
                    let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix_key);
                    active_common_prefix = Some((common_prefix_key.clone(), upper_bound.clone()));
                    if key_marker
                        .as_ref()
                        .is_some_and(|marker| common_prefix_key.as_str() <= marker.as_str())
                    {
                        if let Some(upper_bound) = upper_bound {
                            jump_cursor_to(
                                &mut cursors[cursor_index],
                                ListVersionsPageStart::At(upper_bound),
                            )?;
                        } else {
                            skip_cursor_prefix(
                                &mut cursors[cursor_index],
                                common_prefix_key.as_str(),
                            )?;
                        }
                        continue;
                    }
                    if versions.len() + common_prefixes.len() >= max {
                        is_truncated = true;
                        break;
                    }
                    next_key_marker = Some(common_prefix_key.clone());
                    next_version_id_marker = None;
                    common_prefixes.push(common_prefix_key);
                    continue;
                }
            }

            if versions.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }

            next_key_marker = Some(current.key().clone());
            next_version_id_marker = Some(current.version_id());
            versions.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketObjectVersions {
            versions,
            common_prefixes,
            is_truncated,
            next_key_marker: if is_truncated { next_key_marker } else { None },
            next_version_id_marker: if is_truncated {
                next_version_id_marker
            } else {
                None
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn list_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_object_versions_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
        )
    }

    pub(super) fn load_object_if_on_route<T, E>(
        &self,
        route: &super::ObjectReadMetadataRoute<'_>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let pg_id = route.pg_id.pg_id();
        require_valid_route()?;
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        let object_read_client = read_node.object_read_metadata_client();
        require_valid_route()?;
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            route.pg_id,
            route.bucket,
            route.key,
            read_node.authorization(),
        )?;
        let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
        Ok(action(&subject.stored))
    }

    pub fn load_existing_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, crate::ObjectReadFailure> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)
            .map_err(crate::ObjectReadFailure::from_store)?;
        let object_read_client = read_node.object_read_metadata_client();
        let object_read_route = object_read_client
            .open_object_read_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                read_node.authorization(),
            )
            .map_err(crate::ObjectReadFailure::from_object_pg_action)?;
        match object_read_route.load_object_read_auth_subject(None) {
            Ok(subject) => match subject.stored {
                StoredObject::Live(_) => Ok(Some(subject.stored)),
                StoredObject::DeleteMarker(_) => Ok(None),
            },
            Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)) => Ok(None),
            Err(error) => Err(crate::ObjectReadFailure::from_object_pg_action(error)),
        }
    }

    pub(crate) fn load_object_read_snapshot_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let route = super::ObjectReadMetadataRoute {
            bucket,
            key,
            version_id,
            snapshot_mode,
            pg_id: self.object_metadata_pg(bucket, key),
        };
        self.load_object_read_snapshot_if_on_route(&route, action, || Ok(()))
    }

    pub(super) fn load_object_read_snapshot_if_on_route<T, E>(
        &self,
        route: &super::ObjectReadMetadataRoute<'_>,
        mut action: impl FnMut(&StoredObject) -> Result<T, E>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = route.pg_id.pg_id();
        require_valid_route()?;
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        let object_read_client = read_node.object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            route.pg_id,
            route.bucket,
            route.key,
            read_node.authorization(),
        )?;

        let mut work_budget =
            super::RequestWorkBudget::new(OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET, None)
                .for_operation("load_object_read_snapshot")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("load object read snapshot stale retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            require_valid_route()?;
            let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
            let value = match action(&subject.stored) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            require_valid_route()?;
            match object_read_route.load_object_read_snapshot_for_subject(
                route.version_id,
                &subject.identity,
                route.snapshot_mode,
            ) {
                Ok(snapshot) => return Ok(Ok(ObjectReadSnapshotOutcome { value, snapshot })),
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    work_budget
                        .sleep_after_contention(
                            "load object read snapshot stale retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

}
