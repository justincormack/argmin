impl super::StorageCluster {
    fn delete_bucket_from_acting_set(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket = &root.bucket;
        let publisher =
            crate::metadata_command::metadata_command_publisher!(DeleteBucketFromActingSet);
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_start",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_FINALIZE_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_finalize_delete_command")
        .for_pg(pg_id);
        loop {
            work_budget
                .check("bucket finalized delete command budget exhausted")
                .map_err(BucketWriteDrainError::Store)?;
            let primary = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
                .map_err(BucketWriteDrainError::Store)?;
            let metadata_route = primary
                .bucket_metadata_client()
                .open_bucket_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(pg_id),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            let current_info = match metadata_route
                .head_bucket_raw()
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info) => {
                    if info.bucket_incarnation_generation != root.bucket_incarnation_generation {
                        return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
                    }
                    if info.state != BucketState::Deleting {
                        return Err(BucketWriteDrainError::Metadata(
                            MetadataError::BucketNotFinalizedForDelete { state: info.state },
                        ));
                    }
                    Some(info)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => None,
                Err(other) => return Err(other),
            };
            let (command, clear_pending_on_zero_apply) = if let Some(command) = self
                .pending_metadata_command_for_bucket(pg_id, bucket)
                .map_err(BucketWriteDrainError::Store)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::DeleteFinalizedBucket(delete)
                        if delete.bucket == *bucket
                            && delete.bucket_incarnation_generation
                                == root.bucket_incarnation_generation =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::DeleteFinalizedBucket(_) => {
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        continue;
                    }
                }
            } else {
                let Some(info) = current_info else {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_primary_missing",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
                    );
                    return if self.finalized_bucket_deleted_on_acting_set(pg_id, root)? {
                        Ok(BucketDeleteFinalizeOutcome::NotFound)
                    } else {
                        Ok(BucketDeleteFinalizeOutcome::Pending)
                    };
                };
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                else {
                    continue;
                };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::DeleteFinalizedBucket(
                        DeleteFinalizedBucketCommand::new(
                            bucket.clone(),
                            info.bucket_execution_generation,
                            root.bucket_incarnation_generation,
                        ),
                    ),
                );
                match self
                    .install_snapshot_sensitive_bucket_pg_command_or_drain(
                        publisher,
                        pg_id,
                        bucket,
                        &command,
                        None,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => continue,
                }
                (command, true)
            };
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut work_budget,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            if matches!(
                outcome,
                FinishPendingMetadataCommandResult::Abandoned
                    | FinishPendingMetadataCommandResult::RetryPartialExactConflict
            ) {
                continue;
            }
            if self.finalized_bucket_deleted_on_acting_set_for_recovery(pg_id, root)? {
                break;
            }
        }
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_done",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        Ok(BucketDeleteFinalizeOutcome::Finalized)
    }

    fn finalized_bucket_deleted_on_acting_set(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<bool, BucketWriteDrainError> {
        self.finalized_bucket_deleted_on_acting_set_with_route_mode(
            pg_id,
            root,
            MetadataCommandRouteMode::Normal,
        )
    }

    fn finalized_bucket_deleted_on_acting_set_for_recovery(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<bool, BucketWriteDrainError> {
        self.finalized_bucket_deleted_on_acting_set_with_route_mode(
            pg_id,
            root,
            MetadataCommandRouteMode::Recovery,
        )
    }

    fn finalized_bucket_deleted_on_acting_set_with_route_mode(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<bool, BucketWriteDrainError> {
        let bucket = &root.bucket;
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(self.operation_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    self.operation_epoch(),
                    pg_id,
                ),
        }
        .map_err(BucketWriteDrainError::Store)?;
        let mut found_deleting = false;
        for node in nodes {
            let route = node
                .bucket_metadata_client()
                .open_bucket_delete_replica_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(pg_id),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            match route
                .head_bucket_replica_for_delete()
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info)
                    if info.bucket_incarnation_generation != root.bucket_incarnation_generation => {
                }
                Ok(info) if info.state == BucketState::Deleting => {
                    found_deleting = true;
                }
                Ok(info) => {
                    return Err(BucketWriteDrainError::Metadata(
                        MetadataError::BucketNotFinalizedForDelete { state: info.state },
                    ));
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => {}
                Err(other) => return Err(other),
            }
        }
        Ok(!found_deleting)
    }

    fn bucket_name_absent_on_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketWriteDrainError> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)
            .map_err(BucketWriteDrainError::Store)?;
        let mut found_deleting = false;
        for node in nodes {
            let route = node
                .bucket_metadata_client()
                .open_bucket_delete_replica_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(pg_id),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            match route
                .head_bucket_replica_for_delete()
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info) if info.state == BucketState::Deleting => {
                    found_deleting = true;
                }
                Ok(info) => {
                    return Err(BucketWriteDrainError::Metadata(
                        MetadataError::BucketNotFinalizedForDelete { state: info.state },
                    ));
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => {}
                Err(other) => return Err(other),
            }
        }
        Ok(!found_deleting)
    }

    pub fn load_bucket_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, crate::BucketSnapshotLoadFailure> {
        self.load_bucket_snapshot_internal(bucket, request)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub(crate) fn load_bucket_snapshot_internal(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        let route = node
            .bucket_metadata_client()
            .open_bucket_metadata_read_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
                node.authorization(),
            )?;
        route.load_bucket_snapshot(request)
    }

    pub(super) fn load_bucket_delete_authorization_snapshot_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence: _,
        } = route;
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let client = node.bucket_metadata_client();
        let metadata_route =
            client.open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)?;
        require_valid_route()?;
        let bucket_info = metadata_route.head_bucket_raw()?;
        require_valid_route()?;
        let policy = Self::load_bucket_delete_authorization_subresource(
            metadata_route.as_ref(),
            request.policy,
            BucketSubresourceKind::Policy,
        )?;
        require_valid_route()?;
        let tags = Self::load_bucket_delete_authorization_tags(
            metadata_route.as_ref(),
            request.tags.should_load(&bucket_info),
        )?;
        require_valid_route()?;
        let lifecycle = Self::load_bucket_delete_authorization_subresource(
            metadata_route.as_ref(),
            request.lifecycle,
            BucketSubresourceKind::Lifecycle,
        )?;
        require_valid_route()?;
        let cors = Self::load_bucket_delete_authorization_subresource(
            metadata_route.as_ref(),
            request.cors,
            BucketSubresourceKind::Cors,
        )?;

        Ok(BucketSnapshot {
            bucket: bucket_info,
            request,
            policy,
            tags,
            lifecycle,
            cors,
        })
    }

    /// Load a raw Active-bucket authorization snapshot only after proving that
    /// a preserved DeleteBucket attempt has become a stable write fence.
    ///
    /// The ordering matters: older bucket-write reservations must be drained
    /// before reading policy/tag authorization inputs. Once the live drain is in
    /// place and the reservation list is empty, new bucket writes cannot acquire
    /// a reservation and older writes cannot commit after the snapshot.
    #[cfg(test)]
    pub(crate) fn load_active_bucket_delete_attempt_authorization_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<Option<BucketSnapshot>, BucketSnapshotLoadError> {
        self.load_active_bucket_delete_attempt_authorization_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
        )
    }

    pub(super) fn load_active_bucket_delete_attempt_authorization_snapshot_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
    ) -> Result<Option<BucketSnapshot>, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let reservation_client = node.bucket_write_reservation_client();
        let reservation_route = self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            bucket_pg_id,
            bucket,
        )?;
        require_valid_route()?;
        let Some(drain) = reservation_route.durable_bucket_write_drain()? else {
            return Ok(None);
        };
        if drain.lease_deadline <= crate::clock::current_time_millis() {
            return Ok(None);
        }
        if !reservation_route
            .durable_bucket_write_reservations()?
            .is_empty()
        {
            return Ok(None);
        }
        require_valid_route()?;
        let pending = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
        if pending.is_some_and(|command| {
            !matches!(
                command.payload(),
                MetadataCommandPayload::MarkBucketDeleting(mark) if mark.bucket_name() == bucket
            )
        }) {
            return Ok(None);
        }

        let snapshot = match self.load_bucket_delete_authorization_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: bucket_pg_id,
                bucket,
                effect_fence,
            },
            &mut require_valid_route,
            request,
        ) {
            Ok(snapshot) => snapshot,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if snapshot.bucket.state != BucketState::Active
            || snapshot.bucket.bucket_execution_generation != drain.bucket_execution_generation
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    fn load_bucket_delete_authorization_subresource(
        route: &dyn crate::node_client::BucketMetadataRoute,
        requested: bool,
        kind: BucketSubresourceKind,
    ) -> Result<LoadedBucketSubresource<String>, BucketSnapshotLoadError> {
        if !requested {
            return Ok(LoadedBucketSubresource::NotRequested);
        }
        Ok(match route.get_bucket_subresource(kind)? {
            Some(body) => LoadedBucketSubresource::Loaded(body),
            None => LoadedBucketSubresource::Missing,
        })
    }

    fn load_bucket_delete_authorization_tags(
        route: &dyn crate::node_client::BucketMetadataRoute,
        requested: bool,
    ) -> Result<LoadedBucketSubresource<SerializedBucketTagSet>, BucketSnapshotLoadError> {
        if !requested {
            return Ok(LoadedBucketSubresource::NotRequested);
        }
        Ok(match route.get_bucket_tags()? {
            Some(tags) => LoadedBucketSubresource::Loaded(tags),
            None => LoadedBucketSubresource::Missing,
        })
    }

    pub fn load_available_bucket_execution_generation_batches(
        &self,
        buckets: &[BucketName],
    ) -> Vec<(Vec<BucketName>, HashMap<BucketName, u64>)> {
        let mut buckets_by_pg = HashMap::<u32, Vec<BucketName>>::new();
        for bucket in buckets {
            buckets_by_pg
                .entry(self.bucket_metadata_pg_id(bucket))
                .or_default()
                .push(bucket.clone());
        }

        let mut batches = Vec::new();
        for (pg_id, buckets) in buckets_by_pg {
            let pg_id = PgId::new(pg_id);
            let Ok(node) = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            else {
                continue;
            };
            let client = node.bucket_metadata_client();
            let Ok(route) = client.open_bucket_metadata_scan_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
            ) else {
                continue;
            };
            let Ok(generations) = route.load_bucket_execution_generations(&buckets) else {
                continue;
            };
            batches.push((buckets, generations));
        }
        batches
    }

    pub fn load_bucket_fast_path_identity(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<BucketFastPathIdentity>, crate::BucketSnapshotLoadFailure> {
        self.load_bucket_fast_path_identity_internal(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn load_bucket_fast_path_identity_internal(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let client = node.bucket_metadata_client();
        let route = client.open_bucket_metadata_scan_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
        )?;
        let mut identities =
            route.load_bucket_fast_path_identities(std::slice::from_ref(bucket))?;
        Ok(identities.remove(bucket))
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.with_bucket_write_snapshot_internal(bucket, request, action)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn with_bucket_write_snapshot_internal<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            action,
        )
    }

    pub(super) fn with_bucket_write_snapshot_with_route_validation<T, E>(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot_with_route_validation(
            route,
            require_valid_route,
            request,
            |snapshot| Ok(action(snapshot)),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_bucket_write_snapshot_for_command<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            BucketWriteReservationProof,
        )
            -> Result<super::BucketWriteSnapshotAction<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("bucket_write_snapshot_for_command")
                .for_pg(pg_id);
        loop {
            work_budget.check("bucket write snapshot retry budget exhausted")?;
            let reservation = match self.acquire_durable_bucket_write_reservation(
                bucket,
                "bucket-write-snapshot",
                None,
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);

            let result = (|| {
                let route = reservation.node.open_bucket_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                    bucket,
                )?;
                let snapshot = route.load_bucket_snapshot(request)?;
                action(snapshot, proof)
            })();
            let (result, release_result) = match result {
                Ok(super::BucketWriteSnapshotAction::Release(result)) => (
                    Ok(result),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
                Ok(super::BucketWriteSnapshotAction::TransferredToCommand(result)) => {
                    (Ok(result), Ok(()))
                }
                Err(error) => (
                    Err(error),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
            };
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    pub(super) fn with_put_object_bucket_write_snapshot_for_command_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            BucketWriteReservationProof,
        ) -> super::BucketWriteSnapshotAction<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            bucket,
            effect_fence,
            ..
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("put_object_bucket_write_snapshot_for_command")
                .for_pg(pg_id);
        loop {
            work_budget.check("put object bucket write snapshot retry budget exhausted")?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(route.key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);

            let result = (|| {
                require_valid_route()?;
                let storage_client = &reservation.node;
                let metadata_route = storage_client.open_bucket_metadata_route(
                    self.operation_epoch(),
                    bucket_pg_id,
                    bucket,
                )?;
                let snapshot = metadata_route.load_bucket_snapshot(request)?;
                Ok(action(snapshot, proof))
            })();
            let (result, release_result) = match result {
                Ok(super::BucketWriteSnapshotAction::Release(result)) => (
                    Ok(result),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
                Ok(super::BucketWriteSnapshotAction::TransferredToCommand(result)) => {
                    (Ok(result), Ok(()))
                }
                Err(error) => (
                    Err(error),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
            };
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    fn with_bucket_write_reservation_snapshot_with_route_validation<T, E>(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<Result<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("bucket_write_reservation_snapshot")
                .for_pg(pg_id);
        loop {
            work_budget.check("bucket write reservation snapshot retry budget exhausted")?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                "bucket-write-snapshot",
                None,
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };

            let result = (|| {
                require_valid_route()?;
                let storage_client = &reservation.node;
                let metadata_route = storage_client.open_bucket_metadata_route(
                    self.operation_epoch(),
                    bucket_pg_id,
                    bucket,
                )?;
                let snapshot = metadata_route.load_bucket_snapshot(request)?;
                action(snapshot)
            })();
            let release_result = self.release_durable_bucket_write_reservation(reservation);
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    pub(super) fn acquire_durable_bucket_write_reservation(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        self.acquire_durable_bucket_write_reservation_with_effect_fence(
            bucket,
            operation_kind,
            target_context,
            None,
        )
    }

    pub(super) fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let client = node.bucket_write_reservation_client();
        let route = self.open_bucket_write_reservation_route(
            client.as_ref(),
            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
            bucket,
        )?;
        let acquire = DurableBucketWriteReservationAcquire {
            name: bucket,
            reservation_id: &reservation_id,
            owner_token: &owner_token,
            cluster_epoch: self.operation_epoch(),
            operation_kind,
            created_at: crate::clock::current_time_millis(),
            lease_deadline: self.bucket_write_reservation_lease_deadline(),
            target_context,
        };
        let record = match effect_fence {
            Some(effect_fence) => route
                .acquire_durable_bucket_write_reservation_with_effect_fence(
                    acquire,
                    effect_fence,
                )?,
            None => route.acquire_durable_bucket_write_reservation(acquire)?,
        };
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    #[cfg(test)]
    pub(super) fn acquire_completion_durable_bucket_write_reservation(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let client = node.bucket_write_reservation_client();
        let record = self
            .open_bucket_write_reservation_route(
                client.as_ref(),
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                bucket,
            )?
            .acquire_completion_durable_bucket_write_reservation(
                DurableBucketWriteReservationAcquire {
                    name: bucket,
                    reservation_id: &reservation_id,
                    owner_token: &owner_token,
                    cluster_epoch: self.operation_epoch(),
                    operation_kind,
                    created_at: crate::clock::current_time_millis(),
                    lease_deadline: self.bucket_write_reservation_lease_deadline(),
                    target_context,
                },
            )?;
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    pub(super) fn acquire_completion_durable_bucket_write_reservation_with_effect_fence(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let client = node.bucket_write_reservation_client();
        let record = self
            .open_bucket_write_reservation_route(
                client.as_ref(),
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                bucket,
            )?
            .acquire_completion_durable_bucket_write_reservation_with_effect_fence(
                DurableBucketWriteReservationAcquire {
                    name: bucket,
                    reservation_id: &reservation_id,
                    owner_token: &owner_token,
                    cluster_epoch: self.operation_epoch(),
                    operation_kind,
                    created_at: crate::clock::current_time_millis(),
                    lease_deadline: self.bucket_write_reservation_lease_deadline(),
                    target_context,
                },
                effect_fence,
            )?;
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    pub(super) fn put_object_stream_create_lease_deadline(&self) -> u64 {
        crate::clock::current_time_millis().saturating_add(PUT_OBJECT_STREAM_CREATE_LEASE_MILLIS)
    }

    pub(super) fn bucket_write_reservation_lease_deadline(&self) -> u64 {
        crate::clock::current_time_millis().saturating_add(BUCKET_WRITE_RESERVATION_LEASE_MILLIS)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn heartbeat_put_object_stream_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        self.heartbeat_put_object_stream_session_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            session_id,
            || Ok(()),
        )
    }

    pub(super) fn heartbeat_put_object_stream_session_with_route_validation(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        require_valid_route()?;
        let stream_route = self
            .object_mutation_metadata_primary_client(bucket, key)?
            .open_stream_upload_session_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                session_id,
            )?;
        let upload = stream_route.load_session()?;
        if upload.target != StreamUploadTarget::PutObject {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream session is not a PutObject session".to_string(),
            });
        }
        let Some(stored_proof) = upload.bucket_write_reservation.as_ref() else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "PutObject stream session is missing bucket write proof".to_string(),
            });
        };
        let mut proof = stored_proof.clone();
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(ObjectPgActionError::from)?;
        let reservation_client = node.bucket_write_reservation_client();
        let reservation_route = self
            .open_bucket_write_reservation_route(reservation_client.as_ref(), bucket_pg_id, bucket)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let renewed = reservation_route
            .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                &proof,
                self.put_object_stream_create_lease_deadline(),
                effect_fence,
            );
        let renewed = match renewed {
            Ok(record) => record,
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => {
                let Some(refreshed) = self
                    .refresh_stream_upload_bucket_write_reservation_for_object_action_with_route_validation(
                        route,
                        &upload,
                        &proof,
                        &mut require_valid_route,
                    )?
                else {
                    return Err(ObjectPgActionError::Metadata(
                        MetadataError::BucketWriteReservationConflict {
                            reservation_id: proof.reservation_id.clone(),
                        },
                    ));
                };
                proof = refreshed;
                require_valid_route()?;
                reservation_route
                    .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                        &proof,
                        self.put_object_stream_create_lease_deadline(),
                        effect_fence,
                    )
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };
        let renewed_proof = BucketWriteReservationProof::from(&renewed);
        require_valid_route()?;
        stream_route.update_put_bucket_write_reservation(&proof, &renewed_proof, effect_fence)?;
        Ok(())
    }

    pub(super) fn release_durable_bucket_write_reservation(
        &self,
        reservation: super::DurableBucketWriteReservation,
    ) -> Result<(), BucketSnapshotLoadError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(reservation.pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                &reservation.record.bucket,
            )?
            .release_durable_bucket_write_reservation(&reservation.record)?;
        Ok(())
    }

    pub(super) fn wait_for_durable_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = node.bucket_metadata_client().open_bucket_metadata_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        match metadata_route.head_bucket_info() {
            Ok(_) => {}
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Err(MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into());
            }
            Err(other) => return Err(other),
        }
        let reservation_client = node.bucket_write_reservation_client();
        if let Some(expired) = self
            .open_bucket_write_reservation_route(
                reservation_client.as_ref(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
            )?
            .clear_expired_durable_bucket_write_drain(crate::clock::current_time_millis())?
        {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_write_expired_drain_rollback",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={}",
                    bucket,
                    pg_id.get(),
                    expired.drain_id
                )),
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn begin_durable_bucket_delete_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        self.begin_durable_bucket_delete_drain_with_budget(bucket, None)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_seed_bucket_delete_attempt_outcome(
        &self,
        bucket: &BucketName,
        outcome: crate::TestBucketDeleteAttemptOutcomeKind,
        phase: crate::TestBucketDeleteAttemptPhase,
        detail: String,
        post_reservation_next_object_pg_id: Option<u32>,
    ) -> Result<crate::BucketDeleteBeginRoot, BucketWriteDrainError> {
        let drain = match self.begin_durable_bucket_delete_drain(bucket)? {
            super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
            super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
                return Err(StoreError::MetadataCommandContention {
                    context: "test seed bucket delete attempt outcome already deleting",
                }
                .into());
            }
        };
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(BucketWriteDrainError::from)?;
        let bucket_info = node
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .head_bucket_raw()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if bucket_info.state != BucketState::Active {
            return Err(MetadataError::BucketNotFinalizedForDelete {
                state: bucket_info.state,
            }
            .into());
        }
        if bucket_info.bucket_execution_generation != drain.record.bucket_execution_generation {
            return Err(MetadataError::BucketWriteDrainConflict {
                drain_id: drain.record.drain_id.clone(),
            }
            .into());
        }
        let root = crate::BucketDeleteBeginRoot {
            bucket: drain.record.bucket.clone(),
            bucket_execution_generation: drain.record.bucket_execution_generation,
            bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
        };
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: drain.record.bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: outcome.into(),
            phase: phase.into(),
            detail,
            post_reservation_next_object_pg_id,
            stream_cleanup_next_object_pg_id: None,
            stream_cleanup_next_session_id_marker: None,
            stream_cleanup_aborted_uploads: false,
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        };
        let reservation_client = node.bucket_write_reservation_client();
        self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            self.validated_bucket_metadata_pg(pg_id),
            &record.bucket,
        )
        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
        .record_bucket_delete_attempt_outcome(&record)
        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(root)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn begin_durable_bucket_delete_drain_with_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        let mut require_valid_route = || Ok(());
        self.begin_durable_bucket_delete_drain_with_budget_and_route_validation(
            bucket,
            started,
            None,
            &mut require_valid_route,
        )
    }

    fn begin_durable_bucket_delete_drain_with_budget_and_route_validation(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        effect_fence: Option<AdmittedRouteEffectFence>,
        require_valid_route: &mut impl FnMut() -> Result<(), StoreError>,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        loop {
            self.check_bucket_delete_begin_work_budget(
                bucket,
                started,
                "bucket delete durable drain acquisition budget exhausted",
            )?;
            require_valid_route()?;
            let pg_id = self.bucket_metadata_pg_id(bucket);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let drain_id = self.next_bucket_write_drain_id()?;
            let owner_token = self.bucket_write_owner_token();
            let now = crate::clock::current_time_millis();
            // DeleteBucket begin work is bounded. Give a live caller a small
            // grace window, but make an abandoned Active-bucket drain
            // recoverable by later write-snapshot waiters and delete retries.
            let lease_deadline = now.saturating_add(BUCKET_DELETE_DRAIN_LEASE_MILLIS);
            require_valid_route()?;
            crate::node::maybe_run_before_begin_bucket_delete_drain_hook(bucket);
            let reservation_client = node.bucket_write_reservation_client();
            let reservation_route = self
                .open_bucket_write_reservation_route(
                    reservation_client.as_ref(),
                    self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            let begin_result = match effect_fence {
                Some(effect_fence) => reservation_route
                    .begin_durable_bucket_write_drain_with_effect_fence(
                        &drain_id,
                        &owner_token,
                        now,
                        lease_deadline,
                        effect_fence,
                    ),
                None => reservation_route.begin_durable_bucket_write_drain(
                    &drain_id,
                    &owner_token,
                    now,
                    lease_deadline,
                ),
            };
            match begin_result {
                Ok(record) => {
                    return Ok(super::DurableBucketDeleteDrainBegin::Acquired(
                        super::DurableBucketWriteDrain { pg_id, record },
                    ))
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict { .. },
                )) => {
                    if let Some(expired) = reservation_route
                        .clear_expired_durable_bucket_write_drain(
                            crate::clock::current_time_millis(),
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                    {
                        let _ = observability::event(
                            super::TRACE_TARGET,
                            "bucket_delete_expired_drain_rollback",
                            Some(format_args!(
                                "bucket={:?} pg_id={} drain_id={}",
                                bucket, pg_id, expired.drain_id
                            )),
                        );
                        continue;
                    }
                    if let Some(existing) = reservation_route
                        .durable_bucket_write_drain()
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                    {
                        let metadata_route = node
                            .bucket_metadata_client()
                            .open_bucket_metadata_route(
                                self.operation_epoch(),
                                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                                bucket,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        match metadata_route.head_bucket_raw() {
                            Ok(current)
                                if current.state == BucketState::Active
                                    && current.bucket_execution_generation
                                        == existing.bucket_execution_generation =>
                            {
                                let _ = observability::event(
                                    super::TRACE_TARGET,
                                    "bucket_delete_drain_adopted",
                                    Some(format_args!(
                                        "bucket={:?} pg_id={} drain_id={}",
                                        bucket, pg_id, existing.drain_id
                                    )),
                                );
                                let renewed = self.heartbeat_durable_bucket_delete_drain(
                                    &super::DurableBucketWriteDrain {
                                        pg_id,
                                        record: existing,
                                    },
                                )?;
                                return Ok(super::DurableBucketDeleteDrainBegin::Acquired(renewed));
                            }
                            Ok(current)
                                if current.state == BucketState::Active
                                    && current.bucket_execution_generation
                                        != existing.bucket_execution_generation =>
                            {
                                self.record_bucket_delete_attempt_outcome_for_record(
                                    PgId::new(pg_id),
                                    &existing,
                                    BucketDeleteAttemptOutcomeKind::StaleGeneration,
                                    BucketDeleteAttemptPhase::Initial,
                                    format!(
                                        "stale drain generation {} current generation {}",
                                        existing.bucket_execution_generation,
                                        current.bucket_execution_generation
                                    ),
                                );
                                node.retained_bucket_write_reservation_client()
                                    .open_retained_bucket_write_reservation_route(
                                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                                        &existing.bucket,
                                    )
                                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                                    .clear_durable_bucket_write_drain(&existing)
                                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                                let _ = observability::event(
                                    super::TRACE_TARGET,
                                    "bucket_delete_stale_drain_rollback",
                                    Some(format_args!(
                                        "bucket={:?} pg_id={} drain_id={} drain_generation={} current_generation={}",
                                        bucket,
                                        pg_id,
                                        existing.drain_id,
                                        existing.bucket_execution_generation,
                                        current.bucket_execution_generation
                                    )),
                                );
                                continue;
                            }
                            Ok(_) => {}
                            Err(BucketSnapshotLoadError::Metadata(
                                MetadataError::BucketNotFound { .. },
                            )) => {
                                return Err(MetadataError::BucketNotFound {
                                    name: bucket.clone(),
                                }
                                .into());
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                    error,
                                ));
                            }
                        }
                    }
                    let metadata_route = node
                        .bucket_metadata_client()
                        .open_bucket_metadata_route(
                            self.operation_epoch(),
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            bucket,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                    match metadata_route.head_bucket_raw() {
                        Ok(current) if current.state == BucketState::Deleting => {
                            return Ok(super::DurableBucketDeleteDrainBegin::AlreadyDeleting);
                        }
                        Ok(_) => {}
                        Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                            ..
                        })) => {
                            return Err(MetadataError::BucketNotFound {
                                name: bucket.clone(),
                            }
                            .into());
                        }
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(error) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
            }
        }
    }

    pub(super) fn clear_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(drain.pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
                &drain.record.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .clear_durable_bucket_write_drain(&drain.record)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(())
    }

    fn rollback_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        match self.clear_durable_bucket_delete_drain(drain) {
            Ok(()) => Ok(()),
            Err(BucketWriteDrainError::Metadata(
                MetadataError::BucketWriteDrainNotFound { .. }
                | MetadataError::BucketNotFound { .. },
            )) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn bounded_bucket_delete_attempt_detail(mut detail: String) -> String {
        if detail.len() > BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN {
            let mut end = BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
        }
        detail
    }

    fn record_bucket_delete_attempt_outcome_for_record(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        let result = (|| {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            self.record_bucket_delete_attempt_outcome_with_client(
                node.bucket_write_reservation_client().as_ref(),
                self.validated_bucket_metadata_pg(pg_id),
                record,
                outcome,
                phase,
                detail,
            );
            Ok::<(), BucketWriteDrainError>(())
        })();
        if let Err(error) = result {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_outcome_route_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                    record.bucket,
                    pg_id.get(),
                    record.drain_id,
                    outcome,
                    error
                )),
            );
        }
    }

    fn record_bucket_delete_attempt_outcome_with_client(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
        record: &BucketWriteDrainRecord,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        let detail = Self::bounded_bucket_delete_attempt_detail(detail);
        let route = match self.open_bucket_write_reservation_route(client, pg_id, &record.bucket) {
            Ok(route) => route,
            Err(error) => {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_attempt_outcome_route_failed",
                    Some(format_args!(
                        "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                        record.bucket,
                        pg_id.get(),
                        record.drain_id,
                        outcome,
                        error
                    )),
                );
                return;
            }
        };
        let existing = match route.bucket_delete_attempt_outcome() {
            Ok(existing) => existing.filter(|existing| {
                existing.drain_id == record.drain_id
                    && existing.cluster_epoch == record.cluster_epoch
                    && existing.bucket_execution_generation == record.bucket_execution_generation
            }),
            Err(error) => {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_attempt_outcome_progress_load_failed",
                    Some(format_args!(
                        "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                        record.bucket,
                        pg_id.get(),
                        record.drain_id,
                        outcome,
                        error
                    )),
                );
                None
            }
        };
        let post_reservation_next_object_pg_id = existing
            .as_ref()
            .and_then(|existing| existing.post_reservation_next_object_pg_id);
        let same_phase = existing
            .as_ref()
            .is_some_and(|existing| existing.phase == phase);
        let stream_cleanup_next_object_pg_id = same_phase
            .then(|| {
                existing
                    .as_ref()
                    .and_then(|existing| existing.stream_cleanup_next_object_pg_id)
            })
            .flatten();
        let stream_cleanup_next_session_id_marker = same_phase
            .then(|| {
                existing
                    .as_ref()
                    .and_then(|existing| existing.stream_cleanup_next_session_id_marker.clone())
            })
            .flatten();
        let stream_cleanup_aborted_uploads = same_phase
            && existing
                .as_ref()
                .is_some_and(|existing| existing.stream_cleanup_aborted_uploads);
        let final_visibility_next_object_pg_id = same_phase
            .then(|| {
                existing
                    .as_ref()
                    .and_then(|existing| existing.final_visibility_next_object_pg_id)
            })
            .flatten();
        let finalizer_next_object_pg_id = existing
            .as_ref()
            .and_then(|existing| existing.finalizer_next_object_pg_id);
        let outcome_record = BucketDeleteAttemptOutcomeRecord {
            bucket: record.bucket.clone(),
            drain_id: record.drain_id.clone(),
            cluster_epoch: record.cluster_epoch,
            bucket_execution_generation: record.bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id,
            stream_cleanup_next_object_pg_id,
            stream_cleanup_next_session_id_marker,
            stream_cleanup_aborted_uploads,
            final_visibility_next_object_pg_id,
            finalizer_next_object_pg_id,
            updated_at: crate::clock::current_time_millis(),
        };
        if let Err(error) = route.record_bucket_delete_attempt_outcome(&outcome_record) {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_outcome_record_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                    record.bucket,
                    pg_id.get(),
                    record.drain_id,
                    outcome,
                    error
                )),
            );
        }
    }

    fn record_bucket_delete_attempt_outcome_for_drain_with_client(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        self.record_bucket_delete_attempt_outcome_with_client(
            client,
            self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
            &drain.record,
            outcome,
            phase,
            detail,
        );
    }

    fn heartbeat_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<super::DurableBucketWriteDrain, BucketWriteDrainError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(drain.pg_id))?;
        let lease_deadline =
            crate::clock::current_time_millis().saturating_add(BUCKET_DELETE_DRAIN_LEASE_MILLIS);
        let reservation_client = node.bucket_write_reservation_client();
        let route = self
            .open_bucket_write_reservation_route(
                reservation_client.as_ref(),
                self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
                &drain.record.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        match route.heartbeat_durable_bucket_write_drain(&drain.record, lease_deadline) {
            Ok(record) => Ok(super::DurableBucketWriteDrain {
                pg_id: drain.pg_id,
                record,
            }),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDrainConflict { .. }
                | MetadataError::BucketWriteDrainNotFound { .. },
            )) => Err(StoreError::MetadataCommandContention {
                context: "stale bucket delete drain before mark deleting",
            }
            .into()),
            Err(error) => Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
        }
    }

    fn wait_for_durable_bucket_write_reservations_empty(
        &self,
        bucket: &BucketName,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        delete_started: std::time::Instant,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketWriteDrainError> {
        let started = std::time::Instant::now();
        loop {
            let pg_id = self.bucket_metadata_pg_id(bucket);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let reservation_client = node.bucket_write_reservation_client();
            let reservations = self
                .open_bucket_write_reservation_route(
                    reservation_client.as_ref(),
                    self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                .durable_bucket_write_reservations()
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            let now = crate::clock::current_time_millis();
            let expired: Vec<_> = reservations
                .iter()
                .filter(|reservation| reservation.lease_deadline <= now)
                .cloned()
                .collect();
            if !expired.is_empty() {
                for reservation in expired {
                    match node
                        .retained_bucket_write_reservation_client()
                        .open_retained_bucket_write_reservation_route(
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            &reservation.bucket,
                        )
                        .and_then(|route| {
                            route.release_durable_bucket_write_reservation(&reservation)
                        }) {
                        Ok(()) => {
                            let _ = observability::event(
                                super::TRACE_TARGET,
                                "bucket_write_expired_reservation_release",
                                Some(format_args!(
                                    "bucket={:?} pg_id={} reservation_id={} operation_kind={}",
                                    bucket,
                                    pg_id,
                                    reservation.reservation_id,
                                    reservation.operation_kind
                                )),
                            );
                        }
                        Err(BucketSnapshotLoadError::Metadata(
                            MetadataError::BucketWriteReservationNotFound { .. },
                        )) => {}
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                        }
                    }
                }
                continue;
            }
            if reservations.is_empty() {
                return Ok(());
            }
            if let Err(error) = self.check_bucket_delete_begin_work_budget(
                bucket,
                Some(delete_started),
                "bucket delete reservation wait begin budget exhausted",
            ) {
                self.record_bucket_delete_reservation_wait_blocker(
                    client,
                    drain,
                    &reservations,
                    "begin budget exhausted",
                );
                let _ = error;
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                    conflicting_pending_metadata_command(
                        BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    ),
                ));
            }
            if started.elapsed()
                >= std::time::Duration::from_millis(BUCKET_DELETE_RESERVATION_DRAIN_WAIT_MILLIS)
            {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_reservation_wait_timeout",
                    Some(format_args!(
                        "bucket={:?} pg_id={} {}",
                        bucket,
                        pg_id,
                        Self::bucket_delete_reservation_wait_blocker_detail(
                            &reservations,
                            "timeout",
                        )
                    )),
                );
                self.record_bucket_delete_reservation_wait_blocker(
                    client,
                    drain,
                    &reservations,
                    "timeout",
                );
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                    conflicting_pending_metadata_command(
                        BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    ),
                ));
            }
            self.drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                bucket,
                Some(delete_started),
                work_budget,
                None,
            )?;
            crate::node::maybe_run_bucket_write_drain_wait_hook(bucket);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn record_bucket_delete_reservation_wait_blocker(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        reservations: &[BucketWriteReservationRecord],
        reason: &'static str,
    ) {
        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
            client,
            drain,
            BucketDeleteAttemptOutcomeKind::Retryable,
            BucketDeleteAttemptPhase::ReservationWait,
            Self::bucket_delete_reservation_wait_blocker_detail(reservations, reason),
        );
    }

    fn bucket_delete_reservation_wait_blocker_detail(
        reservations: &[BucketWriteReservationRecord],
        reason: &'static str,
    ) -> String {
        let now = crate::clock::current_time_millis();
        let first = reservations
            .first()
            .expect("reservation-wait blocker detail requires at least one reservation");
        let first_lease_state = if first.lease_deadline <= now {
            "expired"
        } else {
            "live"
        };
        format!(
            "reservation wait {reason}: reservations={} first_reservation_id={} first_operation_kind={} first_target_context={:?} first_lease_state={}",
            reservations.len(),
            first.reservation_id,
            first.operation_kind,
            first.target_context,
            first_lease_state
        )
    }

    pub(super) fn stream_upload_has_live_bucket_write_reservation(
        &self,
        upload: &StreamUploadRecord,
    ) -> Result<bool, BucketWriteDrainError> {
        let Some(proof) = upload.bucket_write_reservation.as_ref() else {
            return Ok(false);
        };
        let pg_id = PgId::new(self.bucket_metadata_pg_id(&proof.bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let reservation_client = node.bucket_write_reservation_client();
        let route = self
            .open_bucket_write_reservation_route(
                reservation_client.as_ref(),
                self.validated_bucket_metadata_pg(pg_id),
                &proof.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        match route.validate_bucket_write_reservation_proof(proof) {
            Ok(()) => self.refresh_stream_upload_bucket_write_reservation(upload, proof),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationNotFound { .. },
            )) => Ok(false),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => self.refresh_stream_upload_bucket_write_reservation(upload, proof),
            Err(error) => Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
        }
    }

    fn refresh_stream_upload_bucket_write_reservation(
        &self,
        upload: &StreamUploadRecord,
        proof: &BucketWriteReservationProof,
    ) -> Result<bool, BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(&proof.bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let now = crate::clock::current_time_millis();
        let reservation_client = node.bucket_write_reservation_client();
        let reservations = self
            .open_bucket_write_reservation_route(
                reservation_client.as_ref(),
                self.validated_bucket_metadata_pg(pg_id),
                &proof.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .durable_bucket_write_reservations()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let Some(current) = reservations
            .into_iter()
            .find(|record| proof.matches_record(record) && record.lease_deadline > now)
        else {
            return Ok(false);
        };
        let renewed = BucketWriteReservationProof::from(&current);
        if *proof == renewed {
            return Ok(true);
        }
        self.object_mutation_metadata_primary_client(&upload.bucket, &upload.key)
            .map_err(BucketWriteDrainError::Store)?
            .open_stream_upload_session_metadata_route(
                self.operation_epoch(),
                self.object_metadata_pg(&upload.bucket, &upload.key),
                &upload.bucket,
                &upload.key,
                &upload.session_id,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .update_put_bucket_write_reservation(
                proof,
                &renewed,
                AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(true)
    }

    fn refresh_stream_upload_bucket_write_reservation_for_object_action_with_route_validation(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        upload: &StreamUploadRecord,
        proof: &BucketWriteReservationProof,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if upload.bucket != *bucket || upload.key != *key || proof.bucket != *bucket {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "refresh put object stream reservation",
                },
            ));
        }
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(ObjectPgActionError::from)?;
        let now = crate::clock::current_time_millis();
        let reservation_client = node.bucket_write_reservation_client();
        let reservations = self
            .open_bucket_write_reservation_route(reservation_client.as_ref(), bucket_pg_id, bucket)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            .durable_bucket_write_reservations()
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let Some(current) = reservations
            .into_iter()
            .find(|record| proof.matches_record(record) && record.lease_deadline > now)
        else {
            return Ok(None);
        };
        let renewed = BucketWriteReservationProof::from(&current);
        require_valid_route()?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_stream_upload_session_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                &upload.session_id,
            )?
            .update_put_bucket_write_reservation(proof, &renewed, effect_fence)?;
        Ok(Some(renewed))
    }

    fn bucket_delete_not_empty_error(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
        source: BucketVisibleDataSource,
    ) -> BucketWriteDrainError {
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_not_empty",
            format!(
                "bucket={:?} pg_id={} source={} source_pg_id={}",
                bucket,
                pg_id.get(),
                source.label(),
                source.pg_id().get()
            ),
        );
        if bucket_delete_visible_data_diagnostics_enabled() {
            eprintln!(
                "bucket delete begin found visible data source={} source_pg_id={}",
                source.label(),
                source.pg_id().get()
            );
        }
        crate::error::MetadataError::BucketNotEmpty.into()
    }

    fn emit_bucket_delete_begin_loop_step(
        bucket: &BucketName,
        pg_id: PgId,
        started: std::time::Instant,
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
            super::TRACE_TARGET,
            "bucket_delete_begin_loop_step",
            format!(
                "bucket={:?} pg_id={} step={} elapsed_us={}{}",
                bucket,
                pg_id.get(),
                step,
                started.elapsed().as_micros(),
                suffix
            ),
        );
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        work_budget: &mut super::RequestWorkBudget,
        progress: Option<BucketDeleteExactDrainProgress<'_>>,
    ) -> Result<(), BucketWriteDrainError> {
        let object_pg_ids: Vec<PgId> = self.metadata_pg_ids().into_iter().map(PgId::new).collect();
        let next_object_pg_id = match progress {
            Some(progress) => self
                .bucket_delete_post_reservation_next_object_pg_id(progress)?
                .unwrap_or(0),
            None => 0,
        };
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_before_bucket_delete_exact_drain_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            progress.is_some(),
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        let mut scanned_count = 0usize;
        let mut drained_count = 0usize;
        for object_pg_id in object_pg_ids
            .iter()
            .copied()
            .filter(|object_pg_id| object_pg_id.get() >= next_object_pg_id)
        {
            self.check_bucket_delete_begin_work_budget(
                bucket,
                started,
                BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
            )?;
            if let Some(started) = started {
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    self.bucket_metadata_pg_id(bucket).into(),
                    started,
                    "probe_exact_bucket_object_pg_start",
                    format!("object_pg_id={}", object_pg_id.get()),
                );
            }
        }
        for chunk in object_pg_ids
            .iter()
            .copied()
            .filter(|object_pg_id| object_pg_id.get() >= next_object_pg_id)
            .collect::<Vec<_>>()
            .chunks(BUCKET_DELETE_EXACT_BUCKET_PENDING_PROBE_PARALLELISM)
        {
            let chunk_pending =
                self.pending_exact_bucket_metadata_commands_on_pgs(bucket, chunk, started)?;
            scanned_count += chunk.len();
            if let Some(started) = started {
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    self.bucket_metadata_pg_id(bucket).into(),
                    started,
                    "probe_exact_bucket_object_pg_chunk_done",
                    format!(
                        "chunk_pg_count={} chunk_exact_pending_count={} scanned_count={}",
                        chunk.len(),
                        chunk_pending.len(),
                        scanned_count
                    ),
                );
            }
            for (object_pg_id, command) in chunk_pending {
                self.check_bucket_delete_begin_work_budget(
                    bucket,
                    started,
                    BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                )?;
                if let Some(started) = started {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        self.bucket_metadata_pg_id(bucket).into(),
                        started,
                        "drain_exact_bucket_object_pg_start",
                        format!(
                            "object_pg_id={} command_kind={}",
                            object_pg_id.get(),
                            command.payload().kind_name()
                        ),
                    );
                }
                self.drain_pending_object_metadata_commands_for_exact_bucket_with_work_budget(
                    object_pg_id,
                    bucket,
                    work_budget,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                drained_count += 1;
                if let Some(started) = started {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        self.bucket_metadata_pg_id(bucket).into(),
                        started,
                        "drain_exact_bucket_object_pg_done",
                        format!("object_pg_id={}", object_pg_id.get()),
                    );
                }
            }
            if let (Some(progress), Some(last_pg)) = (progress, chunk.last()) {
                let next_object_pg_id = last_pg.get().saturating_add(1);
                self.record_bucket_delete_post_reservation_next_object_pg_id(
                    progress,
                    next_object_pg_id,
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                if progress.phase == BucketDeleteAttemptPhase::PostReservationObjectDrain {
                    maybe_run_after_bucket_delete_post_reservation_progress_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        next_object_pg_id,
                    )
                    .map_err(BucketWriteDrainError::from)?;
                }
            }
        }
        self.check_bucket_delete_begin_work_budget(
            bucket,
            started,
            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
        )?;
        if let Some(started) = started {
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                self.bucket_metadata_pg_id(bucket).into(),
                started,
                "probe_exact_bucket_object_pgs_done",
                format!(
                    "object_pg_count={} skipped_before_pg={} scanned_count={} drained_count={}",
                    object_pg_ids.len(),
                    next_object_pg_id,
                    scanned_count,
                    drained_count
                ),
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_record_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        drain: &super::DurableBucketWriteDrain,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.record_bucket_delete_post_reservation_next_object_pg_id(
            BucketDeleteExactDrainProgress {
                client: node.bucket_write_reservation_client().as_ref(),
                drain,
                phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
            },
            next_object_pg_id,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<Option<u32>, BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.bucket_delete_post_reservation_next_object_pg_id(BucketDeleteExactDrainProgress {
            client: node.bucket_write_reservation_client().as_ref(),
            drain,
            phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation(
        &self,
        bucket: &BucketName,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_begin")
        .for_pg(pg_id);
        self.drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
            bucket,
            None,
            &mut work_budget,
            Some(BucketDeleteExactDrainProgress {
                client: node.bucket_write_reservation_client().as_ref(),
                drain,
                phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
            }),
        )
    }

    fn pending_exact_bucket_metadata_commands_on_pgs(
        &self,
        bucket: &BucketName,
        object_pg_ids: &[PgId],
        started: Option<std::time::Instant>,
    ) -> Result<Vec<(PgId, MetadataCommandEnvelope)>, BucketWriteDrainError> {
        let mut exact_pending = Vec::new();
        let mut chunk_pending = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(object_pg_ids.len());
            for &object_pg_id in object_pg_ids {
                handles.push((
                    object_pg_id,
                    scope.spawn(move || {
                        self.pending_metadata_command_for_bucket(object_pg_id, bucket)
                    }),
                ));
            }

            let mut chunk_pending = Vec::new();
            for (object_pg_id, handle) in handles {
                let pending = match handle.join() {
                    Ok(result) => result.map_err(BucketWriteDrainError::from)?,
                    Err(payload) => std::panic::resume_unwind(payload),
                };
                if let Some(command) = pending.filter(|command| command.bucket_name() == bucket) {
                    chunk_pending.push((object_pg_id, command));
                }
            }
            Ok::<_, BucketWriteDrainError>(chunk_pending)
        })?;
        exact_pending.append(&mut chunk_pending);
        self.check_bucket_delete_begin_work_budget(
            bucket,
            started,
            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
        )?;
        Ok(exact_pending)
    }

    fn bucket_delete_post_reservation_next_object_pg_id(
        &self,
        progress: BucketDeleteExactDrainProgress<'_>,
    ) -> Result<Option<u32>, BucketWriteDrainError> {
        let record = self.bucket_delete_matching_attempt_outcome(
            progress.client,
            self.validated_bucket_metadata_pg(PgId::new(progress.drain.pg_id)),
            progress.drain,
        )?;
        Ok(record.and_then(|record| record.post_reservation_next_object_pg_id))
    }

    fn bucket_delete_matching_attempt_outcome(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketWriteDrainError> {
        let record = self
            .open_bucket_write_reservation_route(client, pg_id, &drain.record.bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .bucket_delete_attempt_outcome()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(record.filter(|record| {
            record.drain_id == drain.record.drain_id
                && record.cluster_epoch == drain.record.cluster_epoch
                && record.bucket_execution_generation == drain.record.bucket_execution_generation
        }))
    }

    fn record_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        progress: BucketDeleteExactDrainProgress<'_>,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let route = self
            .open_bucket_write_reservation_route(
                progress.client,
                self.validated_bucket_metadata_pg(PgId::new(progress.drain.pg_id)),
                &progress.drain.record.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let existing = match route.bucket_delete_attempt_outcome() {
            Ok(existing) => existing,
            Err(error) => {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_attempt_progress_load_failed",
                    Some(format_args!(
                        "bucket={:?} pg_id={} drain_id={} next_object_pg_id={} error={:?}",
                        progress.drain.record.bucket,
                        progress.drain.pg_id,
                        progress.drain.record.drain_id,
                        next_object_pg_id,
                        error
                    )),
                );
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
            }
        };
        let existing = existing.filter(|record| {
            record.drain_id == progress.drain.record.drain_id
                && record.cluster_epoch == progress.drain.record.cluster_epoch
                && record.bucket_execution_generation
                    == progress.drain.record.bucket_execution_generation
        });
        let finalizer_next_object_pg_id = existing
            .as_ref()
            .and_then(|record| record.finalizer_next_object_pg_id);
        let stream_cleanup_next_object_pg_id = existing
            .as_ref()
            .and_then(|record| record.stream_cleanup_next_object_pg_id);
        let stream_cleanup_next_session_id_marker = existing
            .as_ref()
            .and_then(|record| record.stream_cleanup_next_session_id_marker.clone());
        let stream_cleanup_aborted_uploads = existing
            .as_ref()
            .is_some_and(|record| record.stream_cleanup_aborted_uploads);
        let final_visibility_next_object_pg_id = existing
            .as_ref()
            .and_then(|record| record.final_visibility_next_object_pg_id);
        let (outcome, phase, detail) = existing
            .map(|record| (record.outcome, record.phase, record.detail))
            .unwrap_or_else(|| {
                (
                    BucketDeleteAttemptOutcomeKind::Retryable,
                    progress.phase,
                    format!(
                        "{:?} exact-bucket drain progressed to object PG {next_object_pg_id}",
                        progress.phase
                    ),
                )
            });
        let detail = Self::bounded_bucket_delete_attempt_detail(detail);
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: progress.drain.record.bucket.clone(),
            drain_id: progress.drain.record.drain_id.clone(),
            cluster_epoch: progress.drain.record.cluster_epoch,
            bucket_execution_generation: progress.drain.record.bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id: Some(next_object_pg_id),
            stream_cleanup_next_object_pg_id,
            stream_cleanup_next_session_id_marker,
            stream_cleanup_aborted_uploads,
            final_visibility_next_object_pg_id,
            finalizer_next_object_pg_id,
            updated_at: crate::clock::current_time_millis(),
        };
        if let Err(error) = route.record_bucket_delete_attempt_outcome(&record) {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_progress_record_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} next_object_pg_id={} error={:?}",
                    progress.drain.record.bucket,
                    progress.drain.pg_id,
                    progress.drain.record.drain_id,
                    next_object_pg_id,
                    error
                )),
            );
            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
        }
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_after_bucket_delete_exact_drain_progress_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            progress.phase,
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        Ok(())
    }

    fn check_bucket_delete_begin_work_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        context: &'static str,
    ) -> Result<(), BucketWriteDrainError> {
        self.require_route_map_valid_now()?;
        let Some(started) = started else {
            return Ok(());
        };
        if started.elapsed()
            < std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS)
        {
            return Ok(());
        }
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_work_budget_exhausted",
            format!("bucket={:?} context={}", bucket, context),
        );
        let _ = observability::emit_metadata_command_budget_exhausted(
            super::TRACE_TARGET,
            observability::MetadataCommandBudgetExhaustedSummary {
                pg_id: Some(self.bucket_metadata_pg_id(bucket)),
                operation: "bucket_delete_begin",
                context,
                elapsed_us: started.elapsed().as_micros(),
                budget_us: u128::from(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS) * 1_000,
                attempts: 0,
                max_attempts: None,
            },
        );
        Err(bucket_snapshot_error_to_bucket_write_drain_error(
            conflicting_pending_metadata_command(context),
        ))
    }

    fn finish_bucket_write_snapshot_operation<T, E>(
        result: Result<Result<T, E>, BucketSnapshotLoadError>,
        release_result: Result<(), BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        match (result, release_result) {
            (Ok(Ok(value)), Ok(())) => Ok(Ok(value)),
            (Ok(Ok(_)), Err(err)) => Err(err),
            (Ok(Err(err)), Ok(())) => Ok(Err(err)),
            (Ok(Err(err)), Err(_)) => Ok(Err(err)),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn begin_bucket_delete_if_current(
        &self,
        bucket: &BucketName,
        bucket_identity: BucketIdentityGenerations,
    ) -> Result<(), BucketWriteDrainError> {
        self.begin_bucket_delete_if_current_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            bucket_identity,
        )
    }

    /// Continue a durably recorded DeleteBucket attempt adopted by background
    /// recovery on the currently installed route.
    ///
    /// This is convergence authority for an already authorized attempt, not a
    /// frontend request entry point. New DeleteBucket requests must use an
    /// admitted [`super::ActiveBucketRoute`].
    pub(crate) fn continue_adopted_bucket_delete(
        &self,
        root: &crate::BucketDeleteBeginRoot,
    ) -> Result<(), BucketWriteDrainError> {
        let bucket = &root.bucket;
        self.begin_bucket_delete_if_current_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || self.require_route_map_valid_now(),
            BucketIdentityGenerations {
                bucket_execution_generation: root.bucket_execution_generation,
                bucket_incarnation_generation: root.bucket_incarnation_generation,
            },
        )
    }

    pub(super) fn begin_bucket_delete_if_current_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        expected_bucket_identity: BucketIdentityGenerations,
    ) -> Result<(), BucketWriteDrainError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(BeginBucketDelete);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        require_valid_route()?;
        let started = std::time::Instant::now();
        let pg_id = bucket_pg_id.pg_id();
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_start",
            format!("bucket={:?} pg_id={}", bucket, pg_id.get()),
        );
        let node_store = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node_store) => node_store,
            Err(error) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_failed",
                    format!(
                        "bucket={:?} pg_id={} phase=primary_node elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error.into());
            }
        };
        let metadata_route = node_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(self.operation_epoch(), bucket_pg_id, bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let current_bucket_execution_generation;
        let current_bucket_incarnation_generation;
        {
            require_valid_route()?;
            let raw_snapshot_started = std::time::Instant::now();
            let _ = observability::emit_flight_event(
                super::TRACE_TARGET,
                "bucket_delete_begin_raw_snapshot_start",
                format!(
                    "bucket={:?} pg_id={} elapsed_us={}",
                    bucket,
                    pg_id.get(),
                    started.elapsed().as_micros()
                ),
            );
            let current = match metadata_route.head_bucket_raw() {
                Ok(current) => {
                    let _ = observability::emit_flight_event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_raw_snapshot_done",
                        format!(
                            "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                            bucket,
                            pg_id.get(),
                            raw_snapshot_started.elapsed().as_micros(),
                            started.elapsed().as_micros()
                        ),
                    );
                    current
                }
                Err(error) => {
                    let _ = observability::emit_flight_event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_raw_snapshot_failed",
                        format!(
                            "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} error={:?}",
                            bucket,
                            pg_id.get(),
                            raw_snapshot_started.elapsed().as_micros(),
                            started.elapsed().as_micros(),
                            error
                        ),
                    );
                    return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                }
            };
            if current.state == BucketState::Deleting {
                if current.bucket_incarnation_generation
                    != expected_bucket_identity.bucket_incarnation_generation
                {
                    return Err(StoreError::MetadataCommandContention {
                        context: "stale delete bucket authorization",
                    }
                    .into());
                }
                if let Some(command) = self
                    .pending_metadata_command_for_bucket(pg_id, bucket)
                    .map_err(BucketWriteDrainError::from)?
                {
                    if matches!(
                        command.payload(),
                        MetadataCommandPayload::MarkBucketDeleting(mark)
                            if mark.bucket_name() == bucket
                    ) {
                        let mut work_budget = super::RequestWorkBudget::new(
                            std::time::Duration::from_millis(
                                BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS,
                            ),
                            None,
                        )
                        .for_operation("bucket_delete_begin")
                        .for_pg(pg_id);
                        let outcome = self
                            .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        if matches!(
                            outcome,
                            FinishPendingMetadataCommandResult::RetryPartialExactConflict
                        ) {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                conflicting_pending_metadata_command(
                                    "retryable partial pending mark bucket deleting command",
                                ),
                            ));
                        }
                    }
                }
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!("bucket={:?} pg_id={}", bucket, pg_id.get()),
                );
                return Ok(());
            }
            if current.bucket_execution_generation
                != expected_bucket_identity.bucket_execution_generation
                || current.bucket_incarnation_generation
                    != expected_bucket_identity.bucket_incarnation_generation
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "stale delete bucket authorization",
                }
                .into());
            }
            current_bucket_execution_generation = current.bucket_execution_generation;
            current_bucket_incarnation_generation = current.bucket_incarnation_generation;
        }
        let durable_drain_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_durable_drain_start",
            format!(
                "bucket={:?} pg_id={} elapsed_us={}",
                bucket,
                pg_id.get(),
                started.elapsed().as_micros()
            ),
        );
        let mut durable_drain = match self
            .begin_durable_bucket_delete_drain_with_budget_and_route_validation(
                bucket,
                Some(started),
                Some(effect_fence),
                &mut require_valid_route,
            ) {
            Ok(super::DurableBucketDeleteDrainBegin::Acquired(drain)) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_acquired",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros()
                    ),
                );
                drain
            }
            Ok(super::DurableBucketDeleteDrainBegin::AlreadyDeleting) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_already_deleting",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros()
                    ),
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros()
                    ),
                );
                return Ok(());
            }
            Err(error) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_failed",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error);
            }
        };
        crate::node::maybe_run_after_begin_bucket_delete_drain_hook(bucket);

        let mut metadata_contention_retries = 0usize;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_begin")
        .for_pg(pg_id);
        let mut loop_iteration = 0u64;
        let mut attempt_phase = BucketDeleteAttemptPhase::Initial;
        let mut can_resume_at_mark_deleting = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::FinalVisibilityProven
            });
        let mut can_resume_at_final_visibility = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::FinalVisibilityCheck
            });
        let mut can_resume_at_stream_cleanup = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::StreamCleanup
            });
        let mut can_resume_at_reservation_wait = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::ReservationWait
            });
        let mut can_resume_at_post_reservation_object_drain = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::PostReservationObjectDrain
            });
        let mut can_resume_at_post_reservation_stream_cleanup = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::PostReservationStreamCleanup
            });
        let result = (|| loop {
            loop_iteration += 1;
            attempt_phase = BucketDeleteAttemptPhase::Initial;
            require_valid_route()?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "iteration_start",
                format!("iteration={loop_iteration}"),
            );
            self.check_bucket_delete_begin_work_budget(
                bucket,
                Some(started),
                "bucket delete begin metadata convergence budget exhausted",
            )?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "pending_command_lookup_start",
                format!("iteration={loop_iteration}"),
            );
            let pending_command = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "pending_command_lookup_done",
                format!(
                    "iteration={} has_pending={} command_kind={}",
                    loop_iteration,
                    pending_command.is_some(),
                    pending_command
                        .as_ref()
                        .map_or("none", |command| command.payload().kind_name())
                ),
            );
            let (command, clear_pending_on_zero_apply) = if let Some(command) = pending_command {
                can_resume_at_mark_deleting = false;
                can_resume_at_final_visibility = false;
                can_resume_at_stream_cleanup = false;
                can_resume_at_reservation_wait = false;
                can_resume_at_post_reservation_object_drain = false;
                can_resume_at_post_reservation_stream_cleanup = false;
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    super::sleep_after_metadata_contention_retry_for(
                        "bucket_delete_begin",
                        Some(pg_id),
                        "bucket delete drain unrelated bucket command",
                        &mut metadata_contention_retries,
                    );
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::MarkBucketDeleting(mark)
                        if mark.bucket_name() == bucket =>
                    {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "mark_matches_current_start",
                            format!("iteration={loop_iteration}"),
                        );
                        if !metadata_route
                            .pending_mark_bucket_deleting_command_matches_current(mark)
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                        {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                conflicting_pending_metadata_command(
                                    "conflicting pending mark bucket deleting command",
                                ),
                            ));
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "mark_matches_current_done",
                            format!("iteration={loop_iteration}"),
                        );
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_existing_mark_start",
                            format!("iteration={loop_iteration}"),
                        );
                        durable_drain =
                            self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_existing_mark_done",
                            format!("iteration={loop_iteration}"),
                        );
                        (command, false)
                    }
                    MetadataCommandPayload::MarkBucketDeleting(_) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_other_mark_deleting_start",
                            format!("iteration={loop_iteration}"),
                        );
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_other_mark_deleting_done",
                            format!("iteration={loop_iteration}"),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain other mark deleting",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                    MetadataCommandPayload::CreateBucket(_)
                    | MetadataCommandPayload::PutBucketVersioning(_)
                    | MetadataCommandPayload::PutBucketAcl(_)
                    | MetadataCommandPayload::PutBucketProperty(_)
                    | MetadataCommandPayload::PutBucketSubresource(_)
                    | MetadataCommandPayload::DeleteFinalizedBucket(_)
                    | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_bucket_pg_command_start",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_bucket_pg_command_done",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain bucket pg command",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
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
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_start",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        match self
                            .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                bucket,
                                Some(started),
                                &mut work_budget,
                                Some(BucketDeleteExactDrainProgress {
                                    client: node_store.bucket_write_reservation_client().as_ref(),
                                    drain: &durable_drain,
                                    phase: BucketDeleteAttemptPhase::Initial,
                                }),
                            ) {
                            Ok(()) => {}
                            Err(error @ BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention {
                                    context:
                                        BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                },
                            )) => return Err(error),
                            Err(BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention { .. },
                            )) => {
                                super::sleep_after_metadata_contention_retry_for(
                                    "bucket_delete_begin",
                                    Some(pg_id),
                                    "bucket delete drain exact bucket object commands contention",
                                    &mut metadata_contention_retries,
                                );
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_done",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain exact bucket object commands",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                }
            } else {
                if can_resume_at_mark_deleting {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "resume_mark_deleting_after_final_visibility",
                        format!("iteration={loop_iteration}"),
                    );
                } else {
                    if can_resume_at_final_visibility {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "resume_final_visibility",
                            format!("iteration={loop_iteration}"),
                        );
                    } else {
                        if can_resume_at_post_reservation_stream_cleanup {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_post_reservation_stream_cleanup",
                                format!("iteration={loop_iteration}"),
                            );
                        } else if can_resume_at_post_reservation_object_drain {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_post_reservation_object_drain",
                                format!("iteration={loop_iteration}"),
                            );
                        } else if can_resume_at_reservation_wait {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_reservation_wait",
                                format!("iteration={loop_iteration}"),
                            );
                        } else if can_resume_at_stream_cleanup {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_stream_cleanup",
                                format!("iteration={loop_iteration}"),
                            );
                        } else {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_start",
                                format!(
                                    "iteration={} command_kind=none pass=initial",
                                    loop_iteration
                                ),
                            );
                            match self
                                .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                    bucket,
                                    Some(started),
                                    &mut work_budget,
                                    Some(BucketDeleteExactDrainProgress {
                                        client: node_store
                                            .bucket_write_reservation_client()
                                            .as_ref(),
                                        drain: &durable_drain,
                                        phase: BucketDeleteAttemptPhase::Initial,
                                    }),
                                ) {
                                Ok(()) => {}
                                Err(error @ BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention {
                                        context:
                                            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                    },
                                )) => return Err(error),
                                Err(BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention { .. },
                                )) => {
                                    super::sleep_after_metadata_contention_retry_for(
                                        "bucket_delete_begin",
                                        Some(pg_id),
                                        "bucket delete initial exact bucket object drain contention",
                                        &mut metadata_contention_retries,
                                    );
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_done",
                                format!(
                                    "iteration={} command_kind=none pass=initial",
                                    loop_iteration
                                ),
                            );
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "pending_command_recheck_start",
                                format!("iteration={} pass=after_object_drain", loop_iteration),
                            );
                            let pending_after_object_drain =
                                self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "pending_command_recheck_done",
                                format!(
                                    "iteration={} pass=after_object_drain has_pending={} command_kind={}",
                                    loop_iteration,
                                    pending_after_object_drain.is_some(),
                                    pending_after_object_drain
                                        .as_ref()
                                        .map_or("none", |command| command.payload().kind_name())
                                ),
                            );
                            if pending_after_object_drain.is_some() {
                                super::sleep_after_metadata_contention_retry_for(
                                    "bucket_delete_begin",
                                    Some(pg_id),
                                    "bucket delete pending command remained after object drain",
                                    &mut metadata_contention_retries,
                                );
                                continue;
                            }
                        }
                        if !can_resume_at_post_reservation_object_drain
                            && !can_resume_at_reservation_wait
                            && !can_resume_at_post_reservation_stream_cleanup
                        {
                            attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_stream_cleanup_start",
                                format!("iteration={loop_iteration}"),
                            );
                            durable_drain =
                                self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_stream_cleanup_done",
                                format!("iteration={loop_iteration}"),
                            );
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "stream_cleanup_start",
                                format!("iteration={loop_iteration}"),
                            );
                            attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                            self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptOutcomeKind::Retryable,
                                BucketDeleteAttemptPhase::StreamCleanup,
                                "stream cleanup started".to_string(),
                            );
                            self.record_bucket_delete_post_reservation_next_object_pg_id(
                                BucketDeleteExactDrainProgress {
                                    client: node_store.bucket_write_reservation_client().as_ref(),
                                    drain: &durable_drain,
                                    phase: BucketDeleteAttemptPhase::StreamCleanup,
                                },
                                0,
                            )?;
                            can_resume_at_stream_cleanup = true;
                            match self.cleanup_abandoned_put_object_stream_uploads_for_bucket(
                                bucket,
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptPhase::StreamCleanup,
                            )? {
                                PutObjectStreamUploadCleanup::Live(source) => {
                                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                        node_store.bucket_write_reservation_client().as_ref(),
                                        &durable_drain,
                                        BucketDeleteAttemptOutcomeKind::NotEmpty,
                                        BucketDeleteAttemptPhase::StreamCleanup,
                                        format!(
                                            "live stream blocker during cleanup before reservation wait: {source:?}"
                                        ),
                                    );
                                    return Err(
                                        self.bucket_delete_not_empty_error(bucket, pg_id, source)
                                    );
                                }
                                PutObjectStreamUploadCleanup::Aborted { count, .. } => {
                                    Self::emit_bucket_delete_begin_loop_step(
                                        bucket,
                                        pg_id,
                                        started,
                                        "stream_cleanup_done",
                                        format!(
                                            "iteration={} pass=before_reservation_wait aborted_stream_uploads={}",
                                            loop_iteration, count
                                        ),
                                    );
                                }
                            }
                            attempt_phase = BucketDeleteAttemptPhase::ReservationWait;
                            self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptOutcomeKind::Retryable,
                                BucketDeleteAttemptPhase::ReservationWait,
                                "stream cleanup completed before reservation wait".to_string(),
                            );
                            can_resume_at_stream_cleanup = false;
                            can_resume_at_reservation_wait = true;
                            #[cfg(any(test, feature = "test-hooks"))]
                            maybe_run_after_bucket_delete_reservation_wait_ready_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                            )
                            .map_err(BucketWriteDrainError::from)?;
                        }
                        if !can_resume_at_post_reservation_object_drain
                            && !can_resume_at_post_reservation_stream_cleanup
                        {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "wait_reservations_empty_start",
                                format!("iteration={loop_iteration}"),
                            );
                            attempt_phase = BucketDeleteAttemptPhase::ReservationWait;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_reservation_wait_start",
                                format!("iteration={loop_iteration}"),
                            );
                            durable_drain =
                                self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_reservation_wait_done",
                                format!("iteration={loop_iteration}"),
                            );
                            self.wait_for_durable_bucket_write_reservations_empty(
                                bucket,
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                started,
                                &mut work_budget,
                            )?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "wait_reservations_empty_done",
                                format!("iteration={loop_iteration}"),
                            );
                        }
                        if !can_resume_at_post_reservation_stream_cleanup {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_start",
                                format!(
                                    "iteration={} command_kind=none pass=after_reservation_wait",
                                    loop_iteration
                                ),
                            );
                            attempt_phase = BucketDeleteAttemptPhase::PostReservationObjectDrain;
                            match self
                                .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                    bucket,
                                    Some(started),
                                    &mut work_budget,
                                    Some(BucketDeleteExactDrainProgress {
                                        client: node_store.bucket_write_reservation_client().as_ref(),
                                        drain: &durable_drain,
                                        phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
                                    }),
                                ) {
                                Ok(()) => {}
                                Err(error @ BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention {
                                        context:
                                            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                    },
                                )) => return Err(error),
                                Err(BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention { .. },
                                )) => {
                                    super::sleep_after_metadata_contention_retry_for(
                                        "bucket_delete_begin",
                                        Some(pg_id),
                                        "bucket delete exact bucket object drain after reservation wait contention",
                                        &mut metadata_contention_retries,
                                    );
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_done",
                                format!(
                                    "iteration={} command_kind=none pass=after_reservation_wait",
                                    loop_iteration
                                ),
                            );
                        }
                        let terminal_post_reservation_next_object_pg_id =
                            self.terminal_bucket_delete_post_reservation_next_object_pg_id();
                        attempt_phase = BucketDeleteAttemptPhase::PostReservationStreamCleanup;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_visibility_stream_cleanup_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        durable_drain =
                            self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_visibility_stream_cleanup_done",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "stream_cleanup_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        if !can_resume_at_post_reservation_stream_cleanup {
                            self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptOutcomeKind::Retryable,
                                BucketDeleteAttemptPhase::PostReservationStreamCleanup,
                                "post-reservation stream cleanup started".to_string(),
                            );
                            can_resume_at_post_reservation_stream_cleanup = true;
                        }
                        let (aborted_stream_uploads, any_aborted_stream_uploads) = match self
                            .cleanup_abandoned_put_object_stream_uploads_for_bucket(
                                bucket,
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptPhase::PostReservationStreamCleanup,
                            )? {
                            PutObjectStreamUploadCleanup::Live(source) => {
                                self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                    node_store.bucket_write_reservation_client().as_ref(),
                                    &durable_drain,
                                    BucketDeleteAttemptOutcomeKind::NotEmpty,
                                    BucketDeleteAttemptPhase::PostReservationStreamCleanup,
                                    format!(
                                        "live stream blocker during cleanup before visibility check: {source:?}"
                                    ),
                                );
                                return Err(
                                    self.bucket_delete_not_empty_error(bucket, pg_id, source)
                                );
                            }
                            PutObjectStreamUploadCleanup::Aborted { count, any_aborted } => {
                                (count, any_aborted)
                            }
                        };
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "stream_cleanup_done",
                            format!(
                                "iteration={} pass=before_visibility_check aborted_stream_uploads={}",
                                loop_iteration, aborted_stream_uploads
                            ),
                        );
                        if any_aborted_stream_uploads {
                            let progress = BucketDeleteExactDrainProgress {
                                client: node_store.bucket_write_reservation_client().as_ref(),
                                drain: &durable_drain,
                                phase: BucketDeleteAttemptPhase::PostReservationStreamCleanup,
                            };
                            if self
                                .bucket_delete_post_reservation_next_object_pg_id(progress)?
                                .unwrap_or(0)
                                >= terminal_post_reservation_next_object_pg_id
                            {
                                self.record_bucket_delete_post_reservation_next_object_pg_id(
                                    progress, 0,
                                )?;
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_start",
                                format!(
                                    "iteration={} command_kind=none pass=after_abandoned_stream_abort",
                                    loop_iteration
                                ),
                            );
                            match self
                                .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                    bucket,
                                    Some(started),
                                    &mut work_budget,
                                    Some(progress),
                                ) {
                                Ok(()) => {}
                                Err(error @ BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention {
                                        context:
                                            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                    },
                                )) => return Err(error),
                                Err(BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention { .. },
                                )) => {
                                    super::sleep_after_metadata_contention_retry_for(
                                        "bucket_delete_begin",
                                        Some(pg_id),
                                        "bucket delete exact bucket object drain after abandoned stream abort contention",
                                        &mut metadata_contention_retries,
                                    );
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_done",
                                format!(
                                    "iteration={} command_kind=none pass=after_abandoned_stream_abort",
                                    loop_iteration
                                ),
                            );
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "pending_command_recheck_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        let pending_before_visibility_check =
                            self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "pending_command_recheck_done",
                            format!(
                                "iteration={} pass=before_visibility_check has_pending={} command_kind={}",
                                loop_iteration,
                                pending_before_visibility_check.is_some(),
                                pending_before_visibility_check
                                    .as_ref()
                                    .map_or("none", |command| command.payload().kind_name())
                            ),
                        );
                        if pending_before_visibility_check.is_some() {
                            super::sleep_after_metadata_contention_retry_for(
                                "bucket_delete_begin",
                                Some(pg_id),
                                "bucket delete pending command remained before visibility check",
                                &mut metadata_contention_retries,
                            );
                            continue;
                        }
                    }
                    attempt_phase = BucketDeleteAttemptPhase::FinalVisibilityCheck;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_before_visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_before_visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                        node_store.bucket_write_reservation_client().as_ref(),
                        &durable_drain,
                        BucketDeleteAttemptOutcomeKind::Retryable,
                        BucketDeleteAttemptPhase::FinalVisibilityCheck,
                        "final visibility check started".to_string(),
                    );
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_before_bucket_delete_final_visibility_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    )
                    .map_err(BucketWriteDrainError::from)?;
                    if let Some(source) = self.bucket_visible_data_source(
                        bucket,
                        true,
                        node_store.bucket_write_reservation_client().as_ref(),
                        &durable_drain,
                    )? {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::NotEmpty,
                            BucketDeleteAttemptPhase::FinalVisibilityCheck,
                            format!("visible data blocker: {source:?}"),
                        );
                        return Err(self.bucket_delete_not_empty_error(bucket, pg_id, source));
                    }
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_after_visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_after_visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    attempt_phase = BucketDeleteAttemptPhase::FinalVisibilityProven;
                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                        node_store.bucket_write_reservation_client().as_ref(),
                        &durable_drain,
                        BucketDeleteAttemptOutcomeKind::Retryable,
                        BucketDeleteAttemptPhase::FinalVisibilityProven,
                        "final visibility check proven".to_string(),
                    );
                    can_resume_at_mark_deleting = true;
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_after_bucket_delete_final_visibility_proven_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    )
                    .map_err(BucketWriteDrainError::from)?;
                }
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "heartbeat_before_build_mark_start",
                    format!("iteration={loop_iteration}"),
                );
                attempt_phase = BucketDeleteAttemptPhase::MarkDeleting;
                durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "heartbeat_before_build_mark_done",
                    format!("iteration={loop_iteration}"),
                );
                #[cfg(test)]
                maybe_run_before_bucket_delete_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "next_command_id_start",
                    format!("iteration={loop_iteration}"),
                );
                let command_id = match self.next_metadata_command_id(pg_id) {
                    Ok(command_id) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "next_command_id_done",
                            format!("iteration={loop_iteration} command_id={command_id:?}"),
                        );
                        command_id
                    }
                    Err(StoreError::MetadataCommandLogConflict { .. }) => {
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete next command id log conflict",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                    Err(error) => return Err(BucketWriteDrainError::from(error)),
                };
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "build_mark_deleting_start",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                let command = match metadata_route
                    .build_mark_bucket_deleting_command(command_id)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    MarkBucketDeletingCommandBuild::AlreadyDeleting => return Ok(()),
                    MarkBucketDeletingCommandBuild::Command(command) => *command,
                };
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "build_mark_deleting_done",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "pending_install_start",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                require_valid_route()?;
                match self
                    .install_snapshot_sensitive_bucket_pg_command_or_drain(
                        publisher,
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete pending install conflict",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                }
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "pending_install_done",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                (command, true)
            };
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "heartbeat_before_apply_start",
                format!(
                    "iteration={} command_kind={}",
                    loop_iteration,
                    command.payload().kind_name()
                ),
            );
            durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "heartbeat_before_apply_done",
                format!(
                    "iteration={} command_kind={}",
                    loop_iteration,
                    command.payload().kind_name()
                ),
            );
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "apply_mark_deleting_start",
                format!(
                    "iteration={} command_kind={} clear_pending_on_zero_apply={}",
                    loop_iteration,
                    command.payload().kind_name(),
                    clear_pending_on_zero_apply
                ),
            );
            let mut mark_apply_budget = super::RequestWorkBudget::new(
                std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
                None,
            )
            .for_operation("bucket_delete_mark_deleting_apply")
            .for_pg(pg_id);
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut mark_apply_budget,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "apply_mark_deleting_done",
                format!("iteration={} outcome={outcome:?}", loop_iteration),
            );
            match outcome {
                FinishPendingMetadataCommandResult::Applied => {}
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                        conflicting_pending_metadata_command(
                            "retryable partial pending mark bucket deleting command",
                        ),
                    ));
                }
                FinishPendingMetadataCommandResult::Abandoned => {
                    super::sleep_after_metadata_contention_retry_for(
                        "bucket_delete_begin",
                        Some(pg_id),
                        "bucket delete mark deleting abandoned",
                        &mut metadata_contention_retries,
                    );
                    continue;
                }
            }

            return Ok(());
        })();

        match result {
            Ok(()) => {
                self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                    node_store.bucket_write_reservation_client().as_ref(),
                    &durable_drain,
                    BucketDeleteAttemptOutcomeKind::MarkDeleting,
                    BucketDeleteAttemptPhase::MarkDeleting,
                    format!("mark bucket deleting applied after {loop_iteration} iteration(s)"),
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros()
                    ),
                );
                Ok(())
            }
            Err(error) => {
                let reservation_wait_blocker_already_recorded = matches!(
                    error,
                    BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                        context: BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    })
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_failed",
                    format!(
                        "bucket={:?} pg_id={} phase={:?} elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        attempt_phase,
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                if Self::bucket_delete_begin_error_should_rollback_drain(&error) {
                    self.rollback_durable_bucket_delete_drain(&durable_drain)?;
                } else if matches!(
                    error,
                    BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
                ) {
                    match metadata_route.head_bucket_raw() {
                        Ok(current)
                            if current.state == BucketState::Deleting
                                && current.bucket_incarnation_generation
                                    == current_bucket_incarnation_generation =>
                        {
                            match self.pending_metadata_command_for_bucket(pg_id, bucket) {
                                Ok(None) => {
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_retryable_error_observed_deleting",
                                        format!(
                                            "bucket={:?} pg_id={} phase={:?} elapsed_us={} error={:?}",
                                            bucket,
                                            pg_id.get(),
                                            attempt_phase,
                                            started.elapsed().as_micros(),
                                            error
                                        ),
                                    );
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_done",
                                        format!(
                                            "bucket={:?} pg_id={} elapsed_us={}",
                                            bucket,
                                            pg_id.get(),
                                            started.elapsed().as_micros()
                                        ),
                                    );
                                    return Ok(());
                                }
                                Ok(Some(_)) => {}
                                Err(pending_recheck_error) => {
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_retryable_error_pending_recheck_failed",
                                        format!(
                                            "bucket={:?} pg_id={} phase={:?} elapsed_us={} original_error={:?} pending_recheck_error={:?}",
                                            bucket,
                                            pg_id.get(),
                                            attempt_phase,
                                            started.elapsed().as_micros(),
                                            error,
                                            pending_recheck_error
                                        ),
                                    );
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(recheck_error) => {
                            let _ = observability::emit_flight_event(
                                super::TRACE_TARGET,
                                "bucket_delete_begin_retryable_error_deleting_recheck_failed",
                                format!(
                                    "bucket={:?} pg_id={} phase={:?} elapsed_us={} original_error={:?} recheck_error={:?}",
                                    bucket,
                                    pg_id.get(),
                                    attempt_phase,
                                    started.elapsed().as_micros(),
                                    error,
                                    recheck_error
                                ),
                            );
                        }
                    }
                    if !reservation_wait_blocker_already_recorded {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::Retryable,
                            attempt_phase,
                            format!("retryable begin error: {error:?}"),
                        );
                    }
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_preserved_retryable_attempt",
                        Some(format_args!(
                            "bucket={:?} pg_id={} drain_id={} error={:?}",
                            bucket, pg_id, durable_drain.record.drain_id, error
                        )),
                    );
                    self.enqueue_bucket_delete_begin(
                        bucket,
                        current_bucket_execution_generation,
                        current_bucket_incarnation_generation,
                    );
                } else {
                    if !reservation_wait_blocker_already_recorded {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::Retryable,
                            attempt_phase,
                            format!("retryable begin error: {error:?}"),
                        );
                    }
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_preserved_retryable_attempt",
                        Some(format_args!(
                            "bucket={:?} pg_id={} drain_id={} error={:?}",
                            bucket, pg_id, durable_drain.record.drain_id, error
                        )),
                    );
                    self.enqueue_bucket_delete_begin(
                        bucket,
                        current_bucket_execution_generation,
                        current_bucket_incarnation_generation,
                    );
                }
                Err(error)
            }
        }
    }

    fn bucket_delete_begin_error_should_rollback_drain(error: &BucketWriteDrainError) -> bool {
        match error {
            BucketWriteDrainError::Store(
                StoreError::MetadataCommandContention { .. }
                | StoreError::PgNotActive { .. }
                | StoreError::RouteMapExpired { .. }
                | StoreError::StaleMetadataOperation { .. }
                | StoreError::StaleMetadataRoute { .. }
                | StoreError::StaleMetadataReadProof { .. },
            ) => false,
            BucketWriteDrainError::Store(StoreError::StorageRpc { failure: code, .. })
                if super::storage_rpc_code_is_retryable_pg_route_error(*code) =>
            {
                false
            }
            BucketWriteDrainError::Store(StoreError::Io { source, .. })
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                ) =>
            {
                false
            }
            _ => true,
        }
    }

    fn record_bucket_delete_stream_cleanup_progress(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        phase: BucketDeleteAttemptPhase,
        next_object_pg_id: u32,
        next_session_id_marker: Option<SessionId>,
        aborted_uploads: bool,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.validated_bucket_metadata_pg(PgId::new(drain.pg_id));
        let existing = self.bucket_delete_matching_attempt_outcome(client, pg_id, drain)?;
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: drain.record.bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: BucketDeleteAttemptOutcomeKind::Retryable,
            phase,
            detail: Self::bounded_bucket_delete_attempt_detail(format!(
                "{phase:?} progressed to object PG {next_object_pg_id}"
            )),
            post_reservation_next_object_pg_id: existing
                .as_ref()
                .and_then(|record| record.post_reservation_next_object_pg_id),
            stream_cleanup_next_object_pg_id: Some(next_object_pg_id),
            stream_cleanup_next_session_id_marker: next_session_id_marker,
            stream_cleanup_aborted_uploads: aborted_uploads
                || existing.as_ref().is_some_and(|record| {
                    record.phase == phase && record.stream_cleanup_aborted_uploads
                }),
            final_visibility_next_object_pg_id: None,
            finalizer_next_object_pg_id: existing
                .as_ref()
                .and_then(|record| record.finalizer_next_object_pg_id),
            updated_at: crate::clock::current_time_millis(),
        };
        client
            .open_bucket_write_reservation_route(self.operation_epoch(), pg_id, &record.bucket)
            .and_then(|route| route.record_bucket_delete_attempt_outcome(&record))
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_after_bucket_delete_stream_cleanup_progress_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            phase,
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        Ok(())
    }

    fn cleanup_abandoned_put_object_stream_uploads_for_bucket(
        &self,
        bucket: &BucketName,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        phase: BucketDeleteAttemptPhase,
    ) -> Result<PutObjectStreamUploadCleanup, BucketWriteDrainError> {
        const STREAM_UPLOAD_DELETE_PAGE_LIMIT: u32 = 128;

        let existing = self.bucket_delete_matching_attempt_outcome(
            client,
            self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
            drain,
        )?;
        let matching_phase = existing.as_ref().filter(|record| record.phase == phase);
        let next_object_pg_id = matching_phase
            .and_then(|record| record.stream_cleanup_next_object_pg_id)
            .unwrap_or(0);
        let initial_marker =
            matching_phase.and_then(|record| record.stream_cleanup_next_session_id_marker.clone());
        let mut any_aborted =
            matching_phase.is_some_and(|record| record.stream_cleanup_aborted_uploads);
        let mut aborted_count = 0usize;
        let pg_ids = self.metadata_pg_ids();
        for raw_pg_id in pg_ids
            .iter()
            .copied()
            .filter(|raw_pg_id| *raw_pg_id >= next_object_pg_id)
        {
            let pg_id = PgId::new(raw_pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let mut marker = (raw_pg_id == next_object_pg_id)
                .then(|| initial_marker.clone())
                .flatten();
            loop {
                self.require_route_map_valid_now()?;
                let node = self
                    .local_map
                    .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
                let scan_route = node
                    .object_mutation_metadata_client()
                    .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                let page = scan_route
                    .list_stream_uploads_for_bucket_page(
                        bucket,
                        marker.as_ref(),
                        STREAM_UPLOAD_DELETE_PAGE_LIMIT,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                if page.uploads.is_empty() {
                    let next_pg_id = pg_ids
                        .iter()
                        .copied()
                        .find(|candidate| *candidate > raw_pg_id)
                        .unwrap_or_else(|| raw_pg_id.saturating_add(1));
                    self.record_bucket_delete_stream_cleanup_progress(
                        client,
                        drain,
                        phase,
                        next_pg_id,
                        None,
                        any_aborted,
                    )?;
                    break;
                }

                let mut aborted_any = false;
                for upload in page.uploads {
                    if self.object_metadata_pg_id(&upload.bucket, &upload.key) != raw_pg_id {
                        return Err(BucketWriteDrainError::Store(StoreError::Io {
                            context: "bucket delete stream upload cleanup PG validation",
                            source: std::io::Error::other(format!(
                                "stream upload session {:?} for bucket {:?} key {:?} is stored on PG {}",
                                upload.session_id,
                                upload.bucket,
                                upload.key,
                                raw_pg_id
                            )),
                        }));
                    }
                    if upload.target != crate::StreamUploadTarget::PutObject {
                        continue;
                    }
                    if self.stream_upload_has_live_bucket_write_reservation(&upload)? {
                        return Ok(PutObjectStreamUploadCleanup::Live(
                            BucketVisibleDataSource::StreamUpload { pg_id },
                        ));
                    }
                    match self.abort_stream_upload_session(
                        &upload.bucket,
                        &upload.key,
                        &upload.session_id,
                    ) {
                        Ok(()) => {}
                        Err(ObjectPgActionError::Metadata(
                            MetadataError::StreamSessionNotFound { .. },
                        )) => {}
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                super::object_pg_action_error_to_bucket_snapshot_error(error),
                            ));
                        }
                    }
                    self.release_object_generation_reservation(
                        &upload.bucket,
                        &upload.key,
                        &upload.session_id,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                    aborted_any = true;
                    any_aborted = true;
                    aborted_count += 1;
                }

                if aborted_any {
                    self.record_bucket_delete_stream_cleanup_progress(
                        client, drain, phase, raw_pg_id, None, true,
                    )?;
                    marker = None;
                    continue;
                }

                if let Some(next_marker) = page.next_session_id_marker {
                    self.record_bucket_delete_stream_cleanup_progress(
                        client,
                        drain,
                        phase,
                        raw_pg_id,
                        Some(next_marker.clone()),
                        any_aborted,
                    )?;
                    marker = Some(next_marker);
                } else {
                    let next_pg_id = pg_ids
                        .iter()
                        .copied()
                        .find(|candidate| *candidate > raw_pg_id)
                        .unwrap_or_else(|| raw_pg_id.saturating_add(1));
                    self.record_bucket_delete_stream_cleanup_progress(
                        client,
                        drain,
                        phase,
                        next_pg_id,
                        None,
                        any_aborted,
                    )?;
                    break;
                }
            }
        }

        Ok(PutObjectStreamUploadCleanup::Aborted {
            count: aborted_count,
            any_aborted,
        })
    }

    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainFailure> {
        self.try_finalize_bucket_delete_internal(bucket)
            .map_err(BucketWriteDrainFailure::from)
    }

    pub(crate) fn try_finalize_bucket_delete_internal(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(bucket_pg_id))?;
        let metadata_route = bucket_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.bucket_metadata_pg(bucket),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let info = match metadata_route.head_bucket_raw() {
            Ok(info) => info,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return if self.bucket_name_absent_on_acting_set(PgId::new(bucket_pg_id), bucket)? {
                    Ok(BucketDeleteFinalizeOutcome::NotFound)
                } else {
                    Ok(BucketDeleteFinalizeOutcome::Pending)
                };
            }
            Err(other) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(other)),
        };
        self.try_finalize_bucket_delete_root(&BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: info.bucket_incarnation_generation,
        })
    }

    pub(crate) fn try_finalize_bucket_delete_root(
        &self,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket = &root.bucket;
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(bucket_pg_id))?;
        let metadata_route = bucket_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.bucket_metadata_pg(bucket),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_start",
            Some(format_args!(
                "bucket={:?} bucket_pg_id={}",
                bucket,
                self.bucket_metadata_pg_id(bucket)
            )),
        );
        let bucket_incarnation_generation = {
            let info = match metadata_route.head_bucket_raw() {
                Ok(info) => info,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                    ..
                })) => {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_not_found",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
                    );
                    if self.finalized_bucket_deleted_on_acting_set(PgId::new(bucket_pg_id), root)? {
                        self.finish_bucket_delete_finalize_work(root);
                        return Ok(BucketDeleteFinalizeOutcome::NotFound);
                    }
                    return Ok(BucketDeleteFinalizeOutcome::Pending);
                }
                Err(other) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(other)),
            };
            if info.bucket_incarnation_generation != root.bucket_incarnation_generation {
                self.finish_bucket_delete_finalize_work(root);
                return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
            }
            if info.state != BucketState::Deleting {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_not_deleting",
                    Some(format_args!(
                        "bucket={:?} pg_id={} state={:?}",
                        bucket, bucket_pg_id, info.state
                    )),
                );
                self.finish_bucket_delete_finalize_work(root);
                return Ok(BucketDeleteFinalizeOutcome::NotDeleting);
            }
            info.bucket_incarnation_generation
        };

        let claim_id = self.next_bucket_delete_finalize_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let claimed_at = crate::clock::current_time_millis();
        let bucket_write_reservation_client =
            Arc::clone(bucket_store.bucket_write_reservation_client());
        let retained_bucket_write_reservation_client =
            Arc::clone(bucket_store.retained_bucket_write_reservation_client());
        let claim = self
            .open_bucket_write_reservation_route(
                bucket_write_reservation_client.as_ref(),
                self.validated_bucket_metadata_pg(PgId::new(bucket_pg_id)),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .acquire_bucket_delete_finalize_claim(
                bucket_incarnation_generation,
                &claim_id,
                &owner_token,
                claimed_at,
                claimed_at.checked_add(60_000),
                claimed_at,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let Some(claim) = claim else {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_finalize_claim_busy",
                Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
            );
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        };
        crate::node::maybe_run_after_bucket_delete_finalize_claim_hook(bucket);

        let retained_bucket_write_reservation_route = retained_bucket_write_reservation_client
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(bucket_pg_id)),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;

        let release_finalizer_claim = || -> Result<(), BucketWriteDrainError> {
            retained_bucket_write_reservation_route
                .release_bucket_delete_finalize_claim(&claim)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            Ok(())
        };
        let stale_finalizer_claim_release = |error: &BucketWriteDrainError| {
            matches!(
                error,
                BucketWriteDrainError::Metadata(
                    MetadataError::ReclaimClaimNotFound { .. }
                        | MetadataError::ReclaimClaimConflict { .. }
                )
            )
        };

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_FINALIZE_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_finalize")
        .for_pg(PgId::new(bucket_pg_id));
        let result = self.try_finalize_bucket_delete_claimed(
            bucket,
            bucket_pg_id,
            bucket_incarnation_generation,
            &mut work_budget,
        );
        match result {
            Ok(
                outcome @ (BucketDeleteFinalizeOutcome::Finalized
                | BucketDeleteFinalizeOutcome::NotFound),
            ) => match release_finalizer_claim() {
                Ok(()) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::ReclaimClaimNotFound {
                    ..
                })) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::ReclaimClaimConflict {
                    ..
                })) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(error) => Err(error),
            },
            Ok(outcome) => {
                if let Err(error) = release_finalizer_claim() {
                    if !stale_finalizer_claim_release(&error) {
                        return Err(error);
                    }
                }
                Ok(outcome)
            }
            Err(error) => {
                if let Err(release_error) = release_finalizer_claim() {
                    if !stale_finalizer_claim_release(&release_error) {
                        return Err(release_error);
                    }
                }
                Err(error)
            }
        }
    }

    fn try_finalize_bucket_delete_claimed(
        &self,
        bucket: &BucketName,
        bucket_pg_id: u32,
        bucket_incarnation_generation: u64,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        self.require_route_map_valid_now()?;
        work_budget.check("bucket delete finalize work budget exhausted")?;
        let bucket_pg_id = PgId::new(bucket_pg_id);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), bucket_pg_id)?;
        let progress_client = bucket_store.bucket_write_reservation_client();
        let progress_route = self
            .open_bucket_write_reservation_route(
                progress_client.as_ref(),
                self.validated_bucket_metadata_pg(bucket_pg_id),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let metadata_route = bucket_store
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(bucket_pg_id),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let bucket_info = metadata_route
            .head_bucket_raw()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if bucket_info.bucket_incarnation_generation != bucket_incarnation_generation
            || bucket_info.state != BucketState::Deleting
        {
            return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
        }
        let existing_progress = progress_route
            .bucket_delete_attempt_outcome()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let trusted_progress = existing_progress.as_ref().filter(|record| {
            record.bucket_execution_generation == bucket_info.bucket_execution_generation
                && record.outcome == BucketDeleteAttemptOutcomeKind::MarkDeleting
                && record.phase == BucketDeleteAttemptPhase::MarkDeleting
        });
        let pg_ids = self.metadata_pg_ids();
        let next_pg_id = trusted_progress
            .and_then(|record| record.finalizer_next_object_pg_id)
            .unwrap_or_else(|| pg_ids.first().copied().unwrap_or(0));
        let window = bounded_pg_scan_window(
            &pg_ids,
            Some(next_pg_id),
            BUCKET_DELETE_FINALIZE_SCAN_PG_BATCH,
        );

        for &raw_pg_id in &pg_ids[window.start..window.end] {
            self.require_route_map_valid_now()?;
            work_budget.check("bucket delete finalize work budget exhausted")?;
            let pg_id = PgId::new(raw_pg_id);
            if let Some(source) = self.bucket_visible_data_source_for_pg(bucket, pg_id, false)? {
                self.record_bucket_delete_finalizer_next_object_pg_id(
                    progress_client.as_ref(),
                    bucket_pg_id,
                    &bucket_info,
                    existing_progress.as_ref(),
                    raw_pg_id,
                )?;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_pending_visible_data",
                    Some(format_args!(
                        "bucket={:?} pg_id={} source={} source_pg_id={}",
                        bucket,
                        bucket_pg_id.get(),
                        source.label(),
                        source.pg_id().get()
                    )),
                );
                if bucket_delete_visible_data_diagnostics_enabled() {
                    eprintln!(
                        "bucket delete finalize found visible data source={} source_pg_id={}",
                        source.label(),
                        source.pg_id().get()
                    );
                }
                return Ok(BucketDeleteFinalizeOutcome::Pending);
            }

            loop {
                work_budget.check("bucket delete finalize work budget exhausted")?;
                let Some(root) = self.bucket_payload_reclaim_root_for_pg(bucket, pg_id)? else {
                    break;
                };
                let lease_count = self.local_map.object_payload_lease_count(
                    &root.bucket,
                    &root.key,
                    root.generation_id,
                )?;
                if lease_count == 0
                    && self
                        .reclaim_object_payload_if_unleased(
                            &root.bucket,
                            &root.key,
                            root.generation_id,
                        )
                        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                if lease_count == 0 {
                    self.enqueue_object_payload_reclaim(
                        &root.bucket,
                        &root.key,
                        root.generation_id,
                    );
                }
                self.record_bucket_delete_finalizer_next_object_pg_id(
                    progress_client.as_ref(),
                    bucket_pg_id,
                    &bucket_info,
                    existing_progress.as_ref(),
                    raw_pg_id,
                )?;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_pending_reclaim",
                    Some(format_args!(
                        "bucket={:?} bucket_pg_id={} object_pg_id={}",
                        bucket,
                        bucket_pg_id.get(),
                        raw_pg_id
                    )),
                );
                return Ok(BucketDeleteFinalizeOutcome::Pending);
            }
        }

        let next_pg_id = window.next_pg_id.unwrap_or_else(|| {
            pg_ids
                .last()
                .copied()
                .map_or(0, |pg_id| pg_id.saturating_add(1))
        });
        self.record_bucket_delete_finalizer_next_object_pg_id(
            progress_client.as_ref(),
            bucket_pg_id,
            &bucket_info,
            existing_progress.as_ref(),
            next_pg_id,
        )?;
        if window.next_pg_id.is_some() {
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        self.delete_bucket_from_acting_set(
            bucket_pg_id,
            &BucketDeleteFinalizeRoot {
                bucket: bucket.clone(),
                bucket_incarnation_generation,
            },
        )
    }

    fn record_bucket_delete_finalizer_next_object_pg_id(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        bucket_pg_id: PgId,
        bucket_info: &BucketInfo,
        existing: Option<&BucketDeleteAttemptOutcomeRecord>,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let matching = existing.filter(|record| {
            record.bucket_execution_generation == bucket_info.bucket_execution_generation
                && record.outcome == BucketDeleteAttemptOutcomeKind::MarkDeleting
                && record.phase == BucketDeleteAttemptPhase::MarkDeleting
        });
        if matching.and_then(|record| record.finalizer_next_object_pg_id) == Some(next_object_pg_id)
        {
            return Ok(());
        }
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: bucket_info.name.clone(),
            drain_id: matching.map_or_else(
                || {
                    format!(
                        "finalizer-{}-{}",
                        bucket_info.bucket_execution_generation,
                        bucket_info.bucket_incarnation_generation
                    )
                },
                |record| record.drain_id.clone(),
            ),
            cluster_epoch: matching.map_or(self.operation_epoch(), |record| record.cluster_epoch),
            bucket_execution_generation: bucket_info.bucket_execution_generation,
            outcome: BucketDeleteAttemptOutcomeKind::MarkDeleting,
            phase: BucketDeleteAttemptPhase::MarkDeleting,
            detail: format!("bucket finalizer advanced to object PG {next_object_pg_id}"),
            post_reservation_next_object_pg_id: matching
                .and_then(|record| record.post_reservation_next_object_pg_id),
            stream_cleanup_next_object_pg_id: matching
                .and_then(|record| record.stream_cleanup_next_object_pg_id),
            stream_cleanup_next_session_id_marker: matching
                .and_then(|record| record.stream_cleanup_next_session_id_marker.clone()),
            stream_cleanup_aborted_uploads: matching
                .is_some_and(|record| record.stream_cleanup_aborted_uploads),
            final_visibility_next_object_pg_id: matching
                .and_then(|record| record.final_visibility_next_object_pg_id),
            finalizer_next_object_pg_id: Some(next_object_pg_id),
            updated_at: crate::clock::current_time_millis(),
        };
        client
            .open_bucket_write_reservation_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(bucket_pg_id),
                &record.bucket,
            )
            .and_then(|route| route.record_bucket_delete_attempt_outcome(&record))
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
    }

    fn record_bucket_delete_final_visibility_progress(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.validated_bucket_metadata_pg(PgId::new(drain.pg_id));
        let existing = self.bucket_delete_matching_attempt_outcome(client, pg_id, drain)?;
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: drain.record.bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: BucketDeleteAttemptOutcomeKind::Retryable,
            phase: BucketDeleteAttemptPhase::FinalVisibilityCheck,
            detail: Self::bounded_bucket_delete_attempt_detail(format!(
                "final visibility progressed to object PG {next_object_pg_id}"
            )),
            post_reservation_next_object_pg_id: existing
                .as_ref()
                .and_then(|record| record.post_reservation_next_object_pg_id),
            stream_cleanup_next_object_pg_id: existing
                .as_ref()
                .and_then(|record| record.stream_cleanup_next_object_pg_id),
            stream_cleanup_next_session_id_marker: existing
                .as_ref()
                .and_then(|record| record.stream_cleanup_next_session_id_marker.clone()),
            stream_cleanup_aborted_uploads: existing
                .as_ref()
                .is_some_and(|record| record.stream_cleanup_aborted_uploads),
            final_visibility_next_object_pg_id: Some(next_object_pg_id),
            finalizer_next_object_pg_id: existing
                .as_ref()
                .and_then(|record| record.finalizer_next_object_pg_id),
            updated_at: crate::clock::current_time_millis(),
        };
        client
            .open_bucket_write_reservation_route(self.operation_epoch(), pg_id, &record.bucket)
            .and_then(|route| route.record_bucket_delete_attempt_outcome(&record))
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_after_bucket_delete_final_visibility_progress_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        Ok(())
    }

    fn bucket_visible_data_source(
        &self,
        bucket: &BucketName,
        include_stream_uploads: bool,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<Option<BucketVisibleDataSource>, BucketWriteDrainError> {
        let existing = self.bucket_delete_matching_attempt_outcome(
            client,
            self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
            drain,
        )?;
        let next_object_pg_id = existing
            .as_ref()
            .filter(|record| record.phase == BucketDeleteAttemptPhase::FinalVisibilityCheck)
            .and_then(|record| record.final_visibility_next_object_pg_id)
            .unwrap_or(0);
        let pg_ids = self.metadata_pg_ids();
        for raw_pg_id in pg_ids
            .iter()
            .copied()
            .filter(|raw_pg_id| *raw_pg_id >= next_object_pg_id)
        {
            self.require_route_map_valid_now()?;
            if let Some(source) = self.bucket_visible_data_source_for_pg(
                bucket,
                PgId::new(raw_pg_id),
                include_stream_uploads,
            )? {
                return Ok(Some(source));
            }
            let next_pg_id = pg_ids
                .iter()
                .copied()
                .find(|candidate| *candidate > raw_pg_id)
                .unwrap_or_else(|| raw_pg_id.saturating_add(1));
            self.record_bucket_delete_final_visibility_progress(client, drain, next_pg_id)?;
        }
        Ok(None)
    }

    fn bucket_visible_data_source_for_pg(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
        include_stream_uploads: bool,
    ) -> Result<Option<BucketVisibleDataSource>, BucketWriteDrainError> {
        let listing_route = self
            .metadata_pg_primary_object_listing_route(pg_id)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let versions = listing_route
            .list_object_versions_page(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                start_at: None,
                max_keys: 1,
            })
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if !versions.versions.is_empty() {
            return Ok(Some(BucketVisibleDataSource::ObjectVersion { pg_id }));
        }

        let uploads = listing_route
            .list_multipart_uploads_page(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                page_start: None,
                max_uploads: 1,
            })
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if !uploads.uploads.is_empty() {
            return Ok(Some(BucketVisibleDataSource::MultipartUpload { pg_id }));
        }

        if include_stream_uploads {
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            let scan_route = node
                .object_mutation_metadata_client()
                .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            let page = scan_route
                .list_stream_uploads_for_bucket_page(bucket, None, 1)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            if !page.uploads.is_empty() {
                return Ok(Some(BucketVisibleDataSource::StreamUpload { pg_id }));
            }
        }
        Ok(None)
    }

    fn bucket_payload_reclaim_root_for_pg(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketWriteDrainError> {
        let scan_pg_id = self.object_metadata_scan_pg(pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let scan_route = node
            .object_mutation_metadata_client()
            .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let root = scan_route
            .get_bucket_payload_reclaim_root(bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if let Some(root) = root.as_ref() {
            self.validate_bucket_payload_reclaim_root_for_pg(pg_id, root, node.node_id())?;
        }
        Ok(root)
    }

    pub(crate) fn validate_bucket_payload_reclaim_root_for_pg(
        &self,
        requested_pg_id: PgId,
        root: &PayloadReclaimRoot,
        node_id: NodeId,
    ) -> Result<(), BucketWriteDrainError> {
        if self.object_metadata_pg_id(&root.bucket, &root.key) != requested_pg_id.get() {
            return Err(BucketWriteDrainError::Store(StoreError::StorageRpc {
                node_id: node_id.as_u32(),
                operation: "object bucket payload reclaim root",
                failure: StorageRpcErrorCode::Internal,
                detail: crate::StorageNodeFailureDetail::new(
                    "payload reclaim root does not belong to requested object metadata PG",
                ),
            }));
        }
        Ok(())
    }

}
