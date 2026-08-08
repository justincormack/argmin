impl StorageCluster {
    pub fn place_payload_shards(
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

    pub fn place_payload_shards_for_pg_route(
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

    pub fn place_payload_shards_for_pg_route_snapshot(
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

    pub fn payload_shard_node(
        &self,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<NodeId, ClusterBuildError> {
        self.local_map.payload_shard_node(
            self.operation_epoch(),
            data_pg_id,
            shard_index,
            ec_shape,
            stable_placement_key,
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

    pub(crate) fn read_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.local_map
            .read_payload_shard(self.operation_epoch(), location, key, expected)
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
            &mut || Ok(()),
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
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let generation_id = reservation.generation_id;
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied => return Ok(generation_id),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation reservation command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned => {
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
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
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
                    Ok(()) => {
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
                        if error.applied_nodes == 0
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
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let reissued = match self.reissue_pending_metadata_command(pg_id, &command)
                        {
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
                        work_budget
                            .sleep_after_contention(
                                "object generation reservation reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
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
                        .finish_exact_pending_object_metadata_command(pg_id, exact)
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
                            if pending.as_ref() != Some(&command) {
                                return Err(conflicting_pending_object_metadata_command(
                                    "pending version reservation changed before stale cleanup",
                                ));
                            }
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            work_budget
                                .sleep_after_contention(
                                    "object version reservation stale cleanup retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    match outcome {
                        PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            return Err(conflicting_pending_object_metadata_command(
                                "retryable partial pending version reservation command",
                            ));
                        }
                        PendingMetadataCommandOutcome::Applied
                        | PendingMetadataCommandOutcome::Abandoned => {
                            // A version reservation has no caller identity. Even when it targets
                            // the same key, it may belong to a concurrent write, so converge it
                            // and allocate a fresh version instead of adopting its result.
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
            let command = match self.install_allocator_cleanup_metadata_command_with_fresh_id(
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
            )? {
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
            match self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command) {
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
        self.finish_object_pg_pending_slot(pg_id, command.command)
    }

    fn apply_exact_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<(), ObjectPgActionError> {
        match self.finish_exact_pending_object_metadata_command(pg_id, command)? {
            PendingMetadataCommandOutcome::Applied => Ok(()),
            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata command",
                ))
            }
            PendingMetadataCommandOutcome::Abandoned => {
                Err(conflicting_pending_object_metadata_command(
                    "abandoned pending object metadata command",
                ))
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
        let mut authority = MetadataCommandDrainAuthority::for_publisher(publisher, work_budget);
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            MetadataCommandRouteMode::Normal,
            None,
        )?;
        self.maybe_run_after_metadata_command_drain_hook();
        match outcome {
            PendingMetadataCommandOutcome::Applied | PendingMetadataCommandOutcome::Abandoned => {
                Ok(outcome)
            }
            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata drain",
                ))
            }
        }
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
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            MetadataCommandRouteMode::Normal,
            None,
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
        let mut drain_authority = MetadataCommandDrainAuthority::for_recovery(authority);
        let outcome = self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut drain_authority,
            self,
            MetadataCommandRouteMode::Normal,
            None,
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
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            self,
            MetadataCommandRouteMode::Recovery,
            None,
        )
    }

    fn drain_pending_metadata_command_with_authorized_recovery_route(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
            .for_operation("metadata_command_authorized_recovery")
            .for_pg(pg_id);
        let mut recovery_authority = MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
        let mut authority = MetadataCommandDrainAuthority::for_recovery(&mut recovery_authority);
        self.drain_pending_metadata_command_with_authority_inner(
            pg_id,
            command,
            &mut authority,
            reservation_authority,
            MetadataCommandRouteMode::Recovery,
            Some(command),
        )
    }

    fn drain_pending_metadata_command_with_authority_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        authority: &mut MetadataCommandDrainAuthority<'_>,
        reservation_authority: &StorageCluster,
        route_mode: MetadataCommandRouteMode,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        loop {
            authority
                .work_budget()
                .check("pending command recovery gate budget exhausted")?;
            let recovery = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery(pg_id, command);
            let recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::Waited { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, command, "drain_wait");
                    let waiter_outcome = self
                        .pending_command_recovery_waiter_outcome_with_route_mode(
                            pg_id, command, route_mode,
                        )?;
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        command,
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
                MetadataCommandRecoveryAdmission::TimedOut { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::TimedOut,
                        wait_us,
                    );
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        command,
                        "timed_out",
                    );
                    self.emit_pending_slot_action_for_command(pg_id, command, "drain_timeout");
                    authority
                        .work_budget()
                        .sleep_after_contention("pending command recovery retry budget exhausted")
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            self.emit_pending_slot_action_for_command(pg_id, command, "drain_attempt");
            let mut leader = authority
                .admit_leader(recovery_guard, pg_id, command)
                .map_err(ObjectPgActionError::Store)?;
            let outcome = self.finish_pending_metadata_command_with_recovery_leader(
                pg_id,
                command,
                &mut leader,
                reservation_authority,
                route_mode,
                recovery_authorized_source,
            )?;
            self.emit_metadata_command_recovery_outcome_for_command(
                pg_id,
                command,
                outcome.metric_label(),
            );
            return Ok(outcome);
        }
    }

    fn finish_pending_metadata_command_with_recovery_leader(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        leader: &mut MetadataCommandRecoveryLeader<'_>,
        reservation_authority: &StorageCluster,
        route_mode: MetadataCommandRouteMode,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = match route_mode {
                    MetadataCommandRouteMode::Normal => self
                        .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                            pg_id,
                            command,
                            false,
                            leader.work_budget(),
                        ),
                    MetadataCommandRouteMode::Recovery => {
                        let (work_budget, proof) = leader.parts();
                        self.finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
                            proof,
                            pg_id,
                            command,
                            false,
                            recovery_authorized_source,
                            work_budget,
                        )
                    }
                }
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
            Ok(match outcome {
                request_ops::FinishPendingMetadataCommandResult::Applied => {
                    PendingMetadataCommandOutcome::Applied
                }
                request_ops::FinishPendingMetadataCommandResult::Abandoned => {
                    PendingMetadataCommandOutcome::Abandoned
                }
                request_ops::FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    PendingMetadataCommandOutcome::RetryPartialExactConflict
                }
            })
        } else {
            let (work_budget, proof) = leader.parts();
            self.finish_object_pg_pending_slot_inner(
                pg_id,
                command,
                true,
                work_budget,
                reservation_authority,
                match route_mode {
                    MetadataCommandRouteMode::Normal => MetadataCommandExecutionRoute::normal(),
                    MetadataCommandRouteMode::Recovery => MetadataCommandExecutionRoute::recovery(
                        proof,
                        recovery_authorized_source,
                        None,
                    ),
                },
            )
        }
    }

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

    fn pending_command_recovery_waiter_outcome_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<MetadataCommandRecoveryWaiterOutcome, ObjectPgActionError> {
        let pending = self.pending_metadata_command_for_bucket_with_route_mode(
            pg_id,
            command.bucket_name(),
            route_mode,
            command.id().cluster_epoch(),
        )?;
        if pending.as_ref() == Some(command) {
            return Ok(MetadataCommandRecoveryWaiterOutcome::StillPending);
        }
        if self
            .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                pg_id, command, route_mode,
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
        matches!(outcome, PendingMetadataCommandOutcome::Applied)
            && !Self::metadata_command_is_bucket_pg_command(command)
    }

    fn finish_object_pg_pending_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut work_budget = RequestWorkBudget::new(
            Duration::from_millis(request_ops::METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("object_metadata_pending_command_apply")
        .for_pg(pg_id);
        self.finish_object_pg_pending_slot_inner(
            pg_id,
            command,
            false,
            &mut work_budget,
            self,
            MetadataCommandExecutionRoute::normal(),
        )
    }

    fn finish_object_pg_pending_slot_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        abandon_zero_apply_stale_reservation: bool,
        work_budget: &mut RequestWorkBudget,
        reservation_authority: &StorageCluster,
        mut execution_route: MetadataCommandExecutionRoute<'_>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        execution_route
            .require_command(pg_id, command)
            .map_err(ObjectPgActionError::Store)?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source.cloned();
        let recovery_abandoned_source = execution_route.recovery_abandoned_source.cloned();
        let mut command = command.clone();
        loop {
            work_budget.check("object metadata pending command apply budget exhausted")?;
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
            };
            if abandoned_on_acting_set
                .map_err(|error| bucket_snapshot_error_to_object_pg_action_error(error.source))?
            {
                let record_result = match route_mode {
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
                };
                record_result.map_err(|error| {
                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                })?;
                self.complete_abandoned_object_metadata_command(
                    pg_id,
                    &command,
                    reservation_authority,
                    execution_route,
                    work_budget,
                )?;
                return Ok(PendingMetadataCommandOutcome::Abandoned);
            }
            let apply_result = match route_mode {
                MetadataCommandRouteMode::Normal => self
                    .apply_metadata_command_to_acting_set_with_reservation_authority(
                        &command,
                        reservation_authority,
                    ),
                MetadataCommandRouteMode::Recovery => match recovery_authorized_source.as_ref() {
                    Some(authorized_source) => self
                        .apply_reissued_metadata_command_to_acting_set_for_recovery(
                            execution_route.recovery_proof(),
                            authorized_source,
                            recovery_abandoned_source.as_ref(),
                            &command,
                            reservation_authority,
                        ),
                    None => self.apply_metadata_command_to_acting_set_for_recovery(
                        execution_route.recovery_proof(),
                        &command,
                        reservation_authority,
                    ),
                },
            };
            match apply_result {
                Ok(()) => {
                    if reservation_authority
                        .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                            pg_id, &command,
                        )
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
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
                        }
                        .map_err(ObjectPgActionError::from)?;
                    }
                    self.after_object_metadata_command_applied(&command);
                    return Ok(PendingMetadataCommandOutcome::Applied);
                }
                Err(error)
                    if request_ops::metadata_command_apply_transport_error_is_retryable(
                        &error.source,
                    ) =>
                {
                    work_budget
                        .sleep_after_contention(
                            "pending metadata command transport retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error)
                    if Self::metadata_command_log_conflict_matches(&command, &error.source)
                        && ((error.applied_nodes == 0
                            && self
                                .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                                    pg_id, &command, route_mode,
                                )
                                .map_err(bucket_snapshot_error_to_object_pg_action_error)?)
                            || (error.applied_nodes > 0
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
                        if reservation_authority
                            .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                                pg_id, &command,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
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
                            }
                            .map_err(ObjectPgActionError::from)?;
                        }
                        self.after_object_metadata_command_applied(&command);
                        return Ok(PendingMetadataCommandOutcome::Applied);
                    }
                    return Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict);
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let reissued = match self.reissue_pending_metadata_command_with_route_mode(
                        pg_id,
                        &command,
                        execution_route,
                        command.payload(),
                    ) {
                        Ok(Some(reissued)) => reissued,
                        Ok(None) => return Ok(PendingMetadataCommandOutcome::Abandoned),
                        Err(BucketSnapshotLoadError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            return Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict);
                        }
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                        }
                    };
                    execution_route = execution_route
                        .for_reissued_command(pg_id, &command, &reissued)
                        .map_err(ObjectPgActionError::Store)?;
                    command = reissued;
                }
                Err(error)
                    if abandon_zero_apply_stale_reservation
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
                    .map_err(|error| {
                        bucket_snapshot_error_to_object_pg_action_error(error.source)
                    })?;
                    self.complete_abandoned_object_metadata_command(
                        pg_id,
                        &command,
                        reservation_authority,
                        execution_route,
                        work_budget,
                    )?;
                    return Ok(PendingMetadataCommandOutcome::Abandoned);
                }
                Err(error) => {
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
            }
        }
    }

    fn complete_abandoned_object_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
        execution_route: MetadataCommandExecutionRoute<'_>,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
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
                .reissue_pending_metadata_command_with_route_mode(
                    pg_id,
                    command,
                    MetadataCommandExecutionRoute::recovery(
                        execution_route.recovery_proof(),
                        Some(authorized_source),
                        Some(command),
                    ),
                    &follow_up_payload,
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
                false,
                work_budget,
                reservation_authority,
                follow_up_route,
            )? {
                PendingMetadataCommandOutcome::Applied => {
                    self.after_object_metadata_command_abandoned_payload_cleanup(command);
                    return Ok(());
                }
                PendingMetadataCommandOutcome::Abandoned
                | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    return Err(conflicting_pending_object_metadata_command(
                        "certified generation cleanup did not apply",
                    ));
                }
            }
        }

        let command_bucket = command.bucket_name();
        match execution_route.mode {
            MetadataCommandRouteMode::Normal => self
                .remove_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    command_bucket,
                    command,
                    work_budget,
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
        self.after_object_metadata_command_abandoned(command, reservation_authority)
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
                            PendingMetadataCommandOutcome::Applied => return Ok(()),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation release partial pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                            PendingMetadataCommandOutcome::Abandoned => {
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
                    Ok(()) => {
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
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command(pg_id, &command)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
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

    fn drain_pending_object_metadata_commands_for_publisher_collect(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<MetadataCommandEnvelope>, ObjectPgActionError> {
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
        }
        Ok(applied)
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
            let outcome = self.drain_pending_metadata_command_with_recovery_authority(
                &mut recovery_authority,
                pg_id,
                &command,
            )?;
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "apply_done",
                format!("iteration={} outcome={outcome:?}", drain_iteration),
            );
            match outcome {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::Abandoned => {}
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
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<StreamAppendCommandApplyOutcome, ObjectPgActionError> {
        let mut command = command.clone();
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command.bucket_name(),
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    return Ok(StreamAppendCommandApplyOutcome::Applied);
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
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command.bucket_name(),
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    return Ok(StreamAppendCommandApplyOutcome::Applied);
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
                        "retryable partial stream append command conflict",
                    ));
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        return Ok(StreamAppendCommandApplyOutcome::RetryFromFreshSnapshot);
                    };
                    command = reissued;
                }
                Err(error) => {
                    if error.applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        self.delete_stream_segment_payload_shard_keys_best_effort(
                            segment_record,
                            shard_batch.iter().map(|(key, _)| (*key).clone()),
                        );
                    }
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
            }
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

    pub(crate) fn release_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            ReleaseObjectGenerationReservation
        );
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
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
                            PendingMetadataCommandOutcome::Applied => return Ok(()),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation release command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned => {
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
                    Ok(()) => {
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
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command(pg_id, &command)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        work_budget
                            .sleep_after_contention(
                                "object generation release reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
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
        macro_rules! sleep_direct_put_before_command_ownership_after_contention {
            ($context:literal) => {{
                if let Err(error) = work_budget.sleep_after_contention($context) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
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

        let stale_commit_snapshot_deadline = Instant::now() + DIRECT_PUT_STALE_COMMIT_RETRY_BUDGET;
        let (command, new_pending_command) = loop {
            require_direct_put_route_before_command_ownership!();
            check_direct_put_work_before_command_ownership!(
                "direct PUT metadata retry budget exhausted"
            );
            let (command, new_pending_command, payload_acks_registered) = loop {
                require_direct_put_route_before_command_ownership!();
                check_direct_put_work_before_command_ownership!(
                    "direct PUT metadata pending retry budget exhausted"
                );
                let Some(command) =
                    (match self.pending_metadata_command_for_bucket(pg_id, &req.bucket) {
                        Ok(command) => command,
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error.into());
                        }
                    })
                else {
                    let snapshot = match direct_put_metadata_route.load_direct_put_commit_snapshot(
                        &req.generation_reservation_id,
                        req.generation_id,
                    ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error);
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
                    match action(snapshot.auth_snapshot.clone()) {
                        Ok(()) => {}
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Ok(Err(error));
                        }
                    }

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
                    self.maybe_run_before_direct_put_command_id_hook();
                    if let Err(error) = require_valid_route() {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(ObjectPgActionError::Store(error));
                    }
                    if let Err(error) =
                        self.register_payload_shard_acks(req.data_pg_id, &shard_batch)
                    {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    let command = match direct_put_metadata_route.build_direct_put_commit_command(
                        BuildDirectPutCommitCommandReq {
                            request: req,
                            version_id,
                            expected_snapshot: &snapshot,
                            bucket_write_reservation: &effective_bucket_write_reservation,
                        },
                    ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::StaleDirectPutCommitSnapshot)
                            if Instant::now() < stale_commit_snapshot_deadline =>
                        {
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT stale snapshot retry budget exhausted"
                            );
                            continue;
                        }
                        Err(ObjectPgActionError::StaleDirectPutCommitSnapshot) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(conflicting_pending_object_metadata_command(
                                "direct PUT stale commit snapshot retry budget exhausted",
                            ));
                        }
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            let pending_visible = match self
                                .pending_metadata_command_for_bucket(pg_id, &req.bucket)
                            {
                                Ok(pending) => pending.is_some(),
                                Err(error) => {
                                    cleanup_direct_put_attempt_before_command_ownership!();
                                    return Err(error.into());
                                }
                            };
                            let drain_result = self.drain_after_object_pg_log_conflict(
                                publisher,
                                pg_id,
                                &req.bucket,
                                pending_visible,
                            );
                            if let Err(error) = drain_result {
                                cleanup_direct_put_attempt_before_command_ownership!();
                                return Err(error);
                            }
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT command log conflict retry budget exhausted"
                            );
                            continue;
                        }
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error);
                        }
                    };
                    break (command, true, true);
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
                let has_abandoned_log = match self
                    .metadata_command_has_abandoned_log_on_acting_set(&command)
                {
                    Ok(has_abandoned_log) => has_abandoned_log,
                    Err(error) => {
                        let error = bucket_snapshot_error_to_object_pg_action_error(error.source);
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                };
                if has_abandoned_log {
                    if is_matching_direct_put {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        let error =
                            match self.finish_exact_pending_object_metadata_command(pg_id, exact) {
                                Ok(PendingMetadataCommandOutcome::Applied) => {
                                    unreachable!(
                                        "already-classified abandoned metadata command was applied"
                                    )
                                }
                                Ok(PendingMetadataCommandOutcome::Abandoned) => {
                                    cleanup_direct_put_attempt_before_command_ownership!();
                                    return Err(conflicting_pending_object_metadata_command(
                                        "abandoned pending command for direct put commit",
                                    ));
                                }
                                Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict) => {
                                    conflicting_pending_object_metadata_command(
                                        "retryable partial pending command for direct put commit",
                                    )
                                }
                                Err(error) => error,
                            };
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    if let Err(error) =
                        self.drain_pending_object_metadata_command(publisher, pg_id, &command)
                    {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    sleep_direct_put_before_command_ownership_after_contention!(
                        "direct PUT abandoned pending drain retry budget exhausted"
                    );
                    continue;
                }
                if is_matching_direct_put {
                    break (command, false, false);
                }
                if let Err(error) =
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)
                {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(error);
                }
                sleep_direct_put_before_command_ownership_after_contention!(
                    "direct PUT unrelated pending drain retry budget exhausted"
                );
            };

            if !payload_acks_registered {
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
                self.maybe_run_before_metadata_command_pending_install_hook();
                let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                    publisher,
                    pg_id,
                    &req.bucket,
                    &command,
                    Some(effect_fence),
                ) {
                    Ok(install) => install,
                    Err(error) => {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                };
                match install {
                    SnapshotSensitiveInstallOutcome::Installed => {
                        payload_ownership = DirectPutPayloadOwnership::DurableCommand;
                        disarm_payload_cleanup();
                    }
                    SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        sleep_direct_put_before_command_ownership_after_contention!(
                            "direct PUT pending install retry budget exhausted"
                        );
                        continue;
                    }
                }
            }
            break (command, new_pending_command);
        };

        let command = loop {
            let recovery = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery(pg_id, &command);
            let _recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::Waited { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_wait");
                    let waiter_outcome =
                        self.pending_command_recovery_waiter_outcome(pg_id, &command)?;
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
                        | MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
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
                            work_budget
                                .sleep_after_contention(
                                    "direct PUT pending recovery retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut { wait_us } => {
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
                    work_budget
                        .sleep_after_contention(
                            "direct PUT pending recovery retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };

            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(()) => break,
                    Err(error)
                        if request_ops::metadata_command_apply_transport_error_is_retryable(
                            &error.source,
                        ) =>
                    {
                        work_budget
                            .sleep_after_contention(
                                "direct PUT metadata command transport retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
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
                        break;
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
                            "retryable partial direct PUT command conflict",
                        ));
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let reissue_result = self.reissue_pending_metadata_command(pg_id, &command);
                        let Some(reissued) = (match reissue_result {
                            Ok(reissued) => reissued,
                            Err(BucketSnapshotLoadError::Store(
                                StoreError::MetadataCommandLogConflict { .. },
                            )) => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable direct PUT commit reissue conflict",
                                ));
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                            }
                        }) else {
                            if new_pending_command {
                                self.release_metadata_command_bucket_write_reservation(&command)
                                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
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
                            }
                            return Err(conflicting_pending_object_metadata_command(
                                "pending direct PUT command was displaced during reissue",
                            ));
                        };
                        work_budget
                            .sleep_after_contention(
                                "direct PUT commit reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if new_pending_command && error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.release_metadata_command_bucket_write_reservation(&command)
                                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id,
                                &req.bucket,
                                &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
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
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }

            if self
                .release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
                    pg_id, &command,
                )
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?
            {
                self.remove_pending_metadata_command_for_bucket(
                    pg_id,
                    command.bucket_name(),
                    &command,
                )
                .map_err(ObjectPgActionError::from)?;
            }
            self.emit_metadata_command_recovery_outcome_for_command(pg_id, &command, "applied");
            break command;
        };

        debug_assert!(matches!(
            payload_ownership,
            DirectPutPayloadOwnership::DurableCommand
        ));

        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                #[cfg(any(test, feature = "test-hooks"))]
                crate::node::maybe_run_after_direct_put_metadata_publish_hook(
                    self.metadata_primary_test_hook_node().test_hook_scope_id(),
                    &commit.object.bucket,
                    &commit.object.key,
                )?;
                Ok(Ok(FinalizeDirectPutObjectOutcome {
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
                }))
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
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("put object stream create retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, bucket,
                )?;
            let expected_command =
                applied_stream_create_command(&applied_commands, &request, cleanup_after);
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
            )? {
                SnapshotSensitiveInstallOutcome::Installed => {}
                SnapshotSensitiveInstallOutcome::ContenderDrained => {
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
        let segment_okh = crate::stream_segment_key_hash(input.session_id, input.segment_index);
        let (target, segment_record) = self
            .prepare_stream_segment_append_with_route_validation(
                route,
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: input.session_id.clone(),
                    segment_index: input.segment_index,
                    size: logical_size,
                    segment_crc64: checksum::crc64::checksum(input.storage_bytes),
                    payload_crc64: input.payload_crc64,
                    segment_okh,
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
        self.commit_stream_segment_append_with_route_validation(
            route,
            input.session_id,
            input.segment_index,
            &segment_record,
            &shard_batch,
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
                match self.drain_pending_metadata_command_with_recovery_authority(
                    &mut recovery_authority,
                    pg_id,
                    &command,
                )? {
                    PendingMetadataCommandOutcome::Applied
                    | PendingMetadataCommandOutcome::Abandoned => {}
                    PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial pending stream append preparation command",
                        ));
                    }
                }
                recovery_authority.sleep_after_contention(
                    "stream append preparation contention retry budget exhausted",
                )?;
                continue;
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
            || Ok(()),
            RequestWorkBudget::new(STREAM_SEGMENT_APPEND_RETRY_BUDGET, None)
                .for_operation("commit_stream_segment_append")
                .for_pg(PgId::new(self.object_metadata_pg_id(bucket, key))),
        )
    }

    fn commit_stream_segment_append_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        segment_index: u32,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        let request = StreamAppendCommitRequest {
            bucket: route.bucket,
            key: route.key,
            session_id,
            segment_index,
            segment_record,
            shard_batch,
        };
        self.commit_stream_segment_append_with_work_budget(
            route,
            request,
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
                if let Err(error) =
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)
                {
                    cleanup_stream_append_payload!();
                    return Err(error);
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
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            };
            let apply_outcome = match self.apply_new_stream_append_command(
                pg_id,
                bucket,
                &command,
                segment_record,
                shard_batch,
            ) {
                Ok(outcome) => outcome,
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

        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(|error| ObjectPgActionError::Store(cluster_build_error_to_store(error)))?;
        for (key, ack) in shard_batch {
            let location = Self::placed_payload_shard_location(&locations, key)
                .map_err(ObjectPgActionError::Store)?;
            self.read_payload_shard(location, key, *ack)
                .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
        }
        Ok(())
    }

}
