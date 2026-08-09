// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl StorageCluster {
    pub(crate) fn audit_shard_storage_for_scavenger(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let referenced_scan = self.collect_shard_scavenger_referenced_shards();
        let mut reference_scan_errors = Vec::new();
        let referenced_scan = match referenced_scan {
            Ok(referenced_scan) => referenced_scan,
            Err(error) => {
                reference_scan_errors.push(format!("reference scan failed: {error}"));
                ShardScavengerReferenceScan::default()
            }
        };
        let mut expected_nodes_by_shard: HashMap<(u32, ShardKey), HashSet<u32>> = HashMap::new();
        for (node_id, data_pg_id, shard_key) in &referenced_scan.locations {
            expected_nodes_by_shard
                .entry((*data_pg_id, shard_key.clone()))
                .or_default()
                .insert(*node_id);
        }
        let mut observations = Vec::new();

        for route in self.local_pg_routes() {
            let primary_node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let primary_node_id = primary_node.node_id().as_u32();
            let data_pg = self.validated_data_pg(route.pg_id())?;
            let data_pg_id = data_pg.get();
            let scavenger_client = primary_node.shard_scavenger_client();
            let observation_route = primary_node
                .shard_scavenger_observation_client()
                .open_shard_scavenger_observation_route(data_pg)?;
            let shard_rows = scavenger_client
                .open_shard_scavenger_data_route(self.operation_epoch(), data_pg)?
                .list_scavenger_shard_rows()?;
            let rows_by_key: HashMap<ShardKey, WriteAck> = shard_rows
                .iter()
                .map(|row| (row.key.clone(), row.ack))
                .collect();

            let mut files_by_node = Vec::new();
            let mut scan_errors = Vec::new();
            for node_id in self.local_map.node_ids() {
                let Some(node) = self.local_map.node(node_id) else {
                    continue;
                };
                match node
                    .shard_scavenger_client()
                    .open_shard_scavenger_data_route(self.operation_epoch(), data_pg)
                    .and_then(|route| route.list_scavenger_shard_files())
                {
                    Ok(scan) if scan.errors.is_empty() => {
                        files_by_node.push((node_id.as_u32(), scan.files));
                    }
                    Ok(scan) => {
                        scan_errors.extend(
                            scan.errors
                                .into_iter()
                                .map(|error| (node_id.as_u32(), error)),
                        );
                    }
                    Err(error) => {
                        scan_errors.push((node_id.as_u32(), error.to_string()));
                    }
                }
            }

            if !reference_scan_errors.is_empty() {
                observation_route.record_shard_scavenger_observation(
                    &Self::shard_scavenger_scan_incomplete_observation(
                        primary_node_id,
                        data_pg_id,
                        &reference_scan_errors,
                    ),
                )?;
                observations.extend(observation_route.list_shard_scavenger_observations()?);
                continue;
            }

            if !scan_errors.is_empty() {
                let mut errors_by_node: BTreeMap<u32, Vec<String>> = BTreeMap::new();
                for (node_id, error) in scan_errors {
                    errors_by_node.entry(node_id).or_default().push(error);
                }
                for (node_id, errors) in errors_by_node {
                    observation_route.record_shard_scavenger_observation(
                        &Self::shard_scavenger_scan_incomplete_observation(
                            node_id, data_pg_id, &errors,
                        ),
                    )?;
                }
                observations.extend(observation_route.list_shard_scavenger_observations()?);
                continue;
            }

            let mut active_observations = HashSet::new();
            let mut file_locations = HashSet::new();
            for (node_id, files) in files_by_node {
                for file in files {
                    file_locations.insert((node_id, data_pg_id, file.key.clone()));
                    let observation_key = crate::types::ShardScavengerObservationKey {
                        node_id,
                        data_pg_id,
                        shard_index: file.key.shard_index(),
                        shard_key: file.key.clone(),
                    };
                    let Some(row_ack) = rows_by_key.get(&file.key).copied() else {
                        active_observations.insert(observation_key.clone());
                        observation_route.record_shard_scavenger_observation(
                            &ShardScavengerObservationRecord {
                                key: observation_key,
                                data_size: Some(file.size),
                                crc64: None,
                                file_exists: true,
                                shard_row_exists: false,
                                reason: ShardScavengerObservationReason::FileWithoutShardRow,
                                last_error: None,
                            },
                        )?;
                        continue;
                    };
                    let shard_identity = (node_id, data_pg_id, file.key.clone());
                    if referenced_scan.locations.contains(&shard_identity) {
                        continue;
                    }
                    active_observations.insert(observation_key.clone());
                    observation_route.record_shard_scavenger_observation(
                        &ShardScavengerObservationRecord {
                            key: observation_key,
                            data_size: Some(row_ack.stored_size),
                            crc64: Some(row_ack.crc64),
                            file_exists: true,
                            shard_row_exists: true,
                            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                            last_error: None,
                        },
                    )?;
                }
            }

            for row in shard_rows {
                let shard_identity = (data_pg_id, row.key.clone());
                if let Some(expected_nodes) = expected_nodes_by_shard.get(&shard_identity) {
                    for expected_node_id in expected_nodes {
                        if file_locations.contains(&(
                            *expected_node_id,
                            data_pg_id,
                            row.key.clone(),
                        )) {
                            continue;
                        }
                        let observation_key = crate::types::ShardScavengerObservationKey {
                            node_id: *expected_node_id,
                            data_pg_id,
                            shard_index: row.key.shard_index(),
                            shard_key: row.key.clone(),
                        };
                        active_observations.insert(observation_key.clone());
                        observation_route.record_shard_scavenger_observation(
                            &ShardScavengerObservationRecord {
                                key: observation_key,
                                data_size: Some(row.ack.stored_size),
                                crc64: Some(row.ack.crc64),
                                file_exists: false,
                                shard_row_exists: true,
                                reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                                last_error: None,
                            },
                        )?;
                        if let Some(work_item) = referenced_scan
                            .repair_work_by_shard
                            .get(&(data_pg_id, row.key.clone()))
                            .copied()
                        {
                            self.schedule_placed_segment_shard_repair(
                                work_item.request,
                                work_item.shard_index,
                            )?;
                        }
                    }
                    continue;
                }

                if file_locations.iter().any(|(_, file_data_pg_id, file_key)| {
                    *file_data_pg_id == data_pg_id && file_key == &row.key
                }) {
                    continue;
                }
                let observation_key = crate::types::ShardScavengerObservationKey {
                    node_id: primary_node_id,
                    data_pg_id,
                    shard_index: row.key.shard_index(),
                    shard_key: row.key,
                };
                active_observations.insert(observation_key.clone());
                observation_route.record_shard_scavenger_observation(
                    &ShardScavengerObservationRecord {
                        key: observation_key,
                        data_size: Some(row.ack.stored_size),
                        crc64: Some(row.ack.crc64),
                        file_exists: false,
                        shard_row_exists: true,
                        reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                        last_error: None,
                    },
                )?;
            }

            for observation in observation_route.list_shard_scavenger_observations()? {
                if observation.key.data_pg_id != data_pg_id
                    || observation.resolved_at.is_some()
                    || !matches!(
                        observation.reason,
                        ShardScavengerObservationReason::FileWithoutShardRow
                            | ShardScavengerObservationReason::ShardRowWithoutFile
                            | ShardScavengerObservationReason::UnreferencedShardRowAndFile
                            | ShardScavengerObservationReason::ScanIncomplete
                    )
                {
                    continue;
                }
                if !active_observations.contains(&observation.key) {
                    observation_route.resolve_shard_scavenger_observation(&observation.key)?;
                }
            }

            observations.extend(observation_route.list_shard_scavenger_observations()?);
        }

        Ok(observations)
    }

    fn shard_scavenger_scan_incomplete_observation(
        node_id: u32,
        data_pg_id: u32,
        errors: &[String],
    ) -> ShardScavengerObservationRecord {
        let shard_key = ShardKey::new(&[0; 16], 0, 0);
        ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id,
                data_pg_id,
                shard_index: shard_key.shard_index(),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: false,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: Some(errors.join("; ")),
        }
    }

    fn collect_shard_scavenger_referenced_shards(
        &self,
    ) -> Result<ShardScavengerReferenceScan, StoreError> {
        let mut scan = ShardScavengerReferenceScan::default();
        for route in self.local_pg_routes() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let scan_pg_id = self.object_metadata_scan_pg(route.pg_id());
            for reference in node
                .shard_scavenger_client()
                .open_shard_scavenger_object_scan_route(self.operation_epoch(), scan_pg_id)?
                .list_shard_scavenger_payload_references()?
            {
                match reference {
                    ShardScavengerPayloadReference::Placed(reference) => {
                        self.extend_referenced_shard_set(&mut scan, &reference)?;
                    }
                    ShardScavengerPayloadReference::ReclaimOnly(reference) => {
                        self.extend_reclaim_referenced_shard_set(&mut scan, &reference)?;
                    }
                }
            }
        }

        Ok(scan)
    }

    fn extend_referenced_shard_set(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        reference: &ShardScavengerPlacedShardSetReference,
    ) -> Result<(), StoreError> {
        self.extend_referenced_shard_locations(
            scan,
            reference.data_pg_id,
            reference.okh,
            reference.generation_id,
            reference.placement_cluster_epoch,
            reference.ec,
        )?;
        let request = SegmentStoredBytesRequest {
            data_pg_id: reference.data_pg_id,
            segment_okh: reference.okh,
            segment_vid: reference.generation_id,
            stored_size: reference.stored_size as usize,
            segment_crc64: reference.crc64,
            ec: reference.ec,
        };
        for key in
            Self::payload_shard_set_keys(&reference.okh, reference.generation_id, reference.ec)
        {
            scan.repair_work_by_shard.insert(
                (reference.data_pg_id, key.clone()),
                PlacedSegmentShardRepairWorkItem {
                    request,
                    shard_index: key.shard_index(),
                },
            );
        }
        Ok(())
    }

    fn extend_reclaim_referenced_shard_set(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        reference: &crate::types::ShardScavengerReclaimShardSetReference,
    ) -> Result<(), StoreError> {
        self.extend_referenced_shard_locations(
            scan,
            reference.data_pg_id,
            reference.okh,
            reference.generation_id,
            self.operation_epoch(),
            reference.ec,
        )
    }

    fn extend_referenced_shard_locations(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        data_pg_id: u32,
        okh: [u8; 16],
        generation_id: GenerationId,
        placement_cluster_epoch: ClusterEpoch,
        ec: EcShape,
    ) -> Result<(), StoreError> {
        let data_pg = self.validated_data_pg(PgId::new(data_pg_id))?;
        let placement_key = segment_payload_placement_key(&okh, generation_id);
        let route =
            self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(&route, data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        for key in Self::payload_shard_set_keys(&okh, generation_id, ec) {
            let location = Self::placed_payload_shard_location(&locations, &key)?;
            scan.locations
                .insert((location.node_id().as_u32(), data_pg_id, key.clone()));
        }
        Ok(())
    }

    pub(crate) fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let route = PutObjectMutationEffectRoute {
            bucket_pg_id: self.bucket_metadata_pg(bucket),
            object_pg_id: self.object_metadata_pg(bucket, key),
            bucket,
            key,
            effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
        };
        let pg_id = route.object_pg_id.pg_id();
        self.abort_stream_upload_session_with_work_budget(
            route,
            session_id,
            || Ok(()),
            RequestWorkBudget::new(STREAM_UPLOAD_ABORT_RETRY_BUDGET, None)
                .for_operation("abort_stream_upload_session")
                .for_pg(pg_id),
        )
    }

    #[cfg(test)]
    fn test_abort_stream_upload_session_with_max_attempts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        max_attempts: usize,
    ) -> Result<(), ObjectPgActionError> {
        let route = PutObjectMutationEffectRoute {
            bucket_pg_id: self.bucket_metadata_pg(bucket),
            object_pg_id: self.object_metadata_pg(bucket, key),
            bucket,
            key,
            effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
        };
        let pg_id = route.object_pg_id.pg_id();
        self.abort_stream_upload_session_with_work_budget(
            route,
            session_id,
            || Ok(()),
            RequestWorkBudget::new(STREAM_UPLOAD_ABORT_RETRY_BUDGET, Some(max_attempts))
                .for_operation("abort_stream_upload_session")
                .for_pg(pg_id),
        )
    }

    fn abort_stream_upload_session_with_route_validation(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        self.abort_stream_upload_session_with_work_budget(
            route,
            session_id,
            require_valid_route,
            RequestWorkBudget::new(STREAM_UPLOAD_ABORT_RETRY_BUDGET, None)
                .for_operation("abort_stream_upload_session")
                .for_pg(route.object_pg_id.pg_id()),
        )
    }

    fn abort_stream_upload_session_with_work_budget(
        &self,
        route: PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut work_budget: RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(AbortStreamUploadSession);
        let bucket = route.bucket;
        let key = route.key;
        let object_pg_id = route.object_pg_id;
        let pg_id = object_pg_id.pg_id();
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let stream_route = mutation_client.open_stream_upload_session_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
            session_id,
        )?;
        let mut pending_completed_session = false;
        let observed_stream_session = loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("stream abort initial recovery retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            pending_completed_session |=
                self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                work_budget
                    .sleep_after_contention(
                        "stream abort initial pending drain retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }
            match stream_route.load_segments() {
                Ok(_) => break true,
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) if pending_completed_session => return Ok(()),
                Err(error) => return Err(error),
            }
        };

        #[cfg(any(test, feature = "test-hooks"))]
        self.maybe_run_before_stream_abort_storage_hook();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("stream abort retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            pending_completed_session |=
                self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                work_budget
                    .sleep_after_contention("stream abort pending drain retry budget exhausted")
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }

            let stream_session = match stream_route.load_session() {
                Ok(stream_session) => stream_session,
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) if pending_completed_session || observed_stream_session => {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let staged_segments = match stream_route.load_segments() {
                Ok(staged_segments) => staged_segments,
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) if pending_completed_session || observed_stream_session => {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    work_budget
                        .sleep_after_contention(
                            "stream abort command id conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    session_id: session_id.clone(),
                    staged_segments,
                    stream_create_bucket_write_reservation: stream_session
                        .bucket_write_reservation
                        .clone(),
                })),
            );
            match self.install_terminal_session_retry_metadata_command(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(route.effect_fence),
                |pending| {
                    pending_command_completes_stream_session(pending, bucket, key, session_id)
                },
            )? {
                TerminalSessionRetryInstallOutcome::Installed => {}
                TerminalSessionRetryInstallOutcome::MatchingContenderVisible(_)
                | TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible
                | TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand => {
                    work_budget
                        .sleep_after_contention(
                            "stream abort pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(());
        }
    }

    fn abort_stream_upload_session_with_retained_cleanup(
        &self,
        cluster_epoch: ClusterEpoch,
        object_pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.maybe_run_before_retained_stream_abort_hook()?;
        if cluster_epoch != self.operation_epoch()
            || object_pg_id != self.object_metadata_pg(bucket, key)
        {
            return Err(ObjectPgActionError::Store(
                StoreError::StaleMetadataOperation {
                    pg_id: object_pg_id.get(),
                    operation_epoch: cluster_epoch,
                    current_epoch: self.operation_epoch(),
                },
            ));
        }
        let pg_id = object_pg_id.pg_id();
        let primary = self
            .local_map
            .metadata_pg_primary_node_for_metadata_command_recovery(cluster_epoch, pg_id)?;
        let Some(prepared) = primary
            .retained_object_mutation_metadata_client()
            .open_retained_object_mutation_route(object_pg_id, cluster_epoch, bucket, key)
            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
            .prepare_retained_stream_upload_abort(session_id)?
        else {
            return Ok(());
        };
        let abort = prepared.abort();

        for node in self
            .local_map
            .metadata_pg_acting_nodes_for_metadata_command_recovery(cluster_epoch, pg_id)?
        {
            node.retained_metadata_command_client()
                .open_retained_stream_upload_abort_route(&prepared)
                .map_err(ObjectPgActionError::Store)?
                .apply()
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
        }

        if let Some(proof) = &abort.stream_create_bucket_write_reservation {
            self.release_stream_create_bucket_write_reservation_proof(proof)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
        }
        self.delete_staged_stream_segment_payload_shards_at_retained_epoch(&abort.staged_segments)?;
        primary
            .retained_metadata_command_client()
            .open_retained_stream_upload_abort_route(&prepared)
            .map_err(ObjectPgActionError::Store)?
            .finish()?;
        Ok(())
    }

    fn list_stream_upload_sessions_best_effort_inner(&self) -> Vec<StreamUploadRecord> {
        const STREAM_UPLOAD_SESSION_BEST_EFFORT_PAGE_LIMIT: u32 = 1024;
        let mut sessions = Vec::new();
        for &pg_id in self.local_map.pg_ids() {
            let pg_id = PgId::new(pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let Ok(node) = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            else {
                continue;
            };
            let mut marker = None;
            let Ok(scan_route) = node
                .object_mutation_metadata_client()
                .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
            else {
                continue;
            };
            while let Ok(page) = scan_route.list_all_stream_uploads_page(
                marker.as_ref(),
                STREAM_UPLOAD_SESSION_BEST_EFFORT_PAGE_LIMIT,
            ) {
                if page.uploads.iter().any(|upload| {
                    self.object_metadata_pg_id(&upload.bucket, &upload.key) != pg_id.get()
                }) {
                    break;
                }
                sessions.extend(page.uploads);
                let Some(next_marker) = page.next_session_id_marker else {
                    break;
                };
                marker = Some(next_marker);
            }
        }
        sessions
    }

    pub(crate) fn scavenge_abandoned_stream_sessions(
        &self,
        max_age_ms: u64,
    ) -> StreamSessionSweepSummary {
        let now = crate::clock::current_time_millis();
        let cutoff = now.saturating_sub(max_age_ms);
        let mut summary = StreamSessionSweepSummary::default();

        for session in self.list_stream_upload_sessions_best_effort_inner() {
            summary.discovered += 1;
            let durable_cleanup_due = session
                .cleanup_after
                .is_some_and(|cleanup_after| cleanup_after <= now);
            if !durable_cleanup_due {
                if session.created_at >= cutoff || session.target != StreamUploadTarget::PutObject {
                    continue;
                }
                match self.stream_upload_has_live_bucket_write_reservation(&session) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(_) => {
                        summary.reservation_check_failed += 1;
                        continue;
                    }
                }
            }
            summary.due += 1;
            match self.abort_stream_upload_session(
                &session.bucket,
                &session.key,
                &session.session_id,
            ) {
                Ok(()) => {
                    if session.target == StreamUploadTarget::PutObject {
                        let _ = self.release_object_generation_reservation(
                            &session.bucket,
                            &session.key,
                            &session.session_id,
                        );
                    }
                    summary.cleaned += 1;
                }
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) => {
                    if session.target == StreamUploadTarget::PutObject {
                        let _ = self.release_object_generation_reservation(
                            &session.bucket,
                            &session.key,
                            &session.session_id,
                        );
                    }
                    summary.cleaned += 1;
                }
                Err(_) => summary.abort_failed += 1,
            }
        }

        summary
    }

    #[cfg(test)]
    pub(crate) fn list_stream_upload_sessions_best_effort(&self) -> Vec<StreamUploadRecord> {
        self.list_stream_upload_sessions_best_effort_inner()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_scavenge_abandoned_stream_sessions(&self, max_age_ms: u64) -> usize {
        self.scavenge_abandoned_stream_sessions(max_age_ms).cleaned
    }

    #[cfg(test)]
    pub(crate) fn read_segment_payload_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.read_segment_payload_stored_bytes_at_placement_epoch_into(
            self.operation_epoch(),
            req,
            dst,
        )
    }

    /// Reads one opaque persisted payload segment, including historical-route
    /// selection when its placement predates the current runtime map.
    fn read_object_payload_segment_stored_bytes_into(
        &self,
        segment: &ObjectPayloadSegment,
        dst: &mut Vec<u8>,
    ) -> Result<(), ObjectReadFailure> {
        self.read_segment_payload_stored_bytes_at_placement_epoch_into(
            segment.placement_cluster_epoch(),
            segment.stored_bytes_request(),
            dst,
        )
        .map_err(ObjectReadFailure::from_store)
    }

    pub(crate) fn read_segment_payload_stored_bytes_at_placement_epoch_into(
        &self,
        placement_cluster_epoch: ClusterEpoch,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        let found = if placement_cluster_epoch == self.operation_epoch() {
            self.try_read_placed_segment_stored_bytes_into(req, dst, true)?
        } else {
            let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
            let route =
                self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
            self.try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
                &route, req, dst, None, None,
            )?
        };
        match found {
            true => Ok(()),
            false => {
                dst.clear();
                Err(StoreError::NotFound)
            }
        }
    }

    fn read_retained_segment_payload_stored_bytes_at_placement_epoch_into(
        &self,
        placement_cluster_epoch: ClusterEpoch,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        leased_node_ids: &BTreeSet<NodeId>,
        repair_fence: Option<&RetainedActiveRouteRepairFence>,
    ) -> Result<(), StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let route =
            self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
        let mut repair_targets = Vec::new();
        match self.try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
            &route,
            req,
            dst,
            Some(leased_node_ids),
            Some(&mut repair_targets),
        )? {
            true => {
                let Some(repair_fence) = repair_fence else {
                    return Ok(());
                };
                if !self.local_map.route_map_lease_snapshot_is_valid_at(
                    repair_fence.admitted_lease,
                    crate::clock::monotonic_time_millis(),
                ) {
                    return Ok(());
                }
                let Some(_permit) = repair_fence
                    .gate
                    .acquire_for_publication_generation(repair_fence.publication_generation)
                else {
                    return Ok(());
                };
                for shard_index in repair_targets {
                    if !self.local_map.route_map_lease_snapshot_is_valid_at(
                        repair_fence.admitted_lease,
                        crate::clock::monotonic_time_millis(),
                    ) {
                        break;
                    }
                    if let Err(error) = self.schedule_placed_segment_shard_repair(req, shard_index)
                    {
                        self.emit_best_effort_payload_repair_error(
                            "record recovered payload shard repair",
                            &error,
                        );
                    }
                }
                Ok(())
            }
            false => {
                dst.clear();
                Err(StoreError::NotFound)
            }
        }
    }

    pub(crate) fn try_take_placed_segment_shard_repair_work(
        &self,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        self.local_map
            .runtime_state()
            .try_take_placed_segment_shard_repair_work()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn try_take_matching_placed_segment_shard_repair_work(
        &self,
        matches: impl FnMut(&PlacedSegmentShardRepairWorkItem) -> bool,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        self.local_map
            .runtime_state()
            .try_take_matching_placed_segment_shard_repair_work(matches)
    }

    pub(crate) fn wait_for_placed_segment_shard_repair_work(
        &self,
        stop: &AtomicBool,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        self.local_map
            .runtime_state()
            .wait_for_placed_segment_shard_repair_work_poll(stop)
    }

    pub(crate) fn wake_placed_segment_shard_repair_workers(&self) {
        self.local_map
            .runtime_state()
            .wake_placed_segment_shard_repair_workers();
    }

    pub(crate) fn enqueue_durable_placed_segment_shard_repair_work(
        &self,
    ) -> Result<DurablePlacedSegmentShardRepairEnqueueSummary, StoreError> {
        let mut summary = DurablePlacedSegmentShardRepairEnqueueSummary::default();
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            for repair in self.list_placed_segment_shard_repairs(route.pg_id().get())? {
                summary.scanned += 1;
                if self.enqueue_placed_segment_shard_repair(
                    repair.work_item.request,
                    repair.work_item.shard_index,
                ) {
                    summary.enqueued += 1;
                }
            }
        }
        Ok(summary)
    }

    pub(crate) fn list_placed_segment_shard_repairs(
        &self,
        data_pg_id: u32,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .list_placed_segment_shard_repairs()
    }

    pub(crate) fn acquire_placed_segment_shard_repair_claim(
        &self,
        data_pg_id: u32,
        params: &PlacedSegmentShardRepairClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        let route = self.metadata_pg_primary_shard_ack_route(data_pg_id)?;
        let request = PlacedSegmentShardRepairClaimAcquire {
            claim_id: params.claim_id.clone(),
            owner_token: params.owner_token.clone(),
            cluster_epoch: self.cluster_epoch(),
            claimed_at: params.claimed_at,
            lease_deadline: params.lease_deadline,
            now: params.now,
        };
        route.acquire_placed_segment_shard_repair_claim(&request)
    }

    pub(crate) fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(claim.work_item.request.data_pg_id))?;
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .complete_placed_segment_shard_repair_claim(claim)
    }

    pub(crate) fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(claim.work_item.request.data_pg_id))?;
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .record_placed_segment_shard_repair_claim_error(claim, last_error, next_attempt_after)
    }

    #[cfg(test)]
    pub(crate) fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.record_placed_segment_shard_backfill_with_remaining_tolerance(
            work_item,
            work_item.request.ec.m,
            last_error,
        )
    }

    pub(crate) fn record_placed_segment_shard_backfill_with_remaining_tolerance(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(work_item.request.data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .record_placed_segment_shard_backfill(work_item, remaining_tolerance, last_error)
    }

    #[cfg(test)]
    pub(crate) fn list_placed_segment_shard_backfills(
        &self,
        data_pg_id: u32,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .list_placed_segment_shard_backfills()
    }

    pub(crate) fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(work_item.request.data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .placed_segment_shard_backfill_exists(work_item)
    }

    pub(crate) fn placed_segment_shard_backfill_backlog_depth(&self) -> Result<usize, StoreError> {
        let mut depth = 0usize;
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            let data_pg_id = self.validated_data_pg(route.pg_id())?;
            depth = depth.saturating_add(
                self.metadata_pg_primary_shard_ack_route(data_pg_id)?
                    .count_placed_segment_shard_backfills()?,
            );
        }
        Ok(depth)
    }

    pub(crate) fn enqueue_placed_segment_shard_backfills_from_scavenger_references(
        &self,
        cursor: &mut PlacedSegmentShardBackfillCandidateScanCursor,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        self.enqueue_placed_segment_shard_backfills_from_scavenger_references_with_cursor_and_limit(
            cursor,
            PLACED_SEGMENT_SHARD_BACKFILL_CANDIDATE_SCAN_LIMIT,
            PLACED_SEGMENT_SHARD_BACKFILL_REFERENCE_SCAN_LIMIT,
            PLACED_SEGMENT_SHARD_BACKFILL_PG_SCAN_LIMIT,
            Some(PLACED_SEGMENT_SHARD_BACKFILL_SCAN_TIME_BUDGET),
        )
    }

    #[cfg(test)]
    fn enqueue_placed_segment_shard_backfills_from_scavenger_references_with_limit(
        &self,
        scan_limit: usize,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut cursor = PlacedSegmentShardBackfillCandidateScanCursor::default();
        self.enqueue_placed_segment_shard_backfills_from_scavenger_references_with_cursor_and_limit(
            &mut cursor,
            scan_limit,
            PLACED_SEGMENT_SHARD_BACKFILL_REFERENCE_SCAN_LIMIT,
            usize::MAX,
            None,
        )
    }

    pub(crate) fn enqueue_placed_segment_shard_backfills_from_scavenger_references_with_cursor_and_limit(
        &self,
        cursor: &mut PlacedSegmentShardBackfillCandidateScanCursor,
        scan_limit: usize,
        reference_scan_limit: usize,
        pg_scan_limit: usize,
        time_budget: Option<Duration>,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut summary = PlacedSegmentShardBackfillCandidateEnqueueSummary::default();
        if scan_limit == 0 || reference_scan_limit == 0 || pg_scan_limit == 0 {
            return Ok(summary);
        }

        let desired_epoch = self.operation_epoch();
        let deadline = time_budget.and_then(|budget| Instant::now().checked_add(budget));
        let mut routes: Vec<_> = self.local_pg_routes().collect();
        routes.sort_by_key(|route| route.pg_id());
        if routes.is_empty() {
            *cursor = PlacedSegmentShardBackfillCandidateScanCursor::default();
            return Ok(summary);
        }
        let start = cursor
            .active_pg_id
            .and_then(|active_pg_id| {
                routes
                    .iter()
                    .position(|route| route.pg_id() == active_pg_id)
            })
            .unwrap_or_else(|| {
                cursor.reference_after = None;
                cursor.active_pg_id = None;
                cursor.after_pg_id.map_or(0, |after_pg_id| {
                    let next = routes.partition_point(|route| route.pg_id() <= after_pg_id);
                    if next == routes.len() {
                        0
                    } else {
                        next
                    }
                })
            });
        let routes_to_scan = routes.len().min(pg_scan_limit);
        let mut verified_candidates = 0usize;
        let mut scanned_references = 0usize;
        let mut seen_candidates = HashSet::new();
        let mut last_scanned_pg_id = None;

        for offset in 0..routes_to_scan {
            let route = routes[(start + offset) % routes.len()];
            last_scanned_pg_id = Some(route.pg_id());
            if route.state() != PgState::Active {
                cursor.active_pg_id = None;
                cursor.reference_after = None;
                cursor.after_pg_id = Some(route.pg_id());
                continue;
            }
            let node = match self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())
            {
                Ok(node) => node,
                Err(error) if shard_backfill_candidate_error_is_deferred(&error) => {
                    summary.deferred += 1;
                    cursor.active_pg_id = None;
                    cursor.reference_after = None;
                    cursor.after_pg_id = Some(route.pg_id());
                    continue;
                }
                Err(error) => return Err(error),
            };
            let scan_route = match node
                .shard_scavenger_client()
                .open_shard_scavenger_object_scan_route(
                    self.operation_epoch(),
                    self.object_metadata_scan_pg(route.pg_id()),
                ) {
                Ok(route) => route,
                Err(error) if shard_backfill_candidate_error_is_deferred(&error) => {
                    summary.deferred += 1;
                    cursor.active_pg_id = None;
                    cursor.reference_after = None;
                    cursor.after_pg_id = Some(route.pg_id());
                    continue;
                }
                Err(error) => return Err(error),
            };

            let mut reference_after = if cursor.active_pg_id == Some(route.pg_id()) {
                cursor.reference_after.clone()
            } else {
                None
            };
            loop {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    cursor.active_pg_id = Some(route.pg_id());
                    cursor.reference_after = reference_after;
                    summary.limit_reached = true;
                    return Ok(summary);
                }
                let remaining = reference_scan_limit - scanned_references;
                if remaining == 0 {
                    cursor.active_pg_id = Some(route.pg_id());
                    cursor.reference_after = reference_after;
                    summary.limit_reached = true;
                    return Ok(summary);
                }
                let page_limit =
                    remaining.min(usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT));
                let page_limit = NonZeroU16::new(
                    u16::try_from(page_limit).expect("bounded backfill page limit fits in u16"),
                )
                .expect("backfill page limit is nonzero");
                let page = match scan_route.list_placed_segment_backfill_reference_page(
                    reference_after.as_ref(),
                    page_limit,
                ) {
                    Ok(page) => page,
                    Err(error) if shard_backfill_candidate_error_is_deferred(&error) => {
                        summary.deferred += 1;
                        cursor.active_pg_id = None;
                        cursor.reference_after = None;
                        cursor.after_pg_id = Some(route.pg_id());
                        break;
                    }
                    Err(error) => return Err(error),
                };

                for item in page.items {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        cursor.active_pg_id = Some(route.pg_id());
                        cursor.reference_after = reference_after;
                        summary.limit_reached = true;
                        return Ok(summary);
                    }
                    scanned_references += 1;
                    let source_epoch = item.reference.placement_cluster_epoch;
                    let stored_size = match usize::try_from(item.reference.stored_size) {
                        Ok(stored_size) => stored_size,
                        Err(error) => {
                            note_shard_backfill_candidate_error(
                                &mut summary,
                                &StoreError::PayloadShardSetMismatch {
                                    reason: format!(
                                        "placed segment stored size exceeds local range: {error}"
                                    ),
                                },
                            );
                            reference_after = Some(item.cursor.clone());
                            cursor.active_pg_id = Some(route.pg_id());
                            cursor.reference_after = Some(item.cursor);
                            continue;
                        }
                    };
                    let request = SegmentStoredBytesRequest {
                        data_pg_id: item.reference.data_pg_id,
                        segment_okh: item.reference.okh,
                        segment_vid: item.reference.generation_id,
                        stored_size,
                        segment_crc64: item.reference.crc64,
                        ec: item.reference.ec,
                    };
                    let candidate_key =
                        PlacedSegmentShardBackfillCandidateKey::new(request, source_epoch);
                    if !seen_candidates.insert(candidate_key) {
                        reference_after = Some(item.cursor.clone());
                        cursor.active_pg_id = Some(route.pg_id());
                        cursor.reference_after = Some(item.cursor);
                        continue;
                    }
                    summary.scanned += 1;
                    if source_epoch == desired_epoch {
                        summary.current_epoch += 1;
                        reference_after = Some(item.cursor.clone());
                        cursor.active_pg_id = Some(route.pg_id());
                        cursor.reference_after = Some(item.cursor);
                        continue;
                    }
                    let work_item = PlacedSegmentShardBackfillWorkItem {
                        request,
                        source_cluster_epoch: source_epoch,
                        desired_cluster_epoch: desired_epoch,
                    };
                    match self.placed_segment_shard_backfill_exists(&work_item) {
                        Ok(true) => {
                            summary.already_queued += 1;
                            reference_after = Some(item.cursor.clone());
                            cursor.active_pg_id = Some(route.pg_id());
                            cursor.reference_after = Some(item.cursor);
                            continue;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            note_shard_backfill_candidate_error(&mut summary, &error);
                            reference_after = Some(item.cursor.clone());
                            cursor.active_pg_id = Some(route.pg_id());
                            cursor.reference_after = Some(item.cursor);
                            continue;
                        }
                    }
                    if verified_candidates >= scan_limit {
                        cursor.active_pg_id = Some(route.pg_id());
                        cursor.reference_after = reference_after;
                        summary.limit_reached = true;
                        return Ok(summary);
                    }
                    verified_candidates += 1;
                    reference_after = Some(item.cursor.clone());
                    cursor.active_pg_id = Some(route.pg_id());
                    cursor.reference_after = Some(item.cursor);
                    let pg_id = PgId::new(request.data_pg_id);
                    let source_route =
                        match self.reconstructed_pg_route_at_epoch(pg_id, source_epoch) {
                            Ok(route) => route,
                            Err(error) => {
                                note_shard_backfill_candidate_error(&mut summary, &error);
                                continue;
                            }
                        };
                    let desired_route =
                        match self.reconstructed_pg_route_at_epoch(pg_id, desired_epoch) {
                            Ok(route) => route,
                            Err(error) => {
                                note_shard_backfill_candidate_error(&mut summary, &error);
                                continue;
                            }
                        };
                    match self.placed_segment_payload_shard_locations_are_equal(
                        &source_route,
                        &desired_route,
                        request,
                    ) {
                        Ok(true) => {
                            summary.already_complete += 1;
                            continue;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            note_shard_backfill_candidate_error(&mut summary, &error);
                            continue;
                        }
                    }
                    let plan = match self.placed_segment_payload_shard_backfill_plan(
                        &source_route,
                        &desired_route,
                        request,
                    ) {
                        Ok(plan) => plan,
                        Err(error) => {
                            note_shard_backfill_candidate_error(&mut summary, &error);
                            continue;
                        }
                    };
                    if !plan.unrecoverable_targets.is_empty() {
                        summary.unrecoverable += 1;
                        continue;
                    }
                    if plan.is_complete() {
                        summary.already_complete += 1;
                        continue;
                    }
                    if self
                        .record_placed_segment_shard_backfill_with_remaining_tolerance(
                            &work_item,
                            plan.source_remaining_tolerance(),
                            None,
                        )
                        .map_err(|error| note_shard_backfill_candidate_error(&mut summary, &error))
                        .is_err()
                    {
                        continue;
                    }
                    summary.enqueued += 1;
                }

                if page.complete {
                    cursor.active_pg_id = None;
                    cursor.reference_after = None;
                    cursor.after_pg_id = Some(route.pg_id());
                    break;
                }
                if scanned_references >= reference_scan_limit {
                    cursor.active_pg_id = Some(route.pg_id());
                    cursor.reference_after = reference_after;
                    summary.limit_reached = true;
                    return Ok(summary);
                }
            }
        }

        cursor.active_pg_id = None;
        cursor.reference_after = None;
        cursor.after_pg_id = last_scanned_pg_id;
        if routes_to_scan < routes.len() {
            summary.limit_reached = true;
        }
        Ok(summary)
    }

    fn placed_segment_payload_shard_locations_are_equal(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<bool, StoreError> {
        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let source_locations = self
            .place_payload_shards_for_pg_route_snapshot(
                source_route,
                data_pg,
                req.ec,
                &placement_key,
            )
            .map_err(cluster_build_error_to_store)?;
        let desired_locations = self
            .place_payload_shards_for_pg_route_snapshot(
                desired_route,
                data_pg,
                req.ec,
                &placement_key,
            )
            .map_err(cluster_build_error_to_store)?;
        Ok(source_locations.len() == desired_locations.len()
            && source_locations
                .iter()
                .zip(desired_locations.iter())
                .all(|(source, desired)| {
                    source.data_pg_id() == desired.data_pg_id()
                        && source.shard_index() == desired.shard_index()
                        && source.node_id() == desired.node_id()
                }))
    }

    pub(crate) fn placed_segment_shard_backfill_source_is_referenced(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        for route in self.local_pg_routes() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let references = node
                .shard_scavenger_client()
                .open_shard_scavenger_object_scan_route(
                    self.operation_epoch(),
                    self.object_metadata_scan_pg(route.pg_id()),
                )?
                .list_shard_scavenger_payload_references()?;
            if references.iter().any(|reference| {
                let ShardScavengerPayloadReference::Placed(reference) = reference else {
                    return false;
                };
                let request = work_item.request;
                reference.data_pg_id == request.data_pg_id
                    && reference.okh == request.segment_okh
                    && reference.generation_id == request.segment_vid
                    && reference.placement_cluster_epoch == work_item.source_cluster_epoch
                    && reference.stored_size == request.stored_size as u64
                    && reference.crc64 == request.segment_crc64
                    && reference.ec == request.ec
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn acquire_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: u32,
        params: &PlacedSegmentShardBackfillClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        let route = self.metadata_pg_primary_shard_ack_route(data_pg_id)?;
        let request = PlacedSegmentShardBackfillClaimAcquire {
            claim_id: params.claim_id.clone(),
            owner_token: params.owner_token.clone(),
            cluster_epoch: self.cluster_epoch(),
            claimed_at: params.claimed_at,
            lease_deadline: params.lease_deadline,
            now: params.now,
        };
        route.acquire_placed_segment_shard_backfill_claim(&request)
    }

    #[cfg(test)]
    pub(crate) fn acquire_next_placed_segment_shard_backfill_claim(
        &self,
        params: &PlacedSegmentShardBackfillClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        self.acquire_next_placed_segment_shard_backfill_claim_with_cursor(params, &mut None)
    }

    pub(crate) fn acquire_next_placed_segment_shard_backfill_claim_with_cursor(
        &self,
        params: &PlacedSegmentShardBackfillClaimAcquireParams,
        last_claimed_pg_id: &mut Option<PgId>,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        let mut routes: Vec<_> = self
            .local_pg_routes()
            .filter(|route| route.state() == PgState::Active)
            .collect();
        routes.sort_by_key(|route| route.pg_id());
        let start = last_claimed_pg_id.map_or(0, |last_pg_id| {
            routes.partition_point(|route| route.pg_id() <= last_pg_id)
        });
        for offset in 0..routes.len() {
            let route = routes[(start + offset) % routes.len()];
            if let Some(claim) =
                self.acquire_placed_segment_shard_backfill_claim(route.pg_id().get(), params)?
            {
                *last_claimed_pg_id = Some(route.pg_id());
                return Ok(Some(claim));
            }
        }
        Ok(None)
    }

    pub(crate) fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(claim.work_item.request.data_pg_id))?;
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .complete_placed_segment_shard_backfill_claim(claim)
    }

    pub(crate) fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(claim.work_item.request.data_pg_id))?;
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: data_pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .record_placed_segment_shard_backfill_claim_error(claim, last_error, next_attempt_after)
    }

    #[cfg(test)]
    pub(crate) fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(work_item.request.data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .resolve_placed_segment_shard_backfill(work_item)
    }

    #[cfg(test)]
    fn repair_placed_segment_payload_shard(
        &self,
        req: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<WrittenShardAck, StoreError> {
        let mut repaired = self.repair_placed_segment_payload_shards(req, &[shard_index])?;
        repaired
            .pop()
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: "single-shard repair produced no shard ack".to_string(),
            })
    }

    pub(crate) fn placed_segment_payload_shard_repair_targets(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardIndex>, StoreError> {
        let health = self.placed_segment_payload_shard_health(req)?;
        if matches!(health.risk, PlacedSegmentShardSetRisk::Unrecoverable) {
            return Err(StoreError::NotFound);
        }
        Ok(health.repair_targets())
    }

    pub(crate) fn placed_segment_payload_shard_health(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        validate_placed_segment_repair_ec_shape(req.ec)?;
        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards(data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        self.placed_segment_payload_shard_health_at_locations(
            req,
            &locations,
            PlacedSegmentShardHealthReadMode::CurrentRoute,
            None,
        )
    }

    pub(crate) fn placed_segment_payload_shard_health_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        validate_placed_segment_repair_ec_shape(req.ec)?;
        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(route, data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        self.placed_segment_payload_shard_health_at_locations(
            req,
            &locations,
            PlacedSegmentShardHealthReadMode::HistoricalInspection,
            Some(route),
        )
    }

    pub(crate) fn placed_segment_payload_shard_backfill_plan(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
        let source_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(source_route, req)?;
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        build_placed_segment_shard_backfill_plan(source_health, desired_health)
    }

    #[cfg(test)]
    pub(crate) fn record_placed_segment_shard_backfill_for_plan(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        last_error: Option<&str>,
    ) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "placed segment backfill plan cannot enqueue unrecoverable targets {:?}",
                    plan.unrecoverable_targets
                ),
            });
        }
        if plan.is_complete() {
            return Ok(plan);
        }
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: req,
            source_cluster_epoch: source_route.cluster_epoch(),
            desired_cluster_epoch: desired_route.cluster_epoch(),
        };
        self.record_placed_segment_shard_backfill_with_remaining_tolerance(
            &work_item,
            plan.source_remaining_tolerance(),
            last_error,
        )?;
        Ok(plan)
    }

    #[cfg(test)]
    pub(crate) fn backfill_placed_segment_payload_shard_direct_copies(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "placed segment direct-copy backfill cannot satisfy unrecoverable targets {:?}",
                    plan.unrecoverable_targets
                ),
            });
        }
        if !plan.reconstruction_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason:
                    "placed segment direct-copy backfill cannot satisfy EC reconstruction targets"
                        .to_string(),
            });
        }
        if plan.copy_targets.is_empty() {
            return Ok(Vec::new());
        }

        let mut copied = Vec::with_capacity(plan.copy_targets.len());
        for target in &plan.copy_targets {
            let ack = self.load_payload_shard_ack_for_pg_route_snapshot(
                source_route,
                req.data_pg_id,
                &target.shard_key,
            )?;
            let payload = self
                .read_payload_shard_for_historical_inspection(target.source, &target.shard_key, ack)
                .map_err(shard_io_error_to_store)?;
            let copied_ack = self
                .repair_payload_shard(target.destination, &target.shard_key, &payload)
                .map_err(shard_io_error_to_store)?;
            copied.push(WrittenShardAck {
                key: target.shard_key.clone(),
                ack: copied_ack,
            });
        }

        let copied_acks: Vec<_> = copied
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &copied_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register backfilled payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        for target in &plan.copy_targets {
            let Some(shard) = desired_health
                .shards
                .iter()
                .find(|shard| shard.shard_index == target.shard_index)
            else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} missing from desired health",
                        target.shard_index.get()
                    ),
                });
            };
            if !shard.validation.is_valid() {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} still fails desired-route verification",
                        target.shard_index.get()
                    ),
                });
            }
        }
        Ok(copied)
    }

    pub(crate) fn backfill_placed_segment_payload_shards(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PlacedSegmentBackfillSourceUnavailable);
        }
        if plan.copy_targets.is_empty() && plan.reconstruction_targets.is_empty() {
            return Ok(Vec::new());
        }

        let mut reconstructed_segment = None;
        if !plan.reconstruction_targets.is_empty() {
            let mut segment = Vec::new();
            if !self.try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
                source_route,
                req,
                &mut segment,
                None,
                None,
            )? {
                return Err(StoreError::NotFound);
            }
            reconstructed_segment = Some(segment);
        }

        let mut backfilled =
            Vec::with_capacity(plan.copy_targets.len() + plan.reconstruction_targets.len());
        for target in &plan.copy_targets {
            let ack = self.load_payload_shard_ack_for_pg_route_snapshot(
                source_route,
                req.data_pg_id,
                &target.shard_key,
            )?;
            let payload = self
                .read_payload_shard_for_historical_inspection(target.source, &target.shard_key, ack)
                .map_err(shard_io_error_to_store)?;
            let copied_ack = self
                .repair_payload_shard(target.destination, &target.shard_key, &payload)
                .map_err(shard_io_error_to_store)?;
            backfilled.push(WrittenShardAck {
                key: target.shard_key.clone(),
                ack: copied_ack,
            });
        }

        if !plan.reconstruction_targets.is_empty() {
            let target_slots: Vec<_> = plan
                .reconstruction_targets
                .iter()
                .map(|shard_index| {
                    let desired = plan
                        .desired_health
                        .shards
                        .iter()
                        .find(|shard| shard.shard_index == *shard_index)
                        .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                            reason: format!(
                                "backfill reconstruction shard index {} missing from desired health",
                                shard_index.get()
                            ),
                        })?;
                    Ok((usize::from(shard_index.get()), desired.location))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            let reconstructed =
                self.local_map.write_erasure_coded_segment_shards_with(
                    &req.segment_okh,
                    req.segment_vid,
                    reconstructed_segment.as_ref().ok_or_else(|| {
                        StoreError::PayloadShardSetMismatch {
                            reason:
                                "backfill reconstruction missing reconstructed source segment"
                                    .to_string(),
                        }
                    })?,
                    req.ec,
                    |shard_batch| {
                        let mut written = Vec::with_capacity(target_slots.len());
                        for (slot, target_location) in &target_slots {
                            let (shard_key, shard_payload) =
                                shard_batch.get(*slot).ok_or_else(|| {
                                    StoreError::PayloadShardSetMismatch {
                                        reason: format!(
                                            "backfill reconstruction shard index {} outside encoded shard batch of {}",
                                            slot,
                                            shard_batch.len()
                                        ),
                                    }
                                })?;
                            let ack = self
                                .repair_payload_shard(*target_location, shard_key, shard_payload)
                                .map_err(shard_io_error_to_store)?;
                            written.push((shard_key.clone(), ack));
                        }
                        Ok::<_, StoreError>(written)
                    },
                )?;
            backfilled.extend(reconstructed);
        }

        let backfilled_acks: Vec<_> = backfilled
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &backfilled_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register backfilled payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        let target_indices: Vec<_> = plan
            .copy_targets
            .iter()
            .map(|target| target.shard_index)
            .chain(plan.reconstruction_targets.iter().copied())
            .collect();
        self.verify_backfilled_placed_segment_payload_shards(desired_route, req, &target_indices)?;
        Ok(backfilled)
    }

    pub(crate) fn backfill_placed_segment_payload_shards_for_work_item(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let pg_id = PgId::new(work_item.request.data_pg_id);
        let source_route =
            self.reconstructed_pg_route_at_epoch(pg_id, work_item.source_cluster_epoch)?;
        // Backfill rows capture the desired global epoch observed by the scanner. Later
        // unrelated PG changes can supersede that epoch while this PG's target route is
        // still the current desired placement, so execute toward the current route once
        // this handle has caught up to the recorded desired epoch.
        let desired_epoch = if self.operation_epoch() >= work_item.desired_cluster_epoch {
            self.operation_epoch()
        } else {
            work_item.desired_cluster_epoch
        };
        let desired_route = self.reconstructed_pg_route_at_epoch(pg_id, desired_epoch)?;
        self.backfill_placed_segment_payload_shards(
            &source_route,
            &desired_route,
            work_item.request,
        )
    }

    fn verify_backfilled_placed_segment_payload_shards(
        &self,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        backfilled_shard_indices: &[ShardIndex],
    ) -> Result<(), StoreError> {
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        for target in backfilled_shard_indices {
            let Some(shard) = desired_health
                .shards
                .iter()
                .find(|shard| shard.shard_index == *target)
            else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} missing from desired health",
                        target.get()
                    ),
                });
            };
            if !shard.validation.is_valid() {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} still fails desired-route verification",
                        target.get()
                    ),
                });
            }
        }
        Ok(())
    }

    fn placed_segment_payload_shard_health_at_locations(
        &self,
        req: SegmentStoredBytesRequest,
        locations: &[ShardLocation],
        read_mode: PlacedSegmentShardHealthReadMode,
        historical_route: Option<&PgRouteSnapshot>,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        let ec_config = validate_placed_segment_repair_ec_shape(req.ec)?;
        let k = usize::from(req.ec.k);
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        let mut shards = Vec::with_capacity(ec_config.total_shards());
        let mut valid_shards = 0usize;
        let current_reader = match read_mode {
            PlacedSegmentShardHealthReadMode::CurrentRoute => {
                let reader = self.current_placed_segment_shard_reader(
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                )?;
                if reader.locations() != locations {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: "current segment health locations do not match installed placement"
                            .to_string(),
                    });
                }
                Some(reader)
            }
            PlacedSegmentShardHealthReadMode::HistoricalInspection => None,
        };

        for shard_index in 0..ec_config.total_shards() as u8 {
            let shard_key = ShardKey::new(&req.segment_okh, req.segment_vid.get(), shard_index);
            let location = locations
                .get(usize::from(shard_index))
                .copied()
                .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "inspect shard index {} outside {} placed shards",
                        shard_index,
                        locations.len()
                    ),
                })?;
            let ack_result = match historical_route {
                Some(route) => self.load_payload_shard_ack_for_pg_route_snapshot(
                    route,
                    req.data_pg_id,
                    &shard_key,
                ),
                None => self.load_payload_shard_ack(req.data_pg_id, &shard_key),
            };
            let validation = match ack_result {
                Ok(ack) if ack.stored_size == shard_size as u64 => {
                    let read_result = match read_mode {
                        PlacedSegmentShardHealthReadMode::CurrentRoute => {
                            current_reader
                                .as_ref()
                                .expect("current read mode constructs a placed reader")
                                .read(usize::from(shard_index), ack)
                        }
                        PlacedSegmentShardHealthReadMode::HistoricalInspection => self
                            .read_payload_shard_for_historical_inspection(
                                location, &shard_key, ack,
                            ),
                    };
                    match read_result {
                        Ok(_) => {
                            valid_shards += 1;
                            PlacedSegmentShardValidation::Valid
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            placed_segment_recoverable_shard_error(error)?;
                            PlacedSegmentShardValidation::Unreadable { reason }
                        }
                    }
                }
                Ok(ack) => PlacedSegmentShardValidation::WrongSize {
                    expected: shard_size as u64,
                    actual: ack.stored_size,
                },
                Err(StoreError::NotFound) => PlacedSegmentShardValidation::MissingAck,
                Err(error) => return Err(error),
            };
            shards.push(PlacedSegmentShardHealth {
                shard_index: ShardIndex::new(shard_index),
                shard_key,
                location,
                validation,
            });
        }

        let risk = if valid_shards == ec_config.total_shards() {
            PlacedSegmentShardSetRisk::Healthy
        } else if valid_shards >= k {
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: valid_shards - k,
            }
        } else {
            PlacedSegmentShardSetRisk::Unrecoverable
        };

        Ok(PlacedSegmentShardSetHealth {
            total_shards: ec_config.total_shards(),
            required_shards: k,
            valid_shards,
            risk,
            shards,
        })
    }

    #[cfg(test)]
    fn repair_placed_segment_payload_shards_if_needed(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let repair_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        self.repair_placed_segment_payload_shards_inner(req, &repair_targets, true)
    }

    pub(crate) fn repair_placed_segment_payload_shards_if_needed_preserving_repair_rows(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let repair_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        self.repair_placed_segment_payload_shards_inner(req, &repair_targets, false)
    }

    #[cfg(test)]
    fn repair_placed_segment_payload_shards(
        &self,
        req: SegmentStoredBytesRequest,
        shard_indices: &[ShardIndex],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        self.repair_placed_segment_payload_shards_inner(req, shard_indices, true)
    }

    fn repair_placed_segment_payload_shards_inner(
        &self,
        req: SegmentStoredBytesRequest,
        shard_indices: &[ShardIndex],
        resolve_repair_rows: bool,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let total_shards =
            req.ec
                .k
                .checked_add(req.ec.m)
                .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                    reason: format!("EC shard count overflow for {}+{}", req.ec.k, req.ec.m),
                })?;
        if shard_indices.len() > usize::from(req.ec.m) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "repair requested {} shards, but EC {}+{} can tolerate at most {}",
                    shard_indices.len(),
                    req.ec.k,
                    req.ec.m,
                    req.ec.m
                ),
            });
        }
        let mut seen = HashSet::with_capacity(shard_indices.len());
        for shard_index in shard_indices {
            if shard_index.get() >= total_shards {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "repair shard index {} outside EC {}+{}",
                        shard_index.get(),
                        req.ec.k,
                        req.ec.m
                    ),
                });
            }
            if !seen.insert(shard_index.get()) {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!("duplicate repair shard index {}", shard_index.get()),
                });
            }
        }
        if shard_indices.is_empty() {
            return Ok(Vec::new());
        }

        let mut recovered_segment = Vec::new();
        match self.try_read_placed_segment_stored_bytes_into(req, &mut recovered_segment, false)? {
            true => {}
            false => return Err(StoreError::NotFound),
        }

        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards(data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let target_slots: Vec<(usize, ShardLocation)> = shard_indices
            .iter()
            .map(|shard_index| {
                let slot = usize::from(shard_index.get());
                let location = locations.get(slot).copied().ok_or_else(|| {
                    StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "repair shard index {} outside {} placed shards",
                            shard_index.get(),
                            locations.len()
                        ),
                    }
                })?;
                Ok((slot, location))
            })
            .collect::<Result<_, StoreError>>()?;

        let repaired = self.local_map.write_erasure_coded_segment_shards_with(
            &req.segment_okh,
            req.segment_vid,
            &recovered_segment,
            req.ec,
            |shard_batch| {
                let mut repaired = Vec::with_capacity(target_slots.len());
                for (slot, target_location) in &target_slots {
                    let (shard_key, shard_payload) = shard_batch.get(*slot).ok_or_else(|| {
                        StoreError::PayloadShardSetMismatch {
                            reason: format!(
                                "repair shard index {} outside encoded shard batch of {}",
                                slot,
                                shard_batch.len()
                            ),
                        }
                    })?;
                    let ack = self
                        .repair_payload_shard(*target_location, shard_key, shard_payload)
                        .map_err(shard_io_error_to_store)?;
                    repaired.push((shard_key.clone(), ack));
                }
                Ok::<_, StoreError>(repaired)
            },
        )?;
        let repaired_acks: Vec<_> = repaired
            .iter()
            .map(|repaired| (&repaired.key, repaired.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &repaired_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register repaired payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        self.verify_repaired_placed_segment_payload_shards(req, shard_indices)?;
        if resolve_repair_rows {
            for shard_index in shard_indices {
                self.resolve_placed_segment_shard_repair(req, *shard_index)?;
            }
        }
        Ok(repaired)
    }

    fn verify_repaired_placed_segment_payload_shards(
        &self,
        req: SegmentStoredBytesRequest,
        repaired_shard_indices: &[ShardIndex],
    ) -> Result<(), StoreError> {
        let remaining_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        let repaired: HashSet<_> = repaired_shard_indices
            .iter()
            .map(|shard_index| shard_index.get())
            .collect();
        for shard_index in &remaining_targets {
            if repaired.contains(&shard_index.get()) {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "repaired shard index {} still fails full-set verification",
                        shard_index.get()
                    ),
                });
            }
        }
        for shard_index in remaining_targets {
            self.schedule_placed_segment_shard_repair(req, shard_index)?;
        }
        Ok(())
    }

    fn try_read_placed_segment_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        schedule_repair_on_recovery: bool,
    ) -> Result<bool, StoreError> {
        let k = req.ec.k as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            dst.clear();
            return Ok(true);
        }

        if self.try_read_placed_segment_direct_into(req, dst)? {
            return Ok(true);
        }

        self.try_read_placed_segment_recovery_into(req, dst, schedule_repair_on_recovery)
    }

    fn try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
        &self,
        route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        leased_node_ids: Option<&BTreeSet<NodeId>>,
        mut repair_targets: Option<&mut Vec<ShardIndex>>,
    ) -> Result<bool, StoreError> {
        let ec_config = validate_placed_segment_repair_ec_shape(req.ec)?;
        let k = usize::from(req.ec.k);
        let total_shards = ec_config.total_shards();
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            dst.clear();
            return Ok(true);
        }

        let data_pg = self.validated_data_pg(PgId::new(req.data_pg_id))?;
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(route, data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let mut all_shards = vec![None; total_shards];
        let mut present_count = 0usize;

        for shard_index in 0..total_shards {
            self.try_load_placed_segment_shard_for_historical_inspection(
                &req.segment_okh,
                req.segment_vid,
                &locations,
                leased_node_ids,
                shard_index,
                shard_size,
                &mut all_shards,
                &mut present_count,
                repair_targets.as_deref_mut(),
            )?;
        }

        if present_count < k {
            return Ok(false);
        }

        let present_indices: Vec<usize> = (0..total_shards)
            .filter(|&index| all_shards[index].is_some())
            .collect();
        let initial_indices = &present_indices[..k];
        let codec = erasure_codec_for_shape(req.ec, "build historical segment recovery codec")?;
        let initial_crc64 = Self::reconstruct_historical_segment_from_selected_shards(
            &codec,
            &all_shards,
            initial_indices,
            k,
            shard_size,
            req.stored_size,
            dst,
        )?;
        if initial_crc64 == req.segment_crc64 {
            return Ok(true);
        }

        // A shard's historical owner can prove which bytes it returned, but it
        // cannot prove that those bytes still match the original write after
        // the historical metadata primary is lost. Use the segment checksum as
        // the end-to-end authority and try each initially participating shard
        // as the single corrupt shard. This is bounded by k reconstructions and
        // preserves one-corrupt-shard recovery without a combinatorial search.
        for &excluded_index in initial_indices {
            let candidate_indices: Vec<usize> = present_indices
                .iter()
                .copied()
                .filter(|&index| index != excluded_index)
                .take(k)
                .collect();
            if candidate_indices.len() < k {
                continue;
            }
            let candidate_crc64 = Self::reconstruct_historical_segment_from_selected_shards(
                &codec,
                &all_shards,
                &candidate_indices,
                k,
                shard_size,
                req.stored_size,
                dst,
            )?;
            if candidate_crc64 == req.segment_crc64 {
                Self::record_placed_segment_repair_target(
                    repair_targets.as_deref_mut(),
                    excluded_index,
                );
                return Ok(true);
            }
        }

        Err(StoreError::IntegrityError {
            expected: req.segment_crc64,
            actual: initial_crc64,
        })
    }

    fn record_placed_segment_repair_target(
        targets: Option<&mut Vec<ShardIndex>>,
        shard_index: usize,
    ) {
        let Some(targets) = targets else {
            return;
        };
        let shard_index = ShardIndex::new(shard_index as u8);
        if !targets.contains(&shard_index) {
            targets.push(shard_index);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reconstruct_historical_segment_from_selected_shards(
        codec: &ErasureCodec,
        all_shards: &[Option<Vec<u8>>],
        selected_indices: &[usize],
        k: usize,
        shard_size: usize,
        stored_size: usize,
        dst: &mut Vec<u8>,
    ) -> Result<u64, StoreError> {
        if selected_indices.len() != k {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "historical segment recovery selected {} shards, expected {k}",
                    selected_indices.len()
                ),
            });
        }

        let mut selected = vec![false; all_shards.len()];
        let mut present_refs = Vec::with_capacity(k);
        for &shard_index in selected_indices {
            let Some(selected_slot) = selected.get_mut(shard_index) else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery selected shard index {shard_index} outside {} shards",
                        all_shards.len()
                    ),
                });
            };
            if *selected_slot {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery selected shard index {shard_index} twice"
                    ),
                });
            }
            *selected_slot = true;
            let Some(shard) = all_shards.get(shard_index).and_then(Option::as_ref) else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery selected shard index {shard_index} without payload"
                    ),
                });
            };
            present_refs.push(shard.as_slice());
        }

        let missing_data_indices: Vec<usize> = (0..k).filter(|&index| !selected[index]).collect();
        let mut recovered = vec![0; missing_data_indices.len() * shard_size];
        if !missing_data_indices.is_empty() {
            let mut output_refs: Vec<&mut [u8]> = recovered
                .chunks_exact_mut(shard_size)
                .take(missing_data_indices.len())
                .collect();
            codec
                .reconstruct(
                    selected_indices,
                    &present_refs,
                    &missing_data_indices,
                    &mut output_refs,
                )
                .map_err(|error| StoreError::ErasureCoding {
                    context: "reconstruct placed segment shards from historical route",
                    reason: error.to_string(),
                })?;
        }

        dst.clear();
        dst.reserve(shard_size * k);
        for (data_index, data_is_selected) in selected.iter().copied().take(k).enumerate() {
            if data_is_selected {
                let Some(shard) = all_shards.get(data_index).and_then(Option::as_ref) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "historical segment recovery selected data shard {data_index} without payload"
                        ),
                    });
                };
                dst.extend_from_slice(shard);
                continue;
            }

            let Some(recovered_slot) = missing_data_indices
                .iter()
                .position(|&index| index == data_index)
            else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery did not reconstruct data shard {data_index}"
                    ),
                });
            };
            let start = recovered_slot * shard_size;
            let end = start + shard_size;
            let Some(recovered_shard) = recovered.get(start..end) else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery range {start}..{end} outside reconstructed payload length {}",
                        recovered.len()
                    ),
                });
            };
            dst.extend_from_slice(recovered_shard);
        }
        dst.truncate(stored_size);
        Ok(checksum::crc64::checksum(dst))
    }

    fn try_read_placed_segment_direct_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let reader = self.current_placed_segment_shard_reader(
            req.data_pg_id,
            req.ec,
            &req.segment_okh,
            req.segment_vid,
        )?;
        let k = req.ec.k as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        dst.resize(padded, 0);
        let mut direct_acks = Vec::with_capacity(k);
        for shard_index in 0..k {
            let shard_key = reader
                .shard_key(shard_index)
                .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "direct read shard index {shard_index} is outside installed placement"
                    ),
                })?;
            let ack = match self.load_payload_shard_ack(req.data_pg_id, &shard_key) {
                Ok(ack) => ack,
                Err(StoreError::NotFound) => return Ok(false),
                Err(error) => return Err(error),
            };
            if ack.stored_size != shard_size as u64 {
                return Ok(false);
            }
            direct_acks.push(ack);
        }
        let mut read_handles = match reader.acquire_read_handles(0..k) {
            Ok(read_handles) => read_handles,
            Err(error) => {
                let _ = placed_segment_recoverable_shard_error(error)?;
                return Ok(false);
            }
        };

        for (shard_index, ack) in direct_acks.into_iter().enumerate() {
            let start = shard_index * shard_size;
            let end = start + shard_size;
            let location = read_handles
                .location(shard_index)
                .expect("leased direct-read shard is inside installed placement");
            let shard_key = read_handles
                .shard_key(shard_index)
                .expect("leased direct-read shard has a derived key");
            if let Err(error) = self
                .maybe_run_before_placed_payload_shard_read_hook(location, &shard_key)
            {
                read_handles.release().map_err(shard_io_error_to_store)?;
                let _ = placed_segment_recoverable_shard_error(error)?;
                return Ok(false);
            }
            match read_handles.read_into(shard_index, ack, &mut dst[start..end]) {
                Ok(()) => {}
                Err(error) => {
                    read_handles.release().map_err(shard_io_error_to_store)?;
                    let _ = placed_segment_recoverable_shard_error(error)?;
                    return Ok(false);
                }
            }
        }
        read_handles.release().map_err(shard_io_error_to_store)?;

        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        Ok(actual_crc64 == req.segment_crc64)
    }

    fn try_read_placed_segment_recovery_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        schedule_repair_on_recovery: bool,
    ) -> Result<bool, StoreError> {
        let k = req.ec.k as usize;
        let m = req.ec.m as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        let reader = self.current_placed_segment_shard_reader(
            req.data_pg_id,
            req.ec,
            &req.segment_okh,
            req.segment_vid,
        )?;
        let mut all_shards = vec![None; k + m];
        let mut present_count = 0usize;
        let mut repair_targets = Vec::new();

        for shard_index in 0..k {
            self.try_load_placed_segment_shard(
                req.data_pg_id,
                &reader,
                shard_index,
                shard_size,
                &mut all_shards,
                &mut present_count,
                Some(&mut repair_targets),
            )?;
        }

        if present_count < k {
            for shard_index in k..(k + m) {
                if present_count >= k {
                    break;
                }
                self.try_load_placed_segment_shard(
                    req.data_pg_id,
                    &reader,
                    shard_index,
                    shard_size,
                    &mut all_shards,
                    &mut present_count,
                    Some(&mut repair_targets),
                )?;
            }
        }

        if present_count < k {
            return Ok(false);
        }

        let mut recovered = None;
        let mut recovered_ranges = vec![None; k];

        if !(0..k).all(|i| all_shards[i].is_some()) {
            let missing_needed: Vec<usize> = (0..k).filter(|&i| all_shards[i].is_none()).collect();
            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
            let mut present_refs = Vec::with_capacity(present_indices.len());
            for &shard_index in &present_indices {
                let Some(shard) = all_shards.get(shard_index).and_then(Option::as_ref) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery present shard index {} missing payload",
                            shard_index
                        ),
                    });
                };
                present_refs.push(shard.as_slice());
            }
            let codec = erasure_codec_for_shape(req.ec, "build segment recovery codec")?;
            let recovered_len = missing_needed.len() * shard_size;
            let mut recovered_buf = vec![0; recovered_len];
            let mut output_refs: Vec<&mut [u8]> = recovered_buf
                .chunks_exact_mut(shard_size)
                .take(missing_needed.len())
                .collect();

            codec
                .reconstruct(
                    &present_indices,
                    &present_refs,
                    &missing_needed,
                    &mut output_refs,
                )
                .map_err(|error| StoreError::ErasureCoding {
                    context: "reconstruct placed segment shards",
                    reason: error.to_string(),
                })?;

            for (slot, &missing_idx) in missing_needed.iter().enumerate() {
                let start = slot * shard_size;
                recovered_ranges[missing_idx] = Some((start, start + shard_size));
            }
            recovered = Some(recovered_buf);
        }

        dst.clear();
        dst.reserve(padded);
        for (idx, shard) in all_shards.iter().take(k).enumerate() {
            if let Some(shard) = shard.as_ref() {
                dst.extend_from_slice(shard);
            } else if let Some((start, end)) = recovered_ranges[idx] {
                let Some(recovered_buf) = recovered.as_ref() else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery missing reconstructed payload for data index {idx}"
                        ),
                    });
                };
                let Some(recovered_shard) = recovered_buf.get(start..end) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery range {start}..{end} outside reconstructed payload length {}",
                            recovered_buf.len()
                        ),
                    });
                };
                dst.extend_from_slice(recovered_shard);
            } else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "segment recovery missing reconstructed shard for data index {idx}"
                    ),
                });
            }
        }
        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        if actual_crc64 != req.segment_crc64 {
            return Err(StoreError::IntegrityError {
                expected: req.segment_crc64,
                actual: actual_crc64,
            });
        }
        if schedule_repair_on_recovery {
            for shard_index in repair_targets {
                self.schedule_placed_segment_shard_repair(req, shard_index)?;
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn try_load_placed_segment_shard(
        &self,
        data_pg_id: u32,
        reader: &LocalPlacedSegmentShardReader<'_>,
        shard_index: usize,
        shard_size: usize,
        all_shards: &mut [Option<Vec<u8>>],
        present_count: &mut usize,
        repair_targets: Option<&mut Vec<ShardIndex>>,
    ) -> Result<(), StoreError> {
        let Some(location) = reader.location(shard_index) else {
            return Ok(());
        };
        let Some(shard_key) = reader.shard_key(shard_index) else {
            return Ok(());
        };
        let ack = match self.load_payload_shard_ack(data_pg_id, &shard_key) {
            Ok(ack) => ack,
            Err(StoreError::NotFound) => {
                Self::record_placed_segment_repair_target(repair_targets, shard_index);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if ack.stored_size != shard_size as u64 {
            Self::record_placed_segment_repair_target(repair_targets, shard_index);
            return Ok(());
        }
        if let Err(error) =
            self.maybe_run_before_placed_payload_shard_read_hook(location, &shard_key)
        {
            if placed_segment_recoverable_shard_error(error)?
                == RecoverableShardReadFailure::RepairRequired
            {
                Self::record_placed_segment_repair_target(repair_targets, shard_index);
            }
            return Ok(());
        }
        match reader.read(shard_index, ack) {
            Ok(shard) => {
                all_shards[shard_index] = Some(shard);
                *present_count += 1;
            }
            Err(error) => {
                if placed_segment_recoverable_shard_error(error)?
                    == RecoverableShardReadFailure::RepairRequired
                {
                    Self::record_placed_segment_repair_target(repair_targets, shard_index);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn try_load_placed_segment_shard_for_historical_inspection(
        &self,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        locations: &[ShardLocation],
        leased_node_ids: Option<&BTreeSet<NodeId>>,
        shard_index: usize,
        shard_size: usize,
        all_shards: &mut [Option<Vec<u8>>],
        present_count: &mut usize,
        repair_targets: Option<&mut Vec<ShardIndex>>,
    ) -> Result<(), StoreError> {
        let Some(location) = locations.get(shard_index).copied() else {
            return Ok(());
        };
        if leased_node_ids.is_some_and(|node_ids| !node_ids.contains(&location.node_id())) {
            return Ok(());
        }
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8);
        if let Err(error) =
            self.maybe_run_before_placed_payload_shard_read_hook(location, &shard_key)
        {
            if placed_segment_recoverable_shard_error(error)?
                == RecoverableShardReadFailure::RepairRequired
            {
                Self::record_placed_segment_repair_target(repair_targets, shard_index);
            }
            return Ok(());
        }
        match self
            .read_payload_shard_for_historical_inspection_self_validating(location, &shard_key)
        {
            Ok((shard, ack)) if ack.stored_size == shard_size as u64 => {
                all_shards[shard_index] = Some(shard);
                *present_count += 1;
            }
            Ok(_) => Self::record_placed_segment_repair_target(repair_targets, shard_index),
            Err(error) => {
                if placed_segment_recoverable_shard_error(error)?
                    == RecoverableShardReadFailure::RepairRequired
                {
                    Self::record_placed_segment_repair_target(repair_targets, shard_index);
                }
            }
        }
        Ok(())
    }

    fn enqueue_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> bool {
        self.local_map
            .runtime_state()
            .enqueue_placed_segment_shard_repair(PlacedSegmentShardRepairWorkItem {
                request,
                shard_index,
            })
    }

    #[cfg(test)]
    pub(crate) fn test_enqueue_placed_segment_shard_repair(
        &self,
        work_item: PlacedSegmentShardRepairWorkItem,
    ) -> bool {
        self.local_map
            .runtime_state()
            .enqueue_placed_segment_shard_repair(work_item)
    }

    fn schedule_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<(), StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(request.data_pg_id))?;
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .record_placed_segment_shard_repair(&work_item, None)?;
        self.enqueue_placed_segment_shard_repair(request, shard_index);
        Ok(())
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<(), StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(request.data_pg_id))?;
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .resolve_placed_segment_shard_repair(&work_item)
    }

    fn load_payload_shard_ack(
        &self,
        data_pg_id: u32,
        shard_key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        // Placed payload bytes are routed by LocalClusterMap; per-shard
        // CRC/size acks are metadata rows in the same PG and are read through
        // that PG's primary.
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        self.metadata_pg_primary_shard_ack_route(data_pg_id)?
            .load_shard_ack(shard_key)
    }

    fn load_payload_shard_ack_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        data_pg_id: u32,
        shard_key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        if route.pg_id() != data_pg_id.pg_id() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "historical shard ack route PG {} does not match data PG {}",
                    route.pg_id().get(),
                    data_pg_id.get()
                ),
            });
        }
        let primary = route.primary_node_id();
        if !route.acting_set().contains(&primary) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: primary.as_u32(),
                pg_id: data_pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
            });
        }
        let node = self
            .local_map
            .node(primary)
            .ok_or(StoreError::NodeNotFound {
                node_id: primary.as_u32(),
                pg_id: data_pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
            })?;
        node.retained_shard_ack_client()
            .open_retained_shard_ack_route(route.cluster_epoch(), data_pg_id, shard_key)
            .and_then(|route| route.load_written_shard_ack_for_historical_inspection())
    }

    fn segment_payload_locations(
        &self,
        req: &SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        self.segment_payload_shard_locations(
            req.data_pg_id,
            req.ec,
            &req.segment_okh,
            req.segment_vid,
        )
    }

    fn current_placed_segment_shard_reader(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<LocalPlacedSegmentShardReader<'_>, StoreError> {
        let data_pg = self.validated_data_pg(PgId::new(data_pg_id))?;
        self.local_map
            .open_placed_segment_shard_reader(
                self.operation_epoch(),
                data_pg,
                ec,
                segment_okh,
                segment_vid,
            )
            .map_err(cluster_build_error_to_store)
    }

    pub(crate) fn segment_payload_shard_locations(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        self.place_payload_shards(data_pg_id, ec, &placement_key)
            .map_err(cluster_build_error_to_store)
    }

    fn segment_payload_shard_locations_at_placement_epoch(
        &self,
        placement_cluster_epoch: ClusterEpoch,
        request: &SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        if placement_cluster_epoch == self.operation_epoch() {
            return self.segment_payload_locations(request);
        }
        let data_pg = self.validated_data_pg(PgId::new(request.data_pg_id))?;
        let route =
            self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
        let placement_key =
            segment_payload_placement_key(&request.segment_okh, request.segment_vid);
        self.place_payload_shards_for_pg_route_snapshot(&route, data_pg, request.ec, &placement_key)
            .map_err(cluster_build_error_to_store)
    }

    fn delete_payload_shard_set(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
    ) -> Result<(), ObjectPgActionError> {
        let shard_keys = Self::payload_shard_set_keys(okh, generation_id, ec);
        self.delete_placed_payload_shard_keys(
            self.validated_data_pg(PgId::new(data_pg_id))?,
            ec,
            okh,
            generation_id,
            &shard_keys,
        )?;
        self.delete_metadata_primary_payload_shard_keys(data_pg_id, &shard_keys)
    }

    fn delete_payload_shard_set_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
    ) {
        let shard_keys = Self::payload_shard_set_keys(okh, generation_id, ec);
        self.delete_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            okh,
            generation_id,
            shard_keys,
        );
    }

    fn delete_payload_shard_keys_best_effort(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            self.operation_epoch(),
            data_pg_id,
            ec,
            okh,
            generation_id,
            shard_keys,
        );
    }

    fn delete_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        let shard_keys: Vec<ShardKey> = shard_keys.into_iter().collect();
        let data_pg = match self.validated_data_pg(PgId::new(data_pg_id)) {
            Ok(data_pg) => data_pg,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error(
                    "validate placed payload cleanup data PG",
                    &error,
                );
                return;
            }
        };
        self.delete_placed_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg,
            ec,
            okh,
            generation_id,
            &shard_keys,
        );
        self.delete_metadata_primary_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            &shard_keys,
        );
    }

    fn delete_placed_payload_shard_keys(
        &self,
        data_pg_id: DataPgId,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: &[ShardKey],
    ) -> Result<(), ObjectPgActionError> {
        let placement_key = segment_payload_placement_key(okh, generation_id);
        let locations = self
            .place_payload_shards(data_pg_id, ec, &placement_key)
            .map_err(|error| ObjectPgActionError::Store(cluster_build_error_to_store(error)))?;

        for shard_key in shard_keys {
            let location = Self::placed_payload_shard_location(&locations, shard_key)
                .map_err(ObjectPgActionError::Store)?;
            self.maybe_run_before_placed_payload_shard_delete_hook(shard_key)
                .map_err(ObjectPgActionError::Store)?;
            self.delete_payload_shard(location, shard_key)
                .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
        }
        Ok(())
    }

    fn delete_placed_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: &[ShardKey],
    ) {
        let placement_key = segment_payload_placement_key(okh, generation_id);
        let route = match self
            .local_map
            .reconstructed_pg_route_at_epoch(data_pg_id.pg_id(), operation_epoch)
        {
            Some(route) => route,
            None => {
                self.emit_best_effort_payload_cleanup_error(
                    "resolve retained payload placement route",
                    &StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "PG {} route for cluster epoch {} is not retained",
                            data_pg_id.get(),
                            operation_epoch.get()
                        ),
                    },
                );
                return;
            }
        };
        if route.state() != PgState::Active {
            self.emit_best_effort_payload_cleanup_error(
                "resolve retained payload placement route",
                &StoreError::PgNotActive {
                    pg_id: data_pg_id.get(),
                    cluster_epoch: route.cluster_epoch(),
                    state: route.state(),
                },
            );
            return;
        }
        let locations = match LocalClusterMap::place_payload_shards_for_pg_route(
            operation_epoch,
            data_pg_id,
            ec,
            &placement_key,
            route.acting_set(),
        ) {
            Ok(locations) => locations,
            Err(error) => {
                let error = cluster_build_error_to_store(error);
                self.emit_best_effort_payload_cleanup_error("place payload shards", &error);
                return;
            }
        };

        for shard_key in shard_keys {
            match Self::placed_payload_shard_location(&locations, shard_key) {
                Ok(location) => {
                    if let Err(error) =
                        self.maybe_run_before_placed_payload_shard_delete_hook(shard_key)
                    {
                        self.emit_best_effort_payload_cleanup_error(
                            "delete placed payload shard",
                            &error,
                        );
                        continue;
                    }
                    if let Err(error) = self
                        .local_map
                        .delete_payload_shard_for_historical_cleanup(location, shard_key)
                    {
                        let error = shard_io_error_to_store(error);
                        self.emit_best_effort_payload_cleanup_error(
                            "delete placed payload shard",
                            &error,
                        );
                    }
                }
                Err(error) => {
                    self.emit_best_effort_payload_cleanup_error(
                        "resolve placed payload shard",
                        &error,
                    );
                }
            }
        }
    }

    fn delete_metadata_primary_payload_shard_keys(
        &self,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) -> Result<(), ObjectPgActionError> {
        let data_pg_id = self.validated_data_pg(PgId::new(data_pg_id))?;
        let shard_ack_route = self.metadata_pg_primary_shard_ack_route(data_pg_id)?;
        for shard_key in shard_keys {
            self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
                .map_err(ObjectPgActionError::Store)?;
            shard_ack_route.delete_shard_ack(shard_key)?;
        }
        Ok(())
    }

    fn delete_metadata_primary_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) {
        let data_pg_id = match self.validated_data_pg(PgId::new(data_pg_id)) {
            Ok(data_pg_id) => data_pg_id,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error(
                    "validate payload acknowledgement cleanup data PG",
                    &error,
                );
                return;
            }
        };
        let shard_ack_client = match self.metadata_pg_primary_shard_ack_client_at_retained_epoch(
            operation_epoch,
            data_pg_id.pg_id(),
        ) {
            Ok(shard_ack_client) => shard_ack_client,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error(
                    "resolve payload ack metadata PG primary",
                    &error,
                );
                return;
            }
        };
        for shard_key in shard_keys {
            if let Err(error) =
                self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
            {
                self.emit_best_effort_payload_cleanup_error("delete payload ack", &error);
                continue;
            }
            if let Err(error) = shard_ack_client
                .open_retained_shard_ack_route(operation_epoch, data_pg_id, shard_key)
                .and_then(|route| route.delete_retained_shard_ack())
            {
                self.emit_best_effort_payload_cleanup_error("delete payload ack", &error);
            }
        }
    }

    fn placed_payload_shard_location(
        locations: &[ShardLocation],
        shard_key: &ShardKey,
    ) -> Result<ShardLocation, StoreError> {
        let shard_index = usize::from(shard_key.shard_index().get());
        locations
            .get(shard_index)
            .copied()
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard index",
                source: std::io::Error::other(format!(
                    "shard index {shard_index} outside {} placed shards",
                    locations.len()
                )),
            })
    }

    fn payload_shard_set_keys(
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) -> Vec<ShardKey> {
        (0..(ec.k + ec.m))
            .map(|shard_index| ShardKey::new(okh, generation_id.get(), shard_index))
            .collect()
    }

    fn delete_staged_stream_segment_payload_shards_best_effort(
        &self,
        segments: &[StreamUploadSegmentRecord],
    ) {
        for segment in segments {
            self.delete_stream_segment_payload_shards_best_effort(segment);
        }
    }

    fn delete_staged_stream_segment_payload_shards_at_retained_epoch(
        &self,
        segments: &[StreamUploadSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        for segment in segments {
            let data_pg_id = self.validated_data_pg(PgId::new(segment.data_pg_id))?;
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            let placement_key =
                segment_payload_placement_key(&segment.segment_okh, segment.segment_vid);
            let route = self
                .local_map
                .reconstructed_pg_route_at_epoch(
                    data_pg_id.pg_id(),
                    segment.placement_cluster_epoch,
                )
                .ok_or_else(|| {
                    ObjectPgActionError::Store(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "PG {} route for staged stream cleanup epoch {} is not retained",
                            data_pg_id.get(),
                            segment.placement_cluster_epoch.get()
                        ),
                    })
                })?;
            if route.state() != PgState::Active {
                return Err(ObjectPgActionError::Store(StoreError::PgNotActive {
                    pg_id: data_pg_id.get(),
                    cluster_epoch: route.cluster_epoch(),
                    state: route.state(),
                }));
            }
            let locations = LocalClusterMap::place_payload_shards_for_pg_route(
                route.cluster_epoch(),
                data_pg_id,
                ec,
                &placement_key,
                route.acting_set(),
            )
            .map_err(|error| ObjectPgActionError::Store(cluster_build_error_to_store(error)))?;
            let shard_keys =
                Self::payload_shard_set_keys(&segment.segment_okh, segment.segment_vid, ec);
            for shard_key in &shard_keys {
                let location = Self::placed_payload_shard_location(&locations, shard_key)
                    .map_err(ObjectPgActionError::Store)?;
                self.maybe_run_before_placed_payload_shard_delete_hook(shard_key)
                    .map_err(ObjectPgActionError::Store)?;
                self.local_map
                    .delete_payload_shard_for_historical_cleanup(location, shard_key)
                    .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
            }
            let shard_ack_client = self
                .metadata_pg_primary_shard_ack_client_at_retained_epoch(
                    segment.placement_cluster_epoch,
                    data_pg_id.pg_id(),
                )
                .map_err(ObjectPgActionError::Store)?;
            for shard_key in &shard_keys {
                self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
                    .map_err(ObjectPgActionError::Store)?;
                shard_ack_client
                    .open_retained_shard_ack_route(
                        segment.placement_cluster_epoch,
                        data_pg_id,
                        shard_key,
                    )
                    .and_then(|route| route.delete_retained_shard_ack())
                    .map_err(ObjectPgActionError::Store)?;
            }
        }
        Ok(())
    }

    fn delete_object_segment_payload_shards_best_effort(&self, segment: &ObjectSegmentRecord) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        );
    }

    fn delete_stream_segment_payload_shards_best_effort(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        );
    }

    fn delete_stream_segment_payload_shard_keys_best_effort(
        &self,
        segment: &StreamUploadSegmentRecord,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            shard_keys,
        );
    }

    fn delete_stream_append_payload_if_unreferenced_best_effort(
        &self,
        object_pg_id: PgId,
        segment: &StreamUploadSegmentRecord,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        let shard_keys = shard_keys.into_iter().collect::<Vec<_>>();
        let cleanup = (|| -> Result<(), StoreError> {
            // The process-local lock serializes embedded clients. The storage
            // node critical section extends that fence across Unix clients so
            // no new metadata command can publish this payload between the
            // reference scan and deletion.
            let pg_lock = self
                .local_map
                .runtime_state()
                .metadata_command_pg_lock(object_pg_id);
            let _pg_guard = pg_lock.lock().unwrap_or_else(|error| error.into_inner());
            let primary = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), object_pg_id)?;
            let primary_metadata_client = primary.metadata_command_client();
            let _critical_section = primary_metadata_client
                .open_metadata_command_critical_section(object_pg_id, self.operation_epoch())?;

            for node in self
                .local_map
                .metadata_pg_acting_nodes(self.operation_epoch(), object_pg_id)?
            {
                let scan_pg_id = self.object_metadata_scan_pg(object_pg_id);
                let references = node
                    .shard_scavenger_client()
                    .open_shard_scavenger_object_scan_route(self.operation_epoch(), scan_pg_id)?
                    .list_shard_scavenger_payload_references()?;
                if references.iter().any(|reference| {
                    self.shard_scavenger_reference_matches_stream_segment(reference, segment)
                }) {
                    return Ok(());
                }
            }

            self.delete_stream_segment_payload_shard_keys_best_effort(segment, shard_keys);
            Ok(())
        })();
        if let Err(error) = cleanup {
            // A failed ownership check must fail closed: retaining an
            // unclassified payload is safer than deleting data that another
            // command may already have published.
            self.emit_best_effort_payload_cleanup_error(
                "resolve staged stream append payload ownership",
                &error,
            );
        }
    }

    fn shard_scavenger_reference_matches_stream_segment(
        &self,
        reference: &ShardScavengerPayloadReference,
        segment: &StreamUploadSegmentRecord,
    ) -> bool {
        match reference {
            ShardScavengerPayloadReference::Placed(reference) => {
                reference.data_pg_id == segment.data_pg_id
                    && reference.okh == segment.segment_okh
                    && reference.generation_id == segment.segment_vid
                    && reference.placement_cluster_epoch == segment.placement_cluster_epoch
            }
            ShardScavengerPayloadReference::ReclaimOnly(reference) => {
                reference.data_pg_id == segment.data_pg_id
                    && reference.okh == segment.segment_okh
                    && reference.generation_id == segment.segment_vid
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_placed_payload_shard_row_exists(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<bool, StoreError> {
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::Io {
                context: "validate placed payload shard test identity",
                source: std::io::Error::other(format!(
                    "location shard index {} does not match key shard index {}",
                    location.shard_index().get(),
                    key.shard_index().get()
                )),
            });
        }
        let route = self.reconstructed_pg_route_at_epoch(
            location.data_pg_id().pg_id(),
            location.cluster_epoch(),
        )?;
        match self.load_payload_shard_ack_for_pg_route_snapshot(
            &route,
            location.data_pg_id().get(),
            key,
        ) {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_placed_payload_shard_file_exists(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<bool, StoreError> {
        if location.shard_index() != key.shard_index() {
            return Err(StoreError::Io {
                context: "validate placed payload shard test identity",
                source: std::io::Error::other(format!(
                    "location shard index {} does not match key shard index {}",
                    location.shard_index().get(),
                    key.shard_index().get()
                )),
            });
        }
        let node = self
            .local_map
            .node(location.node_id())
            .ok_or(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            })?;
        match node
            .test_node()
            .read_shard_file(location.data_pg_id().get(), key)
        {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_payload_shard_file_path(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_index: u8,
    ) -> Result<std::path::PathBuf, StoreError> {
        let data_pg = self.validated_data_pg(PgId::new(data_pg_id))?;
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let location = locations
            .get(usize::from(shard_index))
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard index",
                source: std::io::Error::other(format!(
                    "shard index {shard_index} outside {} placed shards",
                    locations.len()
                )),
            })?;
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index);
        let node = self
            .local_map
            .node(location.node_id())
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard node",
                source: std::io::Error::other(format!(
                    "unknown local node {}",
                    location.node_id().as_u32()
                )),
            })?;
        Ok(node
            .data_dir()
            .join(format!("pg-{data_pg_id:04}"))
            .join("shards")
            .join(shard_key.hex_prefix())
            .join(shard_key.hex()))
    }

    #[cfg(test)]
    pub(crate) fn test_payload_shard_file_exists(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_index: u8,
    ) -> Result<bool, StoreError> {
        Ok(self
            .test_payload_shard_file_path(data_pg_id, ec, segment_okh, segment_vid, shard_index)?
            .exists())
    }

    fn emit_best_effort_payload_cleanup_error(&self, operation: &'static str, error: &StoreError) {
        let Some(trace) = observability::current_context() else {
            return;
        };
        self.maybe_observe_best_effort_payload_cleanup_error(operation, error);
        let _ = observability::event_in_context(
            &trace,
            TRACE_TARGET,
            "payload_cleanup_best_effort_error",
            Some(format_args!("operation={operation:?} error={error}")),
        );
    }

    fn emit_best_effort_payload_repair_error(&self, operation: &'static str, error: &StoreError) {
        let Some(trace) = observability::current_context() else {
            return;
        };
        let _ = observability::event_in_context(
            &trace,
            TRACE_TARGET,
            "payload_repair_best_effort_error",
            Some(format_args!("operation={operation:?} error={error}")),
        );
    }
}
