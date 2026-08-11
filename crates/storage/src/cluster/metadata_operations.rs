// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl StorageCluster {
    fn metadata_command_publication_order_key(
        node_id: NodeId,
        primary_node_id: NodeId,
        witness_node_id: Option<NodeId>,
    ) -> (u8, NodeId) {
        let publication_rank = if Some(node_id) == witness_node_id {
            0
        } else if node_id == primary_node_id {
            1
        } else {
            2
        };
        (publication_rank, node_id)
    }

    fn emit_metadata_command_conflict(
        &self,
        node_id: Option<NodeId>,
        pg_id: PgId,
        log_index: Option<u64>,
        kind: &'static str,
        command_kind: Option<&'static str>,
    ) {
        let _ = observability::emit_metadata_command_conflict(
            TRACE_TARGET,
            observability::MetadataCommandConflictSummary {
                node_id: node_id.map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index,
                kind,
                command_kind,
            },
        );
        if std::env::var_os("ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS").is_some() {
            let node = node_id
                .map(|node_id| node_id.as_u32().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let log = log_index
                .map(|log_index| log_index.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            eprintln!(
                "metadata command conflict source=frontend kind={kind} node_id={node} pg_id={} cluster_epoch={} log_index={log} command_kind={}",
                pg_id.get(),
                self.operation_epoch().get(),
                command_kind.unwrap_or("unknown")
            );
        }
    }

    fn emit_metadata_command_pending_slot_action(
        &self,
        node_id: Option<NodeId>,
        pg_id: PgId,
        log_index: Option<u64>,
        action: &'static str,
        command_kind: Option<&'static str>,
    ) {
        let _ = observability::emit_metadata_command_pending_slot_action(
            TRACE_TARGET,
            observability::MetadataCommandPendingSlotActionSummary {
                node_id: node_id.map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index,
                action,
                command_kind,
            },
        );
    }

    fn pending_slot_primary_node_id(&self, pg_id: PgId) -> Option<NodeId> {
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .ok()
            .map(|node| node.node_id())
    }

    fn emit_pending_slot_action_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        action: &'static str,
    ) {
        self.emit_metadata_command_pending_slot_action(
            self.pending_slot_primary_node_id(pg_id),
            pg_id,
            Some(command.id().log_index().get()),
            action,
            Some(command.payload().kind_name()),
        );
    }

    fn emit_metadata_command_recovery_admission_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        admission: observability::MetadataCommandRecoveryAdmissionKind,
        wait_us: u128,
    ) {
        let _ = observability::emit_metadata_command_recovery_admission(
            TRACE_TARGET,
            observability::MetadataCommandRecoveryAdmissionSummary {
                node_id: self
                    .pending_slot_primary_node_id(pg_id)
                    .map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index: Some(command.id().log_index().get()),
                admission,
                command_kind: Some(command.payload().kind_name()),
                wait_us,
            },
        );
    }

    fn emit_metadata_command_recovery_outcome_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        outcome: &'static str,
    ) {
        let _ = observability::emit_metadata_command_recovery_outcome(
            TRACE_TARGET,
            observability::MetadataCommandRecoveryOutcomeSummary {
                node_id: self
                    .pending_slot_primary_node_id(pg_id)
                    .map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index: Some(command.id().log_index().get()),
                outcome,
                command_kind: Some(command.payload().kind_name()),
            },
        );
    }

    fn metadata_command_conflict(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
    ) -> StoreError {
        self.emit_metadata_command_conflict(
            Some(node_id),
            pg_id,
            Some(log_index),
            "log_conflict",
            None,
        );
        StoreError::MetadataCommandLogConflict {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: self.operation_epoch(),
            log_index,
        }
    }

    fn metadata_command_log_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                pg_id,
                cluster_epoch,
                log_index,
                ..
            }) if *pg_id == command.id().pg_id().get()
                && *cluster_epoch == command.id().cluster_epoch()
                && *log_index == command.id().log_index().get()
        )
    }

    fn reserve_object_generation_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            (command.payload(), error),
            (
                MetadataCommandPayload::ReserveObjectGeneration(reservation),
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectGenerationReservationConflict {
                        reservation_id,
                        generation_id,
                    },
                ),
            ) if reservation.reservation_id.as_str() == reservation_id
                && reservation.generation_id.get() == *generation_id
        )
    }

    fn reserve_object_version_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            (command.payload(), error),
            (
                MetadataCommandPayload::ReserveObjectVersion(reservation),
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectVersionReservationConflict { version_id },
                ),
            ) if reservation.version_id == *version_id
        )
    }

    fn bucket_write_reservation_rejection_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return false;
        };
        matches!(
            error,
            BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { reservation_id }
                    | MetadataError::BucketWriteReservationNotFound { reservation_id }
            ) if *reservation_id == proof.reservation_id
        )
    }

    fn metadata_command_is_bucket_pg_command(command: &MetadataCommandEnvelope) -> bool {
        matches!(
            command.payload(),
            MetadataCommandPayload::CreateBucket(_)
                | MetadataCommandPayload::PutBucketVersioning(_)
                | MetadataCommandPayload::PutBucketAcl(_)
                | MetadataCommandPayload::PutBucketProperty(_)
                | MetadataCommandPayload::PutBucketSubresource(_)
                | MetadataCommandPayload::MarkBucketDeleting(_)
                | MetadataCommandPayload::DeleteFinalizedBucket(_)
                | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_)
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
            pg_id,
            command,
            applied_nodes,
            source,
            MetadataCommandRouteMode::Normal,
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.partial_exact_metadata_command_conflict_is_retryable_with_route_mode_and_deadline(
            pg_id,
            command,
            applied_nodes,
            source,
            route_mode,
            None,
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable_with_route_mode_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
        route_mode: MetadataCommandRouteMode,
        deadline: Instant,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.partial_exact_metadata_command_conflict_is_retryable_with_route_mode_and_deadline(
            pg_id,
            command,
            applied_nodes,
            source,
            route_mode,
            Some(deadline),
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable_with_route_mode_and_deadline(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
        route_mode: MetadataCommandRouteMode,
        deadline: Option<Instant>,
    ) -> Result<bool, BucketSnapshotLoadError> {
        fn entry_hashes_or_not_retryable(
            metadata_command_client: &dyn MetadataCommandInspectionNodeClient,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
            deadline: Option<Instant>,
        ) -> Result<Option<(u64, u64)>, BucketSnapshotLoadError> {
            let result = match deadline {
                Some(deadline) => metadata_command_client
                    .applied_metadata_command_log_entry_hashes_until(pg_id, command, deadline),
                None => metadata_command_client
                    .applied_metadata_command_log_entry_hashes(pg_id, command),
            };
            match result {
                Ok(hashes) => Ok(hashes),
                Err(StoreError::MetadataCommandLogConflict { .. }) => Ok(None),
                Err(error) => Err(error.into()),
            }
        }

        let BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            node_id: conflict_node_id,
            pg_id: conflict_pg_id,
            cluster_epoch,
            log_index,
        }) = source
        else {
            return Ok(false);
        };
        if *conflict_pg_id != command.id().pg_id().get()
            || *cluster_epoch != command.id().cluster_epoch()
            || *log_index != command.id().log_index().get()
        {
            return Ok(false);
        }

        let primary = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }?;
        let primary_node_id = primary.node_id();
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
        }?;
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
        let Some(conflict_index) = nodes
            .iter()
            .position(|node| node.node_id().as_u32() == *conflict_node_id)
        else {
            return Ok(false);
        };
        if conflict_index != applied_nodes {
            return Ok(false);
        }

        let primary_client = primary.metadata_command_inspection_client();
        let primary_state = match deadline {
            Some(deadline) => primary_client.metadata_command_replica_state_until(pg_id, deadline),
            None => primary_client.metadata_command_replica_state(pg_id),
        }?;
        let command_log_index = command.id().log_index().get();
        let expected_previous_log_hash =
            if command_log_index == primary_state.applied_log_index.saturating_add(1) {
                primary_state.applied_log_hash.value()
            } else if command_log_index == primary_state.applied_log_index {
                let Some((previous_log_hash, log_hash)) = entry_hashes_or_not_retryable(
                    primary.metadata_command_inspection_client().as_ref(),
                    pg_id,
                    command,
                    deadline,
                )?
                else {
                    return Ok(false);
                };
                if log_hash != primary_state.applied_log_hash.value() {
                    return Ok(false);
                }
                previous_log_hash
            } else {
                return Ok(false);
            };

        let mut expected_hashes = None;
        for (index, node) in nodes.into_iter().enumerate() {
            let hashes = entry_hashes_or_not_retryable(
                node.metadata_command_inspection_client().as_ref(),
                pg_id,
                command,
                deadline,
            )?;
            match (index <= conflict_index, hashes, expected_hashes) {
                (true, Some(hashes), None) if hashes.0 == expected_previous_log_hash => {
                    expected_hashes = Some(hashes)
                }
                (true, Some(hashes), Some(expected))
                    if hashes == expected && hashes.0 == expected_previous_log_hash => {}
                (true, _, _) => return Ok(false),
                (false, Some(hashes), Some(expected))
                    if hashes == expected && hashes.0 == expected_previous_log_hash => {}
                (false, Some(_), _) => return Ok(false),
                (false, None, _) => {}
            }
        }
        Ok(expected_hashes.is_some())
    }

    fn metadata_command_is_applied_on_all_acting_nodes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
            pg_id,
            command,
            MetadataCommandRouteMode::Normal,
        )
    }

    fn metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let mut expected_hashes = None;
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
        }?;
        for node in nodes {
            let hashes = match node
                .metadata_command_inspection_client()
                .applied_metadata_command_log_entry_hashes(pg_id, command)
            {
                Ok(Some(hashes)) => hashes,
                Ok(None) | Err(StoreError::MetadataCommandLogConflict { .. }) => {
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            };
            match expected_hashes {
                None => expected_hashes = Some(hashes),
                Some(expected) if hashes == expected => {}
                Some(_) => return Ok(false),
            }
        }
        Ok(expected_hashes.is_some())
    }

    fn metadata_command_has_exact_or_uncertain_applied_entry_on_acting_set_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        deadline: Instant,
    ) -> Result<bool, BucketSnapshotLoadError> {
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
        };
        let nodes = match nodes {
            Ok(nodes) => nodes,
            // This probe runs only after the caller's ordinary work budget has
            // expired. A stale/unavailable route cannot prove that no actor
            // durably published the exact command, so preserve the typed
            // convergence state instead of reclassifying it as contention.
            Err(_) => return Ok(true),
        };
        let mut exact_entry = None;
        let mut conflicting_node_id = None;
        let mut uncertain = false;
        for node in nodes {
            if Instant::now() >= deadline {
                uncertain = true;
                break;
            }
            #[cfg(test)]
            let observation = request_ops::maybe_run_post_budget_metadata_command_inspection_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                node.node_id(),
                deadline,
            )
            .unwrap_or_else(|| {
                node.metadata_command_inspection_client()
                    .applied_metadata_command_log_entry_hashes_until(pg_id, command, deadline)
            });
            #[cfg(not(test))]
            let observation = node
                .metadata_command_inspection_client()
                .applied_metadata_command_log_entry_hashes_until(pg_id, command, deadline);
            let hashes = match observation {
                Ok(Some(hashes)) => hashes,
                Ok(None) => continue,
                Err(StoreError::MetadataCommandLogConflict { .. }) => {
                    conflicting_node_id = Some(node.node_id());
                    continue;
                }
                Err(
                    error @ (StoreError::MetadataCommandLogChecksumMismatch { .. }
                    | StoreError::MetadataCommandLogHashMismatch { .. }
                    | StoreError::MetadataCommandReplicaStateEncodingVersion { .. }
                    | StoreError::MetadataCommandReplicaStateDiverged { .. }
                    | StoreError::MetadataStateDigestMismatch { .. }
                    | StoreError::MetadataCheckpointInvalid { .. }),
                ) => return Err(error.into()),
                Err(
                    error @ StoreError::StorageRpc {
                        failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandIntegrity,
                        ..
                    },
                ) => return Err(error.into()),
                Err(_) => {
                    uncertain = true;
                    continue;
                }
            };
            match exact_entry {
                None => exact_entry = Some((node.node_id(), hashes)),
                Some((_, expected)) if hashes == expected => {}
                Some((_, (expected_previous_log_hash, expected_log_hash))) => {
                    let (actual_previous_log_hash, actual_log_hash) = hashes;
                    return Err(StoreError::MetadataCommandLogHashMismatch {
                        node_id: node.node_id().as_u32(),
                        pg_id: pg_id.get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                        expected_previous_log_hash,
                        actual_previous_log_hash,
                        expected_log_hash,
                        actual_log_hash,
                    }
                    .into());
                }
            }
        }
        if let (Some((exact_node_id, _)), Some(conflicting_node_id)) =
            (exact_entry, conflicting_node_id)
        {
            return Err(MetadataError::InvariantViolation {
                context: "confirm metadata command publication after request budget exhaustion",
                reason: format!(
                    "acting set contains exact command on node {} and a conflicting command at the same PG/epoch/log index on node {}",
                    exact_node_id.as_u32(),
                    conflicting_node_id.as_u32()
                ),
            }
            .into());
        }
        Ok(exact_entry.is_some() || uncertain)
    }

    fn retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
    ) -> Result<Option<bool>, BucketSnapshotLoadError> {
        if !Self::metadata_command_log_conflict_matches(command, source)
            || !self.partial_exact_metadata_command_conflict_is_retryable(
                pg_id,
                command,
                applied_nodes,
                source,
            )?
        {
            return Ok(None);
        }
        self.metadata_command_is_applied_on_all_acting_nodes(pg_id, command)
            .map(Some)
    }

    fn exact_metadata_command_conflict_is_retryable(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        source: &BucketSnapshotLoadError,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            node_id: conflict_node_id,
            ..
        }) = source
        else {
            return Ok(false);
        };
        if !Self::metadata_command_log_conflict_matches(command, source) {
            return Ok(false);
        }

        let primary_node_id = self
            .local_map
            .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id)?
            .node_id();
        let mut nodes = self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)?;
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
        let Some(applied_nodes) = nodes
            .iter()
            .position(|node| node.node_id().as_u32() == *conflict_node_id)
        else {
            return Ok(false);
        };

        self.partial_exact_metadata_command_conflict_is_retryable(
            pg_id,
            command,
            applied_nodes,
            source,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn matching_reissued_pending_command_if_safe(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_metadata_client: &dyn MetadataCommandInspectionNodeClient,
        primary_max_log_index: u64,
        acting_set_max_log_index: u64,
        stale_command: &MetadataCommandEnvelope,
        current: MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        self.matching_reissued_pending_command_if_safe_with_route_mode(
            pg_id,
            primary_node_id,
            primary_metadata_client,
            primary_max_log_index,
            acting_set_max_log_index,
            stale_command.payload(),
            current,
            MetadataCommandRouteMode::Normal,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn matching_reissued_pending_command_if_safe_with_route_mode(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_metadata_client: &dyn MetadataCommandInspectionNodeClient,
        primary_max_log_index: u64,
        acting_set_max_log_index: u64,
        expected_payload: &MetadataCommandPayload,
        current: MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let payload_matches = current.payload() == expected_payload;
        let current_log_index = current.id().log_index().get();
        let primary_state = primary_metadata_client.metadata_command_replica_state(pg_id)?;
        if current_log_index == primary_state.applied_log_index {
            let Some((_, log_hash)) = primary_metadata_client
                .applied_metadata_command_log_entry_hashes(pg_id, &current)?
            else {
                return Err(self.metadata_command_conflict(
                    primary_node_id,
                    pg_id,
                    current_log_index,
                ));
            };
            if log_hash != primary_state.applied_log_hash.value() {
                return Err(self.metadata_command_conflict(
                    primary_node_id,
                    pg_id,
                    current_log_index,
                ));
            }
            if !payload_matches {
                return Ok(None);
            }
            return self.matching_terminal_pending_command_if_safe(
                pg_id,
                primary_node_id,
                primary_metadata_client,
                acting_set_max_log_index,
                &primary_state,
                current,
                route_mode,
            );
        }
        let primary_summary = ReissuedPendingCommandPrimarySummary {
            node_id: primary_node_id,
            max_log_index: primary_max_log_index,
            applied_log_index: primary_state.applied_log_index,
            applied_log_hash: primary_state.applied_log_hash.value(),
        };
        match decide_reissued_pending_command(
            primary_summary,
            acting_set_max_log_index,
            current_log_index,
            payload_matches,
            &[],
        ) {
            ReissuedPendingCommandDecision::StaleCommandDisplaced => return Ok(None),
            ReissuedPendingCommandDecision::Conflict { node_id, log_index } => {
                let _ = observability::event(
                    TRACE_TARGET,
                    "metadata_command_reissue_conflict",
                    Some(format_args!(
                        "pg_id={} node_id={:?} log_index={} primary_node_id={:?} primary_max={} primary_applied={} acting_set_max={} current_index={} payload_matches={} phase=primary",
                        pg_id.get(),
                        node_id,
                        log_index,
                        primary_node_id,
                        primary_max_log_index,
                        primary_state.applied_log_index,
                        acting_set_max_log_index,
                        current_log_index,
                        payload_matches,
                    )),
                );
                return Err(self.metadata_command_conflict(node_id, pg_id, log_index));
            }
            ReissuedPendingCommandDecision::ReloadCurrent => {}
        }
        let mut replicas = Vec::new();
        let route_epoch = match route_mode {
            MetadataCommandRouteMode::Normal => self.operation_epoch(),
            MetadataCommandRouteMode::Recovery => current.id().cluster_epoch(),
        };
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => {
                self.local_map.metadata_pg_acting_nodes(route_epoch, pg_id)
            }
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(route_epoch, pg_id),
        }?;
        for node in nodes {
            let node_max_log_index = node
                .metadata_command_inspection_client()
                .max_metadata_command_log_index(pg_id, route_epoch)?;
            let node_state = node
                .metadata_command_inspection_client()
                .metadata_command_replica_state(pg_id)?;
            let replacement_match = if node_max_log_index < current_log_index {
                ReissuedPendingCommandReplicaMatch::BelowReplacement
            } else if node
                .metadata_command_inspection_client()
                .has_matching_applied_metadata_command_log_entry(
                    pg_id,
                    &current,
                    primary_state.applied_log_hash.value(),
                )?
            {
                ReissuedPendingCommandReplicaMatch::MatchesHashChain
            } else {
                ReissuedPendingCommandReplicaMatch::MissingOrMismatched
            };
            replicas.push(ReissuedPendingCommandReplicaSummary {
                node_id: node.node_id(),
                max_log_index: node_max_log_index,
                applied_log_index: node_state.applied_log_index,
                applied_log_hash: node_state.applied_log_hash.value(),
                replacement_match,
            });
        }
        match decide_reissued_pending_command(
            primary_summary,
            acting_set_max_log_index,
            current_log_index,
            payload_matches,
            &replicas,
        ) {
            ReissuedPendingCommandDecision::StaleCommandDisplaced => Ok(None),
            ReissuedPendingCommandDecision::ReloadCurrent => Ok(Some(current)),
            ReissuedPendingCommandDecision::Conflict { node_id, log_index } => {
                let _ = observability::event(
                    TRACE_TARGET,
                    "metadata_command_reissue_conflict",
                    Some(format_args!(
                        "pg_id={} node_id={:?} log_index={} primary_node_id={:?} primary_max={} primary_applied={} acting_set_max={} current_index={} payload_matches={} phase=replica",
                        pg_id.get(),
                        node_id,
                        log_index,
                        primary_node_id,
                        primary_max_log_index,
                        primary_state.applied_log_index,
                        acting_set_max_log_index,
                        current_log_index,
                        payload_matches,
                    )),
                );
                Err(self.metadata_command_conflict(node_id, pg_id, log_index))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn matching_terminal_pending_command_if_safe(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_metadata_client: &dyn MetadataCommandInspectionNodeClient,
        acting_set_max_log_index: u64,
        primary_state: &MetadataCommandReplicaState,
        current: MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let current_log_index = current.id().log_index().get();
        let Some(previous_log_index) = current_log_index.checked_sub(1) else {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        };
        if acting_set_max_log_index > current_log_index {
            return Err(self.metadata_command_conflict(
                primary_node_id,
                pg_id,
                acting_set_max_log_index,
            ));
        }
        let Some((previous_log_hash, terminal_log_hash)) =
            primary_metadata_client.applied_metadata_command_log_entry_hashes(pg_id, &current)?
        else {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        };
        if terminal_log_hash != primary_state.applied_log_hash.value() {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        }

        let route_epoch = match route_mode {
            MetadataCommandRouteMode::Normal => self.operation_epoch(),
            MetadataCommandRouteMode::Recovery => current.id().cluster_epoch(),
        };
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => {
                self.local_map.metadata_pg_acting_nodes(route_epoch, pg_id)
            }
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(route_epoch, pg_id),
        }?;
        for node in nodes {
            let (node_max_log_index, node_state) = if node.node_id() == primary_node_id {
                (
                    primary_metadata_client.max_metadata_command_log_index(pg_id, route_epoch)?,
                    primary_metadata_client.metadata_command_replica_state(pg_id)?,
                )
            } else {
                let metadata_client = node.metadata_command_inspection_client();
                (
                    metadata_client.max_metadata_command_log_index(pg_id, route_epoch)?,
                    metadata_client.metadata_command_replica_state(pg_id)?,
                )
            };
            if node_max_log_index > current_log_index {
                return Err(self.metadata_command_conflict(
                    node.node_id(),
                    pg_id,
                    node_max_log_index,
                ));
            }
            if node_max_log_index < current_log_index {
                if node_max_log_index != previous_log_index
                    || node_state.applied_log_index != previous_log_index
                    || node_state.applied_log_hash.value() != previous_log_hash
                {
                    return Err(self.metadata_command_conflict(
                        node.node_id(),
                        pg_id,
                        previous_log_index.max(node_max_log_index),
                    ));
                }
                continue;
            }
            let matches_applied = if node.node_id() == primary_node_id {
                primary_metadata_client.has_matching_applied_metadata_command_log_entry(
                    pg_id,
                    &current,
                    previous_log_hash,
                )?
            } else {
                node.metadata_command_inspection_client()
                    .has_matching_applied_metadata_command_log_entry(
                        pg_id,
                        &current,
                        previous_log_hash,
                    )?
            };
            if !matches_applied {
                return Err(self.metadata_command_conflict(
                    node.node_id(),
                    pg_id,
                    current_log_index,
                ));
            }
        }
        Ok(Some(current))
    }

    fn reissue_pending_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        self.reissue_pending_metadata_command_with_route_mode(
            pg_id,
            command,
            MetadataCommandExecutionRoute::normal(),
            command.payload(),
        )
    }

    fn reissue_pending_metadata_command_with_route_mode(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        execution_route: MetadataCommandExecutionRoute<'_>,
        replacement_payload: &MetadataCommandPayload,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        execution_route.require_reissue_source(pg_id, command, replacement_payload)?;
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source;
        let recovery_abandoned_source = execution_route.recovery_abandoned_source;
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = command.bucket_name().clone();
        let primary = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }?;
        let route_epoch = match route_mode {
            MetadataCommandRouteMode::Normal => self.operation_epoch(),
            MetadataCommandRouteMode::Recovery => command.id().cluster_epoch(),
        };
        self.emit_metadata_command_pending_slot_action(
            Some(primary.node_id()),
            pg_id,
            Some(command.id().log_index().get()),
            "reissue_attempt",
            Some(command.payload().kind_name()),
        );
        let primary_metadata_client = primary.metadata_command_recovery_client();
        let acting_set_max_log_index = self
            .max_metadata_command_log_index_on_acting_set_with_route_mode(
                pg_id,
                route_mode,
                route_epoch,
            )?;

        enum ReissueReplaceOutcome {
            Replaced(MetadataCommandEnvelope),
            Reload {
                current: MetadataCommandEnvelope,
                primary_max_log_index: u64,
            },
            Missing,
        }

        let replace_outcome = {
            let primary_critical_section = primary_metadata_client
                .open_metadata_command_recovery_critical_section(pg_id, route_epoch)?;
            let primary_max_log_index =
                primary_critical_section.max_metadata_command_log_index()?;
            let Some(current) = primary_critical_section.pending_metadata_command_envelope()?
            else {
                return Ok(None);
            };
            if current != *command || acting_set_max_log_index > primary_max_log_index {
                ReissueReplaceOutcome::Reload {
                    current,
                    primary_max_log_index,
                }
            } else {
                let next_log_index = primary_max_log_index
                    .max(command.id().log_index().get())
                    .checked_add(1)
                    .and_then(MetadataCommandLogIndex::new)
                    .ok_or(StoreError::MetadataCommandLogConflict {
                        node_id: primary.node_id().as_u32(),
                        pg_id: pg_id.get(),
                        cluster_epoch: route_epoch,
                        log_index: u64::MAX,
                    })?;
                let replacement = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(route_epoch, pg_id, next_log_index),
                    replacement_payload.clone(),
                );
                let replaced = match recovery_authorized_source {
                    None => primary_critical_section
                        .replace_pending_metadata_command_slot_for_reissue(
                            command,
                            &replacement,
                            Some(&bucket),
                        ),
                    Some(authorized_source) => primary_critical_section
                        .replace_pending_metadata_command_slot_for_recovery(
                            authorized_source,
                            recovery_abandoned_source,
                            command,
                            &replacement,
                            Some(&bucket),
                        ),
                }?;
                if replaced {
                    ReissueReplaceOutcome::Replaced(replacement)
                } else {
                    let current = primary_critical_section.pending_metadata_command_envelope()?;
                    let primary_max_log_index =
                        primary_critical_section.max_metadata_command_log_index()?;
                    match current {
                        Some(current) => ReissueReplaceOutcome::Reload {
                            current,
                            primary_max_log_index,
                        },
                        None => ReissueReplaceOutcome::Missing,
                    }
                }
            }
        };
        match replace_outcome {
            ReissueReplaceOutcome::Replaced(replacement) => Ok(Some(replacement)),
            ReissueReplaceOutcome::Missing => Ok(None),
            ReissueReplaceOutcome::Reload {
                current,
                primary_max_log_index,
            } => {
                let acting_set_max_log_index = self
                    .max_metadata_command_log_index_on_acting_set_with_route_mode(
                        pg_id,
                        route_mode,
                        route_epoch,
                    )?;
                self.matching_reissued_pending_command_if_safe_with_route_mode(
                    pg_id,
                    primary.node_id(),
                    primary.metadata_command_inspection_client().as_ref(),
                    primary_max_log_index,
                    acting_set_max_log_index,
                    replacement_payload,
                    current,
                    route_mode,
                )
                .map_err(BucketSnapshotLoadError::from)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_reissue_pending_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        self.reissue_pending_metadata_command(pg_id, command)
    }

    fn max_metadata_command_log_index_on_acting_set_with_route_mode(
        &self,
        pg_id: PgId,
        route_mode: MetadataCommandRouteMode,
        route_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let mut max_log_index = 0;
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => {
                self.local_map.metadata_pg_acting_nodes(route_epoch, pg_id)
            }
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(route_epoch, pg_id),
        }?;
        for node in nodes {
            let metadata_client = node.metadata_command_inspection_client();
            max_log_index = max_log_index
                .max(metadata_client.max_metadata_command_log_index(pg_id, route_epoch)?);
        }
        Ok(max_log_index)
    }

    #[allow(dead_code)]
    pub(crate) fn reconstruct_pg_peering_from_retained_metadata_log(
        &self,
        pg_id: PgId,
        primary: NodeId,
    ) -> Result<PgPeeringReconstructionDecision, PgPeeringReconstructionFailure> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let primary_index = nodes
            .iter()
            .position(|node| node.node_id() == primary)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;

        let mut min_applied_log_index = u64::MAX;
        let mut primary_applied_log_index = None;
        let mut replicas = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let metadata_client = node.metadata_command_inspection_client();
            let state = metadata_client.metadata_command_replica_state(pg_id)?;
            min_applied_log_index = min_applied_log_index.min(state.applied_log_index);
            if node.node_id() == primary {
                primary_applied_log_index = Some(state.applied_log_index);
            }
            let has_pending_metadata_command = metadata_client
                .pending_metadata_command_envelope(pg_id, self.operation_epoch())?
                .is_some();
            replicas.push(PgPeeringReplicaReconstructionInput {
                node_id: node.node_id(),
                state,
                has_pending_metadata_command,
                retained_log_hashes: Vec::new(),
            });
        }

        let primary_applied_log_index = primary_applied_log_index
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;
        if min_applied_log_index < primary_applied_log_index {
            let first_log_index = MetadataCommandLogIndex::new(min_applied_log_index + 1)
                .expect("lagging metadata command log index range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(primary_applied_log_index)
                .expect("primary applied log index must be nonzero when a replica is behind");
            replicas[primary_index].retained_log_hashes = nodes[primary_index]
                .metadata_command_inspection_client()
                .retained_metadata_command_log_hashes(
                    pg_id,
                    self.operation_epoch(),
                    first_log_index,
                    last_log_index,
                )?;
        }

        Ok(reconstruct_pg_peering_from_primary_retained_log(
            self.operation_epoch(),
            pg_id,
            primary,
            &replicas,
        )?)
    }

    #[allow(dead_code)]
    pub(crate) fn replay_pg_peering_catchup_from_retained_metadata_log(
        &self,
        pg_id: PgId,
        primary: NodeId,
    ) -> Result<PgPeeringReconstructionDecision, PgPeeringReconstructionFailure> {
        let decision = self.reconstruct_pg_peering_from_retained_metadata_log(pg_id, primary)?;
        let PgPeeringReconstructionDecision::CatchUpRequired { replicas, .. } = decision else {
            return Ok(decision);
        };

        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_replay(self.operation_epoch(), pg_id)?;
        let primary_node = nodes
            .iter()
            .find(|node| node.node_id() == primary)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;
        let first_log_index = replicas
            .iter()
            .map(|replica| replica.from_log_index + 1)
            .min()
            .expect("catch-up decision contains at least one replica");
        let last_log_index = replicas
            .iter()
            .map(|replica| replica.to_log_index)
            .max()
            .expect("catch-up decision contains at least one replica");
        let mut retained_log_entries = Vec::new();
        let mut batch_start = first_log_index;
        while batch_start <= last_log_index {
            let batch_end = last_log_index
                .min(batch_start + STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1);
            let first_log_index = MetadataCommandLogIndex::new(batch_start)
                .expect("catch-up retained entry range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(batch_end)
                .expect("catch-up retained entry range ends after zero");
            retained_log_entries.extend(
                primary_node
                    .metadata_command_inspection_client()
                    .retained_metadata_command_log_entries(
                        pg_id,
                        self.operation_epoch(),
                        first_log_index,
                        last_log_index,
                    )?,
            );
            batch_start = batch_end + 1;
        }

        let replay_plans = build_pg_peering_replay_plan_from_retained_log_entries(
            &replicas,
            &retained_log_entries,
        )?;
        for replay_plan in replay_plans {
            let target_node = nodes
                .iter()
                .find(|node| node.node_id() == replay_plan.node_id)
                .ok_or(PgPeeringReconstructionError::ReplayTargetMissing {
                    node_id: replay_plan.node_id,
                })?;
            let metadata_client = target_node.metadata_command_peering_client();
            let peering_route = metadata_client
                .open_metadata_command_peering_route(pg_id, self.operation_epoch())?;
            for command in replay_plan.commands {
                peering_route.replay_metadata_command_for_peering(&command)?;
            }
        }

        self.reconstruct_pg_peering_from_retained_metadata_log(pg_id, primary)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_retained_log(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_inspection_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        let has_pending_metadata_command = metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some();

        let mut retained_log_entries = Vec::new();
        let mut batch_start = 1;
        while batch_start <= state.applied_log_index {
            let batch_end = state
                .applied_log_index
                .min(batch_start + STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1);
            let first_log_index = MetadataCommandLogIndex::new(batch_start)
                .expect("metadata transfer retained entry range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(batch_end)
                .expect("metadata transfer retained entry range ends after zero");
            retained_log_entries.extend(metadata_client.retained_metadata_command_log_entries(
                pg_id,
                state.cluster_epoch,
                first_log_index,
                last_log_index,
            )?);
            batch_start = batch_end + 1;
        }

        Ok(
            build_pg_metadata_transfer_artifact_from_retained_log_entries(
                state.cluster_epoch,
                pg_id,
                source_node_id,
                state,
                has_pending_metadata_command,
                retained_log_entries,
            )?,
        )
    }

    #[cfg(test)]
    pub(crate) fn export_pg_metadata_transfer_artifact_from_retained_log(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id)
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_checkpoint(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_inspection_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        let checkpoint = metadata_client.metadata_command_checkpoint(pg_id, state.cluster_epoch)?;
        let proof = PgMetadataProof {
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash,
            state_digest: checkpoint.state_digest,
        };
        Ok(PgMetadataTransferArtifact {
            pg_id,
            source_node_id,
            cluster_epoch: checkpoint.cluster_epoch,
            base_kind: PgMetadataTransferBaseKind::Checkpoint,
            base_proof: proof,
            checkpoint_base: Some(checkpoint),
            proof,
            retained_log_entries: Vec::new(),
        })
    }

    fn metadata_transfer_source_state(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<MetadataCommandReplicaState, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let state = source_node
            .metadata_command_inspection_client()
            .metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        Ok(state)
    }

    fn metadata_transfer_source_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, PgPeeringReconstructionFailure> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        Ok(source_node
            .metadata_command_inspection_client()
            .metadata_command_checkpoint_candidates(
                pg_id,
                source_state.cluster_epoch,
                max_applied_log_index,
                limit,
            )?)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        checkpoint: MetadataCommandCheckpoint,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        checkpoint.verify().map_err(|_| {
            PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                node_id: source_node_id,
                pg_id,
                checkpoint: PgMetadataProof {
                    applied_log_index: checkpoint.applied_log_index,
                    applied_log_hash: checkpoint.applied_log_hash,
                    state_digest: checkpoint.state_digest,
                },
                expected: PgMetadataProof {
                    applied_log_index: checkpoint.applied_log_index,
                    applied_log_hash: checkpoint.applied_log_hash,
                    state_digest: checkpoint.state_digest,
                },
            }
        })?;
        let checkpoint_proof = PgMetadataProof {
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash,
            state_digest: checkpoint.state_digest,
        };
        if checkpoint.pg_id != pg_id {
            return Err(
                PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                    node_id: source_node_id,
                    pg_id,
                    checkpoint: checkpoint_proof,
                    expected: checkpoint_proof,
                }
                .into(),
            );
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_inspection_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        if state.cluster_epoch != checkpoint.cluster_epoch {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: checkpoint.cluster_epoch,
            }
            .into());
        }
        if metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some()
        {
            return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                node_id: source_node_id,
            }
            .into());
        }

        let mut retained_log_entries = Vec::new();
        if let Some(mut batch_start) = checkpoint.applied_log_index.checked_add(1) {
            while batch_start <= state.applied_log_index {
                let batch_end =
                    state.applied_log_index.min(batch_start.saturating_add(
                        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1,
                    ));
                let first_log_index = MetadataCommandLogIndex::new(batch_start)
                    .expect("metadata transfer checkpoint suffix range starts after checkpoint");
                let last_log_index = MetadataCommandLogIndex::new(batch_end)
                    .expect("metadata transfer checkpoint suffix range ends after checkpoint");
                retained_log_entries.extend(
                    metadata_client.retained_metadata_command_log_entries(
                        pg_id,
                        state.cluster_epoch,
                        first_log_index,
                        last_log_index,
                    )?,
                );
                if batch_end == u64::MAX {
                    break;
                }
                batch_start = batch_end + 1;
            }
        }

        let artifact = PgMetadataTransferArtifact {
            pg_id,
            source_node_id,
            cluster_epoch: checkpoint.cluster_epoch,
            base_kind: PgMetadataTransferBaseKind::Checkpoint,
            base_proof: checkpoint_proof,
            checkpoint_base: Some(checkpoint),
            proof: PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            },
            retained_log_entries,
        };
        rebase_pg_metadata_transfer_artifact_commands(&artifact, self.operation_epoch())?;
        Ok(artifact)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        checkpoints: impl IntoIterator<Item = MetadataCommandCheckpoint>,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        match self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id) {
            Ok(artifact) => return Ok(artifact),
            Err(error) if retained_log_export_failure_allows_checkpoint_fallback(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let source_state = self
            .metadata_transfer_source_state(pg_id, source_node_id)
            .map_err(PgMetadataTransferError::from)?;
        self.export_pg_metadata_transfer_artifact_from_checkpoint_candidates(
            pg_id,
            source_node_id,
            &source_state,
            checkpoints,
        )
    }

    fn export_pg_metadata_transfer_artifact_from_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        checkpoints: impl IntoIterator<Item = MetadataCommandCheckpoint>,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        let mut candidates: Vec<_> = checkpoints.into_iter().collect();
        candidates.sort_by(|left, right| {
            right
                .applied_log_index
                .cmp(&left.applied_log_index)
                .then_with(|| {
                    right
                        .applied_log_hash
                        .value()
                        .cmp(&left.applied_log_hash.value())
                })
        });

        for checkpoint in candidates {
            if let Some(artifact) = self
                .try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
                    pg_id,
                    source_node_id,
                    source_state,
                    checkpoint,
                )?
            {
                return Ok(artifact);
            }
        }

        self.export_pg_metadata_transfer_from_checkpoint(pg_id, source_node_id)
            .map_err(Into::into)
    }

    fn try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        checkpoint: MetadataCommandCheckpoint,
    ) -> Result<Option<PgMetadataTransferArtifact>, PgMetadataTransferError> {
        if checkpoint.pg_id != pg_id
            || checkpoint.cluster_epoch != source_state.cluster_epoch
            || checkpoint.applied_log_index > source_state.applied_log_index
        {
            return Ok(None);
        }
        if checkpoint.verify().is_err() {
            return Ok(None);
        }
        match self.export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            source_node_id,
            checkpoint,
        ) {
            Ok(artifact) => Ok(Some(artifact)),
            Err(PgPeeringReconstructionFailure::Reconstruction(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn export_pg_metadata_transfer_artifact_from_paged_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        let mut max_applied_log_index = source_state.applied_log_index;
        for _ in 0..STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES {
            let Some(checkpoint) = self
                .metadata_transfer_source_checkpoint_candidates(
                    pg_id,
                    source_node_id,
                    source_state,
                    max_applied_log_index,
                    1,
                )
                .map_err(PgMetadataTransferError::from)?
                .into_iter()
                .next()
            else {
                break;
            };
            let next_max_applied_log_index = checkpoint.applied_log_index.checked_sub(1);
            if let Some(artifact) = self
                .try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
                    pg_id,
                    source_node_id,
                    source_state,
                    checkpoint,
                )?
            {
                return Ok(artifact);
            }
            let Some(next_max_applied_log_index) = next_max_applied_log_index else {
                break;
            };
            max_applied_log_index = next_max_applied_log_index;
        }
        self.export_pg_metadata_transfer_from_checkpoint(pg_id, source_node_id)
            .map_err(Into::into)
    }

    pub(crate) fn export_pg_metadata_transfer_artifact_for_live_transfer(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        match self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id) {
            Ok(artifact)
                if artifact.source_base_kind() != PgMetadataTransferBaseKind::RetainedLogPrefix =>
            {
                return Ok(artifact);
            }
            Ok(_) => {}
            Err(error) if retained_log_export_failure_allows_checkpoint_fallback(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let source_state = self
            .metadata_transfer_source_state(pg_id, source_node_id)
            .map_err(PgMetadataTransferError::from)?;
        self.export_pg_metadata_transfer_artifact_from_paged_checkpoint_candidates(
            pg_id,
            source_node_id,
            &source_state,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn import_pg_metadata_transfer_from_retained_log(
        &self,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, PgPeeringReconstructionFailure> {
        let pg_id = artifact.pg_id;
        let commands =
            rebase_pg_metadata_transfer_artifact_commands(artifact, self.operation_epoch())?;
        let expected_import_proof =
            metadata_transfer_destination_proof(artifact, &commands, self.operation_epoch());
        let base_import_proof = artifact.source_base_metadata_proof();
        let checkpoint_base = artifact.checkpoint_base();
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_replay(self.operation_epoch(), pg_id)?;

        let mut reference: Option<(NodeId, PgMetadataProof)> = None;
        for node in nodes {
            let metadata_client = node.metadata_command_peering_client();
            let peering_route = metadata_client
                .open_metadata_command_peering_route(pg_id, self.operation_epoch())?;
            let state = if let Some(checkpoint) = checkpoint_base {
                let checkpoint_destination_base_proof = PgMetadataProof {
                    applied_log_index: 0,
                    applied_log_hash: crate::control_plane::MetadataCommandLogHash::genesis(),
                    state_digest: checkpoint.state_digest,
                };
                if metadata_client
                    .pending_metadata_command_envelope(pg_id, self.operation_epoch())?
                    .is_some()
                {
                    return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                        node_id: node.node_id(),
                    }
                    .into());
                }
                let current = metadata_client.metadata_command_replica_state(pg_id)?;
                if current.cluster_epoch != self.operation_epoch()
                    && metadata_client
                        .pending_metadata_command_envelope(pg_id, current.cluster_epoch)?
                        .is_some()
                {
                    return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                        node_id: node.node_id(),
                    }
                    .into());
                }
                let current_proof = PgMetadataProof {
                    applied_log_index: current.applied_log_index,
                    applied_log_hash: current.applied_log_hash,
                    state_digest: current.state_digest,
                };
                if current.cluster_epoch == self.operation_epoch() {
                    if let Some(prefix_len) = checkpoint_import_resume_prefix_len(
                        pg_id,
                        checkpoint_destination_base_proof,
                        expected_import_proof,
                        current_proof,
                        &commands,
                        self.operation_epoch(),
                    ) {
                        let validated = peering_route
                            .validate_metadata_command_replay_state_preserving_pending_slot()?;
                        let validated_proof = PgMetadataProof {
                            applied_log_index: validated.applied_log_index,
                            applied_log_hash: validated.applied_log_hash,
                            state_digest: validated.state_digest,
                        };
                        let Some(validated_prefix_len) = checkpoint_import_resume_prefix_len(
                            pg_id,
                            checkpoint_destination_base_proof,
                            expected_import_proof,
                            validated_proof,
                            &commands,
                            self.operation_epoch(),
                        ) else {
                            return Err(PgPeeringReconstructionError::MetadataFork {
                                node_id: node.node_id(),
                                reference_node_id: node.node_id(),
                                replica: validated_proof,
                                reference: expected_import_proof,
                            }
                            .into());
                        };
                        if validated_prefix_len != prefix_len {
                            return Err(PgPeeringReconstructionError::MetadataFork {
                                node_id: node.node_id(),
                                reference_node_id: node.node_id(),
                                replica: validated_proof,
                                reference: current_proof,
                            }
                            .into());
                        }
                        let mut state = validated;
                        for command in &commands[prefix_len..] {
                            state = peering_route
                                .replay_metadata_command_for_peering(&command.command)?;
                        }
                        state
                    } else if metadata_client.metadata_command_replica_state_can_initialize(
                        pg_id,
                        self.operation_epoch(),
                    )? {
                        let mut state =
                            peering_route.install_metadata_transfer_checkpoint_base(checkpoint)?;
                        for command in &commands {
                            state = peering_route
                                .replay_metadata_command_for_peering(&command.command)?;
                        }
                        state
                    } else {
                        return Err(
                            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                                node_id: node.node_id(),
                                pg_id,
                                cluster_epoch: self.operation_epoch(),
                                applied_log_index: current.applied_log_index,
                                applied_log_hash: current.applied_log_hash.value(),
                                state_digest: current.state_digest.value(),
                                expected: expected_import_proof,
                            }
                            .into(),
                        );
                    }
                } else {
                    let mut state =
                        peering_route.install_metadata_transfer_checkpoint_base(checkpoint)?;
                    for command in &commands {
                        state =
                            peering_route.replay_metadata_command_for_peering(&command.command)?;
                    }
                    state
                }
            } else {
                match classify_metadata_transfer_import_destination(
                    metadata_client.as_ref(),
                    node.node_id(),
                    pg_id,
                    self.operation_epoch(),
                    base_import_proof,
                    expected_import_proof,
                    &commands,
                )? {
                    MetadataTransferImportDestination::AlreadyImported(state) => state,
                    MetadataTransferImportDestination::Empty => {
                        if commands.is_empty() {
                            peering_route.initialize_metadata_transfer_empty_state(
                                artifact.proof.state_digest,
                            )?
                        } else {
                            let mut state =
                                metadata_client.metadata_command_replica_state(pg_id)?;
                            for command in &commands {
                                state = peering_route
                                    .replay_metadata_command_for_peering(&command.command)?;
                            }
                            state
                        }
                    }
                    MetadataTransferImportDestination::AdoptBase => {
                        peering_route.initialize_metadata_transfer_matching_state(
                            0,
                            MetadataCommandLogHash::genesis(),
                            base_import_proof.state_digest,
                        )?;
                        let mut state = metadata_client.metadata_command_replica_state(pg_id)?;
                        for command in &commands {
                            state = peering_route
                                .replay_metadata_command_for_peering(&command.command)?;
                        }
                        state
                    }
                    MetadataTransferImportDestination::AdoptExisting => {
                        if commands.is_empty() {
                            peering_route.initialize_metadata_transfer_matching_state(
                                expected_import_proof.applied_log_index,
                                expected_import_proof.applied_log_hash,
                                expected_import_proof.state_digest,
                            )?
                        } else {
                            peering_route.adopt_metadata_transfer_state_from_rebased_commands(
                                &commands,
                                artifact.proof.state_digest,
                            )?
                        }
                    }
                    MetadataTransferImportDestination::AdoptPrefix { prefix_len } => {
                        if prefix_len > 0 {
                            peering_route.adopt_metadata_transfer_state_from_rebased_commands(
                                &commands[..prefix_len],
                                commands[prefix_len - 1].post_state_digest,
                            )?;
                        }
                        let mut state = metadata_client.metadata_command_replica_state(pg_id)?;
                        for command in &commands[prefix_len..] {
                            state = peering_route
                                .replay_metadata_command_for_peering(&command.command)?;
                        }
                        state
                    }
                }
            };
            if state.cluster_epoch != self.operation_epoch() {
                return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                    node_id: node.node_id(),
                    replica_epoch: state.cluster_epoch,
                    cluster_epoch: self.operation_epoch(),
                }
                .into());
            }
            let proof = PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            };
            if let Some((reference_node_id, reference_proof)) = reference {
                if proof != reference_proof {
                    return Err(PgPeeringReconstructionError::MetadataFork {
                        node_id: node.node_id(),
                        reference_node_id,
                        replica: proof,
                        reference: reference_proof,
                    }
                    .into());
                }
            } else {
                reference = Some((node.node_id(), proof));
            }
        }

        let (_reference_node_id, proof) =
            reference.ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: artifact.source_node_id,
            })?;
        if proof != expected_import_proof {
            return Err(PgPeeringReconstructionError::MetadataFork {
                node_id: artifact.source_node_id,
                reference_node_id: artifact.source_node_id,
                replica: proof,
                reference: expected_import_proof,
            }
            .into());
        }
        Ok(proof)
    }

    pub(crate) fn import_pg_metadata_transfer_artifact_from_retained_log(
        &self,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, PgMetadataTransferError> {
        self.import_pg_metadata_transfer_from_retained_log(artifact)
            .map_err(Into::into)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_stream_abort_storage_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_stream_abort_storage
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_retained_stream_abort_hook(&self) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_retained_stream_abort
            .clone();
        hook.map_or(Ok(()), |hook| hook())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_retained_stream_cleanup_capability_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_retained_stream_cleanup_capability
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_after_retained_stream_cleanup_capability_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_retained_stream_cleanup_capability
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_metadata_command_pending_install_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn maybe_run_after_metadata_command_drain_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_metadata_command_drain
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(test))]
    fn maybe_run_after_metadata_command_drain_hook(&self) {}

    #[cfg(test)]
    fn maybe_run_after_multipart_create_upload_id_prepared_hook(&self, upload_id: &UploadId) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_multipart_create_upload_id_prepared
            .clone();
        if let Some(hook) = hook {
            hook(upload_id);
        }
    }

    #[cfg(not(test))]
    fn maybe_run_after_multipart_create_upload_id_prepared_hook(&self, _upload_id: &UploadId) {}

    #[cfg(test)]
    fn maybe_run_before_multipart_create_command_install_hook(
        &self,
        command: &MetadataCommandEnvelope,
    ) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_multipart_create_command_install
            .clone();
        if let Some(hook) = hook {
            hook(command);
        }
    }

    #[cfg(not(test))]
    fn maybe_run_before_multipart_create_command_install_hook(
        &self,
        _command: &MetadataCommandEnvelope,
    ) {
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_after_metadata_listing_pg_complete_hook(&self, pg_id: u32) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_metadata_listing_pg_complete
            .clone();
        if let Some(hook) = hook {
            hook(pg_id);
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_reclaim_ownership_lookup_hook(&self) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_reclaim_ownership_lookup
            .clone();
        hook.map_or(Ok(()), |hook| hook())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_reclaim_ownership_lookup_hook(&self) -> Result<(), ObjectPgActionError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_after_reclaim_claim_acquired_hook(&self) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_reclaim_claim_acquired
            .clone();
        hook.map_or(Ok(()), |hook| hook())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_after_reclaim_claim_acquired_hook(&self) -> Result<(), ObjectPgActionError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_reclaim_claim_release_hook(&self) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_reclaim_claim_release
            .clone();
        hook.map_or(Ok(()), |hook| hook())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_reclaim_claim_release_hook(&self) -> Result<(), ObjectPgActionError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_direct_put_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_direct_put_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_direct_put_command_id_hook(&self) {}

    #[cfg(test)]
    fn maybe_run_before_direct_put_abandoned_log_inspection_hook(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_direct_put_abandoned_log_inspection
            .clone();
        hook.map_or(Ok(()), |hook| hook(command))
    }

    #[cfg(not(test))]
    fn maybe_run_before_direct_put_abandoned_log_inspection_hook(
        &self,
        _command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_object_generation_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_object_generation_command_id_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_object_version_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_object_version_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_object_version_command_id_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_stream_append_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_stream_append_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_stream_append_command_id_hook(&self) {}

    #[cfg(test)]
    fn maybe_run_after_stream_append_command_id_allocated_hook(
        &self,
        command_id: MetadataCommandId,
    ) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_stream_append_command_id_allocated
            .clone();
        if let Some(hook) = hook {
            hook(command_id);
        }
    }

    #[cfg(not(test))]
    fn maybe_run_after_stream_append_command_id_allocated_hook(
        &self,
        _command_id: MetadataCommandId,
    ) {
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_after_object_metadata_reservation_acquired_hook(
        &self,
    ) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired
            .clone();
        if let Some(hook) = hook {
            hook()?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_after_object_metadata_reservation_acquired_hook(
        &self,
    ) -> Result<(), ObjectPgActionError> {
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_metadata_command_pending_install_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_placed_payload_shard_delete_hook(
        &self,
        shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_delete
            .clone();
        if let Some(hook) = hook {
            hook(shard_key)?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_placed_payload_shard_delete_hook(
        &self,
        _shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_placed_payload_shard_write_hook(
        &self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_write
            .clone();
        if let Some(hook) = hook {
            hook(&location, shard_key).map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_placed_payload_shard_write_hook(
        &self,
        _location: ShardLocation,
        _shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_placed_payload_shard_read_hook(
        &self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_read
            .clone();
        if let Some(hook) = hook {
            hook(&location, shard_key).map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_placed_payload_shard_read_hook(
        &self,
        _location: ShardLocation,
        _shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_metadata_primary_payload_ack_delete_hook(
        &self,
        shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_metadata_primary_payload_ack_delete
            .clone();
        if let Some(hook) = hook {
            hook(shard_key)?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_metadata_primary_payload_ack_delete_hook(
        &self,
        _shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_observe_best_effort_payload_cleanup_error(
        &self,
        operation: &'static str,
        error: &StoreError,
    ) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .best_effort_payload_cleanup_error
            .clone();
        if let Some(hook) = hook {
            hook(operation, error);
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_observe_best_effort_payload_cleanup_error(
        &self,
        _operation: &'static str,
        _error: &StoreError,
    ) {
    }

    /// Return the opaque durable identity for this static route authority.
    ///
    /// The representation remains storage-owned; callers can only pass it to
    /// the standalone durable-binding API.
    pub fn standalone_route_identity(
        &self,
    ) -> Result<crate::StandaloneRouteIdentity, crate::StandaloneRouteIdentityError> {
        match self.route_authority {
            StorageClusterRouteAuthority::Static(proof) => {
                Ok(crate::StandaloneRouteIdentity(proof.content_digest.0))
            }
            StorageClusterRouteAuthority::Dynamic(_) => {
                Err(crate::StandaloneRouteIdentityError::DynamicAuthority)
            }
        }
    }

    /// Derive the opaque durable identity for an embedded static topology
    /// before opening any placement-group store.
    ///
    /// Directory preparation and canonicalization are included because their
    /// resulting paths are routing inputs. No database is opened and no
    /// recovery is performed by this operation.
    pub fn prepare_standalone_embedded_topology(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
    ) -> Result<PreparedStandaloneEmbeddedTopology, ClusterBuildError> {
        let configs = configs.into_iter().collect::<Vec<_>>();
        let route_identity = LocalClusterMap::preflight_static_embedded_route_digest(
            metadata_primary_node_id,
            configs.clone(),
            pg_ids,
            default_ec_shape,
            cluster_epoch,
        )
        .map(crate::StandaloneRouteIdentity)?;
        Ok(PreparedStandaloneEmbeddedTopology {
            metadata_primary_node_id,
            configs,
            pg_ids: pg_ids.into(),
            default_ec_shape,
            cluster_epoch,
            route_identity,
        })
    }

    pub fn open_static_local_nodes(
        data_dir: &std::path::Path,
        node_ids: &[NodeId],
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = Arc::new(LocalClusterMap::open(
            data_dir,
            node_ids,
            pg_ids,
            default_ec_shape,
        )?);
        Self::from_static_local_map(local_map)
    }

    /// Construct an immutable static-authority cluster from a complete local
    /// topology. Dynamic runtime-map generations use [`Self::from_runtime_map`]
    /// or one of its transport-specific variants instead.
    pub fn from_static_local_map(
        local_map: Arc<LocalClusterMap>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let operation_epoch = local_map.epoch();
        let route_authority = StorageClusterRouteAuthority::static_for(&local_map)?;
        Self::from_local_map_with_epoch_and_authority(local_map, operation_epoch, route_authority)
    }

    pub fn from_runtime_map(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = Arc::new(
            LocalClusterMap::open_frontend_topology_only_with_runtime_map(
                metadata_primary_node_id,
                runtime_map,
                default_ec_shape,
            )?,
        );
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, runtime_map)?;
        Self::from_local_map_with_epoch_and_authority(
            local_map,
            runtime_map.cluster_epoch(),
            route_authority,
        )
    }

    pub(crate) fn from_runtime_local_map(
        local_map: Arc<LocalClusterMap>,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, runtime_map)?;
        Self::from_local_map_with_epoch_and_authority(
            local_map,
            runtime_map.cluster_epoch(),
            route_authority,
        )
    }

    pub fn from_runtime_map_with_unix_storage_node_clients(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        )
    }

    pub fn unix_storage_node_client_configs_from_runtime_map(
        runtime_map: &ClusterRuntimeMapSnapshot,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Vec<LocalUnixStorageNodeClientConfig> {
        runtime_map
            .nodes()
            .iter()
            .map(|node| {
                LocalUnixStorageNodeClientConfig::with_rpc_admission_settings_from_runtime_node_route(
                    node,
                    admission_settings,
                )
            })
            .collect()
    }

    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            None,
        )
    }

    fn from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_auth_and_process_state(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            rpc_auth,
            None,
        )
    }

    fn from_runtime_map_with_unix_storage_node_client_admission_settings_auth_and_process_state(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
        process_local_state_source: Option<&StorageCluster>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
        )?;
        if let Some(source) = process_local_state_source {
            local_map.inherit_process_local_state_from(&source.local_map);
        }
        let storage_node_configs = Self::unix_storage_node_client_configs_from_runtime_map(
            runtime_map,
            admission_settings,
        )
        .into_iter()
        .map(|config| config.with_optional_rpc_auth(rpc_auth.clone()));
        local_map.install_unix_storage_node_clients(storage_node_configs)?;
        local_map.validate_installed_storage_rpc_clients(runtime_map)?;
        let local_map = Arc::new(local_map);
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, runtime_map)?;
        Self::from_local_map_with_epoch_authority_and_auth(
            local_map,
            runtime_map.cluster_epoch(),
            route_authority,
            rpc_auth,
        )
    }

    pub fn from_runtime_map_with_storage_rpc_endpoints_and_frontend_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<
            Item = (
                NodeId,
                crate::storage_rpc_transport::StorageRpcClientEndpoint,
            ),
        >,
        capability: crate::FrontendStorageRpcClientCapability,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            endpoints,
            capability.into(),
        )
    }

    pub fn from_runtime_map_with_storage_rpc_endpoints_and_maintenance_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<
            Item = (
                NodeId,
                crate::storage_rpc_transport::StorageRpcClientEndpoint,
            ),
        >,
        capability: crate::MaintenanceStorageRpcClientCapability,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            endpoints,
            capability.into(),
        )
    }

    /// Construct a maintenance-authenticated view which shares process-local
    /// leases, recovery coordination, and work queues with a foreground view.
    pub fn from_runtime_map_with_storage_rpc_endpoints_and_maintenance_auth_sharing_process_state(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<
            Item = (
                NodeId,
                crate::storage_rpc_transport::StorageRpcClientEndpoint,
            ),
        >,
        capability: crate::MaintenanceStorageRpcClientCapability,
        process_local_state_source: &StorageCluster,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        StorageClusterRouteAuthority::dynamic_for(
            &process_local_state_source.local_map,
            runtime_map,
        )?;
        Self::from_runtime_map_with_storage_rpc_endpoints_auth_and_process_state(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            endpoints,
            capability.into(),
            Some(process_local_state_source),
        )
    }

    fn from_runtime_map_with_storage_rpc_endpoints_and_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<
            Item = (
                NodeId,
                crate::storage_rpc_transport::StorageRpcClientEndpoint,
            ),
        >,
        rpc_auth: crate::StorageRpcClientAuthConfig,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_storage_rpc_endpoints_auth_and_process_state(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            endpoints,
            rpc_auth,
            None,
        )
    }

    fn from_runtime_map_with_storage_rpc_endpoints_auth_and_process_state(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<
            Item = (
                NodeId,
                crate::storage_rpc_transport::StorageRpcClientEndpoint,
            ),
        >,
        rpc_auth: crate::StorageRpcClientAuthConfig,
        process_local_state_source: Option<&StorageCluster>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let mut endpoint_map = BTreeMap::new();
        for (node_id, endpoint) in endpoints {
            if endpoint_map.insert(node_id, endpoint).is_some() {
                return Err(ClusterBuildError::DuplicateRemoteStorageNodeClientNodeId {
                    id: node_id.as_u32(),
                });
            }
        }
        let endpoints = Arc::new(endpoint_map);
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
        )?;
        if let Some(source) = process_local_state_source {
            local_map.inherit_process_local_state_from(&source.local_map);
        }
        let configs = endpoints.iter().map(|(&node_id, endpoint)| {
            LocalUnixStorageNodeClientConfig::with_rpc_endpoint_and_admission_settings(
                node_id,
                endpoint.clone(),
                admission_settings,
            )
            .with_optional_rpc_auth(Some(rpc_auth.clone()))
        });
        local_map.install_unix_storage_node_clients(configs)?;
        local_map.validate_installed_storage_rpc_clients(runtime_map)?;
        let local_map = Arc::new(local_map);
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, runtime_map)?;
        let mut cluster = Self::from_local_map_with_epoch_authority_and_auth(
            local_map,
            runtime_map.cluster_epoch(),
            route_authority,
            Some(rpc_auth),
        )?;
        Arc::get_mut(&mut cluster)
            .expect("new storage cluster has one owner")
            .rpc_endpoints = Some(endpoints);
        Ok(cluster)
    }

    fn historical_recovery_cluster_with_storage_rpc_clients(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        if let Some(endpoints) = &self.rpc_endpoints {
            let rpc_auth = self
                .rpc_auth
                .clone()
                .ok_or(ClusterBuildError::ResolvedStorageRpcEndpointsRequireAuthentication)?;
            return Self::from_runtime_map_with_storage_rpc_endpoints_auth_and_process_state(
                self.metadata_node_id(),
                runtime_map,
                self.default_payload_ec_shape(),
                admission_settings,
                endpoints
                    .iter()
                    .map(|(&node_id, endpoint)| (node_id, endpoint.clone())),
                rpc_auth,
                Some(self),
            );
        }

        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_auth_and_process_state(
            self.metadata_node_id(),
            runtime_map,
            self.default_payload_ec_shape(),
            admission_settings,
            self.rpc_auth.clone(),
            Some(self),
        )
    }

    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings_and_frontend_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        capability: crate::FrontendStorageRpcClientCapability,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            Some(capability.into()),
        )
    }

    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings_and_maintenance_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        capability: crate::MaintenanceStorageRpcClientCapability,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            Some(capability.into()),
        )
    }

    /// Unix-transport equivalent of the process-state-sharing maintenance
    /// constructor above.
    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings_and_maintenance_auth_sharing_process_state(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        capability: crate::MaintenanceStorageRpcClientCapability,
        process_local_state_source: &StorageCluster,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        StorageClusterRouteAuthority::dynamic_for(
            &process_local_state_source.local_map,
            runtime_map,
        )?;
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_auth_and_process_state(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            Some(capability.into()),
            Some(process_local_state_source),
        )
    }

    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings_and_storage_node_auth(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        capability: crate::StorageNodeStorageRpcClientCapability,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            admission_settings,
            Some(capability.into()),
        )
    }

    pub fn refresh_from_control_plane_runtime_map(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
    ) -> Result<Arc<Self>, StorageClusterRuntimeMapRefreshError> {
        self.require_dynamic_route_authority()?;
        let runtime_map = control_plane.runtime_map_snapshot(authority_now_ms)?;
        let local_map = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
            &self.local_map,
            &runtime_map,
        )?;
        let local_map = Arc::new(local_map);
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, &runtime_map)?;
        Ok(
            Self::from_local_map_with_epoch_authority_auth_and_owner_token(
                local_map,
                runtime_map.cluster_epoch(),
                route_authority,
                self.rpc_auth.clone(),
                Some(Arc::clone(&self.bucket_write_owner_token)),
            )?,
        )
    }

    pub fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<Self>, StorageClusterRuntimeMapRefreshError> {
        self.require_dynamic_route_authority()?;
        let runtime_map = control_plane.runtime_map_snapshot(authority_now_ms)?;
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            self.metadata_node_id(),
            &runtime_map,
            self.default_payload_ec_shape(),
        )?;
        local_map.inherit_process_local_state_from(&self.local_map);
        let storage_node_configs = match &self.rpc_endpoints {
            Some(endpoints) => endpoints
                .iter()
                .map(|(&node_id, endpoint)| {
                    LocalUnixStorageNodeClientConfig::with_rpc_endpoint_and_admission_settings(
                        node_id,
                        endpoint.clone(),
                        admission_settings,
                    )
                    .with_optional_rpc_auth(self.rpc_auth.clone())
                })
                .collect::<Vec<_>>(),
            None => Self::unix_storage_node_client_configs_from_runtime_map(
                &runtime_map,
                admission_settings,
            )
            .into_iter()
            .map(|config| config.with_optional_rpc_auth(self.rpc_auth.clone()))
            .collect(),
        };
        local_map.install_unix_storage_node_clients(storage_node_configs)?;
        local_map.validate_installed_storage_rpc_clients(&runtime_map)?;
        let local_map = Arc::new(local_map);
        let route_authority = StorageClusterRouteAuthority::dynamic_for(&local_map, &runtime_map)?;
        let mut cluster = Self::from_local_map_with_epoch_authority_auth_and_owner_token(
            local_map,
            runtime_map.cluster_epoch(),
            route_authority,
            self.rpc_auth.clone(),
            Some(Arc::clone(&self.bucket_write_owner_token)),
        )?;
        Arc::get_mut(&mut cluster)
            .expect("new storage cluster has one owner")
            .rpc_endpoints = self.rpc_endpoints.clone();
        Ok(cluster)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_from_local_map_with_epoch(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let route_authority = if local_map.route_map_validity() == RouteMapValidity::Forever {
            StorageClusterRouteAuthority::static_for(&local_map)?
        } else {
            StorageClusterRouteAuthority::test_dynamic_for(&local_map)?
        };
        Self::from_local_map_with_epoch_and_authority(local_map, operation_epoch, route_authority)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_clone_with_dynamic_route_map_validity(
        &self,
        validity: RouteMapValidity,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = Arc::new(self.local_map.test_clone_with_route_map_validity(validity));
        let route_authority = StorageClusterRouteAuthority::test_dynamic_for(&local_map)?;
        let mut cluster = Self::from_local_map_with_epoch_authority_auth_and_owner_token(
            local_map,
            self.operation_epoch,
            route_authority,
            self.rpc_auth.clone(),
            Some(Arc::clone(&self.bucket_write_owner_token)),
        )?;
        Arc::get_mut(&mut cluster)
            .expect("new test storage cluster has one owner")
            .rpc_endpoints = self.rpc_endpoints.clone();
        Ok(cluster)
    }

    fn from_local_map_with_epoch_and_authority(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
        route_authority: StorageClusterRouteAuthority,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_local_map_with_epoch_authority_and_auth(
            local_map,
            operation_epoch,
            route_authority,
            None,
        )
    }

    fn from_local_map_with_epoch_authority_and_auth(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
        route_authority: StorageClusterRouteAuthority,
        rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_local_map_with_epoch_authority_auth_and_owner_token(
            local_map,
            operation_epoch,
            route_authority,
            rpc_auth,
            None,
        )
    }

    fn from_local_map_with_epoch_authority_auth_and_owner_token(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
        route_authority: StorageClusterRouteAuthority,
        rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
        bucket_write_owner_token: Option<Arc<str>>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let bucket_write_owner_token = match bucket_write_owner_token {
            Some(token) => token,
            None => random_hex_identifier("bucket-write-owner-")
                .map(Arc::from)
                .map_err(|_| ClusterBuildError::RuntimeIdentityGeneration)?,
        };
        Ok(Arc::new(Self {
            local_map,
            operation_epoch,
            route_authority,
            bucket_write_owner_token,
            rpc_auth,
            rpc_endpoints: None,
            #[cfg(any(test, feature = "test-hooks"))]
            test_hooks: Arc::new(Mutex::new(StorageClusterTestHooks::default())),
        }))
    }

    pub fn cluster_epoch(&self) -> crate::ClusterEpoch {
        self.local_map.epoch()
    }

    pub fn operation_epoch(&self) -> ClusterEpoch {
        self.operation_epoch
    }

    pub(super) fn current_route_effect_fence(&self) -> AdmittedRouteEffectFence {
        let lease = self.local_map.route_map_lease_snapshot();
        match (
            lease.validity.valid_until_ms(),
            lease.local_valid_until_monotonic_ms,
        ) {
            (Some(authority_valid_until_ms), Some(local_valid_until_monotonic_ms)) => {
                AdmittedRouteEffectFence::bounded(
                    self.operation_epoch,
                    authority_valid_until_ms,
                    local_valid_until_monotonic_ms,
                )
            }
            (None, None) => AdmittedRouteEffectFence::unbounded(self.operation_epoch),
            _ => unreachable!("route-map deadline representations must agree"),
        }
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.local_map.route_map_valid_until_ms()
    }

    pub fn route_map_validity(&self) -> RouteMapValidity {
        self.local_map.route_map_validity()
    }

    fn require_dynamic_route_authority(&self) -> Result<(), StorageClusterRuntimeMapRefreshError> {
        self.route_authority.dynamic_proof().map(|_| ())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_store_route_map_validity(&self, validity: RouteMapValidity) {
        self.local_map.test_store_route_map_validity(validity);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_store_route_map_lease(
        &self,
        validity: RouteMapValidity,
        local_valid_until_monotonic_ms: Option<u64>,
    ) {
        self.local_map
            .test_store_route_map_lease(validity, local_valid_until_monotonic_ms);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize {
        self.local_map
            .runtime_state()
            .test_bucket_delete_finalize_outstanding_depth()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_reclaim_outstanding_depth(&self) -> usize {
        self.local_map
            .runtime_state()
            .test_object_payload_reclaim_outstanding_depth()
    }

    fn replace_route_map_lease(
        &self,
        validity: RouteMapValidity,
        bound_lease: Option<BoundRouteMapLease>,
    ) {
        self.local_map
            .replace_route_map_lease(validity, bound_lease);
    }

    #[cfg(test)]
    pub(crate) fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StoreError> {
        self.local_map.require_route_map_valid_at(now_ms)
    }

    pub(crate) fn require_route_map_valid_now(&self) -> Result<(), StoreError> {
        self.local_map.require_route_map_valid_now()
    }

    fn require_current_payload_operation_epoch(&self, pg_id: u32) -> Result<(), StoreError> {
        let current_epoch = self.cluster_epoch();
        if self.operation_epoch() != current_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id,
                operation_epoch: self.operation_epoch(),
                current_epoch,
            });
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn require_current_metadata_primary_bridge_epoch(&self) -> Result<(), StoreError> {
        let current_epoch = self.cluster_epoch();
        if self.operation_epoch() != current_epoch {
            return Err(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: self.metadata_node_id().as_u32(),
                operation_epoch: self.operation_epoch(),
                current_epoch,
            });
        }
        Ok(())
    }

    // Transitional metadata-primary test hook bridge. Production metadata paths
    // must use routed PG primaries.
    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_primary_bridge_node(&self) -> Result<&SharedStorageNode, StoreError> {
        self.require_current_metadata_primary_bridge_epoch()?;
        Ok(self.local_map.metadata_primary().test_node().as_ref())
    }

    fn try_install_pending_metadata_command_for_bucket_with_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<bool, ObjectPgActionError> {
        self.maybe_run_before_metadata_command_pending_install_hook();
        Ok(self
            .try_set_pending_metadata_command_for_bucket_with_effect_fence(
                pg_id,
                bucket,
                command,
                effect_fence,
            )
            .map_err(ObjectPgActionError::from)?
            .is_some())
    }

    #[cfg(test)]
    fn try_set_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<()>, StoreError> {
        self.try_set_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id, bucket, command, None,
        )
    }

    fn try_set_pending_metadata_command_for_bucket_with_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<Option<()>, StoreError> {
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.try_set_pending_metadata_command_for_bucket_locked_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        )
    }

    fn try_set_pending_metadata_command_for_bucket_locked_with_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<Option<()>, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_client = primary.metadata_command_client();
        let result = match effect_fence {
            Some(effect_fence) => metadata_client
                .try_insert_pending_metadata_command_slot_with_effect_fence(
                    pg_id,
                    command,
                    Some(bucket),
                    effect_fence,
                ),
            None => metadata_client.try_insert_pending_metadata_command_slot(
                pg_id,
                command,
                Some(bucket),
            ),
        };
        match result {
            Ok(()) => Ok(Some(())),
            Err(StoreError::MetadataCommandPendingConflict { .. }) => {
                self.emit_metadata_command_conflict(
                    Some(primary.node_id()),
                    pg_id,
                    Some(command.id().log_index().get()),
                    "pending_slot_conflict",
                    Some(command.payload().kind_name()),
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn try_install_object_pg_pending_command_with_fresh_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        effect_fence: Option<AdmittedRouteEffectFence>,
        build_command: impl FnOnce(MetadataCommandId) -> MetadataCommandEnvelope,
    ) -> Result<ObjectPgPendingCommandInstall, ObjectPgActionError> {
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            return Ok(ObjectPgPendingCommandInstall::Pending(command));
        }
        let command_id = match self
            .next_object_metadata_command_id_with_completion_admission(pg_id, completion_admission)
        {
            Ok(command_id) => command_id,
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                return Ok(ObjectPgPendingCommandInstall::LogConflict {
                    pending_visible: self
                        .pending_metadata_command_for_bucket(pg_id, bucket)?
                        .is_some(),
                });
            }
            Err(error) => return Err(error),
        };
        let command = build_command(command_id);
        match self.try_set_pending_metadata_command_for_bucket_locked_with_effect_fence(
            pg_id,
            bucket,
            &command,
            effect_fence,
        ) {
            Ok(Some(())) => Ok(ObjectPgPendingCommandInstall::Installed(command)),
            Ok(None) => {
                let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
                    return Err(conflicting_pending_object_metadata_command(
                        "pending slot conflicted without visible command",
                    ));
                };
                Ok(ObjectPgPendingCommandInstall::Pending(pending))
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                Ok(ObjectPgPendingCommandInstall::LogConflict {
                    pending_visible: self
                        .pending_metadata_command_for_bucket(pg_id, bucket)?
                        .is_some(),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn drain_after_object_pg_log_conflict(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        pending_visible: bool,
    ) -> Result<(), ObjectPgActionError> {
        if pending_visible {
            self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
        }
        Ok(())
    }

    fn install_snapshot_sensitive_metadata_command_or_drain(
        &self,
        publisher: impl crate::metadata_command::SnapshotSensitiveMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<SnapshotSensitiveInstallOutcome, ObjectPgActionError> {
        match self.try_install_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(true) => Ok(SnapshotSensitiveInstallOutcome::Installed),
            Ok(false)
            | Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                Ok(SnapshotSensitiveInstallOutcome::ContenderDrained)
            }
            Err(error) => Err(error),
        }
    }

    fn install_allocator_cleanup_metadata_command_with_fresh_id(
        &self,
        publisher: impl crate::metadata_command::AllocatorCleanupMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        effect_fence: Option<AdmittedRouteEffectFence>,
        build_command: impl FnOnce(MetadataCommandId) -> MetadataCommandEnvelope,
    ) -> Result<AllocatorCleanupFreshInstallOutcome, ObjectPgActionError> {
        match self.try_install_object_pg_pending_command_with_fresh_id(
            pg_id,
            bucket,
            completion_admission,
            effect_fence,
            build_command,
        )? {
            ObjectPgPendingCommandInstall::Installed(command) => Ok(
                AllocatorCleanupFreshInstallOutcome::Installed(Box::new(command)),
            ),
            ObjectPgPendingCommandInstall::Pending(command) => {
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                Ok(AllocatorCleanupFreshInstallOutcome::PendingContenderDrained)
            }
            ObjectPgPendingCommandInstall::LogConflict { pending_visible } => {
                self.drain_after_object_pg_log_conflict(publisher, pg_id, bucket, pending_visible)?;
                Ok(AllocatorCleanupFreshInstallOutcome::LogConflictHandled)
            }
        }
    }

    fn install_allocator_cleanup_pending_command_or_drain(
        &self,
        publisher: impl crate::metadata_command::AllocatorCleanupMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<AllocatorCleanupPendingInstallOutcome, ObjectPgActionError> {
        if self.try_set_object_pg_pending_command_or_drain(
            publisher,
            pg_id,
            bucket,
            command,
            effect_fence,
        )? {
            Ok(AllocatorCleanupPendingInstallOutcome::Installed)
        } else {
            Ok(AllocatorCleanupPendingInstallOutcome::RetryAfterContention)
        }
    }

    fn install_terminal_session_retry_metadata_command(
        &self,
        _publisher: impl crate::metadata_command::TerminalSessionRetryMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        is_matching: impl FnOnce(&MetadataCommandEnvelope) -> bool,
    ) -> Result<TerminalSessionRetryInstallOutcome, ObjectPgActionError> {
        match self.try_install_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(true) => Ok(TerminalSessionRetryInstallOutcome::Installed),
            Ok(false)
            | Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                match self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    Some(pending) if is_matching(&pending) => Ok(
                        TerminalSessionRetryInstallOutcome::MatchingContenderVisible(Box::new(
                            pending,
                        )),
                    ),
                    Some(_) => Ok(TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible),
                    None => Ok(TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn install_matching_outcome_retry_metadata_command(
        &self,
        _publisher: impl crate::metadata_command::MatchingOutcomeRetryMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        is_matching: impl FnOnce(&MetadataCommandEnvelope) -> bool,
    ) -> Result<MatchingOutcomeRetryInstallOutcome, ObjectPgActionError> {
        match self.try_install_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(true) => Ok(MatchingOutcomeRetryInstallOutcome::Installed),
            Ok(false)
            | Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                match self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    Some(pending) if is_matching(&pending) => Ok(
                        MatchingOutcomeRetryInstallOutcome::MatchingContenderVisible(Box::new(
                            pending,
                        )),
                    ),
                    Some(_) => Ok(MatchingOutcomeRetryInstallOutcome::UnrelatedContenderVisible),
                    None => Ok(MatchingOutcomeRetryInstallOutcome::ContentionWithoutVisibleCommand),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn install_apply_validated_metadata_command_with_fresh_id(
        &self,
        publisher: impl crate::metadata_command::ApplyValidatedMetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        effect_fence: Option<AdmittedRouteEffectFence>,
        build_command: impl FnOnce(MetadataCommandId) -> MetadataCommandEnvelope,
    ) -> Result<ApplyValidatedFreshInstallOutcome, ObjectPgActionError> {
        match self.try_install_object_pg_pending_command_with_fresh_id(
            pg_id,
            bucket,
            completion_admission,
            effect_fence,
            build_command,
        )? {
            ObjectPgPendingCommandInstall::Installed(command) => Ok(
                ApplyValidatedFreshInstallOutcome::Installed(Box::new(command)),
            ),
            ObjectPgPendingCommandInstall::Pending(command) => {
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                Ok(ApplyValidatedFreshInstallOutcome::PendingContenderDrained)
            }
            ObjectPgPendingCommandInstall::LogConflict { pending_visible } => {
                self.drain_after_object_pg_log_conflict(publisher, pg_id, bucket, pending_visible)?;
                Ok(ApplyValidatedFreshInstallOutcome::LogConflictHandled)
            }
        }
    }

    fn try_set_object_pg_pending_command_or_drain(
        &self,
        publisher: impl crate::metadata_command::MetadataCommandPublisher,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<bool, ObjectPgActionError> {
        match self.try_set_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(Some(())) => Ok(true),
            Ok(None) | Err(StoreError::MetadataCommandLogConflict { .. }) => {
                self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        self.pending_metadata_command_for_bucket_with_route_mode(
            pg_id,
            bucket,
            MetadataCommandRouteMode::Normal,
            self.operation_epoch(),
        )
    }

    fn pending_metadata_command_for_bucket_with_route_mode(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        route_mode: MetadataCommandRouteMode,
        route_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let _ = bucket;
        let primary = match route_mode {
            MetadataCommandRouteMode::Normal => {
                self.local_map.metadata_pg_primary_node(route_epoch, pg_id)
            }
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(route_epoch, pg_id),
        }?;
        primary
            .metadata_command_client()
            .pending_metadata_command_envelope(pg_id, route_epoch)
    }

    fn drain_pending_metadata_commands_for_current_map(
        &self,
    ) -> Result<usize, ObjectPgActionError> {
        let mut drained = 0usize;
        for raw_pg_id in self.local_map.pg_ids() {
            let pg_id = PgId::new(*raw_pg_id);
            let primary = self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    self.operation_epoch(),
                    pg_id,
                )
                .map_err(ObjectPgActionError::Store)?;
            let Some(command) = primary
                .metadata_command_client()
                .pending_metadata_command_envelope(pg_id, self.operation_epoch())
                .map_err(ObjectPgActionError::Store)?
            else {
                continue;
            };
            let _outcome =
                self.drain_pending_metadata_command_with_local_recovery_route(pg_id, &command)?;
            drained += 1;
        }
        Ok(drained)
    }

    fn remove_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, StoreError> {
        self.remove_pending_metadata_command_for_bucket_inner(pg_id, bucket, command, None)
    }

    fn remove_pending_metadata_command_for_bucket_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, StoreError> {
        self.remove_pending_metadata_command_for_bucket_inner(
            pg_id,
            bucket,
            command,
            Some(work_budget),
        )
    }

    fn remove_pending_metadata_command_for_bucket_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: Option<&mut RequestWorkBudget>,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, StoreError> {
        let _ = bucket;
        request_ops::remove_pending_metadata_command_slot_after_terminal_outcome(
            pg_id,
            work_budget,
            || {
                self.local_map
                    .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
                    .metadata_command_client()
                    .remove_pending_metadata_command_slot(pg_id, command)
            },
        )
    }

    fn remove_pending_metadata_command_for_bucket_recovery(
        &self,
        execution_route: MetadataCommandExecutionRoute<'_>,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<request_ops::PendingMetadataCommandTerminalCleanup, StoreError> {
        execution_route.require_command(pg_id, command)?;
        execution_route.require_recovery_predecessor(pg_id)?;
        let _ = bucket;
        request_ops::remove_pending_metadata_command_slot_after_terminal_outcome(
            pg_id,
            Some(work_budget),
            || {
                self.local_map
                    .metadata_pg_primary_node_for_metadata_command_recovery(
                        self.operation_epoch(),
                        pg_id,
                    )?
                    .metadata_command_client()
                    .remove_pending_metadata_command_slot(pg_id, command)
            },
        )
    }

    fn next_metadata_command_id(&self, pg_id: PgId) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least(
            pg_id,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    fn next_completion_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_completion_metadata_command_id_at_least(
            pg_id,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        primary
            .metadata_command_client()
            .next_metadata_command_id_at_least(pg_id, self.operation_epoch(), min_log_index)
    }

    fn next_completion_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        primary
            .metadata_command_client()
            .next_completion_metadata_command_id_at_least(
                pg_id,
                self.operation_epoch(),
                min_log_index,
            )
    }

    #[cfg(test)]
    fn next_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_from_locked_pg_at_least(
            pg_id,
            pg,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    #[cfg(test)]
    fn next_metadata_command_id_from_locked_pg_at_least(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let max_log_index = pg.max_metadata_command_log_index(self.operation_epoch())?;
        if let Some(slot) =
            pg.pending_metadata_command_slot(primary.node_id().as_u32(), self.operation_epoch())?
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id: primary.node_id().as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
                log_index: slot.id.log_index().get(),
            });
        }
        let next_log_index = max_log_index
            .checked_add(1)
            .map(|next| next.max(min_log_index.get()))
            .and_then(MetadataCommandLogIndex::new)
            .ok_or(StoreError::MetadataCommandLogConflict {
                node_id: primary.node_id().as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
                log_index: u64::MAX,
            })?;
        Ok(MetadataCommandId::new(
            self.operation_epoch(),
            pg_id,
            next_log_index,
        ))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_primary_test_hook_node(&self) -> &SharedStorageNode {
        self.metadata_primary_bridge_node()
            .expect("test hook requires a current storage cluster handle")
    }

    pub fn metadata_node_id(&self) -> NodeId {
        self.local_map.metadata_primary_node_id()
    }

    pub fn local_node_count(&self) -> usize {
        self.local_map.node_count()
    }

    pub fn local_node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.local_map.node_ids()
    }

    #[cfg(test)]
    pub(crate) fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        self.local_map.cluster_map_history_reference_summary()
    }

    pub fn local_pg_route(&self, pg_id: PgId) -> Option<&LocalPgRoute> {
        self.local_map.pg_route(pg_id)
    }

    pub fn local_pg_routes(&self) -> impl Iterator<Item = &LocalPgRoute> + '_ {
        self.local_map.pg_routes()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_historical_pg_routes(&self) -> impl Iterator<Item = &PgRouteSnapshot> + '_ {
        self.local_map.test_historical_pg_routes()
    }

    #[cfg(test)]
    pub(crate) fn test_route_authority_digest_matches_local_map(&self) -> bool {
        let local_digest = self.local_map.static_route_map_content_digest();
        match self.route_authority {
            StorageClusterRouteAuthority::Static(proof) => proof.content_digest.0 == local_digest,
            StorageClusterRouteAuthority::Dynamic(proof) => {
                proof.content_digest == RuntimeMapContentDigest::from_bytes(local_digest)
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_clone_with_pg_routes(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
        historical_pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = self.local_map.test_clone_with_pg_routes(
            cluster_epoch,
            pg_routes,
            historical_pg_routes,
        )?;
        self.test_clone_with_local_map(local_map, cluster_epoch)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_clone_with_stale_current_pg_routes_from_snapshots(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
        historical_pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
        stale_current_pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let mut local_map = self.local_map.test_clone_with_pg_routes(
            cluster_epoch,
            pg_routes,
            historical_pg_routes,
        )?;
        local_map.test_install_pg_routes(stale_current_pg_routes);
        self.test_clone_with_local_map(local_map, cluster_epoch)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_clone_with_local_map(
        &self,
        local_map: LocalClusterMap,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let route_authority = match self.route_authority {
            StorageClusterRouteAuthority::Static(_) => {
                StorageClusterRouteAuthority::static_for(&local_map)?
            }
            StorageClusterRouteAuthority::Dynamic(_) => {
                local_map.test_store_route_map_validity(self.route_map_validity());
                StorageClusterRouteAuthority::test_dynamic_for(&local_map)?
            }
        };
        let mut cluster = Self::from_local_map_with_epoch_authority_auth_and_owner_token(
            Arc::new(local_map),
            cluster_epoch,
            route_authority,
            self.rpc_auth.clone(),
            Some(Arc::clone(&self.bucket_write_owner_token)),
        )?;
        Arc::get_mut(&mut cluster)
            .expect("new test storage cluster has one owner")
            .rpc_endpoints = self.rpc_endpoints.clone();
        Ok(cluster)
    }

    pub(crate) fn reconstructed_pg_route_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<PgRouteSnapshot, StoreError> {
        self.local_map
            .reconstructed_pg_route_at_epoch(pg_id, cluster_epoch)
            .ok_or(StoreError::HistoricalPgRouteNotRetained {
                pg_id: pg_id.get(),
                cluster_epoch,
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_pg_metadata_proof(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<PgMetadataProof, StoreError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let state = primary
            .metadata_command_client()
            .metadata_command_replica_state(pg_id)?;
        Ok(PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        })
    }

    /// Temporary process-local registry key for shared coordinator workers.
    ///
    /// Multiple `StorageCluster` handles backed by the same local node keep
    /// sharing process-local workers until a real cluster identity exists.
    pub fn process_local_registry_key(&self) -> ProcessLocalRegistryKey {
        self.local_map.process_local_registry_key()
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_pg_lock_ptr(&self, pg_id: PgId) -> usize {
        self.local_map
            .runtime_state()
            .test_metadata_command_pg_lock_ptr(pg_id)
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_recovery_flight_count(&self) -> usize {
        self.local_map
            .runtime_state()
            .test_metadata_command_recovery_flight_count()
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_recovery_wait_hook(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        timeout_selected: Arc<std::sync::Barrier>,
        retry_selected: Arc<std::sync::Barrier>,
    ) {
        self.local_map
            .runtime_state()
            .test_install_metadata_command_recovery_wait_hook(
                pg_id,
                command,
                timeout_selected,
                retry_selected,
            );
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_recovery_owner_completion_hook(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        owner_release_selected: Arc<std::sync::Barrier>,
    ) {
        self.local_map
            .runtime_state()
            .test_install_metadata_command_recovery_owner_completion_hook(
                pg_id,
                command,
                owner_release_selected,
            );
    }

    #[cfg(test)]
    pub(crate) fn test_take_metadata_command_recovery_wait_hook_observation(
        &self,
    ) -> (usize, usize) {
        self.local_map
            .runtime_state()
            .test_take_metadata_command_recovery_wait_hook_observation()
    }

    #[cfg(test)]
    pub(crate) fn test_begin_metadata_command_recovery_leader(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> MetadataCommandRecoveryTestGuard {
        match self
            .local_map
            .runtime_state()
            .join_metadata_command_recovery(pg_id, command)
        {
            MetadataCommandRecoveryAdmission::Leader(guard) => MetadataCommandRecoveryTestGuard {
                _guard: Box::new(guard),
            },
            MetadataCommandRecoveryAdmission::Waited { .. } => {
                panic!("metadata command recovery admission unexpectedly waited")
            }
            MetadataCommandRecoveryAdmission::TimedOut { .. } => {
                panic!("metadata command recovery admission unexpectedly timed out")
            }
        }
    }

    fn metadata_pg_primary_shard_ack_route(
        &self,
        data_pg_id: DataPgId,
    ) -> Result<Box<dyn ShardAckRoute + '_>, StoreError> {
        let operation_epoch = self.operation_epoch();
        let node = self
            .local_map
            .metadata_pg_primary_node(operation_epoch, data_pg_id.pg_id())?;
        node.shard_ack_client()
            .open_shard_ack_route(operation_epoch, data_pg_id)
    }

    fn metadata_pg_primary_shard_ack_client_at_retained_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&Arc<dyn RetainedShardAckNodeClient>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node_for_retained_cleanup(operation_epoch, pg_id)?;
        Ok(node.retained_shard_ack_client())
    }

    #[cfg(test)]
    pub(crate) fn record_routine_metadata_command_checkpoints(
        &self,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut cursor = MetadataCommandCheckpointScanCursor::default();
        self.record_routine_metadata_command_checkpoints_with_limit(
            &mut cursor,
            METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT,
            usize::MAX,
        )
    }

    pub(crate) fn record_current_metadata_command_checkpoint_for_pg(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut summary = MetadataCommandCheckpointRecordSummary::default();
        let route = self
            .local_pg_route(pg_id)
            .ok_or_else(|| StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        summary.scanned += 1;
        if route.state() != PgState::Active {
            return Err(StoreError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
                state: route.state(),
            });
        }
        let primary_node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_client = primary_node.metadata_command_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch != route.cluster_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: state.cluster_epoch,
                current_epoch: route.cluster_epoch(),
            });
        }
        if state.applied_log_index == 0 && state.applied_log_hash.value() == 0 {
            summary.skipped_empty += 1;
            return Ok(summary);
        }
        match metadata_client.record_current_metadata_command_checkpoint(pg_id, state.cluster_epoch)
        {
            Ok(_) => {
                summary.recorded += 1;
                compact_metadata_command_log_for_checkpoint_record(
                    metadata_client.as_ref(),
                    pg_id,
                    state.cluster_epoch,
                    &mut summary,
                );
            }
            Err(error) => {
                note_metadata_command_checkpoint_record_error(pg_id, "failed", &error);
                return Err(error);
            }
        }
        Ok(summary)
    }

    pub(crate) fn record_routine_metadata_command_checkpoints_with_limit(
        &self,
        cursor: &mut MetadataCommandCheckpointScanCursor,
        mutation_limit: usize,
        pg_scan_limit: usize,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut summary = MetadataCommandCheckpointRecordSummary::default();
        if mutation_limit == 0 || pg_scan_limit == 0 {
            return Ok(summary);
        }

        let mut routes: Vec<_> = self.local_pg_routes().collect();
        routes.sort_by_key(|route| route.pg_id());
        if routes.is_empty() {
            cursor.after_pg_id = None;
            return Ok(summary);
        }
        let start = cursor.after_pg_id.map_or(0, |after_pg_id| {
            let next = routes.partition_point(|route| route.pg_id() <= after_pg_id);
            if next == routes.len() {
                0
            } else {
                next
            }
        });
        let routes_to_scan = routes.len().min(pg_scan_limit);

        for offset in 0..routes_to_scan {
            if summary.mutations() >= mutation_limit {
                summary.limit_reached = true;
                break;
            }
            let route = routes[(start + offset) % routes.len()];
            cursor.after_pg_id = Some(route.pg_id());
            summary.scanned += 1;
            if route.state() != PgState::Active {
                summary.skipped_inactive += 1;
                continue;
            }
            let primary_node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let metadata_client = primary_node.metadata_command_client();
            let state = match metadata_client.metadata_command_replica_state(route.pg_id()) {
                Ok(state) => state,
                Err(StoreError::MetadataCommandReplicaStateMissing { .. }) => {
                    summary.skipped_empty += 1;
                    continue;
                }
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                    continue;
                }
            };
            if state.cluster_epoch != route.cluster_epoch() {
                summary.skipped_stale_epoch += 1;
                continue;
            }
            if state.applied_log_index == 0 && state.applied_log_hash.value() == 0 {
                summary.skipped_empty += 1;
                continue;
            }
            let latest_checkpoint = match metadata_client.metadata_command_checkpoint_candidates(
                route.pg_id(),
                state.cluster_epoch,
                state.applied_log_index,
                1,
            ) {
                Ok(mut candidates) => candidates.pop(),
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                    continue;
                }
            };
            match metadata_command_checkpoint_record_decision(
                &state,
                latest_checkpoint.as_ref(),
                METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE,
                METADATA_COMMAND_CHECKPOINT_FRAME_RISK_BYTES,
            ) {
                Ok(MetadataCommandCheckpointRecordDecision::Record) => {}
                Ok(MetadataCommandCheckpointRecordDecision::AlreadyCurrent) => {
                    summary.already_current += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                    continue;
                }
                Ok(MetadataCommandCheckpointRecordDecision::SkipCadence) => {
                    summary.skipped_cadence += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                    continue;
                }
                Err(error) => {
                    note_metadata_command_checkpoint_record_error(route.pg_id(), "failed", &error);
                    summary.failed += 1;
                    continue;
                }
            }
            match metadata_client
                .record_current_metadata_command_checkpoint(route.pg_id(), state.cluster_epoch)
            {
                Ok(_) => {
                    summary.recorded += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                }
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                }
            }
        }

        if routes_to_scan < routes.len() {
            summary.limit_reached = true;
        }

        Ok(summary)
    }

    fn metadata_pg_read_object_listing_route(
        &self,
        pg_id: PgId,
    ) -> Result<Box<dyn ObjectListingMetadataRoute + '_>, BucketSnapshotLoadError> {
        let node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)
            .map_err(BucketSnapshotLoadError::Store)?;
        node.object_listing_metadata_client()
            .open_object_listing_metadata_route(
                self.operation_epoch(),
                self.object_metadata_scan_pg(pg_id),
                node.authorization(),
            )
    }

    fn metadata_pg_primary_object_listing_route(
        &self,
        pg_id: PgId,
    ) -> Result<Box<dyn ObjectListingMetadataRoute + '_>, BucketSnapshotLoadError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(BucketSnapshotLoadError::Store)?;
        node.object_listing_metadata_client()
            .open_object_listing_metadata_route(
                self.operation_epoch(),
                self.object_metadata_scan_pg(pg_id),
                MetadataReadAuthorization::active(pg_id),
            )
    }

    fn bucket_metadata_pg_id(&self, bucket: &BucketName) -> u32 {
        self.local_map.bucket_pg_for(bucket)
    }

    fn bucket_metadata_pg(&self, bucket: &BucketName) -> BucketPgId {
        self.local_map.bucket_metadata_pg_for(bucket)
    }

    fn validated_bucket_metadata_pg(&self, pg_id: PgId) -> BucketPgId {
        self.local_map
            .bucket_metadata_pg(pg_id)
            .expect("routed bucket metadata PG must belong to the installed topology")
    }

    pub(crate) fn object_payload_reclaim_pg_id(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_metadata_pg_id(bucket, key)
    }

    fn object_metadata_pg_id(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.local_map.object_pg_for(bucket, key)
    }

    fn object_metadata_pg(&self, bucket: &BucketName, key: &ObjectKey) -> ObjectMetadataPgId {
        self.local_map.object_metadata_pg_for(bucket, key)
    }

    fn object_metadata_scan_pg(&self, pg_id: PgId) -> ObjectMetadataScanPgId {
        self.local_map
            .object_metadata_scan_pg(pg_id)
            .expect("routed object metadata scan PG must belong to the installed topology")
    }

    fn metadata_command_bucket_write_reservation_proof(
        command: &MetadataCommandEnvelope,
    ) -> Option<&BucketWriteReservationProof> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::CommitMultipartObject(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::CreateStreamUpload(create) => {
                Some(&create.bucket_write_reservation)
            }
            MetadataCommandPayload::CommitStreamPart(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::PutObjectMetadata(update) => {
                Some(&update.bucket_write_reservation)
            }
            MetadataCommandPayload::DeleteObjectVersion(delete) => {
                Some(&delete.bucket_write_reservation)
            }
            MetadataCommandPayload::InsertDeleteMarker(marker) => {
                Some(&marker.bucket_write_reservation)
            }
            MetadataCommandPayload::CreateMultipartUpload(create) => {
                Some(create.bucket_write_reservation())
            }
            MetadataCommandPayload::AbortMultipartUpload(abort) => {
                Some(&abort.bucket_write_reservation)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn validate_metadata_command_bucket_write_reservation(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.validate_metadata_command_bucket_write_reservation_inner(command, None)
    }

    pub(super) fn validate_metadata_command_bucket_write_reservation_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.validate_metadata_command_bucket_write_reservation_inner(command, Some(deadline))
    }

    fn validate_metadata_command_bucket_write_reservation_inner(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Option<Instant>,
    ) -> Result<(), BucketSnapshotLoadError> {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return Ok(());
        };
        let command_subject_matches =
            Self::metadata_command_bucket_write_reservation_subject_matches(command, proof);
        if proof.cluster_epoch != command.id().cluster_epoch() || !command_subject_matches {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        let pg_id = self.bucket_metadata_pg_id(&proof.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let route = node
            .bucket_write_reservation_client()
            .open_bucket_write_reservation_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                &proof.bucket,
            )?;
        match deadline {
            Some(deadline) => route.validate_bucket_write_reservation_proof_until(proof, deadline),
            None => route.validate_bucket_write_reservation_proof(proof),
        }
    }

    fn metadata_command_bucket_write_reservation_subject_matches(
        command: &MetadataCommandEnvelope,
        proof: &BucketWriteReservationProof,
    ) -> bool {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => [
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            ]
            .into_iter()
            .any(|operation_kind| {
                proof.matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &commit.object.bucket,
                    operation_kind,
                    Some(commit.object.key.as_str()),
                )
            }),
            MetadataCommandPayload::PutObjectMetadata(update) => proof
                .matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &update.object.bucket,
                    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
                    Some(update.object.key.as_str()),
                ),
            MetadataCommandPayload::DeleteObjectVersion(delete) => proof
                .matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &delete.bucket,
                    match delete.mode {
                        DeleteObjectVersionMode::Current => {
                            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND
                        }
                        DeleteObjectVersionMode::Specific => {
                            DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND
                        }
                    },
                    Some(delete.key.as_str()),
                ),
            MetadataCommandPayload::InsertDeleteMarker(marker) => proof
                .matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &marker.bucket,
                    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
                    Some(marker.key.as_str()),
                ),
            MetadataCommandPayload::CreateMultipartUpload(create) => proof
                .matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &create.upload().bucket,
                    crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(create.upload().key.as_str()),
                ),
            MetadataCommandPayload::CreateStreamUpload(create) => proof
                .matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &create.session.bucket,
                    match create.session.target {
                        StreamUploadTarget::PutObject => {
                            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                        }
                        StreamUploadTarget::UploadPart { .. } => {
                            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                        }
                    },
                    Some(create.session.key.as_str()),
                ),
            MetadataCommandPayload::CommitStreamPart(commit) => {
                commit.has_consistent_subject()
                    && proof.matches_exact_mutation_subject(
                        command.id().cluster_epoch(),
                        &commit.bucket,
                        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                        Some(commit.key.as_str()),
                    )
            }
            MetadataCommandPayload::AbortMultipartUpload(abort) => {
                abort.has_consistent_subject()
                    && proof.matches_exact_mutation_subject(
                        command.id().cluster_epoch(),
                        &abort.bucket,
                        ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                        Some(abort.key.as_str()),
                    )
            }
            _ => true,
        }
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return Ok(());
        };
        if !Self::metadata_command_bucket_write_reservation_subject_matches(command, proof) {
            return Ok(());
        }
        self.release_bucket_write_reservation_proof(proof)
    }

    fn release_applied_metadata_command_bucket_write_reservations(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let preserve_command_reservation = matches!(
            command.payload(),
            MetadataCommandPayload::CreateStreamUpload(create)
                if create.session.target == StreamUploadTarget::PutObject
        );
        if !preserve_command_reservation {
            let expected_reservation_id =
                Self::metadata_command_bucket_write_reservation_proof(command)
                    .map(|proof| proof.reservation_id.as_str());
            match self.release_metadata_command_bucket_write_reservation(command) {
                Ok(()) => {}
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteReservationNotFound { reservation_id },
                )) if expected_reservation_id == Some(reservation_id.as_str()) => {}
                Err(error) => return Err(error),
            }
        }
        if let MetadataCommandPayload::AbortStreamUpload(abort) = command.payload() {
            if let Some(proof) = &abort.stream_create_bucket_write_reservation {
                self.release_stream_create_bucket_write_reservation_proof(proof)?;
            }
        }
        Ok(())
    }

    fn release_applied_metadata_command_bucket_write_reservations_for_terminal_cleanup(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let release_result = self.release_applied_metadata_command_bucket_write_reservations(command);
        #[cfg(test)]
        let release_result = release_result.and_then(|()| {
            request_ops::maybe_run_metadata_command_terminal_reservation_release_hook(
                Arc::as_ptr(&self.local_map) as usize,
                command,
            )
        });
        match release_result {
            Ok(()) => Ok(true),
            Err(error)
                if request_ops::applied_metadata_command_cleanup_error_is_retryable(&error) =>
            {
                request_ops::emit_metadata_command_terminal_cleanup_deferred(
                    pg_id,
                    "applied command reservation release failed transiently",
                    match &error {
                        BucketSnapshotLoadError::Store(error) => Some(error),
                        BucketSnapshotLoadError::Metadata(_) => None,
                    },
                );
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn release_stream_create_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        match self.release_bucket_write_reservation_proof(proof) {
            Ok(()) => Ok(()),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationNotFound { .. },
            )) => Ok(()),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn release_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(&proof.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node_for_retained_cleanup(proof.cluster_epoch, PgId::new(pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                &proof.bucket,
            )?
            .release_metadata_command_bucket_write_reservation(proof)?;
        Ok(())
    }

    fn pending_metadata_command_uses_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        proof: &BucketWriteReservationProof,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .as_ref()
            .and_then(Self::metadata_command_bucket_write_reservation_proof)
            == Some(proof))
    }

    fn next_bucket_write_reservation_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "bucket-write-",
            "generate bucket write reservation id",
        )
    }

    fn next_bucket_write_drain_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id("bucket-drain-", "generate bucket write drain id")
    }

    fn next_object_payload_reclaim_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "object-reclaim-",
            "generate object payload reclaim claim id",
        )
    }

    fn next_bucket_delete_finalize_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "bucket-finalize-",
            "generate bucket delete finalize claim id",
        )
    }

    fn next_lifecycle_sweep_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "lifecycle-sweep-",
            "generate lifecycle sweep claim id",
        )
    }

    fn next_bucket_write_coordination_id(
        &self,
        prefix: &'static str,
        context: &'static str,
    ) -> Result<String, StoreError> {
        random_hex_identifier(prefix).map_err(|_| StoreError::Io {
            context,
            source: std::io::Error::other("failed to generate random reservation id"),
        })
    }

    fn bucket_write_owner_token(&self) -> String {
        self.bucket_write_owner_token.to_string()
    }

    fn object_mutation_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::ObjectMutationMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.object_mutation_metadata_client())
    }

    fn object_delete_metadata_primary_route(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn crate::node_client::ObjectDeleteMetadataRoute + '_>, ObjectPgActionError>
    {
        let pg_id = self.object_metadata_pg(bucket, key);
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_object_delete_metadata_route(self.operation_epoch(), pg_id, bucket, key)
    }

    fn retained_object_mutation_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::RetainedObjectMutationMetadataNodeClient>, StoreError>
    {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.retained_object_mutation_metadata_client())
    }

    fn object_generation_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::ObjectGenerationMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.object_generation_metadata_client())
    }

    fn direct_put_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::DirectPutMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.direct_put_metadata_client())
    }

    pub fn default_payload_ec_shape(&self) -> EcShape {
        self.local_map.default_ec_shape()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_stream_abort_storage_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> StreamAbortTestHookGuard {
        self.test_hooks.lock().unwrap().before_stream_abort_storage = Some(hook);
        StreamAbortTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_retained_stream_abort_hook(
        &self,
        hook: RetainedStreamAbortHook,
    ) -> RetainedStreamAbortTestHookGuard {
        self.test_hooks.lock().unwrap().before_retained_stream_abort = Some(hook);
        RetainedStreamAbortTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_retained_stream_cleanup_capability_hook(
        &self,
        hook: StreamAbortHook,
    ) -> RetainedStreamCleanupCapabilityTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_retained_stream_cleanup_capability = Some(hook);
        RetainedStreamCleanupCapabilityTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_retained_stream_cleanup_capability_hook(
        &self,
        hook: StreamAbortHook,
    ) -> AfterRetainedStreamCleanupCapabilityTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_retained_stream_cleanup_capability = Some(hook);
        AfterRetainedStreamCleanupCapabilityTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_metadata_command_pending_install_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> MetadataCommandPendingInstallHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install = Some(hook);
        MetadataCommandPendingInstallHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_metadata_command_drain_hook(
        &self,
        hook: MetadataCommandDrainedTestHook,
    ) -> MetadataCommandDrainedTestHookGuard {
        self.test_hooks.lock().unwrap().after_metadata_command_drain = Some(hook);
        MetadataCommandDrainedTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_multipart_create_upload_id_prepared_hook(
        &self,
        hook: MultipartCreateUploadIdPreparedTestHook,
    ) -> MultipartCreateUploadIdPreparedTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_multipart_create_upload_id_prepared = Some(hook);
        MultipartCreateUploadIdPreparedTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_multipart_create_command_install_hook(
        &self,
        hook: MultipartCreateCommandInstallTestHook,
    ) -> MultipartCreateCommandInstallTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_multipart_create_command_install = Some(hook);
        MultipartCreateCommandInstallTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_metadata_listing_pg_complete_hook(
        &self,
        hook: Arc<dyn Fn(u32) + Send + Sync>,
    ) -> MetadataListingPgCompleteHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_metadata_listing_pg_complete = Some(hook);
        MetadataListingPgCompleteHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_reclaim_ownership_lookup_hook(
        &self,
        hook: ReclaimCoordinationTestHook,
    ) -> ReclaimOwnershipLookupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_reclaim_ownership_lookup = Some(hook);
        ReclaimOwnershipLookupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_reclaim_claim_acquired_hook(
        &self,
        hook: ReclaimCoordinationTestHook,
    ) -> ReclaimClaimAcquiredTestHookGuard {
        self.test_hooks.lock().unwrap().after_reclaim_claim_acquired = Some(hook);
        ReclaimClaimAcquiredTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_reclaim_claim_release_hook(
        &self,
        hook: ReclaimCoordinationTestHook,
    ) -> ReclaimClaimReleaseTestHookGuard {
        self.test_hooks.lock().unwrap().before_reclaim_claim_release = Some(hook);
        ReclaimClaimReleaseTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_direct_put_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> DirectPutCommandIdHookGuard {
        self.test_hooks.lock().unwrap().before_direct_put_command_id = Some(hook);
        DirectPutCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_direct_put_abandoned_log_inspection_hook(
        &self,
        hook: DirectPutAbandonedLogInspectionHook,
    ) -> DirectPutAbandonedLogInspectionHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_direct_put_abandoned_log_inspection = Some(hook);
        DirectPutAbandonedLogInspectionHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_object_generation_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> ObjectGenerationCommandIdHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id = Some(hook);
        ObjectGenerationCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_object_version_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> ObjectVersionCommandIdHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_object_version_command_id = Some(hook);
        ObjectVersionCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_stream_append_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> StreamAppendCommandIdHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_stream_append_command_id = Some(hook);
        StreamAppendCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_stream_append_command_id_allocated_hook(
        &self,
        hook: Arc<dyn Fn(MetadataCommandId) + Send + Sync>,
    ) -> StreamAppendCommandIdAllocatedHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_stream_append_command_id_allocated = Some(hook);
        StreamAppendCommandIdAllocatedHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_object_metadata_reservation_acquired_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> ObjectMetadataReservationAcquiredHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired = Some(hook);
        ObjectMetadataReservationAcquiredHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_placed_payload_shard_delete_hook(
        &self,
        hook: PayloadShardCleanupTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_delete = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::PlacedShardDelete,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub(crate) fn test_install_before_placed_payload_shard_write_hook(
        &self,
        hook: PayloadShardWriteTestHook,
    ) -> PayloadShardWriteTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_write = Some(hook);
        PayloadShardWriteTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_placed_payload_shard_read_hook(
        &self,
        hook: PayloadShardReadTestHook,
    ) -> PayloadShardReadTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_read = Some(hook);
        PayloadShardReadTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_before_metadata_primary_payload_ack_delete_hook(
        &self,
        hook: PayloadShardCleanupTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_metadata_primary_payload_ack_delete = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::MetadataPrimaryAckDelete,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_best_effort_payload_cleanup_error_hook(
        &self,
        hook: PayloadCleanupErrorTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .best_effort_payload_cleanup_error = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::BestEffortError,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_after_direct_put_metadata_publish_hook(
        &self,
        hook: crate::node::DirectPutMetadataPublishHook,
    ) -> crate::node::DirectPutMetadataPublishTestHookGuard {
        self.metadata_primary_test_hook_node()
            .test_install_after_direct_put_metadata_publish_hook(hook)
    }

    #[cfg(test)]
    pub(crate) fn test_install_after_object_metadata_command_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> crate::node::ObjectMetadataCommandPublishTestHookGuard {
        self.metadata_primary_test_hook_node()
            .test_install_after_object_metadata_command_publish_hook(hook)
    }

}
