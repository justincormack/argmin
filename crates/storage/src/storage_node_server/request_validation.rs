// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl StorageNodeConnectionHandler {
    fn validate_node_epoch(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "request cluster epoch {cluster_epoch} does not match storage node cluster epoch {}",
                    self.config.cluster_epoch
                ),
            });
        }
        Ok(())
    }

    fn validate_metadata_command_replacement_scope<'a>(
        scope_bucket: Option<&'a BucketName>,
        replacement: &MetadataCommandEnvelope,
    ) -> Result<&'a BucketName, StorageRpcErrorResponse> {
        let Some(scope_bucket) = scope_bucket else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command replacement requires canonical bucket scope"
                    .to_string(),
            });
        };
        if scope_bucket != replacement.bucket_name() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command replacement scope bucket does not match command bucket"
                    .to_string(),
            });
        }
        Ok(scope_bucket)
    }

    fn metadata_command_pending_slot_replace_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandPendingSlotReplaceRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) =
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
        {
            return encode_storage_rpc_error_response(&error);
        }
        if !request
            .replacement
            .payload()
            .is_ordinary_pending_slot_reissue_of(request.previous.payload())
        {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command replacement is not an ordinary reissue of the pending command"
                    .to_string(),
            });
        }
        let scope_bucket = match Self::validate_metadata_command_replacement_scope(
            request.scope_bucket.as_ref(),
            &request.replacement,
        ) {
            Ok(scope_bucket) => scope_bucket,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
        let response = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => match pg.replace_pending_metadata_command_slot_for_reissue(
                self.config.node_id.as_u32(),
                &request.previous,
                &request.replacement,
                Some(scope_bucket),
            ) {
            Ok(removed) => {
                let payload = encode_metadata_command_pending_slot_remove_response(
                    &StorageRpcMetadataCommandPendingSlotRemoveResponse { removed },
                );
                encode_storage_rpc_success_response(&payload)
            }
                Err(crate::pg_store::PendingMetadataCommandSlotReplaceError::Definitive(error)) => {
                    encode_storage_rpc_error_response(&store_error_response(error))?
                }
                Err(crate::pg_store::PendingMetadataCommandSlotReplaceError::MayHaveApplied(_)) => {
                    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::MetadataCommandMutationUncertain,
                        message: "metadata command pending slot replacement outcome is uncertain"
                            .to_string(),
                    })?
                }
            },
            Err(error) => encode_storage_rpc_error_response(&store_error_response(error))?,
        };
        Ok(response)
    }

    fn metadata_command_recovery_pending_slot_replace_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let mutation_fence = match self.validate_reissued_metadata_command_recovery(
            request.node_id,
            request.pg_id,
            &request.authorized_source,
            request.abandoned_source.as_ref(),
            &request.replacement,
        ) {
            Ok(fence) => fence,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let cleanup_chain_matches = match request.abandoned_source.as_ref() {
            None => true,
            Some(abandoned_source)
                if request.previous.payload() == request.authorized_source.payload() =>
            {
                request.previous == *abandoned_source
            }
            Some(abandoned_source) => {
                request.previous.payload() == request.replacement.payload()
                    && request.previous.id().log_index() > abandoned_source.id().log_index()
            }
        };
        if request.previous.id().cluster_epoch() != request.cluster_epoch
            || request.previous.id().pg_id() != request.pg_id
            || !request
                .previous
                .payload()
                .is_authorized_recovery_derivative_of(request.authorized_source.payload())
            || request.previous.id().log_index() < request.authorized_source.id().log_index()
            || request.replacement.id().log_index() <= request.previous.id().log_index()
            || !cleanup_chain_matches
        {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command recovery replacement is not derived from its authorized source"
                    .to_string(),
            });
        }
        let reporting_node_is_local = self.config.pending_metadata_command_recoveries.iter().any(
            |(authorized_pg_id, recovery)| {
                *authorized_pg_id == request.pg_id
                    && recovery.reporting_node_id() == self.config.node_id
                    && recovery.pending()
                        == PendingMetadataCommandObservation::new(
                            request.cluster_epoch,
                            std::num::NonZeroU64::new(
                                request.authorized_source.id().log_index().get(),
                            )
                            .expect("typed metadata command log index must be nonzero"),
                            request.authorized_source.checksum_crc64(),
                        )
            },
        );
        if !reporting_node_is_local {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: "metadata command recovery replacement is not executing on the authorized historical primary"
                    .to_string(),
            });
        }
        let scope_bucket = match Self::validate_metadata_command_replacement_scope(
            request.scope_bucket.as_ref(),
            &request.replacement,
        ) {
            Ok(scope_bucket) => scope_bucket,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        metadata_mutation_route_guard_or_return!(self);
        if let Err(error) = mutation_fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let pg = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => pg,
            Err(error) => return encode_storage_rpc_error_response(&store_error_response(error)),
        };
        let durable_log_tip =
            match pg.max_metadata_command_log_index(request.previous.id().cluster_epoch()) {
                Ok(durable_log_tip) => durable_log_tip,
                Err(error) => {
                    return encode_storage_rpc_error_response(&store_error_response(error));
                }
            };
        let Some(expected_replacement_index) = durable_log_tip
            .max(request.previous.id().log_index().get())
            .checked_add(1)
        else {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command recovery replacement index overflows".to_string(),
            });
        };
        if request.replacement.id().log_index().get() != expected_replacement_index {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "metadata command recovery replacement index {} is not the next primary log index {}",
                    request.replacement.id().log_index().get(),
                    expected_replacement_index
                ),
            });
        }
        let response = match pg.replace_pending_metadata_command_slot_for_recovery(
            self.config.node_id.as_u32(),
            &request.authorized_source,
            request.abandoned_source.as_ref(),
            &request.previous,
            &request.replacement,
            Some(scope_bucket),
        ) {
            Ok(removed) => {
                let payload = encode_metadata_command_pending_slot_remove_response(
                    &StorageRpcMetadataCommandPendingSlotRemoveResponse { removed },
                );
                encode_storage_rpc_success_response(&payload)
            }
            Err(crate::pg_store::PendingMetadataCommandSlotReplaceError::Definitive(error)) => {
                encode_storage_rpc_error_response(&store_error_response(error))?
            }
            Err(crate::pg_store::PendingMetadataCommandSlotReplaceError::MayHaveApplied(_)) => {
                encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandMutationUncertain,
                    message: "metadata command recovery pending slot replacement outcome is uncertain"
                        .to_string(),
                })?
            }
        };
        Ok(response)
    }

    fn metadata_command_pg_lock_acquire_response(
        &self,
        session: &mut StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let (route_validation, authority) = if request.cluster_epoch < self.config.cluster_epoch {
            (
                self.validate_metadata_command_recovery_primary_lock(
                    request.node_id,
                    request.cluster_epoch,
                    request.pg_id,
                ),
                StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
            )
        } else {
            (
                self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
                    .and_then(|()| {
                        self.validate_primary_pg(request.pg_id, "metadata command critical section")
                    }),
                StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            )
        };
        if let Err(error) = route_validation {
            return encode_storage_rpc_error_response(&error);
        }
        if let Err(error) = session.acquire_metadata_command_pg_lock(
            &self.metadata_command_locks,
            self.config.node_id,
            StorageNodeMetadataCommandLockBinding {
                pg_id: request.pg_id,
                cluster_epoch: request.cluster_epoch,
                authority,
            },
            session.current_rpc_context(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        Ok(encode_storage_rpc_success_response(&[]))
    }

    fn validate_metadata_command_recovery_primary_lock(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_for_metadata_command_recovery(node_id, cluster_epoch, pg_id)?;
        self.require_current_route_map_valid_rpc()?;
        let authorized = self.config.pending_metadata_command_recoveries.iter().any(
            |(authorized_pg_id, recovery)| {
                *authorized_pg_id == pg_id
                    && recovery.reporting_node_id() == self.config.node_id
                    && recovery.pending().cluster_epoch() == cluster_epoch
            },
        );
        if authorized {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "historical metadata command critical section for PG {} at epoch {} is not authorized for primary node {} by the current runtime map",
                pg_id.get(),
                cluster_epoch.get(),
                self.config.node_id.as_u32()
            ),
        })
    }

    fn validate_metadata_command_recovery_read(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        require_reporting_node: bool,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_for_metadata_command_recovery(node_id, cluster_epoch, pg_id)?;
        self.require_current_route_map_valid_rpc()?;
        let authorized = self.config.pending_metadata_command_recoveries.iter().any(
            |(authorized_pg_id, recovery)| {
                *authorized_pg_id == pg_id
                    && recovery.pending().cluster_epoch() == cluster_epoch
                    && (!require_reporting_node
                        || recovery.reporting_node_id() == self.config.node_id)
            },
        );
        if authorized {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "historical metadata command read for PG {} at epoch {} is not authorized for node {} by the current runtime map",
                pg_id.get(),
                cluster_epoch.get(),
                self.config.node_id.as_u32()
            ),
        })
    }

    fn metadata_command_pg_lock_release_response(
        &self,
        session: &mut StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_cleanup(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        session.release_metadata_command_pg_lock(request.pg_id);
        Ok(encode_storage_rpc_success_response(&[]))
    }

    fn try_begin_shard_delete(
        &self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<StorageNodeShardDeleteFence, StorageRpcErrorResponse> {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_begin_delete(location, shard_key)?;
        Ok(StorageNodeShardDeleteFence {
            read_handles: Arc::clone(&self.read_handles),
            location,
            shard_key: shard_key.clone(),
        })
    }

    fn validated_shard_location(&self, location: StorageRpcShardLocation) -> ShardLocation {
        let data_pg_id = self
            .node
            .data_pg(location.pg_id)
            .expect("validated shard route must belong to the installed topology");
        ShardLocation::new(
            location.cluster_epoch,
            data_pg_id,
            location.shard_index,
            location.node_id,
        )
    }

    fn validate_pg_route(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_with_allowed_states(
            node_id,
            cluster_epoch,
            pg_id,
            &[PgState::Active],
        )
    }

    fn validate_metadata_read_pg_route(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.with_metadata_read_pg(node_id, cluster_epoch, pg_id, |_| ())
    }

    fn with_metadata_read_pg<T>(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        action: impl FnOnce(&PgStore) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        self.validate_pg_route_with_allowed_states(
            node_id,
            cluster_epoch,
            pg_id,
            &[PgState::Active, PgState::Peering],
        )?;
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated metadata read PG route must exist");
        if route.state == PgState::Active {
            self.validate_primary_pg(pg_id, "metadata read")?;
            let pg = self
                .node
                .get_pg_for_metadata_read(
                    self.config.node_id,
                    pg_id,
                    MetadataReadAuthorization::active(pg_id),
                )
                .map_err(store_error_response)?;
            return Ok(action(&pg));
        }

        let Some(read_route) = route.metadata_read_route else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "Peering PG {} has no certified metadata read replica",
                    pg_id.get()
                ),
            });
        };
        if read_route.node_id() != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not the certified metadata read replica for PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        let pg = self
            .node
            .get_pg_for_metadata_read(
                self.config.node_id,
                pg_id,
                MetadataReadAuthorization::peering(pg_id, read_route),
            )
            .map_err(store_error_response)?;
        Ok(action(&pg))
    }

    fn validate_pg_route_for_metadata_command_recovery(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        match self.validate_pg_route(node_id, cluster_epoch, pg_id) {
            Ok(()) => Ok(()),
            Err(_) if cluster_epoch < self.config.cluster_epoch => self
                .validate_historical_active_pg_route_for_metadata_command_recovery(
                    node_id,
                    cluster_epoch,
                    pg_id,
                ),
            Err(error) => Err(error),
        }
    }

    fn validate_pg_route_for_metadata_command_apply(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        let cluster_epoch = command.id().cluster_epoch();
        match self.validate_pg_route(node_id, cluster_epoch, pg_id) {
            Ok(()) => Ok(StorageNodeRouteFence::current(
                &self.config,
                self.current_route_map_lease(),
            )),
            Err(_) if cluster_epoch < self.config.cluster_epoch => {
                self.validate_authorized_metadata_command_recovery_source(node_id, pg_id, command)
            }
            Err(error) => Err(error),
        }
    }

    fn validate_reissued_metadata_command_recovery(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        validate_metadata_command_recovery_certificate(
            authorized_source,
            abandoned_source,
            command,
        )
        .map_err(|error| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: error.to_string(),
        })?;
        let fence = self.validate_authorized_metadata_command_recovery_source(
            node_id,
            pg_id,
            authorized_source,
        )?;
        if command.payload() != authorized_source.payload() {
            let abandoned_source = abandoned_source
                .expect("validated recovery follow-up must carry its abandoned source");
            let pg = self
                .node
                .get_pg(pg_id.get())
                .map_err(store_error_response)?;
            let source_abandoned = pg
                .metadata_command_abandoned(self.config.node_id.as_u32(), abandoned_source)
                .map_err(store_error_response)?;
            if !source_abandoned {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message:
                        "metadata command recovery follow-up requires a durable source tombstone"
                            .to_string(),
                });
            }
        }
        Ok(fence)
    }

    fn validate_authorized_metadata_command_recovery_source(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        authorized_source: &MetadataCommandEnvelope,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        if authorized_source.id().pg_id() != pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "metadata command recovery source does not match the request PG"
                    .to_string(),
            });
        }
        let cluster_epoch = authorized_source.id().cluster_epoch();
        self.validate_pg_route_for_metadata_command_recovery(node_id, cluster_epoch, pg_id)?;
        let expected = PendingMetadataCommandObservation::new(
            cluster_epoch,
            std::num::NonZeroU64::new(authorized_source.id().log_index().get())
                .expect("typed metadata command log index must be nonzero"),
            authorized_source.checksum_crc64(),
        );
        let authorized = self.config.pending_metadata_command_recoveries.iter().any(
            |(authorized_pg_id, recovery)| {
                *authorized_pg_id == pg_id && recovery.pending() == expected
            },
        );
        if !authorized {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "metadata command recovery for PG {} at epoch {} is not authorized by the current runtime map",
                    pg_id.get(),
                    cluster_epoch.get()
                ),
            });
        }
        if cluster_epoch == self.config.cluster_epoch {
            return Ok(StorageNodeRouteFence::current(
                &self.config,
                self.current_route_map_lease(),
            ));
        }
        self.bounded_historical_metadata_command_fence(cluster_epoch, pg_id)
    }

    fn bounded_historical_metadata_command_fence(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        let valid_until_ms = self.config.route_map_valid_until_ms().ok_or_else(|| {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "historical metadata command recovery for PG {} at epoch {} has no bounded runtime-map validity",
                    pg_id.get(),
                    cluster_epoch.get()
                ),
            }
        })?;
        let local_valid_until_monotonic_ms = self
            .current_route_map_lease()
            .map(BoundRouteMapLease::local_valid_until_monotonic_ms)
            .ok_or_else(|| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "historical metadata command recovery for PG {} at epoch {} has no process-bound runtime-map lease",
                    pg_id.get(),
                    cluster_epoch.get()
                ),
            })?;
        let fence = StorageNodeRouteFence::historical(
            cluster_epoch,
            valid_until_ms,
            local_valid_until_monotonic_ms,
        );
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(fence)
    }

    fn validate_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_current_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id)
    }

    fn validate_pg_route_for_metadata_transfer_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if cluster_epoch < self.config.cluster_epoch {
            return self.validate_historical_pg_route_for_peering_inspection(
                node_id,
                cluster_epoch,
                pg_id,
            );
        }
        self.validate_current_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id)
    }

    /// Authorizes transfer mutation at the immutable destination epoch while
    /// requiring the current route to retain that exact transfer marker.
    fn validate_pg_route_for_metadata_transfer_mutation(
        &self,
        node_id: NodeId,
        destination_epoch: ClusterEpoch,
        pg_id: PgId,
        permit_unmarked_current_peering: bool,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if destination_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "metadata-transfer destination epoch {} is newer than storage-node epoch {}",
                    destination_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        self.require_current_route_map_valid_rpc()?;
        let raw_pg_id = pg_id.get();
        let current_route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id)
            .ok_or_else(|| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {raw_pg_id} is not configured on this storage node"),
            })?;
        if current_route.cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                    current_route.cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if current_route.state != PgState::Peering {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!("PG {raw_pg_id} route is {}", current_route.state),
            });
        }
        if !current_route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        let exact_marker =
            current_route.metadata_transfer_destination_epoch == Some(destination_epoch);
        if !exact_marker
            && !(permit_unmarked_current_peering
                && destination_epoch == self.config.cluster_epoch
                && current_route.metadata_transfer_destination_epoch.is_none())
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} current route does not retain metadata-transfer destination epoch {}",
                    destination_epoch.get()
                ),
            });
        }
        if destination_epoch == self.config.cluster_epoch {
            return Ok(StorageNodeRouteFence::current(
                &self.config,
                self.current_route_map_lease(),
            ));
        }
        let retained_route = self
            .config
            .historical_pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == destination_epoch);
        if retained_route.is_none_or(|route| {
            route.state != PgState::Peering
                || !route.acting_set.contains(&self.config.node_id)
                || route.metadata_transfer_destination_epoch != Some(destination_epoch)
        }) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} metadata-transfer destination route at epoch {} is not retained",
                    destination_epoch.get()
                ),
            });
        }
        let valid_until_ms = self.config.route_map_valid_until_ms().ok_or_else(|| {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} metadata-transfer destination epoch {} has no bounded runtime-map validity",
                    destination_epoch.get()
                ),
            }
        })?;
        let local_valid_until_monotonic_ms = self
            .current_route_map_lease()
            .map(BoundRouteMapLease::local_valid_until_monotonic_ms)
            .ok_or_else(|| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} metadata-transfer destination epoch {} has no process-bound runtime-map lease",
                    destination_epoch.get()
                ),
            })?;
        Ok(StorageNodeRouteFence::historical(
            destination_epoch,
            valid_until_ms,
            local_valid_until_monotonic_ms,
        ))
    }

    fn validate_current_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route_with_allowed_states(
            node_id,
            cluster_epoch,
            pg_id,
            &[PgState::Active, PgState::Peering],
        )
    }

    fn validate_historical_pg_route_for_peering_inspection(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let raw_pg_id = pg_id.get();
        let Some(route) = self
            .config
            .historical_pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == cluster_epoch)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} route for historical epoch {} is not retained",
                    cluster_epoch.get()
                ),
            });
        };
        if route.state != PgState::Peering {
            let code = if route.state == PgState::Active {
                StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
            } else {
                StorageRpcErrorCode::InactivePgRoute
            };
            return Err(StorageRpcErrorResponse {
                code,
                message: format!(
                    "historical peering inspection for PG {raw_pg_id} at epoch {} requires Peering route, got {}",
                    cluster_epoch.get(),
                    route.state
                ),
            });
        }
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(())
    }

    fn validate_historical_active_pg_route_for_metadata_command_recovery(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let raw_pg_id = pg_id.get();
        let Some(active_route) = self
            .config
            .historical_pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == cluster_epoch)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "PG {raw_pg_id} route for historical metadata command epoch {} is not retained",
                    cluster_epoch.get()
                ),
            });
        };
        if active_route.state != PgState::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "historical metadata command recovery for PG {raw_pg_id} at epoch {} requires Active route, got {}",
                    cluster_epoch.get(),
                    active_route.state
                ),
            });
        }
        if !active_route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        let has_current_or_retained_peering =
            self.config.pg_routes.iter().any(|route| {
                route.pg_id == raw_pg_id
                    && route.cluster_epoch >= cluster_epoch
                    && route.state == PgState::Peering
                    && route.acting_set.contains(&self.config.node_id)
            }) || self.config.historical_pg_routes.iter().any(|route| {
                route.pg_id == raw_pg_id
                    && route.cluster_epoch >= cluster_epoch
                    && route.state == PgState::Peering
                    && route.acting_set.contains(&self.config.node_id)
            });
        if !has_current_or_retained_peering {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "historical metadata command recovery for PG {raw_pg_id} at epoch {} requires a retained Peering route",
                    cluster_epoch.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_route_for_metadata_log_read(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        if cluster_epoch == self.config.cluster_epoch {
            return self.validate_pg_route_for_peering_inspection(node_id, cluster_epoch, pg_id);
        }
        if cluster_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} is newer than storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        self.require_current_route_map_valid_rpc()?;
        let raw_pg_id = pg_id.get();
        if let Some(route) = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id)
        {
            if route.cluster_epoch != self.config.cluster_epoch {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::WrongClusterEpoch,
                    message: format!(
                        "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                        route.cluster_epoch.get(),
                        self.config.cluster_epoch.get()
                    ),
                });
            }
            if route.state == PgState::Peering && route.acting_set.contains(&self.config.node_id) {
                return Ok(());
            }
        }
        if self.config.historical_pg_routes.iter().any(|route| {
            route.pg_id == raw_pg_id
                && route.cluster_epoch >= cluster_epoch
                && route.state == PgState::Peering
                && route.acting_set.contains(&self.config.node_id)
        }) {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "PG {raw_pg_id} has no retained Peering route covering metadata log epoch {}",
                cluster_epoch.get()
            ),
        })
    }

    fn validate_pg_route_with_allowed_states(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        allowed_states: &[PgState],
    ) -> Result<(), StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} does not match storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        self.require_current_route_map_valid_rpc()?;
        let raw_pg_id = pg_id.get();
        let Some(route) = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == raw_pg_id)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {raw_pg_id} is not configured on this storage node"),
            });
        };
        if route.cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::WrongClusterEpoch,
                message: format!(
                    "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                    route.cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if !allowed_states.contains(&route.state) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!("PG {raw_pg_id} route is {}", route.state),
            });
        }
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_route_for_cleanup(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.cleanup_pg_route(node_id, cluster_epoch, pg_id)
            .map(|_| ())
    }

    fn cleanup_pg_route(
        &self,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&StorageNodePgRoute, StorageRpcErrorResponse> {
        if node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        let raw_pg_id = pg_id.get();
        if !self.config.pg_ids.contains(&raw_pg_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {raw_pg_id} is not configured on this storage node"),
            });
        }
        if cluster_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request route epoch {} is newer than storage-node epoch {}",
                    cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        let route = if cluster_epoch == self.config.cluster_epoch {
            let Some(route) = self
                .config
                .pg_routes
                .iter()
                .find(|route| route.pg_id == raw_pg_id)
            else {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::UnknownPg,
                    message: format!("PG {raw_pg_id} is not configured on this storage node"),
                });
            };
            if route.cluster_epoch != self.config.cluster_epoch {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::WrongClusterEpoch,
                    message: format!(
                        "PG {raw_pg_id} route epoch {} does not match storage-node epoch {}",
                        route.cluster_epoch.get(),
                        self.config.cluster_epoch.get()
                    ),
                });
            }
            route
        } else {
            let Some(route) = self
                .config
                .historical_pg_routes
                .iter()
                .find(|route| route.pg_id == raw_pg_id && route.cluster_epoch == cluster_epoch)
            else {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: format!(
                        "PG {raw_pg_id} route for cleanup epoch {} is not retained",
                        cluster_epoch.get()
                    ),
                });
            };
            route
        };
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {raw_pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(route)
    }

    fn validate_primary_pg_for_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &crate::ObjectKey,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated object metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        self.validate_pg_for_object(pg_id, bucket, key, operation)
    }

    fn validate_pg_for_object(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &crate::ObjectKey,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let expected_pg_id = PgId::new(self.node.pg_topology().object_pg_for(bucket, key));
        if pg_id != expected_pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match object {}/{} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    key.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_pg_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let expected_pg_id = PgId::new(self.node.pg_topology().bucket_pg_for(bucket));
        if pg_id != expected_pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match bucket {} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validate_primary_pg(
        &self,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        Ok(())
    }

    fn validated_data_pg(&self, pg_id: PgId) -> DataPgId {
        self.node
            .data_pg(pg_id)
            .expect("validated data PG must belong to the installed topology")
    }

    fn validate_primary_pg_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id.get())
            .expect("validated bucket metadata PG route must exist");
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on PG {}",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        self.validate_pg_for_bucket(pg_id, bucket, operation)
    }

    fn retained_lifecycle_sweep_claim_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        claim: &'a LifecycleSweepClaimRecord,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedLifecycleSweepClaimRoute<'a>, StorageRpcErrorResponse> {
        if route_cluster_epoch != claim.cluster_epoch || pg_id.get() != claim.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} route epoch/PG ({}, {}) does not match claim ({}, {})",
                    route_cluster_epoch.get(),
                    pg_id.get(),
                    claim.cluster_epoch.get(),
                    claim.pg_id
                ),
            });
        }
        let pg_id = self.validate_retained_bucket_write_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            &claim.bucket,
            operation,
        )?;
        Ok(StorageNodeRetainedLifecycleSweepClaimRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id.pg_id(),
            pg_id,
            claim,
        })
    }

    fn validate_bucket_metadata_control_route(
        &self,
        request: &StorageRpcBucketRequest,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        self.validate_primary_pg_for_bucket(request.pg_id, &request.bucket, operation)
    }

    fn active_bucket_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcBucketRequest,
        operation: &'static str,
    ) -> Result<StorageNodeActiveBucketRoute<'a>, StorageRpcErrorResponse> {
        self.active_bucket_route_for_parts(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            &request.bucket,
            operation,
        )
    }

    fn metadata_read_bucket_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcBucketRequest,
        operation: &'static str,
    ) -> Result<StorageNodeMetadataReadBucketRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_metadata_read_pg_route(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        )?;
        self.validate_pg_for_bucket(request.pg_id, &request.bucket, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeMetadataReadBucketRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self.node.bucket_metadata_pg_for(&request.bucket),
            bucket: &request.bucket,
        })
    }

    fn active_bucket_scan_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeActiveBucketScanRoute<'a>, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        self.validate_pg_route(node_id, cluster_epoch, pg_id)?;
        self.validate_primary_pg(pg_id, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        let pg_id = self
            .node
            .bucket_metadata_pg(pg_id)
            .expect("validated bucket scan PG must belong to the installed topology");
        Ok(StorageNodeActiveBucketScanRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id,
        })
    }

    fn metadata_read_bucket_scan_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeMetadataReadBucketScanRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_metadata_read_pg_route(node_id, cluster_epoch, pg_id)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        let pg_id = self
            .node
            .bucket_metadata_pg(pg_id)
            .expect("validated bucket scan PG must belong to the installed topology");
        Ok(StorageNodeMetadataReadBucketScanRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id,
        })
    }

    fn active_bucket_route_for_parts<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        bucket: &'a BucketName,
        operation: &'static str,
    ) -> Result<StorageNodeActiveBucketRoute<'a>, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        self.validate_pg_route(node_id, cluster_epoch, pg_id)?;
        self.validate_primary_pg_for_bucket(pg_id, bucket, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActiveBucketRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self.node.bucket_metadata_pg_for(bucket),
            bucket,
        })
    }

    fn bucket_delete_replica_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcBucketRequest,
        operation: &'static str,
    ) -> Result<StorageNodeBucketDeleteReplicaHeadRoute<'a>, StorageRpcErrorResponse> {
        self.validate_retained_cleanup_admission(route_permit, operation)?;
        if request.cluster_epoch < self.config.cluster_epoch {
            self.validate_metadata_command_recovery_read(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
                false,
            )?;
        } else {
            self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        }
        self.validate_pg_for_bucket(request.pg_id, &request.bucket, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeBucketDeleteReplicaHeadRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self.node.bucket_metadata_pg_for(&request.bucket),
            bucket: &request.bucket,
        })
    }

    fn active_object_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectRequest,
        operation: &'static str,
    ) -> Result<StorageNodeActiveObjectRoute<'a>, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        self.validate_pg_for_object(request.pg_id, &request.bucket, &request.key, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActiveObjectRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self
                .node
                .object_metadata_pg_for(&request.bucket, &request.key),
            bucket: &request.bucket,
            key: &request.key,
        })
    }

    fn metadata_read_object_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectRequest,
        operation: &'static str,
    ) -> Result<StorageNodeMetadataReadObjectRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_metadata_read_pg_route(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        )?;
        self.validate_pg_for_object(request.pg_id, &request.bucket, &request.key, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeMetadataReadObjectRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self
                .node
                .object_metadata_pg_for(&request.bucket, &request.key),
            bucket: &request.bucket,
            key: &request.key,
        })
    }

    fn active_primary_object_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectRequest,
        operation: &'static str,
    ) -> Result<StorageNodeActivePrimaryObjectRoute<'a>, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        self.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        self.validate_primary_pg_for_object(
            request.pg_id,
            &request.bucket,
            &request.key,
            operation,
        )?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        let route = StorageNodeActiveObjectRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self
                .node
                .object_metadata_pg_for(&request.bucket, &request.key),
            bucket: &request.bucket,
            key: &request.key,
        };
        Ok(StorageNodeActivePrimaryObjectRoute { route })
    }

    fn active_primary_object_scan_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeActivePrimaryObjectScanRoute<'a>, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        self.validate_pg_route(node_id, cluster_epoch, pg_id)?;
        self.validate_primary_pg(pg_id, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActivePrimaryObjectScanRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self
                .node
                .object_metadata_scan_pg(pg_id)
                .expect("validated object metadata scan PG must belong to installed topology"),
        })
    }

    fn metadata_read_object_scan_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeMetadataReadObjectScanRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_metadata_read_pg_route(node_id, cluster_epoch, pg_id)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeMetadataReadObjectScanRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self
                .node
                .object_metadata_scan_pg(pg_id)
                .expect("validated object metadata scan PG must belong to installed topology"),
        })
    }

    fn active_shard_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        location: StorageRpcShardLocation,
        shard_key: &'a ShardKey,
        operation: &'static str,
    ) -> Result<StorageNodeActiveShardRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_pg_route(location.node_id, location.cluster_epoch, location.pg_id)?;
        if location.shard_index != shard_key.shard_index() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} location shard index {} does not match shard key index {}",
                    location.shard_index.get(),
                    shard_key.shard_index().get()
                ),
            });
        }
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActiveShardRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            location: self.validated_shard_location(location),
            shard_key,
        })
    }

    fn active_read_handle_acquire_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        session: &'a mut StorageNodeSession,
        request: &StorageRpcReadHandleAcquireRequest,
        operation: &'static str,
    ) -> Result<StorageNodeActiveReadHandleAcquireRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_read_handle_session(session, operation)?;
        validate_read_handle_acquire_request(request).map_err(|error| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!("invalid {operation} subject: {error}"),
        })?;
        let mut entries = Vec::with_capacity(request.locations.len());
        for (&location, shard_key) in request.locations.iter().zip(&request.shard_keys) {
            self.validate_pg_route(location.node_id, location.cluster_epoch, location.pg_id)?;
            entries.push((self.validated_shard_location(location), shard_key.clone()));
        }
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActiveReadHandleAcquireRoute {
            _route_permit: route_permit,
            fence,
            session,
            read_operation_id: request.read_operation_id.clone(),
            entries,
        })
    }

    fn active_primary_data_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeActivePrimaryDataRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_pg_route(node_id, cluster_epoch, pg_id)?;
        self.validate_primary_pg(pg_id, operation)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActivePrimaryDataRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self.validated_data_pg(pg_id),
        })
    }

    fn active_data_scan_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        operation: &'static str,
    ) -> Result<StorageNodeActiveDataScanRoute<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(route_permit, operation)?;
        self.validate_pg_route(node_id, cluster_epoch, pg_id)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        fence.validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        Ok(StorageNodeActiveDataScanRoute {
            handler: self,
            _route_permit: route_permit,
            fence,
            pg_id: self.validated_data_pg(pg_id),
        })
    }

    fn active_primary_object_mutation_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectRequest,
        bucket_write_reservation: &BucketWriteReservationProof,
        operation: &'static str,
    ) -> Result<StorageNodeActivePrimaryObjectRoute<'a>, StorageRpcErrorResponse> {
        if request.bucket != bucket_write_reservation.bucket {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} bucket write reservation proof does not match object bucket"
                ),
            });
        }
        self.active_primary_object_route(route_permit, request, operation)
    }

    fn active_object_payload_lease_control<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectPayloadLeaseControlRequest,
    ) -> Result<StorageNodeActiveObjectPayloadLeaseControl<'a>, StorageRpcErrorResponse> {
        self.validate_object_payload_lease_control_admission(
            route_permit,
            StorageNodeRouteAdmissionClass::Active,
            request,
        )?;
        self.validate_node_epoch(request.node_id, request.route_cluster_epoch)?;
        self.require_current_route_map_valid_rpc()?;
        if !matches!(
            request.operation,
            StorageRpcObjectPayloadLeaseControlOperation::Acquire
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin
                | StorageRpcObjectPayloadLeaseControlOperation::Count
        ) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "object-payload lease operation requires retained cleanup admission"
                    .to_string(),
            });
        }
        Self::validate_object_payload_reclaim_authority(request)?;
        Ok(StorageNodeActiveObjectPayloadLeaseControl {
            handler: self,
            route_permit,
            fence: StorageNodeRouteFence::current(&self.config, self.current_route_map_lease()),
            request,
        })
    }

    fn retained_object_payload_lease_control<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcObjectPayloadLeaseControlRequest,
    ) -> Result<StorageNodeRetainedObjectPayloadLeaseControl<'a>, StorageRpcErrorResponse> {
        self.validate_object_payload_lease_control_admission(
            route_permit,
            StorageNodeRouteAdmissionClass::RetainedCleanup,
            request,
        )?;
        if request.node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "object-payload lease request targets node {}, server is node {}",
                    request.node_id.as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if request.route_cluster_epoch > self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: "object-payload lease cleanup cannot target a future cluster epoch"
                    .to_string(),
            });
        }
        if !matches!(
            request.operation,
            StorageRpcObjectPayloadLeaseControlOperation::Release
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear
        ) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "object-payload lease operation requires active route admission"
                    .to_string(),
            });
        }
        Self::validate_object_payload_reclaim_authority(request)?;
        Ok(StorageNodeRetainedObjectPayloadLeaseControl {
            handler: self,
            route_permit,
            request,
        })
    }

    fn validate_object_payload_lease_control_admission(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        expected: StorageNodeRouteAdmissionClass,
        request: &StorageRpcObjectPayloadLeaseControlRequest,
    ) -> Result<(), StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message:
                    "object-payload lease route permit belongs to a different admission domain"
                        .to_string(),
            });
        }
        if route_permit.class != expected {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "object-payload lease operation {:?} requires {expected:?} admission, got {:?}",
                    request.operation, route_permit.class
                ),
            });
        }
        Ok(())
    }

    fn validate_object_payload_reclaim_authority(
        request: &StorageRpcObjectPayloadLeaseControlRequest,
    ) -> Result<(), StorageRpcErrorResponse> {
        let requires_authority = matches!(
            request.operation,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
                | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear
        );
        match request.reclaim_authority.as_ref() {
            Some(authority)
                if requires_authority && authority.cluster_epoch == request.route_cluster_epoch =>
            {
                Ok(())
            }
            None if !requires_authority => Ok(()),
            Some(_) if !requires_authority => Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "object-payload lease operation must not carry reclaim authority"
                    .to_string(),
            }),
            _ => Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message:
                    "object-payload reclaim operation requires authority from the same route epoch"
                        .to_string(),
            }),
        }
    }

    fn retained_shard_payload_delete_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcShardDeleteRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedShardPayloadDeleteRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_shard_route(
            route_permit,
            request.location.node_id,
            request.location.cluster_epoch,
            request.location.pg_id,
            request.location.shard_index,
            &request.shard_key,
            operation,
        )?;
        Ok(StorageNodeRetainedShardPayloadDeleteRoute {
            handler: self,
            route_permit,
            node_id: request.location.node_id,
            route_cluster_epoch: request.location.cluster_epoch,
            raw_pg_id: request.location.pg_id,
            location: ShardLocation::new(
                request.location.cluster_epoch,
                pg_id,
                request.location.shard_index,
                request.location.node_id,
            ),
            shard_key: &request.shard_key,
        })
    }

    fn retained_read_handle_release_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        session: &'a mut StorageNodeSession,
        request: &StorageRpcReadHandleReleaseRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedReadHandleReleaseRoute<'a>, StorageRpcErrorResponse> {
        self.validate_retained_cleanup_admission(route_permit, operation)?;
        self.validate_read_handle_session(session, operation)?;
        validate_read_handle_release_request(request).map_err(|error| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!("invalid {operation} subject: {error}"),
        })?;
        Ok(StorageNodeRetainedReadHandleReleaseRoute {
            handler: self,
            route_permit,
            session,
            read_operation_id: request.read_operation_id.clone(),
        })
    }

    fn validate_read_handle_session(
        &self,
        session: &StorageNodeSession,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        if Arc::ptr_eq(&session.shared_handles, &self.read_handles)
            && Arc::ptr_eq(&session.node, &self.node)
        {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: format!("{operation} session belongs to a different storage-node domain"),
        })
    }

    fn retained_shard_inspection_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcHistoricalShardReadRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedShardInspectionRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_shard_route(
            route_permit,
            request.location.node_id,
            request.location.cluster_epoch,
            request.location.pg_id,
            request.location.shard_index,
            &request.shard_key,
            operation,
        )?;
        Ok(StorageNodeRetainedShardInspectionRoute {
            handler: self,
            route_permit,
            node_id: request.location.node_id,
            route_cluster_epoch: request.location.cluster_epoch,
            raw_pg_id: request.location.pg_id,
            location: ShardLocation::new(
                request.location.cluster_epoch,
                pg_id,
                request.location.shard_index,
                request.location.node_id,
            ),
            shard_key: &request.shard_key,
        })
    }

    fn retained_shard_ack_delete_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcShardAckItemRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedShardAckDeleteRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            true,
            operation,
        )?;
        Ok(StorageNodeRetainedShardAckDeleteRoute {
            handler: self,
            route_permit,
            node_id: request.node_id,
            route_cluster_epoch: request.cluster_epoch,
            raw_pg_id: request.pg_id,
            pg_id,
            shard_key: &request.shard_key,
        })
    }

    fn retained_shard_ack_inspection_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcShardAckItemRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedShardAckInspectionRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_data_route(
            route_permit,
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
            true,
            operation,
        )?;
        Ok(StorageNodeRetainedShardAckInspectionRoute {
            handler: self,
            route_permit,
            node_id: request.node_id,
            route_cluster_epoch: request.cluster_epoch,
            raw_pg_id: request.pg_id,
            pg_id,
            shard_key: &request.shard_key,
        })
    }

    fn retained_bucket_write_reservation_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        record: &'a BucketWriteReservationRecord,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedBucketWriteReservationRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_bucket_write_reservation_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            record,
            operation,
        )?;
        Ok(StorageNodeRetainedBucketWriteReservationRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id.pg_id(),
            pg_id,
            record,
        })
    }

    fn validate_retained_bucket_write_reservation_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        record: &BucketWriteReservationRecord,
        operation: &'static str,
    ) -> Result<BucketPgId, StorageRpcErrorResponse> {
        self.validate_retained_bucket_write_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            &record.bucket,
            operation,
        )
    }

    fn retained_metadata_command_proof_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        proof: &'a BucketWriteReservationProof,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedMetadataCommandProofRoute<'a>, StorageRpcErrorResponse> {
        if route_cluster_epoch != proof.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} route epoch {} does not match proof epoch {}",
                    route_cluster_epoch.get(),
                    proof.cluster_epoch.get()
                ),
            });
        }
        let pg_id = self.validate_retained_bucket_write_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            &proof.bucket,
            operation,
        )?;
        Ok(StorageNodeRetainedMetadataCommandProofRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id.pg_id(),
            pg_id,
            proof,
        })
    }

    fn retained_bucket_write_drain_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        record: &'a BucketWriteDrainRecord,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedBucketWriteDrainRoute<'a>, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_bucket_write_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            &record.bucket,
            operation,
        )?;
        Ok(StorageNodeRetainedBucketWriteDrainRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id.pg_id(),
            pg_id,
            record,
        })
    }

    fn retained_bucket_delete_finalize_claim_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        claim: &'a BucketDeleteFinalizeClaimRecord,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedBucketDeleteFinalizeClaimRoute<'a>, StorageRpcErrorResponse>
    {
        if route_cluster_epoch != claim.cluster_epoch || pg_id.get() != claim.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} route epoch/PG ({}, {}) does not match claim ({}, {})",
                    route_cluster_epoch.get(),
                    pg_id.get(),
                    claim.cluster_epoch.get(),
                    claim.pg_id
                ),
            });
        }
        let pg_id = self.validate_retained_bucket_write_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            &claim.bucket,
            operation,
        )?;
        Ok(StorageNodeRetainedBucketDeleteFinalizeClaimRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id.pg_id(),
            pg_id,
            claim,
        })
    }

    fn retained_object_payload_reclaim_claim_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        claim: &'a ObjectPayloadReclaimClaimRecord,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedObjectPayloadReclaimClaimRoute<'a>, StorageRpcErrorResponse>
    {
        self.validate_retained_object_payload_reclaim_claim_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            claim,
            operation,
        )?;
        Ok(StorageNodeRetainedObjectPayloadReclaimClaimRoute {
            handler: self,
            route_permit,
            node_id,
            route_cluster_epoch,
            raw_pg_id: pg_id,
            pg_id: self.node.object_metadata_pg_for(&claim.bucket, &claim.key),
            claim,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_retained_object_payload_reclaim_claim_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        claim: &ObjectPayloadReclaimClaimRecord,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        if route_cluster_epoch != claim.cluster_epoch || pg_id.get() != claim.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} route epoch/PG ({}, {}) does not match claim ({}, {})",
                    route_cluster_epoch.get(),
                    pg_id.get(),
                    claim.cluster_epoch.get(),
                    claim.pg_id
                ),
            });
        }
        self.validate_retained_object_route(
            route_permit,
            &StorageRpcObjectRequest {
                node_id,
                cluster_epoch: route_cluster_epoch,
                pg_id,
                bucket: claim.bucket.clone(),
                key: claim.key.clone(),
            },
            true,
            operation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_retained_bucket_write_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        bucket: &BucketName,
        operation: &'static str,
    ) -> Result<BucketPgId, StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::RetainedCleanup {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires retained-cleanup route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        let route = self.cleanup_pg_route(node_id, route_cluster_epoch, pg_id)?;
        if route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on retained PG {} route",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        let expected_pg_id = self.node.bucket_metadata_pg_for(bucket);
        if pg_id != expected_pg_id.pg_id() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} PG {} does not match bucket {} PG {}",
                    pg_id.get(),
                    bucket.as_str(),
                    expected_pg_id.get()
                ),
            });
        }
        Ok(expected_pg_id)
    }

    fn validate_active_admission(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires active route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        Ok(())
    }

    fn validate_retained_cleanup_admission(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        if !Arc::ptr_eq(&route_permit.gate.inner, &self.route_admission.inner) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} route permit belongs to a different admission domain"
                ),
            });
        }
        if route_permit.class != StorageNodeRouteAdmissionClass::RetainedCleanup {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: format!(
                    "{operation} requires retained-cleanup route admission, got {:?}",
                    route_permit.class
                ),
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_retained_shard_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        shard_index: crate::ShardIndex,
        shard_key: &ShardKey,
        operation: &'static str,
    ) -> Result<DataPgId, StorageRpcErrorResponse> {
        let pg_id = self.validate_retained_data_route(
            route_permit,
            node_id,
            route_cluster_epoch,
            pg_id,
            false,
            operation,
        )?;
        if shard_index != shard_key.shard_index() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} location shard index {} does not match shard key index {}",
                    shard_index.get(),
                    shard_key.shard_index().get()
                ),
            });
        }
        Ok(pg_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_retained_data_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        node_id: NodeId,
        route_cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        require_primary: bool,
        operation: &'static str,
    ) -> Result<DataPgId, StorageRpcErrorResponse> {
        self.validate_retained_cleanup_admission(route_permit, operation)?;
        let route = self.cleanup_pg_route(node_id, route_cluster_epoch, pg_id)?;
        if require_primary && route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on retained PG {} route",
                    self.config.node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        Ok(self
            .node
            .data_pg(pg_id)
            .expect("validated retained data PG must belong to installed topology"))
    }

    fn validate_retained_object_route(
        &self,
        route_permit: &StorageNodeRouteAdmissionPermit,
        object: &StorageRpcObjectRequest,
        require_primary: bool,
        operation: &'static str,
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_retained_cleanup_admission(route_permit, operation)?;
        let route = self.cleanup_pg_route(object.node_id, object.cluster_epoch, object.pg_id)?;
        if route.state != PgState::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!(
                    "retained object cleanup PG {} route is {}",
                    object.pg_id.get(),
                    route.state
                ),
            });
        }
        if require_primary && route.primary_node_id != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not primary for {operation} on retained PG {} route",
                    self.config.node_id.as_u32(),
                    object.pg_id.get()
                ),
            });
        }
        self.validate_pg_for_object(object.pg_id, &object.bucket, &object.key, operation)
    }

    fn retained_stream_abort_route_with_primary<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        subject: StorageNodeRetainedStreamAbortSubject<'a>,
        require_primary: bool,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedStreamAbortRoute<'a>, StorageRpcErrorResponse> {
        self.validate_retained_object_route(
            route_permit,
            &StorageRpcObjectRequest {
                node_id: subject.node_id,
                cluster_epoch: subject.route_cluster_epoch,
                pg_id: subject.raw_pg_id,
                bucket: subject.bucket.clone(),
                key: subject.key.clone(),
            },
            require_primary,
            operation,
        )?;
        Ok(StorageNodeRetainedStreamAbortRoute {
            handler: self,
            route_permit,
            node_id: subject.node_id,
            route_cluster_epoch: subject.route_cluster_epoch,
            raw_pg_id: subject.raw_pg_id,
            pg_id: self
                .node
                .object_metadata_pg_for(subject.bucket, subject.key),
            bucket: subject.bucket,
            key: subject.key,
        })
    }

    fn retained_primary_stream_abort_session_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcStreamUploadSessionRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedPrimaryStreamAbortSessionRoute<'a>, StorageRpcErrorResponse>
    {
        let route = self.retained_stream_abort_route_with_primary(
            route_permit,
            StorageNodeRetainedStreamAbortSubject {
                node_id: request.object.node_id,
                route_cluster_epoch: request.object.cluster_epoch,
                raw_pg_id: request.object.pg_id,
                bucket: &request.object.bucket,
                key: &request.object.key,
            },
            true,
            operation,
        )?;
        Ok(StorageNodeRetainedPrimaryStreamAbortSessionRoute {
            route: StorageNodeRetainedPrimaryStreamAbortRoute { route },
            session_id: &request.session_id,
        })
    }

    fn validate_retained_stream_abort_command_subject<'a>(
        request: &'a StorageRpcMetadataCommandRequest,
        operation: &'static str,
    ) -> Result<&'a crate::metadata_command::AbortStreamUploadCommand, StorageRpcErrorResponse>
    {
        validate_metadata_command_request_epoch(request)?;
        if request.command.id().pg_id() != request.pg_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} command PG {} does not match request route PG {}",
                    request.command.id().pg_id().get(),
                    request.pg_id.get()
                ),
            });
        }
        let MetadataCommandPayload::AbortStreamUpload(abort) = request.command.payload() else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!("{operation} accepts only AbortStreamUpload commands"),
            });
        };
        if abort
            .staged_segments
            .iter()
            .any(|segment| segment.session_id != abort.session_id)
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} staged segment does not match the abort stream session"
                ),
            });
        }
        if abort
            .stream_create_bucket_write_reservation
            .as_ref()
            .is_some_and(|proof| {
                proof.bucket != abort.bucket
                    || proof.cluster_epoch != request.cluster_epoch
                    || !is_stream_create_bucket_write_operation_kind(&proof.operation_kind)
                    || proof.target_context.as_deref() != Some(abort.key.as_str())
            })
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: format!(
                    "{operation} stream-create reservation does not match the abort route"
                ),
            });
        }
        Ok(abort)
    }

    fn retained_stream_abort_command_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcMetadataCommandRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedStreamAbortCommandRoute<'a>, StorageRpcErrorResponse> {
        let abort = Self::validate_retained_stream_abort_command_subject(request, operation)?;
        let route = self.retained_stream_abort_route_with_primary(
            route_permit,
            StorageNodeRetainedStreamAbortSubject {
                node_id: request.node_id,
                route_cluster_epoch: request.cluster_epoch,
                raw_pg_id: request.pg_id,
                bucket: &abort.bucket,
                key: &abort.key,
            },
            false,
            operation,
        )?;
        let prepared = PreparedRetainedStreamUploadAbort::new_if_matches(
            route.pg_id,
            request.cluster_epoch,
            &abort.bucket,
            &abort.key,
            &abort.session_id,
            request.command.clone(),
        )
        .ok_or_else(|| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!("{operation} command does not match its validated subject"),
        })?;
        Ok(StorageNodeRetainedStreamAbortCommandRoute { route, prepared })
    }

    fn retained_primary_stream_abort_command_route<'a>(
        &'a self,
        route_permit: &'a StorageNodeRouteAdmissionPermit,
        request: &'a StorageRpcMetadataCommandRequest,
        operation: &'static str,
    ) -> Result<StorageNodeRetainedPrimaryStreamAbortCommandRoute<'a>, StorageRpcErrorResponse>
    {
        let abort = Self::validate_retained_stream_abort_command_subject(request, operation)?;
        let route = self.retained_stream_abort_route_with_primary(
            route_permit,
            StorageNodeRetainedStreamAbortSubject {
                node_id: request.node_id,
                route_cluster_epoch: request.cluster_epoch,
                raw_pg_id: request.pg_id,
                bucket: &abort.bucket,
                key: &abort.key,
            },
            true,
            operation,
        )?;
        let prepared = PreparedRetainedStreamUploadAbort::new_if_matches(
            route.pg_id,
            request.cluster_epoch,
            &abort.bucket,
            &abort.key,
            &abort.session_id,
            request.command.clone(),
        )
        .ok_or_else(|| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: format!("{operation} command does not match its validated subject"),
        })?;
        Ok(StorageNodeRetainedPrimaryStreamAbortCommandRoute {
            route: StorageNodeRetainedPrimaryStreamAbortRoute { route },
            prepared,
        })
    }

    fn unsupported_operation_response(
        &self,
        kind: StorageRpcMessageKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: format!("{kind:?} is not implemented by this storage-node server slice"),
        })
    }
}
