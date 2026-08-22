// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl super::StorageCluster {
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_default_payload_ec_scratch_allocation_count(&self) -> usize {
        let shape = self.default_payload_ec_shape();
        self.metadata_primary_bridge_node()
            .expect("test hook requires a current storage cluster handle")
            .test_ec_scratch_allocation_count(shape)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_metadata_pg_id(bucket)
    }

    pub fn bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_metadata_pg_id(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_head_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.metadata_primary_bridge_node()?
            .test_head_bucket_raw(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_begin_bucket_delete_if_current(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let bucket_info = self
            .head_bucket_info_internal(bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        self.begin_bucket_delete_if_current(
            bucket,
            BucketIdentityGenerations::from_bucket_info(&bucket_info),
        )
    }

    pub fn bucket_delete_diagnostic(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteDiagnostic, crate::BucketSnapshotLoadFailure> {
        self.bucket_delete_diagnostic_internal(bucket)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    /// Render the current standard payload placement for one logical object.
    ///
    /// The snapshot and every physical placement value remain inside storage;
    /// callers receive only owner-rendered text and a bounded outcome.
    pub fn object_payload_placement_diagnostic(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> ObjectPayloadPlacementDiagnostic {
        match self.load_object_read_snapshot_if(
            bucket,
            key,
            None,
            ObjectReadSnapshotMode::StandardSegments,
            |_| Ok::<(), std::convert::Infallible>(()),
        ) {
            Ok(Ok(outcome)) => match outcome.snapshot.payload_placement_diagnostic() {
                Ok(text) => ObjectPayloadPlacementDiagnostic::success(text),
                Err(ObjectPayloadPlacementDiagnosticError::DeleteMarker) => {
                    ObjectPayloadPlacementDiagnostic::conflict(
                        "object payload placement unavailable: delete_marker\n".to_string(),
                    )
                }
                Err(ObjectPayloadPlacementDiagnosticError::NoStandardPayloadSegments) => {
                    ObjectPayloadPlacementDiagnostic::conflict(
                        "object payload placement unavailable: no_standard_payload_segments\n"
                            .to_string(),
                    )
                }
            },
            Ok(Err(never)) => match never {},
            Err(error) => object_payload_placement_failure(error),
        }
    }

    /// Record and render one metadata-command checkpoint diagnostic.
    ///
    /// The caller supplies only the opaque operator selector from the local
    /// debug route. Storage owns its grammar, PG construction, checkpoint
    /// interpretation, failure classification, and diagnostic representation.
    pub fn metadata_checkpoint_diagnostic(&self, selector: &str) -> MetadataCheckpointDiagnostic {
        let pg_id = match parse_metadata_checkpoint_selector(selector) {
            Ok(pg_id) => pg_id,
            Err(diagnostic) => return diagnostic,
        };
        metadata_checkpoint_diagnostic_from_result(
            pg_id,
            self.record_current_metadata_command_checkpoint_for_pg(pg_id),
        )
    }

    fn bucket_delete_diagnostic_internal(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteDiagnostic, BucketSnapshotLoadError> {
        self.bucket_delete_debug_snapshot(bucket)
            .map(|snapshot| BucketDeleteDiagnostic::from_snapshot(&snapshot))
    }

    fn bucket_delete_debug_snapshot(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteDebugSnapshot, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = node.bucket_metadata_client().open_bucket_metadata_route(
            self.operation_epoch(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        let bucket_row = match metadata_route.head_bucket_raw() {
            Ok(info) => Some(BucketDeleteDebugBucketRow {
                state: info.state,
                bucket_execution_generation: info.bucket_execution_generation,
                bucket_incarnation_generation: info.bucket_incarnation_generation,
            }),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => None,
            Err(error) => return Err(error),
        };

        let reservation_client = node.bucket_write_reservation_client();
        let reservation_route = self.open_bucket_write_reservation_route(
            reservation_client.as_ref(),
            self.validated_bucket_metadata_pg(pg_id),
            bucket,
        )?;
        let durable_write_drain = reservation_route
            .durable_bucket_write_drain()?
            .map(|record| BucketDeleteDebugDrain {
                drain_id: record.drain_id,
                cluster_epoch: record.cluster_epoch,
                bucket_execution_generation: record.bucket_execution_generation,
                created_at: record.created_at,
                lease_deadline: record.lease_deadline,
            });

        let pending_metadata_command = self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .map(|command| {
                let id = command.id();
                let target_bucket = command.bucket_name().clone();
                BucketDeleteDebugPendingCommand {
                    kind: command.payload().kind_name(),
                    matches_bucket: target_bucket == *bucket,
                    target_bucket,
                    cluster_epoch: id.cluster_epoch(),
                    pg_id: id.pg_id().get(),
                    log_index: id.log_index().get(),
                }
            });

        let finalize_claim = reservation_route
            .bucket_delete_finalize_claim()?
            .map(|record| BucketDeleteDebugFinalizeClaim {
                matches_bucket: record.bucket == *bucket,
                bucket: record.bucket,
                bucket_incarnation_generation: record.bucket_incarnation_generation,
                claim_id: record.claim_id,
                cluster_epoch: record.cluster_epoch,
                pg_id: record.pg_id,
                claimed_at: record.claimed_at,
                lease_deadline: record.lease_deadline,
                attempt_count: record.attempt_count,
                last_error: record.last_error,
            });

        let attempt_outcome = reservation_route.bucket_delete_attempt_outcome()?;

        let mut object_version_samples = Vec::new();
        let mut object_version_sample_errors = Vec::new();
        let mut payload_reclaim_roots = Vec::new();
        let mut payload_reclaim_root_errors = Vec::new();
        let mut payload_reclaim_claims = Vec::new();
        let mut payload_reclaim_claim_errors = Vec::new();
        for raw_pg_id in self.metadata_pg_ids() {
            let object_pg_id = PgId::new(raw_pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(object_pg_id);
            let node = match self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), object_pg_id)
            {
                Ok(node) => node,
                Err(error) => {
                    let detail = error.to_string();
                    object_version_sample_errors.push(BucketDeleteDebugObjectVersionSampleError {
                        object_pg_id: raw_pg_id,
                        detail: detail.clone(),
                    });
                    payload_reclaim_claim_errors.push(BucketDeleteDebugPayloadReclaimClaimError {
                        object_pg_id: raw_pg_id,
                        detail: detail.clone(),
                    });
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail,
                    });
                    continue;
                }
            };
            match node
                .object_listing_metadata_client()
                .open_object_listing_metadata_route(
                    self.operation_epoch(),
                    scan_pg_id,
                    MetadataReadAuthorization::active(scan_pg_id.pg_id()),
                )
                .and_then(|route| {
                    route.list_object_versions_page(&ListObjectVersionsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        key_marker: None,
                        version_id_marker: None,
                        start_at: None,
                        max_keys: 1,
                    })
                }) {
                Ok(resp) => {
                    if let Some(stored) = resp.versions.into_iter().next() {
                        object_version_samples
                            .push(Self::bucket_delete_debug_object_sample(raw_pg_id, stored));
                    }
                }
                Err(error) => {
                    object_version_sample_errors.push(BucketDeleteDebugObjectVersionSampleError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                }
            }
            let scan_route = match node
                .object_mutation_metadata_client()
                .open_object_mutation_scan_metadata_route(self.operation_epoch(), scan_pg_id)
            {
                Ok(route) => route,
                Err(error) => {
                    let detail = error.to_string();
                    payload_reclaim_claim_errors.push(BucketDeleteDebugPayloadReclaimClaimError {
                        object_pg_id: raw_pg_id,
                        detail: detail.clone(),
                    });
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail,
                    });
                    continue;
                }
            };
            match scan_route.object_payload_reclaim_claim() {
                Ok(Some(claim)) => {
                    payload_reclaim_claims.push(BucketDeleteDebugPayloadReclaimClaim {
                        object_pg_id: raw_pg_id,
                        matches_bucket: claim.bucket == *bucket,
                        bucket: claim.bucket,
                        bucket_incarnation_generation: claim.bucket_incarnation_generation,
                        key: claim.key,
                        generation_id: claim.generation_id,
                        reclaim_kind: claim.reclaim_kind,
                        claim_id: claim.claim_id,
                        cluster_epoch: claim.cluster_epoch,
                        claimed_at: claim.claimed_at,
                        lease_deadline: claim.lease_deadline,
                        attempt_count: claim.attempt_count,
                        last_error: claim.last_error,
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    payload_reclaim_claim_errors.push(BucketDeleteDebugPayloadReclaimClaimError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                }
            }
            let root = match scan_route.get_bucket_payload_reclaim_root(bucket) {
                Ok(Some(root)) => root,
                Ok(None) => continue,
                Err(error) => {
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                    continue;
                }
            };
            if let Err(error) = self.validate_bucket_payload_reclaim_root_for_pg(
                object_pg_id,
                &root,
                node.node_id(),
            ) {
                payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                    object_pg_id: raw_pg_id,
                    detail: error.to_string(),
                });
                continue;
            }
            let reclaim_route = node
                .object_mutation_metadata_client()
                .open_object_payload_reclaim_metadata_route(
                    self.operation_epoch(),
                    self.object_metadata_pg(&root.bucket, &root.key),
                    &root.bucket,
                    &root.key,
                    root.generation_id,
                );
            let reclaim_details = match reclaim_route.and_then(|route| {
                route
                    .load_payload()
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
            }) {
                Ok(Some(reclaim)) => Some(Self::bucket_delete_debug_reclaim_details(&reclaim)),
                Ok(None) => None,
                Err(error) => {
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                    None
                }
            };
            let (reclaim_kind, reclaim_created_at, reclaim_item_count) =
                reclaim_details.unwrap_or((None, None, None));
            payload_reclaim_roots.push(BucketDeleteDebugPayloadReclaimRoot {
                object_pg_id: raw_pg_id,
                key: root.key,
                generation_id: root.generation_id,
                reclaim_kind,
                reclaim_created_at,
                reclaim_item_count,
            });
        }

        Ok(BucketDeleteDebugSnapshot {
            bucket: bucket.clone(),
            pg_id: pg_id.get(),
            cluster_epoch: self.cluster_epoch(),
            operation_epoch: self.operation_epoch(),
            route_map_valid_until_ms: self.route_map_valid_until_ms(),
            bucket_pg_primary_node_id: node.node_id().as_u32(),
            bucket_row,
            durable_write_drain,
            pending_metadata_command,
            finalize_claim,
            object_version_samples,
            object_version_sample_errors,
            payload_reclaim_roots,
            payload_reclaim_root_errors,
            payload_reclaim_claims,
            payload_reclaim_claim_errors,
            attempt_outcome,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_bucket_delete_progress(
        &self,
        bucket: &BucketName,
    ) -> Result<crate::TestBucketDeleteProgress, BucketSnapshotLoadError> {
        let snapshot = self.bucket_delete_debug_snapshot(bucket)?;
        Ok(crate::TestBucketDeleteProgress {
            bucket_state: snapshot.bucket_row.map(|row| row.state),
            has_durable_write_drain: snapshot.durable_write_drain.is_some(),
            has_pending_metadata_command: snapshot.pending_metadata_command.is_some(),
        })
    }

    fn bucket_delete_debug_object_sample(
        object_pg_id: u32,
        stored: StoredObject,
    ) -> BucketDeleteDebugObjectVersionSample {
        match stored {
            StoredObject::Live(record) => BucketDeleteDebugObjectVersionSample {
                object_pg_id,
                kind: BucketDeleteDebugObjectVersionKind::Live,
                key: record.key,
                version_id: record.version_id,
                generation_id: Some(record.generation_id),
                size: Some(record.size),
                layout: Some(record.layout),
                last_modified: record.last_modified,
                became_noncurrent_at: record.became_noncurrent_at,
            },
            StoredObject::DeleteMarker(record) => BucketDeleteDebugObjectVersionSample {
                object_pg_id,
                kind: BucketDeleteDebugObjectVersionKind::DeleteMarker,
                key: record.key,
                version_id: record.version_id,
                generation_id: None,
                size: None,
                layout: None,
                last_modified: record.last_modified,
                became_noncurrent_at: None,
            },
        }
    }

    fn bucket_delete_debug_reclaim_details(
        reclaim: &ObjectPayloadReclaimCommand,
    ) -> (Option<ObjectPayloadReclaimKind>, Option<u64>, Option<usize>) {
        match reclaim {
            ObjectPayloadReclaimCommand::Segments(record) => (
                Some(ObjectPayloadReclaimKind::ObjectSegments),
                Some(record.created_at),
                Some(record.segments.len()),
            ),
            ObjectPayloadReclaimCommand::Multipart(record) => (
                Some(ObjectPayloadReclaimKind::Multipart),
                Some(record.created_at),
                Some(record.parts.len()),
            ),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_metadata_pg_id(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> u32 {
        self.local_map
            .object_generation_segment_data_pg(bucket, key, generation_id, 0)
            .get()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_generation_reservation_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_object_generation_reservation_for(bucket, key, reservation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_meta(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_multipart_upload(bucket, key, upload_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_multipart_completion_candidate(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadCompletionCandidate, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(crate::MultipartUploadCompletionCandidate::from_record)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_multipart_part_observation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u16,
    ) -> Result<crate::TestMultipartPartObservation, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_multipart_part(bucket, key, upload_id, part_number)
            .map(crate::TestMultipartPartObservation::from_record)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_multipart_uploads_for_bucket(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_capture_object_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<crate::TestObjectPayloadSnapshot, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_capture_object_payload(bucket, key, version_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_is_fully_present(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_object_payload_snapshot_matches_presence(snapshot, true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_is_fully_absent(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_object_payload_snapshot_matches_presence(snapshot, false)
    }

    /// Injects loss of one placed shard file from a captured committed payload.
    ///
    /// The durable acknowledgement is intentionally retained so reads exercise
    /// the production missing-file reconstruction path. Physical placement and
    /// shard identity remain owned by storage.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_object_payload_shard_loss(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<(), StoreError> {
        let path = self.test_object_payload_shard_file_path_from_snapshot(
            snapshot,
            segment_index,
            shard_index,
        )?;
        std::fs::remove_file(path).map_err(|source| StoreError::Io {
            context: "inject object payload shard loss",
            source,
        })
    }

    /// Injects corruption of one placed shard file from a captured payload.
    ///
    /// The durable acknowledgement is intentionally left unchanged so the
    /// production read path discovers the checksum mismatch.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_object_payload_shard_corruption(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<(), StoreError> {
        let path = self.test_object_payload_shard_file_path_from_snapshot(
            snapshot,
            segment_index,
            shard_index,
        )?;
        let mut data = std::fs::read(&path).map_err(|source| StoreError::Io {
            context: "read object payload shard for corruption",
            source,
        })?;
        let first = data.first_mut().ok_or_else(|| StoreError::Io {
            context: "corrupt object payload shard",
            source: std::io::Error::other("placed payload shard is empty"),
        })?;
        *first ^= 0xff;
        std::fs::write(path, data).map_err(|source| StoreError::Io {
            context: "write corrupt object payload shard",
            source,
        })
    }

    /// Reports whether one captured shard file still matches its durable ack.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_shard_file_matches_ack(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<bool, StoreError> {
        let (segment, location, shard_key, path) =
            self.test_object_payload_shard_from_snapshot(snapshot, segment_index, shard_index)?;
        let route = self.reconstructed_pg_route_at_epoch(
            location.data_pg_id().pg_id(),
            segment.placement_cluster_epoch,
        )?;
        let expected = self.load_payload_shard_ack_for_pg_route_snapshot(
            &route,
            segment.data_pg_id,
            &shard_key,
        )?;
        let data = match std::fs::read(path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(StoreError::Io {
                    context: "inspect captured object payload shard",
                    source,
                });
            }
        };
        Ok(data.len() as u64 == expected.stored_size
            && checksum::crc64::checksum(&data) == expected.crc64)
    }

    /// Verifies that opaque payload evidence was written using the data-PG
    /// derivation and placement epoch of this cluster generation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_uses_current_placement(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select object payload generation for placement observation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        Ok(!snapshot.segments().is_empty()
            && snapshot.segments().iter().all(|segment| {
                segment.placement_cluster_epoch == self.operation_epoch()
                    && segment.data_pg_id
                        == self
                            .local_map
                            .object_generation_segment_data_pg(
                                &segment.bucket,
                                &segment.key,
                                generation_id,
                                segment.segment_index,
                            )
                            .get()
            }))
    }

    /// Reports whether the exact shard-owner set selected by a captured
    /// payload currently holds deletion-exclusion leases for that generation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_has_exact_shard_owner_leases(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        let first = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload for lease observation",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if snapshot
            .segments()
            .iter()
            .any(|segment| segment.bucket != first.bucket || segment.key != first.key)
        {
            return Err(StoreError::Io {
                context: "validate object payload lease observation",
                source: std::io::Error::other(
                    "captured object payload contains multiple object generations",
                ),
            });
        }
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select object payload generation for lease observation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        let mut expected_nodes = HashSet::new();
        for segment in snapshot.segments() {
            let request = SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: 0,
                segment_crc64: segment.segment_crc64,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            };
            expected_nodes.extend(
                self.segment_payload_shard_locations_at_placement_epoch(
                    segment.placement_cluster_epoch,
                    &request,
                )?
                .into_iter()
                .map(|location| location.node_id()),
            );
        }
        Ok(expected_nodes
            == self.local_map.object_payload_lease_holder_node_ids(
                &first.bucket,
                &first.key,
                generation_id,
            ))
    }

    /// Reports whether a captured payload generation has no remaining
    /// deletion-exclusion lease on any storage node.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_has_no_leases(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        let first = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload for lease observation",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if snapshot
            .segments()
            .iter()
            .any(|segment| segment.bucket != first.bucket || segment.key != first.key)
        {
            return Err(StoreError::Io {
                context: "validate object payload lease observation",
                source: std::io::Error::other(
                    "captured object payload contains multiple object generations",
                ),
            });
        }
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select object payload generation for lease observation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        Ok(self
            .local_map
            .object_payload_lease_holder_node_ids(&first.bucket, &first.key, generation_id)
            .is_empty())
    }

    /// Acquires one opaque generation-wide deletion-exclusion lease for a
    /// captured test payload without exposing its storage generation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_acquire_object_payload_lease_for_snapshot(
        self: &std::sync::Arc<Self>,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<ObjectPayloadLease, StoreError> {
        let first = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload for test lease",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if snapshot
            .segments()
            .iter()
            .any(|segment| segment.bucket != first.bucket || segment.key != first.key)
        {
            return Err(StoreError::Io {
                context: "validate object payload test lease",
                source: std::io::Error::other(
                    "captured object payload contains multiple object generations",
                ),
            });
        }
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select object payload generation for test lease",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        self.acquire_object_payload_lease(&first.bucket, &first.key, generation_id)
    }

    /// Returns logical repair observations belonging to a captured payload.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_repair_observations(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<Vec<crate::TestObjectPayloadRepairObservation>, StoreError> {
        let mut observations = Vec::new();
        let mut scanned_pgs = HashSet::new();
        for segment in snapshot.segments() {
            if !scanned_pgs.insert(segment.data_pg_id) {
                continue;
            }
            for repair in self.list_placed_segment_shard_repairs(segment.data_pg_id)? {
                if let Some(target) =
                    Self::test_object_payload_repair_segment(snapshot, &repair.work_item)
                {
                    observations.push(crate::TestObjectPayloadRepairObservation {
                        segment_index: target.segment_index,
                        shard_index: repair.work_item.shard_index.get(),
                        last_error: repair.last_error,
                    });
                }
            }
        }
        observations.sort_by_key(|repair| (repair.segment_index, repair.shard_index));
        Ok(observations)
    }

    /// Creates the durable repair record and in-memory wake for one exact
    /// storage-selected fault. The physical subject remains crate-private.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_schedule_object_payload_repair_wake(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<(), StoreError> {
        let segment = snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload segment for repair wake",
                source: std::io::Error::other(format!(
                    "captured payload has no segment {segment_index}"
                )),
            })?;
        if shard_index >= segment.ec_k.saturating_add(segment.ec_m) {
            return Err(StoreError::Io {
                context: "select object payload shard for repair wake",
                source: std::io::Error::other(format!(
                    "shard {shard_index} is outside the captured EC layout"
                )),
            });
        }
        let stored_size = snapshot
            .stored_size_for(segment)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload stored size for repair wake",
                source: std::io::Error::other("captured payload stored size is invalid"),
            })?;
        self.schedule_placed_segment_shard_repair(
            SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size,
                segment_crc64: segment.segment_crc64,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            ShardIndex::new(shard_index),
        )
    }

    /// Atomically consumes the exact repair wake selected from a captured
    /// payload without disturbing another shard's wake.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_take_object_payload_repair_wake(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<bool, StoreError> {
        Ok(self
            .try_take_matching_placed_segment_shard_repair_work(|work_item| {
                Self::test_object_payload_repair_segment(snapshot, work_item).is_some_and(
                    |segment| {
                        segment.segment_index == segment_index
                            && work_item.shard_index.get() == shard_index
                    },
                )
            })
            .is_some())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_object_payload_repair_segment<'a>(
        snapshot: &'a crate::TestObjectPayloadSnapshot,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Option<&'a ObjectSegmentRecord> {
        snapshot.segments().iter().find(|segment| {
            segment.data_pg_id == work_item.request.data_pg_id
                && segment.segment_okh == work_item.request.segment_okh
                && segment.segment_vid == work_item.request.segment_vid
                && snapshot.stored_size_for(segment) == Some(work_item.request.stored_size)
                && segment.segment_crc64 == work_item.request.segment_crc64
                && segment.ec_k == work_item.request.ec.k
                && segment.ec_m == work_item.request.ec.m
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_object_payload_shard_file_path_from_snapshot(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<std::path::PathBuf, StoreError> {
        self.test_object_payload_shard_from_snapshot(snapshot, segment_index, shard_index)
            .map(|(_, _, _, path)| path)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_object_payload_shard_from_snapshot(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    ) -> Result<
        (
            ObjectSegmentRecord,
            ShardLocation,
            ShardKey,
            std::path::PathBuf,
        ),
        StoreError,
    > {
        let segment = snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload segment for fault injection",
                source: std::io::Error::other(format!(
                    "captured payload has no segment {segment_index}"
                )),
            })?;
        let ec = EcShape {
            k: segment.ec_k,
            m: segment.ec_m,
        };
        let request = SegmentStoredBytesRequest {
            data_pg_id: segment.data_pg_id,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            stored_size: 0,
            segment_crc64: segment.segment_crc64,
            ec,
        };
        let locations = self.segment_payload_shard_locations_at_placement_epoch(
            segment.placement_cluster_epoch,
            &request,
        )?;
        let location = locations
            .into_iter()
            .find(|location| location.shard_index().get() == shard_index)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload shard for fault injection",
                source: std::io::Error::other(format!(
                    "shard index {shard_index} outside captured segment EC layout"
                )),
            })?;
        let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), shard_index);
        let node = self
            .local_map
            .node(location.node_id())
            .ok_or(StoreError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            })?;
        let path = node
            .data_dir()
            .join(format!("pg-{:04}", location.data_pg_id().get()))
            .join("shards")
            .join(shard_key.hex_prefix())
            .join(shard_key.hex());
        Ok((segment.clone(), location, shard_key, path))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_object_payload_snapshot_matches_presence(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        expected: bool,
    ) -> Result<bool, StoreError> {
        for segment in snapshot.segments() {
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            let request = SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: 0,
                segment_crc64: segment.segment_crc64,
                ec,
            };
            let locations = self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch,
                &request,
            )?;
            for location in locations {
                let shard_key = ShardKey::new(
                    &segment.segment_okh,
                    segment.segment_vid.get(),
                    location.shard_index().get(),
                );
                if self.test_placed_payload_shard_row_exists(location, &shard_key)? != expected
                    || self.test_placed_payload_shard_file_exists(location, &shard_key)? != expected
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        for segment in snapshot.segments() {
            let request = SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: 0,
                segment_crc64: segment.segment_crc64,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            };
            let locations = self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch,
                &request,
            )?;
            let node_count = locations
                .iter()
                .map(|location| location.node_id())
                .collect::<HashSet<_>>()
                .len();
            if node_count != locations.len() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_uses_generation_layout(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select committed object payload generation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        for segment in snapshot.segments() {
            let expected_hash = segment_key_hash(
                segment.bucket.as_str(),
                segment.key.as_str(),
                generation_id,
                segment.segment_index,
            );
            let expected_pg = self
                .local_map
                .object_generation_segment_data_pg(
                    &segment.bucket,
                    &segment.key,
                    generation_id,
                    segment.segment_index,
                )
                .get();
            if segment.segment_okh != expected_hash || segment.data_pg_id != expected_pg {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_uses_transient_direct_put_layout(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select transient direct PUT payload generation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        for segment in snapshot.segments() {
            let generation_hash = segment_key_hash(
                segment.bucket.as_str(),
                segment.key.as_str(),
                generation_id,
                segment.segment_index,
            );
            let expected_pg = self
                .local_map
                .object_generation_segment_data_pg(
                    &segment.bucket,
                    &segment.key,
                    generation_id,
                    segment.segment_index,
                )
                .get();
            if segment.segment_okh == generation_hash
                || segment.segment_vid != generation_id
                || segment.data_pg_id != expected_pg
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_snapshot_uses_stream_session_layout(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
        session_id: &SessionId,
    ) -> Result<bool, StoreError> {
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select stream PUT payload generation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        for segment in snapshot.segments() {
            let expected_hash = crate::stream_segment_key_hash(session_id, segment.segment_index);
            let expected_pg = self
                .local_map
                .object_generation_segment_data_pg(
                    &segment.bucket,
                    &segment.key,
                    generation_id,
                    segment.segment_index,
                )
                .get();
            if segment.segment_okh != expected_hash || segment.data_pg_id != expected_pg {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Injects a storage-owned read-route failure into the first segment of an
    /// exact captured payload.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_object_payload_first_segment_unknown_data_pg(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<(), ObjectPgActionError> {
        let first = snapshot.segments().first().ok_or(StoreError::NotFound)?;
        let generation_id = snapshot.generation_id().ok_or(StoreError::NotFound)?;
        self.metadata_primary_bridge_node()?
            .test_inject_exact_object_segment_unknown_data_pg(first, generation_id)
    }

    /// Injects a storage-owned checksum mismatch into the first segment of an
    /// exact captured payload.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_object_payload_first_segment_checksum_mismatch(
        &self,
        snapshot: &crate::TestObjectPayloadSnapshot,
    ) -> Result<(), ObjectPgActionError> {
        let first = snapshot.segments().first().ok_or(StoreError::NotFound)?;
        let generation_id = snapshot.generation_id().ok_or(StoreError::NotFound)?;
        self.metadata_primary_bridge_node()?
            .test_inject_exact_object_segment_checksum_mismatch(first, generation_id)
    }

    /// Returns the logical part numbers in one committed multipart manifest.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_object_part_numbers(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<u32>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_parts(bucket, key, version_id)
            .map(|parts| parts.into_iter().map(|part| part.part_number).collect())
    }

    /// Injects a storage-owned payload-checksum mismatch for one logical part.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_object_part_payload_checksum_mismatch(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_corrupt_object_part_payload_crc64(bucket, key, version_id, part_number)
    }

    /// Injects a storage-owned incomplete-manifest fault at one logical part.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_inject_incomplete_multipart_manifest(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_remove_object_part(bucket, key, version_id, part_number)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_version(bucket, key, version_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_replace_object_encryption(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        encryption: &ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_replace_object_encryption(bucket, key, version_id, encryption)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_observe_stored_encrypted_checksum(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        cleartext_checksum: &str,
    ) -> Result<crate::TestStoredEncryptedChecksumObservation, ObjectPgActionError> {
        if cleartext_checksum.is_empty() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "encrypted checksum observation requires a nonempty cleartext value"
                    .to_string(),
            });
        }
        let stored = self.test_get_object_version(bucket, key, version_id)?;
        let live = stored
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "selected encrypted checksum observation is not a live object".to_string(),
            })?;
        let encrypted_checksum_metadata = match &live.encryption {
            ObjectEncryption::SseCustomer(encryption) => {
                encryption.encrypted_checksum_metadata()
            }
            ObjectEncryption::SseS3(encryption) => encryption.encrypted_checksum_metadata(),
            ObjectEncryption::None => {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "selected checksum observation is not encrypted".to_string(),
                });
            }
        };
        let cleartext_checksum = cleartext_checksum.as_bytes();
        let contains_supplied_cleartext =
            live.system_metadata_blob.as_ref().is_some_and(|metadata| {
                metadata
                    .as_slice()
                    .windows(cleartext_checksum.len())
                    .any(|window| window == cleartext_checksum)
            });
        Ok(crate::TestStoredEncryptedChecksumObservation {
            has_encrypted_checksum: !encrypted_checksum_metadata.is_empty(),
            contains_supplied_cleartext,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_encrypted_object_states_are_distinct(
        &self,
        left_bucket: &BucketName,
        left_key: &ObjectKey,
        left_version_id: VersionId,
        right_bucket: &BucketName,
        right_key: &ObjectKey,
        right_version_id: VersionId,
    ) -> Result<bool, ObjectPgActionError> {
        let left = self.test_get_object_version(left_bucket, left_key, left_version_id)?;
        let right = self.test_get_object_version(right_bucket, right_key, right_version_id)?;
        let left = left
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "left encryption-state observation is not a live object".to_string(),
            })?;
        let right = right
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "right encryption-state observation is not a live object".to_string(),
            })?;
        if matches!(left.encryption, ObjectEncryption::None)
            || matches!(right.encryption, ObjectEncryption::None)
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "encryption-state comparison requires two encrypted objects".to_string(),
            });
        }
        Ok(left.encryption != right.encryption)
    }

    /// Observes only whether the durable segmented-payload reclaim root exists.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<()>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_segments_reclaim(bucket, key, generation_id)
            .map(|reclaim| reclaim.map(|_| ()))
    }

    /// Seeds the canonical storage-owned reclaim scenario for one orphaned
    /// segmented-object generation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_seed_segmented_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        created_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let reclaim = ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: crate::object_key_hash(bucket.as_str(), key.as_str()),
                segment_vid: generation_id,
                data_pg_id: self
                    .local_map
                    .object_generation_segment_data_pg(bucket, key, generation_id, 0)
                    .get(),
                ec: self.default_payload_ec_shape(),
            }],
        };
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.put_object_segments_reclaim(&reclaim)?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_next_unreferenced_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mut candidate = None;
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let node_candidate = node
                .test_node()
                .test_next_unreferenced_object_generation(bucket, key)?;
            match candidate {
                None => candidate = Some(node_candidate),
                Some(expected) if expected == node_candidate => {}
                Some(_) => {
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: "acting set disagrees on the next unreferenced object generation"
                            .to_string(),
                    });
                }
            }
        }
        candidate.ok_or_else(|| ObjectPgActionError::InvalidRequest {
            reason: "object metadata acting set is empty".to_string(),
        })
    }

    /// Seeds the canonical storage-owned reclaim scenario for one orphaned
    /// multipart-object generation.
    #[cfg(test)]
    pub(crate) fn test_seed_multipart_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        created_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let reclaim = MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at,
            parts: vec![MultipartReclaimPartRecord {
                part_number: 1,
                segments: vec![MultipartReclaimPartSegmentRecord {
                    part_number: 1,
                    segment_index: 0,
                    segment_okh: crate::object_key_hash(bucket.as_str(), key.as_str()),
                    segment_vid: generation_id,
                    data_pg_id: self
                        .local_map
                        .object_generation_multipart_part_data_pg(bucket, key, generation_id, 1)
                        .get(),
                    ec: self.default_payload_ec_shape(),
                }],
            }],
        };
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.put_multipart_reclaim(&reclaim)?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_payload_reclaim_exists(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_payload_reclaim_count_for_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_payload_reclaim_count_for_object(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_reclaim_is_active(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.local_map
            .test_object_payload_reclaim_is_active(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_age_noncurrent_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .test_age_noncurrent_live_object(bucket, key, version_id, became_noncurrent_at)
    }

    #[cfg(test)]
    pub(crate) fn test_create_and_enqueue_deleting_bucket_finalize(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let owner_canonical_id = CanonicalUserId::from_principal("default-owner");
        let acl_grants = AclGrants::default();
        let create = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "default-owner",
            owner_canonical_id: &owner_canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let _ = self
            .create_bucket_with_config_and_load_info_raw(&create)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        self.test_begin_bucket_delete_if_current(bucket)?;
        self.test_enqueue_current_bucket_delete_finalize(bucket)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_delete_bucket_metadata(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_route = node
            .bucket_metadata_client()
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let info = metadata_route
            .head_bucket_raw()
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: info.bucket_incarnation_generation,
        };
        match self.delete_bucket_from_acting_set(pg_id, &root)? {
            BucketDeleteFinalizeOutcome::Finalized => Ok(()),
            BucketDeleteFinalizeOutcome::NotFound => {
                Err(crate::error::MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into())
            }
            BucketDeleteFinalizeOutcome::NotDeleting
            | BucketDeleteFinalizeOutcome::StaleIncarnation
            | BucketDeleteFinalizeOutcome::Continue
            | BucketDeleteFinalizeOutcome::Pending => {
                unreachable!("test bucket metadata delete bypasses finalization checks")
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_capture_multipart_upload_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<crate::TestMultipartPartPayloadSnapshot, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_all_multipart_part_segments_for_upload(bucket, key, upload_id)
            .map(crate::TestMultipartPartPayloadSnapshot::new)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_capture_multipart_part_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<crate::TestMultipartPartPayloadSnapshot, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_all_multipart_part_segments_for_upload(bucket, key, upload_id)
            .map(|segments| {
                crate::TestMultipartPartPayloadSnapshot::new(
                    segments
                        .into_iter()
                        .filter(|segment| segment.part_number == part_number)
                        .collect(),
                )
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_multipart_part_payload_snapshot_is_fully_present(
        &self,
        snapshot: &crate::TestMultipartPartPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_multipart_part_payload_snapshot_matches_presence(snapshot, true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_multipart_part_payload_snapshot_is_fully_absent(
        &self,
        snapshot: &crate::TestMultipartPartPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_multipart_part_payload_snapshot_matches_presence(snapshot, false)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_multipart_part_payload_snapshot_matches_presence(
        &self,
        snapshot: &crate::TestMultipartPartPayloadSnapshot,
        expected: bool,
    ) -> Result<bool, StoreError> {
        for segment in snapshot.segments() {
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            let request = SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: 0,
                segment_crc64: segment.segment_crc64,
                ec,
            };
            let locations = self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch,
                &request,
            )?;
            for location in locations {
                let shard_key = ShardKey::new(
                    &segment.segment_okh,
                    segment.segment_vid.get(),
                    location.shard_index().get(),
                );
                if self.test_placed_payload_shard_row_exists(location, &shard_key)? != expected
                    || self.test_placed_payload_shard_file_exists(location, &shard_key)? != expected
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    /// Seeds a storage-owned stale-incarnation lifecycle claim scenario.
    pub(crate) fn test_seed_stale_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        let pg = self.metadata_pg(self.bucket_metadata_pg_id(bucket))?;
        let bucket_incarnation_generation = pg
            .head_bucket_raw(bucket)?
            .bucket_incarnation_generation
            .saturating_sub(1);
        pg.test_insert_lifecycle_sweep_claim(
            bucket,
            bucket_incarnation_generation,
            "test-lifecycle-claim",
            "test-owner-token",
            self.operation_epoch(),
            Some(50),
        )?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_begin_durable_bucket_delete_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        match self.begin_durable_bucket_delete_drain(bucket)? {
            super::DurableBucketDeleteDrainBegin::Acquired(_)
            | super::DurableBucketDeleteDrainBegin::AlreadyDeleting => Ok(()),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    /// Seeds the storage-owned terminal state used to test abort recovery.
    pub(crate) fn test_mark_multipart_upload_aborting(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        self.test_set_upload_state(bucket, key, upload_id, UploadState::Aborting)
    }

    /// Seeds the storage-owned terminal state used to test completion recovery.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_mark_multipart_upload_completing(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<(), ObjectPgActionError> {
        self.test_set_upload_state(bucket, key, upload_id, UploadState::Completing)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_set_upload_state(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        state: UploadState,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            node.test_node()
                .test_set_upload_state(bucket, key, upload_id, state)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_list_stream_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_stream_segments(bucket, key, session_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_capture_stream_upload_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<crate::TestStreamUploadPayloadSnapshot, ObjectPgActionError> {
        self.test_list_stream_segments(bucket, key, session_id)
            .map(crate::TestStreamUploadPayloadSnapshot::new)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_stream_upload_payload_snapshot_is_fully_present(
        &self,
        snapshot: &crate::TestStreamUploadPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_stream_upload_payload_snapshot_matches_presence(snapshot, true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_stream_upload_payload_snapshot_is_fully_absent(
        &self,
        snapshot: &crate::TestStreamUploadPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        self.test_stream_upload_payload_snapshot_matches_presence(snapshot, false)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_stream_upload_payload_snapshot_matches_presence(
        &self,
        snapshot: &crate::TestStreamUploadPayloadSnapshot,
        expected: bool,
    ) -> Result<bool, StoreError> {
        for segment in snapshot.segments() {
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            let request = SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: 0,
                segment_crc64: segment.segment_crc64,
                ec,
            };
            let locations = self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch,
                &request,
            )?;
            for location in locations {
                let shard_key = ShardKey::new(
                    &segment.segment_okh,
                    segment.segment_vid.get(),
                    location.shard_index().get(),
                );
                if self.test_placed_payload_shard_row_exists(location, &shard_key)? != expected
                    || self.test_placed_payload_shard_file_exists(location, &shard_key)? != expected
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    /// Marks one logical stream upload stale without exposing its durable
    /// timestamp representation across the storage boundary.
    pub(crate) fn test_mark_stream_upload_stale(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            node.test_node()
                .test_mark_stream_upload_stale(bucket, key, session_id)?;
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_list_all_stream_uploads(
        &self,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_all_stream_uploads()
    }

    #[cfg(test)]
    pub(crate) fn test_create_stream_upload(
        &self,
        req: &CreateStreamUploadReq,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_create_stream_upload(req)
    }

    #[cfg(test)]
    pub(crate) fn test_force_stream_upload_cleanup_after(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        cleanup_after: Option<u64>,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_force_stream_upload_cleanup_after(bucket, key, session_id, cleanup_after)
    }

    #[cfg(test)]
    pub(crate) fn test_shard_exists(&self, pg_id: u32, key: &ShardKey) -> Result<bool, StoreError> {
        self.metadata_primary_bridge_node()?
            .test_shard_exists(pg_id, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_lock_bucket_pg(
        &self,
        bucket: &BucketName,
    ) -> Result<crate::node::BucketPgTestGuard<'_>, StoreError> {
        self.metadata_primary_bridge_node()?
            .test_lock_bucket_pg(bucket)
    }
}
