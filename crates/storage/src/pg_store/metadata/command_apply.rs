// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl PgStore {
    #[cfg(test)]
    pub fn create_bucket_with_config(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<(), MetadataError> {
        self.create_bucket_with_config_inner(
            config,
            PgStore::now_millis(),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn create_bucket_with_config_inner(
        &self,
        config: &CreateBucketConfig<'_>,
        created_at_millis: u64,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_bucket_with_config",
            "pg_id={} bucket={:?} owner={} versioning={:?} object_lock_enabled={}",
            self.pg_id,
            config.name,
            config.owner_principal,
            config.versioning,
            config.object_lock.enabled
        );
        Self::validate_bucket_object_lock_transition(
            config.versioning,
            BucketObjectLockConfig::default(),
            config.object_lock,
            "create bucket",
        )?;
        let created_at_millis =
            i64::try_from(created_at_millis).map_err(|_| MetadataError::Db {
                context: "create bucket (encode created_at)",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                    "bucket created_at exceeds i64",
                )),
            })?;
        let (
            object_lock_enabled,
            object_lock_default_mode,
            object_lock_default_days,
            object_lock_default_years,
        ) = Self::bucket_object_lock_sql_values(config.object_lock).map_err(|e| {
            MetadataError::Db {
                context: "create bucket (encode object lock)",
                source: e.into(),
            }
        })?;
        let multipart_upload_id_key =
            MultipartUploadIdKey::generate().map_err(|reason| MetadataError::Db {
                context: "create bucket (generate multipart upload ID key)",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                    std::io::Error::other(reason),
                )),
            })?;
        self.with_immediate_txn(
            "create bucket (begin txn)",
            "create bucket (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "create bucket (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "create bucket (advance execution generation)",
                        )?;
                        generation
                    }
                };
                match store.conn.execute(
                    "INSERT INTO buckets \
                     (name, owner_principal, owner_canonical_id, created_at, state, versioning, acl_grants, public_read, public_write, ownership_controls_mode, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years, bucket_execution_generation, bucket_incarnation_generation, multipart_upload_id_key) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
                    params![
                        config.name,
                        config.owner_principal,
                        config.owner_canonical_id.as_str(),
                        created_at_millis,
                        BucketState::Active as u8,
                        config.versioning as u8 as i64,
                        config.acl_grants.to_current_storage_string(),
                        i32::from(config.public_read),
                        i32::from(config.public_write),
                        Self::ownership_controls_sql_value(Some(config.ownership_controls)),
                        Option::<u8>::None,
                        object_lock_enabled,
                        object_lock_default_mode,
                        object_lock_default_days,
                        object_lock_default_years,
                        generation as i64,
                        generation as i64,
                        multipart_upload_id_key.as_bytes().as_slice(),
                    ],
                ) {
                    Ok(_) => Ok(()),
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
                    {
                        Err(MetadataError::BucketAlreadyExists)
                    }
                    Err(source) => Err(MetadataError::Db {
                        context: "create bucket",
                        source: source.into(),
                    }),
                }
            },
        )
    }

    fn insert_bucket_record_explicit(&self, bucket: &BucketRecord) -> Result<(), MetadataError> {
        let bucket = bucket.clone().command_metadata_projection();
        Self::validate_bucket_object_lock_transition(
            bucket.versioning,
            BucketObjectLockConfig::default(),
            bucket.object_lock,
            "create bucket record",
        )?;
        let created_at = i64::try_from(bucket.created_at).map_err(|_| MetadataError::Db {
            context: "create bucket record (encode created_at)",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                "bucket created_at exceeds i64",
            )),
        })?;
        let multipart_completion_barrier_sequence =
            i64::try_from(bucket.multipart_completion_barrier_sequence).map_err(|_| {
                MetadataError::Db {
                    context: "create bucket record (encode multipart completion barrier sequence)",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                        "bucket multipart completion barrier sequence exceeds i64",
                    )),
                }
            })?;
        let (
            object_lock_enabled,
            object_lock_default_mode,
            object_lock_default_days,
            object_lock_default_years,
        ) = Self::bucket_object_lock_sql_values(bucket.object_lock).map_err(|e| {
            MetadataError::Db {
                context: "create bucket record (encode object lock)",
                source: e.into(),
            }
        })?;
        let (
            public_access_block_present,
            public_access_block_block_public_acls,
            public_access_block_ignore_public_acls,
            public_access_block_block_public_policy,
            public_access_block_restrict_public_buckets,
        ) = Self::public_access_block_sql_values(bucket.public_access_block);

        self.with_immediate_txn(
            "create bucket record (begin txn)",
            "create bucket record (commit txn)",
            |store| {
                store.advance_bucket_execution_generation_in_txn(
                    bucket.bucket_execution_generation,
                    "create bucket record (advance execution generation)",
                )?;
                match store.conn.execute(
                    "INSERT INTO buckets \
                     (name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, multipart_upload_id_key, multipart_completion_barrier_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30)",
                    params![
                        bucket.name.as_str(),
                        &bucket.owner_principal,
                        bucket.owner_canonical_id.as_str(),
                        created_at,
                        bucket.region as i64,
                        bucket.state as u8 as i64,
                        bucket.versioning as u8 as i64,
                        bucket.acl_grants.to_current_storage_string(),
                        i32::from(bucket.public_read),
                        i32::from(bucket.public_write),
                        public_access_block_present,
                        public_access_block_block_public_acls,
                        public_access_block_ignore_public_acls,
                        public_access_block_block_public_policy,
                        public_access_block_restrict_public_buckets,
                        Self::ownership_controls_sql_value(bucket.ownership_controls),
                        i32::from(bucket.bucket_policy_public),
                        bucket.bucket_policy_generation as i64,
                        bucket.bucket_lifecycle_generation as i64,
                        bucket.bucket_execution_generation as i64,
                        bucket.bucket_incarnation_generation as i64,
                        bucket.multipart_upload_id_key.as_bytes().as_slice(),
                        multipart_completion_barrier_sequence,
                        i32::from(bucket.bucket_abac_enabled),
                        bucket.encryption.default_encryption.map(|value| value as u8),
                        i32::from(bucket.encryption.sse_c_blocked),
                        object_lock_enabled,
                        object_lock_default_mode,
                        object_lock_default_days,
                        object_lock_default_years,
                    ],
                ) {
                    Ok(_) => Ok(()),
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
                    {
                        Err(MetadataError::BucketAlreadyExists)
                    }
                    Err(source) => Err(MetadataError::Db {
                        context: "create bucket record",
                        source: source.into(),
                    }),
                }
            },
        )
    }

    fn put_bucket_record_update_inner(
        &self,
        target: &BucketRecord,
        effect: BucketRecordUpdateEffect,
        _stale_context: &'static str,
        conflict_context: &'static str,
    ) -> Result<(), MetadataError> {
        let current = self.head_bucket_record_raw(&target.name)?;
        if current.command_metadata_eq(target) {
            return Ok(());
        }
        if current.bucket_execution_generation == target.bucket_execution_generation {
            return Err(MetadataError::InvariantViolation {
                context: conflict_context,
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        if current.bucket_execution_generation > target.bucket_execution_generation {
            return Err(MetadataError::StaleBucketMetadataCommand {
                name: target.name.clone(),
                bucket_execution_generation: target.bucket_execution_generation,
            });
        }
        if !Self::bucket_record_preimage_matches_update(&current, target, effect) {
            return Err(MetadataError::InvariantViolation {
                context: conflict_context,
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        if matches!(effect, BucketRecordUpdateEffect::Versioning)
            && target.versioning == BucketVersioningState::Disabled
            && current.versioning != BucketVersioningState::Disabled
        {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current.versioning,
                to: target.versioning,
            });
        }
        if matches!(effect, BucketRecordUpdateEffect::Versioning)
            || matches!(
                effect,
                BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock)
            )
        {
            Self::validate_bucket_object_lock_transition(
                target.versioning,
                current.object_lock,
                target.object_lock,
                match effect {
                    BucketRecordUpdateEffect::Versioning => "put bucket versioning",
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                        "put bucket object lock"
                    }
                    _ => unreachable!("effect was filtered above"),
                },
            )?;
        }

        self.with_immediate_txn(
            "put bucket record update (begin txn)",
            "put bucket record update (commit txn)",
            |store| {
                store.advance_bucket_execution_generation_in_txn(
                    target.bucket_execution_generation,
                    "put bucket record update (advance execution generation)",
                )?;
                let updated = match effect {
                    BucketRecordUpdateEffect::State => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET state = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                target.state as u8 as i64,
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket state",
                            source: source.into(),
                        })?,
                    BucketRecordUpdateEffect::Versioning => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET versioning = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                target.versioning as u8 as i64,
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket versioning",
                            source: source.into(),
                        })?,
                    BucketRecordUpdateEffect::Acl => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET acl_grants = ?1, \
                                 public_read = ?2, \
                                 public_write = ?3, \
                                 bucket_execution_generation = ?4 \
                             WHERE name = ?5",
                            params![
                                target.acl_grants.to_current_storage_string(),
                                i32::from(target.public_read),
                                i32::from(target.public_write),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket acl",
                            source: source.into(),
                        })?,
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(target.object_lock).map_err(
                                |e| MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e.into(),
                                },
                            )?;
                        store
                            .conn
                            .execute(
                                "UPDATE buckets \
                                 SET object_lock_enabled = ?1, \
                                     object_lock_default_mode = ?2, \
                                     object_lock_default_days = ?3, \
                                     object_lock_default_years = ?4, \
                                     bucket_execution_generation = ?5 \
                                 WHERE name = ?6",
                                params![
                                    enabled,
                                    default_mode,
                                    default_days,
                                    default_years,
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket object lock",
                                source: source.into(),
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::Encryption) => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET default_encryption_type = ?1, \
                                 sse_c_blocked = ?2, \
                                 bucket_execution_generation = ?3 \
                             WHERE name = ?4",
                            params![
                                target
                                    .encryption
                                    .default_encryption
                                    .map(|value| value as u8),
                                i32::from(target.encryption.sse_c_blocked),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket encryption",
                            source: source.into(),
                        })?,
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::PublicAccessBlock) => {
                        let (
                            present,
                            block_public_acls,
                            ignore_public_acls,
                            block_public_policy,
                            restrict_public_buckets,
                        ) = Self::public_access_block_sql_values(target.public_access_block);
                        store
                            .conn
                            .execute(
                                "UPDATE buckets SET \
                                     public_access_block_present = ?1, \
                                     public_access_block_block_public_acls = ?2, \
                                     public_access_block_ignore_public_acls = ?3, \
                                     public_access_block_block_public_policy = ?4, \
                                     public_access_block_restrict_public_buckets = ?5, \
                                     bucket_execution_generation = ?6 \
                                 WHERE name = ?7",
                                params![
                                    present,
                                    block_public_acls,
                                    ignore_public_acls,
                                    block_public_policy,
                                    restrict_public_buckets,
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket public access block",
                                source: source.into(),
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::OwnershipControls) => {
                        store
                            .conn
                            .execute(
                                "UPDATE buckets \
                                 SET ownership_controls_mode = ?1, \
                                     bucket_execution_generation = ?2 \
                                 WHERE name = ?3",
                                params![
                                    Self::ownership_controls_sql_value(target.ownership_controls),
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket ownership controls",
                                source: source.into(),
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::AbacEnabled) => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET bucket_abac_enabled = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                i32::from(target.bucket_abac_enabled),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket abac enabled",
                            source: source.into(),
                        })?,
                };
                if updated == 0 {
                    return Err(bucket_not_found(target.name.as_str()));
                }
                Ok(())
            },
        )
    }

    fn bucket_record_preimage_matches_update(
        current: &BucketRecord,
        target: &BucketRecord,
        effect: BucketRecordUpdateEffect,
    ) -> bool {
        let mut expected = current.clone();
        expected.bucket_execution_generation = target.bucket_execution_generation;
        match effect {
            BucketRecordUpdateEffect::State => {
                expected.state = target.state;
            }
            BucketRecordUpdateEffect::Versioning => {
                expected.versioning = target.versioning;
            }
            BucketRecordUpdateEffect::Acl => {
                expected.acl_grants = target.acl_grants.clone();
                expected.public_read = target.public_read;
                expected.public_write = target.public_write;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                expected.object_lock = target.object_lock;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::Encryption) => {
                expected.encryption = target.encryption;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::PublicAccessBlock) => {
                expected.public_access_block = target.public_access_block;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::OwnershipControls) => {
                expected.ownership_controls = target.ownership_controls;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::AbacEnabled) => {
                expected.bucket_abac_enabled = target.bucket_abac_enabled;
            }
        }
        expected.command_metadata_eq(target)
    }

    pub(crate) fn apply_metadata_command(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(MetadataError::InvariantViolation {
                context: "apply metadata command PG mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        if !command.verify_checksum() {
            return Err(MetadataError::InvariantViolation {
                context: "apply metadata command checksum",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        match command.payload() {
            MetadataCommandPayload::CreateBucket(create) => {
                self.apply_create_bucket_command(create)
            }
            MetadataCommandPayload::PutBucketVersioning(versioning) => {
                self.apply_put_bucket_versioning_command(versioning)
            }
            MetadataCommandPayload::PutBucketAcl(acl) => self.apply_put_bucket_acl_command(acl),
            MetadataCommandPayload::PutBucketProperty(property) => {
                self.apply_put_bucket_property_command(property)
            }
            MetadataCommandPayload::PutBucketSubresource(subresource) => {
                self.apply_put_bucket_subresource_command(subresource)
            }
            MetadataCommandPayload::MarkBucketDeleting(mark) => {
                self.apply_mark_bucket_deleting_command(mark)
            }
            MetadataCommandPayload::DeleteFinalizedBucket(delete) => {
                self.apply_delete_finalized_bucket_command(delete)
            }
            MetadataCommandPayload::AdvanceMultipartCompletionBarrier(command) => {
                self.apply_advance_multipart_completion_barrier_command(command)
            }
            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                self.apply_reserve_object_generation_command(reservation)
            }
            MetadataCommandPayload::ReleaseObjectGeneration(reservation) => {
                self.apply_release_object_generation_command(reservation)
            }
            MetadataCommandPayload::ReserveObjectVersion(version) => {
                self.apply_reserve_object_version_command(version)
            }
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                self.apply_commit_direct_put_object_command(command)
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                self.apply_commit_multipart_object_command(command)
            }
            MetadataCommandPayload::DeleteObjectVersion(command) => {
                self.apply_delete_object_version_command(command)
            }
            MetadataCommandPayload::InsertDeleteMarker(command) => {
                self.apply_insert_delete_marker_command(command)
            }
            MetadataCommandPayload::PutObjectMetadata(command) => {
                self.apply_put_object_metadata_command(command)
            }
            MetadataCommandPayload::CreateStreamUpload(command) => {
                self.apply_create_stream_upload_command(command)
            }
            MetadataCommandPayload::AppendStreamSegment(command) => {
                self.apply_append_stream_segment_command(command)
            }
            MetadataCommandPayload::AbortStreamUpload(command) => {
                self.apply_abort_stream_upload_command(command)
            }
            MetadataCommandPayload::CommitStreamPart(command) => {
                self.apply_commit_stream_part_command(command)
            }
            MetadataCommandPayload::CreateMultipartUpload(command) => {
                self.apply_create_multipart_upload_command(command)
            }
            MetadataCommandPayload::AbortMultipartUpload(command) => {
                self.apply_abort_multipart_upload_command(command)
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => {
                self.apply_delete_object_payload_reclaim_command(command)
            }
        }
    }

    pub(crate) fn apply_metadata_command_and_record(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.apply_metadata_command_and_record_with_commit_guard(node_id, command, || Ok(()))
    }

    pub(crate) fn apply_metadata_command_and_record_with_commit_guard(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        commit_guard: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| {
                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                    context: "apply metadata command and record (begin txn)",
                    source: source.into(),
                })
            })?;

        let result = (|| match self
            .metadata_command_acceptance(node_id, command)
            .map_err(BucketSnapshotLoadError::Store)?
        {
            MetadataCommandAcceptance::Apply => {
                self.apply_metadata_command(command)
                    .map_err(BucketSnapshotLoadError::Metadata)?;
                self.record_metadata_command_applied_inner(node_id, command)
                    .map_err(BucketSnapshotLoadError::Store)
            }
            MetadataCommandAcceptance::AlreadyApplied => {
                let record = self
                    .record_metadata_command_applied_inner(node_id, command)
                    .map_err(BucketSnapshotLoadError::Store)?;
                if record.state.applied_log_index >= command.id().log_index().get()
                    && self
                        .cleanup_already_applied_metadata_command_terminal_staging(command)
                        .map_err(BucketSnapshotLoadError::Metadata)?
                {
                    self.update_metadata_command_replica_state(
                        record.state.cluster_epoch,
                        record.state.applied_log_index,
                        record.state.applied_log_hash,
                    )
                    .map_err(BucketSnapshotLoadError::Store)
                } else {
                    Ok(record)
                }
            }
        })();

        match result {
            Ok(record) => {
                if let Err(error) = commit_guard() {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(BucketSnapshotLoadError::Store(error));
                }
                self.commit_immediate_txn("apply metadata command and record (commit txn)")
                    .map_err(BucketSnapshotLoadError::Metadata)?;
                self.mark_metadata_state_digest_clean_at_revision(record.digest_revision);
                Ok(record.state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                self.invalidate_clean_metadata_digest_revision();
                Err(error)
            }
        }
    }

    fn apply_create_bucket_command(
        &self,
        command: &CreateBucketCommand,
    ) -> Result<(), MetadataError> {
        match self.insert_bucket_record_explicit(command.bucket()) {
            Ok(()) => Ok(()),
            Err(MetadataError::BucketAlreadyExists) => {
                let existing = self.head_bucket_record_raw(&command.bucket().name)?;
                if existing.command_metadata_eq(command.bucket()) {
                    Ok(())
                } else {
                    Err(MetadataError::BucketAlreadyExists)
                }
            }
            Err(other) => Err(other),
        }
    }

    fn apply_put_bucket_versioning_command(
        &self,
        command: &PutBucketVersioningCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Versioning,
            "apply stale bucket versioning command",
            "apply conflicting bucket versioning command",
        )
    }

    fn apply_put_bucket_acl_command(
        &self,
        command: &PutBucketAclCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Acl,
            "apply stale bucket acl command",
            "apply conflicting bucket acl command",
        )
    }

    fn apply_mark_bucket_deleting_command(
        &self,
        command: &MarkBucketDeletingCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::State,
            "apply stale mark bucket deleting command",
            "apply conflicting mark bucket deleting command",
        )
    }

    fn apply_delete_finalized_bucket_command(
        &self,
        command: &DeleteFinalizedBucketCommand,
    ) -> Result<(), MetadataError> {
        self.delete_finalized_bucket_rows_in_current_txn(
            &command.bucket,
            Some((
                command.bucket_execution_generation,
                command.bucket_incarnation_generation,
            )),
            false,
        )
        .map(|_| ())
    }

    fn delete_finalized_bucket_rows_in_current_txn(
        &self,
        name: &BucketName,
        expected_generations: Option<(u64, u64)>,
        refresh_command_state_digest: bool,
    ) -> Result<usize, MetadataError> {
        let row = self
            .conn
            .query_row(
                "SELECT state, bucket_execution_generation, bucket_incarnation_generation \
                 FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Ok((
                        row.get::<_, u8>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|source| MetadataError::Db {
                context: "delete finalized bucket (load state)",
                source: source.into(),
            })?;
        let Some((state, bucket_execution_generation, bucket_incarnation_generation)) = row else {
            let _ = observability::event(
                TRACE_TARGET,
                "pg_delete_finalized_bucket_missing",
                Some(format_args!("pg_id={} bucket={:?}", self.pg_id, name)),
            );
            return Ok(0);
        };
        if let Some((expected_execution, expected_incarnation)) = expected_generations {
            if bucket_execution_generation != expected_execution
                || bucket_incarnation_generation != expected_incarnation
            {
                let _ = observability::event(
                    TRACE_TARGET,
                    "pg_delete_finalized_bucket_stale_generation",
                    Some(format_args!(
                        "pg_id={} bucket={:?} expected_execution={} actual_execution={} expected_incarnation={} actual_incarnation={}",
                        self.pg_id,
                        name,
                        expected_execution,
                        bucket_execution_generation,
                        expected_incarnation,
                        bucket_incarnation_generation
                    )),
                );
                return Ok(0);
            }
        }
        let state = BucketState::from_u8(state).ok_or_else(|| MetadataError::Db {
            context: "delete finalized bucket (invalid bucket state)",
            source: crate::error::DatabaseError::new("invalid stored bucket state"),
        })?;
        if state != BucketState::Deleting {
            let _ = observability::event(
                TRACE_TARGET,
                "pg_delete_finalized_bucket_wrong_state",
                Some(format_args!(
                    "pg_id={} bucket={:?} state={:?}",
                    self.pg_id, name, state
                )),
            );
            return Err(MetadataError::BucketNotFinalizedForDelete { state });
        }
        let deleted = self
            .conn
            .execute(
                "DELETE FROM buckets \
                 WHERE name = ?1 AND state = ?2 \
                   AND bucket_execution_generation = ?3 \
                   AND bucket_incarnation_generation = ?4",
                params![
                    name.as_str(),
                    BucketState::Deleting as u8,
                    bucket_execution_generation as i64,
                    bucket_incarnation_generation as i64,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "delete finalized bucket (delete row)",
                source: source.into(),
            })?;
        if deleted != 0 {
            self.conn
                .execute(
                    "DELETE FROM object_version_counters WHERE bucket = ?1",
                    params![name.as_str()],
                )
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (delete version counters)",
                    source: source.into(),
                })?;
            self.conn
                .execute(
                    "DELETE FROM object_write_counters WHERE bucket = ?1",
                    params![name.as_str()],
                )
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (delete write counters)",
                    source: source.into(),
                })?;
            if refresh_command_state_digest {
                self.refresh_metadata_command_state_digest()
                    .map_err(|error| {
                        Self::store_error_as_metadata_db(
                            "delete finalized bucket (refresh metadata command digest)",
                            error,
                        )
                    })?;
            }
        }
        Ok(deleted)
    }

    fn apply_put_bucket_property_command(
        &self,
        command: &PutBucketPropertyCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Property(command.effect),
            bucket_property_stale_context(command.effect),
            bucket_property_conflict_context(command.effect),
        )
    }

    fn apply_put_bucket_subresource_command(
        &self,
        command: &PutBucketSubresourceCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_inner(
            &command.name,
            &command.mutation,
            BucketExecutionGeneration::Explicit(command.bucket_execution_generation),
        )
    }

    fn apply_advance_multipart_completion_barrier_command(
        &self,
        command: &AdvanceMultipartCompletionBarrierCommand,
    ) -> Result<(), MetadataError> {
        self.advance_multipart_completion_barrier_for_bucket(
            &command.bucket,
            command.barrier_sequence,
        )
    }

    fn apply_reserve_object_generation_command(
        &self,
        command: &ReserveObjectGenerationCommand,
    ) -> Result<(), MetadataError> {
        self.reserve_object_generation_explicit(
            &command.bucket,
            &command.key,
            &command.reservation_id,
            command.generation_id,
            command.created_at_millis,
        )
    }

    fn apply_release_object_generation_command(
        &self,
        command: &ReleaseObjectGenerationCommand,
    ) -> Result<(), MetadataError> {
        self.delete_object_generation_reservation_direct(
            &command.bucket,
            &command.key,
            &command.reservation_id,
        )
    }

    fn apply_reserve_object_version_command(
        &self,
        command: &ReserveObjectVersionCommand,
    ) -> Result<(), MetadataError> {
        self.reserve_object_version_explicit(&command.bucket, &command.key, command.version_id)
    }

    fn apply_commit_direct_put_object_command(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<(), MetadataError> {
        self.validate_direct_put_stream_session_proof(command)?;
        if self.direct_put_command_already_applied(command)? {
            return self.cleanup_direct_put_terminal_staging(command);
        }

        let reserved_generation = self.get_object_generation_reservation(
            &command.object.bucket,
            &command.object.key,
            &command.generation_reservation_id,
        )?;
        if reserved_generation != command.object.generation_id {
            return Err(MetadataError::InvariantViolation {
                context: "commit direct put command reservation mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        self.with_immediate_txn(
            "commit direct put command (begin txn)",
            "commit direct put command (commit txn)",
            |store| {
                store.put_object_with_segments_explicit_in_open_txn(
                    &command.object,
                    &command.segments,
                    command.write_sequence,
                    command.last_modified_millis,
                )?;
                store.delete_object_generation_reservation_direct(
                    &command.object.bucket,
                    &command.object.key,
                    &command.generation_reservation_id,
                )?;
                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_direct_put_stale_payload_in_open_txn(
                        &command.object.bucket,
                        &command.object.key,
                        command.object.version_id,
                        stale_payload,
                    )?;
                }
                store.delete_direct_put_stream_upload_in_open_txn(command)?;
                Ok(())
            },
        )
    }

    fn validate_direct_put_stream_session_proof(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<(), MetadataError> {
        let requires_stream_session = command.bucket_write_reservation.operation_kind
            == crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND;
        match self.get_stream_upload(&command.generation_reservation_id) {
            Ok(session)
                if requires_stream_session
                    && session.bucket == command.object.bucket
                    && session.key == command.object.key
                    && session.target == StreamUploadTarget::PutObject
                    && session.state == StreamUploadState::InProgress
                    && session
                        .bucket_write_reservation
                        .as_ref()
                        .is_some_and(|proof| {
                            proof.has_same_stable_identity(&command.bucket_write_reservation)
                        }) =>
            {
                Ok(())
            }
            Err(MetadataError::StreamSessionNotFound { .. }) if !requires_stream_session => Ok(()),
            Ok(_) | Err(MetadataError::StreamSessionNotFound { .. }) => {
                Err(MetadataError::InvariantViolation {
                    context: "commit direct put command stream reservation mismatch",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn cleanup_already_applied_metadata_command_terminal_staging(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, MetadataError> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                self.cleanup_direct_put_terminal_staging(command)?;
                Ok(true)
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                self.cleanup_multipart_object_terminal_staging(command)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn cleanup_direct_put_terminal_staging(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "cleanup already-applied direct put command (begin txn)",
            "cleanup already-applied direct put command (commit txn)",
            |store| {
                store.delete_object_generation_reservation_direct(
                    &command.object.bucket,
                    &command.object.key,
                    &command.generation_reservation_id,
                )?;
                store.delete_direct_put_stream_upload_in_open_txn(command)
            },
        )
    }

    fn delete_direct_put_stream_upload_in_open_txn(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads \
                 WHERE session_id = ?1 AND bucket = ?2 AND key = ?3",
                params![
                    command.generation_reservation_id.as_str(),
                    &command.object.bucket,
                    &command.object.key
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit standard object command (delete stream staging)",
                source: e.into(),
            })?;
        Ok(())
    }

    fn cleanup_multipart_object_terminal_staging(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "cleanup already-applied multipart object command (begin txn)",
            "cleanup already-applied multipart object command (commit txn)",
            |store| {
                store.delete_multipart_part_staging_segments_for_upload_in_open_txn(
                    &command.upload_id,
                )?;
                store.release_multipart_completion_reservation_in_open_txn(command)?;
                for session in &command.stream_uploads {
                    store.delete_stream_upload_in_open_txn(&session.session_id)?;
                }
                store.delete_multipart_upload_if_present_in_open_txn(&command.upload_id)
            },
        )
    }

    fn direct_put_command_already_applied(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let stored_write_sequence = self
            .object_write_sequence(
                command.object.bucket.as_str(),
                command.object.key.as_str(),
                command.object.version_id,
            )?
            .ok_or(MetadataError::ObjectNotFound)?;
        if stored.generation_id != command.object.generation_id
            || stored_write_sequence != command.write_sequence
            || stored.size != command.object.size
            || stored.etag != command.object.etag
            || stored.last_modified != command.last_modified_millis
            || stored.ec != command.object.ec
            || stored.layout != command.object.layout
            || stored.tags != command.object.tags
            || stored.metadata_blob != command.object.metadata_blob
            || stored.system_metadata_blob != command.object.system_metadata_blob
            || stored.object_lock != command.object.object_lock
            || stored.encryption != command.object.encryption
            || stored.owner != command.object.owner
            || stored.acl_grants != command.object.acl_grants
            || stored.public_read != command.object.public_read
        {
            return Ok(false);
        }
        let segments = self.get_object_segments(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        )?;
        Ok(segments == command.segments)
    }

    fn stream_upload_cleanup_records_match(
        actual: &[StreamUploadRecord],
        expected: &[TerminalStreamCleanupRecord],
    ) -> bool {
        actual.len() == expected.len()
            && actual.iter().zip(expected).all(|(actual, expected)| {
                actual.session_id == expected.session_id
                    && actual.bucket == expected.bucket
                    && actual.key == expected.key
                    && actual.target == expected.target
                    && actual.state == expected.state
                    && actual.created_at == expected.created_at
                    && actual.encryption == expected.encryption
            })
    }

    fn apply_commit_multipart_object_command(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        if self.multipart_object_command_already_applied(command)? {
            return Ok(());
        }

        self.with_immediate_txn(
            "commit multipart object command (begin txn)",
            "commit multipart object command (commit txn)",
            |store| {
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&command.upload_id)?;
                if !Self::stream_upload_cleanup_records_match(
                    &stream_uploads,
                    &command.stream_uploads,
                ) {
                    return Err(MetadataError::InvariantViolation {
                        context: "commit multipart object command (stream uploads mismatch)", reason: "metadata state does not satisfy the operation invariant".into() });
                }
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                if stream_upload_segments != command.stream_upload_segments {
                    return Err(MetadataError::InvariantViolation { context: "commit multipart object command (stream upload segments mismatch)", reason: "metadata state does not satisfy the operation invariant".into() });
                }
                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_multipart_overwrite_stale_payload_in_open_txn(
                        &command.object.bucket,
                        &command.object.key,
                        command.object.version_id,
                        stale_payload,
                    )?;
                }
                store.put_multipart_object_explicit_in_open_txn(
                    &command.object,
                    &command.parts,
                    command.write_sequence,
                    command.last_modified_millis,
                )?;
                store
                    .conn
                    .execute(
                        "UPDATE objects \
                         SET multipart_completion_upload_id = ?1, multipart_completion_fingerprint = ?2 \
                         WHERE bucket = ?3 AND key = ?4 AND version_id = ?5",
                        params![
                            command.upload_id.as_str(),
                            command.completion_fingerprint.as_bytes().as_slice(),
                            &command.object.bucket,
                            &command.object.key,
                            command.object.version_id.to_u64() as i64,
                        ],
                    )
                    .map_err(|source| MetadataError::Db { context: "commit multipart object command (record replay identity)",
                        source: source.into(),
                    })?;
                store.delete_multipart_part_segments_direct(
                    &command.object.bucket,
                    &command.object.key,
                    command.object.version_id,
                )?;
                store.delete_multipart_part_staging_segments_for_upload_in_open_txn(
                    &command.upload_id,
                )?;
                store.insert_multipart_part_segments_in_open_txn(
                    &command.object,
                    &command.selected_streaming_segments,
                )?;
                store.release_multipart_completion_reservation_in_open_txn(command)?;
                for session in &command.stream_uploads {
                    store.delete_stream_upload_in_open_txn(&session.session_id)?;
                }
                store.delete_multipart_upload_if_present_in_open_txn(&command.upload_id)?;
                Ok(())
            },
        )
    }

    fn multipart_object_command_already_applied(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let stored_write_sequence = self
            .object_write_sequence(
                command.object.bucket.as_str(),
                command.object.key.as_str(),
                command.object.version_id,
            )?
            .ok_or(MetadataError::ObjectNotFound)?;
        if stored.generation_id != command.object.generation_id
            || stored_write_sequence != command.write_sequence
            || stored.size != command.object.size
            || stored.etag != command.object.etag
            || stored.last_modified != command.last_modified_millis
            || stored.ec != command.object.ec
            || stored.layout != command.object.layout
            || stored.tags != command.object.tags
            || stored.metadata_blob != command.object.metadata_blob
            || stored.system_metadata_blob != command.object.system_metadata_blob
            || stored.object_lock != command.object.object_lock
            || stored.encryption != command.object.encryption
            || stored.owner != command.object.owner
            || stored.acl_grants != command.object.acl_grants
            || stored.public_read != command.object.public_read
            || self.multipart_completion_identity(
                &command.object.bucket,
                &command.object.key,
                command.object.version_id,
            )? != Some((command.upload_id.clone(), command.completion_fingerprint))
        {
            return Ok(false);
        }
        let parts = self.get_object_parts(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        )?;
        if parts != command.parts {
            return Ok(false);
        }
        let mut streaming_segments = Vec::new();
        for part in &parts {
            streaming_segments.extend(self.get_multipart_part_segments(
                &command.object.bucket,
                &command.object.key,
                command.object.version_id,
                part.part_number,
            )?);
        }
        Ok(streaming_segments == command.selected_streaming_segments)
    }

    fn multipart_completion_identity(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<(UploadId, MultipartCompletionFingerprint)>, MetadataError> {
        let row = self
            .conn
            .query_row(
                "SELECT multipart_completion_upload_id, multipart_completion_fingerprint \
                 FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| MetadataError::Db {
                context: "read multipart completion identity",
                source: source.into(),
            })?;
        let Some((upload_id, fingerprint)) = row else {
            return Ok(None);
        };
        match (upload_id, fingerprint) {
            (None, None) => Ok(None),
            (Some(upload_id), Some(fingerprint)) => {
                let upload_id = UploadId::try_from(upload_id).map_err(|_| MetadataError::Db {
                    context: "read multipart completion identity (invalid upload ID)",
                    source: crate::error::DatabaseError::new(
                        "invalid stored multipart completion upload ID",
                    ),
                })?;
                let fingerprint: [u8; 32] =
                    fingerprint.try_into().map_err(|_| MetadataError::Db {
                        context: "read multipart completion identity (invalid fingerprint)",
                        source: crate::error::DatabaseError::new(
                            "invalid stored multipart completion fingerprint",
                        ),
                    })?;
                Ok(Some((
                    upload_id,
                    MultipartCompletionFingerprint::from_bytes(fingerprint),
                )))
            }
            _ => Err(MetadataError::Db {
                context: "read multipart completion identity (incomplete pair)",
                source: crate::error::DatabaseError::new(
                    "incomplete stored multipart completion identity",
                ),
            }),
        }
    }

    pub fn get_multipart_completion_replay(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartCompletionReplay>, MetadataError> {
        let row = self
            .conn
            .query_row(
                "SELECT version_id, multipart_completion_fingerprint \
                 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 AND multipart_completion_upload_id = ?3",
                params![bucket, key, upload_id.as_str()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(|source| MetadataError::Db {
                context: "read multipart completion replay identity",
                source: source.into(),
            })?;
        let Some((version_id, fingerprint)) = row else {
            return Ok(None);
        };
        let version_id =
            VersionId::from_u64(version_id.try_into().map_err(|_| MetadataError::Db {
                context: "read multipart completion replay (invalid version ID)",
                source: crate::error::DatabaseError::new(
                    "invalid stored multipart completion version ID",
                ),
            })?);
        let fingerprint: [u8; 32] = fingerprint.try_into().map_err(|_| MetadataError::Db {
            context: "read multipart completion replay (invalid fingerprint)",
            source: crate::error::DatabaseError::new(
                "invalid stored multipart completion fingerprint",
            ),
        })?;
        let StoredObject::Live(object) = self.get_object_version(bucket, key, version_id)? else {
            return Err(MetadataError::Db {
                context: "read multipart completion replay (delete marker)",
                source: crate::error::DatabaseError::new(
                    "stored multipart completion references a delete marker",
                ),
            });
        };
        Ok(Some(MultipartCompletionReplay {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            fingerprint: MultipartCompletionFingerprint::from_bytes(fingerprint),
            version_id,
            etag: object.etag,
            size: object.size,
            last_modified: object.last_modified,
            tags: object.tags,
            system_metadata_blob: object.system_metadata_blob,
            encryption: object.encryption,
        }))
    }

    fn insert_multipart_part_segments_in_open_txn(
        &self,
        object: &PutLiveObjectReq,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )
            .map_err(|e| MetadataError::Db { context: "commit multipart object command (prepare insert part segments)",
                source: e.into(),
            })?;

        for segment in segments {
            if segment.bucket != object.bucket
                || segment.key != object.key
                || segment.version_id != object.version_id.to_u64()
            {
                return Err(MetadataError::Db {
                    context: "commit multipart object command (segment object mismatch)",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("multipart part segment row does not match object identity"),
                    ),
                });
            }
            stmt.execute(params![
                &segment.bucket,
                &segment.key,
                segment.upload_id.as_str(),
                segment.version_id as i64,
                segment.part_number,
                segment.segment_index,
                segment.size as i64,
                segment.segment_crc64 as i64,
                segment.segment_okh.as_slice(),
                segment.segment_vid.get() as i64,
                segment.data_pg_id,
                segment.placement_cluster_epoch.get() as i64,
                segment.ec_k,
                segment.ec_m,
            ])
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (insert part segment)",
                source: e.into(),
            })?;
        }

        Ok(())
    }

    fn delete_multipart_part_staging_segments_for_upload_in_open_txn(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2",
                params![
                    upload_id.as_str(),
                    PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (delete staging segments)",
                source: e.into(),
            })?;
        Ok(())
    }

    pub(crate) fn advance_multipart_completion_barrier_for_bucket(
        &self,
        bucket: &BucketName,
        barrier_sequence: u64,
    ) -> Result<(), MetadataError> {
        let barrier_sequence = i64::try_from(barrier_sequence).map_err(|_| MetadataError::Db {
            context: "commit multipart object command (completion order overflow)",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("barrier_sequence exceeds SQLite integer range"),
            ),
        })?;
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET multipart_completion_barrier_sequence = \
                     CASE \
                         WHEN multipart_completion_barrier_sequence < ?2 THEN ?2 \
                         ELSE multipart_completion_barrier_sequence \
                     END \
                 WHERE name = ?1",
                params![bucket, barrier_sequence],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (advance completed upload sequence)",
                source: e.into(),
            })?;
        if updated == 0 {
            return Err(bucket_not_found(bucket.as_str()));
        }
        Ok(())
    }

    fn release_multipart_completion_reservation_in_open_txn(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3 AND generation_id = ?4",
                params![
                    command.upload_id.as_str(),
                    &command.object.bucket,
                    &command.object.key,
                    command.object.generation_id.get() as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (release generation reservation)",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_multipart_upload_if_present_in_open_txn(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (delete upload)",
                source: e.into(),
            })?;
        Ok(())
    }

    fn apply_direct_put_stale_payload_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        stale_payload: &ObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        match stale_payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                self.put_object_segments_reclaim_in_open_txn(reclaim)
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                self.put_multipart_reclaim_in_open_txn(reclaim)?;
                self.delete_multipart_part_segments_direct(bucket, key, version_id)?;
                self.delete_object_parts_direct(bucket, key, version_id)
            }
        }
    }

    fn apply_multipart_overwrite_stale_payload_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        stale_payload: &ObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        match stale_payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                self.put_object_segments_reclaim_in_open_txn(reclaim)?;
                self.delete_object_segments_direct(bucket, key, version_id)
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                self.put_multipart_reclaim_in_open_txn(reclaim)?;
                self.delete_multipart_part_segments_direct(bucket, key, version_id)?;
                self.delete_object_parts_direct(bucket, key, version_id)
            }
        }
    }

    fn validate_delete_payload_reclaim_command_root(
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        let matches_root = match &command.payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                reclaim.bucket == command.bucket
                    && reclaim.key == command.key
                    && reclaim.generation_id == command.generation_id
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                reclaim.bucket == command.bucket
                    && reclaim.key == command.key
                    && reclaim.generation_id == command.generation_id
            }
        };
        if matches_root {
            Ok(())
        } else {
            Err(MetadataError::InvariantViolation {
                context: "delete object payload reclaim command root mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            })
        }
    }

    fn clear_object_payload_reclaim_claim_if_matches_in_open_txn(
        &self,
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<bool, MetadataError> {
        if command.payload.kind() != command.reclaim_claim.reclaim_kind {
            return Err(MetadataError::InvariantViolation {
                context: "delete object payload reclaim command claim kind mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        let bucket_incarnation_generation = i64::try_from(
            command.reclaim_claim.bucket_incarnation_generation,
        )
        .map_err(|source| MetadataError::Db {
            context: "delete object payload reclaim command claim incarnation",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let deleted = self
            .conn
            .execute(
                "DELETE FROM object_payload_reclaim_claims \
                 WHERE singleton = 0 AND bucket = ?1 AND bucket_incarnation_generation = ?2 \
                   AND key = ?3 AND generation_id = ?4 AND reclaim_kind = ?5 \
                   AND claim_id = ?6 AND owner_token = ?7 AND cluster_epoch = ?8",
                params![
                    &command.bucket,
                    bucket_incarnation_generation,
                    &command.key,
                    command.generation_id.get() as i64,
                    command.reclaim_claim.reclaim_kind as u8,
                    &command.reclaim_claim.claim_id,
                    &command.reclaim_claim.owner_token,
                    command.reclaim_claim.cluster_epoch.get(),
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "delete object payload reclaim command claim release",
                source: source.into(),
            })?;
        Ok(deleted != 0)
    }

    fn clear_object_payload_reclaim_claim_for_existing_root_in_open_txn(
        &self,
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        if self.clear_object_payload_reclaim_claim_if_matches_in_open_txn(command)? {
            return Ok(());
        }
        let claim_exists = self
            .conn
            .query_row(
                "SELECT 1 FROM object_payload_reclaim_claims WHERE singleton = 0",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(|source| MetadataError::Db {
                context: "delete object payload reclaim command claim conflict check",
                source: source.into(),
            })?
            .is_some();
        if claim_exists {
            return Err(MetadataError::ReclaimClaimConflict {
                claim_id: command.reclaim_claim.claim_id.clone(),
            });
        }
        Ok(())
    }

    fn apply_delete_object_payload_reclaim_command(
        &self,
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        Self::validate_delete_payload_reclaim_command_root(command)?;
        match &command.payload {
            ObjectPayloadReclaimCommand::Segments(expected) => {
                match self.get_object_segments_reclaim(
                    &command.bucket,
                    &command.key,
                    command.generation_id,
                )? {
                    Some(existing) if existing == *expected => {
                        self.clear_object_payload_reclaim_claim_for_existing_root_in_open_txn(
                            command,
                        )?;
                        self.delete_object_segments_reclaim_direct(
                            &command.bucket,
                            &command.key,
                            command.generation_id,
                        )
                    }
                    Some(_) => Err(MetadataError::InvariantViolation {
                        context: "delete object payload reclaim command segment mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    }),
                    None => {
                        if self
                            .get_multipart_reclaim(
                                &command.bucket,
                                &command.key,
                                command.generation_id,
                            )?
                            .is_some()
                        {
                            return Err(MetadataError::InvariantViolation {
                                context: "delete object payload reclaim command kind mismatch",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                        Ok(())
                    }
                }
            }
            ObjectPayloadReclaimCommand::Multipart(expected) => {
                match self.get_multipart_reclaim(
                    &command.bucket,
                    &command.key,
                    command.generation_id,
                )? {
                    Some(existing) if existing == *expected => {
                        self.clear_object_payload_reclaim_claim_for_existing_root_in_open_txn(
                            command,
                        )?;
                        self.delete_multipart_reclaim_direct(
                            &command.bucket,
                            &command.key,
                            command.generation_id,
                        )
                    }
                    Some(_) => Err(MetadataError::InvariantViolation {
                        context: "delete object payload reclaim command multipart mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    }),
                    None => {
                        if self
                            .get_object_segments_reclaim(
                                &command.bucket,
                                &command.key,
                                command.generation_id,
                            )?
                            .is_some()
                        {
                            return Err(MetadataError::InvariantViolation {
                                context: "delete object payload reclaim command kind mismatch",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                        Ok(())
                    }
                }
            }
        }?;
        self.clear_object_payload_reclaim_claim_if_matches_in_open_txn(command)
            .map(|_| ())
    }

    fn apply_delete_object_version_command(
        &self,
        command: &DeleteObjectVersionCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "delete object version command (begin txn)",
            "delete object version command (commit txn)",
            |store| {
                let stored = match store.get_object_version(
                    &command.bucket,
                    &command.key,
                    command.version_id,
                ) {
                    Ok(stored) => stored,
                    Err(MetadataError::ObjectNotFound) => return Ok(()),
                    Err(error) => return Err(error),
                };

                match (&command.target, stored) {
                    (
                        DeleteObjectVersionTarget::DeleteMarker { write_sequence },
                        StoredObject::DeleteMarker(_),
                    ) => {
                        let stored_write_sequence = store
                            .object_write_sequence(
                                command.bucket.as_str(),
                                command.key.as_str(),
                                command.version_id,
                            )?
                            .ok_or(MetadataError::ObjectNotFound)?;
                        if stored_write_sequence != *write_sequence {
                            return Err(MetadataError::StaleObjectWriteCommand {
                                bucket: command.bucket.clone(),
                                key: command.key.clone(),
                                write_sequence: *write_sequence,
                                generation_id: None,
                            });
                        }
                    }
                    (
                        DeleteObjectVersionTarget::Live {
                            generation_id,
                            layout,
                            payload,
                        },
                        StoredObject::Live(record),
                    ) => {
                        if record.generation_id != *generation_id || record.layout != *layout {
                            return Err(MetadataError::InvariantViolation {
                                context: "delete object version command target mismatch",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                        store.validate_object_payload_reclaim_matches_durable_object(
                            &command.bucket,
                            &command.key,
                            &record,
                            payload,
                            "delete object version command reclaim subject mismatch",
                        )?;
                        match payload {
                            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                                store.put_object_segments_reclaim_in_open_txn(reclaim)?;
                                store.delete_object_segments_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                            }
                            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                                store.put_multipart_reclaim_in_open_txn(reclaim)?;
                                store.delete_multipart_part_segments_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                                store.delete_object_parts_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                            }
                        }
                    }
                    _ => {
                        return Err(MetadataError::InvariantViolation {
                            context: "delete object version command kind mismatch",
                            reason: "metadata state does not satisfy the operation invariant"
                                .into(),
                        });
                    }
                }

                store.delete_object_version_in_open_txn(
                    &command.bucket,
                    &command.key,
                    command.version_id,
                )
            },
        )
    }

    fn apply_insert_delete_marker_command(
        &self,
        command: &InsertDeleteMarkerCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "insert delete marker command (begin txn)",
            "insert delete marker command (commit txn)",
            |store| {
                match store.get_object_version(&command.bucket, &command.key, command.version_id) {
                    Ok(StoredObject::DeleteMarker(marker))
                        if marker.owner == command.owner
                            && marker.last_modified == command.last_modified_millis
                            && store.object_write_sequence(
                                command.bucket.as_str(),
                                command.key.as_str(),
                                command.version_id,
                            )? == Some(command.write_sequence) =>
                    {
                        return Ok(());
                    }
                    Ok(StoredObject::Live(record)) if command.version_id.is_null() => {
                        let Some(stale_payload) = command.stale_payload.as_ref() else {
                            return Err(MetadataError::InvariantViolation {
                                context: "insert delete marker command missing stale payload",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        };
                        store.validate_object_payload_reclaim_matches_durable_object(
                            &command.bucket,
                            &command.key,
                            &record,
                            stale_payload,
                            "insert delete marker command stale payload subject mismatch",
                        )?;
                    }
                    Ok(StoredObject::DeleteMarker(_)) if command.version_id.is_null() => {
                        if command.stale_payload.is_some() {
                            return Err(MetadataError::InvariantViolation {
                                context: "insert delete marker command unexpected stale payload",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                    }
                    Ok(_) => {
                        return Err(MetadataError::InvariantViolation {
                            context: "insert delete marker command existing object mismatch",
                            reason: "metadata state does not satisfy the operation invariant"
                                .into(),
                        });
                    }
                    Err(MetadataError::ObjectNotFound) => {
                        if command.stale_payload.is_some() {
                            return Err(MetadataError::InvariantViolation {
                                context: "insert delete marker command unexpected stale payload",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                    }
                    Err(error) => return Err(error),
                }

                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_multipart_overwrite_stale_payload_in_open_txn(
                        &command.bucket,
                        &command.key,
                        command.version_id,
                        stale_payload,
                    )?;
                }
                store.put_delete_marker_explicit_in_open_txn(
                    &command.bucket,
                    &command.key,
                    command.version_id,
                    &command.owner,
                    command.write_sequence,
                    command.last_modified_millis,
                )
            },
        )
    }

    fn validate_object_payload_reclaim_matches_durable_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &LiveObjectRecord,
        payload: &ObjectPayloadReclaimCommand,
        context: &'static str,
    ) -> Result<(), MetadataError> {
        let created_at = match payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => reclaim.created_at,
            ObjectPayloadReclaimCommand::Multipart(reclaim) => reclaim.created_at,
        };
        let expected = match record.layout {
            ObjectLayout::Standard => {
                let segments = self.get_object_segments(bucket, key, record.version_id)?;
                ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
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
                })
            }
            ObjectLayout::MultipartManifest { .. } => {
                let parts = self.get_object_parts(bucket, key, record.version_id)?;
                let mut segments = Vec::new();
                for part in &parts {
                    segments.extend(self.get_multipart_part_segments(
                        bucket,
                        key,
                        record.version_id,
                        part.part_number,
                    )?);
                }
                ObjectPayloadReclaimCommand::Multipart(MultipartReclaimRecord::from_object_parts(
                    bucket,
                    key,
                    record.generation_id,
                    created_at,
                    &parts,
                    &segments,
                ))
            }
        };
        if expected == *payload {
            Ok(())
        } else {
            Err(MetadataError::InvariantViolation {
                context,
                reason: "metadata state does not satisfy the operation invariant".into(),
            })
        }
    }

}
