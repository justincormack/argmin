use super::*;

fn combined_stream_segment_payload_crc64(segments: &[StreamUploadSegmentRecord]) -> u64 {
    segments
        .iter()
        .fold(checksum::crc64::checksum(&[]), |crc64, segment| {
            checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
        })
}

impl PgStore {
    #[cfg(test)]
    fn fail_next_delete_finalized_bucket_commit(&self) {
        self.fail_next_delete_finalized_bucket_commit
            .store(true, Ordering::Relaxed);
    }

    fn store_error_as_metadata_db(context: &'static str, error: StoreError) -> MetadataError {
        match error {
            StoreError::Db { source, .. } => MetadataError::Db { context, source },
            other => MetadataError::Db {
                context,
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    other.to_string(),
                ))),
            },
        }
    }

    pub(super) fn with_immediate_txn<T>(
        &self,
        begin_context: &'static str,
        commit_context: &'static str,
        body: impl FnOnce(&Self) -> Result<T, MetadataError>,
    ) -> Result<T, MetadataError> {
        if !self.conn.is_autocommit() {
            return body(self);
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: begin_context,
                source,
            })?;
        let result = body(self);
        match result {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|source| MetadataError::Db {
                        context: commit_context,
                        source,
                    })?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn next_bucket_execution_generation_in_txn(
        &self,
        context: &'static str,
    ) -> Result<u64, MetadataError> {
        self.query_row_cached_metadata(
            "UPDATE pg_counters \
             SET next_bucket_execution_generation = next_bucket_execution_generation + 1 \
             WHERE singleton = 0 \
             RETURNING next_bucket_execution_generation",
            [],
            context,
            |row| row.get::<_, i64>(0),
        )
        .and_then(|raw| {
            raw.try_into().map_err(|_| MetadataError::Db {
                context: "decode next bucket execution generation",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative next_bucket_execution_generation"),
                ),
            })
        })
    }

    pub(crate) fn next_bucket_execution_generation_candidate(&self) -> Result<u64, MetadataError> {
        let current: u64 = self
            .query_row_cached_metadata(
                "SELECT next_bucket_execution_generation FROM pg_counters WHERE singleton = 0",
                [],
                "read next bucket execution generation candidate",
                |row| row.get::<_, i64>(0),
            )
            .and_then(|raw| {
                raw.try_into().map_err(|_| MetadataError::Db {
                    context: "decode next bucket execution generation candidate",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("negative next_bucket_execution_generation"),
                    ),
                })
            })?;
        current.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "increment next bucket execution generation candidate",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket execution generation exceeds u64",
            )),
        })
    }

    pub(super) fn advance_bucket_execution_generation_in_txn(
        &self,
        generation: u64,
        context: &'static str,
    ) -> Result<(), MetadataError> {
        let generation = i64::try_from(generation).map_err(|_| MetadataError::Db {
            context: "encode bucket execution generation",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket execution generation exceeds i64",
            )),
        })?;
        self.execute_cached_metadata(
            "UPDATE pg_counters \
             SET next_bucket_execution_generation = max(next_bucket_execution_generation, ?1) \
             WHERE singleton = 0",
            params![generation],
            context,
        )?;
        Ok(())
    }

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
        let created_at_millis =
            i64::try_from(created_at_millis).map_err(|_| MetadataError::Db {
                context: "create bucket (encode created_at)",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(
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
                source: e,
            }
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
                     (name, owner_principal, owner_canonical_id, created_at, state, versioning, acl_grants, public_read, public_write, ownership_controls_mode, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years, bucket_execution_generation, bucket_incarnation_generation) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1, ?12, ?13, ?14, ?15, ?16, ?17)",
                    params![
                        config.name,
                        config.owner_principal,
                        config.owner_canonical_id.as_str(),
                        created_at_millis,
                        BucketState::Active as u8,
                        config.versioning as u8 as i64,
                        config.acl_grants.serialized(),
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
                        source,
                    }),
                }
            },
        )
    }

    fn insert_bucket_record_explicit(&self, bucket: &BucketRecord) -> Result<(), MetadataError> {
        let bucket = bucket.clone().command_metadata_projection();
        let created_at = i64::try_from(bucket.created_at).map_err(|_| MetadataError::Db {
            context: "create bucket record (encode created_at)",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket created_at exceeds i64",
            )),
        })?;
        let completed_multipart_upload_sequence =
            i64::try_from(bucket.completed_multipart_upload_sequence).map_err(|_| {
                MetadataError::Db {
                    context: "create bucket record (encode completed multipart sequence)",
                    source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                        "bucket completed multipart sequence exceeds i64",
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
                source: e,
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
                     (name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, completed_multipart_upload_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29)",
                    params![
                        bucket.name.as_str(),
                        &bucket.owner_principal,
                        bucket.owner_canonical_id.as_str(),
                        created_at,
                        bucket.region as i64,
                        bucket.state as u8 as i64,
                        bucket.versioning as u8 as i64,
                        bucket.acl_grants.serialized(),
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
                        completed_multipart_upload_sequence,
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
                        source,
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
            return Err(MetadataError::Db {
                context: conflict_context,
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if current.bucket_execution_generation > target.bucket_execution_generation {
            return Err(MetadataError::StaleBucketMetadataCommand {
                name: target.name.clone(),
                bucket_execution_generation: target.bucket_execution_generation,
            });
        }
        if !Self::bucket_record_preimage_matches_update(&current, target, effect) {
            return Err(MetadataError::Db {
                context: conflict_context,
                source: rusqlite::Error::InvalidQuery,
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
                            source,
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
                            source,
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
                                target.acl_grants.serialized(),
                                i32::from(target.public_read),
                                i32::from(target.public_write),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket acl",
                            source,
                        })?,
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(target.object_lock).map_err(
                                |e| MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e,
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
                                source,
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
                            source,
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
                                source,
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
                                source,
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
                            source,
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
            return Err(MetadataError::Db {
                context: "apply metadata command PG mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if !command.verify_checksum() {
            return Err(MetadataError::Db {
                context: "apply metadata command checksum",
                source: rusqlite::Error::InvalidQuery,
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
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(command) => {
                self.apply_advance_completed_multipart_upload_sequence_command(command)
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
            MetadataCommandPayload::DeleteCompletedMultipartUpload(command) => {
                self.apply_delete_completed_multipart_upload_command(command)
            }
        }
    }

    pub(crate) fn apply_metadata_command_and_record(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| {
                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                    context: "apply metadata command and record (begin txn)",
                    source,
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
                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(BucketSnapshotLoadError::Metadata(MetadataError::Db {
                        context: "apply metadata command and record (commit txn)",
                        source,
                    }));
                }
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
        match self.insert_bucket_record_explicit(&command.bucket) {
            Ok(()) => Ok(()),
            Err(MetadataError::BucketAlreadyExists) => {
                let existing = self.head_bucket_record_raw(&command.bucket.name)?;
                if existing.command_metadata_eq(&command.bucket) {
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

    fn apply_advance_completed_multipart_upload_sequence_command(
        &self,
        command: &AdvanceCompletedMultipartUploadSequenceCommand,
    ) -> Result<(), MetadataError> {
        self.advance_completed_multipart_upload_sequence_for_bucket(
            &command.bucket,
            command.completion_order,
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
        if self.direct_put_command_already_applied(command)? {
            return self.cleanup_direct_put_terminal_staging(command);
        }

        let reserved_generation = self.get_object_generation_reservation(
            &command.object.bucket,
            &command.object.key,
            &command.generation_reservation_id,
        )?;
        if reserved_generation != command.object.generation_id {
            return Err(MetadataError::Db {
                context: "commit direct put command reservation mismatch",
                source: rusqlite::Error::InvalidQuery,
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
                source: e,
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
                    store.delete_stream_upload_direct(&session.session_id)?;
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
                    return Err(MetadataError::Db {
                        context: "commit multipart object command (stream uploads mismatch)",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                if stream_upload_segments != command.stream_upload_segments {
                    return Err(MetadataError::Db {
                        context:
                            "commit multipart object command (stream upload segments mismatch)",
                        source: rusqlite::Error::InvalidQuery,
                    });
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
                store.insert_completed_multipart_upload_in_open_txn(command)?;
                store.release_multipart_completion_reservation_in_open_txn(command)?;
                for session in &command.stream_uploads {
                    store.delete_stream_upload_direct(&session.session_id)?;
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
            if part.part_okh == [0u8; 16] {
                streaming_segments.extend(self.get_multipart_part_segments(
                    &command.object.bucket,
                    &command.object.key,
                    command.object.version_id,
                    part.part_number,
                )?);
            }
        }
        Ok(streaming_segments == command.selected_streaming_segments)
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
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (prepare insert part segments)",
                source: e,
            })?;

        for segment in segments {
            if segment.bucket != object.bucket
                || segment.key != object.key
                || segment.version_id != object.version_id.to_u64()
            {
                return Err(MetadataError::Db {
                    context: "commit multipart object command (segment object mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
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
                source: e,
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
                source: e,
            })?;
        Ok(())
    }

    fn insert_completed_multipart_upload_in_open_txn(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO completed_multipart_uploads \
                 (upload_id, bucket, key, completion_order, completed_at, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    command.upload_id.as_str(),
                    &command.object.bucket,
                    &command.object.key,
                    command.completion_order as i64,
                    command.completed_at_millis as i64,
                    &command.object.owner.principal,
                    command.object.owner.canonical_id.as_str(),
                    command
                        .initiator
                        .as_ref()
                        .map(|owner| owner.principal.as_str()),
                    command
                        .initiator
                        .as_ref()
                        .map(|owner| owner.canonical_id.as_str()),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (insert completed upload)",
                source: e,
            })?;
        Ok(())
    }

    pub(crate) fn advance_completed_multipart_upload_sequence_for_bucket(
        &self,
        bucket: &BucketName,
        completion_order: u64,
    ) -> Result<(), MetadataError> {
        let completion_order = i64::try_from(completion_order).map_err(|_| MetadataError::Db {
            context: "commit multipart object command (completion order overflow)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("completion_order exceeds SQLite integer range"),
            ),
        })?;
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET completed_multipart_upload_sequence = \
                     CASE \
                         WHEN completed_multipart_upload_sequence < ?2 THEN ?2 \
                         ELSE completed_multipart_upload_sequence \
                     END \
                 WHERE name = ?1",
                params![bucket, completion_order],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (advance completed upload sequence)",
                source: e,
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
                source: e,
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
                source: e,
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
            Err(MetadataError::Db {
                context: "delete object payload reclaim command root mismatch",
                source: rusqlite::Error::InvalidQuery,
            })
        }
    }

    fn clear_object_payload_reclaim_claim_if_matches_in_open_txn(
        &self,
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<bool, MetadataError> {
        if command.payload.kind() != command.reclaim_claim.reclaim_kind {
            return Err(MetadataError::Db {
                context: "delete object payload reclaim command claim kind mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let bucket_incarnation_generation = i64::try_from(
            command.reclaim_claim.bucket_incarnation_generation,
        )
        .map_err(|source| MetadataError::Db {
            context: "delete object payload reclaim command claim incarnation",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
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
                source,
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
                source,
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
                    Some(_) => Err(MetadataError::Db {
                        context: "delete object payload reclaim command segment mismatch",
                        source: rusqlite::Error::InvalidQuery,
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
                            return Err(MetadataError::Db {
                                context: "delete object payload reclaim command kind mismatch",
                                source: rusqlite::Error::InvalidQuery,
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
                    Some(_) => Err(MetadataError::Db {
                        context: "delete object payload reclaim command multipart mismatch",
                        source: rusqlite::Error::InvalidQuery,
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
                            return Err(MetadataError::Db {
                                context: "delete object payload reclaim command kind mismatch",
                                source: rusqlite::Error::InvalidQuery,
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

    fn apply_delete_completed_multipart_upload_command(
        &self,
        command: &DeleteCompletedMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        match PgMetadataStore::get_completed_multipart_upload(self, &command.record.upload_id)? {
            Some(existing) if existing == command.record => {
                self.delete_completed_multipart_upload(&command.record.upload_id)
            }
            Some(_) => Err(MetadataError::Db {
                context: "delete completed multipart upload command row mismatch",
                source: rusqlite::Error::InvalidQuery,
            }),
            None => Ok(()),
        }
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
                            return Err(MetadataError::Db {
                                context: "delete object version command target mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
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
                        return Err(MetadataError::Db {
                            context: "delete object version command kind mismatch",
                            source: rusqlite::Error::InvalidQuery,
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
                    Ok(StoredObject::Live(_)) if command.version_id.is_null() => {}
                    Ok(_) => {
                        return Err(MetadataError::Db {
                            context: "insert delete marker command existing object mismatch",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    Err(MetadataError::ObjectNotFound) => {}
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

    fn apply_put_object_metadata_command(
        &self,
        command: &PutObjectMetadataCommand,
    ) -> Result<(), MetadataError> {
        if self.put_object_metadata_command_already_applied(command)? {
            return Ok(());
        }
        self.with_immediate_txn(
            "put object metadata command (begin txn)",
            "put object metadata command (commit txn)",
            |store| store.put_object_metadata_explicit_in_open_txn(&command.object),
        )
    }

    fn put_object_metadata_command_already_applied(
        &self,
        command: &PutObjectMetadataCommand,
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
        Ok(command.matches_object(&stored))
    }

    fn put_object_metadata_explicit_in_open_txn(
        &self,
        object: &LiveObjectRecord,
    ) -> Result<(), MetadataError> {
        let stored = match self.get_object_version(&object.bucket, &object.key, object.version_id) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) => {
                return Err(MetadataError::MethodNotAllowedOnDeleteMarker);
            }
            Err(error) => return Err(error),
        };
        if stored == *object {
            return Ok(());
        }
        if !Self::put_object_metadata_preimage_matches(&stored, object) {
            return Err(MetadataError::Db {
                context: "put object metadata command preimage mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let tags = object.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(object.object_lock).map_err(|e| MetadataError::Db {
                context: "put object metadata command (encode object lock)",
                source: e,
            })?;
        let updated = self.execute_cached_metadata(
            "UPDATE objects \
             SET tags = ?1, acl_grants = ?2, public_read = ?3, \
                 object_lock_retention_mode = ?4, object_lock_retain_until = ?5, \
                 object_lock_legal_hold = ?6 \
             WHERE bucket = ?7 AND key = ?8 AND version_id = ?9 AND status = ?10",
            params![
                tags,
                object.acl_grants.serialized(),
                i32::from(object.public_read),
                object_lock_retention_mode,
                object_lock_retain_until,
                object_lock_legal_hold,
                &object.bucket,
                &object.key,
                object.version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ],
            "put object metadata command",
        )?;
        if updated == 0 {
            return Err(MetadataError::ObjectNotFound);
        }
        Ok(())
    }

    fn put_object_metadata_preimage_matches(
        stored: &LiveObjectRecord,
        object: &LiveObjectRecord,
    ) -> bool {
        stored.bucket == object.bucket
            && stored.key == object.key
            && stored.version_id == object.version_id
            && stored.owner == object.owner
            && stored.generation_id == object.generation_id
            && stored.size == object.size
            && stored.etag == object.etag
            && stored.last_modified == object.last_modified
            && stored.became_noncurrent_at == object.became_noncurrent_at
            && stored.storage_class == object.storage_class
            && stored.ec == object.ec
            && stored.layout == object.layout
            && stored.metadata_blob == object.metadata_blob
            && stored.system_metadata_blob == object.system_metadata_blob
            && stored.encryption == object.encryption
    }

    fn create_stream_upload_explicit(
        &self,
        session: &StreamUploadCommandRecord,
        initial_next_segment_vid: GenerationId,
        bucket_write_reservation: Option<&BucketWriteReservationProof>,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload_explicit",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            session.session_id.as_str(),
            session.bucket.as_str(),
            session.key.as_str()
        );
        let op_kind = session.target.op_kind() as u8;
        let upload_id = session.target.upload_id();
        let part_number = session.target.part_number().map(|n| n as i64);
        let encryption_type = session.encryption.encryption_type() as u8;
        let encryption_state = session.encryption.encode_state();
        let reservation_id = bucket_write_reservation.map(|proof| proof.reservation_id.as_str());
        let owner_token = bucket_write_reservation.map(|proof| proof.owner_token.as_str());
        let cluster_epoch = bucket_write_reservation.map(|proof| proof.cluster_epoch.get() as i64);
        let execution_generation =
            bucket_write_reservation.map(|proof| proof.bucket_execution_generation as i64);
        let incarnation_generation =
            bucket_write_reservation.map(|proof| proof.bucket_incarnation_generation as i64);
        let operation_kind = bucket_write_reservation.map(|proof| proof.operation_kind.as_str());
        let created_at = bucket_write_reservation.map(|proof| proof.created_at as i64);
        let lease_deadline = bucket_write_reservation
            .and_then(|proof| proof.lease_deadline.map(|value| value as i64));
        let target_context =
            bucket_write_reservation.and_then(|proof| proof.target_context.as_deref());
        self.conn
            .execute(
                "INSERT INTO stream_uploads \
                 (session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, encryption_type, encryption_state, next_segment_vid, \
                  bucket_write_reservation_id, bucket_write_owner_token, bucket_write_cluster_epoch, bucket_write_execution_generation, \
                  bucket_write_incarnation_generation, bucket_write_operation_kind, bucket_write_created_at, bucket_write_lease_deadline, bucket_write_target_context) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
                params![
                    session.session_id,
                    session.bucket,
                    session.key,
                    op_kind,
                    upload_id,
                    part_number,
                    session.state as u8,
                    session.created_at as i64,
                    encryption_type,
                    encryption_state,
                    initial_next_segment_vid.get() as i64,
                    reservation_id,
                    owner_token,
                    cluster_epoch,
                    execution_generation,
                    incarnation_generation,
                    operation_kind,
                    created_at,
                    lease_deadline,
                    target_context,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create stream upload explicit",
                source: e,
            })?;
        Ok(())
    }

    fn apply_create_stream_upload_command(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.validate_create_stream_upload_command_target(command)?;
        match self.get_stream_upload(&command.session.session_id) {
            Ok(existing)
                if StreamUploadCommandRecord::from(&existing) == command.session
                    && existing.next_segment_vid == command.initial_next_segment_vid
                    && stream_upload_bucket_write_reservation_matches_command(
                        &existing, command,
                    ) =>
            {
                Ok(())
            }
            Ok(_) => Err(MetadataError::Db {
                context: "create stream upload command existing session mismatch",
                source: rusqlite::Error::InvalidQuery,
            }),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                let bucket_write_reservation = (command.session.target
                    == StreamUploadTarget::PutObject)
                    .then_some(&command.bucket_write_reservation);
                self.create_stream_upload_explicit(
                    &command.session,
                    command.initial_next_segment_vid,
                    bucket_write_reservation,
                )
            }
            Err(error) => Err(error),
        }
    }

    fn validate_create_stream_upload_command_target(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        match &command.session.target {
            StreamUploadTarget::PutObject => {
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::Db {
                        context: "create stream upload command state mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
            StreamUploadTarget::UploadPart { upload_id, .. } => {
                let upload = self.get_multipart_upload(upload_id)?;
                if upload.bucket != command.session.bucket
                    || upload.key != command.session.key
                    || upload.state != UploadState::InProgress
                {
                    return Err(MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if upload.encryption != command.session.encryption {
                    return Err(MetadataError::Db {
                        context: "create stream upload command upload encryption mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::Db {
                        context: "create stream upload command state mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
        }
    }

    fn apply_append_stream_segment_command(
        &self,
        command: &AppendStreamSegmentCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "append stream segment command (begin txn)",
            "append stream segment command (commit txn)",
            |store| {
                let session = store.get_stream_upload(&command.segment.session_id)?;
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::Db {
                        context: "append stream segment command session binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                if let StreamUploadTarget::UploadPart { upload_id, .. } = &session.target {
                    let upload = store.get_multipart_upload(upload_id)?;
                    if upload.bucket != command.bucket
                        || upload.key != command.key
                        || upload.state != UploadState::InProgress
                    {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: upload_id.to_string(),
                        });
                    }
                }

                let existing = store
                    .list_stream_segments(&command.segment.session_id)?
                    .into_iter()
                    .find(|segment| segment.segment_index == command.segment.segment_index);
                match existing {
                    Some(existing) if existing == command.segment => store
                        .advance_stream_segment_vid_floor(
                            &command.segment.session_id,
                            command.segment.segment_vid,
                        ),
                    Some(_) => Err(MetadataError::StreamSegmentConflict {
                        segment_index: command.segment.segment_index,
                    }),
                    None => {
                        store.append_stream_segment_direct(&command.segment)?;
                        store.advance_stream_segment_vid_floor(
                            &command.segment.session_id,
                            command.segment.segment_vid,
                        )
                    }
                }
            },
        )
    }

    fn apply_abort_stream_upload_command(
        &self,
        command: &AbortStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "abort stream upload command (begin txn)",
            "abort stream upload command (commit txn)",
            |store| {
                let session = match store.get_stream_upload(&command.session_id) {
                    Ok(session) => session,
                    Err(MetadataError::StreamSessionNotFound { .. }) => return Ok(()),
                    Err(error) => return Err(error),
                };
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::Db {
                        context: "abort stream upload command session binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                let staged_segments = store.list_stream_segments(&command.session_id)?;
                if staged_segments != command.staged_segments {
                    return Err(MetadataError::Db {
                        context: "abort stream upload command staged segment mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Aborted,
                )?;
                store.delete_stream_upload_direct(&command.session_id)
            },
        )
    }

    fn apply_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if self.commit_stream_part_command_already_applied(command)? {
            return Ok(());
        }

        self.with_immediate_txn(
            "commit stream part command (begin txn)",
            "commit stream part command (commit txn)",
            |store| {
                store.validate_commit_stream_part_command(command)?;

                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Completing,
                )?;
                store.insert_multipart_part_explicit(&command.part)?;
                store.delete_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                store.insert_multipart_part_segments_explicit(&command.segments)?;
                store.delete_stream_upload_direct(&command.session_id)
            },
        )
    }

    fn commit_stream_part_command_already_applied(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<bool, MetadataError> {
        match self.get_stream_upload(&command.session_id) {
            Ok(_) => Ok(false),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                let part = match self
                    .get_multipart_part(&command.part.upload_id, command.part.part_number)
                {
                    Ok(part) => part,
                    Err(MetadataError::PartNotFound { .. }) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let segments = self.get_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                if part == command.part && segments == command.segments {
                    Ok(true)
                } else {
                    Err(MetadataError::Db {
                        context: "commit stream part command applied result mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    fn validate_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if command.upload.bucket != command.bucket
            || command.upload.key != command.key
            || command.upload.state != UploadState::InProgress
            || command.part.upload_id != command.upload.upload_id
        {
            return Err(MetadataError::Db {
                context: "commit stream part command upload binding mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if command.part.part_okh != [0u8; 16] {
            return Err(MetadataError::Db {
                context: "commit stream part command non-streamed part",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let expected_generation = match command.existing_part.as_ref() {
            Some(existing) => {
                if existing.upload_id != command.upload.upload_id
                    || existing.part_number != command.part.part_number
                {
                    return Err(MetadataError::Db {
                        context: "commit stream part command existing part binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                existing
                    .generation
                    .checked_add(1)
                    .ok_or(MetadataError::Db {
                        context: "commit stream part command generation overflow",
                        source: rusqlite::Error::InvalidQuery,
                    })?
            }
            None => 0,
        };
        if command.part.generation != expected_generation {
            return Err(MetadataError::Db {
                context: "commit stream part command generation mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let session = self.get_stream_upload(&command.session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(MetadataError::StreamSessionNotInProgress {
                state: session.state as u8,
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number,
            } if upload_id == &command.upload.upload_id
                && *part_number == command.part.part_number => {}
            _ => {
                return Err(MetadataError::StreamSessionNotFound {
                    session_id: command.session_id.as_str().to_owned(),
                })
            }
        }
        if session.bucket != command.bucket || session.key != command.key {
            return Err(MetadataError::Db {
                context: "commit stream part command session binding mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let upload = self.get_multipart_upload(&command.upload.upload_id)?;
        if upload != command.upload {
            return Err(MetadataError::Db {
                context: "commit stream part command upload mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let existing_part =
            match self.get_multipart_part(&command.part.upload_id, command.part.part_number) {
                Ok(part) => Some(part),
                Err(MetadataError::PartNotFound { .. }) => None,
                Err(error) => return Err(error),
            };
        if existing_part != command.existing_part {
            return Err(MetadataError::Db {
                context: "commit stream part command existing part mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let displaced_segments = self.get_multipart_part_segments_for_upload_part(
            &command.bucket,
            &command.key,
            &command.upload.upload_id,
            command.part.part_number,
        )?;
        if displaced_segments != command.displaced_segments {
            return Err(MetadataError::Db {
                context: "commit stream part command displaced segments mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let staged_segments = self.list_stream_segments(&command.session_id)?;
        if staged_segments.len() != command.segments.len()
            || staged_segments
                .iter()
                .zip(command.segments.iter())
                .any(|(staged, segment)| {
                    staged.segment_index != segment.segment_index
                        || staged.size != segment.size
                        || staged.segment_crc64 != segment.segment_crc64
                        || staged.segment_okh != segment.segment_okh
                        || staged.segment_vid != segment.segment_vid
                        || staged.data_pg_id != segment.data_pg_id
                        || staged.placement_cluster_epoch != segment.placement_cluster_epoch
                        || staged.ec_k != segment.ec_k
                        || staged.ec_m != segment.ec_m
                })
        {
            return Err(MetadataError::Db {
                context: "commit stream part command staged segments mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let staged_segments_total: u64 = staged_segments.iter().map(|segment| segment.size).sum();
        if staged_segments_total != command.part.size {
            return Err(MetadataError::Db {
                context: "commit stream part command staged payload size mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let staged_crc64 = combined_stream_segment_payload_crc64(&staged_segments);
        if staged_crc64 != command.part.payload_crc64 {
            return Err(MetadataError::Db {
                context: "commit stream part command staged payload CRC64 mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        for segment in &command.segments {
            if segment.bucket != command.bucket
                || segment.key != command.key
                || segment.upload_id != command.upload.upload_id
                || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                || segment.part_number != command.part.part_number
            {
                return Err(MetadataError::Db {
                    context: "commit stream part command segment binding mismatch",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }
        Ok(())
    }

    fn insert_multipart_part_explicit(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.payload_crc64 as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.placement_cluster_epoch.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "insert multipart part explicit",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4",
                params![bucket, key, upload_id, part_number],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments for upload part",
                source: e,
            })?;
        Ok(())
    }

    fn insert_multipart_part_segments_explicit(
        &self,
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
            .map_err(|e| MetadataError::Db {
                context: "prepare insert multipart part segments explicit",
                source: e,
            })?;
        for segment in segments {
            stmt.execute(params![
                segment.bucket,
                segment.key,
                segment.upload_id,
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
                context: "insert multipart part segment explicit",
                source: e,
            })?;
        }
        Ok(())
    }

    fn apply_create_multipart_upload_command(
        &self,
        command: &CreateMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        if command.upload.state != UploadState::InProgress {
            return Err(MetadataError::Db {
                context: "create multipart upload command state mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        self.create_multipart_upload_explicit(&command.upload)
    }

    fn create_multipart_upload_explicit(
        &self,
        upload: &MultipartUploadRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_multipart_upload_explicit",
            "pg_id={} upload_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            upload.upload_id.as_str(),
            upload.bucket.as_str(),
            upload.key.as_str()
        );
        let algo = upload.checksum.map(|c| c.algorithm() as u8);
        let ctype = upload.checksum.map(|c| c.checksum_type() as u8);
        let tags = upload.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(upload.object_lock).map_err(|e| MetadataError::Db {
                context: "create multipart upload (encode object lock)",
                source: e,
            })?;
        let encryption_type = upload.encryption.encryption_type() as u8;
        let encryption_state = upload.encryption.encode_state();
        let system_metadata_blob = upload.system_metadata_blob.as_slice();
        self.with_immediate_txn(
            "create multipart upload (begin txn)",
            "create multipart upload (commit txn)",
            |store| {
                match store.conn.execute(
                    "INSERT INTO object_generation_reservations \
                     (reservation_id, bucket, key, generation_id, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        upload.upload_id.as_str(),
                        upload.bucket,
                        upload.key,
                        upload.object_generation_id.get() as i64,
                        upload.initiated_at as i64,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing_generation = store
                            .conn
                            .query_row(
                                "SELECT generation_id FROM object_generation_reservations \
                                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                                params![upload.upload_id.as_str(), upload.bucket, upload.key],
                                |row| {
                                    let raw: i64 = row.get(0)?;
                                    Self::parse_generation_id(raw, 0, "generation_id")
                                },
                            )
                            .optional()
                            .map_err(|e| MetadataError::Db {
                                context: "create multipart upload explicit reservation lookup",
                                source: e,
                            })?;
                        if !matches!(existing_generation, Some(existing) if existing == upload.object_generation_id)
                        {
                            return Err(MetadataError::Db {
                                context: "create multipart upload explicit reservation mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db {
                            context: "create multipart upload (reserve generation)",
                            source: e,
                        });
                    }
                }
                match store.conn.execute(
                    "INSERT INTO multipart_uploads \
                     (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, system_metadata_blob, owner_principal, owner_canonical_id, \
                      initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_generation_id, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
                    params![
                        upload.upload_id,
                        upload.bucket,
                        upload.key,
                        upload.initiated_at as i64,
                        upload.state as u8,
                        tags,
                        upload.metadata_blob.as_slice(),
                        system_metadata_blob,
                        upload.owner.principal,
                        upload.owner.canonical_id.as_str(),
                        upload.initiator.as_ref().map(|owner| owner.principal.as_str()),
                        upload.initiator
                            .as_ref()
                            .map(|owner| owner.canonical_id.as_str()),
                        algo,
                        ctype,
                        encryption_type,
                        encryption_state,
                        upload.acl_grants.serialized(),
                        i32::from(upload.public_read),
                        upload.object_generation_id.get() as i64,
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing = store.get_multipart_upload(&upload.upload_id)?;
                        if existing != *upload {
                            return Err(MetadataError::Db {
                                context: "create multipart upload explicit existing upload mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db {
                            context: "create multipart upload",
                            source: e,
                        });
                    }
                }
                Ok(())
            },
        )
    }

    fn list_stream_uploads_for_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let sql = format!(
            "{STREAM_UPLOAD_SELECT} WHERE op_kind = ?1 AND upload_id = ?2 ORDER BY session_id ASC"
        );
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![StreamUploadKind::UploadPart as u8, upload_id.as_str()],
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (collect)",
                source: e,
            })
    }

    fn list_stream_segments_for_sessions(
        &self,
        sessions: &[StreamUploadRecord],
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut segments = Vec::new();
        for session in sessions {
            segments.extend(self.list_stream_segments(&session.session_id)?);
        }
        Ok(segments)
    }

    pub(crate) fn prepare_abort_multipart_upload_cleanup(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare abort multipart upload cleanup (begin txn)",
            "prepare abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(upload_id) {
                    Ok(upload) => {
                        if upload.bucket != *bucket || upload.key != *key {
                            return Err(MetadataError::NoSuchUpload {
                                upload_id: upload_id.to_string(),
                            });
                        }
                        upload
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(upload_id)?;
                let stream_uploads = store.list_stream_uploads_for_multipart_upload(upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                let stream_uploads = stream_uploads
                    .iter()
                    .map(TerminalStreamCleanupRecord::from)
                    .collect();

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    pub(crate) fn prepare_authorized_abort_multipart_upload_cleanup(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare authorized abort multipart upload cleanup (begin txn)",
            "prepare authorized abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(&authorized_upload.record().upload_id)
                {
                    Ok(upload) => upload,
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if upload != *authorized_upload.record() {
                    return Ok(None);
                }

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload.upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(&upload.upload_id)?;
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&upload.upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                let stream_uploads = stream_uploads
                    .iter()
                    .map(TerminalStreamCleanupRecord::from)
                    .collect();

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    fn apply_abort_multipart_upload_command(
        &self,
        command: &AbortMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "abort multipart upload command (begin txn)",
            "abort multipart upload command (commit txn)",
            |store| {
                let upload_present = match store.get_multipart_upload(&command.upload_id) {
                    Ok(upload) => {
                        if upload.bucket != command.bucket || upload.key != command.key {
                            return Err(MetadataError::Db {
                                context: "abort multipart upload command (upload mismatch)",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        if upload != command.cleanup.upload {
                            return Err(MetadataError::Db {
                                context: "abort multipart upload command (cleanup mismatch)",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        true
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => false,
                    Err(error) => return Err(error),
                };
                if upload_present {
                    let stream_uploads =
                        store.list_stream_uploads_for_multipart_upload(&command.upload_id)?;
                    if !Self::stream_upload_cleanup_records_match(
                        &stream_uploads,
                        &command.cleanup.stream_uploads,
                    ) {
                        return Err(MetadataError::Db {
                            context: "abort multipart upload command (stream uploads mismatch)",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    let stream_upload_segments =
                        store.list_stream_segments_for_sessions(&stream_uploads)?;
                    if stream_upload_segments != command.cleanup.stream_upload_segments {
                        return Err(MetadataError::Db {
                            context:
                                "abort multipart upload command (stream upload segments mismatch)",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    for session in &command.cleanup.stream_uploads {
                        store.delete_stream_upload_direct(&session.session_id)?;
                    }
                }
                store.delete_multipart_part_segments_by_upload_id_direct(&command.upload_id)?;
                store
                    .conn
                    .execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete generation reservation)",
                        source: e,
                    })?;
                store
                    .conn
                    .execute(
                        "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete upload)",
                        source: e,
                    })?;
                Ok(())
            },
        )
    }

    fn put_delete_marker_explicit_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        owner: &OwnerIdentity,
        write_sequence: u64,
        last_modified_millis: u64,
    ) -> Result<(), MetadataError> {
        self.mark_current_live_noncurrent(
            bucket.as_str(),
            key.as_str(),
            version_id,
            last_modified_millis,
        )
        .map_err(|e| MetadataError::Db {
            context: "put object meta (mark noncurrent delete marker)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(bucket, key, version_id)?;
        self.advance_object_write_counter_in_open_txn(bucket, key, write_sequence, None)?;
        let sql = if version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        };
        self.execute_cached_metadata(
            sql,
            params![
                bucket,
                key,
                version_id.to_u64() as i64,
                write_sequence as i64,
                last_modified_millis as i64,
                owner.principal,
                owner.canonical_id.as_str(),
                "",
            ],
            "put object meta (delete marker)",
        )?;
        Ok(())
    }

    fn delete_object_version_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let write_sequence = self.next_object_write_sequence(bucket.as_str(), key.as_str())?;
        self.advance_object_write_counter_in_open_txn(bucket, key, write_sequence, None)?;
        let deleted_was_current: bool = self.query_row_cached_metadata(
            "SELECT EXISTS( \
                 SELECT 1 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
             )",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version (lookup current)",
            |row| row.get::<_, i64>(0).map(|value| value != 0),
        )?;

        self.execute_cached_metadata(
            "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version",
        )?;

        if deleted_was_current {
            self.clear_current_live_noncurrent(bucket.as_str(), key.as_str())
                .map_err(|e| MetadataError::Db {
                    context: "delete object version (restore current)",
                    source: e,
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_bucket_versioning_inner(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current, current_generation): (BucketVersioningState, u64) = self
            .query_row_cached_optional_metadata(
                "SELECT versioning, bucket_execution_generation FROM buckets WHERE name = ?1",
                params![name.as_str()],
                "get bucket versioning",
                |row| {
                    let raw_versioning = row.get::<_, u8>(0)?;
                    let raw_generation = row.get::<_, i64>(1)?;
                    Ok((raw_versioning, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(|(raw_versioning, raw_generation)| {
                let versioning =
                    BucketVersioningState::from_u8(raw_versioning).ok_or_else(|| {
                        MetadataError::Db {
                            context: "invalid versioning state in database",
                            source: rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid versioning: {raw_versioning}")),
                            ),
                        }
                    })?;
                let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?;
                Ok((versioning, generation))
            })?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current == state {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: "apply conflicting bucket versioning command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::Db {
                    context: "apply stale bucket versioning command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }
        if state == BucketVersioningState::Disabled && current != BucketVersioningState::Disabled {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current,
                to: state,
            });
        }

        self.with_immediate_txn(
            "put bucket versioning (begin txn)",
            "put bucket versioning (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket versioning (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket versioning (advance execution generation)",
                        )?;
                        generation
                    }
                };
                store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET versioning = ?1, \
                         bucket_execution_generation = ?2 \
                     WHERE name = ?3",
                    params![state as u8 as i64, generation as i64, name.as_str()],
                    "put bucket versioning",
                )?;
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn put_bucket_acl_inner(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current_acl_grants, current_public_read, current_public_write, current_generation): (
            AclGrants,
            bool,
            bool,
            u64,
        ) = self
            .query_row_cached_optional_metadata(
                "SELECT acl_grants, public_read, public_write, bucket_execution_generation \
                 FROM buckets \
                 WHERE name = ?1",
                params![name.as_str()],
                "get bucket acl",
                |row| {
                    let raw_acl_grants = row.get::<_, String>(0)?;
                    let public_read = row.get::<_, i64>(1)? != 0;
                    let public_write = row.get::<_, i64>(2)? != 0;
                    let raw_generation = row.get::<_, i64>(3)?;
                    Ok((raw_acl_grants, public_read, public_write, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(
                |(raw_acl_grants, public_read, public_write, raw_generation)| {
                    let acl_grants = Self::parse_acl_grants(raw_acl_grants, 0, "bucket acl")
                        .map_err(|source| MetadataError::Db {
                            context: "decode bucket acl",
                            source,
                        })?;
                    let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                        context: "decode bucket execution generation",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Integer,
                            Box::from("negative bucket_execution_generation"),
                        ),
                    })?;
                    Ok((acl_grants, public_read, public_write, generation))
                },
            )?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current_acl_grants == *acl_grants
                    && current_public_read == public_read
                    && current_public_write == public_write
                {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: "apply conflicting bucket acl command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::Db {
                    context: "apply stale bucket acl command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }

        self.with_immediate_txn(
            "put bucket acl (begin txn)",
            "put bucket acl (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket acl (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket acl (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET acl_grants = ?1, \
                         public_read = ?2, \
                         public_write = ?3, \
                         bucket_execution_generation = ?4 \
                     WHERE name = ?5",
                    params![
                        acl_grants.serialized(),
                        i32::from(public_read),
                        i32::from(public_write),
                        generation as i64,
                        name.as_str()
                    ],
                    "put bucket acl",
                )?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn bucket_property_matches(
        &self,
        info: &BucketInfo,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, MetadataError> {
        match mutation {
            BucketPropertyMutation::ObjectLock(config) => Ok(info.object_lock == *config),
            BucketPropertyMutation::Encryption(config) => {
                Ok(PgMetadataStore::get_bucket_encryption(self, &info.name)? == *config)
            }
            BucketPropertyMutation::PublicAccessBlock(config) => {
                Ok(info.public_access_block == *config)
            }
            BucketPropertyMutation::OwnershipControls(config) => {
                Ok(info.ownership_controls == *config)
            }
            BucketPropertyMutation::AbacEnabled(enabled) => {
                Ok(info.bucket_abac_enabled == *enabled)
            }
        }
    }

    #[cfg(test)]
    fn put_bucket_property_inner(
        &self,
        name: &BucketName,
        mutation: &BucketPropertyMutation,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let info = self.head_bucket_raw(name)?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if info.bucket_execution_generation == explicit {
                if self.bucket_property_matches(&info, mutation)? {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: bucket_property_conflict_context(mutation.effect()),
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if info.bucket_execution_generation > explicit {
                return Err(MetadataError::Db {
                    context: bucket_property_stale_context(mutation.effect()),
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }

        self.with_immediate_txn(
            "put bucket property (begin txn)",
            "put bucket property (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket property (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket property (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = match mutation {
                    BucketPropertyMutation::ObjectLock(config) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(*config).map_err(|e| {
                                MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e,
                                }
                            })?;
                        store.execute_cached_metadata(
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
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket object lock",
                        )?
                    }
                    BucketPropertyMutation::Encryption(config) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET default_encryption_type = ?1, \
                                 sse_c_blocked = ?2, \
                                 bucket_execution_generation = ?3 \
                             WHERE name = ?4",
                        params![
                            config.default_encryption.map(|value| value as u8),
                            i32::from(config.sse_c_blocked),
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket encryption",
                    )?,
                    BucketPropertyMutation::PublicAccessBlock(config) => {
                        let (
                            present,
                            block_public_acls,
                            ignore_public_acls,
                            block_public_policy,
                            restrict_public_buckets,
                        ) = Self::public_access_block_sql_values(*config);
                        store.execute_cached_metadata(
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
                                generation as i64,
                                name.as_str(),
                            ],
                            "put bucket public access block",
                        )?
                    }
                    BucketPropertyMutation::OwnershipControls(config) => store
                        .execute_cached_metadata(
                            "UPDATE buckets \
                             SET ownership_controls_mode = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                Self::ownership_controls_sql_value(*config),
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket ownership controls",
                        )?,
                    BucketPropertyMutation::AbacEnabled(enabled) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET bucket_abac_enabled = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                        params![
                            if *enabled { 1 } else { 0 },
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket abac enabled",
                    )?,
                };
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    pub fn load_bucket_execution_generations(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, MetadataError> {
        if buckets.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", buckets.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT name, bucket_execution_generation \
             FROM buckets \
             WHERE name IN ({placeholders})"
        );
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|source| MetadataError::Db {
                context: "prepare load bucket execution generations",
                source,
            })?;
        let rows = stmt
            .query_map(
                params_from_iter(buckets.iter().map(|bucket| bucket.as_str())),
                |row| Ok((row.get::<_, BucketName>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(|source| MetadataError::Db {
                context: "query load bucket execution generations",
                source,
            })?;
        let mut generations = HashMap::with_capacity(buckets.len());
        for row in rows {
            let (bucket, generation) = row.map_err(|source| MetadataError::Db {
                context: "row load bucket execution generations",
                source,
            })?;
            generations.insert(
                bucket,
                generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?,
            );
        }
        Ok(generations)
    }

    pub fn load_bucket_fast_path_identities(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, MetadataError> {
        if buckets.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", buckets.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT name, bucket_execution_generation, bucket_incarnation_generation \
             FROM buckets \
             WHERE name IN ({placeholders})"
        );
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|source| MetadataError::Db {
                context: "prepare load bucket fast path identities",
                source,
            })?;
        let rows = stmt
            .query_map(
                params_from_iter(buckets.iter().map(|bucket| bucket.as_str())),
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(|source| MetadataError::Db {
                context: "query load bucket fast path identities",
                source,
            })?;
        let mut identities = HashMap::with_capacity(buckets.len());
        for row in rows {
            let (bucket, execution, incarnation) = row.map_err(|source| MetadataError::Db {
                context: "row load bucket fast path identities",
                source,
            })?;
            let bucket_execution_generation =
                execution.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket fast path execution generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?;
            let bucket_incarnation_generation =
                incarnation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket fast path incarnation generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_incarnation_generation"),
                    ),
                })?;
            identities.insert(
                bucket,
                BucketFastPathIdentity {
                    bucket_execution_generation,
                    bucket_incarnation_generation,
                },
            );
        }
        Ok(identities)
    }

    pub(crate) fn next_object_write_sequence(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<u64, MetadataError> {
        let stored_next: Option<i64> = self.query_row_cached_optional_metadata(
            "SELECT next_write_sequence FROM object_write_counters \
             WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            "next object write sequence counter",
            |row| row.get(0),
        )?;
        if let Some(value) = stored_next {
            return u64::try_from(value).map_err(|_| MetadataError::Db {
                context: "negative object write counter in database",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("negative next_write_sequence: {value}")),
                ),
            });
        }

        let max: Option<i64> = self
            .query_row_cached_optional_metadata(
                "SELECT MAX(write_sequence) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                "next object write sequence",
                |row| row.get(0),
            )?
            .flatten();

        match max {
            None => Ok(1),
            Some(value) => {
                let current = u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(write_sequence) overflow"),
                    ),
                })
            }
        }
    }

    fn current_object_write_counter_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(u64, Option<u64>), MetadataError> {
        let stored: Option<(i64, Option<i64>)> = self.query_row_cached_optional_metadata(
            "SELECT next_write_sequence, max_committed_generation \
             FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            "load object write counter",
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if let Some((next_write_sequence, max_committed_generation)) = stored {
            let next_write_sequence =
                u64::try_from(next_write_sequence).map_err(|_| MetadataError::Db {
                    context: "negative object write counter in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!(
                            "negative next_write_sequence: {next_write_sequence}"
                        )),
                    ),
                })?;
            let max_committed_generation = max_committed_generation
                .map(|value| {
                    u64::try_from(value).map_err(|_| MetadataError::Db {
                        context: "negative object write generation counter in database",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("negative max_committed_generation: {value}")),
                        ),
                    })
                })
                .transpose()?;
            return Ok((next_write_sequence, max_committed_generation));
        }

        let max: Option<(Option<i64>, Option<i64>)> = self.query_row_cached_optional_metadata(
            "SELECT MAX(write_sequence), MAX(generation_id) FROM objects \
             WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            "bootstrap object write counter",
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let (max_write_sequence, max_generation_id) = max.unwrap_or((None, None));
        let next_write_sequence = match max_write_sequence {
            None => 1,
            Some(value) => {
                let current = u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(write_sequence) overflow"),
                    ),
                })?
            }
        };
        let max_generation_id = max_generation_id
            .map(|value| {
                u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative generation_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(generation_id): {value}")),
                    ),
                })
            })
            .transpose()?;
        Ok((next_write_sequence, max_generation_id))
    }

    fn advance_object_write_counter_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        write_sequence: u64,
        generation_id: Option<GenerationId>,
    ) -> Result<(), MetadataError> {
        let (expected_write_sequence, max_generation_id) =
            self.current_object_write_counter_in_open_txn(bucket, key)?;
        if write_sequence != expected_write_sequence {
            return Err(MetadataError::StaleObjectWriteCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                write_sequence,
                generation_id: generation_id.map(GenerationId::get),
            });
        }
        let next_write_sequence =
            write_sequence
                .checked_add(1)
                .ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("object write counter overflow"),
                    ),
                })?;
        let max_committed_generation = match (max_generation_id, generation_id) {
            (Some(current), Some(generation_id)) => Some(current.max(generation_id.get())),
            (None, Some(generation_id)) => Some(generation_id.get()),
            (current, None) => current,
        };
        self.execute_cached_metadata(
            "INSERT INTO object_write_counters \
             (bucket, key, next_write_sequence, max_committed_generation) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(bucket, key) DO UPDATE SET \
                 next_write_sequence = excluded.next_write_sequence, \
                 max_committed_generation = excluded.max_committed_generation",
            params![
                bucket,
                key,
                next_write_sequence as i64,
                max_committed_generation.map(|value| value as i64),
            ],
            "advance object write counter",
        )?;
        Ok(())
    }

    fn advance_object_version_counter_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        if version_id.is_null() {
            return Ok(());
        }
        let following = version_id
            .to_u64()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Db {
                context: "advance object version counter",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("version_id overflow"),
                ),
            })?;
        let following = i64::try_from(following).map_err(|_| MetadataError::Db {
            context: "advance object version counter",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("version_id exceeds SQLite integer range"),
            ),
        })?;
        self.execute_cached_metadata(
            "INSERT INTO object_version_counters (bucket, key, next_version_id) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(bucket, key) DO UPDATE SET \
                 next_version_id = max(object_version_counters.next_version_id, excluded.next_version_id)",
            params![bucket, key, following],
            "advance object version counter",
        )?;
        Ok(())
    }

    fn reserve_object_version_explicit(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        if version_id.is_null() {
            return Err(MetadataError::Db {
                context: "reserve object version command null version",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let expected = self.next_version_id(bucket, key)?;
        if expected.to_u64() > version_id.to_u64() {
            return Err(MetadataError::ObjectVersionReservationConflict { version_id });
        }
        // A command from the active primary may be ahead of this replica if a
        // prior reservation partially applied before restart and the pending
        // in-memory command was lost. Advancing forward is safe for this
        // allocator: version ids are opaque and gaps are preferable to making
        // the key permanently unwritable.
        self.advance_object_version_counter_in_open_txn(bucket, key, version_id)
    }

    pub(crate) fn object_write_sequence(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Option<u64>, MetadataError> {
        self.conn
            .prepare_cached(
                "SELECT write_sequence FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            )
            .and_then(|mut stmt| {
                stmt.query_row(
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get::<_, i64>(0),
                )
            })
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object write sequence",
                source: e,
            })?
            .map(|value| {
                u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative write_sequence: {value}")),
                    ),
                })
            })
            .transpose()
    }

    fn mark_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
        replacement_version_id: VersionId,
        transition_time: u64,
    ) -> Result<(), rusqlite::Error> {
        self.conn
            .prepare_cached(
                "UPDATE objects SET became_noncurrent_at = ?1 \
                 WHERE bucket = ?2 AND key = ?3 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?2 AND key = ?3 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
                   AND version_id <> ?4 \
                   AND status = ?5 \
                   AND became_noncurrent_at IS NULL",
            )?
            .execute(params![
                transition_time as i64,
                bucket,
                key,
                replacement_version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ])?;
        Ok(())
    }

    fn clear_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn
            .prepare_cached(
                "UPDATE objects SET became_noncurrent_at = NULL \
                 WHERE bucket = ?1 AND key = ?2 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
                   AND status = ?3 \
                   AND became_noncurrent_at IS NOT NULL",
            )?
            .execute(params![bucket, key, ObjectState::Live as u8])?;
        Ok(())
    }

    pub(crate) fn completed_multipart_upload_sequence_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, MetadataError> {
        let bucket = bucket.as_str();
        self.query_row_cached_metadata(
            "SELECT completed_multipart_upload_sequence FROM buckets WHERE name = ?1",
            params![bucket],
            "read completed multipart upload sequence",
            |row| row.get::<_, i64>(0),
        )?
        .try_into()
        .map_err(|_| MetadataError::Db {
            context: "decode completed multipart upload sequence",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("negative completed multipart upload sequence"),
            ),
        })
    }

    pub fn list_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<(UploadId, u64)>, MetadataError> {
        self.list_completed_multipart_upload_records_for_bucket(bucket)
            .map(|records| {
                records
                    .into_iter()
                    .map(|record| (record.upload_id, record.completion_order))
                    .collect()
            })
    }

    pub(crate) fn list_completed_multipart_upload_records_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<CompletedMultipartUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                        owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads \
                 WHERE bucket = ?1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list completed multipart uploads for bucket",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![bucket], |row| {
                Self::completed_multipart_upload_record_from_row(row)
            })
            .map_err(|e| MetadataError::Db {
                context: "query list completed multipart uploads for bucket",
                source: e,
            })?;
        let mut uploads = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "row list completed multipart uploads for bucket",
                source: e,
            })?);
        }
        Ok(uploads)
    }

    pub(crate) fn list_completed_multipart_upload_records_for_bucket_page(
        &self,
        bucket: &BucketName,
        upload_id_marker: Option<&UploadId>,
        limit: u32,
    ) -> Result<CompletedMultipartUploadRecordPage, MetadataError> {
        let fetch_limit = i64::from(limit) + 1;
        let (sql, params_vec): (
            &str,
            Vec<Box<dyn rusqlite::types::ToSql>>,
        ) = match upload_id_marker {
            Some(marker) => (
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                        owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads \
                 WHERE bucket = ?1 AND upload_id > ?2 \
                 ORDER BY upload_id ASC LIMIT ?3",
                vec![
                    Box::new(bucket.clone()),
                    Box::new(marker.clone()),
                    Box::new(fetch_limit),
                ],
            ),
            None => (
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                        owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads \
                 WHERE bucket = ?1 \
                 ORDER BY upload_id ASC LIMIT ?2",
                vec![Box::new(bucket.clone()), Box::new(fetch_limit)],
            ),
        };
        let params = rusqlite::params_from_iter(params_vec.iter());
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list completed multipart uploads for bucket page",
                source: e,
            })?;
        let rows = stmt
            .query_map(params, Self::completed_multipart_upload_record_from_row)
            .map_err(|e| MetadataError::Db {
                context: "query list completed multipart uploads for bucket page",
                source: e,
            })?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(|e| MetadataError::Db {
                context: "row list completed multipart uploads for bucket page",
                source: e,
            })?);
        }
        let next_upload_id_marker = if records.len() > limit as usize {
            records.pop();
            records.last().map(|record| record.upload_id.clone())
        } else {
            None
        };
        Ok(CompletedMultipartUploadRecordPage {
            records,
            next_upload_id_marker,
        })
    }

    fn completed_multipart_upload_record_from_row(
        row: &rusqlite::Row<'_>,
    ) -> Result<CompletedMultipartUploadRecord, rusqlite::Error> {
        let completion_order = row.get::<_, i64>(3)?.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::from("negative completion order"),
            )
        })?;
        let completed_at = row.get::<_, i64>(4)?.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::from("negative completed_at"),
            )
        })?;
        let owner = Self::parse_owner_identity(row, 5, 6, "owner_principal", "owner_canonical_id")?;
        let initiator = Self::parse_optional_owner_identity(
            row,
            7,
            8,
            "initiator_principal",
            "initiator_canonical_id",
        )?;
        Ok(CompletedMultipartUploadRecord {
            upload_id: row.get(0)?,
            bucket: row.get(1)?,
            key: row.get(2)?,
            completion_order,
            completed_at,
            initiator,
            owner,
        })
    }

    fn reserve_object_generation_explicit(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
        created_at_millis: u64,
    ) -> Result<(), MetadataError> {
        match self.conn.execute(
            "INSERT INTO object_generation_reservations \
             (reservation_id, bucket, key, generation_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                reservation_id.as_str(),
                bucket,
                key,
                generation_id.get() as i64,
                created_at_millis as i64,
            ],
        ) {
            Ok(_) => Ok(()),
            Err(source) => match Self::object_generation_reservation_constraint_kind(&source) {
                Some(ObjectGenerationReservationConstraint::ReservationId) => {
                    match self.get_object_generation_reservation_by_id(reservation_id) {
                        Ok(existing)
                            if existing.bucket == *bucket
                                && existing.key == *key
                                && existing.generation_id == generation_id =>
                        {
                            Ok(())
                        }
                        Ok(_) | Err(MetadataError::ObjectGenerationReservationNotFound { .. }) => {
                            Err(MetadataError::Db {
                                context: "reserve object generation explicit",
                                source,
                            })
                        }
                        Err(error) => Err(error),
                    }
                }
                Some(ObjectGenerationReservationConstraint::Generation) => {
                    Err(MetadataError::ObjectGenerationReservationConflict {
                        reservation_id: reservation_id.as_str().to_string(),
                        generation_id: generation_id.get(),
                    })
                }
                None => Err(MetadataError::Db {
                    context: "reserve object generation explicit",
                    source,
                }),
            },
        }
    }

    fn get_object_generation_reservation_by_id(
        &self,
        reservation_id: &SessionId,
    ) -> Result<ObjectGenerationReservationIdentity, MetadataError> {
        let raw = self
            .query_row_cached_optional_metadata(
                "SELECT bucket, key, generation_id FROM object_generation_reservations \
                 WHERE reservation_id = ?1",
                params![reservation_id.as_str()],
                "get object generation reservation by id",
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )?
            .ok_or_else(|| MetadataError::ObjectGenerationReservationNotFound {
                reservation_id: reservation_id.as_str().to_owned(),
            })?;
        let generation_id =
            Self::parse_generation_id(raw.2, 2, "generation_id").map_err(|source| {
                MetadataError::Db {
                    context: "parse object generation reservation by id",
                    source,
                }
            })?;
        Ok(ObjectGenerationReservationIdentity {
            bucket: raw.0,
            key: raw.1,
            generation_id,
        })
    }

    fn object_generation_reservation_constraint_kind(
        source: &rusqlite::Error,
    ) -> Option<ObjectGenerationReservationConstraint> {
        let rusqlite::Error::SqliteFailure(err, _) = source else {
            return None;
        };
        match err.extended_code {
            rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY => {
                Some(ObjectGenerationReservationConstraint::ReservationId)
            }
            rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE => {
                Some(ObjectGenerationReservationConstraint::Generation)
            }
            _ => None,
        }
    }

    fn put_object_segments_reclaim_in_open_txn(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO object_segments_reclaims \
                 (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    reclaim.bucket,
                    reclaim.key,
                    reclaim.generation_id.get() as i64,
                    reclaim.created_at as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object segments reclaim (root)",
                source: e,
            })?;

        for segment in &reclaim.segments {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO object_segment_reclaim_segments \
                     (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                      data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        segment.segment_index as i64,
                        &segment.segment_okh[..],
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id as i64,
                        segment.ec.k,
                        segment.ec.m,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object segments reclaim (segment)",
                    source: e,
                })?;
        }
        Ok(())
    }

    fn put_multipart_reclaim_in_open_txn(
        &self,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO multipart_reclaims \
                 (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    reclaim.bucket,
                    reclaim.key,
                    reclaim.generation_id.get() as i64,
                    reclaim.created_at as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put multipart reclaim (root)",
                source: e,
            })?;

        for part in &reclaim.parts {
            match part {
                MultipartReclaimPartRecord::ShardSet {
                    part_number,
                    part_okh,
                    part_vid,
                    data_pg_id,
                    ec,
                } => {
                    self.conn
                        .execute(
                            "INSERT OR REPLACE INTO multipart_reclaim_parts \
                             (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                              part_vid, data_pg_id, ec_k, ec_m) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                            params![
                                reclaim.bucket,
                                reclaim.key,
                                reclaim.generation_id.get() as i64,
                                *part_number as i64,
                                MultipartReclaimPartKind::ShardSet as u8,
                                &part_okh[..],
                                part_vid.get() as i64,
                                *data_pg_id as i64,
                                ec.k,
                                ec.m,
                            ],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put multipart reclaim (part shard set)",
                            source: e,
                        })?;
                }
                MultipartReclaimPartRecord::Segments {
                    part_number,
                    segments,
                } => {
                    self.conn
                        .execute(
                            "INSERT OR REPLACE INTO multipart_reclaim_parts \
                             (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                              part_vid, data_pg_id, ec_k, ec_m) \
                             VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL)",
                            params![
                                reclaim.bucket,
                                reclaim.key,
                                reclaim.generation_id.get() as i64,
                                *part_number as i64,
                                MultipartReclaimPartKind::Segments as u8,
                            ],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put multipart reclaim (part segments)",
                            source: e,
                        })?;

                    for segment in segments {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_part_segments \
                                 (bucket, key, generation_id, part_number, segment_index, \
                                  segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    segment.part_number as i64,
                                    segment.segment_index as i64,
                                    &segment.segment_okh[..],
                                    segment.segment_vid.get() as i64,
                                    segment.data_pg_id as i64,
                                    segment.ec.k,
                                    segment.ec.m,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part segment)",
                                source: e,
                            })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn put_object_with_segments_explicit_in_open_txn(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
        write_sequence: u64,
        last_modified: u64,
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put explicit segment object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put explicit segment object (non-segment layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put_object_with_segments requires Standard layout"),
                ),
            });
        }

        let data_layout = obj.layout.data_layout() as u8;
        let etag_kind = obj.etag.etag_kind() as u8;
        let status = ObjectState::Live as u8;
        let parts_count = obj.layout.parts_count().map(|n| n as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                context: "put explicit segment object (encode object lock)",
                source: e,
            })?;
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();
        self.mark_current_live_noncurrent(
            obj.bucket.as_str(),
            obj.key.as_str(),
            obj.version_id,
            last_modified,
        )
        .map_err(|e| MetadataError::Db {
            context: "put explicit segment object (mark noncurrent)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;
        self.advance_object_write_counter_in_open_txn(
            &obj.bucket,
            &obj.key,
            write_sequence,
            Some(obj.generation_id),
        )?;

        let obj_sql = if obj.version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        };
        self.execute_cached_metadata(
            obj_sql,
            params![
                obj.bucket,
                obj.key,
                obj.version_id.to_u64() as i64,
                write_sequence as i64,
                obj.generation_id.get() as i64,
                obj.size as i64,
                obj.etag.as_bytes().as_slice(),
                etag_kind,
                last_modified as i64,
                obj.ec.k,
                obj.ec.m,
                status,
                data_layout,
                parts_count,
                tags,
                metadata_blob,
                system_metadata_blob,
                encryption_type,
                encryption_state,
                obj.owner.principal,
                obj.owner.canonical_id.as_str(),
                obj.acl_grants.serialized(),
                i32::from(obj.public_read),
                object_lock_retention_mode,
                object_lock_retain_until,
                object_lock_legal_hold,
            ],
            "put explicit segment object (write object)",
        )?;

        self.execute_cached_metadata(
            "DELETE FROM object_segments \
             WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
            "put explicit segment object (delete prior segments)",
        )?;

        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO object_segments \
                 (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                  data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit segment object (prepare insert segments)",
                source: e,
            })?;
        for segment in segments {
            if segment.bucket != obj.bucket
                || segment.key != obj.key
                || segment.version_id != obj.version_id
            {
                return Err(MetadataError::Db {
                    context: "put explicit segment object (segment object mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("segment row does not match object identity"),
                    ),
                });
            }
            stmt.execute(params![
                segment.bucket,
                segment.key,
                segment.version_id.to_u64() as i64,
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
                context: "put explicit segment object (insert segment)",
                source: e,
            })?;
        }

        Ok(())
    }

    fn put_multipart_object_explicit_in_open_txn(
        &self,
        obj: &PutLiveObjectReq,
        parts: &[ObjectPartRecord],
        write_sequence: u64,
        last_modified: u64,
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put explicit multipart object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if !matches!(obj.layout, ObjectLayout::MultipartManifest { .. }) {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (non-multipart layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put multipart object requires MultipartManifest layout"),
                ),
            });
        }
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (empty parts)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("multipart commit requires at least one part"),
                ),
            });
        }

        let data_layout = obj.layout.data_layout() as u8;
        let etag_kind = obj.etag.etag_kind() as u8;
        let status = ObjectState::Live as u8;
        let parts_count = obj.layout.parts_count().map(|n| n as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (encode object lock)",
                source: e,
            })?;
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();
        self.mark_current_live_noncurrent(
            obj.bucket.as_str(),
            obj.key.as_str(),
            obj.version_id,
            last_modified,
        )
        .map_err(|e| MetadataError::Db {
            context: "put explicit multipart object (mark noncurrent)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;
        self.advance_object_write_counter_in_open_txn(
            &obj.bucket,
            &obj.key,
            write_sequence,
            Some(obj.generation_id),
        )?;

        let obj_sql = if obj.version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        };
        self.conn
            .execute(
                obj_sql,
                params![
                    &obj.bucket,
                    &obj.key,
                    obj.version_id.to_u64() as i64,
                    write_sequence as i64,
                    obj.generation_id.get() as i64,
                    obj.size as i64,
                    obj.etag.as_bytes(),
                    etag_kind,
                    last_modified as i64,
                    obj.ec.k,
                    obj.ec.m,
                    status,
                    data_layout,
                    parts_count,
                    tags,
                    metadata_blob,
                    system_metadata_blob,
                    encryption_type,
                    encryption_state,
                    &obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    obj.acl_grants.serialized(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (write object)",
                source: e,
            })?;

        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![&obj.bucket, &obj.key, obj.version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (delete prior parts)",
                source: e,
            })?;

        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (prepare insert parts)",
                source: e,
            })?;
        let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
        ordered_parts.sort_by_key(|part| part.part_number);
        let mut object_offset_start = 0u64;
        for part in ordered_parts {
            if part.bucket != obj.bucket || part.key != obj.key || part.version_id != obj.version_id
            {
                return Err(MetadataError::Db {
                    context: "put explicit multipart object (part identity mismatch)",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            stmt.execute(params![
                &part.bucket,
                &part.key,
                part.version_id.to_u64() as i64,
                part.part_number,
                object_offset_start as i64,
                part.size as i64,
                part.payload_crc64 as i64,
                &part.etag,
                part.etag_kind as u8,
                part.part_okh.as_slice(),
                part.part_vid.get() as i64,
                part.placement_cluster_epoch.get() as i64,
                part.ec_k,
                part.ec_m,
                part.data_pg_id,
                part.checksum.as_ref().map(|checksum| checksum.as_slice()),
            ])
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (insert part)",
                source: e,
            })?;
            object_offset_start += part.size;
        }

        Ok(())
    }

    fn delete_completed_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart upload",
                source: e,
            })
    }
}

#[allow(dead_code)]
fn bucket_write_reservation_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketWriteReservationRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let cluster_epoch_raw: i64 = row.get(3)?;
    let bucket_execution_generation_raw: i64 = row.get(4)?;
    let bucket_incarnation_generation_raw: i64 = row.get(5)?;
    let created_at_raw: i64 = row.get(7)?;
    let lease_deadline_raw: Option<i64> = row.get(8)?;
    Ok(BucketWriteReservationRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        reservation_id: row.get(1)?,
        owner_token: row.get(2)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        bucket_execution_generation: u64::try_from(bucket_execution_generation_raw).map_err(
            |_| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "invalid bucket_execution_generation: {bucket_execution_generation_raw}"
                    )),
                )
            },
        )?,
        bucket_incarnation_generation: u64::try_from(bucket_incarnation_generation_raw).map_err(
            |_| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "invalid bucket_incarnation_generation: {bucket_incarnation_generation_raw}"
                    )),
                )
            },
        )?,
        operation_kind: row.get(6)?,
        created_at: u64::try_from(created_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid created_at: {created_at_raw}")),
            )
        })?,
        lease_deadline: PgStore::parse_optional_u64(lease_deadline_raw, 8, "lease_deadline")?,
        target_context: row.get(9)?,
    })
}

#[allow(dead_code)]
fn bucket_write_drain_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketWriteDrainRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let cluster_epoch_raw: i64 = row.get(3)?;
    let bucket_execution_generation_raw: i64 = row.get(4)?;
    let state_raw: i64 = row.get(5)?;
    let created_at_raw: i64 = row.get(6)?;
    let lease_deadline_raw: Option<i64> = row.get(7)?;
    let state = match state_raw {
        0 => BucketWriteDrainState::Draining,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid bucket write drain state: {state_raw}")),
            ));
        }
    };
    Ok(BucketWriteDrainRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        drain_id: row.get(1)?,
        owner_token: row.get(2)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        bucket_execution_generation: u64::try_from(bucket_execution_generation_raw).map_err(
            |_| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "invalid bucket_execution_generation: {bucket_execution_generation_raw}"
                    )),
                )
            },
        )?,
        state,
        created_at: u64::try_from(created_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid created_at: {created_at_raw}")),
            )
        })?,
        lease_deadline: PgStore::parse_optional_u64(lease_deadline_raw, 7, "lease_deadline")?,
    })
}

fn bucket_delete_attempt_outcome_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketDeleteAttemptOutcomeRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let cluster_epoch_raw: i64 = row.get(2)?;
    let bucket_execution_generation_raw: i64 = row.get(3)?;
    let outcome_raw: i64 = row.get(4)?;
    let phase_raw: i64 = row.get(5)?;
    let post_reservation_next_object_pg_id_raw: Option<i64> = row.get(7)?;
    let updated_at_raw: i64 = row.get(8)?;
    let outcome = match outcome_raw {
        0 => BucketDeleteAttemptOutcomeKind::Retryable,
        1 => BucketDeleteAttemptOutcomeKind::NotEmpty,
        2 => BucketDeleteAttemptOutcomeKind::StaleGeneration,
        3 => BucketDeleteAttemptOutcomeKind::MarkDeleting,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid bucket delete attempt outcome: {outcome_raw}"
                )),
            ));
        }
    };
    let phase = match phase_raw {
        0 => BucketDeleteAttemptPhase::Initial,
        1 => BucketDeleteAttemptPhase::ReservationWait,
        2 => BucketDeleteAttemptPhase::PostReservationObjectDrain,
        3 => BucketDeleteAttemptPhase::StreamCleanup,
        4 => BucketDeleteAttemptPhase::FinalVisibilityCheck,
        5 => BucketDeleteAttemptPhase::MarkDeleting,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid bucket delete attempt phase: {phase_raw}")),
            ));
        }
    };
    Ok(BucketDeleteAttemptOutcomeRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        drain_id: row.get(1)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        bucket_execution_generation: u64::try_from(bucket_execution_generation_raw).map_err(
            |_| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "invalid bucket_execution_generation: {bucket_execution_generation_raw}"
                    )),
                )
            },
        )?,
        outcome,
        phase,
        detail: row.get(6)?,
        post_reservation_next_object_pg_id: post_reservation_next_object_pg_id_raw
            .map(|raw| {
                u32::try_from(raw).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        7,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid post_reservation_next_object_pg_id: {raw}")),
                    )
                })
            })
            .transpose()?,
        updated_at: u64::try_from(updated_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid updated_at: {updated_at_raw}")),
            )
        })?,
    })
}

fn object_payload_reclaim_claim_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<ObjectPayloadReclaimClaimRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let bucket_incarnation_raw: i64 = row.get(1)?;
    let key_raw: String = row.get(2)?;
    let generation_raw: i64 = row.get(3)?;
    let reclaim_kind_raw: u8 = row.get(4)?;
    let cluster_epoch_raw: i64 = row.get(7)?;
    let pg_id_raw: i64 = row.get(8)?;
    let claimed_at_raw: i64 = row.get(9)?;
    let lease_deadline_raw: Option<i64> = row.get(10)?;
    let attempt_count_raw: i64 = row.get(11)?;
    Ok(ObjectPayloadReclaimClaimRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        bucket_incarnation_generation: u64::try_from(bucket_incarnation_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid bucket_incarnation_generation: {bucket_incarnation_raw}"
                )),
            )
        })?,
        key: ObjectKey::try_from(key_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        generation_id: PgStore::parse_generation_id(generation_raw, 3, "generation_id")?,
        reclaim_kind: ObjectPayloadReclaimKind::from_u8(reclaim_kind_raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid object payload reclaim kind: {reclaim_kind_raw}"
                )),
            )
        })?,
        claim_id: row.get(5)?,
        owner_token: row.get(6)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        pg_id: u32::try_from(pg_id_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid pg_id: {pg_id_raw}")),
            )
        })?,
        claimed_at: u64::try_from(claimed_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid claimed_at: {claimed_at_raw}")),
            )
        })?,
        lease_deadline: PgStore::parse_optional_u64(lease_deadline_raw, 10, "lease_deadline")?,
        attempt_count: u64::try_from(attempt_count_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid attempt_count: {attempt_count_raw}")),
            )
        })?,
        last_error: row.get(12)?,
    })
}

fn bucket_delete_finalize_claim_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketDeleteFinalizeClaimRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let bucket_incarnation_raw: i64 = row.get(1)?;
    let cluster_epoch_raw: i64 = row.get(4)?;
    let pg_id_raw: i64 = row.get(5)?;
    let claimed_at_raw: i64 = row.get(6)?;
    let lease_deadline_raw: Option<i64> = row.get(7)?;
    let attempt_count_raw: i64 = row.get(8)?;
    Ok(BucketDeleteFinalizeClaimRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        bucket_incarnation_generation: u64::try_from(bucket_incarnation_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid bucket_incarnation_generation: {bucket_incarnation_raw}"
                )),
            )
        })?,
        claim_id: row.get(2)?,
        owner_token: row.get(3)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        pg_id: u32::try_from(pg_id_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid pg_id: {pg_id_raw}")),
            )
        })?,
        claimed_at: u64::try_from(claimed_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid claimed_at: {claimed_at_raw}")),
            )
        })?,
        lease_deadline: PgStore::parse_optional_u64(lease_deadline_raw, 7, "lease_deadline")?,
        attempt_count: u64::try_from(attempt_count_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid attempt_count: {attempt_count_raw}")),
            )
        })?,
        last_error: row.get(9)?,
    })
}

fn bucket_delete_finalize_root_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketDeleteFinalizeRoot, rusqlite::Error> {
    let incarnation = row.get::<_, i64>(1)?;
    let bucket_incarnation_generation = u64::try_from(incarnation).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Integer,
            Box::new(source),
        )
    })?;
    Ok(BucketDeleteFinalizeRoot {
        bucket: row.get(0)?,
        bucket_incarnation_generation,
    })
}

fn lifecycle_sweep_claim_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<LifecycleSweepClaimRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let bucket_incarnation_raw: i64 = row.get(1)?;
    let cluster_epoch_raw: i64 = row.get(4)?;
    let pg_id_raw: i64 = row.get(5)?;
    let claimed_at_raw: i64 = row.get(6)?;
    let heartbeat_at_raw: i64 = row.get(7)?;
    let lease_deadline_raw: Option<i64> = row.get(8)?;
    let attempt_count_raw: i64 = row.get(9)?;
    Ok(LifecycleSweepClaimRecord {
        bucket: BucketName::new(bucket_raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::from(error),
            )
        })?,
        bucket_incarnation_generation: u64::try_from(bucket_incarnation_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid bucket_incarnation_generation: {bucket_incarnation_raw}"
                )),
            )
        })?,
        claim_id: row.get(2)?,
        owner_token: row.get(3)?,
        cluster_epoch: u64::try_from(cluster_epoch_raw)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid cluster_epoch: {cluster_epoch_raw}")),
                )
            })?,
        pg_id: u32::try_from(pg_id_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid pg_id: {pg_id_raw}")),
            )
        })?,
        claimed_at: u64::try_from(claimed_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid claimed_at: {claimed_at_raw}")),
            )
        })?,
        heartbeat_at: u64::try_from(heartbeat_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid heartbeat_at: {heartbeat_at_raw}")),
            )
        })?,
        lease_deadline: PgStore::parse_optional_u64(lease_deadline_raw, 8, "lease_deadline")?,
        attempt_count: u64::try_from(attempt_count_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid attempt_count: {attempt_count_raw}")),
            )
        })?,
        last_error: row.get(10)?,
    })
}

fn lifecycle_sweep_root_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<LifecycleSweepRoot, rusqlite::Error> {
    let incarnation = row.get::<_, i64>(1)?;
    let bucket_incarnation_generation = u64::try_from(incarnation).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Integer,
            Box::new(source),
        )
    })?;
    let source_raw: i64 = row.get(2)?;
    let source = match source_raw {
        0 => LifecycleSweepRootSource::ExpiredClaim,
        1 => LifecycleSweepRootSource::BusyClaim,
        2 => LifecycleSweepRootSource::LifecycleConfig,
        3 => LifecycleSweepRootSource::AbortingMultipartUpload,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid lifecycle sweep root source: {source_raw}")),
            ));
        }
    };
    Ok(LifecycleSweepRoot {
        bucket: row.get(0)?,
        bucket_incarnation_generation,
        source,
    })
}

impl PgMetadataStore for PgStore {
    #[cfg(test)]
    fn create_bucket(
        &self,
        name: &BucketName,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        self.create_bucket_with_config(&CreateBucketConfig {
            name: name.as_str(),
            owner_principal,
            owner_canonical_id,
            acl_grants,
            public_read,
            public_write,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
    }

    fn delete_finalized_bucket(&self, name: &BucketName) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_finalized_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete finalized bucket (begin txn)",
                source: e,
            })?;
        let result = (|| -> Result<usize, MetadataError> {
            let state = self
                .conn
                .query_row(
                    "SELECT state FROM buckets WHERE name = ?1",
                    params![name.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (load state)",
                    source,
                })?;
            let Some(state) = state else {
                let _ = observability::event(
                    TRACE_TARGET,
                    "pg_delete_finalized_bucket_missing",
                    Some(format_args!("pg_id={} bucket={:?}", self.pg_id, name)),
                );
                return Ok(0);
            };
            let state = BucketState::from_u8(state).ok_or_else(|| MetadataError::Db {
                context: "delete finalized bucket (invalid bucket state)",
                source: rusqlite::Error::InvalidQuery,
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
                    "DELETE FROM buckets WHERE name = ?1 AND state = ?2",
                    params![name.as_str(), BucketState::Deleting as u8],
                )
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (delete row)",
                    source,
                })?;
            if deleted != 0 {
                self.conn
                    .execute(
                        "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                        params![name.as_str()],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "delete finalized bucket (delete completed MPU records)",
                        source,
                    })?;
                self.conn
                    .execute(
                        "DELETE FROM object_version_counters WHERE bucket = ?1",
                        params![name.as_str()],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "delete finalized bucket (delete version counters)",
                        source,
                    })?;
                self.conn
                    .execute(
                        "DELETE FROM object_write_counters WHERE bucket = ?1",
                        params![name.as_str()],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "delete finalized bucket (delete write counters)",
                        source,
                    })?;
                self.refresh_metadata_command_state_digest()
                    .map_err(|error| {
                        Self::store_error_as_metadata_db(
                            "delete finalized bucket (refresh metadata command digest)",
                            error,
                        )
                    })?;
            }
            Ok(deleted)
        })();
        let deleted = match result {
            Ok(deleted) => {
                #[cfg(test)]
                if self
                    .fail_next_delete_finalized_bucket_commit
                    .swap(false, Ordering::Relaxed)
                {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(MetadataError::Db {
                        context: "delete finalized bucket (commit txn)",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }

                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(MetadataError::Db {
                        context: "delete finalized bucket (commit txn)",
                        source,
                    });
                }
                deleted
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        if deleted == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        let _ = observability::event(
            TRACE_TARGET,
            "pg_delete_finalized_bucket_deleted",
            Some(format_args!(
                "pg_id={} bucket={:?} deleted={}",
                self.pg_id, name, deleted
            )),
        );
        Ok(())
    }

    fn head_bucket(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::head_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        let info = self.head_bucket_raw(name)?;
        if info.state != BucketState::Active {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(info)
    }

    fn head_bucket_raw(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        self.query_row_cached_optional_metadata(
            BUCKET_INFO_BY_NAME_SELECT,
            params![name.as_str()],
            "head bucket raw",
            Self::row_to_bucket_info,
        )?
        .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn head_bucket_record_raw(&self, name: &BucketName) -> Result<BucketRecord, MetadataError> {
        self.query_row_cached_optional_metadata(
            BUCKET_RECORD_BY_NAME_SELECT,
            params![name.as_str()],
            "head bucket record raw",
            Self::row_to_bucket_record,
        )?
        .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn list_buckets(&self, owner_canonical_id: &str) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets",
            "pg_id={} owner_canonical_id={}",
            self.pg_id,
            owner_canonical_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(&format!(
                "{BUCKET_INFO_SELECT} WHERE owner_canonical_id = ?1 AND state = ?2 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![owner_canonical_id, BucketState::Active as u8],
                Self::row_to_bucket_info,
            )
            .map_err(|e| MetadataError::Db {
                context: "list buckets query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_lifecycle",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(&format!(
                "{BUCKET_INFO_SELECT} \
                 JOIN bucket_subresources AS lifecycle \
                   ON lifecycle.bucket_name = buckets.name \
                  AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                  AND lifecycle.body IS NOT NULL \
                 WHERE state = ?1 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with lifecycle",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![BucketState::Active as u8], Self::row_to_bucket_info)
            .map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_aborting_multipart_uploads(
        &self,
    ) -> Result<Vec<BucketName>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_aborting_multipart_uploads",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT DISTINCT bucket FROM multipart_uploads \
                 WHERE state = ?1 ORDER BY bucket ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with aborting multipart uploads",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![UploadState::Aborting as u8], |row| row.get(0))
            .map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    #[cfg(test)]
    fn mark_bucket_deleting(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "mark bucket deleting (begin txn)",
            "mark bucket deleting (commit txn)",
            |store| {
                let generation = store.next_bucket_execution_generation_in_txn(
                    "mark bucket deleting (allocate execution generation)",
                )?;
                let updated = store
                    .conn
                    .execute(
                        "UPDATE buckets \
                         SET state = ?1, \
                             bucket_execution_generation = ?2 \
                         WHERE name = ?3 \
                           AND state = ?4",
                        params![
                            BucketState::Deleting as u8,
                            generation as i64,
                            name.as_str(),
                            BucketState::Active as u8
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "mark bucket deleting",
                        source,
                    })?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        let created_at = i64::try_from(created_at).map_err(|source| MetadataError::Db {
            context: "acquire durable bucket write reservation created_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire durable bucket write reservation lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let inserted = self
            .conn
            .execute(
                "INSERT INTO bucket_write_reservations \
             (bucket_name, reservation_id, owner_token, cluster_epoch, bucket_execution_generation, \
              bucket_incarnation_generation, operation_kind, created_at, lease_deadline, target_context) \
             SELECT name, ?1, ?2, ?3, bucket_execution_generation, bucket_incarnation_generation, ?4, ?5, ?6, ?7 \
             FROM buckets \
             WHERE name = ?8 AND state = ?9 \
               AND NOT EXISTS (\
                   SELECT 1 FROM bucket_write_drains \
                   WHERE bucket_name = buckets.name AND state = ?10\
               ) \
             ON CONFLICT(bucket_name, reservation_id) DO NOTHING",
                params![
                    reservation_id,
                    owner_token,
                    cluster_epoch.get(),
                    operation_kind,
                    created_at,
                    lease_deadline,
                    target_context,
                    name.as_str(),
                    BucketState::Active as u8,
                    BucketWriteDrainState::Draining as u8,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "acquire durable bucket write reservation",
                source,
            })?;

        if inserted == 0 {
            if let Some(existing) = self.durable_bucket_write_reservation(name, reservation_id)? {
                if existing.owner_token == owner_token
                    && existing.cluster_epoch == cluster_epoch
                    && existing.operation_kind == operation_kind
                    && existing.created_at == created_at as u64
                    && existing.lease_deadline == lease_deadline.map(|deadline| deadline as u64)
                    && existing.target_context.as_deref() == target_context
                {
                    return Ok(existing);
                }
                return Err(MetadataError::BucketWriteReservationConflict {
                    reservation_id: reservation_id.to_string(),
                });
            }
            let info = self.head_bucket_raw(name)?;
            if info.state == BucketState::Active && self.durable_bucket_write_drain(name)?.is_some()
            {
                return Err(MetadataError::BucketWriteDraining);
            }
            return Err(bucket_not_found(name.as_str()));
        }

        self.durable_bucket_write_reservation(name, reservation_id)?
            .ok_or_else(|| MetadataError::BucketWriteReservationNotFound {
                reservation_id: reservation_id.to_string(),
            })
    }

    fn durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
    ) -> Result<Option<BucketWriteReservationRecord>, MetadataError> {
        match self.conn.query_row(
            "SELECT bucket_name, reservation_id, owner_token, \
                    cluster_epoch, bucket_execution_generation, bucket_incarnation_generation, operation_kind, created_at, \
                    lease_deadline, target_context \
             FROM bucket_write_reservations \
             WHERE bucket_name = ?1 AND reservation_id = ?2",
            params![name.as_str(), reservation_id],
            bucket_write_reservation_from_row,
        ) {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(MetadataError::Db {
                context: "load durable bucket write reservation",
                source,
            }),
        }
    }

    fn durable_bucket_write_reservations(
        &self,
        name: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket_name, reservation_id, owner_token, \
                        cluster_epoch, bucket_execution_generation, bucket_incarnation_generation, operation_kind, created_at, \
                        lease_deadline, target_context \
                 FROM bucket_write_reservations \
                 WHERE bucket_name = ?1 \
                 ORDER BY reservation_id",
            )
            .map_err(|source| MetadataError::Db {
                context: "prepare list durable bucket write reservations",
                source,
            })?;
        let rows = stmt
            .query_map(params![name.as_str()], bucket_write_reservation_from_row)
            .map_err(|source| MetadataError::Db {
                context: "list durable bucket write reservations",
                source,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| MetadataError::Db {
                context: "collect durable bucket write reservations",
                source,
            })
    }

    fn heartbeat_durable_bucket_write_reservation(
        &self,
        heartbeat: DurableBucketWriteReservationHeartbeat<'_>,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        let generation =
            i64::try_from(heartbeat.bucket_execution_generation).map_err(|source| {
                MetadataError::Db {
                    context: "heartbeat durable bucket write reservation generation",
                    source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                }
            })?;
        let incarnation =
            i64::try_from(heartbeat.bucket_incarnation_generation).map_err(|source| {
                MetadataError::Db {
                    context: "heartbeat durable bucket write reservation incarnation",
                    source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                }
            })?;
        let lease_deadline =
            i64::try_from(heartbeat.lease_deadline).map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write reservation lease deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let updated = self
            .conn
            .execute(
                "UPDATE bucket_write_reservations \
                 SET lease_deadline = ?7 \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6",
                params![
                    heartbeat.name.as_str(),
                    heartbeat.reservation_id,
                    heartbeat.owner_token,
                    heartbeat.cluster_epoch.get(),
                    generation,
                    incarnation,
                    lease_deadline,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write reservation",
                source,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketWriteReservationNotFound {
                reservation_id: heartbeat.reservation_id.to_string(),
            });
        }
        self.durable_bucket_write_reservation(heartbeat.name, heartbeat.reservation_id)?
            .ok_or_else(|| MetadataError::BucketWriteReservationNotFound {
                reservation_id: heartbeat.reservation_id.to_string(),
            })
    }

    fn release_durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) -> Result<(), MetadataError> {
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation generation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let incarnation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let deleted = self
            .conn
            .execute(
                "DELETE FROM bucket_write_reservations \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6",
                params![
                    name.as_str(),
                    reservation_id,
                    owner_token,
                    cluster_epoch.get(),
                    generation,
                    incarnation,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation",
                source,
            })?;
        if deleted == 0 {
            return Err(MetadataError::BucketWriteReservationNotFound {
                reservation_id: reservation_id.to_string(),
            });
        }
        Ok(())
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) -> Result<(), MetadataError> {
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "release metadata command bucket write reservation generation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let incarnation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release metadata command bucket write reservation incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "begin release metadata command bucket write reservation",
                source,
            })?;

        let result = (|| {
            let existing = self.durable_bucket_write_reservation(name, reservation_id)?;
            match existing {
                Some(record)
                    if record.owner_token == owner_token
                        && record.cluster_epoch == cluster_epoch
                        && record.bucket_execution_generation == bucket_execution_generation
                        && record.bucket_incarnation_generation
                            == bucket_incarnation_generation =>
                {
                    let deleted = self
                        .conn
                        .execute(
                            "DELETE FROM bucket_write_reservations \
                             WHERE bucket_name = ?1 AND reservation_id = ?2 \
                               AND owner_token = ?3 AND cluster_epoch = ?4 \
                               AND bucket_execution_generation = ?5 \
                               AND bucket_incarnation_generation = ?6",
                            params![
                                name.as_str(),
                                reservation_id,
                                owner_token,
                                cluster_epoch.get(),
                                generation,
                                incarnation,
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "release metadata command durable bucket write reservation",
                            source,
                        })?;
                    if deleted != 1 {
                        return Err(MetadataError::BucketWriteReservationConflict {
                            reservation_id: reservation_id.to_string(),
                        });
                    }
                }
                Some(_) => {
                    return Err(MetadataError::BucketWriteReservationConflict {
                        reservation_id: reservation_id.to_string(),
                    });
                }
                None => {
                    return Ok(());
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => self.conn.execute_batch("COMMIT").map_err(|source| {
                let _ = self.conn.execute_batch("ROLLBACK");
                MetadataError::Db {
                    context: "commit release metadata command bucket write reservation",
                    source,
                }
            }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn begin_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, MetadataError> {
        let created_at = i64::try_from(created_at).map_err(|source| MetadataError::Db {
            context: "begin durable bucket write drain created_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "begin durable bucket write drain lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let inserted = self
            .conn
            .execute(
                "INSERT INTO bucket_write_drains \
                 (bucket_name, drain_id, owner_token, cluster_epoch, bucket_execution_generation, \
                  state, created_at, lease_deadline) \
                 SELECT name, ?1, ?2, ?3, bucket_execution_generation, ?4, ?5, ?6 \
                 FROM buckets \
                 WHERE name = ?7 AND state = ?8 \
                 ON CONFLICT(bucket_name) DO NOTHING",
                params![
                    drain_id,
                    owner_token,
                    cluster_epoch.get(),
                    BucketWriteDrainState::Draining as u8,
                    created_at,
                    lease_deadline,
                    name.as_str(),
                    BucketState::Active as u8,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "begin durable bucket write drain",
                source,
            })?;
        if inserted == 0 {
            if let Some(existing) = self.durable_bucket_write_drain(name)? {
                if existing.drain_id == drain_id
                    && existing.owner_token == owner_token
                    && existing.cluster_epoch == cluster_epoch
                    && existing.created_at == created_at as u64
                    && existing.lease_deadline == lease_deadline.map(|deadline| deadline as u64)
                {
                    return Ok(existing);
                }
                return Err(MetadataError::BucketWriteDrainConflict {
                    drain_id: drain_id.to_string(),
                });
            }
            return Err(bucket_not_found(name.as_str()));
        }
        self.durable_bucket_write_drain(name)?.ok_or_else(|| {
            MetadataError::BucketWriteDrainNotFound {
                drain_id: drain_id.to_string(),
            }
        })
    }

    fn durable_bucket_write_drain(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketWriteDrainRecord>, MetadataError> {
        match self.conn.query_row(
            "SELECT bucket_name, drain_id, owner_token, cluster_epoch, bucket_execution_generation, \
                    state, created_at, lease_deadline \
             FROM bucket_write_drains \
             WHERE bucket_name = ?1",
            params![name.as_str()],
            bucket_write_drain_from_row,
        ) {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(MetadataError::Db {
                context: "load durable bucket write drain",
                source,
            }),
        }
    }

    fn clear_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
    ) -> Result<(), MetadataError> {
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "clear durable bucket write drain generation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let deleted = self
            .conn
            .execute(
                "DELETE FROM bucket_write_drains \
                 WHERE bucket_name = ?1 AND drain_id = ?2 AND owner_token = ?3 \
                   AND cluster_epoch = ?4 AND bucket_execution_generation = ?5",
                params![
                    name.as_str(),
                    drain_id,
                    owner_token,
                    cluster_epoch.get(),
                    generation
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "clear durable bucket write drain",
                source,
            })?;
        if deleted == 0 {
            return Err(MetadataError::BucketWriteDrainNotFound {
                drain_id: drain_id.to_string(),
            });
        }
        Ok(())
    }

    fn clear_expired_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, MetadataError> {
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "clear expired durable bucket write drain now",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "clear expired durable bucket write drain (begin txn)",
                source,
            })?;
        let result = (|| {
            let Some(record) = (match self.conn.query_row(
                "SELECT bucket_name, drain_id, owner_token, cluster_epoch, bucket_execution_generation, \
                        state, created_at, lease_deadline \
                 FROM bucket_write_drains \
                 WHERE bucket_name = ?1",
                params![name.as_str()],
                bucket_write_drain_from_row,
            ) {
                Ok(record) => Some(record),
                Err(rusqlite::Error::QueryReturnedNoRows) => None,
                Err(source) => {
                    return Err(MetadataError::Db {
                        context: "clear expired durable bucket write drain (load drain)",
                        source,
                    });
                }
            }) else {
                return Ok(None);
            };
            let Some(lease_deadline) = record.lease_deadline else {
                return Ok(None);
            };
            let lease_deadline =
                i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
                    context: "clear expired durable bucket write drain lease deadline",
                    source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                })?;
            if lease_deadline > now {
                return Ok(None);
            }
            let bucket = self.head_bucket_record_raw(name)?;
            if bucket.state != BucketState::Active
                || bucket.bucket_execution_generation != record.bucket_execution_generation
            {
                return Ok(None);
            }
            let deleted = self
                .conn
                .execute(
                    "DELETE FROM bucket_write_drains \
                     WHERE bucket_name = ?1 AND drain_id = ?2 AND owner_token = ?3 \
                       AND cluster_epoch = ?4 AND bucket_execution_generation = ?5 \
                       AND lease_deadline IS NOT NULL AND lease_deadline <= ?6",
                    params![
                        name.as_str(),
                        &record.drain_id,
                        &record.owner_token,
                        record.cluster_epoch.get(),
                        i64::try_from(record.bucket_execution_generation).map_err(|source| {
                            MetadataError::Db {
                                context: "clear expired durable bucket write drain generation",
                                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                            }
                        })?,
                        now,
                    ],
                )
                .map_err(|source| MetadataError::Db {
                    context: "clear expired durable bucket write drain (delete drain)",
                    source,
                })?;
            if deleted == 0 {
                return Ok(None);
            }
            Ok(Some(record))
        })();
        match result {
            Ok(record) => self
                .conn
                .execute_batch("COMMIT")
                .map(|()| record)
                .map_err(|source| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    MetadataError::Db {
                        context: "clear expired durable bucket write drain (commit txn)",
                        source,
                    }
                }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn heartbeat_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
        lease_deadline: u64,
        now: u64,
    ) -> Result<BucketWriteDrainRecord, MetadataError> {
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write drain generation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let lease_deadline = i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
            context: "heartbeat durable bucket write drain lease deadline",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "heartbeat durable bucket write drain now",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write drain (begin txn)",
                source,
            })?;
        let result = (|| {
            let Some(record) = (match self.conn.query_row(
                "SELECT bucket_name, drain_id, owner_token, cluster_epoch, bucket_execution_generation, \
                        state, created_at, lease_deadline \
                 FROM bucket_write_drains \
                 WHERE bucket_name = ?1",
                params![name.as_str()],
                bucket_write_drain_from_row,
            ) {
                Ok(record) => Some(record),
                Err(rusqlite::Error::QueryReturnedNoRows) => None,
                Err(source) => {
                    return Err(MetadataError::Db {
                        context: "heartbeat durable bucket write drain (load drain)",
                        source,
                    });
                }
            }) else {
                return Err(MetadataError::BucketWriteDrainNotFound {
                    drain_id: drain_id.to_string(),
                });
            };
            if record.drain_id != drain_id
                || record.owner_token != owner_token
                || record.cluster_epoch != cluster_epoch
                || record.bucket_execution_generation != bucket_execution_generation
            {
                return Err(MetadataError::BucketWriteDrainConflict {
                    drain_id: drain_id.to_string(),
                });
            }
            let Some(current_deadline) = record.lease_deadline else {
                return Err(MetadataError::BucketWriteDrainConflict {
                    drain_id: drain_id.to_string(),
                });
            };
            if i64::try_from(current_deadline).map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write drain current lease deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })? <= now
            {
                return Err(MetadataError::BucketWriteDrainConflict {
                    drain_id: drain_id.to_string(),
                });
            }
            let bucket = self.head_bucket_record_raw(name)?;
            if bucket.state != BucketState::Active
                || bucket.bucket_execution_generation != bucket_execution_generation
            {
                return Err(MetadataError::BucketWriteDrainConflict {
                    drain_id: drain_id.to_string(),
                });
            }
            let updated = self
                .conn
                .execute(
                    "UPDATE bucket_write_drains \
                     SET lease_deadline = ?6 \
                     WHERE bucket_name = ?1 AND drain_id = ?2 AND owner_token = ?3 \
                       AND cluster_epoch = ?4 AND bucket_execution_generation = ?5 \
                       AND lease_deadline IS NOT NULL AND lease_deadline > ?7",
                    params![
                        name.as_str(),
                        drain_id,
                        owner_token,
                        cluster_epoch.get(),
                        generation,
                        lease_deadline,
                        now,
                    ],
                )
                .map_err(|source| MetadataError::Db {
                    context: "heartbeat durable bucket write drain (update drain)",
                    source,
                })?;
            if updated == 0 {
                return Err(MetadataError::BucketWriteDrainNotFound {
                    drain_id: drain_id.to_string(),
                });
            }
            self.durable_bucket_write_drain(name)?.ok_or_else(|| {
                MetadataError::BucketWriteDrainNotFound {
                    drain_id: drain_id.to_string(),
                }
            })
        })();
        match result {
            Ok(record) => self
                .conn
                .execute_batch("COMMIT")
                .map(|()| record)
                .map_err(|source| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    MetadataError::Db {
                        context: "heartbeat durable bucket write drain (commit txn)",
                        source,
                    }
                }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn record_bucket_delete_attempt_outcome(
        &self,
        record: &BucketDeleteAttemptOutcomeRecord,
    ) -> Result<(), MetadataError> {
        if record.detail.len() > BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN {
            return Err(MetadataError::Db {
                context: "record bucket delete attempt outcome detail length",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                    "bucket delete attempt outcome detail exceeds maximum length",
                )),
            });
        }
        let generation = i64::try_from(record.bucket_execution_generation).map_err(|source| {
            MetadataError::Db {
                context: "record bucket delete attempt outcome generation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            }
        })?;
        let updated_at = i64::try_from(record.updated_at).map_err(|source| MetadataError::Db {
            context: "record bucket delete attempt outcome updated_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let post_reservation_next_object_pg_id =
            record.post_reservation_next_object_pg_id.map(i64::from);
        self.conn
            .execute(
                "INSERT INTO bucket_delete_attempt_outcomes \
                 (bucket_name, drain_id, cluster_epoch, bucket_execution_generation, \
                  outcome, phase, detail, post_reservation_next_object_pg_id, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(bucket_name) DO UPDATE SET \
                   drain_id = excluded.drain_id, \
                   cluster_epoch = excluded.cluster_epoch, \
                   bucket_execution_generation = excluded.bucket_execution_generation, \
                   outcome = excluded.outcome, \
                   phase = excluded.phase, \
                   detail = excluded.detail, \
                   post_reservation_next_object_pg_id = excluded.post_reservation_next_object_pg_id, \
                   updated_at = excluded.updated_at",
                params![
                    record.bucket.as_str(),
                    &record.drain_id,
                    record.cluster_epoch.get(),
                    generation,
                    record.outcome as u8,
                    record.phase as u8,
                    &record.detail,
                    post_reservation_next_object_pg_id,
                    updated_at,
                ],
            )
            .map(|_| ())
            .map_err(|source| MetadataError::Db {
                context: "record bucket delete attempt outcome",
                source,
            })
    }

    fn bucket_delete_attempt_outcome(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, MetadataError> {
        match self.conn.query_row(
            "SELECT bucket_name, drain_id, cluster_epoch, bucket_execution_generation, \
                    outcome, phase, detail, post_reservation_next_object_pg_id, updated_at \
             FROM bucket_delete_attempt_outcomes \
             WHERE bucket_name = ?1",
            params![name.as_str()],
            bucket_delete_attempt_outcome_from_row,
        ) {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(MetadataError::Db {
                context: "load bucket delete attempt outcome",
                source,
            }),
        }
    }

    fn get_bucket_delete_begin_roots(
        &self,
        now: u64,
        start_after_bucket: Option<&BucketName>,
        limit: usize,
    ) -> Result<Vec<crate::BucketDeleteBeginRoot>, MetadataError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "get bucket delete begin roots now",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get bucket delete begin roots limit",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT d.bucket_name, d.bucket_execution_generation, \
                        b.bucket_incarnation_generation \
                 FROM bucket_write_drains d \
                 JOIN buckets b \
                   ON b.name = d.bucket_name \
                  AND b.bucket_execution_generation = d.bucket_execution_generation \
                  AND b.state = ?1 \
                 WHERE d.lease_deadline IS NOT NULL \
                   AND d.lease_deadline <= ?2 \
                   AND (?3 IS NULL OR d.bucket_name > ?3) \
                 ORDER BY d.bucket_name ASC \
                 LIMIT ?4",
            )
            .map_err(|source| MetadataError::Db {
                context: "prepare get bucket delete begin roots",
                source,
            })?;
        let rows = stmt
            .query_map(
                params![
                    BucketState::Active as u8,
                    now,
                    start_after_bucket.map(BucketName::as_str),
                    limit_i64
                ],
                |row| {
                    let bucket_execution_generation_raw = row.get::<_, i64>(1)?;
                    let bucket_incarnation_generation_raw = row.get::<_, i64>(2)?;
                    Ok(crate::BucketDeleteBeginRoot {
                        bucket: row.get(0)?,
                        bucket_execution_generation: u64::try_from(
                            bucket_execution_generation_raw,
                        )
                        .map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Integer,
                                Box::from(format!(
                                    "invalid bucket_execution_generation: {bucket_execution_generation_raw}"
                                )),
                            )
                        })?,
                        bucket_incarnation_generation: u64::try_from(
                            bucket_incarnation_generation_raw,
                        )
                        .map_err(|_| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Integer,
                                Box::from(format!(
                                    "invalid bucket_incarnation_generation: {bucket_incarnation_generation_raw}"
                                )),
                            )
                        })?,
                    })
                },
            )
            .map_err(|source| MetadataError::Db {
                context: "query get bucket delete begin roots",
                source,
            })?;
        let mut roots = Vec::new();
        for row in rows {
            roots.push(row.map_err(|source| MetadataError::Db {
                context: "row get bucket delete begin roots",
                source,
            })?);
        }
        Ok(roots)
    }

    #[cfg(test)]
    fn put_bucket_versioning(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError> {
        self.put_bucket_versioning_inner(name, state, BucketExecutionGeneration::Allocate)
    }

    #[cfg(test)]
    fn put_bucket_object_lock(
        &self,
        name: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::ObjectLock(config),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_acl(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        self.put_bucket_acl_inner(
            name,
            acl_grants,
            public_read,
            public_write,
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_subresource(
        &self,
        name: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_internal(name, req.kind, req.body, req.aux)
    }

    fn get_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError> {
        self.get_bucket_subresource_internal(name.as_str(), kind)
    }

    #[cfg(test)]
    fn delete_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError> {
        self.delete_bucket_subresource_internal(name, kind)
    }

    #[cfg(test)]
    fn put_bucket_public_access_block(
        &self,
        name: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::PublicAccessBlock(Some(config)),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_public_access_block(
        &self,
        name: &BucketName,
    ) -> Result<Option<PublicAccessBlockConfig>, MetadataError> {
        self.conn
            .query_row(
                "SELECT \
                     public_access_block_present, \
                     public_access_block_block_public_acls, \
                     public_access_block_ignore_public_acls, \
                     public_access_block_block_public_policy, \
                     public_access_block_restrict_public_buckets \
                 FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Self::parse_public_access_block(
                        (
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ),
                        [0, 1, 2, 3, 4],
                    )
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket public access block",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn delete_bucket_public_access_block(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::PublicAccessBlock(None),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_ownership_controls(
        &self,
        name: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::OwnershipControls(Some(config)),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_ownership_controls(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketOwnershipControls>, MetadataError> {
        self.conn
            .query_row(
                "SELECT ownership_controls_mode FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| Self::parse_ownership_controls(row.get(0)?, 0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket ownership controls",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn delete_bucket_ownership_controls(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::OwnershipControls(None),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_abac_enabled(
        &self,
        name: &BucketName,
        enabled: bool,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::AbacEnabled(enabled),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_abac_enabled(&self, name: &BucketName) -> Result<bool, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket_abac_enabled FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| Ok(row.get::<_, i64>(0)? != 0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => bucket_not_found(name.as_str()),
                source => MetadataError::Db {
                    context: "get bucket abac enabled",
                    source,
                },
            })
    }

    #[cfg(test)]
    fn put_bucket_encryption(
        &self,
        name: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::Encryption(config),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_encryption(
        &self,
        name: &BucketName,
    ) -> Result<BucketEncryptionConfig, MetadataError> {
        self.conn
            .query_row(
                "SELECT default_encryption_type, sse_c_blocked FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Ok(BucketEncryptionConfig {
                        default_encryption: row
                            .get::<_, Option<u8>>(0)?
                            .map(|value| {
                                ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!(
                                            "invalid default_encryption_type: {value}"
                                        )),
                                    )
                                })
                            })
                            .transpose()?,
                        sse_c_blocked: row.get::<_, i64>(1)? != 0,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket encryption",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::put_object_meta",
            "pg_id={}",
            self.pg_id
        );
        let now = PgStore::now_millis();
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object meta (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| match req {
            PutObjectReq::Live(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                req.validate().map_err(|msg| MetadataError::Db {
                    context: "put object meta (etag/layout mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from(msg),
                    ),
                })?;
                self.mark_current_live_noncurrent(
                    req.bucket.as_str(),
                    req.key.as_str(),
                    req.version_id,
                    now,
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta (mark noncurrent)",
                    source: e,
                })?;
                let data_layout_u8 = req.layout.data_layout() as u8;
                let etag_kind_u8 = req.etag.etag_kind() as u8;
                let status_u8 = ObjectState::Live as u8;
                let parts_count = req.layout.parts_count().map(|n| n as i64);
                let tags = req.tags.as_ref().map(SerializedTagSet::as_str);
                let metadata_blob = req
                    .metadata_blob
                    .as_ref()
                    .map(SerializedMetadataBlob::as_slice);
                let system_metadata_blob = req
                    .system_metadata_blob
                    .as_ref()
                    .map(SerializedSystemMetadataBlob::as_slice);
                let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                    Self::object_lock_sql_values(req.object_lock).map_err(|e| {
                        MetadataError::Db {
                            context: "put object meta (encode object lock)",
                            source: e,
                        }
                    })?;
                let encryption_type = req.encryption.encryption_type() as u8;
                let encryption_state = req.encryption.encode_state();
                self.advance_object_version_counter_in_open_txn(
                    &req.bucket,
                    &req.key,
                    req.version_id,
                )?;
                self.advance_object_write_counter_in_open_txn(
                    &req.bucket,
                    &req.key,
                    write_sequence,
                    Some(req.generation_id),
                )?;
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                };
                self.execute_cached_metadata(
                    sql,
                    params![
                        req.bucket,
                        req.key,
                        req.version_id.to_u64() as i64,
                        write_sequence as i64,
                        req.generation_id.get() as i64,
                        req.size as i64,
                        req.etag.as_bytes().as_slice(),
                        etag_kind_u8,
                        now as i64,
                        req.ec.k,
                        req.ec.m,
                        status_u8,
                        data_layout_u8,
                        parts_count,
                        tags,
                        metadata_blob,
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        req.owner.principal,
                        req.owner.canonical_id.as_str(),
                        req.acl_grants.serialized(),
                        i32::from(req.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                    "put object meta",
                )?;
                Ok(())
            }
            PutObjectReq::DeleteMarker(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                self.put_delete_marker_explicit_in_open_txn(
                    &req.bucket,
                    &req.key,
                    req.version_id,
                    &req.owner,
                    write_sequence,
                    now,
                )
            }
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object meta (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects WHERE bucket = ?1 AND key = ?2 \
             ORDER BY write_sequence DESC LIMIT 1",
            params![bucket, key],
            "get object meta",
            Self::row_to_object_record,
        )?
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id.to_u64() as i64],
            "get object version",
            Self::row_to_object_record,
        )?
            .ok_or(MetadataError::ObjectNotFound)
    }

    #[cfg(test)]
    fn put_object_acl(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET acl_grants = ?1, public_read = ?2 \
             WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
            params![
                acl_grants.serialized(),
                i32::from(public_read),
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object acl",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object acl (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_object_retention(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        retention: ObjectRetention,
    ) -> Result<(), MetadataError> {
        let retain_until =
            i64::try_from(retention.retain_until_unix_seconds).map_err(|_| MetadataError::Db {
                context: "put object retention (encode retain-until)",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(format!(
                    "object lock retain-until exceeds SQLite INTEGER: {}",
                    retention.retain_until_unix_seconds
                ))),
            })?;
        let updated = self.execute_cached_metadata(
            "UPDATE objects \
             SET object_lock_retention_mode = ?1, object_lock_retain_until = ?2 \
             WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
            params![
                retention.mode as u8,
                retain_until,
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object retention",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object retention (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_object_legal_hold(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        legal_hold: StoredLegalHoldStatus,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET object_lock_legal_hold = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = ?5",
            params![
                legal_hold as u8,
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object legal hold",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object legal hold (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn delete_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.conn
            .execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object meta",
                source: e,
            })?;
        Ok(())
    }

    #[cfg(test)]
    fn delete_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete object version (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> =
            self.delete_object_version_in_open_txn(bucket, key, version_id);

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "delete object version (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_objects",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        // Select the current record per key using the durable per-key write
        // sequence. Timestamp equality cannot reliably determine currentness
        // for suspended buckets because a newer null version may share the
        // same last_modified millisecond as an older numbered version.
        let limit = req.max_keys as i64 + 1;

        // Build WHERE clause fragments for key filtering
        let mut where_clauses = vec!["o.bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref start_after) = req.start_after {
            where_clauses.push(format!("o.key > ?{param_idx}"));
            params_vec.push(Box::new(start_after.clone()));
            param_idx += 1;
        } else if let Some(ref start_at) = req.start_at {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("o.key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT o.bucket, o.key, o.version_id, o.generation_id, o.size, o.etag, o.etag_kind, \
                   o.last_modified, o.storage_class, o.ec_k, o.ec_m, o.status, o.tags, \
                   o.data_layout, o.parts_count, o.metadata_blob, o.system_metadata_blob, \
                   o.encryption_type, o.encryption_state, o.owner_principal, o.owner_canonical_id, \
                   o.acl_grants, o.public_read, o.object_lock_retention_mode, \
                   o.object_lock_retain_until, o.object_lock_legal_hold, \
                   o.became_noncurrent_at \
            FROM objects o \
            WHERE {where_str} AND o.status = 0 \
              AND NOT EXISTS ( \
                  SELECT 1 FROM objects newer \
                  WHERE newer.bucket = o.bucket AND newer.key = o.key \
                    AND newer.write_sequence > o.write_sequence \
              ) \
            ORDER BY o.key ASC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list objects",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list objects query",
                source: e,
            })?;

        let mut objects: Vec<StoredObject> = Vec::new();
        for row in rows {
            objects.push(row.map_err(|e| MetadataError::Db {
                context: "list objects row",
                source: e,
            })?);
        }

        let is_truncated = objects.len() as i64 > req.max_keys as i64;
        if is_truncated {
            objects.truncate(req.max_keys as usize);
        }

        let next_start_after = if is_truncated {
            objects.last().map(|o| o.key().clone())
        } else {
            None
        };

        Ok(ListObjectsResp {
            objects,
            is_truncated,
            next_start_after,
        })
    }

    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_object_versions",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        let limit = req.max_keys as i64 + 1;

        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref start_at) = req.start_at {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        } else if let Some(ref key_marker) = req.key_marker {
            if let Some(vid_marker) = req.version_id_marker {
                if let Some(write_sequence) = self.object_write_sequence(
                    req.bucket.as_str(),
                    key_marker.as_str(),
                    vid_marker,
                )? {
                    where_clauses.push(format!(
                        "(key > ?{} OR (key = ?{} AND write_sequence < ?{}))",
                        param_idx,
                        param_idx,
                        param_idx + 1
                    ));
                    params_vec.push(Box::new(key_marker.clone()));
                    params_vec.push(Box::new(write_sequence as i64));
                    param_idx += 2;
                } else {
                    where_clauses.push(format!("key > ?{param_idx}"));
                    params_vec.push(Box::new(key_marker.clone()));
                    param_idx += 1;
                }
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects \
             WHERE {where_str} \
             ORDER BY key ASC, write_sequence DESC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list object versions",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions query",
                source: e,
            })?;

        let mut versions: Vec<StoredObject> = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions row",
                source: e,
            })?);
        }

        let is_truncated = versions.len() as i64 > req.max_keys as i64;
        if is_truncated {
            versions.truncate(req.max_keys as usize);
        }

        let (next_key_marker, next_version_id_marker) = if is_truncated {
            versions.last().map_or((None, None), |o| {
                (Some(o.key().clone()), Some(o.version_id()))
            })
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResp {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    fn list_object_versions_for_key(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, became_noncurrent_at \
                 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY write_sequence DESC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list object versions for key",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key], Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions for key query",
                source: e,
            })?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions for key row",
                source: e,
            })?);
        }
        Ok(versions)
    }

    fn next_version_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, MetadataError> {
        let (max_existing, stored_next): (Option<i64>, Option<i64>) = self
            .query_row_cached_metadata(
                "SELECT \
                    (SELECT MAX(version_id) FROM objects WHERE bucket = ?1 AND key = ?2), \
                    (SELECT next_version_id FROM object_version_counters \
                         WHERE bucket = ?1 AND key = ?2)",
                params![bucket, key],
                "next version id",
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
        let next_from_rows = match max_existing {
            None => 1,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative version_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(version_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "version_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(version_id) overflow"),
                    ),
                })?
            }
        };

        let next_from_counter = match stored_next {
            None => 1,
            Some(v) => u64::try_from(v).map_err(|_| MetadataError::Db {
                context: "negative next_version_id in database",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("negative next_version_id: {v}")),
                ),
            })?,
        };
        let next = next_from_rows.max(next_from_counter);
        next.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "version_id overflow",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("next_version_id overflow"),
            ),
        })?;
        Ok(VersionId::from_u64(next))
    }

    fn next_generation_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, MetadataError> {
        let max: Option<i64> = self
            .query_row_cached_optional_metadata(
                "SELECT MAX(generation_id) FROM (
                     SELECT generation_id FROM objects WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM object_segments_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM multipart_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT object_generation_id FROM multipart_uploads WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM object_generation_reservations WHERE bucket = ?1 AND key = ?2
                 )",
                params![bucket, key],
                "next generation id",
                |row| row.get(0),
            )?
            .flatten();

        let next = match max {
            None => 1u64,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative generation_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(generation_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "generation_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(generation_id) overflow"),
                    ),
                })?
            }
        };
        GenerationId::new(next).ok_or_else(|| MetadataError::Db {
            context: "invalid next generation id",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("next generation id must be nonzero"),
            ),
        })
    }

    #[cfg(test)]
    fn reserve_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "reserve object generation (begin txn)",
                source: e,
            })?;

        let result: Result<GenerationId, MetadataError> = (|| {
            let generation_id = self.next_generation_id(bucket, key)?;
            self.execute_cached_metadata(
                "INSERT INTO object_generation_reservations \
                 (reservation_id, bucket, key, generation_id, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    reservation_id.as_str(),
                    bucket,
                    key,
                    generation_id.get() as i64,
                    PgStore::now_millis() as i64,
                ],
                "reserve object generation (insert reservation)",
            )?;
            Ok(generation_id)
        })();

        match result {
            Ok(generation_id) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "reserve object generation (commit txn)",
                        source: e,
                    });
                }
                Ok(generation_id)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn get_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        let raw: i64 = self
            .query_row_cached_optional_metadata(
                "SELECT generation_id FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                params![reservation_id.as_str(), bucket, key],
                "get object generation reservation",
                |row| row.get(0),
            )?
            .ok_or_else(|| MetadataError::ObjectGenerationReservationNotFound {
                reservation_id: reservation_id.as_str().to_owned(),
            })?;
        Self::parse_generation_id(raw, 0, "generation_id").map_err(|source| MetadataError::Db {
            context: "parse object generation reservation",
            source,
        })
    }

    #[cfg(test)]
    fn delete_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_generation_reservation_direct(bucket, key, reservation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_segments_reclaim(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object segments reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO object_segments_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object segments reclaim (root)",
                    source: e,
                })?;

            for segment in &reclaim.segments {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO object_segment_reclaim_segments \
                         (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                          data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            reclaim.bucket,
                            reclaim.key,
                            reclaim.generation_id.get() as i64,
                            segment.segment_index as i64,
                            &segment.segment_okh[..],
                            segment.segment_vid.get() as i64,
                            segment.data_pg_id as i64,
                            segment.ec.k,
                            segment.ec.m,
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object segments reclaim (segment)",
                        source: e,
                    })?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object segments reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                 FROM object_segment_reclaim_segments \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (prepare segments)",
                source: e,
            })?;

        let segments = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok(ObjectSegmentsReclaimSegmentRecord {
                    segment_index: row.get::<_, i64>(0)? as u32,
                    segment_okh: row.get_ref(1)?.as_blob()?.try_into().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Blob,
                            Box::from("segment_okh must be 16 bytes"),
                        )
                    })?,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get::<_, i64>(3)? as u32,
                    ec: EcShape {
                        k: row.get(4)?,
                        m: row.get(5)?,
                    },
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (query segments)",
                source: e,
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (collect segments)",
                source: e,
            })?;

        Ok(Some(ObjectSegmentsReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            segments,
        }))
    }

    #[cfg(test)]
    fn delete_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.delete_object_segments_reclaim_direct(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_multipart_reclaim(&self, reclaim: &MultipartReclaimRecord) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put multipart reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put multipart reclaim (root)",
                    source: e,
                })?;

            for part in &reclaim.parts {
                match part {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid,
                        data_pg_id,
                        ec,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::ShardSet as u8,
                                    &part_okh[..],
                                    part_vid.get() as i64,
                                    *data_pg_id as i64,
                                    ec.k,
                                    ec.m,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part shard set)",
                                source: e,
                            })?;
                    }
                    MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::Segments as u8,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part segments)",
                                source: e,
                            })?;

                        for segment in segments {
                            self.conn
                                .execute(
                                    "INSERT OR REPLACE INTO multipart_reclaim_part_segments \
                                     (bucket, key, generation_id, part_number, segment_index, \
                                      segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                    params![
                                        reclaim.bucket,
                                        reclaim.key,
                                        reclaim.generation_id.get() as i64,
                                        segment.part_number as i64,
                                        segment.segment_index as i64,
                                        &segment.segment_okh[..],
                                        segment.segment_vid.get() as i64,
                                        segment.data_pg_id as i64,
                                        segment.ec.k,
                                        segment.ec.m,
                                    ],
                                )
                                .map_err(|e| MetadataError::Db {
                                    context: "put multipart reclaim (part segment)",
                                    source: e,
                                })?;
                        }
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put multipart reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<MultipartReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT part_number, storage_kind, part_okh, part_vid, data_pg_id, ec_k, ec_m \
                 FROM multipart_reclaim_parts \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (prepare parts)",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u32,
                    Self::parse_enum(
                        row.get::<_, u8>(1)?,
                        1,
                        "storage_kind",
                        MultipartReclaimPartKind::from_u8,
                    )?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<u8>>(5)?,
                    row.get::<_, Option<u8>>(6)?,
                ))
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (query parts)",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            let (part_number, kind, part_okh, part_vid, data_pg_id, ec_k, ec_m) =
                row.map_err(|e| MetadataError::Db {
                    context: "get multipart reclaim (part row)",
                    source: e,
                })?;
            match kind {
                MultipartReclaimPartKind::ShardSet => {
                    let part_okh = part_okh.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_okh)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_okh"),
                        ),
                    })?;
                    let part_okh =
                        Self::parse_okh_blob(&part_okh, 2).map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (invalid part_okh)",
                            source: e,
                        })?;
                    let part_vid = part_vid.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_vid)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_vid"),
                        ),
                    })?;
                    let data_pg_id = data_pg_id.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing data_pg_id)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing data_pg_id"),
                        ),
                    })?;
                    let ec_k = ec_k.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_k)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_k"),
                        ),
                    })?;
                    let ec_m = ec_m.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_m)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_m"),
                        ),
                    })?;
                    parts.push(MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid: Self::parse_generation_id(part_vid, 3, "part_vid").map_err(
                            |e| MetadataError::Db {
                                context: "get multipart reclaim (invalid part_vid)",
                                source: e,
                            },
                        )?,
                        data_pg_id: data_pg_id as u32,
                        ec: EcShape { k: ec_k, m: ec_m },
                    });
                }
                MultipartReclaimPartKind::Segments => {
                    let mut segment_stmt = self
                        .conn
                        .prepare_cached(
                            "SELECT part_number, segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                             FROM multipart_reclaim_part_segments \
                             WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 AND part_number = ?4 \
                             ORDER BY segment_index ASC",
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (prepare part segments)",
                            source: e,
                        })?;

                    let segments = segment_stmt
                        .query_map(
                            params![bucket, key, generation_id.get() as i64, part_number],
                            |row| {
                                let segment_okh = Self::blob_to_okh(row.get(2)?, 2)?;
                                Ok(MultipartReclaimPartSegmentRecord {
                                    part_number: row.get::<_, i64>(0)? as u32,
                                    segment_index: row.get::<_, i64>(1)? as u32,
                                    segment_okh,
                                    segment_vid: Self::parse_generation_id(
                                        row.get::<_, i64>(3)?,
                                        3,
                                        "segment_vid",
                                    )?,
                                    data_pg_id: row.get::<_, i64>(4)? as u32,
                                    ec: EcShape {
                                        k: row.get(5)?,
                                        m: row.get(6)?,
                                    },
                                })
                            },
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (query part segments)",
                            source: e,
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (collect part segments)",
                            source: e,
                        })?;

                    parts.push(MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    });
                }
            }
        }

        Ok(Some(MultipartReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            parts,
        }))
    }

    #[cfg(test)]
    fn delete_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_reclaim_direct(bucket, key, generation_id)
    }

    fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, MetadataError> {
        self.query_row_cached_metadata(
            "SELECT
                EXISTS(
                    SELECT 1 FROM object_segments_reclaims
                    WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                )
                OR EXISTS(
                    SELECT 1 FROM multipart_reclaims
                    WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                )",
            params![bucket, key, generation_id.get() as i64],
            "payload reclaim exists",
            |row| Ok(row.get::<_, i64>(0)? != 0),
        )
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, MetadataError> {
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, generation_id FROM (
                 SELECT bucket, key, generation_id FROM object_segments_reclaims WHERE bucket = ?1
                 UNION ALL
                 SELECT bucket, key, generation_id FROM multipart_reclaims WHERE bucket = ?1
             )
             ORDER BY key ASC, generation_id ASC
             LIMIT 1",
            params![bucket],
            "get bucket payload reclaim root",
            |row| {
                Ok(PayloadReclaimRoot {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    generation_id: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "generation_id",
                    )?,
                })
            },
        )
    }

    fn get_payload_reclaim_root(&self) -> Result<Option<PayloadReclaimRoot>, MetadataError> {
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, generation_id FROM (
                 SELECT bucket, key, generation_id FROM object_segments_reclaims
                 UNION ALL
                 SELECT bucket, key, generation_id FROM multipart_reclaims
             )
             ORDER BY bucket ASC, key ASC, generation_id ASC
             LIMIT 1",
            [],
            "get payload reclaim root",
            |row| {
                Ok(PayloadReclaimRoot {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    generation_id: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "generation_id",
                    )?,
                })
            },
        )
    }

    fn get_bucket_delete_finalize_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, MetadataError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "get bucket delete finalize roots now",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get bucket delete finalize roots limit",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;

        let mut roots = Vec::new();
        let expired_claim_root = self.query_row_cached_optional_metadata(
            "SELECT c.bucket, c.bucket_incarnation_generation
             FROM bucket_delete_finalize_claims c
             JOIN buckets b
               ON b.name = c.bucket
              AND b.bucket_incarnation_generation = c.bucket_incarnation_generation
              AND b.state = ?1
             WHERE c.singleton = 0
               AND c.lease_deadline <= ?2",
            params![BucketState::Deleting as u8, now],
            "get expired bucket delete finalize claim root",
            bucket_delete_finalize_root_from_row,
        )?;
        if let Some(root) = expired_claim_root {
            roots.push(root);
        }

        if roots.len() == limit {
            return Ok(roots);
        }

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT name, bucket_incarnation_generation
                 FROM buckets b
                 WHERE b.state = ?1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM bucket_delete_finalize_claims c
                     WHERE c.singleton = 0
                       AND c.bucket = b.name
                       AND c.bucket_incarnation_generation = b.bucket_incarnation_generation
                       AND (c.lease_deadline IS NULL OR c.lease_deadline > ?3)
                   )
                 ORDER BY name ASC
                 LIMIT ?2",
            )
            .map_err(|source| MetadataError::Db {
                context: "prepare get bucket delete finalize roots",
                source,
            })?;
        let rows = stmt
            .query_map(
                params![BucketState::Deleting as u8, limit_i64, now],
                bucket_delete_finalize_root_from_row,
            )
            .map_err(|source| MetadataError::Db {
                context: "query get bucket delete finalize roots",
                source,
            })?;
        for row in rows {
            let root = row.map_err(|source| MetadataError::Db {
                context: "row get bucket delete finalize roots",
                source,
            })?;
            if roots.iter().any(|existing| existing == &root) {
                continue;
            }
            roots.push(root);
            if roots.len() == limit {
                break;
            }
        }

        Ok(roots)
    }

    fn get_lifecycle_sweep_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, MetadataError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "get lifecycle sweep roots now",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get lifecycle sweep roots limit",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;

        self.with_immediate_txn(
            "get lifecycle sweep roots (begin txn)",
            "get lifecycle sweep roots (commit txn)",
            |store| {
                store
                    .conn
                    .execute(
                        &format!(
                            "DELETE FROM lifecycle_sweep_claims \
                             WHERE lease_deadline <= ?1 \
                               AND NOT EXISTS ( \
                                 SELECT 1 FROM buckets b \
                                 WHERE b.name = lifecycle_sweep_claims.bucket \
                                   AND b.state = ?2 \
                                   AND b.bucket_incarnation_generation = \
                                       lifecycle_sweep_claims.bucket_incarnation_generation \
                                   AND NOT EXISTS ( \
                                     SELECT 1 FROM bucket_write_drains d \
                                     WHERE d.bucket_name = b.name \
                                   ) \
                                   AND ( \
                                     EXISTS ( \
                                       SELECT 1 FROM bucket_subresources lifecycle \
                                       WHERE lifecycle.bucket_name = b.name \
                                         AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                                         AND lifecycle.body IS NOT NULL \
                                     ) \
                                     OR EXISTS ( \
                                       SELECT 1 FROM multipart_uploads m \
                                       WHERE m.bucket = b.name AND m.state = ?3 \
                                     ) \
                                   ) \
                               )"
                        ),
                        params![now, BucketState::Active as u8, UploadState::Aborting as u8],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "clear stale expired lifecycle sweep claims",
                        source,
                    })?;

                let mut roots = Vec::new();
                let mut expired_stmt = store
                    .conn
                    .prepare_cached(
                        "SELECT bucket, bucket_incarnation_generation, 0 AS source \
                         FROM lifecycle_sweep_claims \
                         WHERE lease_deadline <= ?1 \
                         ORDER BY bucket ASC, bucket_incarnation_generation ASC \
                         LIMIT ?2",
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "prepare get expired lifecycle sweep claim roots",
                        source,
                    })?;
                let expired_rows = expired_stmt
                    .query_map(params![now, limit_i64], lifecycle_sweep_root_from_row)
                    .map_err(|source| MetadataError::Db {
                        context: "query get expired lifecycle sweep claim roots",
                        source,
                    })?;
                for row in expired_rows {
                    roots.push(row.map_err(|source| MetadataError::Db {
                        context: "row get expired lifecycle sweep claim roots",
                        source,
                    })?);
                }
                drop(expired_stmt);

                if roots.len() == limit {
                    return Ok(roots);
                }

                let remaining_limit =
                    i64::try_from(limit - roots.len()).map_err(|source| MetadataError::Db {
                        context: "get lifecycle busy roots remaining limit",
                        source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                    })?;

                let mut busy_stmt = store
                    .conn
                    .prepare_cached(&format!(
                        "SELECT c.bucket, c.bucket_incarnation_generation, 1 AS source \
                         FROM lifecycle_sweep_claims c \
                         JOIN buckets b \
                           ON b.name = c.bucket \
                          AND b.bucket_incarnation_generation = c.bucket_incarnation_generation \
                         WHERE b.state = ?1 \
                           AND (c.lease_deadline IS NULL OR c.lease_deadline > ?2) \
                           AND NOT EXISTS ( \
                             SELECT 1 FROM bucket_write_drains d WHERE d.bucket_name = b.name \
                           ) \
                           AND ( \
                             EXISTS ( \
                               SELECT 1 FROM bucket_subresources lifecycle \
                               WHERE lifecycle.bucket_name = b.name \
                                 AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                                 AND lifecycle.body IS NOT NULL \
                             ) \
                             OR EXISTS ( \
                               SELECT 1 FROM multipart_uploads m \
                               WHERE m.bucket = b.name AND m.state = ?3 \
                             ) \
                           ) \
                         ORDER BY c.bucket ASC, c.bucket_incarnation_generation ASC \
                         LIMIT ?4"
                    ))
                    .map_err(|source| MetadataError::Db {
                        context: "prepare get busy lifecycle sweep roots",
                        source,
                    })?;
                let busy_rows = busy_stmt
                    .query_map(
                        params![
                            BucketState::Active as u8,
                            now,
                            UploadState::Aborting as u8,
                            remaining_limit,
                        ],
                        lifecycle_sweep_root_from_row,
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "query get busy lifecycle sweep roots",
                        source,
                    })?;
                for row in busy_rows {
                    let root = row.map_err(|source| MetadataError::Db {
                        context: "row get busy lifecycle sweep roots",
                        source,
                    })?;
                    roots.push(root);
                }
                drop(busy_stmt);

                if roots.len() == limit {
                    return Ok(roots);
                }

                let remaining_limit =
                    i64::try_from(limit - roots.len()).map_err(|source| MetadataError::Db {
                        context: "get lifecycle sweep roots remaining limit",
                        source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                    })?;

                let mut stmt = store
                    .conn
                    .prepare_cached(&format!(
                        "SELECT b.name, b.bucket_incarnation_generation, \
                                CASE \
                                  WHEN EXISTS ( \
                                    SELECT 1 FROM bucket_subresources lifecycle \
                                    WHERE lifecycle.bucket_name = b.name \
                                      AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                                      AND lifecycle.body IS NOT NULL \
                                  ) THEN 2 \
                                  ELSE 3 \
                                END AS source \
                         FROM buckets b \
                         WHERE b.state = ?1 \
                           AND NOT EXISTS ( \
                             SELECT 1 FROM lifecycle_sweep_claims c \
                             WHERE c.bucket = b.name \
                               AND c.bucket_incarnation_generation = b.bucket_incarnation_generation \
                           ) \
                           AND NOT EXISTS ( \
                             SELECT 1 FROM bucket_write_drains d WHERE d.bucket_name = b.name \
                           ) \
                           AND ( \
                             EXISTS ( \
                               SELECT 1 FROM bucket_subresources lifecycle \
                               WHERE lifecycle.bucket_name = b.name \
                                 AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                                 AND lifecycle.body IS NOT NULL \
                             ) \
                             OR EXISTS ( \
                               SELECT 1 FROM multipart_uploads m \
                               WHERE m.bucket = b.name AND m.state = ?3 \
                             ) \
                           ) \
                         ORDER BY b.name ASC \
                         LIMIT ?2"
                    ))
                    .map_err(|source| MetadataError::Db {
                        context: "prepare get lifecycle sweep roots",
                        source,
                    })?;
                let rows = stmt
                    .query_map(
                        params![
                            BucketState::Active as u8,
                            remaining_limit,
                            UploadState::Aborting as u8,
                        ],
                        lifecycle_sweep_root_from_row,
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "query get lifecycle sweep roots",
                        source,
                    })?;
                for row in rows {
                    let root = row.map_err(|source| MetadataError::Db {
                        context: "row get lifecycle sweep roots",
                        source,
                    })?;
                    roots.push(root);
                }

                Ok(roots)
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_object_payload_reclaim_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "acquire object payload reclaim claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire object payload reclaim claim claimed_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire object payload reclaim claim lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;

        self.with_immediate_txn(
            "acquire object payload reclaim claim (begin txn)",
            "acquire object payload reclaim claim (commit txn)",
            |store| {
                let existing = store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, key, generation_id, reclaim_kind, \
                                claim_id, owner_token, cluster_epoch, pg_id, claimed_at, \
                                lease_deadline, attempt_count, last_error \
                         FROM object_payload_reclaim_claims \
                         WHERE singleton = 0",
                        [],
                        object_payload_reclaim_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "load object payload reclaim claim",
                        source,
                    })?;
                let mut attempt_count = 1_i64;
                if let Some(existing) = existing {
                    let same_work = existing.bucket == *bucket
                        && existing.bucket_incarnation_generation
                            == bucket_incarnation_generation as u64
                        && existing.key == *key
                        && existing.generation_id == generation_id
                        && existing.reclaim_kind == reclaim_kind;
                    if same_work
                        && existing.claim_id == claim_id
                        && existing.owner_token == owner_token
                        && existing.cluster_epoch == cluster_epoch
                    {
                        return Ok(Some(existing));
                    }
                    if existing.lease_deadline.is_none_or(|deadline| deadline > now) {
                        return Ok(None);
                    }
                    if !same_work {
                        return Ok(None);
                    }
                    attempt_count =
                        i64::try_from(existing.attempt_count.saturating_add(1)).map_err(
                            |source| MetadataError::Db {
                                context: "acquire object payload reclaim claim attempt_count",
                                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                            },
                        )?;
                    store
                        .conn
                        .execute(
                            "DELETE FROM object_payload_reclaim_claims WHERE singleton = 0",
                            [],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "clear expired object payload reclaim claim",
                            source,
                        })?;
                }

                let root_exists = match reclaim_kind {
                    ObjectPayloadReclaimKind::ObjectSegments => store.conn.query_row(
                        "SELECT 1 FROM object_segments_reclaims \
                         WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                        params![bucket, key, generation_id.get() as i64],
                        |_| Ok(()),
                    ),
                    ObjectPayloadReclaimKind::Multipart => store.conn.query_row(
                        "SELECT 1 FROM multipart_reclaims \
                         WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                        params![bucket, key, generation_id.get() as i64],
                        |_| Ok(()),
                    ),
                }
                .optional()
                .map_err(|source| MetadataError::Db {
                    context: "acquire object payload reclaim claim (check root)",
                    source,
                })?
                .is_some();
                if !root_exists {
                    return Ok(None);
                }

                store
                    .conn
                    .execute(
                        "INSERT INTO object_payload_reclaim_claims \
                         (singleton, bucket, bucket_incarnation_generation, key, generation_id, \
                          reclaim_kind, claim_id, owner_token, cluster_epoch, pg_id, claimed_at, \
                          lease_deadline, attempt_count, last_error) \
                         VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL)",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            key,
                            generation_id.get() as i64,
                            reclaim_kind as u8,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                            store.pg_id,
                            claimed_at,
                            lease_deadline,
                            attempt_count,
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "insert object payload reclaim claim",
                        source,
                    })?;

                store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, key, generation_id, reclaim_kind, \
                                claim_id, owner_token, cluster_epoch, pg_id, claimed_at, \
                                lease_deadline, attempt_count, last_error \
                         FROM object_payload_reclaim_claims \
                         WHERE singleton = 0",
                        [],
                        object_payload_reclaim_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "reload object payload reclaim claim",
                        source,
                    })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn release_object_payload_reclaim_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release object payload reclaim claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        self.with_immediate_txn(
            "release object payload reclaim claim (begin txn)",
            "release object payload reclaim claim (commit txn)",
            |store| {
                let deleted = store
                    .conn
                    .execute(
                        "DELETE FROM object_payload_reclaim_claims \
                         WHERE singleton = 0 AND bucket = ?1 AND bucket_incarnation_generation = ?2 \
                           AND key = ?3 AND generation_id = ?4 AND reclaim_kind = ?5 \
                           AND claim_id = ?6 AND owner_token = ?7 AND cluster_epoch = ?8",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            key,
                            generation_id.get() as i64,
                            reclaim_kind as u8,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "release object payload reclaim claim",
                        source,
                    })?;
                if deleted == 0 {
                    let claim_exists = store
                        .conn
                        .query_row(
                            "SELECT 1 FROM object_payload_reclaim_claims WHERE singleton = 0",
                            [],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(|source| MetadataError::Db {
                            context: "release object payload reclaim claim (check existing)",
                            source,
                        })?
                        .is_some();
                    let error = if claim_exists {
                        MetadataError::ReclaimClaimConflict {
                            claim_id: claim_id.to_string(),
                        }
                    } else {
                        MetadataError::ReclaimClaimNotFound {
                            claim_id: claim_id.to_string(),
                        }
                    };
                    return Err(error);
                }
                Ok(())
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_bucket_delete_finalize_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "acquire bucket delete finalize claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire bucket delete finalize claim claimed_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire bucket delete finalize claim lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;

        self.with_immediate_txn(
            "acquire bucket delete finalize claim (begin txn)",
            "acquire bucket delete finalize claim (commit txn)",
            |store| {
                let existing = store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, lease_deadline, attempt_count, last_error \
                         FROM bucket_delete_finalize_claims \
                         WHERE singleton = 0",
                        [],
                        bucket_delete_finalize_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "load bucket delete finalize claim",
                        source,
                    })?;
                let mut attempt_count = 1_i64;
                if let Some(existing) = existing {
                    let same_work = existing.bucket == *bucket
                        && existing.bucket_incarnation_generation
                            == bucket_incarnation_generation as u64;
                    if same_work
                        && existing.claim_id == claim_id
                        && existing.owner_token == owner_token
                        && existing.cluster_epoch == cluster_epoch
                    {
                        return Ok(Some(existing));
                    }
                    if existing.lease_deadline.is_none_or(|deadline| deadline > now) {
                        return Ok(None);
                    }
                    if !same_work {
                        let existing_bucket_still_deleting = store
                            .conn
                            .query_row(
                                "SELECT 1 FROM buckets \
                                 WHERE name = ?1 AND state = ?2 AND bucket_incarnation_generation = ?3",
                                params![
                                    &existing.bucket,
                                    BucketState::Deleting as u8,
                                    existing.bucket_incarnation_generation as i64,
                                ],
                                |_| Ok(()),
                            )
                            .optional()
                            .map_err(|source| MetadataError::Db {
                                context: "acquire bucket delete finalize claim (check existing claim bucket)",
                                source,
                            })?
                            .is_some();
                        if existing_bucket_still_deleting {
                            return Ok(None);
                        }
                        store
                            .conn
                            .execute(
                                "DELETE FROM bucket_delete_finalize_claims WHERE singleton = 0",
                                [],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "clear expired terminal bucket delete finalize claim",
                                source,
                            })?;
                    } else {
                        attempt_count =
                            i64::try_from(existing.attempt_count.saturating_add(1)).map_err(
                                |source| MetadataError::Db {
                                    context: "acquire bucket delete finalize claim attempt_count",
                                    source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                                },
                            )?;
                        store
                            .conn
                            .execute(
                                "DELETE FROM bucket_delete_finalize_claims WHERE singleton = 0",
                                [],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "clear expired bucket delete finalize claim",
                                source,
                            })?;
                    }
                }

                let deleting_bucket_exists = store
                    .conn
                    .query_row(
                        "SELECT 1 FROM buckets \
                         WHERE name = ?1 AND state = ?2 AND bucket_incarnation_generation = ?3",
                        params![
                            bucket,
                            BucketState::Deleting as u8,
                            bucket_incarnation_generation,
                        ],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "acquire bucket delete finalize claim (check bucket)",
                        source,
                    })?
                    .is_some();
                if !deleting_bucket_exists {
                    return Ok(None);
                }

                store
                    .conn
                    .execute(
                        "INSERT INTO bucket_delete_finalize_claims \
                         (singleton, bucket, bucket_incarnation_generation, claim_id, owner_token, \
                          cluster_epoch, pg_id, claimed_at, lease_deadline, attempt_count, last_error) \
                         VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                            store.pg_id,
                            claimed_at,
                            lease_deadline,
                            attempt_count,
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "insert bucket delete finalize claim",
                        source,
                    })?;

                store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, lease_deadline, attempt_count, last_error \
                         FROM bucket_delete_finalize_claims \
                         WHERE singleton = 0",
                        [],
                        bucket_delete_finalize_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "reload bucket delete finalize claim",
                        source,
                    })
            },
        )
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release bucket delete finalize claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        self.with_immediate_txn(
            "release bucket delete finalize claim (begin txn)",
            "release bucket delete finalize claim (commit txn)",
            |store| {
                let deleted = store
                    .conn
                    .execute(
                        "DELETE FROM bucket_delete_finalize_claims \
                         WHERE singleton = 0 AND bucket = ?1 AND bucket_incarnation_generation = ?2 \
                           AND claim_id = ?3 AND owner_token = ?4 AND cluster_epoch = ?5",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "release bucket delete finalize claim",
                        source,
                    })?;
                if deleted == 0 {
                    let claim_exists = store
                        .conn
                        .query_row(
                            "SELECT 1 FROM bucket_delete_finalize_claims WHERE singleton = 0",
                            [],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(|source| MetadataError::Db {
                            context: "release bucket delete finalize claim (check existing)",
                            source,
                        })?
                        .is_some();
                    let error = if claim_exists {
                        MetadataError::ReclaimClaimConflict {
                            claim_id: claim_id.to_string(),
                        }
                    } else {
                        MetadataError::ReclaimClaimNotFound {
                            claim_id: claim_id.to_string(),
                        }
                    };
                    return Err(error);
                }
                Ok(())
            },
        )
    }

    fn bucket_delete_finalize_claim(
        &self,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, MetadataError> {
        self.query_row_cached_optional_metadata(
            "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                    cluster_epoch, pg_id, claimed_at, lease_deadline, attempt_count, last_error \
             FROM bucket_delete_finalize_claims \
             WHERE singleton = 0",
            [],
            "bucket delete finalize claim",
            bucket_delete_finalize_claim_from_row,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "acquire lifecycle sweep claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire lifecycle sweep claim claimed_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire lifecycle sweep claim lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;

        self.with_immediate_txn(
            "acquire lifecycle sweep claim (begin txn)",
            "acquire lifecycle sweep claim (commit txn)",
            |store| {
                let existing = store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, heartbeat_at, lease_deadline, \
                                attempt_count, last_error \
                         FROM lifecycle_sweep_claims \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                        params![bucket, bucket_incarnation_generation],
                        lifecycle_sweep_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "load lifecycle sweep claim",
                        source,
                    })?;
                let mut attempt_count = 1_i64;
                let mut last_error: Option<String> = None;
                if let Some(existing) = existing {
                    if existing.claim_id == claim_id
                        && existing.owner_token == owner_token
                        && existing.cluster_epoch == cluster_epoch
                    {
                        return Ok(Some(existing));
                    }
                    if existing
                        .lease_deadline
                        .is_none_or(|deadline| deadline > now)
                    {
                        return Ok(None);
                    }
                    attempt_count = i64::try_from(existing.attempt_count.saturating_add(1))
                        .map_err(|source| MetadataError::Db {
                            context: "acquire lifecycle sweep claim attempt_count",
                            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
                        })?;
                    last_error = existing.last_error;
                    store
                        .conn
                        .execute(
                            "DELETE FROM lifecycle_sweep_claims \
                             WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                            params![bucket, bucket_incarnation_generation],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "clear expired lifecycle sweep claim",
                            source,
                        })?;
                }

                let claimable_bucket_exists = store
                    .conn
                    .query_row(
                        &format!(
                            "SELECT 1 FROM buckets b \
                             WHERE b.name = ?1 \
                               AND b.state = ?2 \
                               AND b.bucket_incarnation_generation = ?3 \
                               AND NOT EXISTS ( \
                                 SELECT 1 FROM bucket_write_drains d WHERE d.bucket_name = b.name \
                               ) \
                               AND ( \
                                 EXISTS ( \
                                   SELECT 1 FROM bucket_subresources lifecycle \
                                   WHERE lifecycle.bucket_name = b.name \
                                     AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                                     AND lifecycle.body IS NOT NULL \
                                 ) \
                                 OR EXISTS ( \
                                   SELECT 1 FROM multipart_uploads m \
                                   WHERE m.bucket = b.name AND m.state = ?4 \
                                 ) \
                               )"
                        ),
                        params![
                            bucket,
                            BucketState::Active as u8,
                            bucket_incarnation_generation,
                            UploadState::Aborting as u8,
                        ],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "acquire lifecycle sweep claim (check bucket)",
                        source,
                    })?
                    .is_some();
                if !claimable_bucket_exists {
                    return Ok(None);
                }

                store
                    .conn
                    .execute(
                        "INSERT INTO lifecycle_sweep_claims \
                         (bucket, bucket_incarnation_generation, claim_id, owner_token, \
                          cluster_epoch, pg_id, claimed_at, heartbeat_at, lease_deadline, \
                          attempt_count, last_error) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8, ?9, ?10)",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                            store.pg_id,
                            claimed_at,
                            lease_deadline,
                            attempt_count,
                            last_error,
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "insert lifecycle sweep claim",
                        source,
                    })?;

                store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, heartbeat_at, lease_deadline, \
                                attempt_count, last_error \
                         FROM lifecycle_sweep_claims \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                        params![bucket, bucket_incarnation_generation],
                        lifecycle_sweep_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| MetadataError::Db {
                        context: "reload lifecycle sweep claim",
                        source,
                    })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn heartbeat_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "heartbeat lifecycle sweep claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let heartbeat_at = i64::try_from(heartbeat_at).map_err(|source| MetadataError::Db {
            context: "heartbeat lifecycle sweep claim heartbeat_at",
            source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "heartbeat lifecycle sweep claim lease_deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;

        self.with_immediate_txn(
            "heartbeat lifecycle sweep claim (begin txn)",
            "heartbeat lifecycle sweep claim (commit txn)",
            |store| {
                let updated = store
                    .conn
                    .execute(
                        "UPDATE lifecycle_sweep_claims \
                         SET heartbeat_at = ?6, lease_deadline = ?7 \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2 \
                           AND claim_id = ?3 AND owner_token = ?4 AND cluster_epoch = ?5",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                            heartbeat_at,
                            lease_deadline,
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "heartbeat lifecycle sweep claim",
                        source,
                    })?;
                if updated == 0 {
                    let claim_exists = store
                        .conn
                        .query_row(
                            "SELECT 1 FROM lifecycle_sweep_claims \
                             WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                            params![bucket, bucket_incarnation_generation],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(|source| MetadataError::Db {
                            context: "heartbeat lifecycle sweep claim (check existing)",
                            source,
                        })?
                        .is_some();
                    let error = if claim_exists {
                        MetadataError::ReclaimClaimConflict {
                            claim_id: claim_id.to_string(),
                        }
                    } else {
                        MetadataError::ReclaimClaimNotFound {
                            claim_id: claim_id.to_string(),
                        }
                    };
                    return Err(error);
                }

                store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, heartbeat_at, lease_deadline, \
                                attempt_count, last_error \
                         FROM lifecycle_sweep_claims \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                        params![bucket, bucket_incarnation_generation],
                        lifecycle_sweep_claim_from_row,
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "reload heartbeat lifecycle sweep claim",
                        source,
                    })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_lifecycle_sweep_claim_error(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "record lifecycle sweep claim error incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        self.with_immediate_txn(
            "record lifecycle sweep claim error (begin txn)",
            "record lifecycle sweep claim error (commit txn)",
            |store| {
                let updated = store
                    .conn
                    .execute(
                        "UPDATE lifecycle_sweep_claims \
                         SET last_error = ?6 \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2 \
                           AND claim_id = ?3 AND owner_token = ?4 AND cluster_epoch = ?5",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                            last_error,
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "record lifecycle sweep claim error",
                        source,
                    })?;
                if updated == 0 {
                    let claim_exists = store
                        .conn
                        .query_row(
                            "SELECT 1 FROM lifecycle_sweep_claims \
                             WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                            params![bucket, bucket_incarnation_generation],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(|source| MetadataError::Db {
                            context: "record lifecycle sweep claim error (check existing)",
                            source,
                        })?
                        .is_some();
                    let error = if claim_exists {
                        MetadataError::ReclaimClaimConflict {
                            claim_id: claim_id.to_string(),
                        }
                    } else {
                        MetadataError::ReclaimClaimNotFound {
                            claim_id: claim_id.to_string(),
                        }
                    };
                    return Err(error);
                }

                store
                    .conn
                    .query_row(
                        "SELECT bucket, bucket_incarnation_generation, claim_id, owner_token, \
                                cluster_epoch, pg_id, claimed_at, heartbeat_at, lease_deadline, \
                                attempt_count, last_error \
                         FROM lifecycle_sweep_claims \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                        params![bucket, bucket_incarnation_generation],
                        lifecycle_sweep_claim_from_row,
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "reload lifecycle sweep claim error",
                        source,
                    })
            },
        )
    }

    fn release_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release lifecycle sweep claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        self.with_immediate_txn(
            "release lifecycle sweep claim (begin txn)",
            "release lifecycle sweep claim (commit txn)",
            |store| {
                let deleted = store
                    .conn
                    .execute(
                        "DELETE FROM lifecycle_sweep_claims \
                         WHERE bucket = ?1 AND bucket_incarnation_generation = ?2 \
                           AND claim_id = ?3 AND owner_token = ?4 AND cluster_epoch = ?5",
                        params![
                            bucket,
                            bucket_incarnation_generation,
                            claim_id,
                            owner_token,
                            cluster_epoch.get(),
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "release lifecycle sweep claim",
                        source,
                    })?;
                if deleted == 0 {
                    let claim_exists = store
                        .conn
                        .query_row(
                            "SELECT 1 FROM lifecycle_sweep_claims \
                             WHERE bucket = ?1 AND bucket_incarnation_generation = ?2",
                            params![bucket, bucket_incarnation_generation],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(|source| MetadataError::Db {
                            context: "release lifecycle sweep claim (check existing)",
                            source,
                        })?
                        .is_some();
                    let error = if claim_exists {
                        MetadataError::ReclaimClaimConflict {
                            claim_id: claim_id.to_string(),
                        }
                    } else {
                        MetadataError::ReclaimClaimNotFound {
                            claim_id: claim_id.to_string(),
                        }
                    };
                    return Err(error);
                }
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn put_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        tags: &str,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET tags = ?1 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = 0",
            params![tags, bucket, key, version_id.to_u64() as i64],
            "put object tags",
        )?;
        if updated == 0 {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object tags (check status)",
                |row| row.get(0),
            )?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<String>, MetadataError> {
        let result = self
            .query_row_cached_optional_metadata(
                "SELECT tags FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
                params![bucket, key, version_id.to_u64() as i64],
                "get object tags",
                |row| row.get(0),
            )?;
        if let Some(tags) = result {
            Ok(tags)
        } else {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "get object tags (check status)",
                |row| row.get(0),
            )?;
            match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            }
        }
    }

    #[cfg(test)]
    fn delete_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET tags = NULL WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object tags",
        )?;
        if updated == 0 {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "delete object tags (check status)",
                |row| row.get(0),
            )?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    // ── Multipart upload methods ──────────────────────────────────

    #[cfg(test)]
    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError> {
        let object_generation_id = self.next_generation_id(&req.bucket, &req.key)?;
        let command = CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
            req.clone(),
            object_generation_id,
            PgStore::now_millis(),
            BucketWriteReservationProof {
                bucket: req.bucket.clone(),
                reservation_id: "test-create-mpu-reservation".to_string(),
                owner_token: "test-owner-token".to_string(),
                cluster_epoch: ClusterEpoch::INITIAL,
                bucket_execution_generation: 1,
                bucket_incarnation_generation: 1,
                operation_kind: "create-multipart-upload".to_string(),
                created_at: PgStore::now_millis(),
                lease_deadline: None,
                target_context: Some(req.key.as_str().to_string()),
            },
        );
        self.create_multipart_upload_explicit(&command.upload)
    }

    fn get_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                 system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
                 checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id \
                 FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                |row| {
                    let state_raw = row.get::<_, u8>(4)?;
                    let algo_raw: Option<u8> = row.get(12)?;
                    let ctype_raw: Option<u8> = row.get(13)?;
                    let object_lock = Self::parse_object_lock_state(
                        row.get::<_, Option<u8>>(18)?,
                        row.get::<_, Option<i64>>(19)?,
                        row.get::<_, u8>(20)?,
                        18,
                        19,
                        20,
                    )?;
                    let checksum = if let Some(algo_val) = algo_raw {
                        let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid checksum algorithm: {algo_val}")),
                            )
                        })?;
                        let ctype = ctype_raw
                            .map(|v| {
                                ChecksumType::from_u8(v).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        13,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!("invalid checksum type: {v}")),
                                    )
                                })
                            })
                            .transpose()?;
                        Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(e.reason),
                            )
                        })?)
                    } else {
                        None
                    };
                    let owner = Self::parse_owner_identity(
                        row,
                        8,
                        9,
                        "owner_principal",
                        "owner_canonical_id",
                    )?;
                    let initiator = Self::parse_optional_owner_identity(
                        row,
                        10,
                        11,
                        "initiator_principal",
                        "initiator_canonical_id",
                    )?;
                    Ok(MultipartUploadRecord {
                        upload_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        initiated_at: row.get::<_, i64>(3)? as u64,
                        state: UploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid upload state: {state_raw}")),
                            )
                        })?,
                        tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                        metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                        system_metadata_blob: SerializedSystemMetadataBlob::from(
                            row.get::<_, Vec<u8>>(7)?,
                        ),
                        initiator,
                        owner,
                        acl_grants: Self::parse_acl_grants(
                            row.get::<_, String>(16)?,
                            16,
                            "multipart acl_grants",
                        )?,
                        public_read: row.get::<_, i64>(17)? != 0,
                        object_generation_id: Self::parse_generation_id(
                            row.get::<_, i64>(21)?,
                            21,
                            "object_generation_id",
                        )?,
                        object_lock,
                        checksum,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(14)?,
                            row.get::<_, Option<Vec<u8>>>(15)?,
                            14,
                            15,
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn set_upload_state(
        &self,
        upload_id: &UploadId,
        new_state: UploadState,
    ) -> Result<(), MetadataError> {
        // Only Completing and Aborting are valid transition targets.
        // Check existence first so we return NoSuchUpload accurately.
        if new_state == UploadState::InProgress {
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (exists check)",
                    source: e,
                })?;
            return match current {
                Some(state) => Err(MetadataError::UploadNotInProgress { state }),
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
            };
        }
        let updated = self
            .conn
            .execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![new_state as u8, upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set upload state",
                source: e,
            })?;
        if updated == 0 {
            // Either the upload doesn't exist or it's not InProgress.
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (check)",
                    source: e,
                })?;
            return match current {
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
                Some(s) => Err(MetadataError::UploadNotInProgress { state: s }),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn delete_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete multipart upload (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<(), MetadataError> {
            self.conn
                .execute(
                    "DELETE FROM object_generation_reservations \
                     WHERE reservation_id = ?1",
                    params![upload_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete multipart upload generation reservation",
                    source: e,
                })?;
            let deleted = self
                .conn
                .execute(
                    "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete multipart upload",
                    source: e,
                })?;
            if deleted == 0 {
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "delete multipart upload (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn get_completed_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<CompletedMultipartUploadRecord>, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                 owner_principal, owner_canonical_id, \
                 initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                Self::completed_multipart_upload_record_from_row,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get completed multipart upload",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                params![bucket],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart uploads for bucket",
                source: e,
            })
    }

    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_multipart_uploads",
            "pg_id={} bucket={:?} max_uploads={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_uploads
        );
        let limit = req.max_uploads as i64 + 1;
        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        if let Some(ref key_marker) = req.key_marker {
            if let Some(uid_marker) = req.upload_id_marker.as_ref().map(UploadId::as_str) {
                // Resume after (key_marker, initiated_at of marker, uid_marker).
                // Use a subquery to resolve the marker's initiated_at so the
                // cursor is consistent with the (key, initiated_at, upload_id)
                // sort order. COALESCE to 0 so a deleted marker row safely
                // returns all remaining uploads for that key (duplicates are
                // preferable to silently dropped entries).
                where_clauses.push(format!(
                    "(key > ?{km} OR (key = ?{km} AND (\
                     initiated_at > COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     OR (initiated_at = COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     AND upload_id > ?{um}))))",
                    km = param_idx,
                    um = param_idx + 1,
                    bkt = param_idx + 2
                ));
                params_vec.push(Box::new(key_marker.clone()));
                params_vec.push(Box::new(uid_marker.to_string()));
                params_vec.push(Box::new(req.bucket.clone()));
                param_idx += 3;
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");
        let sql = format!(
            "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
             system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
             checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id \
             FROM multipart_uploads \
             WHERE {where_str} \
             ORDER BY key ASC, initiated_at ASC, upload_id ASC \
             LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list multipart uploads",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let state_raw = row.get::<_, u8>(4)?;
                let algo_raw: Option<u8> = row.get(12)?;
                let ctype_raw: Option<u8> = row.get(13)?;
                let object_lock = Self::parse_object_lock_state(
                    row.get::<_, Option<u8>>(18)?,
                    row.get::<_, Option<i64>>(19)?,
                    row.get::<_, u8>(20)?,
                    18,
                    19,
                    20,
                )?;
                let checksum = if let Some(algo_val) = algo_raw {
                    let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid checksum algorithm: {algo_val}")),
                        )
                    })?;
                    let ctype = ctype_raw
                        .map(|v| {
                            ChecksumType::from_u8(v).ok_or_else(|| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    13,
                                    rusqlite::types::Type::Integer,
                                    Box::from(format!("invalid checksum type: {v}")),
                                )
                            })
                        })
                        .transpose()?;
                    Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(e.reason),
                        )
                    })?)
                } else {
                    None
                };
                let owner =
                    Self::parse_owner_identity(row, 8, 9, "owner_principal", "owner_canonical_id")?;
                let initiator = Self::parse_optional_owner_identity(
                    row,
                    10,
                    11,
                    "initiator_principal",
                    "initiator_canonical_id",
                )?;
                Ok(MultipartUploadRecord {
                    upload_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    initiated_at: row.get::<_, i64>(3)? as u64,
                    state: UploadState::from_u8(state_raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid upload state: {state_raw}")),
                        )
                    })?,
                    tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                    metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                    system_metadata_blob: SerializedSystemMetadataBlob::from(
                        row.get::<_, Vec<u8>>(7)?,
                    ),
                    initiator,
                    owner,
                    acl_grants: Self::parse_acl_grants(
                        row.get::<_, String>(16)?,
                        16,
                        "multipart acl_grants",
                    )?,
                    public_read: row.get::<_, i64>(17)? != 0,
                    object_generation_id: Self::parse_generation_id(
                        row.get::<_, i64>(21)?,
                        21,
                        "object_generation_id",
                    )?,
                    object_lock,
                    checksum,
                    encryption: Self::parse_object_encryption(
                        row.get::<_, u8>(14)?,
                        row.get::<_, Option<Vec<u8>>>(15)?,
                        14,
                        15,
                    )?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list multipart uploads query",
                source: e,
            })?;

        let mut uploads: Vec<MultipartUploadRecord> = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart uploads row",
                source: e,
            })?);
        }

        let is_truncated = uploads.len() as i64 > req.max_uploads as i64;
        if is_truncated {
            uploads.truncate(req.max_uploads as usize);
        }

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            uploads.last().map_or((None, None), |u| {
                (Some(u.key.clone()), Some(u.upload_id.clone()))
            })
        } else {
            (None, None)
        };

        Ok(ListMultipartUploadsResp {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    #[cfg(test)]
    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert part (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<Option<u32>, rusqlite::Error> {
            // Read previous generation before overwrite.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get::<_, i64>(0).map(|v| v as u32),
                )
                .optional()?;

            self.conn.execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.payload_crc64 as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.placement_cluster_epoch.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )?;

            Ok(prev_gen)
        })();

        match result {
            Ok(prev_gen) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert part (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_gen)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                // FK violation means the upload_id doesn't exist.
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part",
                    source: e,
                })
            }
        }
    }

    #[cfg(test)]
    fn upsert_multipart_part_segments(
        &self,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), MetadataError> {
        if part.part_okh != [0u8; 16] {
            return Err(MetadataError::Db {
                context: "upsert multipart part segments (non-segment part)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("segmented multipart parts must use zero part_okh sentinel"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert multipart part segments (begin txn)",
                source: e,
            })?;

        let result =
            (|| -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), rusqlite::Error> {
                let prev_gen: Option<u32> = self
                    .conn
                    .query_row(
                        "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                        params![part.upload_id, part.part_number],
                        |row| row.get::<_, i64>(0).map(|v| v as u32),
                    )
                    .optional()?;

                let mut prev_stmt = self.conn.prepare_cached(
                    "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, \
                 segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3 \
                 ORDER BY segment_index ASC",
                )?;
                let prev_rows = prev_stmt.query_map(
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                    |row| {
                        let okh_blob: Vec<u8> = row.get(8)?;
                        let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                        Ok(MultipartPartSegmentRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            version_id: row.get::<_, i64>(3)? as u64,
                            part_number: row.get(4)?,
                            segment_index: row.get(5)?,
                            size: row.get::<_, i64>(6)? as u64,
                            segment_crc64: row.get::<_, i64>(7)? as u64,
                            segment_okh: okh,
                            segment_vid: Self::parse_generation_id(
                                row.get::<_, i64>(9)?,
                                9,
                                "segment_vid",
                            )?,
                            data_pg_id: row.get(10)?,
                            placement_cluster_epoch: Self::parse_cluster_epoch(
                                row.get::<_, i64>(11)?,
                                11,
                                "placement_cluster_epoch",
                            )?,
                            ec_k: row.get(12)?,
                            ec_m: row.get(13)?,
                        })
                    },
                )?;
                let prev_segments = prev_rows.collect::<Result<Vec<_>, _>>()?;

                self.conn.execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        part.upload_id,
                        part.part_number,
                        part.generation,
                        part.size as i64,
                        part.payload_crc64 as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.placement_cluster_epoch.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )?;

                self.conn.execute(
                    "DELETE FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3",
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                )?;

                let mut stmt = self.conn.prepare_cached(
                    "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                )?;
                for segment in segments {
                    if segment.upload_id != part.upload_id
                        || segment.part_number != part.part_number
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                    {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match multipart part identity"),
                        ));
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
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
                    ])?;
                }

                Ok((prev_gen, prev_segments))
            })();

        match result {
            Ok(prev_state) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert multipart part segments (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_state)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part segments",
                    source: e,
                })
            }
        }
    }

    fn get_multipart_part(
        &self,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                 part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
                 FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2",
                params![upload_id.as_str(), part_number],
                Self::row_to_multipart_part,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part",
                source: e,
            })?
            .ok_or(MetadataError::PartNotFound {
                upload_id: upload_id.to_string(),
                part_number,
            })
    }

    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        // Verify the upload exists so we return NoSuchUpload, not an empty list.
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM multipart_uploads WHERE upload_id = ?1",
                params![req.upload_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "list parts (upload exists check)",
                source: e,
            })?;
        if exists.is_none() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: req.upload_id.to_string(),
            });
        }
        if req.max_parts == 0 {
            return Ok(ListPartsResp {
                parts: Vec::new(),
                is_truncated: false,
                next_part_number_marker: Some(req.part_number_marker.unwrap_or(0)),
            });
        }

        let limit = req.max_parts as i64 + 1;

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.upload_id.clone())];
        let sql = if let Some(marker) = req.part_number_marker {
            params_vec.push(Box::new(marker));
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
             part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 AND part_number > ?2 \
             ORDER BY part_number ASC LIMIT ?3"
                .to_string()
        } else {
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
             part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 \
             ORDER BY part_number ASC LIMIT ?2"
                .to_string()
        };

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list multipart parts",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_multipart_part)
            .map_err(|e| MetadataError::Db {
                context: "list multipart parts query",
                source: e,
            })?;

        let mut parts: Vec<MultipartPartRecord> = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart parts row",
                source: e,
            })?);
        }

        let is_truncated = parts.len() as i64 > req.max_parts as i64;
        if is_truncated {
            parts.truncate(req.max_parts as usize);
        }

        let next_part_number_marker = if is_truncated {
            parts.last().map(|p| p.part_number)
        } else {
            None
        };

        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit object parts (begin txn)",
                source: e,
            })?;

        let result = (|| {
            let mut stmt = self.conn.prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            )?;

            let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
            ordered_parts.sort_by_key(|part| part.part_number);
            let mut object_offset_start = 0u64;
            for part in ordered_parts {
                stmt.execute(params![
                    part.bucket,
                    part.key,
                    part.version_id.to_u64() as i64,
                    part.part_number,
                    object_offset_start as i64,
                    part.size as i64,
                    part.payload_crc64 as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.placement_cluster_epoch.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.data_pg_id,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ])?;
                object_offset_start += part.size;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit object parts (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "commit object parts",
                    source: e,
                })
            }
        }
    }

    fn get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, payload_crc64, etag, etag_kind, \
                 part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64],
                Self::row_to_object_part,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts query",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts row",
                source: e,
            })?);
        }
        Ok(parts)
    }

    #[cfg(test)]
    fn get_object_parts_overlapping_range(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<ObjectPartRangeRecord>, MetadataError> {
        if start >= end_exclusive {
            return Ok(Vec::new());
        }

        let mut first_stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, payload_crc64, etag, etag_kind, \
                 part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND object_offset_start <= ?4 \
                   AND object_offset_start + size > ?4 \
                 ORDER BY object_offset_start DESC, part_number ASC \
                 LIMIT 1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range first part",
                source: e,
            })?;

        let first = first_stmt
            .query_row(
                params![bucket, key, version_id.to_u64() as i64, start as i64],
                Self::row_to_object_part_range,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range first part",
                source: e,
            })?;

        let Some(first) = first else {
            return Ok(Vec::new());
        };

        let first_part_number = first.part.part_number;
        let first_part_end = first.object_offset_start + first.part.size;
        if first_part_end >= end_exclusive {
            return Ok(vec![first]);
        }

        let mut parts = vec![first];
        let mut tail_stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, payload_crc64, etag, etag_kind, \
                 part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND part_number > ?4 \
                   AND object_offset_start < ?5 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range tail parts",
                source: e,
            })?;

        let rows = tail_stmt
            .query_map(
                params![
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    first_part_number,
                    end_exclusive as i64
                ],
                Self::row_to_object_part_range,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail parts",
                source: e,
            })?;

        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail row",
                source: e,
            })?);
        }

        Ok(parts)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn delete_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_parts_direct(bucket, key, version_id)
    }

    #[cfg(test)]
    fn complete_multipart_commit(
        &self,
        upload_id: &UploadId,
        completion_order: u64,
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<CompleteMultipartCommitCleanup, MetadataError> {
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "complete multipart commit (empty parts)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("multipart commit requires at least one part"),
                ),
            });
        }
        let now = PgStore::now_millis();
        let data_layout = DataLayout::MultipartManifest as u8;
        let parts_count = Some(parts.len() as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "complete multipart commit (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<CompleteMultipartCommitCleanup, rusqlite::Error> {
            // Validate part identity matches object.
            let mut selected_part_numbers = std::collections::BTreeSet::new();
            for part in parts {
                if part.bucket != obj.bucket
                    || part.key != obj.key
                    || part.version_id != obj.version_id
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("part does not match object"),
                    ));
                }
                if !selected_part_numbers.insert(part.part_number) {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("duplicate multipart completion part"),
                    ));
                }
            }

            // 1. Transition upload to Completing.
            let updated = self.conn.execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![UploadState::Completing as u8, upload_id.as_str()],
            )?;
            if updated == 0 {
                // Check if it's already Completing (idempotent retry).
                let current: Option<u8> = self
                    .conn
                    .query_row(
                        "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                        params![upload_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                match current {
                    Some(1) => { /* Already Completing — allow idempotent retry */ }
                    _ => {
                        return Err(rusqlite::Error::QueryReturnedNoRows);
                    }
                }
            }
            let upload_generation_id: i64 = self.conn.query_row(
                "SELECT object_generation_id FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
                |row| row.get(0),
            )?;
            if upload_generation_id != obj.generation_id.get() as i64 {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "multipart upload generation {} does not match commit generation {}",
                        upload_generation_id,
                        obj.generation_id.get()
                    )),
                ));
            }

            let omitted_parts = {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                     part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
                     FROM multipart_parts WHERE upload_id = ?1 ORDER BY part_number ASC",
                )?;
                let rows =
                    stmt.query_map(params![upload_id.as_str()], Self::row_to_multipart_part)?;
                let mut omitted = Vec::new();
                for row in rows {
                    let part = row?;
                    if !selected_part_numbers.contains(&part.part_number) {
                        omitted.push(part);
                    }
                }
                omitted
            };

            let (omitted_streaming_segments, omitted_streaming_part_numbers) = {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                     size, segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m \
                     FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND version_id = ?4 \
                     ORDER BY part_number, segment_index",
                )?;
                let rows = stmt.query_map(
                    params![
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64
                    ],
                    |row| {
                        let okh_blob: Vec<u8> = row.get(8)?;
                        let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                        Ok(MultipartPartSegmentRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            version_id: row.get::<_, i64>(3)? as u64,
                            part_number: row.get(4)?,
                            segment_index: row.get(5)?,
                            size: row.get::<_, i64>(6)? as u64,
                            segment_crc64: row.get::<_, i64>(7)? as u64,
                            segment_okh,
                            segment_vid: Self::parse_generation_id(
                                row.get::<_, i64>(9)?,
                                9,
                                "segment_vid",
                            )?,
                            data_pg_id: row.get(10)?,
                            placement_cluster_epoch: Self::parse_cluster_epoch(
                                row.get::<_, i64>(11)?,
                                11,
                                "placement_cluster_epoch",
                            )?,
                            ec_k: row.get(12)?,
                            ec_m: row.get(13)?,
                        })
                    },
                )?;
                let mut omitted = Vec::new();
                let mut omitted_part_numbers = std::collections::BTreeSet::new();
                for row in rows {
                    let segment = row?;
                    if !selected_part_numbers.contains(&segment.part_number) {
                        omitted_part_numbers.insert(segment.part_number);
                        omitted.push(segment);
                    }
                }
                (omitted, omitted_part_numbers)
            };
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock)?;
            let write_sequence = self
                .next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())
                .map_err(|error| match error {
                    MetadataError::Db { source, .. } => source,
                    other => rusqlite::Error::ToSqlConversionFailure(Box::new(
                        std::io::Error::other(other.to_string()),
                    )),
                })?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)
                .map_err(|error| match error {
                    MetadataError::Db { source, .. } => source,
                    other => rusqlite::Error::ToSqlConversionFailure(Box::new(
                        std::io::Error::other(other.to_string()),
                    )),
                })?;
            self.advance_object_write_counter_in_open_txn(
                &obj.bucket,
                &obj.key,
                write_sequence,
                Some(obj.generation_id),
            )
            .map_err(|error| match error {
                MetadataError::Db { source, .. } => source,
                other => rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    other.to_string(),
                ))),
            })?;

            // 2. Write/overwrite object metadata row.
            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn.execute(
                obj_sql,
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    write_sequence as i64,
                    obj.generation_id.get() as i64,
                    obj.size as i64,
                    obj.etag_crc64.as_slice(),
                    EtagKind::MultipartComposite as u8,
                    now as i64,
                    obj.ec.k,
                    obj.ec.m,
                    ObjectState::Live as u8,
                    tags,
                    data_layout,
                    parts_count,
                    metadata_blob,
                    system_metadata_blob,
                    encryption_type,
                    encryption_state,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    obj.acl_grants.serialized(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )?;

            // 3. Delete prior object_parts (null-version overwrite).
            self.conn.execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
            )?;

            // 4. Insert new manifest rows.
            {
                let mut stmt = self.conn.prepare_cached(
                    "INSERT INTO object_parts \
                     (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, etag, etag_kind, \
                      part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                )?;
                let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
                ordered_parts.sort_by_key(|part| part.part_number);
                let mut object_offset_start = 0u64;
                for part in ordered_parts {
                    stmt.execute(params![
                        part.bucket,
                        part.key,
                        part.version_id.to_u64() as i64,
                        part.part_number,
                        object_offset_start as i64,
                        part.size as i64,
                        part.payload_crc64 as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.placement_cluster_epoch.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.data_pg_id,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ])?;
                    object_offset_start += part.size;
                }
            }

            // 5. Clean up stale multipart_part_segments from prior uploads to
            //    the same key+version_id (e.g. overwriting in unversioned mode).
            self.conn.execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND upload_id != ?4",
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    upload_id
                ],
            )?;

            // 6. Delete omitted streamed part segment rows. Their shard files
            //    are returned to the caller for post-commit cleanup.
            for part_number in &omitted_streaming_part_numbers {
                self.conn.execute(
                    "DELETE FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 \
                     AND version_id = ?4 AND part_number = ?5",
                    params![
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part_number,
                    ],
                )?;
            }

            // 7. Reparent only selected streamed segments from staging
            //    version_id to the real object version_id so reads can find
            //    them. Omitted parts must not survive as unreachable rows.
            for part_number in &selected_part_numbers {
                self.conn.execute(
                    "UPDATE multipart_part_segments \
                     SET version_id = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4 \
                     AND version_id = ?5 AND part_number = ?6",
                    params![
                        obj.version_id.to_u64() as i64,
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part_number,
                    ],
                )?;
            }

            // 8. Record this upload as completed so AbortMultipartUpload can
            //    remain idempotently successful for the exact completed upload_id.
            let (initiator_principal, initiator_canonical_id): (Option<String>, Option<String>) =
                self.conn.query_row(
                    "SELECT initiator_principal, initiator_canonical_id \
                     FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
            self.conn.execute(
                "INSERT OR REPLACE INTO completed_multipart_uploads \
                 (upload_id, bucket, key, completion_order, completed_at, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    upload_id,
                    obj.bucket,
                    obj.key,
                    completion_order as i64,
                    now as i64,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    initiator_principal,
                    initiator_canonical_id,
                ],
            )?;

            // 9. Release the durable generation reservation now that the
            //    generation is visible on the committed object row.
            let released = self.conn.execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3 AND generation_id = ?4",
                params![
                    upload_id,
                    obj.bucket,
                    obj.key,
                    obj.generation_id.get() as i64,
                ],
            )?;
            if released == 0 {
                return Err(rusqlite::Error::QueryReturnedNoRows);
            }

            // 10. Delete in-progress upload + parts (CASCADE).
            self.conn.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )?;

            Ok(CompleteMultipartCommitCleanup {
                omitted_parts,
                omitted_streaming_segments,
                stream_uploads: Vec::new(),
                stream_upload_segments: Vec::new(),
            })
        })();

        match result {
            Ok(cleanup) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "complete multipart commit (commit txn)",
                        source: e,
                    });
                }
                Ok(cleanup)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "complete multipart commit",
                    source: e,
                })
            }
        }
    }
    // ── Streaming upload session methods ──────────────────────────────

    #[cfg(any(test, feature = "test-hooks"))]
    fn create_stream_upload(&self, req: &CreateStreamUploadReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            req.session_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let session = StreamUploadCommandRecord {
            session_id: req.session_id.clone(),
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            target: req.target.clone(),
            state: StreamUploadState::InProgress,
            created_at: PgStore::now_millis(),
            encryption: req.encryption.clone(),
        };
        self.create_stream_upload_explicit(&session, GenerationId::MIN, None)
    }

    fn get_stream_upload(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, MetadataError> {
        self.conn
            .query_row(
                &format!("{STREAM_UPLOAD_SELECT} WHERE session_id = ?1"),
                params![session_id.as_str()],
                parse_stream_upload_record,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })
    }

    fn allocate_stream_segment_vid(
        &self,
        session_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        let allocated = self
            .conn
            .query_row(
                "UPDATE stream_uploads \
                 SET next_segment_vid = next_segment_vid + 1 \
                 WHERE session_id = ?1 AND state = ?2 \
                 RETURNING next_segment_vid - 1",
                params![session_id.as_str(), StreamUploadState::InProgress as u8],
                |row| Self::parse_generation_id(row.get::<_, i64>(0)?, 0, "next_segment_vid"),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "allocate stream segment VID",
                source: e,
            })?;
        match allocated {
            Some(vid) => Ok(vid),
            None => match self.get_stream_upload(session_id) {
                Ok(session) => Err(MetadataError::StreamSessionNotInProgress {
                    state: session.state as u8,
                }),
                Err(error) => Err(error),
            },
        }
    }

    #[cfg(test)]
    fn set_stream_upload_state(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        self.set_stream_upload_state_direct(session_id, new_state)
    }

    #[cfg(test)]
    fn delete_stream_upload(&self, session_id: &SessionId) -> Result<(), MetadataError> {
        self.delete_stream_upload_direct(session_id)
    }

    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(STREAM_UPLOAD_SELECT)
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map([], parse_stream_upload_record)
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (collect)",
                source: e,
            })
    }

    fn list_all_stream_uploads_page(
        &self,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, MetadataError> {
        let fetch_limit = i64::from(limit) + 1;
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            match session_id_marker {
            Some(marker) => (
                format!("{STREAM_UPLOAD_SELECT} WHERE session_id > ?1 ORDER BY session_id ASC LIMIT ?2"),
                vec![Box::new(marker.clone()), Box::new(fetch_limit)],
            ),
            None => (
                format!("{STREAM_UPLOAD_SELECT} ORDER BY session_id ASC LIMIT ?1"),
                vec![Box::new(fetch_limit)],
            ),
        };
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads page (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(params_vec.iter()),
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads page (query)",
                source: e,
            })?;
        let mut uploads = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads page (collect)",
                source: e,
            })?;
        let next_session_id_marker = if uploads.len() > limit as usize {
            uploads.pop();
            uploads.last().map(|upload| upload.session_id.clone())
        } else {
            None
        };
        Ok(StreamUploadRecordPage {
            uploads,
            next_session_id_marker,
        })
    }

    fn list_stream_uploads_for_bucket_page(
        &self,
        bucket: &BucketName,
        session_id_marker: Option<&SessionId>,
        limit: u32,
    ) -> Result<StreamUploadRecordPage, MetadataError> {
        let fetch_limit = i64::from(limit) + 1;
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            match session_id_marker {
            Some(marker) => (
                format!(
                    "{STREAM_UPLOAD_SELECT} WHERE bucket = ?1 AND session_id > ?2 ORDER BY session_id ASC LIMIT ?3"
                ),
                vec![
                    Box::new(bucket.clone()),
                    Box::new(marker.clone()),
                    Box::new(fetch_limit),
                ],
            ),
            None => (
                format!("{STREAM_UPLOAD_SELECT} WHERE bucket = ?1 ORDER BY session_id ASC LIMIT ?2"),
                vec![Box::new(bucket.clone()), Box::new(fetch_limit)],
            ),
        };
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for bucket page (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(params_vec.iter()),
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for bucket page (query)",
                source: e,
            })?;
        let mut uploads = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for bucket page (collect)",
                source: e,
            })?;
        let next_session_id_marker = if uploads.len() > limit as usize {
            uploads.pop();
            uploads.last().map(|upload| upload.session_id.clone())
        } else {
            None
        };
        Ok(StreamUploadRecordPage {
            uploads,
            next_session_id_marker,
        })
    }

    #[cfg(test)]
    fn append_stream_segment(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.append_stream_segment_direct(segment)
    }

    fn list_stream_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT session_id, segment_index, size, segment_okh, segment_vid, data_pg_id, \
                 placement_cluster_epoch, segment_crc64, payload_crc64, ec_k, ec_m FROM stream_upload_segments \
                 WHERE session_id = ?1 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list stream segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![session_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(3)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 3)?;
                Ok(StreamUploadSegmentRecord {
                    session_id: row.get(0)?,
                    segment_index: row.get(1)?,
                    size: row.get::<_, i64>(2)? as u64,
                    segment_crc64: row.get::<_, i64>(7)? as u64,
                    payload_crc64: row.get::<_, i64>(8)? as u64,
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(4)?,
                        4,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(5)?,
                    placement_cluster_epoch: Self::parse_cluster_epoch(
                        row.get::<_, i64>(6)?,
                        6,
                        "placement_cluster_epoch",
                    )?,
                    ec_k: row.get(9)?,
                    ec_m: row.get(10)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list stream segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "list stream segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_with_segments(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put segment object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put segment object (non-segment layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put_object_with_segments requires Standard layout"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put segment object (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            let now = PgStore::now_millis();
            let data_layout = obj.layout.data_layout() as u8;
            let etag_kind = obj.etag.etag_kind() as u8;
            let status = ObjectState::Live as u8;
            let parts_count = obj.layout.parts_count().map(|n| n as i64);
            let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
            let metadata_blob = obj
                .metadata_blob
                .as_ref()
                .map(SerializedMetadataBlob::as_slice);
            let system_metadata_blob = obj
                .system_metadata_blob
                .as_ref()
                .map(SerializedSystemMetadataBlob::as_slice);
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                    context: "put segment object (encode object lock)",
                    source: e,
                })?;
            let encryption_type = obj.encryption.encryption_type() as u8;
            let encryption_state = obj.encryption.encode_state();
            let write_sequence =
                self.next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )
            .map_err(|e| MetadataError::Db {
                context: "put segment object (mark noncurrent)",
                source: e,
            })?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
                        write_sequence as i64,
                        obj.generation_id.get() as i64,
                        obj.size as i64,
                        obj.etag.as_bytes().as_slice(),
                        etag_kind,
                        now as i64,
                        obj.ec.k,
                        obj.ec.m,
                        status,
                        data_layout,
                        parts_count,
                        tags,
                        metadata_blob,
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        obj.owner.principal,
                        obj.owner.canonical_id.as_str(),
                        obj.acl_grants.serialized(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (write object)",
                    source: e,
                })?;

            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (delete prior segments)",
                    source: e,
                })?;

            let mut stmt = self
                .conn
                .prepare_cached(
                    "INSERT INTO object_segments \
                     (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                      data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (prepare insert segments)",
                    source: e,
                })?;
            for segment in segments {
                if segment.bucket != obj.bucket
                    || segment.key != obj.key
                    || segment.version_id != obj.version_id
                {
                    return Err(MetadataError::Db {
                        context: "put segment object (segment object mismatch)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match object identity"),
                        ),
                    });
                }
                stmt.execute(params![
                    segment.bucket,
                    segment.key,
                    segment.version_id.to_u64() as i64,
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
                    context: "put segment object (insert segment)",
                    source: e,
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put segment object (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    #[cfg(test)]
    fn commit_stream_part(
        &self,
        session_id: &SessionId,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream part (begin txn)",
                source: e,
            })?;

        let result: Result<Vec<MultipartPartSegmentRecord>, MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is UploadPart kind, and matches
            //    the target bucket/key/upload_id/part_number. Then transition to Completing.
            let sess_row = self.get_stream_upload(session_id)?;

            if sess_row.state != StreamUploadState::InProgress {
                return Err(MetadataError::StreamSessionNotInProgress {
                    state: sess_row.state as u8,
                });
            }
            // Validate session binding matches commit target.
            match &sess_row.target {
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number,
                } if upload_id == &part.upload_id && *part_number == part.part_number => {}
                _ => {
                    return Err(MetadataError::StreamSessionNotFound {
                        session_id: session_id.as_str().to_owned(),
                    })
                }
            }
            let sess_bucket = sess_row.bucket;
            let sess_key = sess_row.key;

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (set completing)",
                    source: e,
                })?;

            // 2. Upsert multipart part metadata.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (read prev gen)",
                    source: e,
                })?;

            let new_gen = prev_gen.map_or(0, |g| g + 1);

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                     (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                      part_okh, part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        part.upload_id,
                        part.part_number,
                        new_gen,
                        part.size as i64,
                        part.payload_crc64 as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.placement_cluster_epoch.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (upsert part)",
                    source: e,
                })?;

            // 3. Capture prior part segments for this upload+part before deleting
            //    their metadata rows so the caller can reclaim their shards.
            let displaced_segments = {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                         segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m FROM multipart_part_segments \
                         WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4 \
                         ORDER BY segment_index ASC",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare displaced segments)",
                        source: e,
                    })?;

                let rows = stmt
                    .query_map(
                        params![sess_bucket, sess_key, part.upload_id, part.part_number],
                        |row| {
                            let okh_blob: Vec<u8> = row.get(8)?;
                            let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                            Ok(MultipartPartSegmentRecord {
                                bucket: row.get(0)?,
                                key: row.get(1)?,
                                upload_id: row.get(2)?,
                                version_id: row.get::<_, i64>(3)? as u64,
                                part_number: row.get(4)?,
                                segment_index: row.get(5)?,
                                size: row.get::<_, i64>(6)? as u64,
                                segment_crc64: row.get::<_, i64>(7)? as u64,
                                segment_okh: okh,
                                segment_vid: Self::parse_generation_id(
                                    row.get::<_, i64>(9)?,
                                    9,
                                    "segment_vid",
                                )?,
                                data_pg_id: row.get(10)?,
                                placement_cluster_epoch: Self::parse_cluster_epoch(
                                    row.get::<_, i64>(11)?,
                                    11,
                                    "placement_cluster_epoch",
                                )?,
                                ec_k: row.get(12)?,
                                ec_m: row.get(13)?,
                            })
                        },
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (query displaced segments)",
                        source: e,
                    })?;

                let mut displaced = Vec::new();
                for row in rows {
                    displaced.push(row.map_err(|e| MetadataError::Db {
                        context: "commit stream part (read displaced segment row)",
                        source: e,
                    })?);
                }
                displaced
            };

            // 4. Delete prior part segments for this upload+part (re-upload support).
            //    Scoped by upload_id to avoid clobbering concurrent uploads for the same key.
            self.conn
                .execute(
                    "DELETE FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 \
                     AND part_number = ?4",
                    params![sess_bucket, sess_key, part.upload_id, part.part_number],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete prior segments)",
                    source: e,
                })?;

            // 5. Insert committed multipart part segment rows.
            {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "INSERT INTO multipart_part_segments \
                         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                          segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare insert segments)",
                        source: e,
                    })?;
                for segment in segments {
                    if segment.bucket != sess_bucket
                        || segment.key != sess_key
                        || segment.upload_id != part.upload_id
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                        || segment.part_number != part.part_number
                    {
                        return Err(MetadataError::StreamSessionNotFound {
                            session_id: session_id.as_str().to_owned(),
                        });
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
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
                        context: "commit stream part (insert segment)",
                        source: e,
                    })?;
                }
            }

            // 5. Delete staging rows.
            self.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete staging)",
                    source: e,
                })?;

            Ok(displaced_segments)
        })();

        match result {
            Ok(displaced_segments) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream part (commit txn)",
                        source: e,
                    });
                }
                Ok(displaced_segments)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                 data_pg_id, placement_cluster_epoch, ec_k, ec_m FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get stream object segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, version_id.to_u64() as i64], |row| {
                let okh_blob: Vec<u8> = row.get(6)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 6)?;
                Ok(ObjectSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    version_id: PgStore::parse_version_id(row.get::<_, i64>(2)?, 2)?,
                    segment_index: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    segment_crc64: row.get::<_, i64>(5)? as u64,
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(7)?,
                        7,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(8)?,
                    placement_cluster_epoch: Self::parse_cluster_epoch(
                        row.get::<_, i64>(9)?,
                        9,
                        "placement_cluster_epoch",
                    )?,
                    ec_k: row.get(10)?,
                    ec_m: row.get(11)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get stream object segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get stream object segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    #[cfg(test)]
    fn delete_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_segments_direct(bucket, key, version_id)
    }

    fn get_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                 segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND part_number = ?4 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get multipart part segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64, part_number],
                |row| {
                    let okh_blob: Vec<u8> = row.get(8)?;
                    let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                    Ok(MultipartPartSegmentRecord {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        upload_id: row.get(2)?,
                        version_id: row.get::<_, i64>(3)? as u64,
                        part_number: row.get(4)?,
                        segment_index: row.get(5)?,
                        size: row.get::<_, i64>(6)? as u64,
                        segment_crc64: row.get::<_, i64>(7)? as u64,
                        segment_okh: okh,
                        segment_vid: Self::parse_generation_id(
                            row.get::<_, i64>(9)?,
                            9,
                            "segment_vid",
                        )?,
                        data_pg_id: row.get(10)?,
                        placement_cluster_epoch: Self::parse_cluster_epoch(
                            row.get::<_, i64>(11)?,
                            11,
                            "placement_cluster_epoch",
                        )?,
                        ec_k: row.get(12)?,
                        ec_m: row.get(13)?,
                    })
                },
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get multipart part segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    fn get_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                 size, segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get multipart part segments for upload part",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, upload_id, part_number], |row| {
                let okh_blob: Vec<u8> = row.get(8)?;
                let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                Ok(MultipartPartSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    upload_id: row.get(2)?,
                    version_id: row.get::<_, i64>(3)? as u64,
                    part_number: row.get(4)?,
                    segment_index: row.get(5)?,
                    size: row.get::<_, i64>(6)? as u64,
                    segment_crc64: row.get::<_, i64>(7)? as u64,
                    segment_okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(9)?,
                        9,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(10)?,
                    placement_cluster_epoch: Self::parse_cluster_epoch(
                        row.get::<_, i64>(11)?,
                        11,
                        "placement_cluster_epoch",
                    )?,
                    ec_k: row.get(12)?,
                    ec_m: row.get(13)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments for upload part",
                source: e,
            })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments for upload part row",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_part_segments_direct(bucket, key, version_id)
    }

    fn get_all_multipart_part_segments_for_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                 size, segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 \
                 ORDER BY part_number, segment_index",
            )
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![upload_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(8)?;
                let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                Ok(MultipartPartSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    upload_id: row.get(2)?,
                    version_id: row.get::<_, i64>(3)? as u64,
                    part_number: row.get(4)?,
                    segment_index: row.get(5)?,
                    size: row.get::<_, i64>(6)? as u64,
                    segment_crc64: row.get::<_, i64>(7)? as u64,
                    segment_okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(9)?,
                        9,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(10)?,
                    placement_cluster_epoch: Self::parse_cluster_epoch(
                        row.get::<_, i64>(11)?,
                        11,
                        "placement_cluster_epoch",
                    )?,
                    ec_k: row.get(12)?,
                    ec_m: row.get(13)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (collect)",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_multipart_part_segments_by_upload_id(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_part_segments_by_upload_id_direct(upload_id)
    }
}

impl PgStore {
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_insert_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        lease_deadline: Option<u64>,
    ) -> Result<(), MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim incarnation",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim lease deadline",
                source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
            })?;
        self.conn
            .execute(
                "INSERT INTO lifecycle_sweep_claims \
                 (bucket, bucket_incarnation_generation, claim_id, owner_token, cluster_epoch, \
                  pg_id, claimed_at, heartbeat_at, lease_deadline, attempt_count, last_error) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, ?7, 1, NULL)",
                params![
                    bucket,
                    bucket_incarnation_generation,
                    claim_id,
                    owner_token,
                    cluster_epoch.get(),
                    self.pg_id,
                    lease_deadline,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim",
                source,
            })?;
        Ok(())
    }

    fn delete_object_generation_reservation_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                params![reservation_id.as_str(), bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object generation reservation",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_segments_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object segments reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_parts_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object parts",
                source: e,
            })?;
        Ok(())
    }

    fn set_stream_upload_state_direct(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        let current: u8 = self
            .conn
            .query_row(
                "SELECT state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload state",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })?;

        if current != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state: current });
        }

        self.conn
            .execute(
                "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                params![new_state as u8, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set stream upload state",
                source: e,
            })?;
        Ok(())
    }

    fn delete_stream_upload_direct(&self, session_id: &SessionId) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload",
                source: e,
            })?;
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload generation reservation",
                source: e,
            })?;
        Ok(())
    }

    fn append_stream_segment_direct(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                 (session_id, segment_index, size, segment_crc64, payload_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    segment.session_id,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64 as i64,
                    segment.payload_crc64 as i64,
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.data_pg_id,
                    segment.placement_cluster_epoch.get() as i64,
                    segment.ec_k,
                    segment.ec_m,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "append stream segment",
                source: e,
            })?;
        Ok(())
    }

    fn advance_stream_segment_vid_floor(
        &self,
        session_id: &SessionId,
        segment_vid: GenerationId,
    ) -> Result<(), MetadataError> {
        let next_vid = segment_vid
            .get()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Db {
                context: "advance stream segment VID floor overflow",
                source: rusqlite::Error::InvalidQuery,
            })?;
        self.conn
            .execute(
                "UPDATE stream_uploads \
                 SET next_segment_vid = CASE \
                     WHEN next_segment_vid < ?1 THEN ?1 \
                     ELSE next_segment_vid \
                 END \
                 WHERE session_id = ?2",
                params![next_vid as i64, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "advance stream segment VID floor",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream object segments",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_by_upload_id_direct(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments by upload_id",
                source: e,
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
