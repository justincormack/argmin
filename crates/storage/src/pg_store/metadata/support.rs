#[cfg(any(test, feature = "test-hooks"))]
fn require_one_test_mutation(changed: usize) -> Result<(), StoreError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StoreError::IntegrityError {
            expected: 1,
            actual: changed as u64,
        })
    }
}

fn combined_stream_segment_payload_crc64(segments: &[StreamUploadSegmentRecord]) -> u64 {
    segments
        .iter()
        .fold(checksum::crc64::checksum(&[]), |crc64, segment| {
            checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
        })
}

impl PgStore {
    #[cfg(test)]
    pub(crate) fn fail_next_metadata_txn_commit(&self) {
        self.fail_next_metadata_txn_commit
            .store(true, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_object_payload_reclaim_claim_effect_check_hook(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) {
        *self
            .before_object_payload_reclaim_claim_effect_check
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(hook));
    }

    #[cfg(test)]
    fn maybe_run_before_object_payload_reclaim_claim_effect_check_hook(&self) {
        let hook = self
            .before_object_payload_reclaim_claim_effect_check
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_age_noncurrent_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE objects SET became_noncurrent_at = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 \
               AND status = 0 AND became_noncurrent_at IS NOT NULL",
            params![became_noncurrent_at, bucket, key, version_id.to_u64()],
            "age noncurrent live object for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_force_stream_upload_created_at(
        &self,
        session_id: &SessionId,
        created_at: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE stream_uploads SET created_at = ?1 WHERE session_id = ?2",
            params![created_at as i64, session_id],
            "force stream upload created_at for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_force_stream_upload_cleanup_after(
        &self,
        session_id: &SessionId,
        cleanup_after: Option<u64>,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE stream_uploads SET cleanup_after = ?1 WHERE session_id = ?2",
            params![cleanup_after.map(|deadline| deadline as i64), session_id],
            "force stream upload cleanup_after for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_force_stream_upload_next_segment_vid(
        &self,
        session_id: &SessionId,
        next_segment_vid: GenerationId,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
            params![next_segment_vid.get() as i64, session_id],
            "force stream upload next segment VID for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_force_multipart_upload_object_generation(
        &self,
        upload_id: &UploadId,
        generation_id: GenerationId,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE multipart_uploads SET object_generation_id = ?1 WHERE upload_id = ?2",
            params![generation_id.get() as i64, upload_id],
            "force multipart upload object generation for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_force_multipart_upload_initiated_object_identity(
        &self,
        upload_id: &UploadId,
        identity: MultipartObjectIdentity,
    ) -> Result<(), StoreError> {
        let (kind, version_id, generation_or_sequence) = match identity {
            MultipartObjectIdentity::Live {
                version_id,
                generation_id,
            } => (1_i64, version_id.to_u64(), generation_id.get()),
            MultipartObjectIdentity::DeleteMarker {
                version_id,
                write_sequence,
            } => (2_i64, version_id.to_u64(), write_sequence),
        };
        let changed = self.execute_cached(
            "UPDATE multipart_uploads SET initiated_object_kind = ?1, initiated_object_version_id = ?2, initiated_object_generation_or_write_sequence = ?3 WHERE upload_id = ?4",
            params![
                kind,
                version_id as i64,
                generation_or_sequence as i64,
                upload_id,
            ],
            "force multipart upload initiated object identity for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_exact_live_object_segment_subject_exists_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        generation_id: GenerationId,
    ) -> Result<bool, MetadataError> {
        self.query_row_cached_metadata(
            "SELECT EXISTS( \
                 SELECT 1 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND generation_id = ?4 AND status = ?5 \
                   AND write_sequence = ( \
                       SELECT MAX(write_sequence) FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                   ) \
             )",
            params![
                bucket,
                key,
                version_id.to_u64() as i64,
                generation_id.get() as i64,
                ObjectState::Live as u8,
            ],
            "validate exact live object segment subject for test",
            |row| row.get::<_, i64>(0).map(|value| value != 0),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_set_exact_live_object_segment_data_pg(
        &self,
        segment: &ObjectSegmentRecord,
        generation_id: GenerationId,
        replacement_data_pg_id: u32,
    ) -> Result<bool, MetadataError> {
        self.with_immediate_txn(
            "set exact live object segment data PG for test (begin txn)",
            "set exact live object segment data PG for test (commit txn)",
            |store| {
                if !store.test_exact_live_object_segment_subject_exists_in_open_txn(
                    &segment.bucket,
                    &segment.key,
                    segment.version_id,
                    generation_id,
                )? {
                    return Ok(false);
                }
                let changed = store.execute_cached_metadata(
                    "UPDATE object_segments SET data_pg_id = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 \
                       AND segment_index = ?5 AND data_pg_id = ?6",
                    params![
                        replacement_data_pg_id,
                        segment.bucket,
                        segment.key,
                        segment.version_id.to_u64() as i64,
                        segment.segment_index,
                        segment.data_pg_id,
                    ],
                    "set exact live object segment data PG for test",
                )?;
                Ok(changed == 1)
            },
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_set_exact_live_object_segment_crc64(
        &self,
        segment: &ObjectSegmentRecord,
        generation_id: GenerationId,
        replacement_crc64: u64,
    ) -> Result<bool, MetadataError> {
        self.with_immediate_txn(
            "set exact live object segment CRC64 for test (begin txn)",
            "set exact live object segment CRC64 for test (commit txn)",
            |store| {
                if !store.test_exact_live_object_segment_subject_exists_in_open_txn(
                    &segment.bucket,
                    &segment.key,
                    segment.version_id,
                    generation_id,
                )? {
                    return Ok(false);
                }
                let changed = store.execute_cached_metadata(
                    "UPDATE object_segments SET segment_crc64 = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 \
                       AND segment_index = ?5 AND segment_crc64 = ?6",
                    params![
                        replacement_crc64 as i64,
                        segment.bucket,
                        segment.key,
                        segment.version_id.to_u64() as i64,
                        segment.segment_index,
                        segment.segment_crc64 as i64,
                    ],
                    "set exact live object segment CRC64 for test",
                )?;
                Ok(changed == 1)
            },
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_corrupt_object_part_payload_crc64(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE object_parts SET payload_crc64 = ~payload_crc64 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND part_number = ?4",
            params![bucket, key, version_id.to_u64() as i64, part_number],
            "corrupt object part payload CRC64 for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_remove_object_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "DELETE FROM object_parts WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND part_number = ?4",
            params![bucket, key, version_id.to_u64() as i64, part_number],
            "remove object part for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_insert_object_segment(
        &self,
        segment: &ObjectSegmentRecord,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "INSERT INTO object_segments (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
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
            ],
            "insert object segment for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_insert_multipart_part_segments(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        self.insert_multipart_part_segments_explicit(segments)
    }

    #[cfg(test)]
    pub(crate) fn test_force_object_last_modified(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        last_modified: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE objects SET last_modified = ?1 WHERE bucket = ?2 AND key = ?3",
            params![last_modified as i64, bucket, key],
            "force object last-modified time for test",
        )?;
        if changed == 0 {
            return Err(StoreError::IntegrityError {
                expected: 1,
                actual: 0,
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_set_bucket_public_read(
        &self,
        bucket: &BucketName,
        public_read: bool,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE buckets SET public_read = ?1 WHERE name = ?2",
            params![public_read, bucket],
            "force bucket public-read state for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_bucket_incarnation_generation(
        &self,
        bucket: &BucketName,
        generation: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE buckets SET bucket_incarnation_generation = ?1 WHERE name = ?2",
            params![generation as i64, bucket],
            "force bucket incarnation generation for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_bucket_write_reservation_lease_deadline(
        &self,
        reservation_id: &str,
        lease_deadline: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE bucket_write_reservations SET lease_deadline = ?1 WHERE reservation_id = ?2",
            params![lease_deadline as i64, reservation_id],
            "force bucket write reservation lease deadline for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_delete_bucket_row(&self, bucket: &BucketName) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "DELETE FROM buckets WHERE name = ?1",
            params![bucket],
            "delete bucket row for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_bucket_execution_generation(
        &self,
        generation: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE pg_counters SET next_bucket_execution_generation = ?1 WHERE singleton = 0",
            params![generation as i64],
            "force bucket execution generation for test",
        )?;
        require_one_test_mutation(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_delete_multipart_part_segments_for_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<u64, StoreError> {
        let changed = self.execute_cached(
            "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
            params![upload_id],
            "delete multipart part segments for test",
        )?;
        Ok(changed as u64)
    }

    #[cfg(test)]
    pub(crate) fn test_bucket_execution_generation(&self) -> Result<u64, MetadataError> {
        self.query_row_cached_metadata(
            "SELECT next_bucket_execution_generation FROM pg_counters WHERE singleton = 0",
            [],
            "inspect bucket execution generation for test",
            |row| row.get::<_, i64>(0),
        )?
        .try_into()
        .map_err(|_| MetadataError::Db {
            context: "decode bucket execution generation for test",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("negative next_bucket_execution_generation"),
            ),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_object_version_counter_rows_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, MetadataError> {
        let count = self.query_row_cached_metadata(
            "SELECT COUNT(*) FROM object_version_counters WHERE bucket = ?1",
            params![bucket],
            "inspect object version counter rows for test",
            |row| row.get::<_, i64>(0),
        )?;
        count.try_into().map_err(|source| MetadataError::Db {
            context: "decode object version counter row count for test",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_object_version_counter(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<u64>, MetadataError> {
        self.query_row_cached_optional_metadata(
            "SELECT next_version_id FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            "inspect object version counter for test",
            |row| row.get::<_, i64>(0),
        )?
        .map(|raw| {
            raw.try_into().map_err(|_| MetadataError::Db {
                context: "decode object version counter for test",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative next_version_id"),
                ),
            })
        })
        .transpose()
    }

    #[cfg(test)]
    pub(crate) fn test_insert_listing_objects(
        &self,
        bucket: &BucketName,
        keys: &[ObjectKey],
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "test listing objects (begin txn)",
            "test listing objects (commit txn)",
            |store| {
                let owner = OwnerIdentity::from_principal("listing-scale-test-owner");
                let mut statement = store
                    .conn
                    .prepare_cached(
                        "INSERT INTO objects \
                         (bucket, key, version_id, write_sequence, generation_id, size, etag, \
                          etag_kind, last_modified, ec_k, ec_m, owner_principal, \
                          owner_canonical_id) \
                         VALUES (?1, ?2, 0, ?3, ?3, 0, ?4, 0, 1, 1, 0, ?5, ?6)",
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "prepare test listing objects",
                        source: source.into(),
                    })?;
                for (index, key) in keys.iter().enumerate() {
                    let sequence =
                        i64::try_from(index + 1).map_err(|source| MetadataError::Db {
                            context: "convert test listing object sequence",
                            source: crate::error::DatabaseError::to_sql_conversion_failure(
                                Box::new(source),
                            ),
                        })?;
                    statement
                        .execute(params![
                            bucket,
                            key,
                            sequence,
                            [0_u8; 8].as_slice(),
                            &owner.principal,
                            owner.canonical_id.as_str(),
                        ])
                        .map_err(|source| MetadataError::Db {
                            context: "insert test listing object",
                            source: source.into(),
                        })?;
                }
                Ok(())
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn test_insert_listing_multipart_uploads(
        &self,
        bucket: &BucketName,
        uploads: &[(ObjectKey, UploadId)],
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "test listing multipart uploads (begin txn)",
            "test listing multipart uploads (commit txn)",
            |store| {
                let owner = OwnerIdentity::from_principal("listing-scale-test-owner");
                let mut statement = store
                    .conn
                    .prepare_cached(
                        "INSERT INTO multipart_uploads \
                         (upload_id, bucket, key, initiated_at, metadata_blob, \
                          system_metadata_blob, owner_principal, owner_canonical_id, \
                          initiator_principal, initiator_canonical_id, object_generation_id, \
                          listing_cluster_epoch, listing_log_index) \
                         VALUES (?1, ?2, ?3, 1, X'', X'', ?5, ?6, ?5, ?6, ?4, 1, ?4)",
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "prepare test listing multipart uploads",
                        source: source.into(),
                    })?;
                for (index, (key, upload_id)) in uploads.iter().enumerate() {
                    let sequence =
                        i64::try_from(index + 1).map_err(|source| MetadataError::Db {
                            context: "convert test listing multipart upload sequence",
                            source: crate::error::DatabaseError::to_sql_conversion_failure(
                                Box::new(source),
                            ),
                        })?;
                    statement
                        .execute(params![
                            upload_id,
                            bucket,
                            key,
                            sequence,
                            &owner.principal,
                            owner.canonical_id.as_str(),
                        ])
                        .map_err(|source| MetadataError::Db {
                            context: "insert test listing multipart upload",
                            source: source.into(),
                        })?;
                }
                Ok(())
            },
        )
    }

    fn store_error_as_metadata_db(context: &'static str, error: StoreError) -> MetadataError {
        match error {
            StoreError::Db { source, .. } => MetadataError::Db { context, source },
            other => MetadataError::Db {
                context,
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                    std::io::Error::other(other.to_string()),
                )),
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
                source: source.into(),
            })?;
        let result = body(self);
        match result {
            Ok(value) => {
                self.commit_immediate_txn(commit_context)?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn commit_immediate_txn(&self, context: &'static str) -> Result<(), MetadataError> {
        #[cfg(test)]
        if self
            .fail_next_metadata_txn_commit
            .swap(false, Ordering::Relaxed)
        {
            let _ = self.conn.execute_batch("ROLLBACK");
            self.invalidate_clean_metadata_digest_revision();
            return Err(MetadataError::Db {
                context,
                source: crate::error::DatabaseError::new(
                    "injected metadata transaction commit failure",
                ),
            });
        }

        if let Err(source) = self.conn.execute_batch("COMMIT") {
            let _ = self.conn.execute_batch("ROLLBACK");
            self.invalidate_clean_metadata_digest_revision();
            return Err(MetadataError::Db {
                context,
                source: source.into(),
            });
        }
        Ok(())
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
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("negative next_bucket_execution_generation"),
                    ),
                })
            })?;
        current.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "increment next bucket execution generation candidate",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
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
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
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

    fn validate_bucket_object_lock_transition(
        versioning: BucketVersioningState,
        current: BucketObjectLockConfig,
        target: BucketObjectLockConfig,
        context: &'static str,
    ) -> Result<(), MetadataError> {
        if !target.enabled && target.default_retention.is_some() {
            return Err(MetadataError::InvariantViolation {
                context,
                reason: "bucket object lock defaults require object lock enabled".into(),
            });
        }
        if target.enabled && versioning != BucketVersioningState::Enabled {
            return Err(MetadataError::InvariantViolation {
                context,
                reason: "bucket object lock requires enabled versioning".into(),
            });
        }
        if current.enabled && !target.enabled {
            return Err(MetadataError::InvariantViolation {
                context,
                reason: "bucket object lock cannot be disabled once enabled".into(),
            });
        }
        Ok(())
    }


}
