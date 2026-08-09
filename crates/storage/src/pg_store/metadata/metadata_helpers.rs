// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl PgStore {
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
                source: source.into(),
            })?;
        let rows = stmt
            .query_map(
                params_from_iter(buckets.iter().map(|bucket| bucket.as_str())),
                |row| Ok((row.get::<_, BucketName>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(|source| MetadataError::Db {
                context: "query load bucket execution generations",
                source: source.into(),
            })?;
        let mut generations = HashMap::with_capacity(buckets.len());
        for row in rows {
            let (bucket, generation) = row.map_err(|source| MetadataError::Db {
                context: "row load bucket execution generations",
                source: source.into(),
            })?;
            generations.insert(
                bucket,
                generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: source.into(),
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
                source: source.into(),
            })?;
        let mut identities = HashMap::with_capacity(buckets.len());
        for row in rows {
            let (bucket, execution, incarnation) = row.map_err(|source| MetadataError::Db {
                context: "row load bucket fast path identities",
                source: source.into(),
            })?;
            let bucket_execution_generation =
                execution.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket fast path execution generation",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?;
            let bucket_incarnation_generation =
                incarnation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket fast path incarnation generation",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                        source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("version_id overflow"),
                ),
            })?;
        let following = i64::try_from(following).map_err(|_| MetadataError::Db {
            context: "advance object version counter",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
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
            return Err(MetadataError::InvariantViolation {
                context: "reserve object version command null version",
                reason: "metadata state does not satisfy the operation invariant".into(),
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
            .map_err(|e| MetadataError::Db { context: "get object write sequence",
                source: e.into(),
            })?
            .map(|value| {
                u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative write_sequence: {value}")),
                    ),
                })
            })
            .transpose()
    }

    pub(crate) fn multipart_object_identity(
        &self,
        object: &StoredObject,
    ) -> Result<MultipartObjectIdentity, MetadataError> {
        match object {
            StoredObject::Live(object) => Ok(MultipartObjectIdentity::Live {
                version_id: object.version_id,
                generation_id: object.generation_id,
            }),
            StoredObject::DeleteMarker(marker) => {
                let write_sequence = self
                    .object_write_sequence(
                        marker.bucket.as_str(),
                        marker.key.as_str(),
                        marker.version_id,
                    )?
                    .ok_or_else(|| MetadataError::Db {
                        context: "load multipart delete-marker identity write sequence",
                        source: rusqlite::Error::QueryReturnedNoRows.into(),
                    })?;
                Ok(MultipartObjectIdentity::DeleteMarker {
                    version_id: marker.version_id,
                    write_sequence,
                })
            }
        }
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

    pub(crate) fn multipart_completion_barrier_sequence_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, MetadataError> {
        let bucket = bucket.as_str();
        self.query_row_cached_metadata(
            "SELECT multipart_completion_barrier_sequence FROM buckets WHERE name = ?1",
            params![bucket],
            "read multipart completion barrier sequence",
            |row| row.get::<_, i64>(0),
        )?
        .try_into()
        .map_err(|_| MetadataError::Db {
            context: "decode multipart completion barrier sequence",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("negative multipart completion barrier sequence"),
            ),
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
                                source: source.into(),
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
                    source: source.into(),
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
                    source: source.into(),
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
                source: e.into(),
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
                    source: e.into(),
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
                source: e.into(),
            })?;

        for part in &reclaim.parts {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_reclaim_parts \
                     (bucket, key, generation_id, part_number) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        part.part_number as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put multipart reclaim (part)",
                    source: e.into(),
                })?;

            for segment in &part.segments {
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
                        source: e.into(),
                    })?;
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
        obj.validate().map_err(|err| MetadataError::Db {
            context: "put explicit segment object (etag/layout mismatch)",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Null,
                Box::new(err),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put explicit segment object (non-segment layout)",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
            source: e.into(),
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
                obj.acl_grants.to_current_storage_string(),
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
                source: e.into(),
            })?;
        for segment in segments {
            if segment.bucket != obj.bucket
                || segment.key != obj.key
                || segment.version_id != obj.version_id
            {
                return Err(MetadataError::Db {
                    context: "put explicit segment object (segment object mismatch)",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
        obj.validate().map_err(|err| MetadataError::Db {
            context: "put explicit multipart object (etag/layout mismatch)",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Null,
                Box::new(err),
            ),
        })?;
        if !matches!(obj.layout, ObjectLayout::MultipartManifest { .. }) {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (non-multipart layout)",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put multipart object requires MultipartManifest layout"),
                ),
            });
        }
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (empty parts)",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
            source: e.into(),
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
                    obj.acl_grants.to_current_storage_string(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (write object)",
                source: e.into(),
            })?;

        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![&obj.bucket, &obj.key, obj.version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (delete prior parts)",
                source: e.into(),
            })?;

        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, etag, etag_kind, \
                  part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (prepare insert parts)",
                source: e.into(),
            })?;
        let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
        ordered_parts.sort_by_key(|part| part.part_number);
        let mut object_offset_start = 0u64;
        for part in ordered_parts {
            if part.bucket != obj.bucket || part.key != obj.key || part.version_id != obj.version_id
            {
                return Err(MetadataError::InvariantViolation {
                    context: "put explicit multipart object (part identity mismatch)",
                    reason: "metadata state does not satisfy the operation invariant".into(),
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
                part.part_vid.get() as i64,
                part.placement_cluster_epoch.get() as i64,
                part.ec_k,
                part.ec_m,
                part.data_pg_id,
                part.checksum.as_ref().map(|checksum| checksum.as_slice()),
            ])
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (insert part)",
                source: e.into(),
            })?;
            object_offset_start += part.size;
        }

        Ok(())
    }
}
