// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

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
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(
                params![owner_canonical_id, BucketState::Active as u8],
                Self::row_to_bucket_info,
            )
            .map_err(|e| MetadataError::Db {
                context: "list buckets query",
                source: e.into(),
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets row",
                source: e.into(),
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
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(params![BucketState::Active as u8], Self::row_to_bucket_info)
            .map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle query",
                source: e.into(),
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle row",
                source: e.into(),
            })?);
        }
        Ok(buckets)
    }

    fn list_aborting_multipart_upload_bucket_witnesses(
        &self,
    ) -> Result<Vec<crate::types::AbortingMultipartUploadBucketWitness>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_aborting_multipart_upload_bucket_witnesses",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, MIN(key) FROM multipart_uploads \
                 WHERE state = ?1 GROUP BY bucket ORDER BY bucket ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list aborting multipart upload bucket witnesses",
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(params![UploadState::Aborting as u8], |row| {
                Ok(crate::types::AbortingMultipartUploadBucketWitness {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list aborting multipart upload bucket witnesses query",
                source: e.into(),
            })?;

        let mut witnesses = Vec::new();
        for row in rows {
            witnesses.push(row.map_err(|e| MetadataError::Db {
                context: "list aborting multipart upload bucket witnesses row",
                source: e.into(),
            })?);
        }
        Ok(witnesses)
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
                        source: source.into(),
                    })?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    fn acquire_durable_bucket_write_reservation(
        &self,
        acquire: DurableBucketWriteReservationAcquire<'_>,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        let created_at = i64::try_from(acquire.created_at).map_err(|source| MetadataError::Db {
            context: "acquire durable bucket write reservation created_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline =
            i64::try_from(acquire.lease_deadline).map_err(|source| MetadataError::Db {
                context: "acquire durable bucket write reservation lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                    acquire.reservation_id,
                    acquire.owner_token,
                    acquire.cluster_epoch.get(),
                    acquire.operation_kind,
                    created_at,
                    lease_deadline,
                    acquire.target_context,
                    acquire.name.as_str(),
                    BucketState::Active as u8,
                    BucketWriteDrainState::Draining as u8,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "acquire durable bucket write reservation",
                source: source.into(),
            })?;

        if inserted == 0 {
            if let Some(existing) =
                self.durable_bucket_write_reservation(acquire.name, acquire.reservation_id)?
            {
                if existing.owner_token == acquire.owner_token
                    && existing.cluster_epoch == acquire.cluster_epoch
                    && existing.operation_kind == acquire.operation_kind
                    && existing.created_at == created_at as u64
                    && existing.lease_deadline == lease_deadline as u64
                    && existing.target_context.as_deref() == acquire.target_context
                {
                    return Ok(existing);
                }
                return Err(MetadataError::BucketWriteReservationConflict {
                    reservation_id: acquire.reservation_id.to_string(),
                });
            }
            let info = self.head_bucket_raw(acquire.name)?;
            if info.state == BucketState::Active
                && self.durable_bucket_write_drain(acquire.name)?.is_some()
            {
                return Err(MetadataError::BucketWriteDraining);
            }
            return Err(bucket_not_found(acquire.name.as_str()));
        }

        self.durable_bucket_write_reservation(acquire.name, acquire.reservation_id)?
            .ok_or_else(|| MetadataError::BucketWriteReservationNotFound {
                reservation_id: acquire.reservation_id.to_string(),
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
                source: source.into(),
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
                source: source.into(),
            })?;
        let rows = stmt
            .query_map(params![name.as_str()], bucket_write_reservation_from_row)
            .map_err(|source| MetadataError::Db {
                context: "list durable bucket write reservations",
                source: source.into(),
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| MetadataError::Db {
                context: "collect durable bucket write reservations",
                source: source.into(),
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
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
                }
            })?;
        let incarnation =
            i64::try_from(heartbeat.bucket_incarnation_generation).map_err(|source| {
                MetadataError::Db {
                    context: "heartbeat durable bucket write reservation incarnation",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
                }
            })?;
        let lease_deadline =
            i64::try_from(heartbeat.lease_deadline).map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write reservation lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let current_lease_deadline =
            i64::try_from(heartbeat.current_lease_deadline).map_err(|source| {
                MetadataError::Db {
                    context: "heartbeat durable bucket write reservation current lease deadline",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
                }
            })?;
        let now = i64::try_from(heartbeat.now).map_err(|source| MetadataError::Db {
            context: "heartbeat durable bucket write reservation now",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let updated = self
            .conn
            .execute(
                "UPDATE bucket_write_reservations \
                 SET lease_deadline = ?8 \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6 \
                   AND lease_deadline = ?7 AND lease_deadline > ?9",
                params![
                    heartbeat.name.as_str(),
                    heartbeat.reservation_id,
                    heartbeat.owner_token,
                    heartbeat.cluster_epoch.get(),
                    generation,
                    incarnation,
                    current_lease_deadline,
                    lease_deadline,
                    now,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write reservation",
                source: source.into(),
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
        record: &BucketWriteReservationRecord,
    ) -> Result<(), MetadataError> {
        let name = &record.bucket;
        let reservation_id = record.reservation_id.as_str();
        let owner_token = record.owner_token.as_str();
        let cluster_epoch = record.cluster_epoch;
        let bucket_execution_generation = record.bucket_execution_generation;
        let bucket_incarnation_generation = record.bucket_incarnation_generation;
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation generation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let incarnation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation incarnation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let lease_deadline =
            i64::try_from(record.lease_deadline).map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let deleted = self
            .conn
            .execute(
                "DELETE FROM bucket_write_reservations \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6 \
                   AND lease_deadline = ?7",
                params![
                    name.as_str(),
                    reservation_id,
                    owner_token,
                    cluster_epoch.get(),
                    generation,
                    incarnation,
                    lease_deadline,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "release durable bucket write reservation",
                source: source.into(),
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
        proof: &BucketWriteReservationProof,
    ) -> Result<(), MetadataError> {
        let name = &proof.bucket;
        let reservation_id = proof.reservation_id.as_str();

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "begin release metadata command bucket write reservation",
                source: source.into(),
            })?;

        let result =
            (|| {
                let existing = self.durable_bucket_write_reservation(name, reservation_id)?;
                match existing {
                    Some(record) if proof.matches_record(&record) => {
                        let generation = i64::try_from(record.bucket_execution_generation)
                            .map_err(|source| MetadataError::Db {
                                context:
                                    "release metadata command bucket write reservation generation",
                                source: crate::error::DatabaseError::to_sql_conversion_failure(
                                    Box::new(source),
                                ),
                            })?;
                        let incarnation = i64::try_from(record.bucket_incarnation_generation)
                            .map_err(|source| MetadataError::Db {
                                context:
                                    "release metadata command bucket write reservation incarnation",
                                source: crate::error::DatabaseError::to_sql_conversion_failure(
                                    Box::new(source),
                                ),
                            })?;
                        let lease_deadline =
                            i64::try_from(record.lease_deadline).map_err(|source| {
                                MetadataError::Db {
                            context:
                                "release metadata command bucket write reservation lease deadline",
                            source: crate::error::DatabaseError::to_sql_conversion_failure(
                                Box::new(source),
                            ),
                        }
                            })?;
                        let deleted = self
                            .conn
                            .execute(
                                "DELETE FROM bucket_write_reservations \
                             WHERE bucket_name = ?1 AND reservation_id = ?2 \
                               AND owner_token = ?3 AND cluster_epoch = ?4 \
                               AND bucket_execution_generation = ?5 \
                               AND bucket_incarnation_generation = ?6 \
                               AND lease_deadline = ?7",
                                params![
                                    name.as_str(),
                                    reservation_id,
                                    record.owner_token,
                                    record.cluster_epoch.get(),
                                    generation,
                                    incarnation,
                                    lease_deadline,
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context:
                                    "release metadata command durable bucket write reservation",
                                source: source.into(),
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
            Ok(()) => self
                .commit_immediate_txn("commit release metadata command bucket write reservation"),
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
        lease_deadline: u64,
    ) -> Result<BucketWriteDrainRecord, MetadataError> {
        let created_at = i64::try_from(created_at).map_err(|source| MetadataError::Db {
            context: "begin durable bucket write drain created_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
            context: "begin durable bucket write drain lease_deadline",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                source: source.into(),
            })?;
        if inserted == 0 {
            if let Some(existing) = self.durable_bucket_write_drain(name)? {
                if existing.drain_id == drain_id
                    && existing.owner_token == owner_token
                    && existing.cluster_epoch == cluster_epoch
                    && existing.created_at == created_at as u64
                    && existing.lease_deadline == lease_deadline as u64
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
                source: source.into(),
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
        lease_deadline: u64,
    ) -> Result<(), MetadataError> {
        let generation =
            i64::try_from(bucket_execution_generation).map_err(|source| MetadataError::Db {
                context: "clear durable bucket write drain generation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let lease_deadline = i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
            context: "clear durable bucket write drain lease deadline",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let deleted = self
            .conn
            .execute(
                "DELETE FROM bucket_write_drains \
                 WHERE bucket_name = ?1 AND drain_id = ?2 AND owner_token = ?3 \
                   AND cluster_epoch = ?4 AND bucket_execution_generation = ?5 \
                   AND lease_deadline = ?6",
                params![
                    name.as_str(),
                    drain_id,
                    owner_token,
                    cluster_epoch.get(),
                    generation,
                    lease_deadline
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "clear durable bucket write drain",
                source: source.into(),
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
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "clear expired durable bucket write drain (begin txn)",
                source: source.into(),
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
                        source: source.into(),
                    });
                }
            }) else {
                return Ok(None);
            };
            let lease_deadline =
                i64::try_from(record.lease_deadline).map_err(|source| MetadataError::Db {
                    context: "clear expired durable bucket write drain lease deadline",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
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
                       AND lease_deadline <= ?6",
                    params![
                        name.as_str(),
                        &record.drain_id,
                        &record.owner_token,
                        record.cluster_epoch.get(),
                        i64::try_from(record.bucket_execution_generation).map_err(|source| {
                            MetadataError::Db {
                                context: "clear expired durable bucket write drain generation",
                                source: crate::error::DatabaseError::to_sql_conversion_failure(
                                    Box::new(source),
                                ),
                            }
                        })?,
                        now,
                    ],
                )
                .map_err(|source| MetadataError::Db {
                    context: "clear expired durable bucket write drain (delete drain)",
                    source: source.into(),
                })?;
            if deleted == 0 {
                return Ok(None);
            }
            Ok(Some(record))
        })();
        match result {
            Ok(record) => self
                .commit_immediate_txn("clear expired durable bucket write drain (commit txn)")
                .map(|()| record),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let lease_deadline = i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
            context: "heartbeat durable bucket write drain lease deadline",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let now = i64::try_from(now).map_err(|source| MetadataError::Db {
            context: "heartbeat durable bucket write drain now",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write drain (begin txn)",
                source: source.into(),
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
                        source: source.into(),
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
            if i64::try_from(record.lease_deadline).map_err(|source| MetadataError::Db {
                context: "heartbeat durable bucket write drain current lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                       AND lease_deadline > ?7",
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
                    source: source.into(),
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
                .commit_immediate_txn("heartbeat durable bucket write drain (commit txn)")
                .map(|()| record),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                    "bucket delete attempt outcome detail exceeds maximum length",
                )),
            });
        }
        let generation = i64::try_from(record.bucket_execution_generation).map_err(|source| {
            MetadataError::Db {
                context: "record bucket delete attempt outcome generation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            }
        })?;
        let updated_at = i64::try_from(record.updated_at).map_err(|source| MetadataError::Db {
            context: "record bucket delete attempt outcome updated_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let post_reservation_next_object_pg_id =
            record.post_reservation_next_object_pg_id.map(i64::from);
        let stream_cleanup_next_object_pg_id =
            record.stream_cleanup_next_object_pg_id.map(i64::from);
        let stream_cleanup_next_session_id_marker = record
            .stream_cleanup_next_session_id_marker
            .as_ref()
            .map(SessionId::as_str);
        let final_visibility_next_object_pg_id =
            record.final_visibility_next_object_pg_id.map(i64::from);
        let finalizer_next_object_pg_id = record.finalizer_next_object_pg_id.map(i64::from);
        self.conn
            .execute(
                "INSERT INTO bucket_delete_attempt_outcomes \
                 (bucket_name, drain_id, cluster_epoch, bucket_execution_generation, \
                  outcome, phase, detail, post_reservation_next_object_pg_id, \
                  stream_cleanup_next_object_pg_id, stream_cleanup_next_session_id_marker, \
                  stream_cleanup_aborted_uploads, final_visibility_next_object_pg_id, \
                  finalizer_next_object_pg_id, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
                 ON CONFLICT(bucket_name) DO UPDATE SET \
                   drain_id = excluded.drain_id, \
                   cluster_epoch = excluded.cluster_epoch, \
                   bucket_execution_generation = excluded.bucket_execution_generation, \
                   outcome = excluded.outcome, \
                   phase = excluded.phase, \
                   detail = excluded.detail, \
                   post_reservation_next_object_pg_id = excluded.post_reservation_next_object_pg_id, \
                   stream_cleanup_next_object_pg_id = excluded.stream_cleanup_next_object_pg_id, \
                   stream_cleanup_next_session_id_marker = excluded.stream_cleanup_next_session_id_marker, \
                   stream_cleanup_aborted_uploads = excluded.stream_cleanup_aborted_uploads, \
                   final_visibility_next_object_pg_id = excluded.final_visibility_next_object_pg_id, \
                   finalizer_next_object_pg_id = excluded.finalizer_next_object_pg_id, \
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
                    stream_cleanup_next_object_pg_id,
                    stream_cleanup_next_session_id_marker,
                    record.stream_cleanup_aborted_uploads,
                    final_visibility_next_object_pg_id,
                    finalizer_next_object_pg_id,
                    updated_at,
                ],
            )
            .map(|_| ())
            .map_err(|source| MetadataError::Db {
                context: "record bucket delete attempt outcome",
                source: source.into(),
            })
    }

    fn bucket_delete_attempt_outcome(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, MetadataError> {
        match self.conn.query_row(
            "SELECT bucket_name, drain_id, cluster_epoch, bucket_execution_generation, \
                    outcome, phase, detail, post_reservation_next_object_pg_id, \
                    stream_cleanup_next_object_pg_id, stream_cleanup_next_session_id_marker, \
                    stream_cleanup_aborted_uploads, final_visibility_next_object_pg_id, \
                    finalizer_next_object_pg_id, updated_at \
             FROM bucket_delete_attempt_outcomes \
             WHERE bucket_name = ?1",
            params![name.as_str()],
            bucket_delete_attempt_outcome_from_row,
        ) {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(MetadataError::Db {
                context: "load bucket delete attempt outcome",
                source: source.into(),
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
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get bucket delete begin roots limit",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                source: source.into(),
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
                source: source.into(),
            })?;
        let mut roots = Vec::new();
        for row in rows {
            roots.push(row.map_err(|source| MetadataError::Db {
                context: "row get bucket delete begin roots",
                source: source.into(),
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
        summary: BucketAclSummary,
    ) -> Result<(), MetadataError> {
        self.put_bucket_acl_inner(
            name,
            acl_grants,
            summary,
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_subresource(
        &self,
        name: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_internal(name, req.kind(), req.body(), req.aux())
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
                source: e.into(),
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
                source: e.into(),
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
                    source: source.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?;

        let result: Result<(), MetadataError> = (|| match req {
            PutObjectReq::Live(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                req.validate().map_err(|err| MetadataError::Db {
                    context: "put object meta (etag/layout mismatch)",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::new(err),
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
                    source: e.into(),
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
                            source: e.into(),
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
                        req.acl_grants.to_current_storage_string(),
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
                self.commit_immediate_txn("put object meta (commit txn)")?;
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
                acl_grants.to_current_storage_string(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(format!(
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
                source: e.into(),
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
                source: e.into(),
            })?;

        let result: Result<(), MetadataError> =
            self.delete_object_version_in_open_txn(bucket, key, version_id);

        match result {
            Ok(()) => {
                self.commit_immediate_txn("delete object version (commit txn)")?;
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
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list objects query",
                source: e.into(),
            })?;

        let mut objects: Vec<StoredObject> = Vec::new();
        for row in rows {
            objects.push(row.map_err(|e| MetadataError::Db {
                context: "list objects row",
                source: e.into(),
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
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions query",
                source: e.into(),
            })?;

        let mut versions: Vec<StoredObject> = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions row",
                source: e.into(),
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
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(params![bucket, key], Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions for key query",
                source: e.into(),
            })?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions for key row",
                source: e.into(),
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(version_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "version_id overflow",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("negative next_version_id: {v}")),
                ),
            })?,
        };
        let next = next_from_rows.max(next_from_counter);
        next.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "version_id overflow",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(generation_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "generation_id overflow",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(generation_id) overflow"),
                    ),
                })?
            }
        };
        GenerationId::new(next).ok_or_else(|| MetadataError::Db {
            context: "invalid next generation id",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
                self.commit_immediate_txn("reserve object generation (commit txn)")?;
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
            source: source.into(),
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
                source: e.into(),
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
        })();

        match result {
            Ok(()) => {
                self.commit_immediate_txn("put object segments reclaim (commit txn)")?;
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
                source: e.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (collect segments)",
                source: e.into(),
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
                source: e.into(),
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
        })();

        match result {
            Ok(()) => {
                self.commit_immediate_txn("put multipart reclaim (commit txn)")?;
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
                source: e.into(),
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT part_number \
                 FROM multipart_reclaim_parts \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (prepare parts)",
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                row.get::<_, i64>(0).map(|value| value as u32)
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (query parts)",
                source: e.into(),
            })?;

        let mut parts = Vec::new();
        for row in rows {
            let part_number = row.map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (part row)",
                source: e.into(),
            })?;
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
                            source: e.into(),
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
                    source: e.into(),
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| MetadataError::Db {
                    context: "get multipart reclaim (collect part segments)",
                    source: e.into(),
                })?;

            parts.push(MultipartReclaimPartRecord {
                part_number,
                segments,
            });
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

    #[cfg(any(test, feature = "test-hooks"))]
    fn payload_reclaim_count_for_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, MetadataError> {
        self.query_row_cached_metadata(
            "SELECT
                (SELECT COUNT(*) FROM object_segments_reclaims
                 WHERE bucket = ?1 AND key = ?2)
                +
                (SELECT COUNT(*) FROM multipart_reclaims
                 WHERE bucket = ?1 AND key = ?2)",
            params![bucket, key],
            "payload reclaim count for object",
            |row| {
                let count = row.get::<_, i64>(0)?;
                usize::try_from(count).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative payload reclaim count: {count}")),
                    )
                })
            },
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn next_unreferenced_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, MetadataError> {
        self.next_generation_id(bucket, key)
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
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get bucket delete finalize roots limit",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                source: source.into(),
            })?;
        let rows = stmt
            .query_map(
                params![BucketState::Deleting as u8, limit_i64, now],
                bucket_delete_finalize_root_from_row,
            )
            .map_err(|source| MetadataError::Db {
                context: "query get bucket delete finalize roots",
                source: source.into(),
            })?;
        for row in rows {
            let root = row.map_err(|source| MetadataError::Db {
                context: "row get bucket delete finalize roots",
                source: source.into(),
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
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let limit_i64 = i64::try_from(limit).map_err(|source| MetadataError::Db {
            context: "get lifecycle sweep roots limit",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                        source: source.into(),
                    })?;
                let expired_rows = expired_stmt
                    .query_map(params![now, limit_i64], lifecycle_sweep_root_from_row)
                    .map_err(|source| MetadataError::Db {
                        context: "query get expired lifecycle sweep claim roots",
                        source: source.into(),
                    })?;
                for row in expired_rows {
                    roots.push(row.map_err(|source| MetadataError::Db {
                        context: "row get expired lifecycle sweep claim roots",
                        source: source.into(),
                    })?);
                }
                drop(expired_stmt);

                if roots.len() == limit {
                    return Ok(roots);
                }

                let remaining_limit =
                    i64::try_from(limit - roots.len()).map_err(|source| MetadataError::Db {
                        context: "get lifecycle busy roots remaining limit",
                        source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                        source: source.into(),
                    })?;
                for row in busy_rows {
                    let root = row.map_err(|source| MetadataError::Db {
                        context: "row get busy lifecycle sweep roots",
                        source: source.into(),
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
                        source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                        source: source.into(),
                    })?;
                for row in rows {
                    let root = row.map_err(|source| MetadataError::Db {
                        context: "row get lifecycle sweep roots",
                        source: source.into(),
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
        effect_fence: AdmittedRouteEffectFence,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "acquire object payload reclaim claim incarnation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire object payload reclaim claim claimed_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire object payload reclaim claim lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;

        self.with_immediate_txn(
            "acquire object payload reclaim claim (begin txn)",
            "acquire object payload reclaim claim (commit txn)",
            |store| {
                effect_fence
                    .require_valid_for(cluster_epoch)
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
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
                        source: source.into(),
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
                                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
                            },
                        )?;
                    #[cfg(test)]
                    store.maybe_run_before_object_payload_reclaim_claim_effect_check_hook();
                    effect_fence
                        .require_valid_for(cluster_epoch)
                        .map_err(|source| MetadataError::RouteEffectRejected { source })?;
                    store
                        .conn
                        .execute(
                            "DELETE FROM object_payload_reclaim_claims WHERE singleton = 0",
                            [],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "clear expired object payload reclaim claim",
                            source: source.into(),
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
                    source: source.into(),
                })?
                .is_some();
                if !root_exists {
                    return Ok(None);
                }

                #[cfg(test)]
                store.maybe_run_before_object_payload_reclaim_claim_effect_check_hook();
                effect_fence
                    .require_valid_for(cluster_epoch)
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
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
                        source: source.into(),
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
                        source: source.into(),
                    })
            },
        )
    }

    fn object_payload_reclaim_claim(
        &self,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, MetadataError> {
        self.conn
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
                source: source.into(),
            })
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire bucket delete finalize claim claimed_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire bucket delete finalize claim lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                                source: source.into(),
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
                                context: "clear stale terminal bucket delete finalize claim",
                                source: source.into(),
                            })?;
                    } else if existing.lease_deadline.is_none_or(|deadline| deadline > now) {
                        return Ok(None);
                    } else {
                        attempt_count =
                            i64::try_from(existing.attempt_count.saturating_add(1)).map_err(
                                |source| MetadataError::Db {
                                    context: "acquire bucket delete finalize claim attempt_count",
                                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                                source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let claimed_at = i64::try_from(claimed_at).map_err(|source| MetadataError::Db {
            context: "acquire lifecycle sweep claim claimed_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "acquire lifecycle sweep claim lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: crate::error::DatabaseError::to_sql_conversion_failure(
                                Box::new(source),
                            ),
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
                            source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let heartbeat_at = i64::try_from(heartbeat_at).map_err(|source| MetadataError::Db {
            context: "heartbeat lifecycle sweep claim heartbeat_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "heartbeat lifecycle sweep claim lease_deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: source.into(),
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
                        source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: source.into(),
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
                        source: source.into(),
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
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                        source: source.into(),
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
                            source: source.into(),
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
        tags: &SerializedTagSet,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET tags = ?1 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = 0",
            params![tags.as_str(), bucket, key, version_id.to_u64() as i64],
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
    ) -> Result<Option<SerializedTagSet>, MetadataError> {
        let result = self
            .query_row_cached_optional_metadata(
                "SELECT tags FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
                params![bucket, key, version_id.to_u64() as i64],
                "get object tags",
                |row| {
                    row.get::<_, Option<String>>(0)?
                        .map(|xml| Self::parse_object_tags(xml, 0))
                        .transpose()
                },
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
        let command =
            CreateMultipartUploadCommand::from_request_with_bucket_write_reservation_for_test(
            req.clone(),
            object_generation_id,
            None,
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
                lease_deadline: PgStore::now_millis().saturating_add(1_000),
                target_context: Some(req.key.as_str().to_string()),
            },
        );
        self.create_multipart_upload_explicit(command.upload())
    }

    fn get_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                 system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
                 checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id, initiated_object_kind, initiated_object_version_id, initiated_object_generation_or_write_sequence \
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
                    let initiator = Self::parse_owner_identity(
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
                        tags: row
                            .get::<_, Option<String>>(5)?
                            .map(|xml| Self::parse_object_tags(xml, 5))
                            .transpose()?,
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
                        initiated_object_identity: Self::parse_multipart_object_identity(
                            row, 22, 23, 24,
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
                source: e.into(),
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
                    source: e.into(),
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
                source: e.into(),
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
                    source: e.into(),
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
                source: e.into(),
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
                    source: e.into(),
                })?;
            let deleted = self
                .conn
                .execute(
                    "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete multipart upload",
                    source: e.into(),
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
                self.commit_immediate_txn("delete multipart upload (commit txn)")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
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

        if let Some(ListMultipartUploadsPageStart::At(start_at)) = req.page_start.as_ref() {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        } else if let Some(ListMultipartUploadsPageStart::After {
            key_marker,
            upload_id_marker,
        }) = req.page_start.as_ref()
        {
            if let Some(uid_marker) = upload_id_marker.as_ref() {
                // The authenticated production upload ID carries the durable,
                // non-reusable metadata-command sequence allocated when its
                // create command was built. Use the live row when it still
                // exists and the ID claim after completion/abort removes that
                // row, preserving the same-key continuation position without a
                // terminal tombstone.
                let (fallback_epoch, fallback_log_index) =
                    MultipartUploadIdKey::listing_position(uid_marker).unwrap_or((0, 0));
                let fallback_epoch = i64::try_from(fallback_epoch).unwrap_or(0);
                let fallback_log_index = i64::try_from(fallback_log_index).unwrap_or(0);
                where_clauses.push(format!(
                    "(key > ?{km} OR (key = ?{km} AND \
                     (listing_cluster_epoch > COALESCE((SELECT listing_cluster_epoch FROM multipart_uploads \
                      WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), ?{fallback_epoch}) \
                      OR (listing_cluster_epoch = COALESCE((SELECT listing_cluster_epoch FROM multipart_uploads \
                          WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), ?{fallback_epoch}) \
                          AND listing_log_index > COALESCE((SELECT listing_log_index FROM multipart_uploads \
                              WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), ?{fallback_log_index})))))",
                    km = param_idx,
                    um = param_idx + 1,
                    bkt = param_idx + 2,
                    fallback_epoch = param_idx + 3,
                    fallback_log_index = param_idx + 4,
                ));
                params_vec.push(Box::new(key_marker.clone()));
                params_vec.push(Box::new(uid_marker.as_str().to_string()));
                params_vec.push(Box::new(req.bucket.clone()));
                params_vec.push(Box::new(fallback_epoch));
                params_vec.push(Box::new(fallback_log_index));
                param_idx += 5;
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
             checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id, initiated_object_kind, initiated_object_version_id, initiated_object_generation_or_write_sequence \
             FROM multipart_uploads \
             WHERE {where_str} \
             ORDER BY key ASC, listing_cluster_epoch ASC, listing_log_index ASC, upload_id ASC \
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
                source: e.into(),
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
                let initiator = Self::parse_owner_identity(
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
                    tags: row
                        .get::<_, Option<String>>(5)?
                        .map(|xml| Self::parse_object_tags(xml, 5))
                        .transpose()?,
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
                    initiated_object_identity: Self::parse_multipart_object_identity(
                        row, 22, 23, 24,
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
                source: e.into(),
            })?;

        let mut uploads: Vec<MultipartUploadRecord> = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart uploads row",
                source: e.into(),
            })?);
        }

        let is_truncated = uploads.len() as i64 > req.max_uploads as i64;
        if is_truncated {
            uploads.truncate(req.max_uploads as usize);
        }

        // AWS reports both next markers for the final returned upload on every
        // nonempty page, independently of IsTruncated.
        let (next_key_marker, next_upload_id_marker) =
            uploads.last().map_or((None, None), |upload| {
                (Some(upload.key.clone()), Some(upload.upload_id.clone()))
            });

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
                source: e.into(),
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
                  part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.payload_crc64 as i64,
                    part.etag,
                    part.etag_kind as u8,
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
                self.commit_immediate_txn("upsert part (commit txn)")?;
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
                    source: e.into(),
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
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert multipart part segments (begin txn)",
                source: e.into(),
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
                  part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        part.upload_id,
                        part.part_number,
                        part.generation,
                        part.size as i64,
                        part.payload_crc64 as i64,
                        part.etag,
                        part.etag_kind as u8,
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
                self.commit_immediate_txn("upsert multipart part segments (commit txn)")?;
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
                    source: e.into(),
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
                 part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
                 FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2",
                params![upload_id.as_str(), part_number],
                Self::row_to_multipart_part,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part",
                source: e.into(),
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
                source: e.into(),
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
                next_part_number_marker: Some(0),
            });
        }

        let limit = req.max_parts as i64 + 1;

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.upload_id.clone())];
        let sql = if let Some(marker) = req.part_number_marker {
            params_vec.push(Box::new(marker));
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
             part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 AND part_number > ?2 \
             ORDER BY part_number ASC LIMIT ?3"
                .to_string()
        } else {
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
             part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
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
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_multipart_part)
            .map_err(|e| MetadataError::Db {
                context: "list multipart parts query",
                source: e.into(),
            })?;

        let mut parts: Vec<MultipartPartRecord> = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart parts row",
                source: e.into(),
            })?);
        }

        let is_truncated = parts.len() as i64 > req.max_parts as i64;
        if is_truncated {
            parts.truncate(req.max_parts as usize);
        }

        // AWS always includes NextPartNumberMarker. It is zero when no parts
        // were returned and otherwise names the final part in this page,
        // independently of IsTruncated and the request marker.
        let next_part_number_marker = Some(parts.last().map_or(0, |part| part.part_number));

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
                source: e.into(),
            })?;

        let result: Result<(), rusqlite::Error> = (|| {
            let mut stmt = self.conn.prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, etag, etag_kind, \
                  part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                self.commit_immediate_txn("commit object parts (commit txn)")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "commit object parts",
                    source: e.into(),
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
                 part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts",
                source: e.into(),
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64],
                Self::row_to_object_part,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts query",
                source: e.into(),
            })?;

        let mut parts = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts row",
                source: e.into(),
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
                 part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND object_offset_start <= ?4 \
                   AND object_offset_start + size > ?4 \
                 ORDER BY object_offset_start DESC, part_number ASC \
                 LIMIT 1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range first part",
                source: e.into(),
            })?;

        let first = first_stmt
            .query_row(
                params![bucket, key, version_id.to_u64() as i64, start as i64],
                Self::row_to_object_part_range,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range first part",
                source: e.into(),
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
                 part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND part_number > ?4 \
                   AND object_offset_start < ?5 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range tail parts",
                source: e.into(),
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
                source: e.into(),
            })?;

        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail row",
                source: e.into(),
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
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<CompleteMultipartCommitCleanup, MetadataError> {
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "complete multipart commit (empty parts)",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
                     part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum \
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
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            self.advance_object_write_counter_in_open_txn(
                &obj.bucket,
                &obj.key,
                write_sequence,
                Some(obj.generation_id),
            )
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

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
                    obj.acl_grants.to_current_storage_string(),
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
                      part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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

            // 8. Release the durable generation reservation now that the
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

            // 9. Delete in-progress upload + parts (CASCADE).
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
                self.commit_immediate_txn("complete multipart commit (commit txn)")?;
                Ok(cleanup)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "complete multipart commit",
                    source: e.into(),
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
        self.create_stream_upload_explicit(&session, GenerationId::MIN, None, None)
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
                source: e.into(),
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })
    }

    fn update_stream_upload_bucket_write_reservation(
        &self,
        session_id: &SessionId,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
    ) -> Result<(), MetadataError> {
        if !current.has_same_stable_identity(renewed) {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: current.reservation_id.clone(),
            });
        }

        let current_cluster_epoch =
            i64::try_from(current.cluster_epoch.get()).map_err(|source| MetadataError::Db {
                context: "update stream upload bucket write reservation current cluster epoch",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let current_execution_generation = i64::try_from(current.bucket_execution_generation)
            .map_err(|source| MetadataError::Db {
                context:
                    "update stream upload bucket write reservation current execution generation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let current_incarnation_generation = i64::try_from(current.bucket_incarnation_generation)
            .map_err(|source| MetadataError::Db {
            context: "update stream upload bucket write reservation current incarnation generation",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let current_created_at =
            i64::try_from(current.created_at).map_err(|source| MetadataError::Db {
                context: "update stream upload bucket write reservation current created_at",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let current_lease_deadline =
            i64::try_from(current.lease_deadline).map_err(|source| MetadataError::Db {
                context: "update stream upload bucket write reservation current lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let renewed_lease_deadline =
            i64::try_from(renewed.lease_deadline).map_err(|source| MetadataError::Db {
                context: "update stream upload bucket write reservation renewed lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let updated = self
            .conn
            .execute(
                "UPDATE stream_uploads \
                 SET bucket_write_lease_deadline = ?12 \
                 WHERE session_id = ?1 AND state = ?2 \
                   AND bucket = ?3 \
                   AND bucket_write_reservation_id = ?4 \
                   AND bucket_write_owner_token = ?5 \
                   AND bucket_write_cluster_epoch = ?6 \
                   AND bucket_write_execution_generation = ?7 \
                   AND bucket_write_incarnation_generation = ?8 \
                   AND bucket_write_operation_kind = ?9 \
                   AND bucket_write_created_at = ?10 \
                   AND bucket_write_lease_deadline = ?11 \
                   AND bucket_write_target_context IS ?13",
                params![
                    session_id.as_str(),
                    StreamUploadState::InProgress as u8,
                    current.bucket.as_str(),
                    current.reservation_id.as_str(),
                    current.owner_token.as_str(),
                    current_cluster_epoch,
                    current_execution_generation,
                    current_incarnation_generation,
                    current.operation_kind.as_str(),
                    current_created_at,
                    current_lease_deadline,
                    renewed_lease_deadline,
                    current.target_context.as_deref(),
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "update stream upload bucket write reservation",
                source: source.into(),
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: current.reservation_id.clone(),
            });
        }
        Ok(())
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
                source: e.into(),
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
        self.with_immediate_txn(
            "delete stream upload (begin txn)",
            "delete stream upload (commit txn)",
            |store| store.delete_stream_upload_in_open_txn(session_id),
        )
    }

    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(STREAM_UPLOAD_SELECT)
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (prepare)",
                source: e.into(),
            })?;
        let rows = stmt
            .query_map([], parse_stream_upload_record)
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (query)",
                source: e.into(),
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (collect)",
                source: e.into(),
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
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(params_vec.iter()),
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads page (query)",
                source: e.into(),
            })?;
        let mut uploads = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads page (collect)",
                source: e.into(),
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
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(params_vec.iter()),
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for bucket page (query)",
                source: e.into(),
            })?;
        let mut uploads = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for bucket page (collect)",
                source: e.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "list stream segments row",
                source: e.into(),
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
        obj.validate().map_err(|err| MetadataError::Db {
            context: "put segment object (etag/layout mismatch)",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Null,
                Box::new(err),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put segment object (non-segment layout)",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
                source: e.into(),
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
                    source: e.into(),
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
                source: e.into(),
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
                        obj.acl_grants.to_current_storage_string(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (write object)",
                    source: e.into(),
                })?;

            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (delete prior segments)",
                    source: e.into(),
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
                    source: e.into(),
                })?;
            for segment in segments {
                if segment.bucket != obj.bucket
                    || segment.key != obj.key
                    || segment.version_id != obj.version_id
                {
                    return Err(MetadataError::Db {
                        context: "put segment object (segment object mismatch)",
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
                    context: "put segment object (insert segment)",
                    source: e.into(),
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                self.commit_immediate_txn("put segment object (commit txn)")?;
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
                source: e.into(),
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
                    source: e.into(),
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
                    source: e.into(),
                })?;

            let new_gen = prev_gen.map_or(0, |g| g + 1);

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                     (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                      part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        part.upload_id,
                        part.part_number,
                        new_gen,
                        part.size as i64,
                        part.payload_crc64 as i64,
                        part.etag,
                        part.etag_kind as u8,
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
                    source: e.into(),
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
                        source: e.into(),
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
                        source: e.into(),
                    })?;

                let mut displaced = Vec::new();
                for row in rows {
                    displaced.push(row.map_err(|e| MetadataError::Db {
                        context: "commit stream part (read displaced segment row)",
                        source: e.into(),
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
                    source: e.into(),
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
                        source: e.into(),
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
                        source: e.into(),
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
                    source: e.into(),
                })?;

            Ok(displaced_segments)
        })();

        match result {
            Ok(displaced_segments) => {
                self.commit_immediate_txn("commit stream part (commit txn)")?;
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
                source: e.into(),
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
                source: e.into(),
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get stream object segments row",
                source: e.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get multipart part segments row",
                source: e.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments for upload part row",
                source: e.into(),
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
                source: e.into(),
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
                source: e.into(),
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (collect)",
                source: e.into(),
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
