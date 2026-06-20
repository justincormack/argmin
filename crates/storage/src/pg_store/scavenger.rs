use super::*;

fn shard_scavenger_observation_reason_name(
    reason: ShardScavengerObservationReason,
) -> &'static str {
    match reason {
        ShardScavengerObservationReason::FileWithoutShardRow => "file_without_shard_row",
        ShardScavengerObservationReason::ShardRowWithoutFile => "shard_row_without_file",
        ShardScavengerObservationReason::UnreferencedShardRowAndFile => {
            "unreferenced_shard_row_and_file"
        }
        ShardScavengerObservationReason::ScanIncomplete => "scan_incomplete",
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardRow {
    pub(crate) key: ShardKey,
    pub(crate) ack: WriteAck,
}

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardFile {
    pub(crate) key: ShardKey,
    pub(crate) size: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardFileScan {
    pub(crate) files: Vec<ScavengerShardFile>,
    pub(crate) errors: Vec<String>,
}

fn is_canonical_shard_prefix(prefix: &str) -> bool {
    prefix.len() == SHARD_KEY_HEX_PREFIX_LEN
        && prefix
            .as_bytes()
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

impl PgStore {
    /// Persist a placed segment shard repair candidate.
    ///
    /// The in-memory repair queue is only a wake hint. This durable row is the
    /// authoritative record that a recovered read or scrub observed a shard
    /// needing reconstruction.
    pub fn record_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_repair_work_item(work_item)?;
        validate_placed_segment_shard_repair_pg(self.pg_id(), work_item)?;
        if let Some(last_error) = last_error {
            validate_placed_segment_shard_repair_last_error(last_error)?;
        }
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT INTO placed_segment_shard_repairs \
                 (data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, ec_k, ec_m, \
                  shard_index, first_seen_at, last_seen_at, observation_count, last_error) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, 1, ?10) \
                 ON CONFLICT(segment_okh, segment_vid, shard_index) DO UPDATE SET \
                  data_pg_id = excluded.data_pg_id, \
                  stored_size = excluded.stored_size, \
                  segment_crc64 = excluded.segment_crc64, \
                  ec_k = excluded.ec_k, \
                  ec_m = excluded.ec_m, \
                  last_seen_at = excluded.last_seen_at, \
                  observation_count = placed_segment_shard_repairs.observation_count + 1, \
                  last_error = COALESCE(excluded.last_error, placed_segment_shard_repairs.last_error)",
                params![
                    work_item.request.data_pg_id as i64,
                    work_item.request.segment_okh.as_slice(),
                    work_item.request.segment_vid.get() as i64,
                    work_item.request.stored_size as i64,
                    work_item.request.segment_crc64 as i64,
                    work_item.request.ec.k as i64,
                    work_item.request.ec.m as i64,
                    work_item.shard_index.get() as i64,
                    now as i64,
                    last_error,
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "record placed segment shard repair",
                source,
            })?;
        Ok(())
    }

    pub fn list_placed_segment_shard_repairs(
        &self,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                        ec_k, ec_m, shard_index, first_seen_at, last_seen_at, observation_count, \
                        last_error \
                 FROM placed_segment_shard_repairs \
                 ORDER BY last_seen_at, segment_okh, segment_vid, shard_index \
                 LIMIT ?1",
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard repairs (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map(
                params![PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT as i64],
                |row| {
                    Ok((
                        placed_segment_shard_repair_work_item_from_row(row)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, Option<String>>(11)?,
                    ))
                },
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard repairs",
                source,
            })?;

        let mut repairs = Vec::new();
        for row in rows {
            let (work_item, first_seen_at, last_seen_at, observation_count, last_error) = row
                .map_err(|source| StoreError::Db {
                    context: "read placed segment shard repair",
                    source,
                })?;
            repairs.push(PlacedSegmentShardRepairRecord {
                work_item,
                first_seen_at: first_seen_at as u64,
                last_seen_at: last_seen_at as u64,
                observation_count: observation_count as u64,
                last_error,
            });
        }
        Ok(repairs)
    }

    pub fn resolve_placed_segment_shard_repair(
        &self,
        work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_work_item(work_item)?;
        validate_placed_segment_shard_repair_pg(self.pg_id(), work_item)?;
        let updated = self
            .conn
            .execute(
                "DELETE FROM placed_segment_shard_repairs \
                 WHERE segment_okh = ?1 AND segment_vid = ?2 AND shard_index = ?3",
                params![
                    work_item.request.segment_okh.as_slice(),
                    work_item.request.segment_vid.get() as i64,
                    work_item.shard_index.get() as i64,
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "resolve placed segment shard repair",
                source,
            })?;
        Ok(updated > 0)
    }

    /// Persist a segment-level backfill candidate between two cluster-map epochs.
    ///
    /// The row records that a scanner or planner has verified useful work for
    /// moving a segment from its historical source placement to the desired
    /// placement. The later worker reconstructs both routes from retained
    /// cluster-map history instead of treating this row as placement authority.
    pub fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_work_item(work_item)?;
        validate_placed_segment_shard_backfill_pg(self.pg_id(), work_item)?;
        if let Some(last_error) = last_error {
            validate_placed_segment_shard_backfill_last_error(last_error)?;
        }
        let now = Self::now_secs();
        self.with_durable_repair_txn(
            "record placed segment shard backfill (begin txn)",
            "record placed segment shard backfill (commit txn)",
            |store| {
                let existing = store
                    .conn
                    .query_row(
                        "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                                ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch \
                         FROM placed_segment_shard_backfills \
                         WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                           AND source_cluster_epoch = ?4 AND desired_cluster_epoch = ?5",
                        params![
                            work_item.request.data_pg_id as i64,
                            work_item.request.segment_okh.as_slice(),
                            work_item.request.segment_vid.get() as i64,
                            work_item.source_cluster_epoch.get(),
                            work_item.desired_cluster_epoch.get(),
                        ],
                        placed_segment_shard_backfill_work_item_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load existing placed segment shard backfill",
                        source,
                    })?;
                if let Some(existing) = existing {
                    validate_placed_segment_shard_backfill_coalesces_exactly(&existing, work_item)?;
                    store
                        .conn
                        .execute(
                            "UPDATE placed_segment_shard_backfills \
                             SET last_seen_at = ?6, \
                                 observation_count = observation_count + 1, \
                                 last_error = COALESCE(?7, last_error) \
                             WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                               AND source_cluster_epoch = ?4 AND desired_cluster_epoch = ?5",
                            params![
                                work_item.request.data_pg_id as i64,
                                work_item.request.segment_okh.as_slice(),
                                work_item.request.segment_vid.get() as i64,
                                work_item.source_cluster_epoch.get(),
                                work_item.desired_cluster_epoch.get(),
                                now as i64,
                                last_error,
                            ],
                        )
                        .map_err(|source| StoreError::Db {
                            context: "coalesce placed segment shard backfill",
                            source,
                        })?;
                    return Ok(());
                }
                store
                    .conn
                    .execute(
                        "INSERT INTO placed_segment_shard_backfills \
                         (data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                          ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, first_seen_at, \
                          last_seen_at, observation_count, last_error) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, 1, ?11)",
                        params![
                            work_item.request.data_pg_id as i64,
                            work_item.request.segment_okh.as_slice(),
                            work_item.request.segment_vid.get() as i64,
                            work_item.request.stored_size as i64,
                            work_item.request.segment_crc64 as i64,
                            work_item.request.ec.k as i64,
                            work_item.request.ec.m as i64,
                            work_item.source_cluster_epoch.get(),
                            work_item.desired_cluster_epoch.get(),
                            now as i64,
                            last_error,
                        ],
                    )
                    .map_err(|source| StoreError::Db {
                        context: "record placed segment shard backfill",
                        source,
                    })?;
                Ok(())
            },
        )
    }

    pub fn list_placed_segment_shard_backfills(
        &self,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                        ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, first_seen_at, \
                        last_seen_at, observation_count, last_error \
                 FROM placed_segment_shard_backfills \
                 ORDER BY last_seen_at, segment_okh, segment_vid, source_cluster_epoch, \
                          desired_cluster_epoch \
                 LIMIT ?1",
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard backfills (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map(
                params![PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT as i64],
                |row| {
                    Ok((
                        placed_segment_shard_backfill_work_item_from_row(row)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard backfills",
                source,
            })?;

        let mut backfills = Vec::new();
        for row in rows {
            let (work_item, first_seen_at, last_seen_at, observation_count, last_error) = row
                .map_err(|source| StoreError::Db {
                    context: "read placed segment shard backfill",
                    source,
                })?;
            backfills.push(PlacedSegmentShardBackfillRecord {
                work_item,
                first_seen_at: first_seen_at as u64,
                last_seen_at: last_seen_at as u64,
                observation_count: observation_count as u64,
                last_error,
            });
        }
        Ok(backfills)
    }

    pub fn placed_segment_shard_backfill_count(&self) -> Result<usize, StoreError> {
        let count = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM placed_segment_shard_backfills",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|source| StoreError::Db {
                context: "count placed segment shard backfills",
                source,
            })?;
        usize::try_from(count).map_err(|_| StoreError::Db {
            context: "count placed segment shard backfills range",
            source: rusqlite::Error::InvalidQuery,
        })
    }

    pub fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_work_item(work_item)?;
        validate_placed_segment_shard_backfill_pg(self.pg_id(), work_item)?;
        let updated = self
            .conn
            .execute(
                "DELETE FROM placed_segment_shard_backfills \
                 WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                   AND stored_size = ?4 AND segment_crc64 = ?5 AND ec_k = ?6 AND ec_m = ?7 \
                   AND source_cluster_epoch = ?8 AND desired_cluster_epoch = ?9",
                params![
                    work_item.request.data_pg_id as i64,
                    work_item.request.segment_okh.as_slice(),
                    work_item.request.segment_vid.get() as i64,
                    work_item.request.stored_size as i64,
                    work_item.request.segment_crc64 as i64,
                    work_item.request.ec.k as i64,
                    work_item.request.ec.m as i64,
                    work_item.source_cluster_epoch.get(),
                    work_item.desired_cluster_epoch.get(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "resolve placed segment shard backfill",
                source,
            })?;
        Ok(updated > 0)
    }

    pub fn acquire_placed_segment_shard_backfill_claim(
        &self,
        request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        validate_placed_segment_shard_backfill_claim_identity(
            &request.claim_id,
            &request.owner_token,
        )?;
        let Some(lease_deadline_value) = request.lease_deadline else {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "durable backfill claim lease deadline is required".to_string(),
            });
        };
        if lease_deadline_value <= request.claimed_at {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "durable backfill claim lease deadline must be after claimed_at"
                    .to_string(),
            });
        }
        let claimed_at = durable_repair_u64_to_i64(
            request.claimed_at,
            "acquire placed segment shard backfill claim claimed_at",
        )?;
        let lease_deadline = durable_repair_u64_to_i64(
            lease_deadline_value,
            "acquire placed segment shard backfill claim lease_deadline",
        )?;
        let now = durable_repair_u64_to_i64(
            request.now,
            "acquire placed segment shard backfill claim now",
        )?;

        self.with_durable_repair_txn(
            "acquire placed segment shard backfill claim (begin txn)",
            "acquire placed segment shard backfill claim (commit txn)",
            |store| {
                let existing_sql = durable_backfill_claim_select_sql(
                    "claim_id = ?1 AND owner_token = ?2 AND cluster_epoch = ?3",
                );
                if let Some(existing) = store
                    .conn
                    .query_row(
                        &existing_sql,
                        params![
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get()
                        ],
                        placed_segment_shard_backfill_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load existing placed segment shard backfill claim",
                        source,
                    })?
                {
                    return Ok(Some(existing));
                }

                let candidate = store
                    .conn
                    .query_row(
                        "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                                ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch \
                         FROM placed_segment_shard_backfills \
                         WHERE next_attempt_after <= ?1 \
                           AND (claim_id IS NULL OR (lease_deadline IS NOT NULL AND lease_deadline <= ?1)) \
                         ORDER BY last_seen_at, segment_okh, segment_vid, source_cluster_epoch, \
                                  desired_cluster_epoch \
                         LIMIT 1",
                        params![now],
                        placed_segment_shard_backfill_work_item_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load claimable placed segment shard backfill",
                        source,
                    })?;

                let Some(candidate) = candidate else {
                    return Ok(None);
                };
                store
                    .conn
                    .execute(
                        "UPDATE placed_segment_shard_backfills \
                         SET claim_id = ?10, owner_token = ?11, cluster_epoch = ?12, \
                             claimed_at = ?13, lease_deadline = ?14, \
                             attempt_count = attempt_count + 1 \
                         WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                           AND stored_size = ?4 AND segment_crc64 = ?5 AND ec_k = ?6 AND ec_m = ?7 \
                           AND source_cluster_epoch = ?8 AND desired_cluster_epoch = ?9",
                        params![
                            candidate.request.data_pg_id as i64,
                            candidate.request.segment_okh.as_slice(),
                            candidate.request.segment_vid.get() as i64,
                            candidate.request.stored_size as i64,
                            candidate.request.segment_crc64 as i64,
                            candidate.request.ec.k as i64,
                            candidate.request.ec.m as i64,
                            candidate.source_cluster_epoch.get(),
                            candidate.desired_cluster_epoch.get(),
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get(),
                            claimed_at,
                            lease_deadline,
                        ],
                    )
                    .map_err(|source| StoreError::Db {
                        context: "install placed segment shard backfill claim",
                        source,
                    })?;

                let reload_sql = durable_backfill_claim_select_sql(
                    "claim_id = ?1 AND owner_token = ?2 AND cluster_epoch = ?3",
                );
                store
                    .conn
                    .query_row(
                        &reload_sql,
                        params![
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get()
                        ],
                        placed_segment_shard_backfill_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "reload placed segment shard backfill claim",
                        source,
                    })
            },
        )
    }

    pub fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_claim_record(self.pg_id(), claim)?;
        let updated = self
            .conn
            .execute(
                "DELETE FROM placed_segment_shard_backfills \
                 WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                   AND stored_size = ?4 AND segment_crc64 = ?5 AND ec_k = ?6 AND ec_m = ?7 \
                   AND source_cluster_epoch = ?8 AND desired_cluster_epoch = ?9 \
                   AND claim_id = ?10 AND owner_token = ?11 AND cluster_epoch = ?12",
                params![
                    claim.work_item.request.data_pg_id as i64,
                    claim.work_item.request.segment_okh.as_slice(),
                    claim.work_item.request.segment_vid.get() as i64,
                    claim.work_item.request.stored_size as i64,
                    claim.work_item.request.segment_crc64 as i64,
                    claim.work_item.request.ec.k as i64,
                    claim.work_item.request.ec.m as i64,
                    claim.work_item.source_cluster_epoch.get(),
                    claim.work_item.desired_cluster_epoch.get(),
                    claim.claim_id,
                    claim.owner_token,
                    claim.cluster_epoch.get(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "complete placed segment shard backfill claim",
                source,
            })?;
        Ok(updated > 0)
    }

    pub fn acquire_placed_segment_shard_repair_claim(
        &self,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        validate_placed_segment_shard_repair_claim_identity(
            &request.claim_id,
            &request.owner_token,
        )?;
        let Some(lease_deadline_value) = request.lease_deadline else {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "durable repair claim lease deadline is required".to_string(),
            });
        };
        if lease_deadline_value <= request.claimed_at {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "durable repair claim lease deadline must be after claimed_at".to_string(),
            });
        }
        let claimed_at = durable_repair_u64_to_i64(
            request.claimed_at,
            "acquire placed segment shard repair claim claimed_at",
        )?;
        let lease_deadline = durable_repair_u64_to_i64(
            lease_deadline_value,
            "acquire placed segment shard repair claim lease_deadline",
        )?;
        let now = durable_repair_u64_to_i64(
            request.now,
            "acquire placed segment shard repair claim now",
        )?;

        self.with_durable_repair_txn(
            "acquire placed segment shard repair claim (begin txn)",
            "acquire placed segment shard repair claim (commit txn)",
            |store| {
                let existing_sql = durable_repair_claim_select_sql(
                    "claim_id = ?1 AND owner_token = ?2 AND cluster_epoch = ?3",
                );
                if let Some(existing) = store
                    .conn
                    .query_row(
                        &existing_sql,
                        params![
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get()
                        ],
                        placed_segment_shard_repair_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load existing placed segment shard repair claim",
                        source,
                    })?
                {
                    return Ok(Some(existing));
                }

                let candidate = store
                    .conn
                    .query_row(
                        "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                                ec_k, ec_m, shard_index \
                         FROM placed_segment_shard_repairs \
                         WHERE next_attempt_after <= ?1 \
                           AND (claim_id IS NULL OR (lease_deadline IS NOT NULL AND lease_deadline <= ?1)) \
                         ORDER BY last_seen_at, segment_okh, segment_vid, shard_index \
                         LIMIT 1",
                        params![now],
                        placed_segment_shard_repair_work_item_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load claimable placed segment shard repair",
                        source,
                    })?;

                let Some(candidate) = candidate else {
                    return Ok(None);
                };
                store
                    .conn
                    .execute(
                        "UPDATE placed_segment_shard_repairs \
                         SET claim_id = ?4, owner_token = ?5, cluster_epoch = ?6, \
                             claimed_at = ?7, lease_deadline = ?8, \
                             attempt_count = attempt_count + 1 \
                         WHERE segment_okh = ?1 AND segment_vid = ?2 AND shard_index = ?3",
                        params![
                            candidate.request.segment_okh.as_slice(),
                            candidate.request.segment_vid.get() as i64,
                            candidate.shard_index.get() as i64,
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get(),
                            claimed_at,
                            lease_deadline,
                        ],
                    )
                    .map_err(|source| StoreError::Db {
                        context: "install placed segment shard repair claim",
                        source,
                    })?;

                let reload_sql = durable_repair_claim_select_sql(
                    "claim_id = ?1 AND owner_token = ?2 AND cluster_epoch = ?3",
                );
                store
                    .conn
                    .query_row(
                        &reload_sql,
                        params![
                            &request.claim_id,
                            &request.owner_token,
                            request.cluster_epoch.get()
                        ],
                        placed_segment_shard_repair_claim_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "reload placed segment shard repair claim",
                        source,
                    })
            },
        )
    }

    pub fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_record(self.pg_id(), claim)?;
        let updated = self
            .conn
            .execute(
                "DELETE FROM placed_segment_shard_repairs \
                 WHERE segment_okh = ?1 AND segment_vid = ?2 AND shard_index = ?3 \
                   AND claim_id = ?4 AND owner_token = ?5 AND cluster_epoch = ?6",
                params![
                    claim.work_item.request.segment_okh.as_slice(),
                    claim.work_item.request.segment_vid.get() as i64,
                    claim.work_item.shard_index.get() as i64,
                    claim.claim_id,
                    claim.owner_token,
                    claim.cluster_epoch.get(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "complete placed segment shard repair claim",
                source,
            })?;
        Ok(updated > 0)
    }

    fn with_durable_repair_txn<T>(
        &self,
        begin_context: &'static str,
        commit_context: &'static str,
        body: impl FnOnce(&Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.conn.is_autocommit() {
            return body(self);
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: begin_context,
                source,
            })?;
        let result = body(self);
        match result {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|source| StoreError::Db {
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

    pub fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_repair_claim_record(self.pg_id(), claim)?;
        validate_placed_segment_shard_repair_last_error(last_error)?;
        let next_attempt_after = durable_repair_u64_to_i64(
            next_attempt_after,
            "record placed segment shard repair claim error next_attempt_after",
        )?;
        let updated = self
            .conn
            .execute(
                "UPDATE placed_segment_shard_repairs \
                 SET claim_id = NULL, owner_token = NULL, cluster_epoch = NULL, \
                     claimed_at = NULL, lease_deadline = NULL, \
                     next_attempt_after = ?7, last_error = ?8 \
                 WHERE segment_okh = ?1 AND segment_vid = ?2 AND shard_index = ?3 \
                   AND claim_id = ?4 AND owner_token = ?5 AND cluster_epoch = ?6",
                params![
                    claim.work_item.request.segment_okh.as_slice(),
                    claim.work_item.request.segment_vid.get() as i64,
                    claim.work_item.shard_index.get() as i64,
                    claim.claim_id,
                    claim.owner_token,
                    claim.cluster_epoch.get(),
                    next_attempt_after,
                    last_error,
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "record placed segment shard repair claim error",
                source,
            })?;
        Ok(updated > 0)
    }

    pub fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_claim_record(self.pg_id(), claim)?;
        validate_placed_segment_shard_backfill_last_error(last_error)?;
        let next_attempt_after = durable_repair_u64_to_i64(
            next_attempt_after,
            "record placed segment shard backfill claim error next_attempt_after",
        )?;
        let updated = self
            .conn
            .execute(
                "UPDATE placed_segment_shard_backfills \
                 SET claim_id = NULL, owner_token = NULL, cluster_epoch = NULL, \
                     claimed_at = NULL, lease_deadline = NULL, \
                     next_attempt_after = ?13, last_error = ?14 \
                 WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                   AND stored_size = ?4 AND segment_crc64 = ?5 AND ec_k = ?6 AND ec_m = ?7 \
                   AND source_cluster_epoch = ?8 AND desired_cluster_epoch = ?9 \
                   AND claim_id = ?10 AND owner_token = ?11 AND cluster_epoch = ?12",
                params![
                    claim.work_item.request.data_pg_id as i64,
                    claim.work_item.request.segment_okh.as_slice(),
                    claim.work_item.request.segment_vid.get() as i64,
                    claim.work_item.request.stored_size as i64,
                    claim.work_item.request.segment_crc64 as i64,
                    claim.work_item.request.ec.k as i64,
                    claim.work_item.request.ec.m as i64,
                    claim.work_item.source_cluster_epoch.get(),
                    claim.work_item.desired_cluster_epoch.get(),
                    claim.claim_id,
                    claim.owner_token,
                    claim.cluster_epoch.get(),
                    next_attempt_after,
                    last_error,
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "record placed segment shard backfill claim error",
                source,
            })?;
        Ok(updated > 0)
    }

    /// Record a non-authoritative shard scavenger audit observation.
    pub fn record_shard_scavenger_observation(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_shard_scavenger_observation_record(observation)?;
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT INTO shard_scavenger_observations \
                 (node_id, data_pg_id, shard_index, shard_key, first_seen_at, last_seen_at, \
                  observation_count, data_size, crc64_nvme, file_exists, shard_row_exists, \
                  reason, last_error, resolved_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, 1, ?6, ?7, ?8, ?9, ?10, ?11, NULL) \
                 ON CONFLICT(node_id, data_pg_id, shard_index, shard_key) DO UPDATE SET \
                  last_seen_at = excluded.last_seen_at, \
                  observation_count = shard_scavenger_observations.observation_count + 1, \
                  data_size = excluded.data_size, \
                  crc64_nvme = excluded.crc64_nvme, \
                  file_exists = excluded.file_exists, \
                  shard_row_exists = excluded.shard_row_exists, \
                  reason = excluded.reason, \
                  last_error = excluded.last_error, \
                  resolved_at = NULL",
                params![
                    observation.key.node_id as i64,
                    observation.key.data_pg_id as i64,
                    observation.key.shard_index.get() as i64,
                    observation.key.shard_key.as_bytes().as_slice(),
                    now as i64,
                    observation.data_size.map(|size| size as i64),
                    observation.crc64.map(|crc| crc as i64),
                    if observation.file_exists {
                        1_i64
                    } else {
                        0_i64
                    },
                    if observation.shard_row_exists {
                        1_i64
                    } else {
                        0_i64
                    },
                    observation.reason as u8 as i64,
                    observation.last_error.as_deref(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "record shard scavenger observation",
                source,
            })?;
        let _ = observability::emit_shard_scavenger_observation(
            TRACE_TARGET,
            observability::ShardScavengerObservationSummary {
                node_id: observation.key.node_id,
                data_pg_id: observation.key.data_pg_id,
                shard_index: observation.key.shard_index.get(),
                shard_key_hex: &observation.key.shard_key.hex(),
                reason: shard_scavenger_observation_reason_name(observation.reason),
                file_exists: observation.file_exists,
                shard_row_exists: observation.shard_row_exists,
                last_error: observation.last_error.as_deref(),
            },
        );
        Ok(())
    }

    /// Mark a shard scavenger audit observation as resolved.
    pub fn resolve_shard_scavenger_observation(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<bool, StoreError> {
        self.validate_shard_scavenger_observation_key(key)?;
        let now = Self::now_secs();
        let updated = self
            .conn
            .execute(
                "UPDATE shard_scavenger_observations \
                 SET resolved_at = ?1 \
                 WHERE node_id = ?2 AND data_pg_id = ?3 AND shard_index = ?4 AND shard_key = ?5",
                params![
                    now as i64,
                    key.node_id as i64,
                    key.data_pg_id as i64,
                    key.shard_index.get() as i64,
                    key.shard_key.as_bytes().as_slice(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "resolve shard scavenger observation",
                source,
            })?;
        Ok(updated > 0)
    }

    pub fn list_shard_scavenger_observations(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT node_id, data_pg_id, shard_index, shard_key, first_seen_at, last_seen_at, \
                        observation_count, data_size, crc64_nvme, file_exists, shard_row_exists, \
                        reason, last_error, resolved_at \
                 FROM shard_scavenger_observations \
                 ORDER BY node_id, data_pg_id, shard_index, shard_key",
            )
            .map_err(|source| StoreError::Db {
                context: "list shard scavenger observations (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list shard scavenger observations",
                source,
            })?;

        let mut observations = Vec::new();
        for row in rows {
            let (
                node_id,
                data_pg_id,
                shard_index,
                shard_key,
                first_seen_at,
                last_seen_at,
                observation_count,
                data_size,
                crc64,
                file_exists,
                shard_row_exists,
                reason,
                last_error,
                resolved_at,
            ) = row.map_err(|source| StoreError::Db {
                context: "read shard scavenger observation",
                source,
            })?;
            observations.push(ShardScavengerObservation {
                key: ShardScavengerObservationKey {
                    node_id: node_id as u32,
                    data_pg_id: data_pg_id as u32,
                    shard_index: ShardIndex::new(shard_index as u8),
                    shard_key: ShardKey::from_bytes(&shard_key)?,
                },
                first_seen_at: first_seen_at as u64,
                last_seen_at: last_seen_at as u64,
                observation_count: observation_count as u64,
                data_size: data_size.map(|size| size as u64),
                crc64: crc64.map(|crc| crc as u64),
                file_exists: file_exists != 0,
                shard_row_exists: shard_row_exists != 0,
                reason: ShardScavengerObservationReason::from_u8(reason as u8)
                    .expect("schema restricts shard scavenger observation reasons"),
                last_error,
                resolved_at: resolved_at.map(|value| value as u64),
            });
        }
        Ok(observations)
    }

    /// Audit local shard file/index mismatches for this data PG.
    ///
    /// This is intentionally non-destructive. It records only local file/row
    /// inconsistencies and resolves prior observations of those classes once
    /// the mismatch disappears.
    pub fn audit_local_shard_storage_for_scavenger(
        &self,
        node_id: u32,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let shard_rows = self.list_scavenger_shard_rows()?;
        let shard_file_scan = self.list_scavenger_shard_files()?;
        let shard_files = shard_file_scan.files;
        let row_keys: HashSet<ShardKey> = shard_rows.iter().map(|row| row.key.clone()).collect();
        let file_keys: HashSet<ShardKey> =
            shard_files.iter().map(|file| file.key.clone()).collect();
        let mut active_mismatch_keys = HashSet::new();

        for row in &shard_rows {
            let observation_key = ShardScavengerObservationKey {
                node_id,
                data_pg_id: self.pg_id,
                shard_index: row.key.shard_index(),
                shard_key: row.key.clone(),
            };
            if !file_keys.contains(&row.key) {
                active_mismatch_keys.insert(observation_key.clone());
                self.record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                    key: observation_key,
                    data_size: Some(row.ack.stored_size),
                    crc64: Some(row.ack.crc64),
                    file_exists: false,
                    shard_row_exists: true,
                    reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                    last_error: None,
                })?;
            }
        }

        for file in &shard_files {
            let observation_key = ShardScavengerObservationKey {
                node_id,
                data_pg_id: self.pg_id,
                shard_index: file.key.shard_index(),
                shard_key: file.key.clone(),
            };
            if !row_keys.contains(&file.key) {
                active_mismatch_keys.insert(observation_key.clone());
                self.record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                    key: observation_key,
                    data_size: Some(file.size),
                    crc64: None,
                    file_exists: true,
                    shard_row_exists: false,
                    reason: ShardScavengerObservationReason::FileWithoutShardRow,
                    last_error: None,
                })?;
            }
        }

        if shard_file_scan.errors.is_empty() {
            for observation in self.list_shard_scavenger_observations()? {
                if observation.key.node_id != node_id || observation.key.data_pg_id != self.pg_id {
                    continue;
                }
                if observation.resolved_at.is_some()
                    || active_mismatch_keys.contains(&observation.key)
                {
                    continue;
                }
                if matches!(
                    observation.reason,
                    ShardScavengerObservationReason::FileWithoutShardRow
                        | ShardScavengerObservationReason::ShardRowWithoutFile
                ) {
                    self.resolve_shard_scavenger_observation(&observation.key)?;
                }
            }
        }

        if !shard_file_scan.errors.is_empty() {
            return Err(StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                errors: shard_file_scan.errors.join("; "),
            });
        }

        self.list_shard_scavenger_observations()
    }

    pub(crate) fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let mut references = Vec::new();
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, size, segment_crc64, ec_k, ec_m \
             FROM object_segments",
            "list object segment shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, part_okh, part_vid, size, payload_crc64, ec_k, ec_m \
             FROM object_parts WHERE part_okh != zeroblob(16)",
            "list object part shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, size, segment_crc64, ec_k, ec_m \
             FROM stream_upload_segments",
            "list stream upload segment shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, size, segment_crc64, ec_k, ec_m \
             FROM multipart_part_segments",
            "list multipart part segment shard scavenger references",
        )?;
        self.extend_scavenger_reclaim_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m \
             FROM object_segment_reclaim_segments",
            "list object segment reclaim shard scavenger references",
        )?;
        self.extend_scavenger_reclaim_references(
            &mut references,
            "SELECT data_pg_id, part_okh, part_vid, ec_k, ec_m \
             FROM multipart_reclaim_parts WHERE storage_kind = 0",
            "list multipart reclaim part shard scavenger references",
        )?;
        self.extend_scavenger_reclaim_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m \
             FROM multipart_reclaim_part_segments",
            "list multipart reclaim segment shard scavenger references",
        )?;
        self.extend_scavenger_routed_multipart_part_references(&mut references)?;
        self.extend_scavenger_pending_command_references(&mut references)?;
        Ok(references)
    }

    fn extend_scavenger_pending_command_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
    ) -> Result<(), StoreError> {
        let Some(command_bytes) = self.query_row_cached_optional(
            "SELECT command_bytes FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
            "load pending metadata command for shard scavenger references",
            |row| row.get::<_, Vec<u8>>(0),
        )?
        else {
            return Ok(());
        };
        let command = decode_metadata_command_envelope(&command_bytes).map_err(|reason| {
            StoreError::ShardScavengerScanIncomplete {
                context: "decode pending metadata command for shard scavenger references",
                errors: reason,
            }
        })?;
        self.extend_scavenger_command_payload_references(references, command.payload());
        Ok(())
    }

    fn extend_scavenger_command_payload_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        payload: &MetadataCommandPayload,
    ) {
        match payload {
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                Self::extend_object_segment_references(references, &command.segments);
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                Self::extend_object_part_references(references, &command.parts);
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.selected_streaming_segments,
                );
                Self::extend_routed_multipart_part_references(
                    references,
                    &command.object.bucket,
                    &command.object.key,
                    command.object.generation_id,
                    &command.omitted_parts,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.omitted_streaming_segments,
                );
                Self::extend_stream_segment_references(references, &command.stream_upload_segments);
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::DeleteObjectVersion(command) => {
                if let DeleteObjectVersionTarget::Live { payload, .. } = &command.target {
                    Self::extend_reclaim_payload_references(references, payload);
                }
            }
            MetadataCommandPayload::InsertDeleteMarker(command) => {
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::AppendStreamSegment(command) => {
                Self::extend_stream_segment_reference(references, &command.segment);
            }
            MetadataCommandPayload::AbortStreamUpload(command) => {
                Self::extend_stream_segment_references(references, &command.staged_segments);
            }
            MetadataCommandPayload::CommitStreamPart(command) => {
                Self::extend_multipart_part_segment_references(references, &command.segments);
                if let Some(existing_part) = &command.existing_part {
                    Self::extend_routed_multipart_part_references(
                        references,
                        &command.upload.bucket,
                        &command.upload.key,
                        command.upload.object_generation_id,
                        std::slice::from_ref(existing_part),
                    );
                }
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.displaced_segments,
                );
            }
            MetadataCommandPayload::AbortMultipartUpload(command) => {
                Self::extend_routed_multipart_part_references(
                    references,
                    &command.cleanup.upload.bucket,
                    &command.cleanup.upload.key,
                    command.cleanup.upload.object_generation_id,
                    &command.cleanup.parts,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.cleanup.streaming_segments,
                );
                Self::extend_stream_segment_references(
                    references,
                    &command.cleanup.stream_upload_segments,
                );
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => {
                Self::extend_reclaim_payload_references(references, &command.payload);
            }
            MetadataCommandPayload::CreateBucket(_)
            | MetadataCommandPayload::PutBucketVersioning(_)
            | MetadataCommandPayload::PutBucketAcl(_)
            | MetadataCommandPayload::PutBucketProperty(_)
            | MetadataCommandPayload::PutBucketSubresource(_)
            | MetadataCommandPayload::MarkBucketDeleting(_)
            | MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::ReleaseObjectGeneration(_)
            | MetadataCommandPayload::ReserveObjectVersion(_)
            | MetadataCommandPayload::PutObjectMetadata(_)
            | MetadataCommandPayload::CreateStreamUpload(_)
            | MetadataCommandPayload::CreateMultipartUpload(_)
            | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
            | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {}
        }
    }

    fn extend_object_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[ObjectSegmentRecord],
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                segment.size,
                segment.segment_crc64,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn extend_object_part_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        parts: &[ObjectPartRecord],
    ) {
        for part in parts {
            if part.part_okh == [0; 16] {
                continue;
            }
            Self::push_placed_reference(
                references,
                part.data_pg_id,
                part.part_okh,
                part.part_vid,
                part.size,
                part.payload_crc64,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
    }

    fn extend_stream_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[StreamUploadSegmentRecord],
    ) {
        for segment in segments {
            Self::extend_stream_segment_reference(references, segment);
        }
    }

    fn extend_stream_segment_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segment: &StreamUploadSegmentRecord,
    ) {
        Self::push_placed_reference(
            references,
            segment.data_pg_id,
            segment.segment_okh,
            segment.segment_vid,
            segment.size,
            segment.segment_crc64,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        );
    }

    fn extend_multipart_part_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[MultipartPartSegmentRecord],
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                segment.size,
                segment.segment_crc64,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn extend_routed_multipart_part_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        bucket: &BucketName,
        key: &ObjectKey,
        object_generation_id: GenerationId,
        parts: &[MultipartPartRecord],
    ) {
        for part in parts {
            if part.part_okh == [0; 16] {
                continue;
            }
            references.push(ShardScavengerPayloadReference::RoutedMultipartPart(
                ShardScavengerRoutedMultipartPartReference {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    object_generation_id,
                    part_number: part.part_number,
                    stored_size: part.size,
                    crc64: part.payload_crc64,
                    part_okh: part.part_okh,
                    part_vid: part.part_vid,
                    ec: EcShape {
                        k: part.ec_k,
                        m: part.ec_m,
                    },
                },
            ));
        }
    }

    fn extend_reclaim_payload_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        payload: &ObjectPayloadReclaimCommand,
    ) {
        match payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                for segment in &reclaim.segments {
                    Self::push_reclaim_reference(
                        references,
                        segment.data_pg_id,
                        segment.segment_okh,
                        segment.segment_vid,
                        segment.ec,
                    );
                }
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                for part in &reclaim.parts {
                    match part {
                        MultipartReclaimPartRecord::ShardSet {
                            part_okh,
                            part_vid,
                            data_pg_id,
                            ec,
                            ..
                        } => Self::push_reclaim_reference(
                            references,
                            *data_pg_id,
                            *part_okh,
                            *part_vid,
                            *ec,
                        ),
                        MultipartReclaimPartRecord::Segments { segments, .. } => {
                            for segment in segments {
                                Self::push_reclaim_reference(
                                    references,
                                    segment.data_pg_id,
                                    segment.segment_okh,
                                    segment.segment_vid,
                                    segment.ec,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    fn push_placed_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        data_pg_id: u32,
        okh: [u8; 16],
        generation_id: GenerationId,
        stored_size: u64,
        crc64: u64,
        ec: EcShape,
    ) {
        references.push(ShardScavengerPayloadReference::Placed(
            ShardScavengerPlacedShardSetReference {
                data_pg_id,
                okh,
                generation_id,
                stored_size,
                crc64,
                ec,
            },
        ));
    }

    fn push_reclaim_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        data_pg_id: u32,
        okh: [u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        references.push(ShardScavengerPayloadReference::ReclaimOnly(
            ShardScavengerReclaimShardSetReference {
                data_pg_id,
                okh,
                generation_id,
                ec,
            },
        ));
    }

    pub(crate) fn list_scavenger_shard_rows(&self) -> Result<Vec<ScavengerShardRow>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT shard_key, data_size, crc64_nvme \
                 FROM shards \
                 ORDER BY shard_key",
            )
            .map_err(|source| StoreError::Db {
                context: "list scavenger shard rows (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list scavenger shard rows",
                source,
            })?;
        let mut shard_rows = Vec::new();
        for row in rows {
            let (key, stored_size, crc64) = row.map_err(|source| StoreError::Db {
                context: "read scavenger shard row",
                source,
            })?;
            shard_rows.push(ScavengerShardRow {
                key: ShardKey::from_bytes(&key)?,
                ack: WriteAck {
                    stored_size: stored_size as u64,
                    crc64: crc64 as u64,
                },
            });
        }
        Ok(shard_rows)
    }

    fn extend_scavenger_placed_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        sql: &'static str,
        context: &'static str,
    ) -> Result<(), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|source| StoreError::Db { context, source })?;
        let rows = stmt
            .query_map([], |row| {
                let okh_blob: Vec<u8> = row.get(1)?;
                Ok(ShardScavengerPayloadReference::Placed(
                    ShardScavengerPlacedShardSetReference {
                        data_pg_id: row.get(0)?,
                        okh: PgStore::parse_okh_blob(&okh_blob, 1)?,
                        generation_id: PgStore::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "shard scavenger reference generation",
                        )?,
                        stored_size: row.get::<_, i64>(3)? as u64,
                        crc64: row.get::<_, i64>(4)? as u64,
                        ec: EcShape {
                            k: row.get(5)?,
                            m: row.get(6)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db { context, source })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db { context, source })?);
        }
        Ok(())
    }

    fn extend_scavenger_reclaim_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        sql: &'static str,
        context: &'static str,
    ) -> Result<(), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|source| StoreError::Db { context, source })?;
        let rows = stmt
            .query_map([], |row| {
                let okh_blob: Vec<u8> = row.get(1)?;
                Ok(ShardScavengerPayloadReference::ReclaimOnly(
                    ShardScavengerReclaimShardSetReference {
                        data_pg_id: row.get(0)?,
                        okh: PgStore::parse_okh_blob(&okh_blob, 1)?,
                        generation_id: PgStore::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "shard scavenger reclaim reference generation",
                        )?,
                        ec: EcShape {
                            k: row.get(3)?,
                            m: row.get(4)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db { context, source })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db { context, source })?);
        }
        Ok(())
    }

    fn extend_scavenger_routed_multipart_part_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
    ) -> Result<(), StoreError> {
        let context = "list routed multipart part shard scavenger references";
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT u.bucket, u.key, u.object_generation_id, p.part_number, \
                 p.size, p.payload_crc64, p.part_okh, p.part_vid, p.ec_k, p.ec_m \
                 FROM multipart_parts p \
                 JOIN multipart_uploads u ON u.upload_id = p.upload_id \
                 WHERE p.part_okh != zeroblob(16)",
            )
            .map_err(|source| StoreError::Db { context, source })?;
        let rows = stmt
            .query_map([], |row| {
                let okh_blob: Vec<u8> = row.get(6)?;
                Ok(ShardScavengerPayloadReference::RoutedMultipartPart(
                    ShardScavengerRoutedMultipartPartReference {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        object_generation_id: PgStore::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "multipart upload object generation",
                        )?,
                        part_number: row.get(3)?,
                        stored_size: row.get::<_, i64>(4)? as u64,
                        crc64: row.get::<_, i64>(5)? as u64,
                        part_okh: PgStore::parse_okh_blob(&okh_blob, 6)?,
                        part_vid: PgStore::parse_generation_id(
                            row.get::<_, i64>(7)?,
                            7,
                            "multipart part payload generation",
                        )?,
                        ec: EcShape {
                            k: row.get(8)?,
                            m: row.get(9)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db { context, source })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db { context, source })?);
        }
        Ok(())
    }

    pub(crate) fn list_scavenger_shard_files(&self) -> Result<ScavengerShardFileScan, StoreError> {
        Self::list_scavenger_shard_files_in_dir(&self.shards_dir)
    }

    pub(crate) fn list_scavenger_shard_files_in_dir(
        shards_dir: &Path,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        let mut files = Vec::new();
        let mut errors = Vec::new();
        for prefix in fs::read_dir(shards_dir).map_err(|source| StoreError::Io {
            context: "scan shard prefix directory",
            source,
        })? {
            let prefix = prefix.map_err(|source| StoreError::Io {
                context: "read shard prefix directory entry",
                source,
            })?;
            let file_type = prefix.file_type().map_err(|source| StoreError::Io {
                context: "read shard prefix directory entry type",
                source,
            })?;
            let prefix_path = prefix.path();
            if !file_type.is_dir() {
                errors.push(format!(
                    "unexpected non-directory entry under shard root {}",
                    prefix_path.display()
                ));
                continue;
            }
            let prefix_name = prefix.file_name();
            let Some(prefix_name) = prefix_name.to_str() else {
                errors.push(format!(
                    "non-UTF8 shard prefix directory {}",
                    prefix_path.display()
                ));
                continue;
            };
            if !is_canonical_shard_prefix(prefix_name) {
                errors.push(format!(
                    "invalid shard prefix directory {}",
                    prefix_path.display()
                ));
                continue;
            }
            for entry in fs::read_dir(prefix.path()).map_err(|source| StoreError::Io {
                context: "scan shard directory",
                source,
            })? {
                let entry = entry.map_err(|source| StoreError::Io {
                    context: "read shard directory entry",
                    source,
                })?;
                let file_type = entry.file_type().map_err(|source| StoreError::Io {
                    context: "read shard directory entry type",
                    source,
                })?;
                let path = entry.path();
                if !file_type.is_file() {
                    errors.push(format!(
                        "unexpected non-file entry under shard prefix {}",
                        path.display()
                    ));
                    continue;
                }
                let file_name = entry.file_name();
                let Some(file_name) = file_name.to_str() else {
                    errors.push(format!("non-UTF8 shard file {}", path.display()));
                    continue;
                };
                let Ok(key) = ShardKey::from_hex(file_name) else {
                    errors.push(format!("invalid shard file name {}", path.display()));
                    continue;
                };
                let canonical_prefix = key.hex_prefix();
                let canonical_file_name = key.to_string();
                if prefix_name != canonical_prefix || file_name != canonical_file_name {
                    errors.push(format!(
                        "non-canonical shard file {} expected shards/{}/{}",
                        path.display(),
                        canonical_prefix,
                        canonical_file_name
                    ));
                    continue;
                };
                let metadata = entry.metadata().map_err(|source| StoreError::Io {
                    context: "stat shard file during scavenger scan",
                    source,
                })?;
                files.push(ScavengerShardFile {
                    key,
                    size: metadata.len(),
                });
            }
        }
        files.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
        Ok(ScavengerShardFileScan { files, errors })
    }

    fn validate_shard_scavenger_observation_key(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        if key.data_pg_id != self.pg_id {
            return Err(StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: self.pg_id,
                observation_pg_id: key.data_pg_id,
            });
        }
        let key_shard_index = key.shard_key.shard_index();
        if key.shard_index != key_shard_index {
            return Err(StoreError::ShardScavengerObservationShardIndexMismatch {
                observation_shard_index: key.shard_index.get(),
                key_shard_index: key_shard_index.get(),
            });
        }
        Ok(())
    }

    fn validate_shard_scavenger_observation_record(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_shard_scavenger_observation_key(&observation.key)?;
        let state_matches_reason = match observation.reason {
            ShardScavengerObservationReason::FileWithoutShardRow => {
                observation.file_exists && !observation.shard_row_exists
            }
            ShardScavengerObservationReason::ShardRowWithoutFile => {
                !observation.file_exists && observation.shard_row_exists
            }
            ShardScavengerObservationReason::UnreferencedShardRowAndFile => {
                observation.file_exists && observation.shard_row_exists
            }
            ShardScavengerObservationReason::ScanIncomplete => true,
        };
        if !state_matches_reason {
            return Err(StoreError::ShardScavengerObservationInconsistentReason {
                reason: observation.reason,
                file_exists: observation.file_exists,
                shard_row_exists: observation.shard_row_exists,
            });
        }
        Ok(())
    }
}

fn validate_placed_segment_shard_repair_pg(
    pg_id: u32,
    work_item: &PlacedSegmentShardRepairWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.data_pg_id != pg_id {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair work item data PG {} does not match routed PG {}",
                work_item.request.data_pg_id, pg_id
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_pg(
    pg_id: u32,
    work_item: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.data_pg_id != pg_id {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill work item data PG {} does not match routed PG {}",
                work_item.request.data_pg_id, pg_id
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_work_item(
    work_item: &PlacedSegmentShardRepairWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.ec.k == 0 {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable repair work item has invalid EC k=0".to_string(),
        });
    }
    let total = work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or_else(|| StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair work item EC shard count overflow for {}+{}",
                work_item.request.ec.k, work_item.request.ec.m
            ),
        })?;
    if work_item.shard_index.get() >= total {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair shard index {} outside EC {}+{}",
                work_item.shard_index.get(),
                work_item.request.ec.k,
                work_item.request.ec.m
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_work_item(
    work_item: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StoreError> {
    if work_item.request.ec.k == 0 {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable backfill work item has invalid EC k=0".to_string(),
        });
    }
    work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or_else(|| StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill work item EC shard count overflow for {}+{}",
                work_item.request.ec.k, work_item.request.ec.m
            ),
        })?;
    if work_item.source_cluster_epoch > work_item.desired_cluster_epoch {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill source epoch {} is newer than desired epoch {}",
                work_item.source_cluster_epoch.get(),
                work_item.desired_cluster_epoch.get()
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_coalesces_exactly(
    existing: &PlacedSegmentShardBackfillWorkItem,
    incoming: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StoreError> {
    if existing.request != incoming.request {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill duplicate for data PG {}, segment {:?}/{}, epochs {}->{}, \
                 has mismatched request identity",
                incoming.request.data_pg_id,
                incoming.request.segment_okh,
                incoming.request.segment_vid.get(),
                incoming.source_cluster_epoch.get(),
                incoming.desired_cluster_epoch.get()
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StoreError> {
    if claim_id.is_empty() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable repair claim id is empty".to_string(),
        });
    }
    if owner_token.is_empty() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable repair owner token is empty".to_string(),
        });
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair claim id length {} exceeds {}",
                claim_id.len(),
                PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN
            ),
        });
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair owner token length {} exceeds {}",
                owner_token.len(),
                PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_record(
    pg_id: u32,
    claim: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StoreError> {
    validate_placed_segment_shard_repair_work_item(&claim.work_item)?;
    validate_placed_segment_shard_repair_pg(pg_id, &claim.work_item)?;
    validate_placed_segment_shard_repair_claim_identity(&claim.claim_id, &claim.owner_token)
}

fn validate_placed_segment_shard_backfill_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StoreError> {
    if claim_id.is_empty() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable backfill claim id is empty".to_string(),
        });
    }
    if owner_token.is_empty() {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: "durable backfill owner token is empty".to_string(),
        });
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill claim id length {} exceeds {}",
                claim_id.len(),
                PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN
            ),
        });
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill owner token length {} exceeds {}",
                owner_token.len(),
                PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_record(
    pg_id: u32,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) -> Result<(), StoreError> {
    validate_placed_segment_shard_backfill_work_item(&claim.work_item)?;
    validate_placed_segment_shard_backfill_pg(pg_id, &claim.work_item)?;
    validate_placed_segment_shard_backfill_claim_identity(&claim.claim_id, &claim.owner_token)
}

fn validate_placed_segment_shard_repair_last_error(last_error: &str) -> Result<(), StoreError> {
    if last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable repair last_error length {} exceeds {}",
                last_error.len(),
                PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN
            ),
        });
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_last_error(last_error: &str) -> Result<(), StoreError> {
    if last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill last_error length {} exceeds {}",
                last_error.len(),
                PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN
            ),
        });
    }
    Ok(())
}

fn durable_repair_u64_to_i64(value: u64, context: &'static str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|source| StoreError::Db {
        context,
        source: rusqlite::Error::ToSqlConversionFailure(Box::new(source)),
    })
}

fn durable_repair_claim_select_sql(where_clause: &str) -> String {
    format!(
        "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                ec_k, ec_m, shard_index, claim_id, owner_token, cluster_epoch, claimed_at, \
                lease_deadline, attempt_count, last_error \
         FROM placed_segment_shard_repairs \
         WHERE {where_clause} \
         ORDER BY last_seen_at, segment_okh, segment_vid, shard_index \
         LIMIT 1"
    )
}

fn durable_backfill_claim_select_sql(where_clause: &str) -> String {
    format!(
        "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, claim_id, \
                owner_token, cluster_epoch, claimed_at, lease_deadline, attempt_count, \
                last_error \
         FROM placed_segment_shard_backfills \
         WHERE {where_clause} \
         ORDER BY last_seen_at, segment_okh, segment_vid, source_cluster_epoch, \
                  desired_cluster_epoch \
         LIMIT 1"
    )
}

fn placed_segment_shard_repair_claim_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<PlacedSegmentShardRepairClaimRecord, rusqlite::Error> {
    let work_item = placed_segment_shard_repair_work_item_from_row(row)?;
    let cluster_epoch = row.get::<_, i64>(10)?;
    let cluster_epoch = ClusterEpoch::new(cluster_epoch as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            10,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable repair claim row has invalid cluster epoch",
            )),
        )
    })?;
    Ok(PlacedSegmentShardRepairClaimRecord {
        work_item,
        claim_id: row.get(8)?,
        owner_token: row.get(9)?,
        cluster_epoch,
        claimed_at: row.get::<_, i64>(11)? as u64,
        lease_deadline: row
            .get::<_, Option<i64>>(12)?
            .map(|deadline| deadline as u64),
        attempt_count: row.get::<_, i64>(13)? as u64,
        last_error: row.get(14)?,
    })
}

fn placed_segment_shard_backfill_claim_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<PlacedSegmentShardBackfillClaimRecord, rusqlite::Error> {
    let work_item = placed_segment_shard_backfill_work_item_from_row(row)?;
    let cluster_epoch = row.get::<_, i64>(11)?;
    let cluster_epoch = ClusterEpoch::new(cluster_epoch as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable backfill claim row has invalid cluster epoch",
            )),
        )
    })?;
    Ok(PlacedSegmentShardBackfillClaimRecord {
        work_item,
        claim_id: row.get(9)?,
        owner_token: row.get(10)?,
        cluster_epoch,
        claimed_at: row.get::<_, i64>(12)? as u64,
        lease_deadline: row
            .get::<_, Option<i64>>(13)?
            .map(|deadline| deadline as u64),
        attempt_count: row.get::<_, i64>(14)? as u64,
        last_error: row.get(15)?,
    })
}

fn placed_segment_shard_repair_work_item_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<PlacedSegmentShardRepairWorkItem, rusqlite::Error> {
    let segment_okh = row.get::<_, Vec<u8>>(1)?;
    let segment_okh: [u8; 16] = segment_okh.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::other(
                "durable repair row has invalid segment OKH length",
            )),
        )
    })?;
    let segment_vid = row.get::<_, i64>(2)?;
    let segment_vid = GenerationId::new(segment_vid as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable repair row has invalid segment version",
            )),
        )
    })?;
    let work_item = PlacedSegmentShardRepairWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: row.get::<_, i64>(0)? as u32,
            segment_okh,
            segment_vid,
            stored_size: row.get::<_, i64>(3)? as usize,
            segment_crc64: row.get::<_, i64>(4)? as u64,
            ec: EcShape {
                k: row.get::<_, i64>(5)? as u8,
                m: row.get::<_, i64>(6)? as u8,
            },
        },
        shard_index: ShardIndex::new(row.get::<_, i64>(7)? as u8),
    };
    validate_placed_segment_shard_repair_work_item(&work_item).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Null,
            Box::new(std::io::Error::other(error.to_string())),
        )
    })?;
    Ok(work_item)
}

fn placed_segment_shard_backfill_work_item_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<PlacedSegmentShardBackfillWorkItem, rusqlite::Error> {
    let segment_okh = row.get::<_, Vec<u8>>(1)?;
    let segment_okh: [u8; 16] = segment_okh.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::other(
                "durable backfill row has invalid segment OKH length",
            )),
        )
    })?;
    let segment_vid = row.get::<_, i64>(2)?;
    let segment_vid = GenerationId::new(segment_vid as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable backfill row has invalid segment version",
            )),
        )
    })?;
    let source_cluster_epoch = row.get::<_, i64>(7)?;
    let source_cluster_epoch = ClusterEpoch::new(source_cluster_epoch as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable backfill row has invalid source cluster epoch",
            )),
        )
    })?;
    let desired_cluster_epoch = row.get::<_, i64>(8)?;
    let desired_cluster_epoch =
        ClusterEpoch::new(desired_cluster_epoch as u64).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::new(std::io::Error::other(
                    "durable backfill row has invalid desired cluster epoch",
                )),
            )
        })?;
    let work_item = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: row.get::<_, i64>(0)? as u32,
            segment_okh,
            segment_vid,
            stored_size: row.get::<_, i64>(3)? as usize,
            segment_crc64: row.get::<_, i64>(4)? as u64,
            ec: EcShape {
                k: row.get::<_, i64>(5)? as u8,
                m: row.get::<_, i64>(6)? as u8,
            },
        },
        source_cluster_epoch,
        desired_cluster_epoch,
    };
    validate_placed_segment_shard_backfill_work_item(&work_item).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Null,
            Box::new(std::io::Error::other(error.to_string())),
        )
    })?;
    Ok(work_item)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repair_claim_acquire(
        claim_id: &str,
        owner_token: &str,
        epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> PlacedSegmentShardRepairClaimAcquire {
        PlacedSegmentShardRepairClaimAcquire {
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            cluster_epoch: epoch,
            claimed_at,
            lease_deadline,
            now,
        }
    }

    fn backfill_claim_acquire(
        claim_id: &str,
        owner_token: &str,
        epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> PlacedSegmentShardBackfillClaimAcquire {
        PlacedSegmentShardBackfillClaimAcquire {
            claim_id: claim_id.to_string(),
            owner_token: owner_token.to_string(),
            cluster_epoch: epoch,
            claimed_at,
            lease_deadline,
            now,
        }
    }

    #[test]
    fn placed_segment_shard_repair_rows_are_durable_and_coalesced() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xA7; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            shard_index: ShardIndex::new(5),
        };

        store
            .record_placed_segment_shard_repair(&work_item, Some("first"))
            .unwrap();
        store
            .record_placed_segment_shard_repair(&work_item, Some("second"))
            .unwrap();
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();

        drop(store);
        let reopened = PgStore::open(tmp.path(), 7).unwrap();
        let repairs = reopened.list_placed_segment_shard_repairs().unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].work_item, work_item);
        assert_eq!(repairs[0].observation_count, 3);
        assert_eq!(repairs[0].last_error.as_deref(), Some("second"));

        assert!(reopened
            .resolve_placed_segment_shard_repair(&work_item)
            .unwrap());
        assert!(reopened
            .list_placed_segment_shard_repairs()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placed_segment_shard_backfill_rows_are_durable_and_coalesced() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xB7; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };

        store
            .record_placed_segment_shard_backfill(&work_item, Some("first"))
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, Some("second"))
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();

        drop(store);
        let reopened = PgStore::open(tmp.path(), 7).unwrap();
        let backfills = reopened.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].observation_count, 3);
        assert_eq!(backfills[0].last_error.as_deref(), Some("second"));

        assert!(reopened
            .resolve_placed_segment_shard_backfill(&work_item)
            .unwrap());
        assert!(reopened
            .list_placed_segment_shard_backfills()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placed_segment_shard_backfill_rejects_inexact_coalescing() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xBA; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let mut mismatched = work_item;
        mismatched.request.segment_crc64 = 0x5678;

        store
            .record_placed_segment_shard_backfill(&work_item, Some("first"))
            .unwrap();
        assert!(matches!(
            store.record_placed_segment_shard_backfill(&mismatched, Some("second")),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));

        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].observation_count, 1);
        assert_eq!(backfills[0].last_error.as_deref(), Some("first"));
    }

    #[test]
    fn placed_segment_shard_backfill_resolve_requires_exact_request_identity() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xBB; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let mut stale = work_item;
        stale.request.stored_size = 2048;

        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();
        assert!(!store.resolve_placed_segment_shard_backfill(&stale).unwrap());

        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);

        assert!(store
            .resolve_placed_segment_shard_backfill(&work_item)
            .unwrap());
    }

    #[test]
    fn shard_scavenger_payload_references_include_part_size_and_crc() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        store
            .conn
            .execute(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, \
                  etag, etag_kind, part_okh, part_vid, ec_k, ec_m, data_pg_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                rusqlite::params![
                    "bucket",
                    "object",
                    0i64,
                    1i64,
                    0i64,
                    1234i64,
                    0xAABB_i64,
                    b"etag".as_slice(),
                    0i64,
                    [0x11u8; 16].as_slice(),
                    9i64,
                    4i64,
                    2i64,
                    7i64,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO multipart_uploads \
                 (upload_id, bucket, key, initiated_at, state, metadata_blob, system_metadata_blob, \
                  owner_principal, owner_canonical_id, acl_grants, public_read, \
                  object_generation_id, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                rusqlite::params![
                    "u".repeat(128),
                    "bucket",
                    "multipart",
                    10i64,
                    0i64,
                    b"".as_slice(),
                    b"".as_slice(),
                    "owner",
                    "c".repeat(32),
                    "",
                    0i64,
                    22i64,
                    0i64,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO multipart_parts \
                 (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                rusqlite::params![
                    "u".repeat(128),
                    2i64,
                    0i64,
                    5678i64,
                    0xCCDD_i64,
                    b"etag".as_slice(),
                    0i64,
                    [0x22u8; 16].as_slice(),
                    10i64,
                    4i64,
                    2i64,
                    11i64,
                ],
            )
            .unwrap();
        store
            .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
                bucket: crate::tests::bucket_name("bucket"),
                key: crate::tests::object_key("reclaim"),
                generation_id: GenerationId::new(33).unwrap(),
                created_at: 12,
                segments: vec![ObjectSegmentsReclaimSegmentRecord {
                    segment_index: 0,
                    segment_okh: [0x33; 16],
                    segment_vid: GenerationId::new(44).unwrap(),
                    data_pg_id: 7,
                    ec: EcShape { k: 4, m: 2 },
                }],
            })
            .unwrap();

        let references = store.list_shard_scavenger_payload_references().unwrap();
        let object_part = references
            .iter()
            .find_map(|reference| match reference {
                ShardScavengerPayloadReference::Placed(reference)
                    if reference.okh == [0x11; 16] =>
                {
                    Some(reference)
                }
                _ => None,
            })
            .expect("object part placed reference should be listed");
        assert_eq!(object_part.stored_size, 1234);
        assert_eq!(object_part.crc64, 0xAABB);

        let routed_part = references
            .iter()
            .find_map(|reference| match reference {
                ShardScavengerPayloadReference::RoutedMultipartPart(reference)
                    if reference.part_okh == [0x22; 16] =>
                {
                    Some(reference)
                }
                _ => None,
            })
            .expect("multipart part routed reference should be listed");
        assert_eq!(routed_part.bucket.as_str(), "bucket");
        assert_eq!(routed_part.key.as_str(), "multipart");
        assert_eq!(
            routed_part.object_generation_id,
            GenerationId::new(22).unwrap()
        );
        assert_eq!(routed_part.part_number, 2);
        assert_eq!(routed_part.stored_size, 5678);
        assert_eq!(routed_part.crc64, 0xCCDD);

        let reclaim = references
            .iter()
            .find_map(|reference| match reference {
                ShardScavengerPayloadReference::ReclaimOnly(reference)
                    if reference.okh == [0x33; 16] =>
                {
                    Some(reference)
                }
                _ => None,
            })
            .expect("object segment reclaim reference should be listed");
        assert_eq!(reclaim.data_pg_id, 7);
        assert_eq!(reclaim.generation_id, GenerationId::new(44).unwrap());
        assert_eq!(reclaim.ec, EcShape { k: 4, m: 2 });
    }

    #[test]
    fn placed_segment_shard_repair_list_is_bounded() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        for index in 0..=PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT {
            let mut okh = [0u8; 16];
            okh[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let work_item = PlacedSegmentShardRepairWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: okh,
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: index as u64,
                    ec: EcShape { k: 4, m: 2 },
                },
                shard_index: ShardIndex::new(5),
            };
            store
                .record_placed_segment_shard_repair(&work_item, None)
                .unwrap();
        }

        assert_eq!(
            store.list_placed_segment_shard_repairs().unwrap().len(),
            PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT
        );
    }

    #[test]
    fn placed_segment_shard_backfill_list_is_bounded() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        for index in 0..=PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT {
            let mut okh = [0u8; 16];
            okh[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let work_item = PlacedSegmentShardBackfillWorkItem {
                request: SegmentStoredBytesRequest {
                    data_pg_id: 7,
                    segment_okh: okh,
                    segment_vid: GenerationId::new(42).unwrap(),
                    stored_size: 1024,
                    segment_crc64: index as u64,
                    ec: EcShape { k: 4, m: 2 },
                },
                source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
                desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
            };
            store
                .record_placed_segment_shard_backfill(&work_item, None)
                .unwrap();
        }

        assert_eq!(
            store.list_placed_segment_shard_backfills().unwrap().len(),
            PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT
        );
        assert_eq!(
            store.placed_segment_shard_backfill_count().unwrap(),
            PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT + 1
        );
    }

    #[test]
    fn placed_segment_shard_repair_rejects_wrong_data_pg() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 8,
                segment_okh: [0xA8; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            shard_index: ShardIndex::new(5),
        };

        assert!(matches!(
            store.record_placed_segment_shard_repair(&work_item, None),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.resolve_placed_segment_shard_repair(&work_item),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_rejects_wrong_data_pg_and_epoch_order() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let wrong_pg = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 8,
                segment_okh: [0xB8; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let reversed_epochs = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xB9; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(5).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(3).unwrap(),
        };

        assert!(matches!(
            store.record_placed_segment_shard_backfill(&wrong_pg, None),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.resolve_placed_segment_shard_backfill(&wrong_pg),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.record_placed_segment_shard_backfill(&reversed_epochs, None),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_claim_is_single_owner_and_retries_after_backoff() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xBC; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let epoch = ClusterEpoch::new(7).unwrap();

        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-0",
                "worker-0",
                epoch,
                9,
                Some(19),
                9
            ))
            .unwrap()
            .is_none());
        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();

        let claim = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(20),
                10,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(claim.work_item, work_item);
        assert_eq!(claim.attempt_count, 1);

        assert_eq!(
            store
                .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                    "claim-1",
                    "worker-1",
                    epoch,
                    11,
                    Some(21),
                    11,
                ))
                .unwrap()
                .unwrap(),
            claim
        );
        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                12,
                Some(22),
                12,
            ))
            .unwrap()
            .is_none());

        assert!(store
            .record_placed_segment_shard_backfill_claim_error(&claim, "backfill failed", 30)
            .unwrap());
        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();
        assert_eq!(
            store.list_placed_segment_shard_backfills().unwrap()[0]
                .last_error
                .as_deref(),
            Some("backfill failed")
        );
        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                29,
                Some(39),
                29,
            ))
            .unwrap()
            .is_none());

        let retry = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                30,
                Some(40),
                30,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(retry.work_item, work_item);
        assert_eq!(retry.attempt_count, 2);
        assert_eq!(retry.last_error.as_deref(), Some("backfill failed"));
        assert!(!store
            .complete_placed_segment_shard_backfill_claim(&claim)
            .unwrap());
        assert!(store
            .complete_placed_segment_shard_backfill_claim(&retry)
            .unwrap());
        assert!(store
            .list_placed_segment_shard_backfills()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placed_segment_shard_backfill_claim_can_be_stolen_after_expiry() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xBD; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let epoch = ClusterEpoch::new(7).unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();
        let first = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(20),
                10,
            ))
            .unwrap()
            .unwrap();

        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                19,
                Some(29),
                19,
            ))
            .unwrap()
            .is_none());
        let stolen = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                20,
                Some(30),
                20,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(stolen.work_item, work_item);
        assert_eq!(stolen.attempt_count, 2);
        assert!(!store
            .complete_placed_segment_shard_backfill_claim(&first)
            .unwrap());
    }

    #[test]
    fn placed_segment_shard_backfill_claim_requires_finite_forward_lease() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xBE; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let epoch = ClusterEpoch::new(7).unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, None)
            .unwrap();

        assert!(matches!(
            store.acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1", "worker-1", epoch, 10, None, 10
            )),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(10),
                10
            )),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
    }

    #[test]
    fn placed_segment_shard_repair_schema_rejects_missing_crc_row() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let err = store
            .conn
            .execute(
                "INSERT INTO placed_segment_shard_repairs \
                 (data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, ec_k, ec_m, \
                  shard_index, first_seen_at, last_seen_at, observation_count, last_error) \
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, ?9, ?10, NULL)",
                params![
                    7i64,
                    [0xADu8; 16].as_slice(),
                    42i64,
                    1024i64,
                    4i64,
                    2i64,
                    5i64,
                    10i64,
                    20i64,
                    1i64,
                ],
            )
            .unwrap_err();
        assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));
    }

    #[test]
    fn placed_segment_shard_backfill_schema_rejects_partial_claim_row() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let err = store
            .conn
            .execute(
                "INSERT INTO placed_segment_shard_backfills \
                 (data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, ec_k, ec_m, \
                  source_cluster_epoch, desired_cluster_epoch, first_seen_at, last_seen_at, \
                  observation_count, claim_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    7i64,
                    [0xBFu8; 16].as_slice(),
                    42i64,
                    1024i64,
                    0x1234i64,
                    4i64,
                    2i64,
                    3i64,
                    5i64,
                    10i64,
                    20i64,
                    1i64,
                    "claim-1",
                ],
            )
            .unwrap_err();
        assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));
    }

    #[test]
    fn placed_segment_shard_repair_claim_is_single_owner_and_retries_after_backoff() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xA9; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            shard_index: ShardIndex::new(5),
        };
        let epoch = ClusterEpoch::new(7).unwrap();

        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-0",
                "worker-0",
                epoch,
                9,
                Some(19),
                9
            ))
            .unwrap()
            .is_none());
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();

        let claim = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(20),
                10,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(claim.work_item, work_item);
        assert_eq!(claim.attempt_count, 1);

        assert_eq!(
            store
                .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                    "claim-1",
                    "worker-1",
                    epoch,
                    11,
                    Some(21),
                    11,
                ))
                .unwrap()
                .unwrap(),
            claim
        );
        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                12,
                Some(22),
                12,
            ))
            .unwrap()
            .is_none());

        assert!(store
            .record_placed_segment_shard_repair_claim_error(&claim, "repair failed", 30)
            .unwrap());
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();
        assert_eq!(
            store.list_placed_segment_shard_repairs().unwrap()[0]
                .last_error
                .as_deref(),
            Some("repair failed")
        );
        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                29,
                Some(39),
                29,
            ))
            .unwrap()
            .is_none());

        let retry = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                30,
                Some(40),
                30,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(retry.work_item, work_item);
        assert_eq!(retry.attempt_count, 2);
        assert_eq!(retry.last_error.as_deref(), Some("repair failed"));
        assert!(!store
            .complete_placed_segment_shard_repair_claim(&claim)
            .unwrap());
        assert!(store
            .complete_placed_segment_shard_repair_claim(&retry)
            .unwrap());
        assert!(store
            .list_placed_segment_shard_repairs()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placed_segment_shard_repair_claim_can_be_stolen_after_expiry() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xAA; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            shard_index: ShardIndex::new(5),
        };
        let epoch = ClusterEpoch::new(7).unwrap();
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();
        let first = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(20),
                10,
            ))
            .unwrap()
            .unwrap();

        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                19,
                Some(29),
                19,
            ))
            .unwrap()
            .is_none());
        let stolen = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2",
                "worker-2",
                epoch,
                20,
                Some(30),
                20,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(stolen.work_item, work_item);
        assert_eq!(stolen.attempt_count, 2);
        assert!(!store
            .complete_placed_segment_shard_repair_claim(&first)
            .unwrap());
    }

    #[test]
    fn placed_segment_shard_repair_claim_requires_finite_forward_lease() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xAB; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            shard_index: ShardIndex::new(5),
        };
        let epoch = ClusterEpoch::new(7).unwrap();
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();

        assert!(matches!(
            store.acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-1", "worker-1", epoch, 10, None, 10
            )),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-1",
                "worker-1",
                epoch,
                10,
                Some(10),
                10
            )),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
    }

    #[test]
    fn shard_scavenger_observation_is_location_keyed_and_non_authoritative() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        let shard_key = ShardKey::new(&[0xCA; 16], 42, 3);
        let ack = store.write_shard(&shard_key, b"candidate").unwrap();
        let node_zero = ShardScavengerObservationKey {
            node_id: 0,
            data_pg_id: 7,
            shard_index: shard_key.shard_index(),
            shard_key: shard_key.clone(),
        };
        let node_one = ShardScavengerObservationKey {
            node_id: 1,
            data_pg_id: 7,
            shard_index: shard_key.shard_index(),
            shard_key: shard_key.clone(),
        };

        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_zero.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: None,
            })
            .unwrap();
        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_zero.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: Some("second scan".to_owned()),
            })
            .unwrap();
        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_one.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: None,
            })
            .unwrap();

        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].key, node_zero);
        assert_eq!(observations[0].observation_count, 2);
        assert_eq!(observations[0].last_error.as_deref(), Some("second scan"));
        assert_eq!(observations[1].key, node_one);
        assert_eq!(observations[1].observation_count, 1);

        let stat = store.stat_shard(&shard_key).unwrap();
        assert_eq!(stat.size, ack.stored_size);
        assert_eq!(stat.crc64, ack.crc64);

        assert!(store
            .resolve_shard_scavenger_observation(&node_zero)
            .unwrap());
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert!(observations[0].resolved_at.is_some());
        assert!(observations[1].resolved_at.is_none());
    }

    #[test]
    fn shard_scavenger_observation_rejects_inconsistent_location_identity() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let shard_key = ShardKey::new(&[0xCB; 16], 42, 3);

        let wrong_pg = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 8,
                shard_index: shard_key.shard_index(),
                shard_key: shard_key.clone(),
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&wrong_pg)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: 7,
                observation_pg_id: 8
            }
        ));

        let wrong_index = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: ShardIndex::new(4),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&wrong_index)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationShardIndexMismatch {
                observation_shard_index: 4,
                key_shard_index: 3
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn shard_scavenger_observation_rejects_inconsistent_reason_state() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let shard_key = ShardKey::new(&[0xCC; 16], 42, 3);

        let inconsistent = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: shard_key.shard_index(),
                shard_key: shard_key.clone(),
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&inconsistent)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationInconsistentReason {
                reason: ShardScavengerObservationReason::FileWithoutShardRow,
                file_exists: true,
                shard_row_exists: true
            }
        ));

        let scan_incomplete = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: shard_key.shard_index(),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: Some("metadata pg unavailable".to_owned()),
        };
        store
            .record_shard_scavenger_observation(&scan_incomplete)
            .unwrap();
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].reason,
            ShardScavengerObservationReason::ScanIncomplete
        );
    }

    #[test]
    fn shard_scavenger_local_audit_records_file_row_mismatches_without_deleting() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let file_without_row = ShardKey::new(&[0xCD; 16], 42, 1);
        let row_without_file = ShardKey::new(&[0xCE; 16], 42, 2);

        let file_without_row_ack = store.write_shard(&file_without_row, b"file-only").unwrap();
        store.delete_shard_record(&file_without_row).unwrap();
        let row_without_file_ack = store.write_shard(&row_without_file, b"row-only").unwrap();
        fs::remove_file(PgStore::shard_path_for_shards_dir(
            &store.shards_dir,
            &row_without_file,
        ))
        .unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert_eq!(observations.len(), 2);
        let file_observation = observations
            .iter()
            .find(|observation| observation.key.shard_key == file_without_row)
            .unwrap();
        assert_eq!(
            file_observation.reason,
            ShardScavengerObservationReason::FileWithoutShardRow
        );
        assert!(file_observation.file_exists);
        assert!(!file_observation.shard_row_exists);
        assert_eq!(
            file_observation.data_size,
            Some(file_without_row_ack.stored_size)
        );

        let row_observation = observations
            .iter()
            .find(|observation| observation.key.shard_key == row_without_file)
            .unwrap();
        assert_eq!(
            row_observation.reason,
            ShardScavengerObservationReason::ShardRowWithoutFile
        );
        assert!(!row_observation.file_exists);
        assert!(row_observation.shard_row_exists);
        assert_eq!(row_observation.crc64, Some(row_without_file_ack.crc64));

        assert!(
            PgStore::shard_path_for_shards_dir(&store.shards_dir, &file_without_row).exists(),
            "audit-only scan must not delete file-only candidates"
        );

        store
            .register_written_shard(&file_without_row, file_without_row_ack)
            .unwrap();
        PgStore::write_shard_file_durable(
            &store.tmp_dir,
            &store.shards_dir,
            &row_without_file,
            b"row-only",
        )
        .unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert_eq!(observations.len(), 2);
        assert!(
            observations
                .iter()
                .all(|observation| observation.resolved_at.is_some()),
            "later local consistency should resolve prior local mismatch observations"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_does_not_make_negative_reference_claims() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let candidate_key = ShardKey::new(&[0xD1; 16], 42, 0);
        store
            .write_shard(&candidate_key, b"orphan-candidate")
            .unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert!(
            observations.is_empty(),
            "PgStore-local audit can only prove local row/file mismatches; negative reference \
             classification needs a cluster-wide metadata PG scan"
        );
        assert!(
            PgStore::shard_path_for_shards_dir(&store.shards_dir, &candidate_key).exists(),
            "local audit must not delete row+file candidates"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_requires_canonical_shard_file_paths() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let row_key = ShardKey::new(&[0xCF; 16], 42, 3);
        store.write_shard(&row_key, b"row").unwrap();
        fs::remove_file(PgStore::shard_path_for_shards_dir(
            &store.shards_dir,
            &row_key,
        ))
        .unwrap();

        let wrong_prefix = if row_key.hex_prefix() == "00" {
            "01"
        } else {
            "00"
        };
        let wrong_prefix_dir = store.shards_dir.join(wrong_prefix);
        fs::create_dir_all(&wrong_prefix_dir).unwrap();
        fs::write(wrong_prefix_dir.join(row_key.to_string()), b"wrong-prefix").unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].key.shard_key, row_key);
        assert_eq!(
            observations[0].reason,
            ShardScavengerObservationReason::ShardRowWithoutFile
        );
        assert!(
            observations[0].resolved_at.is_none(),
            "non-canonical file must not satisfy or resolve the canonical shard row"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_surfaces_malformed_shard_files() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let malformed_dir = store.shards_dir.join("aa");
        fs::create_dir_all(&malformed_dir).unwrap();
        fs::write(malformed_dir.join("not-a-shard-key"), b"junk").unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn shard_scavenger_local_audit_surfaces_unexpected_shard_tree_entries() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let misplaced_key = ShardKey::new(&[0xD0; 16], 42, 4);
        fs::write(
            store.shards_dir.join(misplaced_key.to_string()),
            b"misplaced",
        )
        .unwrap();
        let canonical_dir = store.shards_dir.join(misplaced_key.hex_prefix());
        fs::create_dir_all(&canonical_dir).unwrap();
        fs::create_dir(canonical_dir.join("nested")).unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }
}
