use super::*;

const PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN: usize = 54;

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
pub(crate) struct ShardInventoryRow {
    pub(crate) key: ShardKey,
    pub(crate) ack: WriteAck,
    pub(crate) status: ShardStatus,
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
                source: source.into(),
            })?;
        Ok(())
    }

    pub(crate) fn list_placed_segment_shard_repairs(
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
                source: source.into(),
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
                source: source.into(),
            })?;

        let mut repairs = Vec::new();
        for row in rows {
            let (work_item, first_seen_at, last_seen_at, observation_count, last_error) = row
                .map_err(|source| StoreError::Db {
                    context: "read placed segment shard repair",
                    source: source.into(),
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
                source: source.into(),
            })?;
        Ok(updated > 0)
    }

    /// Persist a segment-level backfill candidate between two cluster-map epochs.
    ///
    /// The row records that a scanner or planner has verified useful work for
    /// moving a segment from its historical source placement. The first desired
    /// epoch is a catch-up lower bound, not part of the logical work identity:
    /// later observations coalesce because the worker targets the current route.
    pub fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        validate_placed_segment_shard_backfill_work_item(work_item)?;
        validate_placed_segment_shard_backfill_pg(self.pg_id(), work_item)?;
        validate_placed_segment_shard_backfill_remaining_tolerance(work_item, remaining_tolerance)?;
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
                           AND source_cluster_epoch = ?4",
                        params![
                            work_item.request.data_pg_id as i64,
                            work_item.request.segment_okh.as_slice(),
                            work_item.request.segment_vid.get() as i64,
                            work_item.source_cluster_epoch.get(),
                        ],
                        placed_segment_shard_backfill_work_item_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load existing placed segment shard backfill",
                        source: source.into(),
                    })?;
                if let Some(existing) = existing {
                    validate_placed_segment_shard_backfill_coalesces_exactly(&existing, work_item)?;
                    store
                        .conn
                        .execute(
                            "UPDATE placed_segment_shard_backfills \
                             SET last_seen_at = ?5, \
                                 observation_count = observation_count + 1, \
                                 remaining_tolerance = min(remaining_tolerance, ?6), \
                                 last_error = COALESCE(?7, last_error) \
                             WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                               AND source_cluster_epoch = ?4",
                            params![
                                work_item.request.data_pg_id as i64,
                                work_item.request.segment_okh.as_slice(),
                                work_item.request.segment_vid.get() as i64,
                                work_item.source_cluster_epoch.get(),
                                now as i64,
                                i64::from(remaining_tolerance),
                                last_error,
                            ],
                        )
                        .map_err(|source| StoreError::Db {
                            context: "coalesce placed segment shard backfill",
                            source: source.into(),
                        })?;
                    return Ok(());
                }
                store
                    .conn
                    .execute(
                        "INSERT INTO placed_segment_shard_backfills \
                         (data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                          ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, \
                          remaining_tolerance, first_seen_at, last_seen_at, observation_count, \
                          last_error) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, 1, ?12)",
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
                            i64::from(remaining_tolerance),
                            now as i64,
                            last_error,
                        ],
                    )
                    .map_err(|source| StoreError::Db {
                        context: "record placed segment shard backfill",
                        source: source.into(),
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
                        ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, \
                        remaining_tolerance, first_seen_at, last_seen_at, observation_count, \
                        last_error \
                 FROM placed_segment_shard_backfills \
                 ORDER BY remaining_tolerance, source_cluster_epoch, last_seen_at, segment_okh, \
                          segment_vid, desired_cluster_epoch \
                 LIMIT ?1",
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard backfills (prepare)",
                source: source.into(),
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
                        row.get::<_, i64>(12)?,
                        row.get::<_, Option<String>>(13)?,
                    ))
                },
            )
            .map_err(|source| StoreError::Db {
                context: "list placed segment shard backfills",
                source: source.into(),
            })?;

        let mut backfills = Vec::new();
        for row in rows {
            let (
                work_item,
                remaining_tolerance,
                first_seen_at,
                last_seen_at,
                observation_count,
                last_error,
            ) = row.map_err(|source| StoreError::Db {
                context: "read placed segment shard backfill",
                source: source.into(),
            })?;
            let remaining_tolerance =
                u8::try_from(remaining_tolerance).map_err(|source| StoreError::Db {
                    context: "read placed segment shard backfill remaining tolerance",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
                })?;
            validate_placed_segment_shard_backfill_remaining_tolerance(
                &work_item,
                remaining_tolerance,
            )?;
            backfills.push(PlacedSegmentShardBackfillRecord {
                work_item,
                remaining_tolerance,
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
                source: source.into(),
            })?;
        usize::try_from(count).map_err(|source| StoreError::Db {
            context: "count placed segment shard backfills range",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(source),
            ),
        })
    }

    pub fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        validate_placed_segment_shard_backfill_work_item(work_item)?;
        validate_placed_segment_shard_backfill_pg(self.pg_id(), work_item)?;
        let existing = self
            .conn
            .query_row(
                "SELECT data_pg_id, segment_okh, segment_vid, stored_size, segment_crc64, \
                        ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch \
                 FROM placed_segment_shard_backfills \
                 WHERE data_pg_id = ?1 AND segment_okh = ?2 AND segment_vid = ?3 \
                   AND source_cluster_epoch = ?4",
                params![
                    work_item.request.data_pg_id as i64,
                    work_item.request.segment_okh.as_slice(),
                    work_item.request.segment_vid.get() as i64,
                    work_item.source_cluster_epoch.get(),
                ],
                placed_segment_shard_backfill_work_item_from_row,
            )
            .optional()
            .map_err(|source| StoreError::Db {
                context: "check placed segment shard backfill exists",
                source: source.into(),
            })?;
        let Some(existing) = existing else {
            return Ok(false);
        };
        validate_placed_segment_shard_backfill_coalesces_exactly(&existing, work_item)?;
        Ok(true)
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
                source: source.into(),
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
        if request.lease_deadline <= request.claimed_at {
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
            request.lease_deadline,
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
                        source: source.into(),
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
                         ORDER BY remaining_tolerance, source_cluster_epoch, last_seen_at, \
                                  segment_okh, segment_vid, desired_cluster_epoch \
                         LIMIT 1",
                        params![now],
                        placed_segment_shard_backfill_work_item_from_row,
                    )
                    .optional()
                    .map_err(|source| StoreError::Db {
                        context: "load claimable placed segment shard backfill",
                        source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                source: source.into(),
            })?;
        Ok(updated > 0)
    }

    pub(crate) fn acquire_placed_segment_shard_repair_claim(
        &self,
        request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        validate_placed_segment_shard_repair_claim_identity(
            &request.claim_id,
            &request.owner_token,
        )?;
        if request.lease_deadline <= request.claimed_at {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "durable repair claim lease deadline must be after claimed_at".to_string(),
            });
        }
        let claimed_at = durable_repair_u64_to_i64(
            request.claimed_at,
            "acquire placed segment shard repair claim claimed_at",
        )?;
        let lease_deadline = durable_repair_u64_to_i64(
            request.lease_deadline,
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
                        source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
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
                        source: source.into(),
                    })
            },
        )
    }

    pub(crate) fn complete_placed_segment_shard_repair_claim(
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
                source: source.into(),
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
                source: source.into(),
            })?;
        let result = body(self);
        match result {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|source| StoreError::Db {
                        context: commit_context,
                        source: source.into(),
                    })?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(crate) fn record_placed_segment_shard_repair_claim_error(
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
                source: source.into(),
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
                source: source.into(),
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
                source: source.into(),
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
                source: source.into(),
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
                source: source.into(),
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
                source: source.into(),
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
                source: source.into(),
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

    pub(crate) fn list_placed_segment_backfill_reference_page(
        &self,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: std::num::NonZeroU16,
    ) -> Result<PlacedSegmentBackfillReferencePage, StoreError> {
        debug_assert!(limit.get() <= PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT);
        let limit = usize::from(
            limit
                .get()
                .min(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT),
        );
        let mut items = Vec::with_capacity(limit);
        let start_phase = match after {
            None | Some(PlacedSegmentBackfillReferenceCursor::ObjectSegment { .. }) => 0,
            Some(PlacedSegmentBackfillReferenceCursor::StreamUploadSegment { .. }) => 1,
            Some(PlacedSegmentBackfillReferenceCursor::MultipartPartSegment { .. }) => 2,
            Some(PlacedSegmentBackfillReferenceCursor::PendingCommand { .. }) => 3,
        };

        if start_phase == 0 {
            let object_after = match after {
                Some(PlacedSegmentBackfillReferenceCursor::ObjectSegment {
                    bucket,
                    key,
                    version_id,
                    segment_index,
                }) => (bucket.as_str(), key.as_str(), *version_id, *segment_index),
                _ => ("", "", 0, 0),
            };
            let remaining = limit - items.len();
            self.extend_placed_segment_backfill_object_page(&mut items, object_after, remaining)?;
            if items.len() == limit {
                return Ok(PlacedSegmentBackfillReferencePage {
                    items,
                    complete: false,
                });
            }
        }

        if start_phase <= 1 {
            let stream_after = match after {
                Some(PlacedSegmentBackfillReferenceCursor::StreamUploadSegment {
                    session_id,
                    segment_index,
                }) => (session_id.as_str(), *segment_index),
                _ => ("", 0),
            };
            let remaining = limit - items.len();
            self.extend_placed_segment_backfill_stream_page(&mut items, stream_after, remaining)?;
            if items.len() == limit {
                return Ok(PlacedSegmentBackfillReferencePage {
                    items,
                    complete: false,
                });
            }
        }

        if start_phase <= 2 {
            let multipart_after = match after {
                Some(PlacedSegmentBackfillReferenceCursor::MultipartPartSegment {
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    segment_index,
                }) => (
                    bucket.as_str(),
                    key.as_str(),
                    upload_id.as_str(),
                    *part_number,
                    *segment_index,
                ),
                _ => ("", "", "", 0, 0),
            };
            let remaining = limit - items.len();
            self.extend_placed_segment_backfill_multipart_page(
                &mut items,
                multipart_after,
                remaining,
            )?;
            if items.len() == limit {
                return Ok(PlacedSegmentBackfillReferencePage {
                    items,
                    complete: false,
                });
            }
        }

        let remaining = limit - items.len();
        let complete =
            self.extend_placed_segment_backfill_pending_page(&mut items, after, remaining)?;
        Ok(PlacedSegmentBackfillReferencePage { items, complete })
    }

    fn extend_placed_segment_backfill_object_page(
        &self,
        items: &mut Vec<PlacedSegmentBackfillReferencePageItem>,
        after: (&str, &str, u64, u32),
        limit: usize,
    ) -> Result<(), StoreError> {
        if limit == 0 {
            return Ok(());
        }
        let version_id = i64::try_from(after.2).map_err(|_| StoreError::Db {
            context: "list object segment backfill reference page",
            source: crate::error::DatabaseError::new("object version cursor exceeds SQLite range"),
        })?;
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT s.bucket, s.key, s.version_id, s.segment_index, \
                        s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                        s.placement_cluster_epoch, s.ec_k, s.ec_m, o.encryption_type \
                 FROM object_segments s \
                 JOIN objects o ON o.bucket = s.bucket AND o.key = s.key AND o.version_id = s.version_id \
                 WHERE (s.bucket, s.key, s.version_id, s.segment_index) > (?1, ?2, ?3, ?4) \
                 ORDER BY s.bucket, s.key, s.version_id, s.segment_index \
                 LIMIT ?5",
            )
            .map_err(|source| StoreError::Db {
                context: "list object segment backfill reference page",
                source: source.into(),
            })?;
        let sql_limit = i64::try_from(limit).expect("backfill reference page limit fits in i64");
        let rows = statement
            .query_map(
                params![after.0, after.1, version_id, i64::from(after.3), sql_limit],
                |row| {
                    let bucket: BucketName = row.get(0)?;
                    let key: ObjectKey = row.get(1)?;
                    let raw_version_id = row.get::<_, i64>(2)?;
                    let version_id = u64::try_from(raw_version_id)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, raw_version_id))?;
                    let segment_index = row.get(3)?;
                    Ok(PlacedSegmentBackfillReferencePageItem {
                        cursor: PlacedSegmentBackfillReferenceCursor::ObjectSegment {
                            bucket,
                            key,
                            version_id,
                            segment_index,
                        },
                        reference: self.placed_segment_backfill_reference_from_row(row, 4, 12)?,
                    })
                },
            )
            .map_err(|source| StoreError::Db {
                context: "list object segment backfill reference page",
                source: source.into(),
            })?;
        for row in rows {
            items.push(row.map_err(|source| StoreError::Db {
                context: "list object segment backfill reference page",
                source: source.into(),
            })?);
        }
        Ok(())
    }

    fn extend_placed_segment_backfill_stream_page(
        &self,
        items: &mut Vec<PlacedSegmentBackfillReferencePageItem>,
        after: (&str, u32),
        limit: usize,
    ) -> Result<(), StoreError> {
        if limit == 0 {
            return Ok(());
        }
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT s.session_id, s.segment_index, \
                        s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                        s.placement_cluster_epoch, s.ec_k, s.ec_m, u.encryption_type \
                 FROM stream_upload_segments s \
                 JOIN stream_uploads u ON u.session_id = s.session_id \
                 WHERE (s.session_id, s.segment_index) > (?1, ?2) \
                 ORDER BY s.session_id, s.segment_index \
                 LIMIT ?3",
            )
            .map_err(|source| StoreError::Db {
                context: "list stream segment backfill reference page",
                source: source.into(),
            })?;
        let sql_limit = i64::try_from(limit).expect("backfill reference page limit fits in i64");
        let rows = statement
            .query_map(params![after.0, i64::from(after.1), sql_limit], |row| {
                Ok(PlacedSegmentBackfillReferencePageItem {
                    cursor: PlacedSegmentBackfillReferenceCursor::StreamUploadSegment {
                        session_id: row.get(0)?,
                        segment_index: row.get(1)?,
                    },
                    reference: self.placed_segment_backfill_reference_from_row(row, 2, 10)?,
                })
            })
            .map_err(|source| StoreError::Db {
                context: "list stream segment backfill reference page",
                source: source.into(),
            })?;
        for row in rows {
            items.push(row.map_err(|source| StoreError::Db {
                context: "list stream segment backfill reference page",
                source: source.into(),
            })?);
        }
        Ok(())
    }

    fn extend_placed_segment_backfill_multipart_page(
        &self,
        items: &mut Vec<PlacedSegmentBackfillReferencePageItem>,
        after: (&str, &str, &str, u32, u32),
        limit: usize,
    ) -> Result<(), StoreError> {
        if limit == 0 {
            return Ok(());
        }
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT s.bucket, s.key, s.upload_id, s.part_number, s.segment_index, \
                        s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                        s.placement_cluster_epoch, s.ec_k, s.ec_m, o.encryption_type \
                 FROM multipart_part_segments s \
                 JOIN objects o ON o.bucket = s.bucket AND o.key = s.key AND o.version_id = s.version_id \
                 WHERE (s.bucket, s.key, s.upload_id, s.part_number, s.segment_index) \
                       > (?1, ?2, ?3, ?4, ?5) \
                 ORDER BY s.bucket, s.key, s.upload_id, s.part_number, s.segment_index \
                 LIMIT ?6",
            )
            .map_err(|source| StoreError::Db {
                context: "list multipart segment backfill reference page",
                source: source.into(),
            })?;
        let sql_limit = i64::try_from(limit).expect("backfill reference page limit fits in i64");
        let rows = statement
            .query_map(
                params![
                    after.0,
                    after.1,
                    after.2,
                    i64::from(after.3),
                    i64::from(after.4),
                    sql_limit
                ],
                |row| {
                    Ok(PlacedSegmentBackfillReferencePageItem {
                        cursor: PlacedSegmentBackfillReferenceCursor::MultipartPartSegment {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            part_number: row.get(3)?,
                            segment_index: row.get(4)?,
                        },
                        reference: self.placed_segment_backfill_reference_from_row(row, 5, 13)?,
                    })
                },
            )
            .map_err(|source| StoreError::Db {
                context: "list multipart segment backfill reference page",
                source: source.into(),
            })?;
        for row in rows {
            items.push(row.map_err(|source| StoreError::Db {
                context: "list multipart segment backfill reference page",
                source: source.into(),
            })?);
        }
        Ok(())
    }

    fn extend_placed_segment_backfill_pending_page(
        &self,
        items: &mut Vec<PlacedSegmentBackfillReferencePageItem>,
        after: Option<&PlacedSegmentBackfillReferenceCursor>,
        limit: usize,
    ) -> Result<bool, StoreError> {
        if limit == 0 {
            return Ok(false);
        }
        let slot = self.query_row_cached_optional(
            "SELECT cluster_epoch, pg_id, log_index, command_checksum, \
                    placed_segment_reference_count \
             FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
            "load pending command identity for placed segment backfill reference page",
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?;
        let Some((cluster_epoch, pg_id, log_index, command_checksum, reference_count)) = slot
        else {
            return Ok(true);
        };
        let cluster_epoch = u64::try_from(cluster_epoch)
            .ok()
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| Self::invalid_pending_placed_reference_index("invalid cluster epoch"))?;
        let pg_id = u32::try_from(pg_id)
            .map(PgId::new)
            .map_err(|_| Self::invalid_pending_placed_reference_index("invalid PG ID"))?;
        if pg_id.get() != self.pg_id {
            return Err(Self::invalid_pending_placed_reference_index(
                "pending command belongs to another PG",
            ));
        }
        let log_index = u64::try_from(log_index)
            .map_err(|_| Self::invalid_pending_placed_reference_index("negative log index"))?;
        let command_checksum = command_checksum as u64;
        let total_references = u32::try_from(reference_count)
            .map_err(|_| Self::invalid_pending_placed_reference_index("invalid reference count"))?
            as usize;
        let start = match after {
            Some(PlacedSegmentBackfillReferenceCursor::PendingCommand {
                cluster_epoch: cursor_epoch,
                pg_id: cursor_pg_id,
                log_index: cursor_log_index,
                command_checksum: cursor_checksum,
                reference_index,
            }) if *cursor_epoch == cluster_epoch
                && *cursor_pg_id == pg_id
                && *cursor_log_index == log_index
                && *cursor_checksum == command_checksum =>
            {
                *reference_index as usize + 1
            }
            _ => 0,
        };
        if start >= total_references {
            return Ok(true);
        }

        let references_per_page = usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT);
        let first_page = start / references_per_page;
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT page_index, reference_count, encoded_references \
                 FROM metadata_command_pending_placed_reference_pages \
                 WHERE singleton = 0 AND page_index >= ?1 \
                 ORDER BY page_index LIMIT 2",
            )
            .map_err(|source| StoreError::Db {
                context: "list pending command placed segment backfill reference pages",
                source: source.into(),
            })?;
        let rows = statement
            .query_map(params![first_page as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list pending command placed segment backfill reference pages",
                source: source.into(),
            })?;
        let mut next_reference = start;
        for row in rows {
            let (page_index, page_reference_count, encoded_page) =
                row.map_err(|source| StoreError::Db {
                    context: "read pending command placed segment backfill reference page",
                    source: source.into(),
                })?;
            let page_index = usize::try_from(page_index)
                .map_err(|_| Self::invalid_pending_placed_reference_index("negative page index"))?;
            let expected_page_index = next_reference / references_per_page;
            if page_index != expected_page_index {
                return Err(Self::invalid_pending_placed_reference_index(
                    "non-contiguous page index",
                ));
            }
            let page_reference_count = usize::try_from(page_reference_count).map_err(|_| {
                Self::invalid_pending_placed_reference_index("negative page reference count")
            })?;
            let page_start = page_index * references_per_page;
            let expected_page_count = (total_references - page_start).min(references_per_page);
            if page_reference_count != expected_page_count
                || encoded_page.len()
                    != page_reference_count * PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN
            {
                return Err(Self::invalid_pending_placed_reference_index(
                    "malformed fixed-width page",
                ));
            }
            let skip = next_reference - page_start;
            for encoded in encoded_page
                .chunks_exact(PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN)
                .skip(skip)
                .take(limit - (next_reference - start))
            {
                let reference_index = next_reference;
                items.push(PlacedSegmentBackfillReferencePageItem {
                    cursor: PlacedSegmentBackfillReferenceCursor::PendingCommand {
                        cluster_epoch,
                        pg_id,
                        log_index,
                        command_checksum,
                        reference_index: u32::try_from(reference_index)
                            .expect("metadata command reference count must fit in u32"),
                    },
                    reference: Self::decode_pending_placed_reference(encoded)?,
                });
                next_reference += 1;
            }
            if next_reference - start == limit || next_reference == total_references {
                break;
            }
        }
        if next_reference == start {
            return Err(Self::invalid_pending_placed_reference_index(
                "missing encoded reference page",
            ));
        }
        Ok(next_reference >= total_references)
    }

    pub(super) fn encode_pending_placed_segment_reference_pages(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(u32, Vec<Vec<u8>>), StoreError> {
        let mut references = Vec::new();
        self.extend_scavenger_command_payload_references(&mut references, command.payload())?;
        let references_per_page = usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT);
        let mut reference_count = 0u32;
        let mut pages: Vec<Vec<u8>> = Vec::new();
        for reference in references {
            let ShardScavengerPayloadReference::Placed(reference) = reference else {
                continue;
            };
            if (reference_count as usize).is_multiple_of(references_per_page) {
                pages.push(Vec::with_capacity(
                    references_per_page * PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN,
                ));
            }
            let encoded = pages.last_mut().expect("placed reference page exists");
            encoded.extend_from_slice(&reference.data_pg_id.to_be_bytes());
            encoded.extend_from_slice(&reference.okh);
            encoded.extend_from_slice(&reference.generation_id.get().to_be_bytes());
            encoded.extend_from_slice(&reference.placement_cluster_epoch.get().to_be_bytes());
            encoded.extend_from_slice(&reference.stored_size.to_be_bytes());
            encoded.extend_from_slice(&reference.crc64.to_be_bytes());
            encoded.push(reference.ec.k);
            encoded.push(reference.ec.m);
            reference_count = reference_count.checked_add(1).ok_or_else(|| {
                Self::invalid_pending_placed_reference_index("reference count overflow")
            })?;
        }
        Ok((reference_count, pages))
    }

    fn decode_pending_placed_reference(
        encoded: &[u8],
    ) -> Result<ShardScavengerPlacedShardSetReference, StoreError> {
        debug_assert_eq!(encoded.len(), PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN);
        let data_pg_id = u32::from_be_bytes(encoded[0..4].try_into().unwrap());
        let okh = encoded[4..20].try_into().unwrap();
        let generation_id = GenerationId::new(u64::from_be_bytes(
            encoded[20..28].try_into().unwrap(),
        ))
        .ok_or_else(|| Self::invalid_pending_placed_reference_index("zero generation id"))?;
        let placement_cluster_epoch = ClusterEpoch::new(u64::from_be_bytes(
            encoded[28..36].try_into().unwrap(),
        ))
        .ok_or_else(|| Self::invalid_pending_placed_reference_index("zero placement epoch"))?;
        let stored_size = u64::from_be_bytes(encoded[36..44].try_into().unwrap());
        let crc64 = u64::from_be_bytes(encoded[44..52].try_into().unwrap());
        Ok(ShardScavengerPlacedShardSetReference {
            data_pg_id,
            okh,
            generation_id,
            placement_cluster_epoch,
            stored_size,
            crc64,
            ec: EcShape {
                k: encoded[52],
                m: encoded[53],
            },
        })
    }

    fn invalid_pending_placed_reference_index(reason: &str) -> StoreError {
        StoreError::ShardScavengerScanIncomplete {
            context: "decode pending command placed segment reference index",
            errors: reason.to_owned(),
        }
    }

    fn placed_segment_backfill_reference_from_row(
        &self,
        row: &rusqlite::Row<'_>,
        reference_start: usize,
        encryption_type_index: usize,
    ) -> rusqlite::Result<ShardScavengerPlacedShardSetReference> {
        let okh_blob: Vec<u8> = row.get(reference_start + 1)?;
        Ok(ShardScavengerPlacedShardSetReference {
            data_pg_id: row.get(reference_start)?,
            okh: Self::parse_okh_blob(&okh_blob, reference_start + 1)?,
            generation_id: Self::parse_generation_id(
                row.get(reference_start + 2)?,
                reference_start + 2,
                "backfill reference generation",
            )?,
            stored_size: Self::stored_segment_size_for_encryption_type(
                row.get::<_, i64>(reference_start + 3)? as u64,
                row.get(encryption_type_index)?,
            )?,
            crc64: row.get::<_, i64>(reference_start + 4)? as u64,
            placement_cluster_epoch: Self::parse_cluster_epoch(
                row.get(reference_start + 5)?,
                reference_start + 5,
                "placement_cluster_epoch",
            )?,
            ec: EcShape {
                k: row.get(reference_start + 6)?,
                m: row.get(reference_start + 7)?,
            },
        })
    }

    pub(crate) fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let mut references = Vec::new();
        self.extend_scavenger_encrypted_placed_references(
            &mut references,
            "SELECT s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                    s.placement_cluster_epoch, s.ec_k, s.ec_m, o.encryption_type \
             FROM object_segments s \
             JOIN objects o ON o.bucket = s.bucket AND o.key = s.key AND o.version_id = s.version_id",
            "list object segment shard scavenger references",
        )?;
        self.extend_scavenger_encrypted_placed_references(
            &mut references,
            "SELECT s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                    s.placement_cluster_epoch, s.ec_k, s.ec_m, u.encryption_type \
             FROM stream_upload_segments s \
             JOIN stream_uploads u ON u.session_id = s.session_id",
            "list stream upload segment shard scavenger references",
        )?;
        self.extend_scavenger_encrypted_placed_references(
            &mut references,
            "SELECT s.data_pg_id, s.segment_okh, s.segment_vid, s.size, s.segment_crc64, \
                    s.placement_cluster_epoch, s.ec_k, s.ec_m, o.encryption_type \
             FROM multipart_part_segments s \
             JOIN objects o ON o.bucket = s.bucket AND o.key = s.key AND o.version_id = s.version_id",
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
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m \
             FROM multipart_reclaim_part_segments",
            "list multipart reclaim segment shard scavenger references",
        )?;
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
        let command = decode_metadata_command_envelope(
            &command_bytes,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|reason| StoreError::ShardScavengerScanIncomplete {
            context: "decode pending metadata command for shard scavenger references",
            errors: reason,
        })?;
        self.extend_scavenger_command_payload_references(references, command.payload())
    }

    fn extend_scavenger_command_payload_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        payload: &MetadataCommandPayload,
    ) -> Result<(), StoreError> {
        match payload {
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                Self::extend_object_segment_references(
                    references,
                    &command.segments,
                    &command.object.encryption,
                );
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.selected_streaming_segments,
                    &command.object.encryption,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.omitted_streaming_segments,
                    &command.object.encryption,
                );
                Self::extend_terminal_stream_segment_references(
                    references,
                    &command.stream_upload_segments,
                    &command.stream_uploads,
                );
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
                if let Some(encryption) =
                    self.load_stream_upload_encryption_for_session(&command.segment.session_id)?
                {
                    Self::extend_stream_segment_reference(
                        references,
                        &command.segment,
                        &encryption,
                    );
                }
            }
            MetadataCommandPayload::AbortStreamUpload(command) => {
                if let Some(encryption) =
                    self.load_stream_upload_encryption_for_session(&command.session_id)?
                {
                    Self::extend_stream_segment_references(
                        references,
                        &command.staged_segments,
                        &encryption,
                    );
                }
            }
            MetadataCommandPayload::CommitStreamPart(command) => {
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.segments,
                    &command.upload.encryption,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.displaced_segments,
                    &command.upload.encryption,
                );
            }
            MetadataCommandPayload::AbortMultipartUpload(command) => {
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.cleanup.streaming_segments,
                    &command.cleanup.upload.encryption,
                );
                Self::extend_terminal_stream_segment_references(
                    references,
                    &command.cleanup.stream_upload_segments,
                    &command.cleanup.stream_uploads,
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
            | MetadataCommandPayload::DeleteFinalizedBucket(_)
            | MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::ReleaseObjectGeneration(_)
            | MetadataCommandPayload::ReserveObjectVersion(_)
            | MetadataCommandPayload::PutObjectMetadata(_)
            | MetadataCommandPayload::CreateStreamUpload(_)
            | MetadataCommandPayload::CreateMultipartUpload(_)
            | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {}
        }
        Ok(())
    }

    fn extend_object_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[ObjectSegmentRecord],
        encryption: &ObjectEncryption,
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                ShardScavengerPlacedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    stored_size: Self::stored_segment_size_for_encryption(segment.size, encryption),
                    crc64: segment.segment_crc64,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
            );
        }
    }

    fn extend_stream_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[StreamUploadSegmentRecord],
        encryption: &ObjectEncryption,
    ) {
        for segment in segments {
            Self::extend_stream_segment_reference(references, segment, encryption);
        }
    }

    fn extend_stream_segment_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segment: &StreamUploadSegmentRecord,
        encryption: &ObjectEncryption,
    ) {
        Self::push_placed_reference(
            references,
            ShardScavengerPlacedShardSetReference {
                data_pg_id: segment.data_pg_id,
                okh: segment.segment_okh,
                generation_id: segment.segment_vid,
                placement_cluster_epoch: segment.placement_cluster_epoch,
                stored_size: Self::stored_segment_size_for_encryption(segment.size, encryption),
                crc64: segment.segment_crc64,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
        );
    }

    fn extend_terminal_stream_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[StreamUploadSegmentRecord],
        stream_uploads: &[TerminalStreamCleanupRecord],
    ) {
        for segment in segments {
            let Some(stream_upload) = stream_uploads
                .iter()
                .find(|upload| upload.session_id == segment.session_id)
            else {
                continue;
            };
            Self::extend_stream_segment_reference(references, segment, &stream_upload.encryption);
        }
    }

    fn extend_multipart_part_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[MultipartPartSegmentRecord],
        encryption: &ObjectEncryption,
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                ShardScavengerPlacedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    stored_size: Self::stored_segment_size_for_encryption(segment.size, encryption),
                    crc64: segment.segment_crc64,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
            );
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
                    for segment in &part.segments {
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

    fn push_placed_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        reference: ShardScavengerPlacedShardSetReference,
    ) {
        references.push(ShardScavengerPayloadReference::Placed(reference));
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
        Ok(self
            .list_shard_inventory_rows()?
            .into_iter()
            .map(|row| ScavengerShardRow {
                key: row.key,
                ack: row.ack,
            })
            .collect())
    }

    pub(crate) fn list_shard_inventory_rows(&self) -> Result<Vec<ShardInventoryRow>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT shard_key, data_size, crc64_nvme, status \
                 FROM shards \
                 ORDER BY shard_key",
            )
            .map_err(|source| StoreError::Db {
                context: "list shard inventory rows (prepare)",
                source: source.into(),
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list shard inventory rows",
                source: source.into(),
            })?;
        let mut inventory_rows = Vec::new();
        for row in rows {
            let (key, stored_size, crc64, status) = row.map_err(|source| StoreError::Db {
                context: "read shard inventory row",
                source: source.into(),
            })?;
            let status = u8::try_from(status)
                .ok()
                .and_then(ShardStatus::from_u8)
                .ok_or_else(|| StoreError::ShardScavengerScanIncomplete {
                    context: "read shard inventory rows",
                    errors: format!("shard row has invalid status {status}"),
                })?;
            inventory_rows.push(ShardInventoryRow {
                key: ShardKey::from_bytes(&key)?,
                ack: WriteAck {
                    stored_size: stored_size as u64,
                    crc64: crc64 as u64,
                },
                status,
            });
        }
        Ok(inventory_rows)
    }

    fn extend_scavenger_encrypted_placed_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        sql: &'static str,
        context: &'static str,
    ) -> Result<(), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
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
                        placement_cluster_epoch: PgStore::parse_cluster_epoch(
                            row.get::<_, i64>(5)?,
                            5,
                            "placement_cluster_epoch",
                        )?,
                        stored_size: Self::stored_segment_size_for_encryption_type(
                            row.get::<_, i64>(3)? as u64,
                            row.get(8)?,
                        )?,
                        crc64: row.get::<_, i64>(4)? as u64,
                        ec: EcShape {
                            k: row.get(6)?,
                            m: row.get(7)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?);
        }
        Ok(())
    }

    fn stored_segment_size_for_encryption(logical_size: u64, encryption: &ObjectEncryption) -> u64 {
        logical_size
            .checked_add(encryption.segment_ciphertext_extra_len() as u64)
            .expect("stored segment size overflow")
    }

    fn stored_segment_size_for_encryption_type(
        logical_size: u64,
        encryption_type: u8,
    ) -> rusqlite::Result<u64> {
        let encryption_type = ObjectEncryptionType::from_u8(encryption_type).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid object encryption type: {encryption_type}")),
            )
        })?;
        let extra = match encryption_type {
            ObjectEncryptionType::None => 0,
            ObjectEncryptionType::SseCustomer | ObjectEncryptionType::SseS3 => {
                OBJECT_ENCRYPTION_SEGMENT_TAG_LEN as u64
            }
        };
        logical_size.checked_add(extra).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::from("stored segment size overflow"),
            )
        })
    }

    fn load_stream_upload_encryption_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<ObjectEncryption>, StoreError> {
        self.query_row_cached_optional(
            "SELECT encryption_type, encryption_state \
             FROM stream_uploads WHERE session_id = ?1",
            params![session_id.as_str()],
            "load stream upload encryption for shard scavenger pending references",
            |row| {
                Self::parse_object_encryption(
                    row.get::<_, u8>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    0,
                    1,
                )
            },
        )
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
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
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
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?);
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

fn validate_placed_segment_shard_backfill_remaining_tolerance(
    work_item: &PlacedSegmentShardBackfillWorkItem,
    remaining_tolerance: u8,
) -> Result<(), StoreError> {
    if remaining_tolerance > work_item.request.ec.m {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "durable backfill remaining tolerance {} exceeds EC m={}",
                remaining_tolerance, work_item.request.ec.m
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
    validate_placed_segment_shard_backfill_remaining_tolerance(
        &claim.work_item,
        claim.remaining_tolerance,
    )?;
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
        source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
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
                ec_k, ec_m, source_cluster_epoch, desired_cluster_epoch, \
                remaining_tolerance, claim_id, owner_token, cluster_epoch, claimed_at, \
                lease_deadline, attempt_count, last_error \
         FROM placed_segment_shard_backfills \
         WHERE {where_clause} \
         ORDER BY remaining_tolerance, source_cluster_epoch, last_seen_at, segment_okh, \
                  segment_vid, desired_cluster_epoch \
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
    let remaining_tolerance = row.get::<_, i64>(9)?;
    let remaining_tolerance = u8::try_from(remaining_tolerance).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            9,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable backfill claim row has invalid remaining tolerance",
            )),
        )
    })?;
    validate_placed_segment_shard_backfill_remaining_tolerance(&work_item, remaining_tolerance)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Integer,
                Box::new(std::io::Error::other(error.to_string())),
            )
        })?;
    let cluster_epoch = row.get::<_, i64>(12)?;
    let cluster_epoch = ClusterEpoch::new(cluster_epoch as u64).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(
                "durable backfill claim row has invalid cluster epoch",
            )),
        )
    })?;
    Ok(PlacedSegmentShardBackfillClaimRecord {
        work_item,
        remaining_tolerance,
        claim_id: row.get(10)?,
        owner_token: row.get(11)?,
        cluster_epoch,
        claimed_at: row.get::<_, i64>(13)? as u64,
        lease_deadline: row
            .get::<_, Option<i64>>(14)?
            .map(|deadline| deadline as u64),
        attempt_count: row.get::<_, i64>(15)? as u64,
        last_error: row.get(16)?,
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
        lease_deadline: u64,
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
        lease_deadline: u64,
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
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, Some("first"))
            .unwrap();
        store
            .record_placed_segment_shard_backfill(
                &work_item,
                work_item.request.ec.m,
                Some("second"),
            )
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();
        assert!(store
            .placed_segment_shard_backfill_exists(&work_item)
            .unwrap());

        drop(store);
        let reopened = PgStore::open(tmp.path(), 7).unwrap();
        let backfills = reopened.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].remaining_tolerance, work_item.request.ec.m);
        assert_eq!(backfills[0].observation_count, 3);
        assert_eq!(backfills[0].last_error.as_deref(), Some("second"));
        assert!(reopened
            .placed_segment_shard_backfill_exists(&work_item)
            .unwrap());

        assert!(reopened
            .resolve_placed_segment_shard_backfill(&work_item)
            .unwrap());
        assert!(!reopened
            .placed_segment_shard_backfill_exists(&work_item)
            .unwrap());
        assert!(reopened
            .list_placed_segment_shard_backfills()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placed_segment_shard_backfill_coalescing_keeps_lowest_remaining_tolerance() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xC2; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };

        store
            .record_placed_segment_shard_backfill(&work_item, 2, None)
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, 0, None)
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&work_item, 1, None)
            .unwrap();

        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].remaining_tolerance, 0);
        assert_eq!(backfills[0].observation_count, 3);
    }

    #[test]
    fn placed_segment_shard_backfill_coalesces_superseding_desired_epoch() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xC3; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let superseding = PlacedSegmentShardBackfillWorkItem {
            desired_cluster_epoch: ClusterEpoch::new(9).unwrap(),
            ..work_item
        };

        store
            .record_placed_segment_shard_backfill(&work_item, 2, None)
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&superseding, 1, None)
            .unwrap();

        assert!(store
            .placed_segment_shard_backfill_exists(&superseding)
            .unwrap());
        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].remaining_tolerance, 1);
        assert_eq!(backfills[0].observation_count, 2);
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
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, Some("first"))
            .unwrap();
        assert!(matches!(
            store.record_placed_segment_shard_backfill(
                &mismatched,
                mismatched.request.ec.m,
                Some("second")
            ),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));

        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].remaining_tolerance, work_item.request.ec.m);
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
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();
        assert!(!store.resolve_placed_segment_shard_backfill(&stale).unwrap());

        let backfills = store.list_placed_segment_shard_backfills().unwrap();
        assert_eq!(backfills.len(), 1);
        assert_eq!(backfills[0].work_item, work_item);
        assert_eq!(backfills[0].remaining_tolerance, work_item.request.ec.m);

        assert!(store
            .resolve_placed_segment_shard_backfill(&work_item)
            .unwrap());
    }

    #[test]
    fn shard_scavenger_payload_references_include_segment_size_crc_and_epoch() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        store
            .conn
            .execute(
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, \
                  last_modified, ec_k, ec_m, status, data_layout, parts_count, encryption_type, \
                  owner_principal, owner_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                rusqlite::params![
                    "bucket",
                    "object",
                    0i64,
                    1i64,
                    1i64,
                    1234i64,
                    b"etag".as_slice(),
                    0i64,
                    1i64,
                    4i64,
                    2i64,
                    0i64,
                    1i64,
                    1i64,
                    2i64,
                    "owner",
                    "c".repeat(32),
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, payload_crc64, \
                  etag, etag_kind, part_vid, placement_cluster_epoch, ec_k, ec_m, data_pg_id) \
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
                    9i64,
                    3i64,
                    4i64,
                    2i64,
                    7i64,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, \
                  segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                rusqlite::params![
                    "bucket",
                    "object",
                    "u".repeat(128),
                    0i64,
                    1i64,
                    0i64,
                    1234i64,
                    0xAABB_i64,
                    [0x11u8; 16].as_slice(),
                    9i64,
                    7i64,
                    3i64,
                    4i64,
                    2i64,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, \
                  last_modified, ec_k, ec_m, status, data_layout, encryption_type, \
                  owner_principal, owner_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                rusqlite::params![
                    "bucket",
                    "segment-object",
                    0i64,
                    1i64,
                    2i64,
                    1000i64,
                    b"etag".as_slice(),
                    0i64,
                    1i64,
                    4i64,
                    2i64,
                    0i64,
                    0i64,
                    2i64,
                    "owner",
                    "c".repeat(32),
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO object_segments \
                 (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, \
                  segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                rusqlite::params![
                    "bucket",
                    "segment-object",
                    0i64,
                    0i64,
                    1000i64,
                    0xDDDD_i64,
                    [0x44u8; 16].as_slice(),
                    12i64,
                    7i64,
                    6i64,
                    4i64,
                    2i64,
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
        let mut page_after = None;
        let mut paged_placed = Vec::new();
        loop {
            let page = store
                .list_placed_segment_backfill_reference_page(
                    page_after.as_ref(),
                    std::num::NonZeroU16::new(1).unwrap(),
                )
                .unwrap();
            if let Some(item) = page.items.into_iter().next() {
                page_after = Some(item.cursor);
                paged_placed.push(item.reference);
            }
            if page.complete {
                break;
            }
        }
        assert_eq!(paged_placed.len(), 2);
        assert!(paged_placed
            .iter()
            .any(|reference| reference.okh == [0x44; 16]));
        assert!(paged_placed
            .iter()
            .any(|reference| reference.okh == [0x11; 16]));
        assert!(paged_placed
            .iter()
            .all(|reference| reference.okh != [0x33; 16]));
        let object_segment = references
            .iter()
            .find_map(|reference| match reference {
                ShardScavengerPayloadReference::Placed(reference)
                    if reference.okh == [0x44; 16] =>
                {
                    Some(reference)
                }
                _ => None,
            })
            .expect("object segment placed reference should be listed");
        assert_eq!(
            object_segment.stored_size,
            1000 + OBJECT_ENCRYPTION_SEGMENT_TAG_LEN as u64
        );
        assert_eq!(object_segment.crc64, 0xDDDD);
        assert_eq!(
            object_segment.placement_cluster_epoch,
            ClusterEpoch::new(6).unwrap()
        );

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
        assert_eq!(
            object_part.stored_size,
            1234 + OBJECT_ENCRYPTION_SEGMENT_TAG_LEN as u64
        );
        assert_eq!(object_part.crc64, 0xAABB);
        assert_eq!(
            object_part.placement_cluster_epoch,
            ClusterEpoch::new(3).unwrap()
        );

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
    fn pending_command_backfill_reference_pages_are_bounded_and_restart_after_replacement() {
        const REFERENCE_COUNT: usize = 512;

        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let bucket = trusted_bucket_name("pending-backfill-page");
        let key = trusted_object_key("large-object");
        let segments = (0..REFERENCE_COUNT)
            .map(|index| {
                let mut okh = [0u8; 16];
                okh[..8].copy_from_slice(&(index as u64).to_be_bytes());
                ObjectSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    segment_index: index as u32,
                    size: 1024 + index as u64,
                    segment_crc64: index as u64,
                    segment_okh: okh,
                    segment_vid: GenerationId::new(index as u64 + 1).unwrap(),
                    data_pg_id: 7,
                    placement_cluster_epoch: ClusterEpoch::INITIAL,
                    ec_k: 4,
                    ec_m: 2,
                }
            })
            .collect();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::single_part(0),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                segments,
                generation_reservation_id: SessionId::try_from("12".repeat(16)).unwrap(),
                write_sequence: 1,
                last_modified_millis: 1,
                stale_payload: None,
                bucket_write_reservation: BucketWriteReservationProof {
                    bucket: bucket.clone(),
                    reservation_id: "pending-page-proof".to_owned(),
                    owner_token: "pending-page-owner".to_owned(),
                    cluster_epoch: ClusterEpoch::INITIAL,
                    bucket_execution_generation: 1,
                    bucket_incarnation_generation: 1,
                    operation_kind: "direct-put-commit".to_owned(),
                    created_at: 1,
                    lease_deadline: 2,
                    target_context: Some(key.as_str().to_owned()),
                },
            })),
        );
        store
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        let (indexed_references, indexed_pages, largest_page_bytes): (i64, i64, i64) = store
            .conn
            .query_row(
                "SELECT placed_segment_reference_count, \
                        (SELECT count(*) \
                         FROM metadata_command_pending_placed_reference_pages), \
                        (SELECT max(length(encoded_references)) \
                         FROM metadata_command_pending_placed_reference_pages) \
                 FROM metadata_command_pending_slot WHERE singleton = 0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(indexed_references as usize, REFERENCE_COUNT);
        assert_eq!(indexed_pages, 8);
        assert_eq!(
            largest_page_bytes as usize,
            usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT)
                * PENDING_PLACED_SEGMENT_REFERENCE_RECORD_LEN
        );

        // The candidate path must consume only the fixed-width sidecar page.
        // Corrupting the large command proves it is neither decoded nor
        // canonically re-encoded for each page.
        store
            .test_set_pending_metadata_command_bytes(b"not a metadata command")
            .unwrap();
        let mut cursor = None;
        let mut first_page_cursor = None;
        let mut seen = 0usize;
        loop {
            let page = store
                .list_placed_segment_backfill_reference_page(
                    cursor.as_ref(),
                    std::num::NonZeroU16::new(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT)
                        .unwrap(),
                )
                .unwrap();
            assert!(page.items.len() <= usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT));
            if first_page_cursor.is_none() {
                first_page_cursor = page.items.last().map(|item| item.cursor.clone());
            }
            seen += page.items.len();
            cursor = page.items.last().map(|item| item.cursor.clone());
            if page.complete {
                break;
            }
        }
        assert_eq!(seen, REFERENCE_COUNT);
        assert!(matches!(
            first_page_cursor.as_ref(),
            Some(PlacedSegmentBackfillReferenceCursor::PendingCommand {
                log_index: 1,
                reference_index: 63,
                ..
            })
        ));

        let mut replacement_payload = command.payload().clone();
        let MetadataCommandPayload::CommitDirectPutObject(replacement) = &mut replacement_payload
        else {
            unreachable!("test command is a direct PUT");
        };
        replacement.segments.truncate(2);
        let replacement = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            replacement_payload,
        );
        store
            .test_replace_pending_metadata_command_slot(&replacement, Some(&bucket))
            .unwrap();
        let restarted = store
            .list_placed_segment_backfill_reference_page(
                first_page_cursor.as_ref(),
                std::num::NonZeroU16::new(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT).unwrap(),
            )
            .unwrap();
        assert!(restarted.complete);
        assert_eq!(restarted.items.len(), 2);
        assert!(matches!(
            restarted.items[0].cursor,
            PlacedSegmentBackfillReferenceCursor::PendingCommand {
                log_index: 2,
                reference_index: 0,
                ..
            }
        ));
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
                .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
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
            store.record_placed_segment_shard_backfill(&wrong_pg, wrong_pg.request.ec.m, None),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.resolve_placed_segment_shard_backfill(&wrong_pg),
            Err(StoreError::PayloadShardSetMismatch { .. })
        ));
        assert!(matches!(
            store.record_placed_segment_shard_backfill(
                &reversed_epochs,
                reversed_epochs.request.ec.m,
                None
            ),
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
                "claim-0", "worker-0", epoch, 9, 19, 9
            ))
            .unwrap()
            .is_none());
        store
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();

        let claim = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1", "worker-1", epoch, 10, 20, 10,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(claim.work_item, work_item);
        assert_eq!(claim.attempt_count, 1);

        assert_eq!(
            store
                .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                    "claim-1", "worker-1", epoch, 11, 21, 11,
                ))
                .unwrap()
                .unwrap(),
            claim
        );
        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2", "worker-2", epoch, 12, 22, 12,
            ))
            .unwrap()
            .is_none());

        assert!(store
            .record_placed_segment_shard_backfill_claim_error(&claim, "backfill failed", 30)
            .unwrap());
        store
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();
        assert_eq!(
            store.list_placed_segment_shard_backfills().unwrap()[0]
                .last_error
                .as_deref(),
            Some("backfill failed")
        );
        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2", "worker-2", epoch, 29, 39, 29,
            ))
            .unwrap()
            .is_none());

        let retry = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2", "worker-2", epoch, 30, 40, 30,
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
    fn placed_segment_shard_backfill_claim_prefers_lower_remaining_tolerance() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let routine = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xC0; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x1234,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };
        let urgent = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 7,
                segment_okh: [0xC1; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                stored_size: 1024,
                segment_crc64: 0x5678,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(5).unwrap(),
        };

        store
            .record_placed_segment_shard_backfill(&routine, 2, None)
            .unwrap();
        store
            .record_placed_segment_shard_backfill(&urgent, 0, None)
            .unwrap();

        let claim = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-priority",
                "worker-priority",
                ClusterEpoch::new(7).unwrap(),
                10,
                20,
                10,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(claim.work_item, urgent);
        assert_eq!(claim.remaining_tolerance, 0);
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
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();
        let first = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1", "worker-1", epoch, 10, 20, 10,
            ))
            .unwrap()
            .unwrap();

        assert!(store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2", "worker-2", epoch, 19, 29, 19,
            ))
            .unwrap()
            .is_none());
        let stolen = store
            .acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-2", "worker-2", epoch, 20, 30, 20,
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
            .record_placed_segment_shard_backfill(&work_item, work_item.request.ec.m, None)
            .unwrap();

        assert!(matches!(
            store.acquire_placed_segment_shard_backfill_claim(&backfill_claim_acquire(
                "claim-1", "worker-1", epoch, 10, 10, 10
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
                "claim-0", "worker-0", epoch, 9, 19, 9
            ))
            .unwrap()
            .is_none());
        store
            .record_placed_segment_shard_repair(&work_item, None)
            .unwrap();

        let claim = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-1", "worker-1", epoch, 10, 20, 10,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(claim.work_item, work_item);
        assert_eq!(claim.attempt_count, 1);

        assert_eq!(
            store
                .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                    "claim-1", "worker-1", epoch, 11, 21, 11,
                ))
                .unwrap()
                .unwrap(),
            claim
        );
        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2", "worker-2", epoch, 12, 22, 12,
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
                "claim-2", "worker-2", epoch, 29, 39, 29,
            ))
            .unwrap()
            .is_none());

        let retry = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2", "worker-2", epoch, 30, 40, 30,
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
                "claim-1", "worker-1", epoch, 10, 20, 10,
            ))
            .unwrap()
            .unwrap();

        assert!(store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2", "worker-2", epoch, 19, 29, 19,
            ))
            .unwrap()
            .is_none());
        let stolen = store
            .acquire_placed_segment_shard_repair_claim(&repair_claim_acquire(
                "claim-2", "worker-2", epoch, 20, 30, 20,
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
                "claim-1", "worker-1", epoch, 10, 10, 10
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
