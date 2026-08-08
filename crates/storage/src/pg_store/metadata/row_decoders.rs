#[allow(dead_code)]
fn bucket_write_reservation_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<BucketWriteReservationRecord, rusqlite::Error> {
    let bucket_raw: String = row.get(0)?;
    let cluster_epoch_raw: i64 = row.get(3)?;
    let bucket_execution_generation_raw: i64 = row.get(4)?;
    let bucket_incarnation_generation_raw: i64 = row.get(5)?;
    let created_at_raw: i64 = row.get(7)?;
    let lease_deadline_raw: i64 = row.get(8)?;
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
        lease_deadline: u64::try_from(lease_deadline_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid lease_deadline: {lease_deadline_raw}")),
            )
        })?,
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
    let lease_deadline_raw: i64 = row.get(7)?;
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
        lease_deadline: u64::try_from(lease_deadline_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid lease_deadline: {lease_deadline_raw}")),
            )
        })?,
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
    let stream_cleanup_next_object_pg_id_raw: Option<i64> = row.get(8)?;
    let stream_cleanup_next_session_id_marker_raw: Option<String> = row.get(9)?;
    let stream_cleanup_aborted_uploads_raw: i64 = row.get(10)?;
    let final_visibility_next_object_pg_id_raw: Option<i64> = row.get(11)?;
    let finalizer_next_object_pg_id_raw: Option<i64> = row.get(12)?;
    let updated_at_raw: i64 = row.get(13)?;
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
        5 => BucketDeleteAttemptPhase::FinalVisibilityProven,
        6 => BucketDeleteAttemptPhase::MarkDeleting,
        7 => BucketDeleteAttemptPhase::PostReservationStreamCleanup,
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
        stream_cleanup_next_object_pg_id: stream_cleanup_next_object_pg_id_raw
            .map(|raw| {
                u32::try_from(raw).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid stream_cleanup_next_object_pg_id: {raw}")),
                    )
                })
            })
            .transpose()?,
        stream_cleanup_next_session_id_marker: stream_cleanup_next_session_id_marker_raw
            .map(|raw| {
                SessionId::try_from(raw).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .transpose()?,
        stream_cleanup_aborted_uploads: match stream_cleanup_aborted_uploads_raw {
            0 => false,
            1 => true,
            raw => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid stream_cleanup_aborted_uploads: {raw}")),
                ));
            }
        },
        final_visibility_next_object_pg_id: final_visibility_next_object_pg_id_raw
            .map(|raw| {
                u32::try_from(raw).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid final_visibility_next_object_pg_id: {raw}")),
                    )
                })
            })
            .transpose()?,
        finalizer_next_object_pg_id: finalizer_next_object_pg_id_raw
            .map(|raw| {
                u32::try_from(raw).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        12,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid finalizer_next_object_pg_id: {raw}")),
                    )
                })
            })
            .transpose()?,
        updated_at: u64::try_from(updated_at_raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                13,
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

