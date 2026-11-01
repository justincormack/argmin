// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[cfg(test)]
#[path = "reservation_heartbeat_tests.rs"]
mod tests;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatEffect {
    BucketReservation,
    StreamProof,
}

#[cfg(test)]
type HeartbeatEffectHook = Box<dyn FnOnce(HeartbeatEffect, bool)>;

#[cfg(test)]
thread_local! {
    static BEFORE_HEARTBEAT_EFFECT: std::cell::RefCell<Option<HeartbeatEffectHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn run_before_heartbeat_effect_hook(store: &PgStore, effect: HeartbeatEffect) {
    if let Some(hook) = BEFORE_HEARTBEAT_EFFECT.with(|slot| slot.borrow_mut().take()) {
        hook(effect, !store.conn.is_autocommit());
    }
}

impl PgStore {
    pub(super) fn heartbeat_bucket_reservation_at_recorded_time(
        &self,
        heartbeat: DurableBucketWriteReservationHeartbeat<'_>,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        let now = heartbeat.now;
        let fence = AdmittedRouteEffectFence::unbounded(heartbeat.cluster_epoch);
        self.heartbeat_bucket_reservation_inner(heartbeat, fence, || now)
    }

    pub(in crate::node_runtime) fn heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &self,
        heartbeat: DurableBucketWriteReservationHeartbeat<'_>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteReservationRecord, MetadataError> {
        // The proof's durable epoch may precede the active authorizing route.
        // Refresh lease time only after SQLite admission, never from the caller's
        // earlier observation. Heartbeat does not renew the route effect fence.
        self.heartbeat_bucket_reservation_inner(
            heartbeat,
            effect_fence,
            crate::clock::current_time_millis,
        )
    }

    fn heartbeat_bucket_reservation_inner(
        &self,
        heartbeat: DurableBucketWriteReservationHeartbeat<'_>,
        effect_fence: AdmittedRouteEffectFence,
        now_at_effect: impl FnOnce() -> u64,
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
        self.with_immediate_txn(
            "heartbeat durable bucket write reservation (begin txn)",
            "heartbeat durable bucket write reservation (commit txn)",
            |store| {
                let mut statement = store
                    .conn
                    .prepare_cached(
                        "UPDATE bucket_write_reservations \
                 SET lease_deadline = ?8 \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6 \
                   AND lease_deadline = ?7 AND lease_deadline > ?9",
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "prepare durable bucket write reservation heartbeat",
                        source: source.into(),
                    })?;
                #[cfg(test)]
                run_before_heartbeat_effect_hook(store, HeartbeatEffect::BucketReservation);
                let now = i64::try_from(now_at_effect()).map_err(|source| MetadataError::Db {
                    context: "heartbeat durable bucket write reservation now",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(
                        source,
                    )),
                })?;
                effect_fence
                    .require_valid_for(effect_fence.cluster_epoch())
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
                let updated = statement
                    .execute(params![
                        heartbeat.name.as_str(),
                        heartbeat.reservation_id,
                        heartbeat.owner_token,
                        heartbeat.cluster_epoch.get(),
                        generation,
                        incarnation,
                        current_lease_deadline,
                        lease_deadline,
                        now,
                    ])
                    .map_err(|source| MetadataError::Db {
                        context: "heartbeat durable bucket write reservation",
                        source: source.into(),
                    })?;
                if updated == 0 {
                    return Err(MetadataError::BucketWriteReservationNotFound {
                        reservation_id: heartbeat.reservation_id.to_string(),
                    });
                }
                store
                    .durable_bucket_write_reservation(heartbeat.name, heartbeat.reservation_id)?
                    .ok_or_else(|| MetadataError::BucketWriteReservationNotFound {
                        reservation_id: heartbeat.reservation_id.to_string(),
                    })
            },
        )
    }

    pub(in crate::node_runtime) fn update_stream_upload_bucket_write_reservation_with_effect_fence(
        &self,
        session_id: &SessionId,
        current: &BucketWriteReservationProof,
        renewed: &BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
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
        self.with_immediate_txn(
            "update stream upload bucket write reservation (begin txn)",
            "update stream upload bucket write reservation (commit txn)",
            |store| {
                let mut statement = store
                    .conn
                    .prepare_cached(
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
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "prepare stream upload bucket write reservation update",
                        source: source.into(),
                    })?;
                #[cfg(test)]
                run_before_heartbeat_effect_hook(store, HeartbeatEffect::StreamProof);
                effect_fence
                    .require_valid_for(effect_fence.cluster_epoch())
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
                let updated = statement
                    .execute(params![
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
                    ])
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
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_before_heartbeat_effect_hook<T>(
        hook: impl FnOnce(HeartbeatEffect, bool) + 'static,
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<HeartbeatEffectHook>);
        impl Drop for Reset {
            fn drop(&mut self) {
                BEFORE_HEARTBEAT_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset = Reset(BEFORE_HEARTBEAT_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))));
        action()
    }
}
