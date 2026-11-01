// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[cfg(test)]
#[path = "bucket_drain_begin_tests.rs"]
mod tests;

#[cfg(test)]
type DrainEffectHook = Box<dyn FnOnce(bool)>;

#[cfg(test)]
thread_local! {
    static BEFORE_DRAIN_EFFECT: std::cell::RefCell<Option<DrainEffectHook>> =
        const { std::cell::RefCell::new(None) };
}

impl PgStore {
    /// Acquire SQLite write serialization before the final effect check. The
    /// insert and exact retry/conflict readback share this transaction.
    #[allow(clippy::too_many_arguments)] // Durable drain fields plus immutable effect authority.
    pub(in crate::node_runtime) fn begin_durable_bucket_write_drain_with_effect_fence(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: u64,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<BucketWriteDrainRecord, MetadataError> {
        let created_at = i64::try_from(created_at).map_err(|source| MetadataError::Db {
            context: "begin durable bucket write drain created_at",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let lease_deadline = i64::try_from(lease_deadline).map_err(|source| MetadataError::Db {
            context: "begin durable bucket write drain lease_deadline",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        self.with_immediate_txn(
            "begin durable bucket write drain (begin txn)",
            "begin durable bucket write drain (commit txn)",
            |store| {
                let mut insert = store.conn.prepare_cached(
                    "INSERT INTO bucket_write_drains \
                     (bucket_name, drain_id, owner_token, cluster_epoch, bucket_execution_generation, \
                      state, created_at, lease_deadline) \
                     SELECT name, ?1, ?2, ?3, bucket_execution_generation, ?4, ?5, ?6 \
                     FROM buckets \
                     WHERE name = ?7 AND state = ?8 \
                     ON CONFLICT(bucket_name) DO NOTHING"
                ).map_err(|source| MetadataError::Db {
                    context: "prepare durable bucket write drain",
                    source: source.into(),
                })?;
                #[cfg(test)]
                if let Some(hook) = BEFORE_DRAIN_EFFECT.with(|slot| slot.borrow_mut().take()) {
                    hook(!store.conn.is_autocommit());
                }
                effect_fence.require_valid_for(cluster_epoch)
                    .map_err(|source| MetadataError::RouteEffectRejected { source })?;
                let inserted = insert.execute(params![
                    drain_id, owner_token, cluster_epoch.get(),
                    BucketWriteDrainState::Draining as u8, created_at, lease_deadline,
                    name.as_str(), BucketState::Active as u8,
                ]).map_err(|source| MetadataError::Db {
                    context: "begin durable bucket write drain",
                    source: source.into(),
                })?;
                if inserted == 0 {
                    if let Some(existing) = store.durable_bucket_write_drain(name)? {
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
                store.durable_bucket_write_drain(name)?.ok_or_else(|| {
                    MetadataError::BucketWriteDrainNotFound { drain_id: drain_id.to_string() }
                })
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_before_bucket_drain_effect_hook<T>(
        hook: impl FnOnce(bool) + 'static,
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<DrainEffectHook>);
        impl Drop for Reset {
            fn drop(&mut self) {
                BEFORE_DRAIN_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset = Reset(BEFORE_DRAIN_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))));
        action()
    }
}
