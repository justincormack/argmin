// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact proof release: SQLite admission and inspection precede the final
//! deadline check. A renewable reservation lease is not the operation deadline.

use super::*;
use std::time::Instant;

#[cfg(test)]
type ReleaseEffectHook = Box<dyn FnOnce(bool, Option<Instant>)>;

#[cfg(test)]
thread_local! {
    static BEFORE_PROOF_RELEASE_EFFECT: std::cell::RefCell<Option<ReleaseEffectHook>> =
        const { std::cell::RefCell::new(None) };
}

fn require_deadline(deadline: Option<Instant>) -> Result<(), MetadataError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(MetadataError::RouteEffectRejected {
            source: StoreError::OperationDeadlineExceeded {
                context: "release metadata command bucket write reservation",
            },
        });
    }
    Ok(())
}

impl PgStore {
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_proof_release_sql_changes(&self) -> u64 {
        self.conn.total_changes()
    }

    pub(in crate::node_runtime) fn release_metadata_command_bucket_write_reservation_with_deadline(
        &self,
        proof: &BucketWriteReservationProof,
        deadline: Option<Instant>,
    ) -> Result<(), MetadataError> {
        self.require_usable_connection()
            .map_err(|source| MetadataError::RouteEffectRejected { source })?;
        require_deadline(deadline)?;
        let transaction =
            self.begin_immediate_txn("begin release metadata command bucket write reservation")?;
        let result = (|| {
            let existing =
                self.durable_bucket_write_reservation(&proof.bucket, &proof.reservation_id)?;
            if existing
                .as_ref()
                .is_some_and(|record| !proof.matches_record(record))
            {
                return Err(MetadataError::BucketWriteReservationConflict {
                    reservation_id: proof.reservation_id.clone(),
                });
            }
            let mut delete = self
                .conn
                .prepare_cached(
                    "DELETE FROM bucket_write_reservations \
                 WHERE bucket_name = ?1 AND reservation_id = ?2 \
                   AND owner_token = ?3 AND cluster_epoch = ?4 \
                   AND bucket_execution_generation = ?5 \
                   AND bucket_incarnation_generation = ?6 AND lease_deadline = ?7",
                )
                .map_err(|source| MetadataError::Db {
                    context: "prepare metadata command bucket write reservation release",
                    source: source.into(),
                })?;
            #[cfg(test)]
            if let Some(hook) = BEFORE_PROOF_RELEASE_EFFECT.with(|slot| slot.borrow_mut().take()) {
                hook(!self.conn.is_autocommit(), deadline);
            }
            // Do not acknowledge even an absent proof from an expired operation.
            // In the present-row case this is immediately beside the first write,
            // after writer admission, reads and statement preparation.
            require_deadline(deadline)?;
            match existing {
                Some(record) => {
                    let deleted = delete
                        .execute(params![
                            proof.bucket.as_str(),
                            proof.reservation_id,
                            record.owner_token,
                            record.cluster_epoch.get(),
                            record.bucket_execution_generation,
                            record.bucket_incarnation_generation,
                            record.lease_deadline,
                        ])
                        .map_err(|source| MetadataError::Db {
                            context: "release metadata command durable bucket write reservation",
                            source: source.into(),
                        })?;
                    if deleted != 1 {
                        return Err(MetadataError::BucketWriteReservationConflict {
                            reservation_id: proof.reservation_id.clone(),
                        });
                    }
                    Ok(())
                }
                None => Ok(()),
            }
        })();
        self.finish_immediate_txn(
            transaction,
            result,
            "commit release metadata command bucket write reservation",
        )
    }

    #[cfg(test)]
    pub(crate) fn test_with_before_proof_release_effect_hook<T>(
        hook: impl FnOnce(bool, Option<Instant>) + 'static,
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<ReleaseEffectHook>);
        impl Drop for Reset {
            fn drop(&mut self) {
                BEFORE_PROOF_RELEASE_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset =
            Reset(BEFORE_PROOF_RELEASE_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))));
        action()
    }
}
