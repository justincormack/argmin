// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request-local limits, not permission to mutate or recover a prefix. Startup
//! recovery does not use this foreground allowance. Never replenish between
//! phases of a validation pass; retries retain the original work deadline.
use super::*;

#[cfg(test)]
thread_local! {
    static LIMITS: std::cell::Cell<Option<(usize, usize, usize)>> = const { std::cell::Cell::new(None) };
    static BEFORE_ENTRY: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
    static EXPIRE_DEADLINE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) struct PrefixValidationWork {
    deadline: Instant,
    fence: AdmittedRouteEffectFence,
    entries: usize,
    rows: usize,
    bytes: usize,
}

impl PrefixValidationWork {
    pub(super) fn new(deadline: Instant, fence: AdmittedRouteEffectFence) -> Self {
        let (entries, rows, bytes) = (4096, 100_000, 16 * 1024 * 1024);
        #[cfg(test)]
        let (entries, rows, bytes) = LIMITS
            .with(|slot| slot.get())
            .unwrap_or((entries, rows, bytes));
        Self {
            deadline,
            fence,
            entries,
            rows,
            bytes,
        }
    }

    pub(super) fn check(&self) -> Result<(), StoreError> {
        self.fence.require_valid_for(self.fence.cluster_epoch())?;
        let now = Instant::now();
        #[cfg(test)]
        let now = if EXPIRE_DEADLINE.with(|slot| slot.get()) {
            self.deadline
        } else {
            now
        };
        if now >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete prefix reinspection validation",
            });
        }
        Ok(())
    }

    pub(super) fn row(&mut self, bytes: usize) -> Result<(), StoreError> {
        self.check()?;
        let remaining_rows =
            self.rows
                .checked_sub(1)
                .ok_or(StoreError::MetadataCommandContention {
                    context: "complete prefix validation row budget exhausted",
                })?;
        let remaining_bytes =
            self.bytes
                .checked_sub(bytes)
                .ok_or(StoreError::MetadataCommandContention {
                    context: "complete prefix validation byte budget exhausted",
                })?;
        self.rows = remaining_rows;
        self.bytes = remaining_bytes;
        Ok(())
    }

    pub(super) fn entry(&mut self, bytes: usize) -> Result<(), StoreError> {
        #[cfg(test)]
        BEFORE_ENTRY.with(|slot| {
            if let Some(hook) = slot.borrow_mut().as_mut() {
                hook();
            }
        });
        self.check()?;
        self.entries =
            self.entries
                .checked_sub(1)
                .ok_or(StoreError::MetadataCommandContention {
                    context: "complete prefix validation entry budget exhausted",
                })?;
        self.row(bytes)
    }
}

#[cfg(test)]
impl PgStore {
    pub(in crate::node_runtime::pg_store) fn test_expire_prefix_validation_deadline() {
        EXPIRE_DEADLINE.with(|slot| slot.set(true));
    }
    pub(in crate::node_runtime::pg_store) fn test_with_prefix_validation_bounds<R>(
        limits: (usize, usize, usize),
        hook: impl FnMut() + 'static,
        action: impl FnOnce() -> R,
    ) -> R {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                LIMITS.with(|slot| slot.set(None));
                BEFORE_ENTRY.with(|slot| slot.borrow_mut().take());
                EXPIRE_DEADLINE.with(|slot| slot.set(false));
            }
        }
        LIMITS.with(|slot| assert!(slot.replace(Some(limits)).is_none()));
        BEFORE_ENTRY.with(|slot| assert!(slot.borrow_mut().replace(Box::new(hook)).is_none()));
        let _reset = Reset;
        action()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_direct_reinspection_prefix_production_limits_are_finite_and_inclusive() {
        let new = || {
            PrefixValidationWork::new(
                Instant::now() + std::time::Duration::from_secs(60),
                AdmittedRouteEffectFence::unbounded(ClusterEpoch::new(1).unwrap()),
            )
        };
        let mut work = new();
        for _ in 0..4096 {
            work.entry(0).unwrap();
        }
        assert!(work
            .entry(0)
            .unwrap_err()
            .to_string()
            .contains("entry budget exhausted"));
        let mut work = new();
        work.row(16 * 1024 * 1024).unwrap();
        assert!(work
            .row(1)
            .unwrap_err()
            .to_string()
            .contains("byte budget exhausted"));
        let mut work = new();
        for _ in 0..100_000 {
            work.row(0).unwrap();
        }
        assert!(work
            .row(0)
            .unwrap_err()
            .to_string()
            .contains("row budget exhausted"));
    }
}
