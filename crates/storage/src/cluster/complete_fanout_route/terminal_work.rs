// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One preparation/installation budget retained by the encrypted payload owner.
//! This is accounting, not permission to drain a contender or perform effects.
use super::*;

pub(crate) struct DirectPutTerminalWork {
    budget: RequestWorkBudget,
    portable: StorageRpcOperationDeadline,
}

impl DirectPutTerminalWork {
    pub(super) fn new(
        route: &CompleteFanoutRoute<'_>,
        original: Instant,
        portable: StorageRpcOperationDeadline,
    ) -> Self {
        let mut budget = RequestWorkBudget::new(DIRECT_PUT_METADATA_RETRY_BUDGET, None)
            .for_operation("commit_direct_put_metadata")
            .for_pg(route.pg_id);
        let deadline = budget.deadline().min(original);
        let (deadline, portable) = route.intersect_deadline(
            deadline,
            StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: portable.portable_wall_valid_until_ms.min(
                    StorageRpcOperationDeadline::from_instant(deadline)
                        .portable_wall_valid_until_ms,
                ),
            },
        );
        budget.budget = deadline.saturating_duration_since(budget.started);
        Self { budget, portable }
    }

    pub(crate) fn bounds(&self) -> (Instant, StorageRpcOperationDeadline) {
        (self.budget.deadline(), self.portable)
    }

    pub(crate) fn require_deadline(&self) -> Result<(), StoreError> {
        if crate::clock::current_time_millis() >= self.portable.portable_wall_valid_until_ms {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encrypted direct terminal original work cutoff",
            });
        }
        self.budget.check_deadline("encrypted direct terminal work")
    }

    pub(crate) fn begin_attempt(&mut self) -> Result<(), StoreError> {
        self.require_deadline()?;
        self.budget
            .check("encrypted direct terminal attempt budget")
    }

    #[cfg(test)]
    pub(crate) fn test_attempts(&self) -> usize {
        self.budget.attempts
    }

    #[cfg(test)]
    pub(crate) fn test_limit_attempts(&mut self, limit: usize) {
        self.budget.max_attempts = Some(limit);
    }
}
