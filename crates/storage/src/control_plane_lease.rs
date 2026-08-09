// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Pure clock and lease rules shared by the control-plane authority and its consumers.
//!
//! The wire deadline remains an authority wall-clock timestamp. A process must bind it to
//! its monotonic clock before serving; wall-clock changes after binding cannot extend it.

use std::sync::{Mutex, OnceLock};

use thiserror::Error;

/// Maximum supported wall-clock offset between any two control-plane participants.
pub(crate) const CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum LeaseClockError {
    #[error("{field} timestamp arithmetic overflowed")]
    TimestampOverflow { field: &'static str },
    #[error(
        "authority timestamp {authority_issued_at_ms}ms is more than the {skew_budget_ms}ms skew budget ahead of local wall time {local_wall_ms}ms"
    )]
    AuthorityClockTooFarAhead {
        authority_issued_at_ms: u64,
        local_wall_ms: u64,
        skew_budget_ms: u64,
    },
    #[error(
        "process wall clock moved outside the {skew_budget_ms}ms budget relative to monotonic time"
    )]
    LocalClockUnhealthy { skew_budget_ms: u64 },
    #[error("process clock-health source is unavailable")]
    ClockHealthSourceUnavailable,
    #[error(
        "serving deadline {serving_deadline_ms}ms exceeds effective committed time {effective_committed_now_ms}ms plus lease {max_lease_ms}ms and skew {skew_budget_ms}ms"
    )]
    ServingDeadlineOutOfBounds {
        serving_deadline_ms: u64,
        effective_committed_now_ms: u64,
        max_lease_ms: u64,
        skew_budget_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum LeaseHorizonError {
    #[error("{field} timestamp arithmetic overflowed")]
    TimestampOverflow { field: &'static str },
    #[error(
        "lease horizon belongs to clock generation {horizon_clock_generation} and Raft term {horizon_raft_term:?}, not generation {requested_clock_generation} and term {requested_raft_term:?}"
    )]
    #[cfg(test)]
    AuthorityMismatch {
        horizon_clock_generation: u64,
        horizon_raft_term: Option<u64>,
        requested_clock_generation: u64,
        requested_raft_term: Option<u64>,
    },
    #[error(
        "lease deadline {lease_deadline_ms}ms exceeds committed grant horizon {grant_not_after_ms}ms"
    )]
    #[cfg(test)]
    DeadlineBeyondHorizon {
        lease_deadline_ms: u64,
        grant_not_after_ms: u64,
    },
    #[error(
        "previous lease horizon remains fenced through {fenced_until_ms}ms at accepted authority time {authority_now_ms}ms"
    )]
    PreviousHorizonStillActive {
        authority_now_ms: u64,
        fenced_until_ms: u64,
    },
}

#[derive(Debug)]
struct ProcessLeaseClockHealth {
    initial_wall_ms: u64,
    initial_clock_health_ms: u64,
    healthy: bool,
}

impl ProcessLeaseClockHealth {
    fn new(
        local_wall_ms: u64,
        local_clock_health_ms: Option<u64>,
    ) -> Result<Self, LeaseClockError> {
        let local_clock_health_ms =
            local_clock_health_ms.ok_or(LeaseClockError::ClockHealthSourceUnavailable)?;
        Ok(Self {
            initial_wall_ms: local_wall_ms,
            initial_clock_health_ms: local_clock_health_ms,
            healthy: true,
        })
    }

    fn observe(
        &mut self,
        local_wall_ms: u64,
        local_clock_health_ms: Option<u64>,
        skew_budget_ms: u64,
    ) -> bool {
        if !self.healthy {
            return false;
        }
        let Some(local_clock_health_ms) = local_clock_health_ms else {
            self.healthy = false;
            return false;
        };
        let Some(wall_elapsed_ms) = local_wall_ms.checked_sub(self.initial_wall_ms) else {
            self.healthy = false;
            return false;
        };
        let Some(monotonic_elapsed_ms) =
            local_clock_health_ms.checked_sub(self.initial_clock_health_ms)
        else {
            self.healthy = false;
            return false;
        };
        self.healthy = wall_elapsed_ms.abs_diff(monotonic_elapsed_ms) <= skew_budget_ms;
        self.healthy
    }
}

/// Latch a process unhealthy when wall time departs from monotonic elapsed time.
///
/// Test clock overrides deliberately bypass this process-global monitor. Tests
/// that exercise clock faults use the pure model below so parallel test clocks
/// cannot poison unrelated cases.
pub(crate) fn validate_process_lease_clock(
    local_wall_ms: u64,
    local_clock_health_ms: Option<u64>,
    skew_budget_ms: u64,
) -> Result<(), LeaseClockError> {
    if crate::clock::override_time_millis().is_some() {
        return Ok(());
    }
    static PROCESS_CLOCK_HEALTH: OnceLock<Mutex<ProcessLeaseClockHealth>> = OnceLock::new();
    if let Some(health) = PROCESS_CLOCK_HEALTH.get() {
        return if health
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe(local_wall_ms, local_clock_health_ms, skew_budget_ms)
        {
            Ok(())
        } else if local_clock_health_ms.is_none() {
            Err(LeaseClockError::ClockHealthSourceUnavailable)
        } else {
            Err(LeaseClockError::LocalClockUnhealthy { skew_budget_ms })
        };
    }
    let initial_health = ProcessLeaseClockHealth::new(local_wall_ms, local_clock_health_ms)?;
    let health = PROCESS_CLOCK_HEALTH.get_or_init(|| Mutex::new(initial_health));
    if health
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .observe(local_wall_ms, local_clock_health_ms, skew_budget_ms)
    {
        Ok(())
    } else {
        Err(LeaseClockError::LocalClockUnhealthy { skew_budget_ms })
    }
}

/// Process-local binding of an authority deadline to a monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundRouteMapLease {
    authority_valid_until_ms: u64,
    local_valid_until_monotonic_ms: u64,
}

impl BoundRouteMapLease {
    pub(crate) fn expired(authority_valid_until_ms: u64, local_monotonic_ms: u64) -> Self {
        Self {
            authority_valid_until_ms,
            local_valid_until_monotonic_ms: local_monotonic_ms,
        }
    }

    /// Bind a freshly read authority deadline to a process-local monotonic clock.
    pub(crate) fn bind(
        authority_issued_at_ms: u64,
        authority_valid_until_ms: u64,
        local_wall_ms: u64,
        local_monotonic_ms: u64,
        skew_budget_ms: u64,
    ) -> Result<Self, LeaseClockError> {
        if authority_issued_at_ms > local_wall_ms.saturating_add(skew_budget_ms) {
            return Err(LeaseClockError::AuthorityClockTooFarAhead {
                authority_issued_at_ms,
                local_wall_ms,
                skew_budget_ms,
            });
        }

        // A slow consumer can trail the deadline issuer by the full skew budget.
        // Subtracting it makes the consumer stop no later than the issuer reaches
        // the authority deadline. Network and processing delay only shorten this.
        let conservative_local_wall_deadline_ms =
            authority_valid_until_ms.saturating_sub(skew_budget_ms);
        let remaining_ms = conservative_local_wall_deadline_ms.saturating_sub(local_wall_ms);
        let local_valid_until_monotonic_ms = local_monotonic_ms.checked_add(remaining_ms).ok_or(
            LeaseClockError::TimestampOverflow {
                field: "local monotonic lease deadline",
            },
        )?;
        Ok(Self {
            authority_valid_until_ms,
            local_valid_until_monotonic_ms,
        })
    }

    #[cfg(test)]
    fn authority_valid_until_ms(self) -> u64 {
        self.authority_valid_until_ms
    }

    pub(crate) fn local_valid_until_monotonic_ms(self) -> u64 {
        self.local_valid_until_monotonic_ms
    }

    pub(crate) fn is_valid_at_monotonic(self, local_monotonic_ms: u64) -> bool {
        self.local_valid_until_monotonic_ms > local_monotonic_ms
    }
}

/// Enforce the replicated upper bound for any deadline that can authorize serving.
pub(crate) fn validate_serving_deadline_bound(
    serving_deadline_ms: u64,
    effective_committed_now_ms: u64,
    max_lease_ms: u64,
    skew_budget_ms: u64,
) -> Result<(), LeaseClockError> {
    let maximum_deadline_ms = effective_committed_now_ms
        .checked_add(max_lease_ms)
        .and_then(|deadline_ms| deadline_ms.checked_add(skew_budget_ms))
        .ok_or(LeaseClockError::TimestampOverflow {
            field: "maximum serving deadline",
        })?;
    if serving_deadline_ms > maximum_deadline_ms {
        return Err(LeaseClockError::ServingDeadlineOutOfBounds {
            serving_deadline_ms,
            effective_committed_now_ms,
            max_lease_ms,
            skew_budget_ms,
        });
    }
    Ok(())
}

/// Compute a non-regressing renewal without allowing an old deadline to escape the bound.
pub(crate) fn bounded_renewal_deadline(
    current_deadline_ms: Option<u64>,
    effective_committed_now_ms: u64,
    requested_lease_ms: u64,
    max_lease_ms: u64,
    skew_budget_ms: u64,
) -> Result<u64, LeaseClockError> {
    let proposed_deadline_ms = effective_committed_now_ms
        .checked_add(requested_lease_ms.min(max_lease_ms))
        .ok_or(LeaseClockError::TimestampOverflow {
            field: "proposed serving deadline",
        })?;
    let candidate_deadline_ms = current_deadline_ms.map_or(proposed_deadline_ms, |current| {
        current.max(proposed_deadline_ms)
    });
    validate_serving_deadline_bound(
        candidate_deadline_ms,
        effective_committed_now_ms,
        max_lease_ms,
        skew_budget_ms,
    )?;
    Ok(candidate_deadline_ms)
}

/// Return whether a successor can activate after a previous authority deadline.
pub(crate) fn successor_activation_fence_satisfied(
    successor_authority_now_ms: u64,
    previous_primary_deadline_ms: u64,
    skew_budget_ms: u64,
) -> bool {
    previous_primary_deadline_ms
        .checked_add(skew_budget_ms)
        .is_some_and(|fenced_until_ms| successor_authority_now_ms >= fenced_until_ms)
}

/// Authority identity under which a durable lease-grant horizon was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseHorizonAuthorityBinding {
    clock_generation: u64,
    raft_term: Option<u64>,
}

impl LeaseHorizonAuthorityBinding {
    #[cfg(test)]
    pub(crate) fn new(clock_generation: u64, raft_term: Option<u64>) -> Self {
        Self::checked_new(clock_generation, raft_term)
            .expect("test lease horizon authority binding must be valid")
    }

    pub fn checked_new(clock_generation: u64, raft_term: Option<u64>) -> Option<Self> {
        if clock_generation == 0 || raft_term == Some(0) {
            return None;
        }
        Some(Self {
            clock_generation,
            raft_term,
        })
    }

    pub fn clock_generation(self) -> u64 {
        self.clock_generation
    }

    pub fn raft_term(self) -> Option<u64> {
        self.raft_term
    }
}

/// Durable upper bound for leases acknowledged without another durable command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommittedLeaseGrantHorizon {
    authority: LeaseHorizonAuthorityBinding,
    grant_not_after_ms: u64,
}

impl CommittedLeaseGrantHorizon {
    pub(crate) fn from_parts(
        authority: LeaseHorizonAuthorityBinding,
        grant_not_after_ms: u64,
    ) -> Self {
        Self {
            authority,
            grant_not_after_ms,
        }
    }

    /// Establish or extend a horizon for one authority generation/term.
    ///
    /// Rebinding to a new authority is deliberately conservative: the previous
    /// horizon plus symmetric skew must have elapsed first. A later optimized
    /// handoff may replace this only with an equally strong committed proof.
    pub(crate) fn establish(
        previous: Option<Self>,
        authority: LeaseHorizonAuthorityBinding,
        authority_now_ms: u64,
        horizon_duration_ms: u64,
        skew_budget_ms: u64,
    ) -> Result<Self, LeaseHorizonError> {
        let proposed_grant_not_after_ms = authority_now_ms.checked_add(horizon_duration_ms).ok_or(
            LeaseHorizonError::TimestampOverflow {
                field: "lease grant horizon",
            },
        )?;
        let Some(previous) = previous else {
            return Ok(Self {
                authority,
                grant_not_after_ms: proposed_grant_not_after_ms,
            });
        };
        if previous.authority == authority {
            return Ok(Self {
                authority,
                grant_not_after_ms: previous.grant_not_after_ms.max(proposed_grant_not_after_ms),
            });
        }
        previous.validate_rebinding(authority, authority_now_ms, skew_budget_ms)?;
        Ok(Self {
            authority,
            grant_not_after_ms: proposed_grant_not_after_ms,
        })
    }

    pub(crate) fn validate_rebinding(
        self,
        authority: LeaseHorizonAuthorityBinding,
        authority_now_ms: u64,
        skew_budget_ms: u64,
    ) -> Result<(), LeaseHorizonError> {
        if self.authority == authority {
            return Ok(());
        }
        let fenced_until_ms = self.grant_not_after_ms.checked_add(skew_budget_ms).ok_or(
            LeaseHorizonError::TimestampOverflow {
                field: "previous lease horizon fence",
            },
        )?;
        if authority_now_ms < fenced_until_ms {
            return Err(LeaseHorizonError::PreviousHorizonStillActive {
                authority_now_ms,
                fenced_until_ms,
            });
        }
        Ok(())
    }

    /// Validate a volatile heartbeat lease against the durable horizon.
    #[cfg(test)]
    pub(crate) fn validate_grant(
        self,
        authority: LeaseHorizonAuthorityBinding,
        lease_deadline_ms: u64,
    ) -> Result<(), LeaseHorizonError> {
        if self.authority != authority {
            return Err(LeaseHorizonError::AuthorityMismatch {
                horizon_clock_generation: self.authority.clock_generation,
                horizon_raft_term: self.authority.raft_term,
                requested_clock_generation: authority.clock_generation,
                requested_raft_term: authority.raft_term,
            });
        }
        if lease_deadline_ms > self.grant_not_after_ms {
            return Err(LeaseHorizonError::DeadlineBeyondHorizon {
                lease_deadline_ms,
                grant_not_after_ms: self.grant_not_after_ms,
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn successor_fence_satisfied(
        self,
        successor_authority_now_ms: u64,
        skew_budget_ms: u64,
    ) -> bool {
        successor_activation_fence_satisfied(
            successor_authority_now_ms,
            self.grant_not_after_ms,
            skew_budget_ms,
        )
    }

    pub(crate) fn grant_not_after_ms(self) -> u64 {
        self.grant_not_after_ms
    }

    pub(crate) fn authority(self) -> LeaseHorizonAuthorityBinding {
        self.authority
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MAX_LEASE_MS: u64 = 10_000;

    #[derive(Debug, Clone, Copy)]
    enum ModelEvent {
        AdvanceReal(u16),
        StepAuthorityWall(i16),
        StepFrontendWall(i16),
        StepStorageWall { node_slot: u8, delta_ms: i16 },
        IssueLease(u16),
        RefreshFrontend,
        RefreshStorage(u8),
        RestartFrontend,
        RestartStorage(u8),
        ChangeLeader(i16),
        SuspendFrontend(u16),
        SuspendStorage { node_slot: u8, elapsed_ms: u16 },
        ReestablishAuthorityClock,
        ReestablishFrontendClock,
        ReestablishStorageClock(u8),
        TryActivateSuccessor,
    }

    fn model_event_strategy() -> impl Strategy<Value = ModelEvent> {
        prop_oneof![
            8 => (0_u16..2_000).prop_map(ModelEvent::AdvanceReal),
            2 => any::<i16>().prop_map(ModelEvent::StepAuthorityWall),
            2 => any::<i16>().prop_map(ModelEvent::StepFrontendWall),
            2 => (0_u8..8, any::<i16>()).prop_map(|(node_slot, delta_ms)| {
                ModelEvent::StepStorageWall { node_slot, delta_ms }
            }),
            5 => (1_u16..=10_000).prop_map(ModelEvent::IssueLease),
            4 => Just(ModelEvent::RefreshFrontend),
            4 => (0_u8..8).prop_map(ModelEvent::RefreshStorage),
            1 => Just(ModelEvent::RestartFrontend),
            1 => (0_u8..8).prop_map(ModelEvent::RestartStorage),
            2 => any::<i16>().prop_map(ModelEvent::ChangeLeader),
            1 => (1_u16..2_000).prop_map(ModelEvent::SuspendFrontend),
            1 => (0_u8..8, 1_u16..2_000).prop_map(|(node_slot, elapsed_ms)| {
                ModelEvent::SuspendStorage { node_slot, elapsed_ms }
            }),
            2 => Just(ModelEvent::ReestablishAuthorityClock),
            2 => Just(ModelEvent::ReestablishFrontendClock),
            2 => (0_u8..8).prop_map(ModelEvent::ReestablishStorageClock),
            3 => Just(ModelEvent::TryActivateSuccessor),
        ]
    }

    #[derive(Debug)]
    struct LeaseModel {
        real_ms: u64,
        authority_offset_ms: i64,
        authority_clock_healthy: bool,
        authority_health_reference_offset_ms: i64,
        frontend_offset_ms: i64,
        frontend_clock_healthy: bool,
        frontend_health_reference_offset_ms: i64,
        storage_offset_ms: Vec<i64>,
        storage_clock_healthy: Vec<bool>,
        storage_health_reference_offset_ms: Vec<i64>,
        frontend_monotonic_ms: u64,
        storage_monotonic_ms: Vec<u64>,
        issued_at_ms: Option<u64>,
        deadline_ms: Option<u64>,
        deadline_issuer_offset_ms: Option<i64>,
        frontend_lease: Option<BoundRouteMapLease>,
        storage_lease: Vec<Option<BoundRouteMapLease>>,
        successor_active: bool,
    }

    impl LeaseModel {
        fn new(storage_node_count: usize) -> Self {
            Self {
                real_ms: 100_000,
                authority_offset_ms: 0,
                authority_clock_healthy: true,
                authority_health_reference_offset_ms: 0,
                frontend_offset_ms: 0,
                frontend_clock_healthy: true,
                frontend_health_reference_offset_ms: 0,
                storage_offset_ms: vec![0; storage_node_count],
                storage_clock_healthy: vec![true; storage_node_count],
                storage_health_reference_offset_ms: vec![0; storage_node_count],
                frontend_monotonic_ms: 10_000,
                storage_monotonic_ms: (0..storage_node_count)
                    .map(|index| 20_000 + u64::try_from(index).unwrap())
                    .collect(),
                issued_at_ms: None,
                deadline_ms: None,
                deadline_issuer_offset_ms: None,
                frontend_lease: None,
                storage_lease: vec![None; storage_node_count],
                successor_active: false,
            }
        }

        fn wall(real_ms: u64, offset_ms: i64) -> u64 {
            real_ms.saturating_add_signed(offset_ms)
        }

        fn authority_wall(&self) -> u64 {
            Self::wall(self.real_ms, self.authority_offset_ms)
        }

        fn frontend_wall(&self) -> u64 {
            Self::wall(self.real_ms, self.frontend_offset_ms)
        }

        fn storage_index(&self, node_slot: u8) -> usize {
            usize::from(node_slot) % self.storage_offset_ms.len()
        }

        fn storage_wall(&self, index: usize) -> u64 {
            Self::wall(self.real_ms, self.storage_offset_ms[index])
        }

        fn advance_real(&mut self, elapsed_ms: u64) {
            self.real_ms = self.real_ms.saturating_add(elapsed_ms);
            self.frontend_monotonic_ms = self.frontend_monotonic_ms.saturating_add(elapsed_ms);
            for monotonic_ms in &mut self.storage_monotonic_ms {
                *monotonic_ms = monotonic_ms.saturating_add(elapsed_ms);
            }
        }

        fn step_offset(offset_ms: &mut i64, delta_ms: i16) {
            *offset_ms = offset_ms.saturating_add(i64::from(delta_ms));
        }

        fn issue_lease(&mut self, requested_ms: u64) {
            if !self.authority_clock_healthy {
                return;
            }
            let issued_at_ms = self.authority_wall();
            let previous_deadline_ms = self.deadline_ms;
            let Ok(deadline_ms) = bounded_renewal_deadline(
                previous_deadline_ms,
                issued_at_ms,
                requested_ms,
                MAX_LEASE_MS,
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            ) else {
                return;
            };
            self.issued_at_ms = Some(issued_at_ms);
            self.deadline_ms = Some(deadline_ms);
            if previous_deadline_ms != Some(deadline_ms) {
                self.deadline_issuer_offset_ms = Some(self.authority_offset_ms);
            }
            self.successor_active = false;
        }

        fn refresh_frontend(&mut self) {
            if !self.frontend_clock_healthy {
                return;
            }
            if let (Some(issued_at_ms), Some(deadline_ms)) = (self.issued_at_ms, self.deadline_ms) {
                self.frontend_lease = BoundRouteMapLease::bind(
                    issued_at_ms,
                    deadline_ms,
                    self.frontend_wall(),
                    self.frontend_monotonic_ms,
                    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                )
                .ok();
            }
        }

        fn refresh_storage(&mut self, node_slot: u8) {
            let index = self.storage_index(node_slot);
            if !self.storage_clock_healthy[index] {
                return;
            }
            if let (Some(issued_at_ms), Some(deadline_ms)) = (self.issued_at_ms, self.deadline_ms) {
                self.storage_lease[index] = BoundRouteMapLease::bind(
                    issued_at_ms,
                    deadline_ms,
                    self.storage_wall(index),
                    self.storage_monotonic_ms[index],
                    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                )
                .ok();
            }
        }

        fn frontend_serving(&self) -> bool {
            self.frontend_lease
                .is_some_and(|lease| lease.is_valid_at_monotonic(self.frontend_monotonic_ms))
        }

        fn storage_serving(&self) -> bool {
            self.storage_lease
                .iter()
                .zip(&self.storage_monotonic_ms)
                .any(|(lease, monotonic_ms)| {
                    lease.is_some_and(|lease| lease.is_valid_at_monotonic(*monotonic_ms))
                })
        }

        fn try_activate_successor(&mut self) {
            self.successor_active = self.authority_clock_healthy
                && self.deadline_basis_within_budget()
                && self.deadline_ms.is_some_and(|deadline_ms| {
                    successor_activation_fence_satisfied(
                        self.authority_wall(),
                        deadline_ms,
                        CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                    )
                });
        }

        fn offset_difference_within_budget(left: i64, right: i64) -> bool {
            left.abs_diff(right) <= CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        }

        fn deadline_basis_within_budget(&self) -> bool {
            self.deadline_issuer_offset_ms
                .is_none_or(|issuer_offset_ms| {
                    Self::offset_difference_within_budget(
                        self.authority_offset_ms,
                        issuer_offset_ms,
                    )
                })
        }

        fn clocks_within_budget(&self) -> bool {
            let clocks = [self.authority_offset_ms, self.frontend_offset_ms]
                .into_iter()
                .chain(self.storage_offset_ms.iter().copied());
            let minimum = clocks.clone().min().unwrap();
            let maximum = clocks.max().unwrap();
            maximum.saturating_sub(minimum)
                <= i64::try_from(CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS).unwrap()
        }

        fn reestablish_authority_clock(&mut self) {
            if self.clocks_within_budget() && self.deadline_basis_within_budget() {
                self.authority_clock_healthy = true;
                self.authority_health_reference_offset_ms = self.authority_offset_ms;
            }
        }

        fn reestablish_frontend_clock(&mut self) {
            if self.authority_clock_healthy
                && Self::offset_difference_within_budget(
                    self.frontend_offset_ms,
                    self.authority_offset_ms,
                )
            {
                self.frontend_clock_healthy = true;
                self.frontend_health_reference_offset_ms = self.frontend_offset_ms;
            }
        }

        fn reestablish_storage_clock(&mut self, node_slot: u8) {
            let index = self.storage_index(node_slot);
            if self.authority_clock_healthy
                && Self::offset_difference_within_budget(
                    self.storage_offset_ms[index],
                    self.authority_offset_ms,
                )
            {
                self.storage_clock_healthy[index] = true;
                self.storage_health_reference_offset_ms[index] = self.storage_offset_ms[index];
            }
        }

        fn assert_invariants(&self) -> Result<(), TestCaseError> {
            if let (Some(issued_at_ms), Some(deadline_ms)) = (self.issued_at_ms, self.deadline_ms) {
                prop_assert!(validate_serving_deadline_bound(
                    deadline_ms,
                    issued_at_ms,
                    MAX_LEASE_MS,
                    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                )
                .is_ok());
            }
            if self.successor_active {
                prop_assert!(!self.frontend_serving());
                prop_assert!(!self.storage_serving());
            }
            Ok(())
        }

        fn apply(&mut self, event: ModelEvent) {
            match event {
                ModelEvent::AdvanceReal(elapsed_ms) => self.advance_real(u64::from(elapsed_ms)),
                ModelEvent::StepAuthorityWall(delta_ms) => {
                    Self::step_offset(&mut self.authority_offset_ms, delta_ms);
                    if !Self::offset_difference_within_budget(
                        self.authority_offset_ms,
                        self.authority_health_reference_offset_ms,
                    ) {
                        self.authority_clock_healthy = false;
                    }
                }
                ModelEvent::StepFrontendWall(delta_ms) => {
                    Self::step_offset(&mut self.frontend_offset_ms, delta_ms);
                    if !Self::offset_difference_within_budget(
                        self.frontend_offset_ms,
                        self.frontend_health_reference_offset_ms,
                    ) {
                        self.frontend_clock_healthy = false;
                    }
                }
                ModelEvent::StepStorageWall {
                    node_slot,
                    delta_ms,
                } => {
                    let index = self.storage_index(node_slot);
                    Self::step_offset(&mut self.storage_offset_ms[index], delta_ms);
                    if !Self::offset_difference_within_budget(
                        self.storage_offset_ms[index],
                        self.storage_health_reference_offset_ms[index],
                    ) {
                        self.storage_clock_healthy[index] = false;
                    }
                }
                ModelEvent::IssueLease(requested_ms) => {
                    self.issue_lease(u64::from(requested_ms));
                }
                ModelEvent::RefreshFrontend => self.refresh_frontend(),
                ModelEvent::RefreshStorage(node_slot) => self.refresh_storage(node_slot),
                ModelEvent::RestartFrontend => {
                    self.frontend_lease = None;
                    self.frontend_clock_healthy = false;
                }
                ModelEvent::RestartStorage(node_slot) => {
                    let index = self.storage_index(node_slot);
                    self.storage_lease[index] = None;
                    self.storage_clock_healthy[index] = false;
                }
                ModelEvent::ChangeLeader(delta_ms) => {
                    Self::step_offset(&mut self.authority_offset_ms, delta_ms);
                    self.authority_clock_healthy = false;
                }
                // A process that cannot prove its monotonic clock included suspend
                // time loses serving authority and must install a fresh map.
                ModelEvent::SuspendFrontend(elapsed_ms) => {
                    self.real_ms = self.real_ms.saturating_add(u64::from(elapsed_ms));
                    for monotonic_ms in &mut self.storage_monotonic_ms {
                        *monotonic_ms = monotonic_ms.saturating_add(u64::from(elapsed_ms));
                    }
                    self.frontend_lease = None;
                    self.frontend_clock_healthy = false;
                }
                ModelEvent::SuspendStorage {
                    node_slot,
                    elapsed_ms,
                } => {
                    let index = self.storage_index(node_slot);
                    self.real_ms = self.real_ms.saturating_add(u64::from(elapsed_ms));
                    self.frontend_monotonic_ms = self
                        .frontend_monotonic_ms
                        .saturating_add(u64::from(elapsed_ms));
                    for (storage_index, monotonic_ms) in
                        self.storage_monotonic_ms.iter_mut().enumerate()
                    {
                        if storage_index != index {
                            *monotonic_ms = monotonic_ms.saturating_add(u64::from(elapsed_ms));
                        }
                    }
                    self.storage_lease[index] = None;
                    self.storage_clock_healthy[index] = false;
                }
                ModelEvent::ReestablishAuthorityClock => self.reestablish_authority_clock(),
                ModelEvent::ReestablishFrontendClock => self.reestablish_frontend_clock(),
                ModelEvent::ReestablishStorageClock(node_slot) => {
                    self.reestablish_storage_clock(node_slot);
                }
                ModelEvent::TryActivateSuccessor => self.try_activate_successor(),
            }
        }
    }

    #[test]
    fn route_map_binding_is_not_reopened_by_wall_clock_rollback() {
        let lease = BoundRouteMapLease::bind(10_000, 20_000, 10_000, 500, 1_000).unwrap();
        assert_eq!(lease.authority_valid_until_ms(), 20_000);
        assert_eq!(lease.local_valid_until_monotonic_ms(), 9_500);
        assert!(lease.is_valid_at_monotonic(9_499));
        assert!(!lease.is_valid_at_monotonic(9_500));
    }

    #[test]
    fn process_clock_health_latches_wall_rollback_until_restart() {
        let mut health = ProcessLeaseClockHealth::new(10_000, Some(500)).unwrap();
        assert!(health.observe(10_500, Some(1_000), 1_000));
        assert!(!health.observe(4_000, Some(1_100), 1_000));
        assert!(!health.observe(10_600, Some(1_100), 1_000));

        let mut restarted = ProcessLeaseClockHealth::new(10_600, Some(0)).unwrap();
        assert!(restarted.observe(10_700, Some(100), 1_000));
    }

    #[test]
    fn process_clock_health_latches_excessive_forward_step() {
        let mut health = ProcessLeaseClockHealth::new(10_000, Some(500)).unwrap();
        assert!(!health.observe(12_001, Some(1_000), 1_000));
        assert!(!health.observe(10_500, Some(1_000), 1_000));
    }

    #[test]
    fn process_clock_health_uses_adjusted_elapsed_instead_of_raw_lease_rate() {
        let mut health = ProcessLeaseClockHealth::new(100_000, Some(10_000)).unwrap();
        let wall_ms = 111_500_u64;
        let raw_lease_clock_ms = 20_000_u64;
        let adjusted_clock_health_ms = 21_500_u64;

        let wall_elapsed_ms = wall_ms - 100_000;
        let raw_lease_elapsed_ms = raw_lease_clock_ms - 10_000;
        assert!(
            wall_elapsed_ms.abs_diff(raw_lease_elapsed_ms) > CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        );
        assert!(health.observe(
            wall_ms,
            Some(adjusted_clock_health_ms),
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        ));
    }

    #[test]
    fn process_clock_health_latches_later_source_failure() {
        let mut health = ProcessLeaseClockHealth::new(10_000, Some(500)).unwrap();
        assert!(health.observe(10_500, Some(1_000), 1_000));
        assert!(!health.observe(10_600, None, 1_000));
        assert!(!health.observe(10_700, Some(1_200), 1_000));
    }

    #[test]
    fn process_clock_health_rejects_missing_first_sample() {
        assert!(matches!(
            ProcessLeaseClockHealth::new(10_000, None),
            Err(LeaseClockError::ClockHealthSourceUnavailable)
        ));
    }

    #[test]
    fn route_map_binding_rejects_authority_ahead_of_skew_budget() {
        assert!(matches!(
            BoundRouteMapLease::bind(12_001, 20_000, 11_000, 500, 1_000),
            Err(LeaseClockError::AuthorityClockTooFarAhead { .. })
        ));
    }

    #[test]
    fn successor_waits_beyond_previous_deadline_by_skew_budget() {
        assert!(!successor_activation_fence_satisfied(10_999, 10_000, 1_000));
        assert!(successor_activation_fence_satisfied(11_000, 10_000, 1_000));
    }

    #[test]
    fn successor_fence_overflow_fails_closed() {
        assert!(!successor_activation_fence_satisfied(
            u64::MAX,
            u64::MAX - 500,
            1_000
        ));
    }

    #[test]
    fn fast_replacement_leader_cannot_activate_before_clock_reestablishment() {
        let mut model = LeaseModel::new(1);
        model.apply(ModelEvent::IssueLease(10_000));
        model.apply(ModelEvent::RefreshFrontend);
        model.apply(ModelEvent::ChangeLeader(32_767));
        model.apply(ModelEvent::ReestablishAuthorityClock);
        model.apply(ModelEvent::TryActivateSuccessor);

        assert!(!model.authority_clock_healthy);
        assert!(model.frontend_serving());
        assert!(!model.successor_active);

        model.apply(ModelEvent::StepAuthorityWall(-32_767));
        model.apply(ModelEvent::ReestablishAuthorityClock);
        assert!(model.authority_clock_healthy);
    }

    #[test]
    fn delayed_old_map_cannot_rebind_after_consumer_clock_rollback() {
        let mut model = LeaseModel::new(1);
        for event in [
            ModelEvent::IssueLease(343),
            ModelEvent::AdvanceReal(1_112),
            ModelEvent::StepFrontendWall(-5_343),
            ModelEvent::AdvanceReal(1_361),
            ModelEvent::AdvanceReal(1_870),
            ModelEvent::TryActivateSuccessor,
            ModelEvent::RefreshFrontend,
        ] {
            model.apply(event);
        }

        assert!(model.successor_active);
        assert!(!model.frontend_clock_healthy);
        assert!(!model.frontend_serving());
    }

    #[test]
    fn serving_deadline_rejects_far_future_authority_proposal() {
        assert!(matches!(
            validate_serving_deadline_bound(22_001, 10_000, 10_000, 2_000),
            Err(LeaseClockError::ServingDeadlineOutOfBounds { .. })
        ));
        validate_serving_deadline_bound(22_000, 10_000, 10_000, 2_000).unwrap();
    }

    #[test]
    fn renewal_does_not_shorten_a_deadline_or_preserve_one_outside_the_bound() {
        assert_eq!(
            bounded_renewal_deadline(Some(12_000), 10_000, 100, 10_000, 1_000).unwrap(),
            12_000
        );
        assert!(matches!(
            bounded_renewal_deadline(Some(22_001), 10_000, 100, 10_000, 2_000),
            Err(LeaseClockError::ServingDeadlineOutOfBounds { .. })
        ));
    }

    #[test]
    fn lease_arithmetic_overflow_fails_closed() {
        assert!(matches!(
            validate_serving_deadline_bound(u64::MAX, u64::MAX, 1, 0),
            Err(LeaseClockError::TimestampOverflow { .. })
        ));
        assert!(matches!(
            bounded_renewal_deadline(None, u64::MAX, 1, 10, 0),
            Err(LeaseClockError::TimestampOverflow { .. })
        ));
        assert!(matches!(
            BoundRouteMapLease::bind(1, u64::MAX, 1, u64::MAX, 0),
            Err(LeaseClockError::TimestampOverflow { .. })
        ));
    }

    #[test]
    fn committed_horizon_allows_many_volatile_renewals_without_advancing() {
        let authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
        let horizon = CommittedLeaseGrantHorizon::establish(
            None,
            authority,
            10_000,
            60_000,
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .unwrap();

        for heartbeat_now_ms in (10_000..60_000).step_by(250) {
            let lease_deadline_ms = heartbeat_now_ms + 1_000;
            horizon
                .validate_grant(authority, lease_deadline_ms)
                .unwrap();
        }
        assert_eq!(horizon.grant_not_after_ms(), 70_000);
        assert_eq!(horizon.authority().clock_generation(), 7);
        assert_eq!(horizon.authority().raft_term(), Some(11));
    }

    #[test]
    fn committed_horizon_rejects_deadline_or_authority_outside_capability() {
        let authority = LeaseHorizonAuthorityBinding::new(3, Some(5));
        let horizon =
            CommittedLeaseGrantHorizon::establish(None, authority, 10_000, 5_000, 1_000).unwrap();

        assert!(matches!(
            horizon.validate_grant(authority, 15_001),
            Err(LeaseHorizonError::DeadlineBeyondHorizon { .. })
        ));
        assert!(matches!(
            horizon.validate_grant(LeaseHorizonAuthorityBinding::new(4, Some(5)), 14_000),
            Err(LeaseHorizonError::AuthorityMismatch { .. })
        ));
        assert!(matches!(
            horizon.validate_grant(LeaseHorizonAuthorityBinding::new(3, Some(6)), 14_000),
            Err(LeaseHorizonError::AuthorityMismatch { .. })
        ));
    }

    #[test]
    fn replacement_authority_waits_for_previous_horizon_and_skew() {
        let old_authority = LeaseHorizonAuthorityBinding::new(3, Some(5));
        let new_authority = LeaseHorizonAuthorityBinding::new(4, Some(6));
        let horizon =
            CommittedLeaseGrantHorizon::establish(None, old_authority, 10_000, 5_000, 1_000)
                .unwrap();

        assert!(!horizon.successor_fence_satisfied(15_999, 1_000));
        assert!(matches!(
            CommittedLeaseGrantHorizon::establish(
                Some(horizon),
                new_authority,
                15_999,
                5_000,
                1_000,
            ),
            Err(LeaseHorizonError::PreviousHorizonStillActive { .. })
        ));
        let replacement = CommittedLeaseGrantHorizon::establish(
            Some(horizon),
            new_authority,
            16_000,
            5_000,
            1_000,
        )
        .unwrap();
        assert_eq!(replacement.grant_not_after_ms(), 21_000);
    }

    #[test]
    fn committed_horizon_extension_lost_response_extends_successor_fence() {
        let authority = LeaseHorizonAuthorityBinding::new(1, Some(1));
        let initial =
            CommittedLeaseGrantHorizon::establish(None, authority, 10_000, 5_000, 1_000).unwrap();
        // The response is lost after durability, so recovery observes the
        // extended horizon even though no caller saw an acknowledgement.
        let recovered =
            CommittedLeaseGrantHorizon::establish(Some(initial), authority, 12_000, 10_000, 1_000)
                .unwrap();

        assert!(!recovered.successor_fence_satisfied(22_999, 1_000));
        assert!(recovered.successor_fence_satisfied(23_000, 1_000));
    }

    #[test]
    fn committed_horizon_arithmetic_overflow_fails_closed() {
        let authority = LeaseHorizonAuthorityBinding::new(1, None);
        assert!(matches!(
            CommittedLeaseGrantHorizon::establish(None, authority, u64::MAX, 1, 1_000),
            Err(LeaseHorizonError::TimestampOverflow { .. })
        ));
        let horizon =
            CommittedLeaseGrantHorizon::establish(None, authority, u64::MAX - 2_000, 1_500, 1_000)
                .unwrap();
        assert!(!horizon.successor_fence_satisfied(u64::MAX, 1_000));
        assert!(matches!(
            CommittedLeaseGrantHorizon::establish(
                Some(horizon),
                LeaseHorizonAuthorityBinding::new(2, None),
                u64::MAX,
                0,
                1_000,
            ),
            Err(LeaseHorizonError::TimestampOverflow { .. })
        ));
    }

    #[derive(Debug, Clone, Copy)]
    enum HorizonModelEvent {
        Advance(u16),
        Extend(u16),
        ExtendDurableResponseLost(u16),
        ExtendLostBeforeDurable(u16),
        IssueLease(u16),
        RefreshConsumer,
        ChangeAuthority,
        ReestablishClock,
        TryActivateSuccessor,
    }

    fn horizon_model_event_strategy() -> impl Strategy<Value = HorizonModelEvent> {
        prop_oneof![
            8 => (0_u16..2_000).prop_map(HorizonModelEvent::Advance),
            3 => (1_u16..30_000).prop_map(HorizonModelEvent::Extend),
            2 => (1_u16..30_000).prop_map(HorizonModelEvent::ExtendDurableResponseLost),
            2 => (1_u16..30_000).prop_map(HorizonModelEvent::ExtendLostBeforeDurable),
            8 => (1_u16..=10_000).prop_map(HorizonModelEvent::IssueLease),
            5 => Just(HorizonModelEvent::RefreshConsumer),
            2 => Just(HorizonModelEvent::ChangeAuthority),
            2 => Just(HorizonModelEvent::ReestablishClock),
            3 => Just(HorizonModelEvent::TryActivateSuccessor),
        ]
    }

    #[derive(Debug)]
    struct LeaseHorizonModel {
        authority_now_ms: u64,
        consumer_monotonic_ms: u64,
        authority: LeaseHorizonAuthorityBinding,
        authority_clock_healthy: bool,
        durable_horizon: Option<CommittedLeaseGrantHorizon>,
        acknowledged_issued_at_ms: Option<u64>,
        acknowledged_deadline_ms: Option<u64>,
        consumer_lease: Option<BoundRouteMapLease>,
        successor_active: bool,
    }

    impl LeaseHorizonModel {
        fn new() -> Self {
            Self {
                authority_now_ms: 100_000,
                consumer_monotonic_ms: 10_000,
                authority: LeaseHorizonAuthorityBinding::new(1, Some(1)),
                authority_clock_healthy: true,
                durable_horizon: None,
                acknowledged_issued_at_ms: None,
                acknowledged_deadline_ms: None,
                consumer_lease: None,
                successor_active: false,
            }
        }

        fn extend(&mut self, duration_ms: u64) {
            if !self.authority_clock_healthy {
                return;
            }
            let Ok(horizon) = CommittedLeaseGrantHorizon::establish(
                self.durable_horizon,
                self.authority,
                self.authority_now_ms,
                duration_ms,
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            ) else {
                return;
            };
            self.durable_horizon = Some(horizon);
        }

        fn issue_lease(&mut self, requested_ms: u64) {
            if !self.authority_clock_healthy {
                return;
            }
            let Some(horizon) = self.durable_horizon else {
                return;
            };
            let Some(deadline_ms) = self.authority_now_ms.checked_add(requested_ms) else {
                return;
            };
            if horizon.validate_grant(self.authority, deadline_ms).is_err() {
                return;
            }
            self.acknowledged_issued_at_ms = Some(self.authority_now_ms);
            self.acknowledged_deadline_ms = Some(deadline_ms);
            self.successor_active = false;
        }

        fn refresh_consumer(&mut self) {
            let (Some(issued_at_ms), Some(deadline_ms)) = (
                self.acknowledged_issued_at_ms,
                self.acknowledged_deadline_ms,
            ) else {
                return;
            };
            self.consumer_lease = BoundRouteMapLease::bind(
                issued_at_ms,
                deadline_ms,
                self.authority_now_ms,
                self.consumer_monotonic_ms,
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            )
            .ok();
        }

        fn consumer_serving(&self) -> bool {
            self.consumer_lease
                .is_some_and(|lease| lease.is_valid_at_monotonic(self.consumer_monotonic_ms))
        }

        fn apply(&mut self, event: HorizonModelEvent) {
            match event {
                HorizonModelEvent::Advance(elapsed_ms) => {
                    self.authority_now_ms =
                        self.authority_now_ms.saturating_add(u64::from(elapsed_ms));
                    self.consumer_monotonic_ms = self
                        .consumer_monotonic_ms
                        .saturating_add(u64::from(elapsed_ms));
                }
                HorizonModelEvent::Extend(duration_ms)
                | HorizonModelEvent::ExtendDurableResponseLost(duration_ms) => {
                    self.extend(u64::from(duration_ms));
                }
                HorizonModelEvent::ExtendLostBeforeDurable(_duration_ms) => {}
                HorizonModelEvent::IssueLease(requested_ms) => {
                    self.issue_lease(u64::from(requested_ms));
                }
                HorizonModelEvent::RefreshConsumer => self.refresh_consumer(),
                HorizonModelEvent::ChangeAuthority => {
                    self.authority = LeaseHorizonAuthorityBinding::new(
                        self.authority.clock_generation().saturating_add(1),
                        self.authority
                            .raft_term()
                            .map(|term| term.saturating_add(1)),
                    );
                    self.authority_clock_healthy = false;
                    self.acknowledged_issued_at_ms = None;
                    self.acknowledged_deadline_ms = None;
                    self.successor_active = false;
                }
                HorizonModelEvent::ReestablishClock => {
                    self.authority_clock_healthy = true;
                }
                HorizonModelEvent::TryActivateSuccessor => {
                    self.successor_active = self.authority_clock_healthy
                        && self.durable_horizon.is_none_or(|horizon| {
                            horizon.successor_fence_satisfied(
                                self.authority_now_ms,
                                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                            )
                        });
                }
            }
        }

        fn assert_invariants(&self) -> Result<(), TestCaseError> {
            if let Some(deadline_ms) = self.acknowledged_deadline_ms {
                let horizon = self
                    .durable_horizon
                    .expect("acknowledged lease requires durable horizon");
                prop_assert!(horizon.validate_grant(self.authority, deadline_ms).is_ok());
            }
            if self.successor_active {
                prop_assert!(!self.consumer_serving());
            }
            Ok(())
        }
    }

    #[test]
    fn corrected_clock_recovers_with_one_fresh_binding() {
        assert!(BoundRouteMapLease::bind(20_000, 30_000, 10_000, 500, 1_000).is_err());

        let recovered = BoundRouteMapLease::bind(20_000, 30_000, 19_500, 600, 1_000).unwrap();
        assert!(recovered.is_valid_at_monotonic(9_999));
        assert!(!recovered.is_valid_at_monotonic(10_100));
    }

    proptest! {
        #[test]
        fn independent_clock_events_preserve_lease_and_successor_fences(
            events in proptest::collection::vec(model_event_strategy(), 1..128),
            storage_node_count in 1_usize..8,
        ) {
            let mut model = LeaseModel::new(storage_node_count);
            for event in events {
                model.apply(event);
                model.assert_invariants()?;
            }
        }

        #[test]
        fn committed_horizon_events_preserve_volatile_lease_and_successor_fences(
            events in proptest::collection::vec(horizon_model_event_strategy(), 1..192),
        ) {
            let mut model = LeaseHorizonModel::new();
            for event in events {
                model.apply(event);
                model.assert_invariants()?;
            }
        }
    }
}
