// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockCheckpointBinding(
    [u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN],
);

impl std::fmt::Debug for ControlPlaneAuthorityClockCheckpointBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAuthorityClockCheckpointBinding")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneAuthorityClockCheckpointBinding {
    #[must_use]
    pub fn for_raft(cluster_name: &str, node_id: u64) -> Self {
        let mut context = checksum::sha256::Sha256::new();
        context.update(b"argmin-control-plane-clock-checkpoint-raft-v1\0");
        context.update(&(cluster_name.len() as u64).to_be_bytes());
        context.update(cluster_name.as_bytes());
        context.update(&node_id.to_be_bytes());
        Self(context.finalize())
    }

    fn generate_single_authority() -> Result<Self, ControlPlaneError> {
        let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        argmin_crypto::random::fill(&mut binding).map_err(|_| {
            ControlPlaneError::io(
                "generate single-authority control-plane durable identity",
                std::io::Error::other(
                    "secure random source failed while generating control-plane identity",
                ),
            )
        })?;
        Ok(Self(binding))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockRestartCheckpoint {
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    authority_generation: u64,
    committed_timestamp_high_water_ms: Option<u64>,
    wall_time_ms: u64,
    health_time_ms: u64,
}

impl ControlPlaneAuthorityClockRestartCheckpoint {
    #[must_use]
    pub fn new(
        binding: ControlPlaneAuthorityClockCheckpointBinding,
        authority_generation: u64,
        committed_timestamp_high_water_ms: Option<u64>,
        wall_time_ms: u64,
        health_time_ms: u64,
    ) -> Self {
        Self {
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            wall_time_ms,
            health_time_ms,
        }
    }

    #[must_use]
    pub fn authority_generation(self) -> u64 {
        self.authority_generation
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn wall_time_ms(self) -> u64 {
        self.wall_time_ms
    }

    #[must_use]
    pub fn health_time_ms(self) -> u64 {
        self.health_time_ms
    }

    fn from_process_clock(
        binding: ControlPlaneAuthorityClockCheckpointBinding,
        authority_generation: u64,
        committed_timestamp_high_water_ms: Option<u64>,
    ) -> Result<Self, ControlPlaneError> {
        if authority_generation == 0 {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let sample = control_plane_process_clock_sample()?;
        let health_time_ms = sample
            .health_time_ms()
            .ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        Ok(Self::new(
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            sample.wall_time_ms(),
            health_time_ms,
        ))
    }

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CONTROL_PLANE_CLOCK_CHECKPOINT_LEN);
        bytes.extend_from_slice(CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.binding.0);
        bytes.extend_from_slice(&self.authority_generation.to_be_bytes());
        match self.committed_timestamp_high_water_ms {
            Some(timestamp_ms) => {
                bytes.push(1);
                bytes.extend_from_slice(&timestamp_ms.to_be_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&0u64.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&self.wall_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.health_time_ms.to_be_bytes());
        bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
        bytes
    }

    fn decode(
        bytes: &[u8],
        expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Result<Self, ControlPlaneError> {
        if bytes.len() != CONTROL_PLANE_CLOCK_CHECKPOINT_LEN {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: format!(
                    "checkpoint length {} does not match required fixed length {CONTROL_PLANE_CLOCK_CHECKPOINT_LEN}",
                    bytes.len()
                ),
            });
        }
        let (body, encoded_checksum) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_CLOCK_CHECKPOINT_CHECKSUM_LEN);
        let actual_checksum = checksum::crc64::checksum(body);
        let expected_checksum = u64::from_be_bytes(
            encoded_checksum
                .try_into()
                .expect("clock checkpoint checksum has fixed length"),
        );
        if actual_checksum != expected_checksum {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint checksum mismatch".to_owned(),
            });
        }
        let mut offset = 0usize;
        let mut take = |len: usize| -> Result<&[u8], ControlPlaneError> {
            let end = offset.checked_add(len).ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "checkpoint offset overflow".to_owned(),
                }
            })?;
            let value = body.get(offset..end).ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "checkpoint is truncated".to_owned(),
                }
            })?;
            offset = end;
            Ok(value)
        };
        if take(CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len())? != CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC
        {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint magic mismatch".to_owned(),
            });
        }
        let version = u16::from_be_bytes(
            take(2)?
                .try_into()
                .expect("clock checkpoint version has fixed length"),
        );
        if version != CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: format!("unsupported checkpoint version {version}"),
            });
        }
        let binding = Self::read_binding(take(CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN)?);
        if binding != expected_binding {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint durable-state identity does not match this authority"
                    .to_owned(),
            });
        }
        let authority_generation = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint authority generation has fixed length"),
        );
        if authority_generation == 0 {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let timestamp_tag = take(1)?[0];
        let encoded_timestamp = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint timestamp has fixed length"),
        );
        let committed_timestamp_high_water_ms = match timestamp_tag {
            0 if encoded_timestamp == 0 => None,
            0 => {
                return Err(ControlPlaneError::AuthorityClockCheckpoint {
                    message: "absent checkpoint timestamp must use canonical zero value".to_owned(),
                });
            }
            1 => Some(encoded_timestamp),
            tag => {
                return Err(ControlPlaneError::AuthorityClockCheckpoint {
                    message: format!("invalid checkpoint timestamp option tag {tag}"),
                });
            }
        };
        let wall_time_ms = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint wall time has fixed length"),
        );
        let health_time_ms = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint health time has fixed length"),
        );
        if offset != body.len() {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint has trailing bytes".to_owned(),
            });
        }
        Ok(Self::new(
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            wall_time_ms,
            health_time_ms,
        ))
    }

    fn read_binding(bytes: &[u8]) -> ControlPlaneAuthorityClockCheckpointBinding {
        let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        binding.copy_from_slice(bytes);
        ControlPlaneAuthorityClockCheckpointBinding(binding)
    }
}

/// Process-local authority clock gate for timestamp-bearing control-plane work.
///
/// Replicated apply can validate timestamp ordering and deadline relationships,
/// but only the command issuer can compare wall-clock progress with monotonic
/// elapsed time. Durable process startup additionally requires node-local
/// wall/health lineage evidence whenever restored state has a timestamp
/// high-water. Production construction always applies that restart-continuity
/// rule; sample-driven constructors exist only on the test surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneAuthorityClockBlockedReason {
    InitialTimestampDiscontinuity,
    RaftLeadershipChanged,
    ClockSourceUnavailable,
    ClockHealthRegression,
    WallClockRegression,
    WallClockForwardJump,
    CheckpointPersistenceFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockStatus {
    generation: u64,
    established: bool,
    blocked_reason: Option<ControlPlaneAuthorityClockBlockedReason>,
    committed_timestamp_high_water_ms: Option<u64>,
    bound_raft_leadership_term: Option<u64>,
    current_raft_leadership_term: Option<u64>,
    local_raft_authority_leader: bool,
    local_raft_authority_serving: bool,
}

impl ControlPlaneAuthorityClockStatus {
    #[must_use]
    pub fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn established(self) -> bool {
        self.established
    }

    #[must_use]
    pub fn blocked_reason(self) -> Option<ControlPlaneAuthorityClockBlockedReason> {
        self.blocked_reason
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn bound_raft_leadership_term(self) -> Option<u64> {
        self.bound_raft_leadership_term
    }

    #[must_use]
    pub fn current_raft_leadership_term(self) -> Option<u64> {
        self.current_raft_leadership_term
    }

    #[must_use]
    pub fn local_raft_authority_leader(self) -> bool {
        self.local_raft_authority_leader
    }

    #[must_use]
    pub fn local_raft_authority_serving(self) -> bool {
        self.local_raft_authority_serving
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockContext {
    committed_timestamp_high_water_ms: Option<u64>,
    current_raft_leadership_term: Option<u64>,
    local_raft_authority_leader: bool,
    local_raft_authority_serving: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockAdminSample {
    auth_authority_now_ms: u64,
    wall_ms: u64,
    clock_health_ms: Option<u64>,
}

impl ControlPlaneAuthorityClockAdminSample {
    #[must_use]
    pub fn new(auth_authority_now_ms: u64, wall_ms: u64, clock_health_ms: Option<u64>) -> Self {
        Self {
            auth_authority_now_ms,
            wall_ms,
            clock_health_ms,
        }
    }

    pub fn from_process_clock() -> Result<Self, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        Ok(Self::new(
            sample.wall_time_ms(),
            sample.wall_time_ms(),
            sample.health_time_ms(),
        ))
    }
}

impl ControlPlaneAuthorityClockContext {
    #[must_use]
    pub fn new(
        committed_timestamp_high_water_ms: Option<u64>,
        current_raft_leadership_term: Option<u64>,
        local_raft_authority_leader: bool,
        local_raft_authority_serving: bool,
    ) -> Self {
        debug_assert!(!local_raft_authority_serving || local_raft_authority_leader);
        Self {
            committed_timestamp_high_water_ms,
            current_raft_leadership_term,
            local_raft_authority_leader,
            local_raft_authority_serving,
        }
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn current_raft_leadership_term(self) -> Option<u64> {
        self.current_raft_leadership_term
    }

    #[must_use]
    pub fn local_raft_authority_leader(self) -> bool {
        self.local_raft_authority_leader
    }
}

#[derive(Debug)]
pub struct ControlPlaneAuthorityClock {
    reference_wall_ms: u64,
    reference_clock_health_ms: u64,
    minimum_timestamp_ms: Option<u64>,
    raft_leadership_term: Option<u64>,
    initial_raft_term_binding_available: bool,
    established: bool,
    generation: u64,
    blocked_reason: Option<ControlPlaneAuthorityClockBlockedReason>,
    restart_continuity_generation: Option<u64>,
}

impl ControlPlaneAuthorityClock {
    pub fn new_from_process_clock_with_restart_checkpoint(
        max_committed_timestamp_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ) -> Result<Self, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        Self::new_internal(
            max_committed_timestamp_ms,
            sample.wall_time_ms(),
            sample.health_time_ms(),
            restart_checkpoint,
            false,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_internal(
            max_committed_timestamp_ms,
            wall_ms,
            clock_health_ms,
            None,
            true,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new_with_restart_checkpoint(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_internal(
            max_committed_timestamp_ms,
            wall_ms,
            clock_health_ms,
            restart_checkpoint,
            false,
        )
    }

    fn new_internal(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
        allow_uncheckpointed_initial_state: bool,
    ) -> Result<Self, ControlPlaneError> {
        if restart_checkpoint.is_some_and(|checkpoint| checkpoint.authority_generation == 0) {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let clock_health_ms =
            clock_health_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        let established = max_committed_timestamp_ms.is_none_or(|committed_ms| {
            restart_checkpoint.map_or_else(
                || {
                    allow_uncheckpointed_initial_state
                        && wall_ms.abs_diff(committed_ms) <= CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
                },
                |checkpoint| {
                    checkpoint
                        .committed_timestamp_high_water_ms
                        .is_none_or(|checkpoint_ms| checkpoint_ms <= committed_ms)
                        && checkpoint
                            .committed_timestamp_high_water_ms
                            .is_none_or(|checkpoint_ms| checkpoint.wall_time_ms >= checkpoint_ms)
                        && wall_ms >= committed_ms
                        && wall_ms
                            .checked_sub(checkpoint.wall_time_ms)
                            .zip(clock_health_ms.checked_sub(checkpoint.health_time_ms))
                            .is_some_and(|(wall_elapsed_ms, health_elapsed_ms)| {
                                wall_elapsed_ms.abs_diff(health_elapsed_ms)
                                    <= CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
                            })
                },
            )
        });
        let blocked_reason = (!established)
            .then_some(ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity);
        let checkpoint_generation =
            restart_checkpoint.map(|checkpoint| checkpoint.authority_generation);
        let restart_continuity_generation = established.then_some(checkpoint_generation).flatten();
        Ok(Self {
            reference_wall_ms: wall_ms,
            reference_clock_health_ms: clock_health_ms,
            minimum_timestamp_ms: max_committed_timestamp_ms,
            raft_leadership_term: None,
            initial_raft_term_binding_available: max_committed_timestamp_ms.is_none(),
            established,
            generation: checkpoint_generation.unwrap_or(1),
            blocked_reason,
            restart_continuity_generation,
        })
    }

    fn advance_generation(&mut self) -> Result<(), ControlPlaneError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        Ok(())
    }

    #[must_use]
    pub fn is_established(&self) -> bool {
        self.established
    }

    fn latch_unhealthy(
        &mut self,
        reason: ControlPlaneAuthorityClockBlockedReason,
    ) -> Result<(), ControlPlaneError> {
        if self.established || self.blocked_reason != Some(reason) {
            self.advance_generation()?;
        }
        self.established = false;
        self.blocked_reason = Some(reason);
        self.restart_continuity_generation = None;
        Ok(())
    }

    pub fn fail_closed_after_checkpoint_persistence_failure(
        &mut self,
    ) -> Result<(), ControlPlaneError> {
        self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure)
    }

    #[must_use]
    pub fn status(
        &self,
        context: ControlPlaneAuthorityClockContext,
    ) -> ControlPlaneAuthorityClockStatus {
        ControlPlaneAuthorityClockStatus {
            generation: self.generation,
            established: self.established,
            blocked_reason: self.blocked_reason,
            committed_timestamp_high_water_ms: context.committed_timestamp_high_water_ms,
            bound_raft_leadership_term: self.raft_leadership_term,
            current_raft_leadership_term: context.current_raft_leadership_term,
            local_raft_authority_leader: context.local_raft_authority_leader,
            local_raft_authority_serving: context.local_raft_authority_serving,
        }
    }

    /// Return the durable lease-horizon identity for the currently established authority.
    pub fn lease_horizon_authority_binding(
        &self,
        current_raft_leadership_term: Option<u64>,
    ) -> Result<LeaseHorizonAuthorityBinding, ControlPlaneError> {
        if !self.established {
            return Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: self.blocked_reason,
            });
        }
        if self.raft_leadership_term != current_raft_leadership_term {
            return Err(ControlPlaneError::AuthorityClockRaftTermMismatch {
                expected_term: self.raft_leadership_term,
                actual_term: current_raft_leadership_term,
            });
        }
        LeaseHorizonAuthorityBinding::checked_new(self.generation, current_raft_leadership_term)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)
    }

    /// Ensure a restarted process cannot reuse the generation that established
    /// a restored volatile-lease capability.
    pub fn advance_generation_past_lease_horizon(
        &mut self,
        previous_authority: LeaseHorizonAuthorityBinding,
    ) -> Result<(), ControlPlaneError> {
        let next_generation = previous_authority
            .clock_generation()
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        self.generation = self.generation.max(next_generation);
        self.restart_continuity_generation = None;
        Ok(())
    }

    /// Resume a single-authority horizon after a checkpoint-proven restart.
    ///
    /// The process must already hold the durable-state lock. Raft horizons
    /// never use this path because leadership terms provide their own
    /// successor identity and must retain the ordinary rebinding fence.
    pub fn resume_single_authority_lease_horizon_generation(
        &mut self,
        previous_authority: LeaseHorizonAuthorityBinding,
    ) -> bool {
        if self.restart_continuity_generation != Some(self.generation)
            || !self.established
            || self.raft_leadership_term.is_some()
            || previous_authority.raft_term().is_some()
            || previous_authority.clock_generation() != self.generation
        {
            return false;
        }
        self.restart_continuity_generation = None;
        true
    }

    /// Observe current authority and clock state before reporting status.
    pub fn observe_status(
        &mut self,
        context: ControlPlaneAuthorityClockContext,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.observe_committed_timestamp_high_water(context.committed_timestamp_high_water_ms);
        if let Some(term) = context.current_raft_leadership_term {
            if let Err(error) = self.validate_raft_leadership_term(term) {
                return if self.established {
                    Err(error)
                } else {
                    Ok(self.status(context))
                };
            }
        }
        if let Err(error) = self.effective_now_ms(wall_ms, clock_health_ms) {
            return if self.established {
                Err(error)
            } else {
                Ok(self.status(context))
            };
        }
        Ok(self.status(context))
    }

    pub fn reestablish(
        &mut self,
        expected_generation: u64,
        expected_committed_timestamp_high_water_ms: Option<u64>,
        expected_raft_leadership_term: Option<u64>,
        context: ControlPlaneAuthorityClockContext,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        if self.established {
            return Err(ControlPlaneError::AuthorityClockAlreadyEstablished);
        }
        if expected_generation != self.generation {
            return Err(ControlPlaneError::AuthorityClockGenerationMismatch {
                expected_generation,
                actual_generation: self.generation,
            });
        }
        if expected_committed_timestamp_high_water_ms != context.committed_timestamp_high_water_ms {
            return Err(
                ControlPlaneError::AuthorityClockCommittedTimestampMismatch {
                    expected_timestamp_ms: expected_committed_timestamp_high_water_ms,
                    actual_timestamp_ms: context.committed_timestamp_high_water_ms,
                },
            );
        }
        if expected_raft_leadership_term != context.current_raft_leadership_term {
            return Err(ControlPlaneError::AuthorityClockRaftTermMismatch {
                expected_term: expected_raft_leadership_term,
                actual_term: context.current_raft_leadership_term,
            });
        }
        if context.current_raft_leadership_term.is_some() && !context.local_raft_authority_serving {
            return Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority);
        }
        if let Some(committed_timestamp_high_water_ms) = context.committed_timestamp_high_water_ms {
            if wall_ms < committed_timestamp_high_water_ms {
                return Err(
                    ControlPlaneError::AuthorityClockWallBehindCommittedTimestamp {
                        wall_ms,
                        committed_timestamp_high_water_ms,
                    },
                );
            }
        }
        let clock_health_ms =
            clock_health_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        self.reference_wall_ms = wall_ms;
        self.reference_clock_health_ms = clock_health_ms;
        self.minimum_timestamp_ms = context.committed_timestamp_high_water_ms;
        self.raft_leadership_term = context.current_raft_leadership_term;
        self.initial_raft_term_binding_available = false;
        self.established = true;
        self.blocked_reason = None;
        self.generation = next_generation;
        self.restart_continuity_generation = None;
        Ok(self.status(context))
    }

    pub fn bind_initial_raft_leadership_term(&mut self, term: Option<u64>) {
        self.raft_leadership_term = term;
        self.initial_raft_term_binding_available = false;
        self.restart_continuity_generation = None;
    }

    pub fn observe_committed_timestamp_high_water(&mut self, timestamp_ms: Option<u64>) {
        if let Some(timestamp_ms) = timestamp_ms {
            self.minimum_timestamp_ms = Some(
                self.minimum_timestamp_ms
                    .map_or(timestamp_ms, |current| current.max(timestamp_ms)),
            );
        }
    }

    /// Bind clock authority to one locally serving Raft leadership term.
    pub fn validate_raft_leadership_term(&mut self, term: u64) -> Result<(), ControlPlaneError> {
        match self.raft_leadership_term {
            Some(established_term) if established_term == term => Ok(()),
            None if self.minimum_timestamp_ms.is_none()
                && self.initial_raft_term_binding_available =>
            {
                self.raft_leadership_term = Some(term);
                self.initial_raft_term_binding_available = false;
                Ok(())
            }
            established_term => {
                self.latch_unhealthy(
                    ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged,
                )?;
                Err(ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term,
                    current_term: term,
                })
            }
        }
    }

    /// Validate the process clock and return a non-regressing command timestamp.
    pub fn effective_now_ms(
        &mut self,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<u64, ControlPlaneError> {
        let Some(clock_health_ms) = clock_health_ms else {
            self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable)?;
            return Err(ControlPlaneError::AuthorityClockSourceUnavailable);
        };
        let max_committed_timestamp_ms = self.minimum_timestamp_ms.unwrap_or(wall_ms);
        if !self.established {
            return Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: self.blocked_reason,
            });
        }

        let Some(monotonic_elapsed_ms) =
            clock_health_ms.checked_sub(self.reference_clock_health_ms)
        else {
            self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression)?;
            return Err(ControlPlaneError::CommittedTimestampRegression {
                timestamp_ms: clock_health_ms,
                max_committed_timestamp_ms: self.reference_clock_health_ms,
            });
        };
        let expected_wall_ms = self
            .reference_wall_ms
            .checked_add(monotonic_elapsed_ms)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        if wall_ms.abs_diff(expected_wall_ms) > CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS {
            self.latch_unhealthy(if wall_ms < expected_wall_ms {
                ControlPlaneAuthorityClockBlockedReason::WallClockRegression
            } else {
                ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump
            })?;
            return Err(if wall_ms < expected_wall_ms {
                ControlPlaneError::CommittedTimestampRegression {
                    timestamp_ms: wall_ms,
                    max_committed_timestamp_ms: expected_wall_ms,
                }
            } else {
                ControlPlaneError::CommittedTimestampTooFarAhead {
                    timestamp_ms: wall_ms,
                    max_committed_timestamp_ms: expected_wall_ms,
                    max_forward_jump_ms: CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                }
            });
        }

        let effective_now_ms = wall_ms.max(max_committed_timestamp_ms);
        self.minimum_timestamp_ms = Some(effective_now_ms);
        Ok(effective_now_ms)
    }

    pub fn effective_process_now_ms(&mut self) -> Result<u64, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        self.effective_now_ms(sample.wall_time_ms(), sample.health_time_ms())
    }
}
