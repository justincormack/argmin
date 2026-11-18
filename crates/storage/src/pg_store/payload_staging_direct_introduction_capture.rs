// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One read-only construction algorithm, entered only with owner-minted embedded
//! or receiver admission. No raw command, generation or timestamp is accepted.
use super::*;
use crate::node_runtime::engine::DirectIntroductionAuthority;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::server::RemoteDirectIntroductionAuthority;

#[must_use]
pub(in crate::node_runtime) struct CapturedDirectIntroductionCore {
    pub(super) submission: submission::ContextualSubmission,
    pub(super) fence: AdmittedRouteEffectFence,
    pub(super) publication_deadline: Instant,
    pub(super) publication_cutoff: StorageRpcOperationDeadline,
}

// Private to this construction owner: callers cannot inject a validation closure
// or implement an alternative authority. The two adapters retain actual owners.
trait Authority {
    fn require_store(&self, pg: &PgStore) -> Result<(), StoreError>;
    fn environment(&self) -> Result<Environment<'_>, StoreError>;
}

struct Environment<'a> {
    topology: &'a PgTopology,
    placement: PayloadPlacementInput<'a>,
    node: NodeId,
    pg: PgId,
    epoch: ClusterEpoch,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
}

impl Authority for DirectIntroductionAuthority<'_, '_, '_> {
    fn require_store(&self, pg: &PgStore) -> Result<(), StoreError> {
        DirectIntroductionAuthority::require_store(self, pg)
    }
    fn environment(&self) -> Result<Environment<'_>, StoreError> {
        let section = self.section();
        Ok(Environment {
            topology: section.runtime()?.pg_topology(),
            placement: section.staging_placement()?,
            node: section.node_id(),
            pg: section.pg_id(),
            epoch: section.epoch(),
            fence: section.staging_capture_fence()?,
            deadline: section.deadline(),
        })
    }
}

impl Authority for RemoteDirectIntroductionAuthority<'_> {
    fn require_store(&self, pg: &PgStore) -> Result<(), StoreError> {
        self.require_capture(pg)
    }
    fn environment(&self) -> Result<Environment<'_>, StoreError> {
        let route = self.route();
        Ok(Environment {
            topology: self.topology(),
            placement: self.placement(),
            node: route.node,
            pg: route.pg,
            epoch: route.epoch,
            fence: self.effect_fence(),
            deadline: self.deadline(),
        })
    }
}

impl CapturedDirectIntroductionCore {
    pub(in crate::node_runtime) fn encode_response(
        &self,
        scope: &crate::node_runtime::server::DirectIntroductionResponseScope<'_, '_>,
    ) -> Result<(Vec<u8>, Instant), StagingWindowConfirmationError> {
        let bytes = self.submission.encode_introduction_response(self, scope)?;
        Ok((bytes, self.publication_deadline))
    }

    pub(in crate::node_runtime::pg_store) fn require_response_submission(
        &self,
        submission: &submission::ContextualSubmission,
    ) -> Result<StorageRpcOperationDeadline, StoreError> {
        if !std::ptr::eq(submission, &self.submission) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction response submission",
            });
        }
        self.require_valid()?;
        // The immutable authority cutoff was captured in absolute form. Shorten
        // it for this host crossing; never project a freshly sampled duration.
        // The separate, often shorter receiver I/O deadline stays on this
        // server in its held-session identity. Projecting that one-second local
        // timeout with a one-second skew budget would expire every response.
        Ok(StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: self
                .publication_cutoff
                .portable_wall_valid_until_ms
                .saturating_sub(crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS),
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn assert_context_for_test(&self, expected: &[u8]) {
        self.submission.assert_encoded_for_test(expected);
        assert_eq!(
            self.submission.observation().installation,
            submission::InstallationObservation::NoAcceptanceObserved
        );
    }
    pub(super) fn embedded(
        authority: &DirectIntroductionAuthority<'_, '_, '_>,
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<Self, StagingWindowConfirmationError> {
        Self::capture(
            authority,
            pg,
            bucket,
            key,
            attempt,
            proof,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn remote(
        authority: &RemoteDirectIntroductionAuthority<'_>,
        pg: &PgStore,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<Self, StagingWindowConfirmationError> {
        let (bucket, key, attempt, proof) = authority.subject();
        Self::capture(
            authority,
            pg,
            bucket,
            key,
            attempt,
            proof,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.observation().command_id
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.fence.require_valid_for(self.id().cluster_epoch())?;
        if Instant::now() >= self.publication_deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "direct introduction capture cutoff",
            });
        }
        Ok(())
    }

    fn capture(
        authority: &impl Authority,
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<Self, StagingWindowConfirmationError> {
        let invalid = |why: &str| StagingWindowConfirmationError::Window(why.into());
        authority.require_store(pg)?;
        let env = authority.environment()?;
        if env.topology.object_pg_for(bucket, key) != env.pg.get()
            || !proof.matches_exact_mutation_subject(
                env.epoch,
                bucket,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            )
        {
            return Err(invalid("direct introduction scoped subject/proof"));
        }
        env.fence.require_valid_for(env.epoch)?;
        pg.require_live_allocation_lifetime()?;
        let snapshot =
            pg.inspect_complete_settled_prefix(env.node.as_u32(), env.epoch, env.topology)?;
        let generation = pg
            .next_generation_id(bucket, key)
            .map_err(|e| invalid(&e.to_string()))?;
        let index = pg
            .max_metadata_command_log_index(env.epoch)?
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or_else(|| invalid("direct introduction command index exhausted"))?;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(env.epoch, env.pg, index),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                attempt.clone(),
                generation,
                crate::clock::current_time_millis(),
            )),
        );
        #[cfg(test)]
        hook("before capture");
        authority.require_store(pg)?;
        let candidate = capture::StagingCapture {
            topology: env.topology,
            placement: env.placement,
            effect_fence: env.fence,
        }
        .introduction_candidate(pg, env.node, command, proof)
        .map_err(|e| invalid(&e.to_string()))?;
        let cutoff = candidate
            .introduction_cutoff_ms()
            .map_err(|e| invalid(&e.to_string()))?;
        let fence = env
            .fence
            .intersect(AdmittedRouteEffectFence::bind_portable(
                env.epoch, cutoff, cutoff,
            ))?;
        let publication_cutoff = StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff,
        };
        let publication_deadline = env.deadline.min(publication_cutoff.local_deadline());
        let context = ContextualCommand::from_described(candidate);
        context
            .observe_pending_inventory_in_open_transaction(pg, env.topology)
            .map_err(|e| invalid(&e.to_string()))?;
        snapshot.finish()?;
        #[cfg(test)]
        hook("after snapshot");
        authority.require_store(pg)?;
        pg.require_live_allocation_lifetime()?;
        let captured = Self {
            submission: context.into_submission(),
            fence,
            publication_deadline,
            publication_cutoff,
        };
        captured.require_valid()?;
        Ok(captured)
    }
}
