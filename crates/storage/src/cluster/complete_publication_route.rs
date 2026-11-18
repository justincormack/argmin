// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admission-owned embedded primary serialization for candidate publication.
//! No raw lock, node, fence or validation callback can substitute for this scope.

use super::*;
use crate::control_plane::PendingMetadataCommandObservation;
use crate::node::LocalNodeRuntime;

mod application_target;
pub(crate) use application_target::EmbeddedApplicationTarget;
mod historical_remote;
pub(crate) use historical_remote::HistoricalRemoteApplicationRoute;
mod historical_fanout;
pub(super) use historical_fanout::HistoricalFanoutSubject;

struct HistoricalApplicationSubject {
    pg_id: PgId,
    primary: NodeId,
    pending: PendingMetadataCommandObservation,
    members: Vec<NodeId>,
    required_members: Vec<NodeId>,
}

enum PublicationSubject {
    Current,
    Historical {
        pending: PendingMetadataCommandObservation,
        application_members: Vec<NodeId>,
    },
}

pub(crate) struct EmbeddedPublicationRoute<'a> {
    admission: &'a StorageClusterRouteAdmission,
    pg_id: PgId,
    node_id: NodeId,
    runtime: &'a LocalNodeRuntime,
    subject: PublicationSubject,
    fence: AdmittedRouteEffectFence,
    route_cutoff: Option<Instant>,
    lock: Arc<local::MetadataCommandPgLock>,
}

pub(crate) struct EmbeddedPublicationSection<'a> {
    route: &'a EmbeddedPublicationRoute<'a>,
    _guard: local::MetadataCommandPgGuard<'a>,
    deadline: Instant,
    identity: Arc<()>,
}

/// Identity of one held section, not permission to reacquire or use its lock.
pub(crate) struct EmbeddedPublicationSectionIdentity(Arc<()>);

impl EmbeddedPublicationSectionIdentity {
    pub(crate) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        if !Arc::ptr_eq(&self.0, &section.identity) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction requires its original primary section",
            });
        }
        Ok(())
    }
}

/// Only the admitted section selects this peer. Callers cannot substitute a
/// convenient replica/client or detach its admission for the witness read.
pub(crate) struct WindowConfirmationWitness<'a> {
    section: &'a EmbeddedPublicationSection<'a>,
    peer: &'a local::LocalNodeStore,
}

impl WindowConfirmationWitness<'_> {
    pub(crate) fn node_id(&self) -> NodeId {
        self.peer.node_id()
    }

    pub(crate) fn embedded_runtime(&self) -> Result<Option<&LocalNodeRuntime>, StoreError> {
        self.section.require_effect()?;
        self.peer.confirmation_embedded_runtime()
    }

    pub(crate) fn remote_prefix_until(
        &self,
        deadline: Instant,
    ) -> Result<crate::metadata_command::MetadataCommandReplicaState, StoreError> {
        self.section.require_effect()?;
        if self.peer.confirmation_embedded_runtime()?.is_some() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote witness requires its captured RPC endpoint",
            });
        }
        let state = self
            .peer
            .metadata_command_inspection_client()
            .complete_settled_witness_prefix_until(
                self.section.pg_id(),
                self.section.epoch(),
                deadline.min(self.section.deadline()),
            )?;
        self.section.require_effect()?;
        Ok(state)
    }
}

impl StorageClusterRouteAdmission {
    pub(crate) fn embedded_publication_route(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        epoch: ClusterEpoch,
    ) -> Result<EmbeddedPublicationRoute<'_>, StoreError> {
        self.require_valid_now_raw()?;
        let primary = self
            .cluster
            .local_map
            .metadata_pg_primary_node(epoch, pg_id)?;
        if primary.node_id() != node_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded publication requires the admitted current primary",
            });
        }
        self.publication_route(
            pg_id,
            node_id,
            primary.embedded_publication_runtime()?,
            PublicationSubject::Current,
        )
    }

    pub(crate) fn embedded_historical_publication_route(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
        pg_id: PgId,
    ) -> Result<EmbeddedPublicationRoute<'_>, StoreError> {
        let subject = self.historical_application_subject(runtime_map, pg_id)?;
        let runtime = self
            .cluster
            .local_map
            .node(subject.primary)
            .expect("validated historical primary")
            .embedded_publication_runtime()?;
        self.publication_route(
            pg_id,
            subject.primary,
            runtime,
            PublicationSubject::Historical {
                pending: subject.pending,
                application_members: subject.members,
            },
        )
    }

    fn historical_application_subject(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
        pg_id: PgId,
    ) -> Result<HistoricalApplicationSubject, StoreError> {
        self.require_valid_now_raw()?;
        let mismatch = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "embedded historical publication requires the admitted recovery report",
        };
        let StorageClusterRouteAuthority::Dynamic(proof) = self.cluster.route_authority else {
            return Err(mismatch());
        };
        // LocalPgRoute deliberately does not carry recovery reports. Authenticate
        // this projection against the admitted map's complete content identity,
        // which includes the reporting node and exact pending command. Freshness
        // comes from the live admission, never from this supplied snapshot.
        if runtime_map.cluster_epoch() != self.cluster_epoch()
            || runtime_map.content_digest() != proof.content_digest
        {
            return Err(mismatch());
        }
        let current = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .ok_or_else(mismatch)?;
        let recovery = current
            .pending_metadata_command_recovery()
            .ok_or_else(mismatch)?;
        let pending = recovery.pending();
        if current.state() != PgState::Peering || pending.cluster_epoch() >= self.cluster_epoch() {
            return Err(mismatch());
        }
        let historical = runtime_map
            .reconstructed_pg_route_at_epoch(pg_id, pending.cluster_epoch())
            .map_err(|_| mismatch())?;
        let node_id = recovery.reporting_node_id();
        if historical.state() != PgState::Active
            || historical.primary_node_id() != node_id
            || !historical.acting_set().contains(&node_id)
        {
            return Err(mismatch());
        }
        // Match historical receiver placement admission: an old Active member
        // must also have current or retained Peering placement at/after that
        // epoch. Capture only these members from the authenticated map, not a
        // later local-map lookup or a caller-supplied cohort.
        let application_members: Vec<_> = historical
            .acting_set()
            .iter()
            .copied()
            .filter(|member| {
                runtime_map
                    .pg_routes()
                    .iter()
                    .chain(runtime_map.historical_pg_routes())
                    .any(|route| {
                        route.pg_id() == pg_id
                            && route.cluster_epoch() >= pending.cluster_epoch()
                            && route.state() == PgState::Peering
                            && route.acting_set().contains(member)
                    })
            })
            .collect();
        if !application_members.contains(&node_id) {
            return Err(mismatch());
        }
        self.cluster.local_map.node(node_id).ok_or_else(mismatch)?;
        // Historical effects must never be minted from static/unbounded authority.
        if self.effect_fence().deadline().is_none() {
            return Err(mismatch());
        }
        Ok(HistoricalApplicationSubject {
            pg_id,
            primary: node_id,
            pending,
            members: application_members,
            required_members: historical.acting_set().to_vec(),
        })
    }

    fn publication_route<'a>(
        &'a self,
        pg_id: PgId,
        node_id: NodeId,
        runtime: &'a LocalNodeRuntime,
        subject: PublicationSubject,
    ) -> Result<EmbeddedPublicationRoute<'a>, StoreError> {
        let epoch = self.cluster_epoch();
        let now = Instant::now();
        let lease = self.effective_lease();
        let (fence, route_cutoff) = match (
            lease.validity.valid_until_ms(),
            lease.local_valid_until_monotonic_ms,
        ) {
            (Some(wall), Some(monotonic)) => (
                AdmittedRouteEffectFence::bounded(epoch, wall, monotonic),
                Some(
                    now.checked_add(Duration::from_millis(
                        monotonic.saturating_sub(crate::clock::monotonic_time_millis()),
                    ))
                    .unwrap_or(now),
                ),
            ),
            (None, None) => {
                if matches!(subject, PublicationSubject::Historical { .. }) {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "embedded historical publication requires bounded admission",
                    });
                }
                (AdmittedRouteEffectFence::unbounded(epoch), None)
            }
            _ => unreachable!("route admission deadline representations must agree"),
        };
        Ok(EmbeddedPublicationRoute {
            admission: self,
            pg_id,
            node_id,
            runtime,
            subject,
            fence,
            route_cutoff,
            lock: self
                .cluster
                .local_map
                .runtime_state()
                .metadata_command_pg_lock(pg_id),
        })
    }
}

impl EmbeddedPublicationRoute<'_> {
    pub(crate) fn lock_until(
        &self,
        deadline: Instant,
    ) -> Result<EmbeddedPublicationSection<'_>, StoreError> {
        self.require_effect()?;
        let deadline = self
            .route_cutoff
            .map_or(deadline, |cutoff| deadline.min(cutoff));
        let guard =
            self.lock
                .lock_until(deadline)
                .ok_or(StoreError::OperationDeadlineExceeded {
                    context: "acquire embedded publication serialization",
                })?;
        self.require_effect()?;
        Ok(EmbeddedPublicationSection {
            route: self,
            _guard: guard,
            deadline,
            identity: Arc::new(()),
        })
    }

    fn require_effect(&self) -> Result<(), StoreError> {
        // Never resample a longer fence after same-epoch renewal. The admission
        // additionally prevents replacement publication until this borrow ends.
        self.fence
            .require_valid_for(self.admission.cluster_epoch())?;
        self.admission.require_valid_now_raw()?;
        Ok(())
    }
}

impl<'route> EmbeddedPublicationSection<'route> {
    pub(crate) fn introduction_identity(&self) -> EmbeddedPublicationSectionIdentity {
        EmbeddedPublicationSectionIdentity(Arc::clone(&self.identity))
    }

    pub(crate) fn introduction_cohort(&self) -> Result<CompleteFanoutRoute<'route>, StoreError> {
        self.staging_placement()?;
        let cohort = self
            .route
            .admission
            .complete_fanout_route(self.pg_id(), self.epoch())?;
        cohort.require_embedded_section(self)?;
        Ok(cohort)
    }

    pub(crate) fn staging_capture_fence(&self) -> Result<AdmittedRouteEffectFence, StoreError> {
        // Reject historical sections before minting fresh introduction authority.
        self.staging_placement()?;
        let cutoff = crate::storage_rpc::StorageRpcOperationDeadline::from_instant(self.deadline);
        self.route
            .fence
            .intersect(AdmittedRouteEffectFence::bind_portable(
                self.epoch(),
                cutoff.portable_wall_valid_until_ms,
                cutoff.portable_wall_valid_until_ms,
            ))
    }

    pub(crate) fn staging_placement(&self) -> Result<PayloadPlacementInput<'_>, StoreError> {
        self.require_current_admission(
            self.route.admission,
            self.pg_id(),
            self.route.admission.cluster_epoch(),
            self.node_id(),
        )?;
        Ok(PayloadPlacementInput::from_local_map(
            &self.route.admission.cluster.local_map,
        ))
    }
    /// Confirmation is CURRENT-primary work, never historical recovery. Select
    /// the same designated witness as publication; a missing witness is
    /// not permission to silently fall back to singleton semantics.
    pub(crate) fn confirmation_witness(
        &self,
    ) -> Result<Option<WindowConfirmationWitness<'_>>, StoreError> {
        self.require_current_admission(
            self.route.admission,
            self.pg_id(),
            self.route.admission.cluster_epoch(),
            self.node_id(),
        )?;
        let map = &self.route.admission.cluster.local_map;
        let witness = map
            .metadata_pg_acting_nodes(self.epoch(), self.pg_id())?
            .into_iter()
            .filter(|node| node.node_id() != self.node_id())
            .min_by_key(|node| node.node_id());
        Ok(witness.map(|peer| WindowConfirmationWitness {
            section: self,
            peer,
        }))
    }

    fn application_member(&self, node_id: NodeId) -> Result<&local::LocalNodeStore, StoreError> {
        self.require_effect()?;
        let map = &self.route.admission.cluster.local_map;
        let mismatch = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "embedded application target is not in the admitted acting set",
        };
        match &self.route.subject {
            PublicationSubject::Current => map
                .metadata_pg_acting_nodes(self.epoch(), self.pg_id())?
                .into_iter()
                .find(|member| member.node_id() == node_id)
                .ok_or_else(mismatch),
            PublicationSubject::Historical {
                application_members,
                ..
            } => {
                if !application_members.contains(&node_id) {
                    return Err(mismatch());
                }
                map.node(node_id).ok_or_else(mismatch)
            }
        }
    }

    pub(super) fn require_current_admission(
        &self,
        admission: &StorageClusterRouteAdmission,
        pg_id: PgId,
        epoch: ClusterEpoch,
        primary: NodeId,
    ) -> Result<(), StoreError> {
        self.require_effect()?;
        if !matches!(self.route.subject, PublicationSubject::Current)
            || !std::ptr::eq(self.route.admission, admission)
            || self.pg_id() != pg_id
            || self.epoch() != epoch
            || self.node_id() != primary
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded fanout requires its original admitted primary section",
            });
        }
        Ok(())
    }

    pub(crate) fn require_effect(&self) -> Result<(), StoreError> {
        self.route.require_effect()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "embedded publication serialization deadline",
            });
        }
        Ok(())
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn pg_id(&self) -> PgId {
        self.route.pg_id
    }
    pub(crate) fn node_id(&self) -> NodeId {
        self.route.node_id
    }
    pub(crate) fn epoch(&self) -> ClusterEpoch {
        match &self.route.subject {
            PublicationSubject::Current => self.route.admission.cluster_epoch(),
            PublicationSubject::Historical { pending, .. } => pending.cluster_epoch(),
        }
    }

    pub(crate) fn require_command(
        &self,
        id: MetadataCommandId,
        checksum_crc64: u64,
        command_sha256: [u8; 32],
    ) -> Result<(), StoreError> {
        if id.pg_id() != self.pg_id()
            || id.cluster_epoch() != self.epoch()
            || matches!(&self.route.subject, PublicationSubject::Historical { pending, .. }
                if id.log_index().get() != pending.log_index()
                    || checksum_crc64 != pending.command_checksum()
                    || command_sha256 != pending.command_sha256())
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded publication command differs from admitted subject",
            });
        }
        Ok(())
    }

    pub(crate) fn runtime(&self) -> Result<&LocalNodeRuntime, StoreError> {
        self.require_effect()?;
        Ok(self.route.runtime)
    }
}
