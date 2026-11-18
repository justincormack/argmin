// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Immutable, admission-owned current or complete historical acting set. No supplied node list, raw
//! route DTO or later route renewal can substitute for this captured cohort.

use super::complete_publication_route::HistoricalFanoutSubject;
use super::*;
use crate::node_client::UnixStorageNodeClient;
use crate::storage_rpc::StorageRpcOperationDeadline;
use crate::PgTopology;

mod embedded_replica;
mod terminal_work;
mod window_confirmation;
pub(super) use embedded_replica::RemotePrimaryEmbeddedTarget;
pub(crate) use terminal_work::DirectPutTerminalWork;

enum FanoutSubject<'a> {
    Current,
    Historical(HistoricalFanoutSubject<'a>),
}

pub(crate) struct CompleteFanoutRoute<'a> {
    admission: &'a StorageClusterRouteAdmission,
    pg_id: PgId,
    primary: NodeId,
    peers: Vec<&'a local::LocalNodeStore>,
    fence: AdmittedRouteEffectFence,
    cutoff: Option<(Instant, StorageRpcOperationDeadline)>,
    subject: FanoutSubject<'a>,
}

impl StorageClusterRouteAdmission {
    pub(crate) fn complete_fanout_route(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
    ) -> Result<CompleteFanoutRoute<'_>, StoreError> {
        self.require_valid_now_raw()?;
        let map = &self.cluster.local_map;
        let primary = map.metadata_pg_primary_node(epoch, pg_id)?.node_id();
        let peers = map.metadata_pg_acting_nodes(epoch, pg_id)?;
        self.bind_fanout_route(pg_id, primary, peers, FanoutSubject::Current)
    }

    pub(crate) fn complete_historical_fanout_route(
        &self,
        map: &ClusterRuntimeMapSnapshot,
        pg_id: PgId,
    ) -> Result<CompleteFanoutRoute<'_>, StoreError> {
        let subject = self.historical_fanout_subject(map, pg_id)?;
        let peers = subject
            .nodes()
            .map(|node| {
                let member = self.cluster.local_map.node(node).ok_or(
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation: "historical fanout member is absent from admitted map",
                    },
                )?;
                Ok(member)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        self.bind_fanout_route(
            pg_id,
            subject.primary(),
            peers,
            FanoutSubject::Historical(subject),
        )
    }

    fn bind_fanout_route<'a>(
        &'a self,
        pg_id: PgId,
        primary: NodeId,
        peers: Vec<&'a local::LocalNodeStore>,
        subject: FanoutSubject<'a>,
    ) -> Result<CompleteFanoutRoute<'a>, StoreError> {
        let unique: std::collections::BTreeSet<_> =
            peers.iter().map(|peer| peer.node_id()).collect();
        if peers.is_empty() || unique.len() != peers.len() || !unique.contains(&primary) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout requires a nonempty unique acting set with its primary",
            });
        }
        let fence = self.effect_fence();
        let cutoff = fence.deadline().map(|deadline| {
            let portable = StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
            };
            (portable.local_deadline(), portable)
        });
        let route = CompleteFanoutRoute {
            admission: self,
            pg_id,
            primary,
            peers,
            fence,
            cutoff,
            subject,
        };
        route.require_valid()?;
        Ok(route)
    }
}

/// Shared EC scratch/encoding only; not a physical effect grant.
pub(crate) struct StagingPayloadEncoder<'a>(&'a LocalClusterMap);

impl StagingPayloadEncoder<'_> {
    pub(crate) fn encode<E: From<StoreError>>(
        &self,
        hash: &[u8; 16],
        generation: GenerationId,
        bytes: &[u8],
        ec: EcShape,
        action: impl FnOnce(&[(ShardKey, &[u8])]) -> Result<Vec<(ShardKey, WriteAck)>, E>,
    ) -> Result<(), E> {
        self.0
            .write_erasure_coded_segment_shards_with(hash, generation, bytes, ec, action)?;
        Ok(())
    }
}

impl<'a> CompleteFanoutRoute<'a> {
    /// Retain the existing budget, not only its cutoff. Consuming encrypted
    /// custody carries it through every preparation/installation retry.
    pub(crate) fn begin_direct_put_terminal_work(
        &self,
        original: Instant,
        portable: StorageRpcOperationDeadline,
    ) -> DirectPutTerminalWork {
        DirectPutTerminalWork::new(self, original, portable)
    }

    pub(crate) fn staging_payload_encoder(&self) -> Result<StagingPayloadEncoder<'a>, StoreError> {
        self.staging_file_placement()?;
        Ok(StagingPayloadEncoder(&self.admission.cluster.local_map))
    }
    pub(super) fn require_current_admission(
        &self,
        admission: &StorageClusterRouteAdmission,
        pg: PgId,
        epoch: ClusterEpoch,
        primary: NodeId,
    ) -> Result<(), StoreError> {
        self.require_valid()?;
        if !matches!(self.subject, FanoutSubject::Current)
            || !std::ptr::eq(self.admission, admission)
            || self.pg_id != pg
            || self.epoch() != epoch
            || self.primary != primary
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "request introduction requires its original admitted primary route",
            });
        }
        Ok(())
    }

    /// Retain the exact immutable admission for a consuming custody handoff.
    /// Never re-read the runtime map or recapture a deadline.
    pub(crate) fn retain_current(&self) -> Result<Self, StoreError> {
        self.require_valid()?;
        if !matches!(self.subject, FanoutSubject::Current) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction requires current fanout authority",
            });
        }
        Ok(Self {
            admission: self.admission,
            pg_id: self.pg_id,
            primary: self.primary,
            peers: self.peers.clone(),
            fence: self.fence,
            cutoff: self.cutoff,
            subject: FanoutSubject::Current,
        })
    }

    pub(crate) fn require_staging_ack_client(
        &self,
        pg: PgId,
        primary: NodeId,
        client: &UnixStorageNodeClient,
    ) -> Result<(), StoreError> {
        self.staging_file_placement()?;
        let map = &self.admission.cluster.local_map;
        let route = map
            .pg_route(pg)
            .ok_or(StoreError::PgNotFound { pg_id: pg.get() })?;
        let member = map
            .node(primary)
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement captured remote primary",
            })?;
        if route.cluster_epoch() != self.epoch()
            || route.state() != PgState::Active
            || route.primary_node_id() != primary
            || !member.is_complete_fanout_client(client)
            || member.confirmation_embedded_runtime()?.is_some()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement captured remote primary",
            });
        }
        Ok(())
    }

    pub(crate) fn require_staging_file_client(
        &self,
        location: &ShardLocation,
        client: &UnixStorageNodeClient,
    ) -> Result<(), StoreError> {
        self.staging_file_placement()?;
        let map = &self.admission.cluster.local_map;
        let pg = PgId::new(location.data_pg_id().get());
        let route = map
            .pg_route(pg)
            .ok_or(StoreError::PgNotFound { pg_id: pg.get() })?;
        let member =
            map.node(location.node_id())
                .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "staging file requires the exact admitted remote participant",
                })?;
        if location.cluster_epoch() != self.epoch()
            || route.cluster_epoch() != self.epoch()
            || route.state() != PgState::Active
            || !route.acting_set().contains(&location.node_id())
            || !member.is_complete_fanout_client(client)
            || member.confirmation_embedded_runtime()?.is_some()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging file requires the exact admitted remote participant",
            });
        }
        Ok(())
    }

    pub(crate) fn embedded_staging_ack_runtime(
        &self,
        pg: PgId,
        primary: NodeId,
        epoch: ClusterEpoch,
    ) -> Result<&crate::node::LocalNodeRuntime, StoreError> {
        self.staging_file_placement()?;
        let map = &self.admission.cluster.local_map;
        let route = map
            .pg_route(pg)
            .ok_or(StoreError::PgNotFound { pg_id: pg.get() })?;
        if epoch != self.epoch()
            || route.cluster_epoch() != epoch
            || route.state() != PgState::Active
            || route.primary_node_id() != primary
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement captured Active primary",
            });
        }
        let runtime = map
            .node(primary)
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement captured node",
            })?
            .embedded_publication_runtime()?;
        if runtime.pg_topology() != self.topology() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement topology",
            });
        }
        Ok(runtime)
    }

    /// Captured placement data, not a physical grant. The consuming staging
    /// holder validates its exact descriptor and retains the effect fence.
    pub(crate) fn staging_file_placement(
        &self,
    ) -> Result<
        (
            crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
            AdmittedRouteEffectFence,
        ),
        StoreError,
    > {
        self.require_valid()?;
        if self.is_historical() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging files require current admission",
            });
        }
        Ok((
            crate::node_runtime::payload_placement::PayloadPlacementInput::from_local_map(
                &self.admission.cluster.local_map,
            ),
            self.fence,
        ))
    }

    pub(crate) fn embedded_staging_file_runtime(
        &self,
        location: &ShardLocation,
    ) -> Result<&crate::node::LocalNodeRuntime, StoreError> {
        self.staging_file_placement()?;
        let map = &self.admission.cluster.local_map;
        // Resolve only the captured map, never the runtime handle's replacement.
        let pg = PgId::new(location.data_pg_id().get());
        let route = map
            .pg_route(pg)
            .ok_or(StoreError::PgNotFound { pg_id: pg.get() })?;
        if location.cluster_epoch() != self.epoch()
            || route.cluster_epoch() != self.epoch()
            || route.state() != PgState::Active
            || !route.acting_set().contains(&location.node_id())
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging file captured Active route",
            });
        }
        let member =
            map.node(location.node_id())
                .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "staging file captured node",
                })?;
        let runtime = member.embedded_publication_runtime()?;
        if runtime.pg_topology() != self.topology() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging file runtime topology",
            });
        }
        Ok(runtime)
    }

    pub(crate) fn require_embedded_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        self.require_valid()?;
        match &self.subject {
            FanoutSubject::Current => section.require_current_admission(
                self.admission,
                self.pg_id,
                self.epoch(),
                self.primary,
            ),
            FanoutSubject::Historical(subject) => subject.require_section(section),
        }
    }

    pub(crate) fn require_command(
        &self,
        id: MetadataCommandId,
        crc: u64,
        sha: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_valid()?;
        match &self.subject {
            FanoutSubject::Historical(subject) => subject.require_command(id, crc, sha),
            FanoutSubject::Current
                if id.pg_id() == self.pg_id && id.cluster_epoch() == self.epoch() =>
            {
                Ok(())
            }
            FanoutSubject::Current => Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout command differs from admitted route",
            }),
        }
    }

    pub(crate) fn is_historical(&self) -> bool {
        matches!(self.subject, FanoutSubject::Historical(_))
    }

    /// Only the complete terminal owner can produce this borrowed operation.
    /// Resolve the proof's historical bucket PG from the SAME admitted map;
    /// neither a caller-provided client nor the current object PG may replace it.
    pub(crate) fn release_terminal_reservation(
        &self,
        release: &crate::node_runtime::CompleteTerminalReservationRelease<'_>,
    ) -> Result<(), BucketSnapshotLoadError> {
        release.require_route(self)?;
        let cluster = &self.admission.cluster;
        let pg = PgId::new(cluster.bucket_metadata_pg_id(release.bucket()));
        let node = cluster
            .local_map
            .metadata_pg_primary_node_for_retained_cleanup(release.epoch(), pg)?;
        let route = node
            .retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route_until(
                cluster.validated_bucket_metadata_pg(pg),
                release.bucket(),
                release.deadline(),
            )?;
        release.send(route.as_ref())
    }

    pub(crate) fn require_valid(&self) -> Result<(), StoreError> {
        self.fence
            .require_valid_for(self.admission.cluster_epoch())?;
        self.admission.require_valid_now_raw()?;
        if self
            .cutoff
            .is_some_and(|(cutoff, _)| Instant::now() >= cutoff)
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete fanout captured route cutoff",
            });
        }
        Ok(())
    }

    pub(crate) fn pg_id(&self) -> PgId {
        self.pg_id
    }
    pub(crate) fn epoch(&self) -> ClusterEpoch {
        match &self.subject {
            FanoutSubject::Current => self.fence.cluster_epoch(),
            FanoutSubject::Historical(subject) => subject.epoch(),
        }
    }
    pub(crate) fn primary(&self) -> NodeId {
        self.primary
    }
    pub(crate) fn topology(&self) -> &PgTopology {
        self.admission.cluster.local_map.pg_topology()
    }
    pub(crate) fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.iter().map(|peer| peer.node_id())
    }

    pub(crate) fn require_client(&self, client: &UnixStorageNodeClient) -> Result<(), StoreError> {
        self.require_valid()?;
        if !self
            .peers
            .iter()
            .any(|peer| peer.is_complete_fanout_client(client))
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout requires the exact admitted peer client",
            });
        }
        Ok(())
    }

    /// The client owner keeps raw epoch selection private. Only this admitted
    /// cohort can select its primary and historical command epoch; the sender
    /// additionally intersects its original work cutoff before opening.
    pub(crate) fn primary_session_parameters(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<(PgId, ClusterEpoch, Instant), StoreError> {
        self.require_client(client)?;
        if client.node_id() != self.primary {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout session requires its admitted primary",
            });
        }
        let deadline = self
            .cutoff
            .map_or(deadline, |(cutoff, _)| deadline.min(cutoff));
        if Instant::now() >= deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete fanout primary session",
            });
        }
        Ok((self.pg_id, self.epoch(), deadline))
    }

    /// Can only narrow the holder's already-captured authority. This projection
    /// carries no command, cohort, effect or completion capability of its own.
    pub(crate) fn intersect_deadline(
        &self,
        local: Instant,
        portable: StorageRpcOperationDeadline,
    ) -> (Instant, StorageRpcOperationDeadline) {
        self.cutoff.map_or((local, portable), |(bound, wire)| {
            (
                local.min(bound),
                StorageRpcOperationDeadline {
                    portable_wall_valid_until_ms: portable
                        .portable_wall_valid_until_ms
                        .min(wire.portable_wall_valid_until_ms),
                },
            )
        })
    }
}
