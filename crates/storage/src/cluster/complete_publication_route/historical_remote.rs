// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-only historical bridge. This is deliberately not a fanout route:
//! it cannot mint all-member completion or terminal retirement authority.

use super::*;
use crate::node_client::{
    CompletePrimarySessionBorrow, UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession,
};

pub(crate) struct HistoricalRemoteApplicationRoute<'a> {
    admission: &'a StorageClusterRouteAdmission,
    subject: HistoricalApplicationSubject,
    fence: AdmittedRouteEffectFence,
    cutoff: Instant,
}

pub(super) struct HistoricalRemoteApplicationTarget<'a> {
    route: &'a HistoricalRemoteApplicationRoute<'a>,
    _primary: CompletePrimarySessionBorrow<'a>,
    member: &'a local::LocalNodeStore,
    deadline: Instant,
}

impl StorageClusterRouteAdmission {
    pub(crate) fn historical_remote_application_route(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
        pg_id: PgId,
    ) -> Result<HistoricalRemoteApplicationRoute<'_>, StoreError> {
        let subject = self.historical_application_subject(runtime_map, pg_id)?;
        let fence = self.effect_fence();
        let deadline = fence
            .deadline()
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical remote application requires bounded admission",
            })?;
        let cutoff = crate::storage_rpc::StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
        }
        .local_deadline();
        let route = HistoricalRemoteApplicationRoute {
            admission: self,
            subject,
            fence,
            cutoff,
        };
        route.require_effect()?;
        Ok(route)
    }
}

impl HistoricalRemoteApplicationRoute<'_> {
    pub(crate) fn open_primary_session(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let session = client.open_complete_historical_primary(self, deadline)?;
        self.require_effect()?;
        Ok(session)
    }

    // Only scalar session parameters escape; the route remains necessary to
    // authorize a target. The client keeps its raw epoch-selecting opener private.
    pub(crate) fn primary_session_parameters(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<(PgId, ClusterEpoch, Instant), StoreError> {
        self.require_effect()?;
        let primary = self
            .admission
            .cluster
            .local_map
            .node(self.subject.primary)
            .expect("validated historical primary");
        if client.node_id() != self.subject.primary || !primary.is_complete_fanout_client(client) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical application requires the exact admitted primary client",
            });
        }
        Ok((
            self.subject.pg_id,
            self.subject.pending.cluster_epoch(),
            deadline.min(self.cutoff),
        ))
    }

    fn require_effect(&self) -> Result<(), StoreError> {
        self.fence
            .require_valid_for(self.admission.cluster_epoch())?;
        self.admission.require_valid_now_raw()?;
        if Instant::now() >= self.cutoff {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "historical remote application captured cutoff",
            });
        }
        Ok(())
    }

    pub(crate) fn embedded_replica_target<'a>(
        &'a self,
        client: &UnixStorageNodeClient,
        session: &'a UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
    ) -> Result<EmbeddedApplicationTarget<'a>, StoreError> {
        let (_, _, deadline) = self.primary_session_parameters(client, deadline)?;
        let map = &self.admission.cluster.local_map;
        if node == self.subject.primary || !self.subject.members.contains(&node) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical embedded replica requires Active and Peering membership",
            });
        }
        let member = map
            .node(node)
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical embedded replica is absent from the admitted map",
            })?;
        member.embedded_publication_runtime()?;
        let primary = session.borrow_complete_primary(
            client,
            self.subject.pg_id,
            self.subject.pending.cluster_epoch(),
            deadline,
        )?;
        self.require_effect()?;
        Ok(EmbeddedApplicationTarget::from_historical_remote(
            HistoricalRemoteApplicationTarget {
                route: self,
                _primary: primary,
                member,
                deadline,
            },
        ))
    }
}

impl HistoricalRemoteApplicationTarget<'_> {
    pub(super) fn require_command(
        &self,
        id: MetadataCommandId,
        crc: u64,
        sha: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_effect()?;
        let pending = self.route.subject.pending;
        if id.pg_id() != self.pg_id()
            || id.cluster_epoch() != self.epoch()
            || id.log_index().get() != pending.log_index()
            || crc != pending.command_checksum()
            || sha != pending.command_sha256()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical embedded application differs from admitted recovery report",
            });
        }
        Ok(())
    }
    pub(super) fn require_effect(&self) -> Result<(), StoreError> {
        self.route.require_effect()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "historical remote-primary embedded application",
            });
        }
        Ok(())
    }
    pub(super) fn pg_id(&self) -> PgId {
        self.route.subject.pg_id
    }
    pub(super) fn epoch(&self) -> ClusterEpoch {
        self.route.subject.pending.cluster_epoch()
    }
    pub(super) fn node_id(&self) -> NodeId {
        self.member.node_id()
    }
    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(super) fn runtime(&self) -> Result<&LocalNodeRuntime, StoreError> {
        self.require_effect()?;
        self.member.embedded_publication_runtime()
    }
}
