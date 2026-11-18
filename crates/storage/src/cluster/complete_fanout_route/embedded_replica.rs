// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed remote-primary to embedded-replica bridge. No supplied runtime, route
//! fence, arbitrary member list or callback can substitute for the captured map.

use super::*;
use crate::node::LocalNodeRuntime;
use crate::node_client::{CompletePrimarySessionBorrow, UnixStorageNodeMetadataCommandSession};

pub(crate) struct RemotePrimaryEmbeddedTarget<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    _primary: CompletePrimarySessionBorrow<'a>,
    member: &'a local::LocalNodeStore,
    deadline: Instant,
}

impl CompleteFanoutRoute<'_> {
    pub(crate) fn embedded_replica_target<'a>(
        &'a self,
        client: &UnixStorageNodeClient,
        session: &'a UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
    ) -> Result<EmbeddedApplicationTarget<'a>, StoreError> {
        self.embedded_replica_target_inner(client, session, node, deadline, None)
    }

    pub(crate) fn embedded_replica_target_in_session<'a>(
        &'a self,
        client: &UnixStorageNodeClient,
        session: &'a UnixStorageNodeMetadataCommandSession,
        identity: &crate::node_client::MetadataCommandSessionIdentity,
        node: NodeId,
        deadline: Instant,
    ) -> Result<EmbeddedApplicationTarget<'a>, StoreError> {
        self.embedded_replica_target_inner(client, session, node, deadline, Some(identity))
    }

    fn embedded_replica_target_inner<'a>(
        &'a self,
        client: &UnixStorageNodeClient,
        session: &'a UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
        identity: Option<&crate::node_client::MetadataCommandSessionIdentity>,
    ) -> Result<EmbeddedApplicationTarget<'a>, StoreError> {
        self.require_client(client)?;
        if client.node_id() != self.primary || node == self.primary {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete embedded replica requires the admitted remote primary",
            });
        }
        let member = self
            .peers
            .iter()
            .copied()
            .find(|peer| peer.node_id() == node)
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete embedded replica is not in the admitted cohort",
            })?;
        member.embedded_publication_runtime()?;
        let deadline = self
            .cutoff
            .map_or(deadline, |(cutoff, _)| deadline.min(cutoff));
        let primary =
            session.borrow_complete_primary(client, self.pg_id, self.epoch(), deadline)?;
        if let Some(identity) = identity {
            primary.require_identity(identity)?;
        }
        self.require_valid()?;
        Ok(EmbeddedApplicationTarget::from_remote_primary(
            RemotePrimaryEmbeddedTarget {
                route: self,
                _primary: primary,
                member,
                deadline,
            },
        ))
    }
}

impl RemotePrimaryEmbeddedTarget<'_> {
    pub(in crate::cluster) fn require_command(
        &self,
        id: MetadataCommandId,
        crc: u64,
        sha: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_effect()?;
        self.route.require_command(id, crc, sha)
    }

    pub(in crate::cluster) fn require_effect(&self) -> Result<(), StoreError> {
        self.route.require_valid()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete remote-primary embedded application",
            });
        }
        Ok(())
    }
    pub(in crate::cluster) fn pg_id(&self) -> PgId {
        self.route.pg_id()
    }
    pub(in crate::cluster) fn epoch(&self) -> ClusterEpoch {
        self.route.epoch()
    }
    pub(in crate::cluster) fn node_id(&self) -> NodeId {
        self.member.node_id()
    }
    pub(in crate::cluster) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::cluster) fn runtime(&self) -> Result<&LocalNodeRuntime, StoreError> {
        self.require_effect()?;
        self.member.embedded_publication_runtime()
    }
}
