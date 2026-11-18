// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current-route selection only. These read observations cannot authorize effects.

use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node::LocalNodeRuntime;

pub(crate) struct RemotePrimaryWindowWitness<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    peer: &'a local::LocalNodeStore,
}

impl CompleteFanoutRoute<'_> {
    pub(crate) fn window_confirmation_deadline(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<Instant, StoreError> {
        if self.is_historical() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "window confirmation requires current primary admission",
            });
        }
        self.primary_session_parameters(client, deadline)
            .map(|(_, _, deadline)| deadline)
    }

    pub(crate) fn window_confirmation_witness(
        &self,
    ) -> Result<Option<RemotePrimaryWindowWitness<'_>>, StoreError> {
        self.require_valid()?;
        if self.is_historical() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "window confirmation requires current primary admission",
            });
        }
        Ok(self
            .peers
            .iter()
            .copied()
            .filter(|node| node.node_id() != self.primary)
            .min_by_key(|node| node.node_id())
            .map(|peer| RemotePrimaryWindowWitness { route: self, peer }))
    }
}

impl RemotePrimaryWindowWitness<'_> {
    pub(crate) fn node_id(&self) -> NodeId {
        self.peer.node_id()
    }
    pub(crate) fn embedded_runtime(&self) -> Result<Option<&LocalNodeRuntime>, StoreError> {
        self.route.require_valid()?;
        self.peer.confirmation_embedded_runtime()
    }
    pub(crate) fn remote_prefix_until(
        &self,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.route.require_valid()?;
        if self.peer.confirmation_embedded_runtime()?.is_some() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "window witness requires its actual RPC endpoint",
            });
        }
        let state = self
            .peer
            .metadata_command_inspection_client()
            .complete_settled_witness_prefix_until(
                self.route.pg_id(),
                self.route.epoch(),
                deadline,
            )?;
        self.route.require_valid()?;
        Ok(state)
    }
}
