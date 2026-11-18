// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Only the admitted maintenance owner can mint this authority. Borrowing the
//! handler, session, serialization guard and exact locked store keeps admission
//! and serialization alive; neither a request DTO nor a topology is authority.

use super::*;
use crate::node_runtime::pg_store::PgStore;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum Mutation {
    Record,
    Compact,
}

pub(in crate::node_runtime) struct CompleteLogMutationAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    store: &'a PgStore,
    pg_id: PgId,
    mutation: Mutation,
    _session: &'a StorageNodeSession,
    _serialization: &'a Option<StorageNodeMetadataCommandGuard>,
}

impl<'a> CompleteLogMutationAuthority<'a> {
    pub(super) fn admit(
        handler: &'a StorageNodeConnectionHandler,
        session: &'a StorageNodeSession,
        serialization: &'a Option<StorageNodeMetadataCommandGuard>,
        store: &'a PgStore,
        request: &StorageRpcMetadataCommandStateRequest,
        mutation: Mutation,
    ) -> Result<Self, StorageRpcErrorResponse> {
        handler.validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)?;
        handler.validate_primary_pg(request.pg_id, "complete metadata command maintenance")?;
        handler.validate_metadata_mutation_route_not_expired()?;
        let serialized = match serialization {
            Some(guard) => !guard.released && guard.pg_id == request.pg_id,
            None => session.holds_metadata_command_pg_lock(request.pg_id),
        };
        if !serialized || store.pg_id() != request.pg_id.get() {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "complete maintenance lacks exact PG serialization".into(),
            });
        }
        Ok(Self {
            handler,
            store,
            pg_id: request.pg_id,
            mutation,
            _session: session,
            _serialization: serialization,
        })
    }

    pub(in crate::node_runtime) fn require_request(
        &self,
        store: &PgStore,
        node_id: u32,
        pg_id: PgId,
        epoch: ClusterEpoch,
        topology: &crate::PgTopology,
        mutation: Mutation,
    ) -> Result<(), StoreError> {
        if node_id != self.handler.config.node_id.as_u32()
            || pg_id != self.pg_id
            || topology != self.handler.node.pg_topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete maintenance admitted request",
            });
        }
        self.require_effect(store, epoch, mutation)
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        store: &PgStore,
        epoch: ClusterEpoch,
        mutation: Mutation,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(store, self.store)
            || epoch != self.handler.config.cluster_epoch
            || mutation != self.mutation
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete maintenance admitted store/epoch/operation",
            });
        }
        // Use this handler's captured monotonic lease, never a renewed raw map.
        if !self.handler.current_route_map_lease_is_valid() {
            return Err(StoreError::RouteMapExpired {
                cluster_epoch: epoch,
                valid_until_ms: self.handler.config.route_map_valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            });
        }
        Ok(())
    }
}
