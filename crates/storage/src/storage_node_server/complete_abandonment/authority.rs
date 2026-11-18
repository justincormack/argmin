// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Only the admitted abandonment owner can mint this capability. Neither a
//! publication permit nor decoded wire bytes can substitute for it.

use super::*;

pub(in crate::node_runtime) struct CompleteAbandonmentAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _route: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    request: &'a CompleteAbandonmentRequest<'a>,
    binding: StorageNodeMetadataCommandLockBinding,
    fence: StorageNodeRouteFence,
    deadline: Instant,
}

impl<'a> CompleteAbandonmentAuthority<'a> {
    pub(super) fn admit(
        handler: &'a StorageNodeConnectionHandler,
        session: &'a StorageNodeSession,
        route: &'a StorageNodeRouteAdmissionPermit,
        store: &'a PgStore,
        request: &'a CompleteAbandonmentRequest<'a>,
        captured: (StorageNodeMetadataCommandLockBinding, StorageNodeRouteFence),
        deadline: Instant,
    ) -> Result<Self, StorageRpcErrorResponse> {
        let (binding, _) = handler.validate_abandonment_section(session, route, request)?;
        if binding != captured.0 {
            return Err(store_error_response(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete abandonment captured primary binding",
                },
            ));
        }
        let authority = Self {
            handler,
            session,
            _route: route,
            store,
            request,
            binding,
            fence: captured.1,
            deadline: request.bound_deadline(deadline),
        };
        authority.validate(store).map_err(store_error_response)?;
        Ok(authority)
    }

    fn validate(&self, store: &PgStore) -> Result<(), StoreError> {
        let id = self.request.id();
        if !std::ptr::eq(store, self.store)
            || store.pg_id() != id.pg_id().get()
            || self.request.topology() != self.handler.node.pg_topology()
            || self.request.node_id() != self.handler.config.node_id
            || !self
                .session
                .holds_metadata_command_pg_lock_with_binding(self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete abandonment admitted command/store/primary",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        if Instant::now() >= self.request.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete abandonment",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        pg: &PgStore,
        request: &crate::node_runtime::pg_store::CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        if request.node_id() != self.request.node_id()
            || request.id() != self.request.id()
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != self.request.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete abandonment exact context",
            });
        }
        self.validate(pg)?;
        if Instant::now() >= request.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete abandonment request cutoff",
            });
        }
        Ok(())
    }
}
