// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Mintable only by the admitted publication owner. Retains the captured route,
//! exact primary lock, command identity, locked store and immutable work cutoff.

use super::*;

pub(in crate::node_runtime) struct CompletePublicationAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _route: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    request: &'a CompletePublicationRequest<'a>,
    deadline: Instant,
    binding: StorageNodeMetadataCommandLockBinding,
    fence: StorageNodeRouteFence,
}

impl<'a> CompletePublicationAuthority<'a> {
    pub(super) fn admit(
        handler: &'a StorageNodeConnectionHandler,
        session: &'a StorageNodeSession,
        route: &'a StorageNodeRouteAdmissionPermit,
        store: &'a PgStore,
        request: &'a CompletePublicationRequest<'a>,
        deadline: Instant,
    ) -> Result<Self, StorageRpcErrorResponse> {
        handler.validate_active_admission(route, "complete publication start")?;
        let deadline = request.bound_deadline(deadline);
        let (binding, fence) = handler.complete_publication_route(request)?;
        let authority = Self {
            handler,
            session,
            _route: route,
            store,
            request,
            deadline,
            binding,
            fence,
        };
        authority
            .require_effect(store, request)
            .map_err(store_error_response)?;
        Ok(authority)
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        store: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        let id = self.request.id();
        if !std::ptr::eq(store, self.store)
            || store.pg_id() != id.pg_id().get()
            || request.node_id() != self.handler.config.node_id
            || request.id() != id
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != self.handler.node.pg_topology()
            || !self
                .session
                .holds_metadata_command_pg_lock_with_binding(self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete publication admitted command/store/primary",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        // The admitted cutoff may not be extended by a later same-command
        // request, and a request with a stricter wire cutoff must still shorten it.
        if Instant::now() >= request.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete publication start",
            });
        }
        Ok(())
    }
}
