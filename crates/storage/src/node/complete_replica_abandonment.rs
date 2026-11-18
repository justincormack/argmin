// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-side store binding for an owner-minted, primary-confirmed replica
//! abandonment permit. No arbitrary target, runtime or production callback.

use super::*;
use crate::node_runtime::pg_store::{
    CompletePublicationError, CompletePublicationRequest, ReplicaAbandonmentPermit,
};
use std::time::Instant;

#[cfg(test)]
#[path = "complete_replica_abandonment/api_tests.rs"]
mod api_tests;

pub(in crate::node_runtime) struct EmbeddedReplicaAbandonmentAuthority<'a> {
    permit: &'a ReplicaAbandonmentPermit<'a, 'a>,
    store: &'a PgStore,
    request: &'a CompletePublicationRequest<'a>,
    deadline: Instant,
}

impl<'a> EmbeddedReplicaAbandonmentAuthority<'a> {
    pub(in crate::node_runtime) fn request(&self) -> &CompletePublicationRequest<'a> {
        self.request
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.permit.require_request(request)?;
        let target = self.permit.target();
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != target.pg_id().get()
            || request.id() != self.request.id()
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != target.runtime()?.pg_topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded replica abandonment admitted store/context",
            });
        }
        if Instant::now() >= request.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "embedded replica abandonment",
            });
        }
        Ok(())
    }
}

impl ReplicaAbandonmentPermit<'_, '_> {
    pub(in crate::node_runtime) fn record_embedded(
        &self,
        request: &CompletePublicationRequest<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.require_request(request)?;
        let target = self.target();
        let deadline = request.bound_deadline(deadline.min(target.deadline()));
        let runtime = target.runtime()?;
        let pg = runtime.node.get_pg_until(target.pg_id().get(), deadline)?;
        let authority = EmbeddedReplicaAbandonmentAuthority {
            permit: self,
            store: &pg,
            request,
            deadline,
        };
        authority.require_effect(&pg, request)?;
        pg.record_complete_replica_abandonment(
            &authority,
            #[cfg(test)]
            hook,
        )
    }
}
