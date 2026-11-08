// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact pending retention private to complete-context inventory derivation.
//! Only owner-local unit tests can extract routes/windows. Current v13
//! commands cannot authenticate this context: activation must install these
//! requirements atomically with the mandatory authenticated command carrier.

use super::super::{CandidateBindingError, ContextError, ContextualCommand, Input};
use crate::metadata_command::MetadataCommandId;
use crate::node_runtime::pg_store::command_log::{
    PendingMetadataCommandRouteDependency, MAX_PENDING_METADATA_COMMAND_ROUTE_DEPENDENCIES,
};
use crate::types::ShardScavengerStagingProtection;
use crate::{PgTopology, StoreError};

/// Descriptive retention only: not readable payload, write/deletion authority,
/// custody or a re-installable sidecar. The slot's own PG route and the command's
/// already-selected payload references remain separately protected as today.
/// This adds the captured future window and context-specific route dependencies.
#[derive(Debug, PartialEq, Eq)]
pub(in super::super::super::super) struct ContextPendingRetention {
    command_id: MetadataCommandId,
    routes: Vec<PendingMetadataCommandRouteDependency>,
    future_window: Option<ShardScavengerStagingProtection>,
}

impl ContextPendingRetention {
    pub(super) fn route_dependency_count(&self) -> usize {
        self.routes.len()
    }

    pub(super) fn has_future_window(&self) -> bool {
        self.future_window.is_some()
    }
}

#[cfg(test)]
impl ContextPendingRetention {
    pub(in super::super::super::super) fn command_id(&self) -> MetadataCommandId {
        self.command_id
    }

    pub(in super::super::super::super) fn routes(
        &self,
    ) -> &[PendingMetadataCommandRouteDependency] {
        &self.routes
    }

    pub(in super::super::super::super) fn future_window(
        &self,
    ) -> Option<&ShardScavengerStagingProtection> {
        self.future_window.as_ref()
    }
}

pub(super) fn derive(
    context: &ContextualCommand,
    topology: &PgTopology,
) -> Result<ContextPendingRetention, ContextError> {
    if topology != &context.topology {
        return Err(CandidateBindingError::Topology.into());
    }
    if !topology.contains_pg(context.id().pg_id()) {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "pending-context-command-pg",
        }
        .into());
    }
    let (command, mut routes, future_window) = match &context.input {
        Input::Ordinary(command) => (command, Vec::new(), None),
        Input::Described(candidate) => {
            let bound = candidate.bind(topology)?;
            let protection = bound.pending_protection();
            (
                candidate.command(),
                protection.route_dependencies(),
                Some(protection.staging_reference()),
            )
        }
        Input::AbandonedPart(candidate) => (
            candidate.command(),
            candidate.bind(topology)?.pending_route_dependencies(),
            None,
        ),
    };
    // Use the same generic proof-route projection as current slot insertion;
    // captured exact PGs supplement it rather than replacing/renewing proofs.
    routes.extend(PendingMetadataCommandRouteDependency::for_bucket_write_reservations(command));
    routes.sort_unstable();
    routes.dedup();
    if routes.len() > MAX_PENDING_METADATA_COMMAND_ROUTE_DEPENDENCIES {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "pending-context-route-count",
        }
        .into());
    }
    for route in &routes {
        route.target.resolve(topology)?;
    }
    Ok(ContextPendingRetention {
        command_id: context.id(),
        routes,
        future_window,
    })
}
