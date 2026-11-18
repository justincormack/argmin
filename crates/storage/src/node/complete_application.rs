// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed embedded replica application, borrowing the real primary section or
//! remote session and its captured route authority through the target.

use super::*;
use crate::cluster::EmbeddedApplicationTarget;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::{CompleteApplicationError, CompleteApplicationRequest};
use std::time::Instant;

#[cfg(test)]
#[path = "complete_application/api_tests.rs"]
mod api_tests;

pub(in crate::node_runtime) struct EmbeddedApplicationAuthority<'a> {
    target: &'a EmbeddedApplicationTarget<'a>,
    store: &'a PgStore,
    request: &'a CompleteApplicationRequest<'a>,
    deadline: Instant,
}

impl<'a> EmbeddedApplicationAuthority<'a> {
    pub(in crate::node_runtime) fn request(&self) -> &CompleteApplicationRequest<'a> {
        self.request
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        store: &PgStore,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.target.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )?;
        if !std::ptr::eq(store, self.store)
            || store.pg_id() != self.target.pg_id().get()
            || request.node_id() != self.target.node_id()
            || request.id().pg_id() != self.target.pg_id()
            || request.id().cluster_epoch() != self.target.epoch()
            || request.id() != self.request.id()
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != self.target.runtime()?.pg_topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded complete application admitted command/store/member",
            });
        }
        if Instant::now() >= self.deadline.min(request.deadline()) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "embedded complete application",
            });
        }
        Ok(())
    }
}

impl EmbeddedApplicationTarget<'_> {
    pub(in crate::node_runtime) fn apply_complete_command(
        &self,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.with_authority(request, |pg, authority| {
            pg.apply_embedded_complete_metadata_command(authority)
        })
    }

    // No production callback, raw runtime, store or independent command input.
    fn with_authority<T>(
        &self,
        request: &CompleteApplicationRequest<'_>,
        action: impl FnOnce(
            &PgStore,
            &EmbeddedApplicationAuthority<'_>,
        ) -> Result<T, CompleteApplicationError>,
    ) -> Result<T, CompleteApplicationError> {
        let deadline = request.deadline().min(self.deadline());
        self.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )?;
        let runtime = self.runtime()?;
        let pg = runtime.node.get_pg_until(self.pg_id().get(), deadline)?;
        let authority = EmbeddedApplicationAuthority {
            target: self,
            store: &pg,
            request,
            deadline,
        };
        authority.require_effect(&pg, request)?;
        action(&pg, &authority)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_with_application_authority<T>(
        &self,
        request: &CompleteApplicationRequest<'_>,
        action: impl FnOnce(
            &PgStore,
            &EmbeddedApplicationAuthority<'_>,
        ) -> Result<T, CompleteApplicationError>,
    ) -> Result<T, CompleteApplicationError> {
        self.with_authority(request, action)
    }
}
