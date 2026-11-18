// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Received description, never independent mutation authority. Only the admitted
//! server owner combines this with its exact-store current-primary capability.
use super::*;
use crate::node_runtime::pg_store::{PgStore, StagingWindowConfirmationError as Error};
use crate::node_runtime::server::RemoteStreamAllocationAuthority;
use crate::storage_rpc::{
    complete_settled_prefix::Request as Route, complete_stream_allocation as wire,
    StorageRpcPayloadError,
};
use crate::types::AdmittedRouteEffectFence;
use std::time::Instant;

#[path = "payload_staging_remote_append_preparation.rs"]
mod append;
pub(in crate::node_runtime) use append::RemoteStreamAppendQuery;

pub(in crate::node_runtime) struct RemoteStreamAllocationQuery {
    route: Route,
    description: StagingWindowInspectionDescription,
    prefix: crate::metadata_command::MetadataCommandReplicaState,
    preparation: crate::storage_rpc::StorageRpcStreamSegmentAppendPrepareRequest,
    topology: PgTopology,
    deadline: Instant,
    fence: AdmittedRouteEffectFence,
    digest: [u8; 32],
}

impl RemoteStreamAllocationQuery {
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_expire_at_update<T>(
        &self,
        action: impl FnOnce() -> T,
    ) -> T {
        let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let control = clock.control();
        let cutoff = self.route.deadline.portable_wall_valid_until_ms;
        preparation::with_before_effect(move || control.set(cutoff + 1), action)
    }
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let request = wire::decode_request(bytes)?;
        let deadline = request.window.route.deadline.local_deadline();
        let invalid =
            |_| StorageRpcPayloadError::InvalidResponseEnvelope("stream allocation descriptor");
        let description = StagingWindowInspectionDescription::decode(request.window.descriptor)
            .map_err(invalid)?;
        let route = request.window.route;
        if route.topology != crate::storage_rpc::complete_settled_prefix::topology_digest(topology)
            || request.prefix.applied_log_index == 0
        {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "stream allocation topology/prefix",
            ));
        }
        let (deadline, fence) = description
            .request(topology)
            .map_err(invalid)?
            .bind_deadline(route.pg, topology, route.epoch, deadline)
            .map_err(|_| {
                StorageRpcPayloadError::InvalidResponseEnvelope("stream allocation cutoff")
            })?;
        Ok(Self {
            route,
            description,
            prefix: request.prefix,
            preparation: request.preparation,
            topology: topology.clone(),
            deadline,
            fence,
            digest: wire::digest(bytes),
        })
    }
    pub(in crate::node_runtime) fn route(&self) -> Route {
        self.route
    }
    pub(in crate::node_runtime) fn topology(&self) -> &PgTopology {
        &self.topology
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime::pg_store) fn id(
        &self,
    ) -> crate::metadata_command::MetadataCommandId {
        crate::metadata_command::MetadataCommandId::new(
            self.route.epoch,
            self.route.pg,
            crate::metadata_command::MetadataCommandLogIndex::new(self.prefix.applied_log_index)
                .expect("decoded positive prefix"),
        )
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), crate::StoreError> {
        self.fence.require_valid_for(self.route.epoch)?;
        if Instant::now() >= self.deadline
            || crate::clock::current_time_millis()
                >= self.route.deadline.portable_wall_valid_until_ms
        {
            return Err(crate::StoreError::OperationDeadlineExceeded {
                context: "remote confirmed stream allocation",
            });
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn require_prefix(
        &self,
        pg: &PgStore,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let current = pg.require_complete_settled_prefix_in_transaction(
            self.route.node.as_u32(),
            self.route.epoch,
            &self.topology,
        )?;
        if current != self.prefix {
            return Err(Error::PrefixDisagreement);
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn prepare(
        &self,
        pg: &PgStore,
        authority: &RemoteStreamAllocationAuthority<'_>,
    ) -> Result<crate::StreamUploadSegmentRecord, Error> {
        authority.require_effect(pg)?;
        if !std::ptr::eq(self, authority.query()) {
            return Err(Error::Window("remote allocation query identity".into()));
        }
        self.description
            .request(&self.topology)?
            .require_stream_preparation(pg, &self.preparation.request)?;
        preparation::prepare_remote_confirmed_stream_window(pg, authority)
    }
    pub(in crate::node_runtime::pg_store) fn preparation_inputs(
        &self,
    ) -> (
        &crate::PrepareStreamUploadSegmentAppendReq,
        &crate::types::ExpectedStreamAppendState,
        AdmittedRouteEffectFence,
    ) {
        (
            &self.preparation.request,
            self.preparation
                .expected_append_state
                .as_ref()
                .expect("decoded required authority"),
            self.fence,
        )
    }
    pub(in crate::node_runtime) fn response(
        &self,
        segment: &crate::StreamUploadSegmentRecord,
    ) -> Vec<u8> {
        wire::encode_response(self.digest, segment)
    }

    /// Observation only: hold one SQLite read snapshot across exact prefix,
    /// descriptor, session/generation, issuer and consumed-cursor validation.
    /// This cannot allocate, update a cursor or construct a physical writer.
    pub(in crate::node_runtime) fn inspect_files(
        &self,
        pg: &PgStore,
        segment: &crate::StreamUploadSegmentRecord,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let window = self.description.request(&self.topology)?;
        let read = window.inspect_primary(pg, self.route.node, self.route.epoch)?;
        if *read.prefix() != self.prefix {
            return Err(Error::PrefixDisagreement);
        }
        window.require_allocated_segment(&self.preparation.request, segment)?;
        window.require_stream_files(
            pg,
            self.route.node,
            segment,
            self.preparation
                .expected_append_state
                .as_ref()
                .expect("decoded authority"),
        )?;
        self.require_valid()?;
        read.finish()?;
        Ok(())
    }
}
