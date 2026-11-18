// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only receiver input. Receipt claims validate geometry, not data-PG
//! persistence. Only the consuming sender may combine this with physical custody.
use super::*;
use crate::node_runtime::pg_store::{PgStore, StagingWindowConfirmationError as Error};
use crate::node_runtime::server::RemoteDirectSnapshotAuthority;
use crate::storage_rpc::{
    complete_direct_snapshot as wire, complete_settled_prefix::Request as Route,
    complete_staging_ack as ack, StorageRpcPayloadError,
};
use crate::types::AdmittedRouteEffectFence;
use std::time::Instant;

pub(in crate::node_runtime) struct RemoteDirectSnapshotQuery<'a> {
    bytes: &'a [u8],
    route: Route,
    topology: PgTopology,
    description: StagingWindowInspectionDescription,
    size: u64,
    receipts: Vec<ack::Receipt>,
    deadline: Instant,
    fence: AdmittedRouteEffectFence,
}

impl<'a> RemoteDirectSnapshotQuery<'a> {
    pub(in crate::node_runtime) fn decode(
        bytes: &'a [u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let request = wire::decode_request(bytes)?;
        let route = request.receiver;
        // Bind the absolute wire cutoff before decoding the descriptor. Retries
        // never restart a remaining duration; the durable window may shorten it.
        let deadline = route.deadline.local_deadline();
        let cutoff = route.deadline.portable_wall_valid_until_ms;
        let wire_fence = AdmittedRouteEffectFence::bind_portable(route.epoch, cutoff, cutoff);
        let ack::Provenance::Direct {
            descriptor,
            logical_size,
        } = request.provenance
        else {
            unreachable!("Direct-only codec")
        };
        let invalid =
            |_| StorageRpcPayloadError::InvalidResponseEnvelope("direct snapshot descriptor");
        if route.topology != crate::storage_rpc::complete_settled_prefix::topology_digest(topology)
        {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "direct snapshot topology",
            ));
        }
        let description =
            StagingWindowInspectionDescription::decode(descriptor).map_err(invalid)?;
        let (deadline, fence) = description
            .request(topology)
            .map_err(invalid)?
            .bind_deadline(route.pg, topology, route.epoch, deadline)
            .and_then(|(deadline, fence)| Ok((deadline, fence.intersect(wire_fence)?)))
            .map_err(|_| {
                StorageRpcPayloadError::InvalidResponseEnvelope("direct snapshot cutoff/subject")
            })?;
        Ok(Self {
            bytes,
            route,
            topology: topology.clone(),
            description,
            size: logical_size,
            receipts: request.receipts,
            deadline,
            fence,
        })
    }

    pub(in crate::node_runtime) fn route(&self) -> Route {
        self.route
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime) fn effect_fence(&self) -> AdmittedRouteEffectFence {
        self.fence
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), crate::StoreError> {
        self.fence.require_valid_for(self.route.epoch)?;
        if Instant::now() >= self.deadline
            || crate::clock::current_time_millis()
                >= self.route.deadline.portable_wall_valid_until_ms
        {
            return Err(crate::StoreError::OperationDeadlineExceeded {
                context: "remote direct snapshot original cutoff",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn capture_response(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
    ) -> Result<Vec<u8>, Error> {
        let window = self.validated_window(pg, authority)?;
        let (deadline, fence) = authority.reinspection_bounds(pg, self)?;
        pg.validate_complete_direct_reinspection_prefix(
            self.route.node,
            self.route.epoch,
            &self.topology,
            deadline,
            fence,
        )?;
        authority.require_capture(pg, self)?;
        let read = window.inspect_primary(pg, self.route.node, self.route.epoch)?;
        let snapshot = window.direct_commit_snapshot(pg, self.route.node)?;
        let encoded = crate::storage_rpc::encode_direct_put_commit_snapshot_response(
            &crate::storage_rpc::StorageRpcDirectPutCommitSnapshotResponse { snapshot },
        );
        let reply = wire::encode_response(self.bytes, &encoded)
            .map_err(|e| Error::Window(e.to_string()))?;
        read.finish()?;
        authority.require_completed_capture(pg, self)?;
        Ok(reply)
    }

    pub(in crate::node_runtime::pg_store) fn topology(&self) -> &PgTopology {
        &self.topology
    }

    pub(in crate::node_runtime::pg_store) fn validated_window(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
    ) -> Result<StagingWindowConfirmationRequest<'_>, Error> {
        authority.require_capture(pg, self)?;
        let window = self.description.request(&self.topology)?;
        let receipts = self
            .receipts
            .iter()
            .map(|r| (r.shard_index, r.ack))
            .collect();
        window.acknowledgement_batch(self.size, &receipts)?;
        for receipt in &self.receipts {
            window.direct_file_location(
                authority.placement(),
                receipt.shard_index,
                self.size,
                usize::try_from(receipt.ack.stored_size)
                    .map_err(|e| Error::Window(e.to_string()))?,
            )?;
        }
        Ok(window)
    }

    pub(in crate::node_runtime::pg_store) fn direct_manifest(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
        crc64: u64,
    ) -> Result<super::DirectStagingManifest, Error> {
        self.validated_window(pg, authority)?.direct_manifest(
            self.size,
            crc64,
            &self
                .receipts
                .iter()
                .map(|r| (r.shard_index, r.ack))
                .collect(),
        )
    }
}
