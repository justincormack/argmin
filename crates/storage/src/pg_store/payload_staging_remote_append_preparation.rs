// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Received claims require the server's exact-store/section authority. Only an
//! installation admission bound to the full request permits a slot mutation.
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandLogIndex, MetadataCommandPayload,
};
use crate::node_runtime::server::RemoteStreamAppendAuthority;
use crate::storage_rpc::{
    complete_staging_ack as ack, complete_stream_append_prepare as append_wire,
};

pub(in crate::node_runtime) struct RemoteStreamAppendQuery<'a> {
    allocation: RemoteStreamAllocationQuery,
    request: &'a [u8],
    route: Route,
    segment: crate::StreamUploadSegmentRecord,
    receipts: Vec<ack::Receipt>,
    deadline: Instant,
    fence: AdmittedRouteEffectFence,
    digest: [u8; 32],
}

impl<'a> RemoteStreamAppendQuery<'a> {
    pub(in crate::node_runtime) fn install_response(
        &self,
        pg: &PgStore,
        authority: &RemoteStreamAppendAuthority<'_>,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Error> {
        use crate::storage_rpc::complete_stream_append_install as install;
        let fence = authority.require_installation(pg, self, bytes)?;
        let request = install::decode_request(bytes).map_err(|e| Error::Window(e.to_string()))?;
        if request.preparation != self.request {
            return Err(Error::Window(
                "remote append installation preparation identity".into(),
            ));
        }
        let context = ContextualCommand::decode_command(
            request.command.command,
            request.command.checksum,
            &self.allocation.topology,
        )
        .map_err(|e| Error::Window(e.to_string()))?;
        let index = self
            .allocation
            .prefix
            .applied_log_index
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or_else(|| Error::Window("remote append installation index exhausted".into()))?;
        context.require_remote_append_installation(
            &self
                .allocation
                .description
                .request(&self.allocation.topology)?,
            MetadataCommandId::new(self.route.epoch, self.route.pg, index),
            &self.expected_append(authority.placement())?,
            authority.placement(),
            self.route.deadline.portable_wall_valid_until_ms,
        )?;
        // Exact-existing retries are checked again by the insertion transaction,
        // including scope and all indexes. A pending slot is not a settled prefix
        // and must not be rebuilt. Fresh preparation is revalidated while this
        // same server PG mutex and primary section remain held.
        if pg
            .inspect_complete_pending_command(self.route.node.as_u32(), &self.allocation.topology)
            .map_err(|e| Error::Window(e.to_string()))?
            .is_none()
        {
            self.capture_response(pg, authority)?;
        }
        authority.require_installation(pg, self, bytes)?;
        let encoded = context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        pg.try_insert_complete_pending_metadata_command_slot(
            self.route.node.as_u32(),
            &encoded,
            &self.allocation.topology,
            fence,
            Some(authority.installation_deadline()),
        )
        .map_err(|e| Error::Store(e.into_source()))?;
        // Even SQL errors are deliberately not wire-level absence evidence.
        // Only this exact request-bound success can confirm installation.
        install::confirmation(bytes).map_err(|e| Error::Window(e.to_string()))
    }

    pub(in crate::node_runtime) fn decode(
        bytes: &'a [u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let request = append_wire::decode_request(bytes)?;
        // Bind once, before descriptor decoding or storage admission.
        let deadline = request.receiver.deadline.local_deadline();
        let cutoff = request.receiver.deadline.portable_wall_valid_until_ms;
        let fence = AdmittedRouteEffectFence::bind_portable(request.receiver.epoch, cutoff, cutoff);
        let ack::Provenance::Stream { inspection } = request.provenance else {
            unreachable!()
        };
        let files = crate::storage_rpc::complete_stream_file_primary::decode_request(inspection)?;
        let allocation = RemoteStreamAllocationQuery::decode(files.allocation, topology)?;
        Ok(Self {
            deadline: deadline.min(allocation.deadline()),
            fence: allocation.fence.intersect(fence).map_err(|_| {
                StorageRpcPayloadError::InvalidResponseEnvelope("append effect epoch")
            })?,
            allocation,
            request: bytes,
            route: request.receiver,
            segment: files.segment,
            receipts: request.receipts,
            digest: wire::digest(bytes),
        })
    }
    pub(in crate::node_runtime) fn route(&self) -> Route {
        self.route
    }
    pub(in crate::node_runtime) fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime) fn effect_fence(
        &self,
    ) -> Result<AdmittedRouteEffectFence, crate::StoreError> {
        Ok(self.fence)
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), crate::StoreError> {
        self.allocation.require_valid()?;
        if Instant::now() >= self.deadline
            || crate::clock::current_time_millis()
                >= self.route.deadline.portable_wall_valid_until_ms
        {
            return Err(crate::StoreError::OperationDeadlineExceeded {
                context: "remote append preparation original cutoff",
            });
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn capture_response(
        &self,
        pg: &PgStore,
        authority: &RemoteStreamAppendAuthority<'_>,
    ) -> Result<Vec<u8>, Error> {
        let effect_fence = authority.require_capture(pg, self)?;
        let allocation = &self.allocation;
        let window = allocation.description.request(&allocation.topology)?;
        let read = window.inspect_primary(pg, self.route.node, self.route.epoch)?;
        if *read.prefix() != allocation.prefix {
            return Err(Error::PrefixDisagreement);
        }
        let (_, expected, _) = allocation.preparation_inputs();
        window.require_allocated_segment(&allocation.preparation.request, &self.segment)?;
        window.require_stream_files(pg, self.route.node, &self.segment, expected)?;
        let append = self.expected_append(authority.placement())?;
        pg.validate_append_stream_segment_command(&append)
            .map_err(|e| Error::Window(e.to_string()))?
            .require_primary_preparation()?;
        let index = pg
            .max_metadata_command_log_index(self.route.epoch)?
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or_else(|| Error::Window("append command index exhausted".into()))?;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(self.route.epoch, self.route.pg, index),
            MetadataCommandPayload::AppendStreamSegment(Box::new(append)),
        );
        let capture = capture::StagingCapture {
            topology: &allocation.topology,
            placement: authority.placement(),
            effect_fence,
        };
        let context = ContextualCommand::from_described(
            capture
                .append_candidate(pg, self.route.node, command)
                .map_err(|e| Error::Window(e.to_string()))?,
        );
        context
            .observe_pending_inventory_in_open_transaction(pg, &allocation.topology)
            .map_err(|e| Error::Window(e.to_string()))?;
        let encoded = context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        let response = append_wire::encode_response(
            self.request,
            crate::storage_rpc::complete_applied::BorrowedRequest {
                node_id: self.route.node,
                epoch: self.route.epoch,
                pg_id: self.route.pg,
                checksum: encoded.checksum_crc64(),
                command: encoded.bytes(),
            },
        )
        .map_err(|e| Error::Window(e.to_string()))?;
        read.finish()?;
        authority.require_completed_capture(pg, self)?;
        Ok(response)
    }

    fn expected_append(
        &self,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
    ) -> Result<crate::metadata_command::AppendStreamSegmentCommand, Error> {
        let allocation = &self.allocation;
        let window = allocation.description.request(&allocation.topology)?;
        let (_, expected, _) = allocation.preparation_inputs();
        window.require_allocated_segment(&allocation.preparation.request, &self.segment)?;
        for receipt in &self.receipts {
            window.stream_file_location(
                placement,
                &self.segment,
                receipt.shard_index,
                receipt.ack.stored_size as usize,
            )?;
        }
        let receipts = self
            .receipts
            .iter()
            .map(|r| (r.shard_index, r.ack))
            .collect();
        let rows = window.acknowledgement_batch(self.segment.size, &receipts)?;
        let session = match expected {
            crate::ExpectedStreamAppendState::PutObject { session, .. }
            | crate::ExpectedStreamAppendState::UploadPart { session, .. } => session,
        };
        let stored_size = session
            .encryption
            .segment_stored_size(self.segment.size)
            .ok_or_else(|| Error::Window("remote append codec geometry".into()))?;
        let mut segment = self.segment.clone();
        segment.shard_integrity = Some(
            crate::types::SegmentShardIntegrityWitness::from_ordered_shard_acks(
                segment.data_pg_id,
                crate::EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
                &segment.segment_okh,
                segment.segment_vid,
                stored_size,
                segment.segment_crc64,
                &rows
                    .rows()
                    .iter()
                    .map(|(key, ack)| (key, *ack))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| Error::Window(format!("remote append integrity: {e}")))?
            .durable_record(),
        );
        Ok(crate::metadata_command::AppendStreamSegmentCommand {
            bucket: session.bucket.clone(),
            key: session.key.clone(),
            target: session.target.clone(),
            segment,
        })
    }
}
