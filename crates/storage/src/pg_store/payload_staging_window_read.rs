// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A bounded read result, not physical authority. Only the consuming terminal
//! handoff combines it with original publication custody. No descriptor escapes.

use super::*;
use crate::cluster::{CompleteFanoutRoute, EmbeddedPublicationSection};
use crate::node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationError as Error, StagingWindowConfirmationObservation as Observation,
    StagingWindowConfirmationRequest,
};
use crate::types::AdmittedRouteEffectFence;
use crate::{ClusterEpoch, StoreError};
use std::time::Instant;

pub(in crate::node_runtime::pg_store) struct CapturedWindowRead {
    observation: Observation,
    deadline: Instant,
    portable_cutoff_ms: u64,
    fence: AdmittedRouteEffectFence,
    epoch: ClusterEpoch,
    remote_session: Option<crate::node_client::MetadataCommandSessionIdentity>,
    descriptor: super::super::super::StagingWindowInspectionDescription,
}

impl ContextualCommand {
    pub(in crate::node_runtime::pg_store) fn require_remote_append_installation(
        &self,
        previous: &StagingWindowConfirmationRequest<'_>,
        id: crate::metadata_command::MetadataCommandId,
        expected: &crate::metadata_command::AppendStreamSegmentCommand,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        cutoff: u64,
    ) -> Result<(), Error> {
        let Input::Described(candidate) = &self.input else {
            return Err(Error::Window(
                "remote append installation context kind".into(),
            ));
        };
        let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(actual) =
            candidate.command().payload()
        else {
            return Err(Error::Window(
                "remote append installation command kind".into(),
            ));
        };
        if self.id() != id || actual.as_ref() != expected {
            return Err(Error::Window(
                "remote append installation command correspondence".into(),
            ));
        }
        let bound = candidate
            .bind(&self.topology)
            .map_err(|e| Error::Window(e.to_string()))?;
        previous.require_append_successor(
            &bound.pending_protection().confirmation_request(),
            placement,
            id.cluster_epoch(),
            cutoff,
        )
    }
}

impl CapturedWindowRead {
    /// Reconfirm only the observation after another command has settled. Keep
    /// the exact descriptor, issuer, both original cutoffs and effect fence.
    /// No new physical allocation or write authority is created.
    pub(in crate::node_runtime::pg_store) fn reinspect_direct_prefix_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        topology: &crate::PgTopology,
        deadline: Instant,
    ) -> Result<(), Error> {
        self.require_valid()?;
        if self.remote_session.is_some() {
            return Err(Error::Window(
                "embedded reinspection cannot replace a remote window session".into(),
            ));
        }
        let request = self.descriptor.request(topology)?;
        let observation =
            section.reinspect_staging_window(&request, deadline.min(self.deadline))?;
        self.require_valid()?;
        self.observation = observation;
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn require_direct_terminal_response(
        &self,
        topology: &crate::PgTopology,
        manifest: &super::super::DirectStagingManifest,
        input: &crate::StreamPutCommitInput,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
        command: &crate::metadata_command::CommitDirectPutObjectCommand,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let expected = self
            .descriptor
            .request(topology)?
            .assemble_direct_terminal(
                manifest,
                input,
                snapshot,
                command.write_sequence,
                command.last_modified_millis,
            )?;
        if command != &expected {
            return Err(Error::Window(
                "remote direct terminal command correspondence".into(),
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn require_direct_snapshot_response(
        &self,
        topology: &crate::PgTopology,
        client: &UnixStorageNodeClient,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<(), Error> {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .require_direct_snapshot_response(client, snapshot)
    }
    pub(in crate::node_runtime::pg_store) fn require_direct_terminal_snapshot(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
        expected: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<(), Error> {
        self.require_valid()?;
        // An uncertain exact retry may already own the pending slot. Validate
        // the live subject/snapshot here, not a previously empty-slot prefix.
        let current = self
            .descriptor
            .request(topology)?
            .direct_commit_snapshot(pg, node)?;
        if &current != expected {
            return Err(Error::Window("direct terminal snapshot changed".into()));
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn direct_encryption_subject(
        &self,
        topology: &crate::PgTopology,
        provisional: &crate::ObjectEncryption,
    ) -> Result<crate::ObjectEncryptionSubject, Error> {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .direct_encryption_subject(provisional)
    }
    pub(in crate::node_runtime::pg_store) fn require_append_response(
        &self,
        context: &ContextualCommand,
        expected: &crate::metadata_command::AppendStreamSegmentCommand,
        topology: &crate::PgTopology,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        cutoff: u64,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let Input::Described(candidate) = &context.input else {
            return Err(Error::Window(
                "remote append lacks successor context".into(),
            ));
        };
        let id = context.id();
        let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(actual) =
            candidate.command().payload()
        else {
            return Err(Error::Window("remote append command kind".into()));
        };
        if topology != &context.topology
            || actual.as_ref() != expected
            || id.cluster_epoch() != self.epoch
            || Some(id.log_index().get())
                != self.observation.prefix.applied_log_index.checked_add(1)
        {
            return Err(Error::Window(
                "remote append command differs from acknowledged custody".into(),
            ));
        }
        let bound = candidate
            .bind(topology)
            .map_err(|e| Error::Window(e.to_string()))?;
        let protection = bound.pending_protection();
        self.descriptor.request(topology)?.require_append_successor(
            &protection.confirmation_request(),
            placement,
            self.epoch,
            cutoff.min(self.portable_cutoff_ms),
        )
    }

    pub(in crate::node_runtime::pg_store) fn file_descriptor(
        &self,
        topology: &crate::PgTopology,
    ) -> Result<Vec<u8>, Error> {
        self.require_valid()?;
        self.descriptor.request(topology)?.encode_descriptor()
    }

    pub(in crate::node_runtime::pg_store) fn require_remote_session(
        &self,
        session: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let identity = self.remote_session.as_ref().ok_or_else(|| {
            Error::Window("remote operation requires a remotely confirmed window".into())
        })?;
        session.require_identity(identity)?;
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn inspect_direct_files_remote(
        &self,
        session: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
        node: crate::NodeId,
        pg: crate::PgId,
        topology: &crate::PgTopology,
    ) -> Result<(), Error> {
        self.require_remote_session(session)?;
        let descriptor = self.descriptor.request(topology)?.encode_descriptor()?;
        let request = crate::storage_rpc::complete_window_primary::Request {
            route: crate::storage_rpc::complete_settled_prefix::Request {
                node,
                pg,
                epoch: self.epoch,
                topology: crate::storage_rpc::complete_settled_prefix::topology_digest(topology),
                // Reuse the original absolute upper bound, never recalculate a
                // duration/cutoff when binding or retrying transport.
                deadline: crate::storage_rpc::StorageRpcOperationDeadline {
                    portable_wall_valid_until_ms: self.portable_cutoff_ms,
                },
            },
            descriptor: &descriptor,
        };
        let prefix = session.inspect_direct_file_primary(request, self.observation.witness)?;
        self.require_valid()?;
        if prefix != self.observation.prefix {
            return Err(Error::PrefixDisagreement);
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn acknowledgement_batch(
        &self,
        topology: &crate::PgTopology,
        logical_size: u64,
        acks: &std::collections::BTreeMap<u8, crate::WriteAck>,
    ) -> Result<crate::node_runtime::pg_store::payload_staging::StagingAcknowledgementRecords, Error>
    {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .acknowledgement_batch(logical_size, acks)
    }

    pub(in crate::node_runtime::pg_store) fn require_stream_files(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
        segment: &crate::StreamUploadSegmentRecord,
        expected: &crate::types::ExpectedStreamAppendState,
    ) -> Result<(), Error> {
        self.require_store_prefix(pg, node, topology)?;
        self.descriptor
            .request(topology)?
            .require_stream_files(pg, node, segment, expected)
    }

    pub(in crate::node_runtime::pg_store) fn stream_file_location(
        &self,
        topology: &crate::PgTopology,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        segment: &crate::StreamUploadSegmentRecord,
        shard_index: u8,
        bytes: usize,
    ) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), Error> {
        self.require_valid()?;
        self.descriptor.request(topology)?.stream_file_location(
            placement,
            segment,
            shard_index,
            bytes,
        )
    }

    pub(in crate::node_runtime::pg_store) fn require_direct_files(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
    ) -> Result<(), Error> {
        self.require_store_prefix(pg, node, topology)?;
        self.descriptor
            .request(topology)?
            .require_direct_file_reservation(pg, node)
    }

    pub(in crate::node_runtime::pg_store) fn direct_commit_snapshot(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
    ) -> Result<crate::DirectPutCommitStorageSnapshot, Error> {
        self.require_store_prefix(pg, node, topology)?;
        self.descriptor
            .request(topology)?
            .direct_commit_snapshot(pg, node)
    }

    pub(in crate::node_runtime::pg_store) fn direct_manifest(
        &self,
        topology: &crate::PgTopology,
        logical_size: u64,
        crc64: u64,
        acks: &std::collections::BTreeMap<u8, crate::WriteAck>,
    ) -> Result<super::super::DirectStagingManifest, Error> {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .direct_manifest(logical_size, crc64, acks)
    }

    pub(in crate::node_runtime::pg_store) fn direct_terminal_command(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        request: &crate::node_runtime::pg_store::CompletePublicationRequest<'_>,
        manifest: &super::super::DirectStagingManifest,
        input: &crate::StreamPutCommitInput,
        expected: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<crate::metadata_command::CommitDirectPutObjectCommand, Error> {
        let current = self.direct_commit_snapshot(pg, request.node_id(), request.topology())?;
        if &current != expected {
            return Err(Error::Window("direct terminal snapshot changed".into()));
        }
        self.descriptor
            .request(request.topology())?
            .direct_terminal_command(pg, manifest, input, &current)
    }

    pub(in crate::node_runtime::pg_store) fn direct_file_location(
        &self,
        topology: &crate::PgTopology,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        shard_index: u8,
        logical_size: u64,
        bytes: usize,
    ) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), Error> {
        self.require_valid()?;
        self.descriptor.request(topology)?.direct_file_location(
            placement,
            shard_index,
            logical_size,
            bytes,
        )
    }

    pub(in crate::node_runtime::pg_store) fn direct_ciphertext_size(
        &self,
        topology: &crate::PgTopology,
        logical_size: u64,
    ) -> Result<u64, Error> {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .direct_ciphertext_size(logical_size)
    }

    pub(in crate::node_runtime::pg_store) fn direct_encoding_geometry(
        &self,
        topology: &crate::PgTopology,
        logical_size: u64,
    ) -> Result<([u8; 16], crate::GenerationId, crate::EcShape), Error> {
        self.require_valid()?;
        self.descriptor
            .request(topology)?
            .direct_encoding_geometry(logical_size)
    }

    pub(in crate::node_runtime::pg_store) fn file_effect_fence(
        &self,
    ) -> Result<AdmittedRouteEffectFence, Error> {
        self.require_valid()?;
        Ok(self
            .fence
            .intersect(AdmittedRouteEffectFence::bind_portable(
                self.epoch,
                self.portable_cutoff_ms,
                self.portable_cutoff_ms,
            ))?)
    }
    pub(in crate::node_runtime::pg_store) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime::pg_store) fn effect_fence(&self) -> AdmittedRouteEffectFence {
        self.fence
    }
    pub(in crate::node_runtime::pg_store) fn require_store_prefix(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let current =
            pg.require_complete_settled_prefix_in_transaction(node.as_u32(), self.epoch, topology)?;
        if current != self.observation.prefix {
            return Err(Error::PrefixDisagreement);
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn observation(&self) -> &Observation {
        &self.observation
    }

    pub(in crate::node_runtime::pg_store) fn require_valid(&self) -> Result<(), Error> {
        self.fence.require_valid_for(self.epoch)?;
        if Instant::now() >= self.deadline
            || crate::clock::current_time_millis() >= self.portable_cutoff_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "confirmed staging window original cutoff",
            }
            .into());
        }
        Ok(())
    }
}

impl ContextualCommand {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cross_remote_append_reply(
        &self,
        field: &str,
    ) -> Self {
        use crate::metadata_command::{
            MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
            MetadataCommandPayload,
        };
        let Input::Described(candidate) = &self.input else {
            panic!("append context");
        };
        let mut payload = candidate.command().payload().clone();
        let MetadataCommandPayload::AppendStreamSegment(command) = &mut payload else {
            panic!("append command");
        };
        let mut id = self.id();
        let mut descriptor = super::super::descriptor::AppendDescriptor::decode(
            &candidate.encode_descriptor_field().unwrap(),
        )
        .unwrap();
        match field {
            "payload" => command.segment.payload_crc64 ^= 1,
            "witness" => {
                assert!(command.segment.ec_m > 0);
                let mut bytes = command.segment.shard_integrity.as_ref().unwrap().encode();
                *bytes.last_mut().unwrap() ^= 1;
                command.segment.shard_integrity =
                    Some(crate::types::SegmentShardIntegrityRecord::decode(&bytes).unwrap());
            }
            "index" => {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                )
            }
            "cutoff" | "participants" | "issuer" => descriptor.test_cross_remote_reply(field),
            _ => panic!("unknown reply fault"),
        }
        let candidate = DescribedStagingCommand::decode_descriptor_field(
            MetadataCommandEnvelope::new(id, payload),
            &descriptor.encode().unwrap(),
            &self.topology,
        )
        .unwrap();
        Self::from_described(candidate)
    }

    pub(in crate::node_runtime::pg_store) fn encode_stream_allocation(
        &self,
        node: crate::NodeId,
        read: &CapturedWindowRead,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
        expected: &crate::types::ExpectedStreamAppendState,
    ) -> Result<Vec<u8>, Error> {
        read.require_valid()?;
        let Input::Described(candidate) = &self.input else {
            return Err(Error::Window("allocation lacks described window".into()));
        };
        let bound = candidate
            .bind(&self.topology)
            .map_err(|e| Error::Window(e.to_string()))?;
        let window = bound.pending_protection().confirmation_request();
        window.require_stream_inputs(request, expected)?;
        let descriptor = window.encode_descriptor()?;
        let session = match expected {
            crate::types::ExpectedStreamAppendState::PutObject { session, .. }
            | crate::types::ExpectedStreamAppendState::UploadPart { session, .. } => session,
        };
        let wire = crate::storage_rpc::complete_stream_allocation::Request {
            window: crate::storage_rpc::complete_window_primary::Request {
                route: crate::storage_rpc::complete_settled_prefix::Request {
                    node,
                    epoch: self.id().cluster_epoch(),
                    pg: self.id().pg_id(),
                    deadline: crate::storage_rpc::StorageRpcOperationDeadline {
                        portable_wall_valid_until_ms: read.portable_cutoff_ms,
                    },
                    topology: crate::storage_rpc::complete_settled_prefix::topology_digest(
                        &self.topology,
                    ),
                },
                descriptor: &descriptor,
            },
            prefix: read.observation.prefix,
            preparation: crate::storage_rpc::StorageRpcStreamSegmentAppendPrepareRequest {
                object: crate::storage_rpc::StorageRpcObjectRequest {
                    node_id: node,
                    cluster_epoch: self.id().cluster_epoch(),
                    pg_id: self.id().pg_id(),
                    bucket: session.bucket.clone(),
                    key: session.key.clone(),
                },
                request: request.clone(),
                expected_append_state: Some(expected.clone()),
                effect_deadline: None,
            },
        };
        crate::storage_rpc::complete_stream_allocation::encode_request(&wire)
            .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime::pg_store) fn require_stream_allocation_window(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(), Error> {
        let Input::Described(candidate) = &self.input else {
            return Err(Error::Window("allocation lacks described window".into()));
        };
        let bound = candidate
            .bind(&self.topology)
            .map_err(|e| Error::Window(e.to_string()))?;
        bound
            .pending_protection()
            .confirmation_request()
            .require_stream_preparation(pg, request)
    }
    // This callback is private to the closed operations below, never an
    // extension point for caller-selected validation or mutation.
    fn capture_window_read(
        &self,
        deadline: Instant,
        remote_session: Option<crate::node_client::MetadataCommandSessionIdentity>,
        inspect: impl FnOnce(
            &StagingWindowConfirmationRequest<'_>,
            Instant,
        ) -> Result<
            (
                Observation,
                Instant,
                crate::storage_rpc::StorageRpcOperationDeadline,
            ),
            Error,
        >,
    ) -> Result<CapturedWindowRead, Error> {
        let Input::Described(candidate) = &self.input else {
            return Err(Error::Window("command has no future window".into()));
        };
        let bound = candidate
            .bind(&self.topology)
            .map_err(|error| Error::Window(error.to_string()))?;
        let request = bound.pending_protection().confirmation_request();
        let epoch = self.id().cluster_epoch();
        let (deadline, fence) =
            request.bind_deadline(self.id().pg_id(), &self.topology, epoch, deadline)?;
        // Capture the actual inspection bounds, including route intersection,
        // rather than independently projecting a second portable deadline.
        let (observation, deadline, portable) = inspect(&request, deadline)?;
        let read = CapturedWindowRead {
            remote_session,
            observation,
            deadline,
            portable_cutoff_ms: portable.portable_wall_valid_until_ms,
            fence,
            epoch,
            descriptor: super::super::super::StagingWindowInspectionDescription::decode(
                &request.encode_descriptor()?,
            )?,
        };
        read.require_valid()?;
        Ok(read)
    }

    pub(in crate::node_runtime::pg_store) fn capture_window_read_embedded(
        &self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<CapturedWindowRead, Error> {
        self.capture_window_read(deadline, None, |request, deadline| {
            let deadline = deadline.min(section.deadline());
            let portable = crate::storage_rpc::StorageRpcOperationDeadline::from_instant(deadline);
            let observation = section.inspect_staging_window(
                request,
                deadline,
                #[cfg(test)]
                hook,
            )?;
            Ok((observation, deadline, portable))
        })
    }

    pub(in crate::node_runtime::pg_store) fn capture_window_read_remote(
        &self,
        route: &CompleteFanoutRoute<'_>,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<CapturedWindowRead, Error> {
        self.capture_window_read(deadline, Some(session.identity()), |request, deadline| {
            #[cfg(test)]
            hook("before portable cutoff");
            route.inspect_staging_window_remote(
                request,
                client,
                session,
                deadline,
                #[cfg(test)]
                hook,
            )
        })
    }
}
