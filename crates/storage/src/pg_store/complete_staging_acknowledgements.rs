// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed acknowledgement publication from actual file successes. No raw
//! caller batch, renewed cutoff or cleanup is exposed. Confirmed persistence
//! closes the file-writing phase through a consuming, no-effect handoff.
use super::*;
use crate::storage_rpc::{
    complete_settled_prefix as route_wire, complete_staging_ack as wire,
    StorageRpcOperationDeadline,
};
use crate::types::AdmittedRouteEffectFence;
use crate::{ClusterEpoch, NodeId, PgId, ShardKey, WriteAck};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct RecordedFileAcknowledgements {
    logical_size: Option<u64>,
    acks: BTreeMap<u8, WriteAck>,
    remote: Option<RemoteAcknowledgementState>,
    embedded_confirmed: bool,
}

/// A failed no-effect handoff retains the identical writer and retry history.
#[must_use]
pub(in crate::node_runtime) struct StagingFileHandoffFailure<W> {
    writer: Box<W>,
    error: Error,
}

impl<W> StagingFileHandoffFailure<W> {
    pub(super) fn new(writer: W, error: Error) -> Self {
        Self {
            writer: Box::new(writer),
            error,
        }
    }
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_writer(self) -> W {
        *self.writer
    }
    pub(super) fn into_parts(self) -> (Box<W>, Error) {
        (self.writer, self.error)
    }
}

struct RemoteAcknowledgementState {
    request: wire::EncodedRequest,
    confirmed: bool,
}

pub(super) struct RemoteAcknowledgementScope<'a, 'route> {
    pub(super) window: &'a ConfirmedStagingWindow<'route>,
    pub(super) fence: AdmittedRouteEffectFence,
    pub(super) deadline: Instant,
    pub(super) cutoff: StorageRpcOperationDeadline,
    pub(super) inspection: Option<&'a [u8]>,
}

/// Borrows original custody and the immutable retained request. Only the
/// owning receipt journal can construct this; transport cannot detach/reset it.
pub(in crate::node_runtime) struct RemoteStagingAcknowledgementAttempt<'a, 'route> {
    scope: RemoteAcknowledgementScope<'a, 'route>,
    pg: PgId,
    primary: NodeId,
    request: &'a wire::EncodedRequest,
}

impl RemoteStagingAcknowledgementAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_client(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        self.scope.window.require_valid()?;
        let route = &self.scope.window.terminal.owner.route;
        route.require_staging_ack_client(self.pg, self.primary, client)?;
        self.scope.fence.require_valid_for(route.epoch())?;
        if Instant::now() >= self.scope.deadline
            || crate::clock::current_time_millis() >= self.scope.cutoff.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote staging acknowledgement original cutoff",
            }
            .into());
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.scope.deadline
    }
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        self.request.bytes()
    }
    pub(in crate::node_runtime) fn confirm(&self, bytes: &[u8]) -> Result<(), Error> {
        // Authentic success remains evidence even if authority expires in transit.
        self.request
            .decode_response(bytes)
            .map_err(|e| Error::Window(e.to_string()))
    }
}

impl RecordedFileAcknowledgements {
    pub(super) fn is_unattempted(&self) -> bool {
        self.logical_size.is_none()
            && self.acks.is_empty()
            && self.remote.is_none()
            && !self.embedded_confirmed
    }
    pub(super) fn confirmed_size(&self) -> Result<u64, Error> {
        self.require_confirmed()?;
        self.logical_size
            .ok_or_else(|| Error::Window("no acknowledged file size".into()))
    }
    pub(super) fn direct_manifest(
        &self,
        window: &ConfirmedStagingWindow<'_>,
        crc64: u64,
    ) -> Result<crate::node_runtime::pg_store::payload_staging::DirectStagingManifest, Error> {
        self.require_confirmed()?;
        let logical_size = self
            .logical_size
            .ok_or_else(|| Error::Window("no direct file size".into()))?;
        window.read.direct_manifest(
            window.terminal.owner.route.topology(),
            logical_size,
            crc64,
            &self.acks,
        )
    }

    pub(super) fn confirmed_receipts(&self) -> Result<Vec<wire::Receipt>, Error> {
        self.require_confirmed()?;
        Ok(self
            .acks
            .iter()
            .map(|(&shard_index, &ack)| wire::Receipt { shard_index, ack })
            .collect())
    }

    pub(super) fn stream_integrity(
        &self,
        window: &ConfirmedStagingWindow<'_>,
        fence: AdmittedRouteEffectFence,
        deadline: Instant,
        segment: &crate::StreamUploadSegmentRecord,
        encryption: &crate::ObjectEncryption,
    ) -> Result<crate::types::SegmentShardIntegrityWitness, Error> {
        self.require_confirmed()?;
        let batch = self.batch(window, fence, deadline)?;
        let stored_size = encryption
            .segment_stored_size(segment.size)
            .ok_or_else(|| Error::Window("acknowledged append codec geometry".into()))?;
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
            &batch
                .rows
                .iter()
                .map(|(key, ack)| (key, *ack))
                .collect::<Vec<_>>(),
        )
        .map_err(|e| Error::Window(format!("acknowledged append integrity: {e}")))
    }

    pub(super) fn persist_remote(
        &mut self,
        scope: RemoteAcknowledgementScope<'_, '_>,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        let batch = self.batch(scope.window, scope.fence, scope.deadline)?;
        let route = &scope.window.terminal.owner.route;
        route.require_staging_ack_client(batch.pg, batch.primary, client)?;
        let descriptor = scope.window.read.file_descriptor(route.topology())?;
        let provenance = match scope.inspection {
            Some(inspection) => wire::Provenance::Stream { inspection },
            None => wire::Provenance::Direct {
                descriptor: &descriptor,
                logical_size: self.logical_size.expect("complete batch has logical size"),
            },
        };
        let request = wire::encode_request(&wire::Request {
            receiver: route_wire::Request {
                node: batch.primary,
                pg: scope.window.id().pg_id(),
                epoch: batch.epoch,
                topology: route_wire::topology_digest(route.topology()),
                deadline: scope.cutoff,
            },
            provenance,
            receipts: self
                .acks
                .iter()
                .map(|(&shard_index, &ack)| wire::Receipt { shard_index, ack })
                .collect(),
        })
        .map_err(|e| Error::Window(e.to_string()))?;
        if let Some(previous) = &self.remote {
            if previous.request.bytes() != request.bytes() {
                return Err(Error::Window(
                    "remote staging acknowledgement retry changed request".into(),
                ));
            }
        }
        // Validate the endpoint/deadline before recording a new attempt. A later
        // rejection never erases a previous uncertain or confirmed observation.
        RemoteStagingAcknowledgementAttempt {
            scope: RemoteAcknowledgementScope {
                window: scope.window,
                fence: scope.fence,
                deadline: scope.deadline,
                cutoff: scope.cutoff,
                inspection: scope.inspection,
            },
            pg: batch.pg,
            primary: batch.primary,
            request: &request,
        }
        .require_client(client)?;
        let state = self.remote.get_or_insert(RemoteAcknowledgementState {
            request,
            confirmed: false,
        });
        if state.confirmed {
            return Ok(());
        }
        let attempt = RemoteStagingAcknowledgementAttempt {
            scope,
            pg: batch.pg,
            primary: batch.primary,
            request: &state.request,
        };
        client.send_staging_acknowledgements(&attempt)?;
        state.confirmed = true;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn remote_observation(&self) -> Option<(Vec<u8>, bool)> {
        self.remote
            .as_ref()
            .map(|state| (state.request.bytes().to_vec(), state.confirmed))
    }

    // Pin geometry before dispatch, including a may-have-written failure.
    pub(super) fn bind_size(&mut self, size: u64) -> Result<(), Error> {
        if self.logical_size.is_some_and(|previous| previous != size) {
            return Err(Error::Window("staging file logical size changed".into()));
        }
        self.logical_size = Some(size);
        Ok(())
    }

    pub(super) fn record(&mut self, index: u8, ack: WriteAck) {
        // Exact file retries preserve confirmation. Any changed receipt must
        // never inherit confirmation for an older complete batch.
        if self.acks.get(&index) != Some(&ack) {
            self.embedded_confirmed = false;
            if let Some(state) = &mut self.remote {
                state.confirmed = false;
            }
        }
        self.acks.insert(index, ack);
    }

    pub(super) fn require_confirmed(&self) -> Result<(), Error> {
        if self.embedded_confirmed || self.remote.as_ref().is_some_and(|state| state.confirmed) {
            Ok(())
        } else {
            Err(Error::Window(
                "staging acknowledgement persistence is not confirmed".into(),
            ))
        }
    }

    pub(super) fn persist_embedded(
        &mut self,
        window: &ConfirmedStagingWindow<'_>,
        fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), Error> {
        let batch = self.batch(window, fence, deadline)?;
        let runtime = window.terminal.owner.route.embedded_staging_ack_runtime(
            batch.pg,
            batch.primary,
            batch.epoch,
        )?;
        runtime.persist_staging_acknowledgements(&batch)?;
        self.embedded_confirmed = true;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn persist_on_runtime(
        &self,
        window: &ConfirmedStagingWindow<'_>,
        fence: AdmittedRouteEffectFence,
        deadline: Instant,
        runtime: &crate::node::LocalNodeRuntime,
    ) -> Result<(), Error> {
        runtime.persist_staging_acknowledgements(&self.batch(window, fence, deadline)?)
    }

    fn batch<'a, 'route>(
        &self,
        window: &'a ConfirmedStagingWindow<'route>,
        fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<StagingAcknowledgementBatch<'a, 'route>, Error> {
        window.require_valid()?;
        let route = &window.terminal.owner.route;
        fence.require_valid_for(route.epoch())?;
        let size = self
            .logical_size
            .ok_or_else(|| Error::Window("no staging file receipts".into()))?;
        let records = window
            .read
            .acknowledgement_batch(route.topology(), size, &self.acks)?;
        Ok(StagingAcknowledgementBatch {
            window,
            fence,
            deadline,
            pg: records.pg,
            primary: records.primary,
            epoch: records.epoch,
            rows: records.rows,
        })
    }
}

pub(in crate::node_runtime) struct StagingAcknowledgementBatch<'a, 'route> {
    window: &'a ConfirmedStagingWindow<'route>,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
    pg: PgId,
    primary: NodeId,
    epoch: ClusterEpoch,
    rows: Vec<(ShardKey, WriteAck)>,
}

impl StagingAcknowledgementBatch<'_, '_> {
    pub(in crate::node_runtime) fn require_runtime(
        &self,
        runtime: &crate::node::LocalNodeRuntime,
    ) -> Result<(PgId, Instant), Error> {
        self.window.require_valid()?;
        self.fence.require_valid_for(self.epoch)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "staging acknowledgement original deadline",
            }
            .into());
        }
        let route = &self.window.terminal.owner.route;
        if !std::ptr::eq(
            runtime,
            route.embedded_staging_ack_runtime(self.pg, self.primary, self.epoch)?,
        ) {
            return Err(Error::Window(
                "staging acknowledgement runtime domain".into(),
            ));
        }
        Ok((self.pg, self.deadline))
    }

    pub(in crate::node_runtime) fn apply_in_store(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        authority: &crate::node_runtime::engine::EmbeddedStagingAckAuthority<'_, '_, '_>,
    ) -> Result<(), Error> {
        authority.require(self, pg)?;
        let rows: Vec<_> = self.rows.iter().map(|(key, ack)| (key, *ack)).collect();
        // The existing PG transaction checks this captured fence beside every
        // INSERT/exact retry. A failure leaves the writer/custody intact.
        Ok(pg.register_written_shards_batch_exact_with_effect_fence(&rows, self.fence)?)
    }
}
