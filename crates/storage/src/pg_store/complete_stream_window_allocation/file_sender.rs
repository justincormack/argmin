// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A borrowed file attempt cannot detach successful allocation custody. Exact
//! request identity and uncertain history survive every failed transport attempt.
use super::*;
use crate::storage_rpc::{complete_settled_prefix as route_wire, complete_staging_file as wire};
use crate::WriteAck;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct FileAttempts(BTreeMap<u8, ([u8; 32], Option<WriteAck>)>);

pub(in crate::node_runtime) struct RemoteStreamFileAttempt<'a, 'route> {
    writer: &'a RemoteStreamStagingFileWriter<'route>,
    location: crate::cluster::ShardLocation,
    request: wire::EncodedRequest,
}

impl RemoteStreamStagingFileWriter<'_> {
    pub(in crate::node_runtime) fn persist_acknowledgements_remote(
        &mut self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        self.writer.acknowledgements.persist_remote(
            RemoteAcknowledgementScope {
                window: &self.writer.allocated.window,
                fence: self.writer.fence,
                deadline: self.writer.deadline,
                cutoff: self.cutoff,
                inspection: Some(&self.inspection),
            },
            client,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_ack_attempt(&self) -> Option<(Vec<u8>, bool)> {
        self.writer.acknowledgements.remote_observation()
    }

    pub(in crate::node_runtime) fn write_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        shard_index: u8,
        bytes: &[u8],
    ) -> Result<WriteAck, Error> {
        let window = &self.writer.allocated.window;
        window.require_valid()?;
        let route = &window.terminal.owner.route;
        self.writer.fence.require_valid_for(route.epoch())?;
        let (placement, _) = route.staging_file_placement()?;
        let (location, _) = window.read.stream_file_location(
            route.topology(),
            placement,
            &self.writer.allocated.segment,
            shard_index,
            bytes.len(),
        )?;
        route.require_staging_file_client(&location, client)?;
        let request = wire::encode_request(wire::Request {
            receiver: route_wire::Request {
                node: location.node_id(),
                pg: self.id().pg_id(),
                epoch: route.epoch(),
                topology: route_wire::topology_digest(route.topology()),
                deadline: self.cutoff,
            },
            provenance: wire::Provenance::Stream {
                inspection: &self.inspection,
            },
            shard_index,
            bytes,
        })
        .map_err(|error| Error::Window(error.to_string()))?;
        self.writer
            .acknowledgements
            .bind_size(self.writer.allocated.segment.size)?;
        let digest = request.digest();
        if let Some((original, confirmed)) = self.attempts.0.get(&shard_index) {
            if original != &digest {
                return Err(Error::Window(
                    "remote staging file retry changed request".into(),
                ));
            }
            if let Some(ack) = confirmed {
                return Ok(*ack);
            }
        }
        // An early rejection is not evidence that an earlier attempt failed.
        self.attempts.0.insert(shard_index, (digest, None));
        let attempt = RemoteStreamFileAttempt {
            writer: self,
            location,
            request,
        };
        let ack = client.send_stream_staging_file(&attempt)?;
        self.attempts.0.insert(shard_index, (digest, Some(ack)));
        self.writer.acknowledgements.record(shard_index, ack);
        Ok(ack)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_file_attempt(
        &self,
        index: u8,
    ) -> Option<([u8; 32], Option<WriteAck>, u64)> {
        self.attempts
            .0
            .get(&index)
            .map(|(digest, ack)| (*digest, *ack, self.cutoff.portable_wall_valid_until_ms))
    }
}

impl RemoteStreamFileAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_client(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        let window = &self.writer.writer.allocated.window;
        window.require_valid()?;
        let route = &window.terminal.owner.route;
        route.require_staging_file_client(&self.location, client)?;
        self.writer.writer.fence.require_valid_for(route.epoch())?;
        if Instant::now() >= self.deadline()
            || crate::clock::current_time_millis()
                >= self.writer.cutoff.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote staging file original cutoff",
            }
            .into());
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.writer.writer.deadline
    }
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        self.request.bytes()
    }
    pub(in crate::node_runtime) fn confirm(&self, bytes: &[u8]) -> Result<WriteAck, Error> {
        // A valid success remains evidence even if the cutoff passes in transit.
        self.request
            .decode_response(bytes)
            .map_err(|error| Error::Window(error.to_string()))
    }
}
