// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A file attempt borrows the released-primary owner. Uncertain replies retain
//! the request digest, custody and original cutoff; there is no reset or cleanup.
use super::*;
use crate::storage_rpc::{complete_settled_prefix as route_wire, complete_staging_file as wire};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct FileAttempts(BTreeMap<u8, ([u8; 32], Option<WriteAck>)>);

impl FileAttempts {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Only the released-primary writer can construct this borrowed transport input.
pub(in crate::node_runtime) struct RemoteDirectFileAttempt<'a, 'route> {
    writer: &'a RemoteDirectStagingFileWriter<'route>,
    location: crate::cluster::ShardLocation,
    request: wire::EncodedRequest,
}

impl RemoteDirectStagingFileWriter<'_> {
    pub(in crate::node_runtime) fn persist_acknowledgements_remote(
        &mut self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        self.writer.acknowledgements.persist_remote(
            RemoteAcknowledgementScope {
                window: &self.writer.window,
                fence: self.writer.fence,
                deadline: self.writer.deadline,
                cutoff: self.cutoff,
                inspection: None,
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
        logical_size: u64,
        bytes: &[u8],
    ) -> Result<WriteAck, Error> {
        self.writer.window.require_valid()?;
        let route = &self.writer.window.terminal.owner.route;
        self.writer.fence.require_valid_for(route.epoch())?;
        let (placement, _) = route.staging_file_placement()?;
        let (location, _) = self.writer.window.read.direct_file_location(
            route.topology(),
            placement,
            shard_index,
            logical_size,
            bytes.len(),
        )?;
        route.require_staging_file_client(&location, client)?;
        let descriptor = self.writer.window.read.file_descriptor(route.topology())?;
        let request = wire::encode_request(wire::Request {
            receiver: route_wire::Request {
                node: location.node_id(),
                pg: self.id().pg_id(),
                epoch: route.epoch(),
                topology: route_wire::topology_digest(route.topology()),
                deadline: self.cutoff,
            },
            provenance: wire::Provenance::Direct {
                descriptor: &descriptor,
                logical_size,
            },
            shard_index,
            bytes,
        })
        .map_err(|error| Error::Window(error.to_string()))?;
        self.writer.acknowledgements.bind_size(logical_size)?;
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
        // Conservatively retain uncertainty even for an early transport error.
        // A later rejection cannot erase an earlier possibly successful write.
        self.attempts.0.insert(shard_index, (digest, None));
        let attempt = RemoteDirectFileAttempt {
            writer: self,
            location,
            request,
        };
        let ack = client.send_direct_staging_file(&attempt)?;
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

impl RemoteDirectFileAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_client(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), Error> {
        self.writer.writer.window.require_valid()?;
        let route = &self.writer.writer.window.terminal.owner.route;
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
        // Authenticated success is evidence even if authority expires while the
        // reply travels. Never convert that evidence back into uncertainty.
        self.request
            .decode_response(bytes)
            .map_err(|error| Error::Window(error.to_string()))
    }
}
