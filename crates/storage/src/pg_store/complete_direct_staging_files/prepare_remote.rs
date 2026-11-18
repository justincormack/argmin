// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming remote snapshot preparation. One post-file section, fixed physical
//! cutoff and original receipts remain inseparable from acknowledged custody.
use super::*;
use crate::node_client::CompletePrimarySessionBorrow;
use crate::storage_rpc::{complete_direct_snapshot as wire, complete_staging_ack as ack};

impl<'route> AcknowledgedDirectStagingPayload<'route> {
    pub(in crate::node_runtime) fn open_direct_primary_session(
        &mut self,
        client: &UnixStorageNodeClient,
    ) -> Result<UnixStorageNodeMetadataCommandSession, Error> {
        let permit = DirectStagingPreparation { custody: self };
        permit.require_valid()?;
        permit.remote_request()?; // Reject embedded-only physical custody before transport.
        if self.preparation_session.is_some() {
            return Err(Error::Window(
                "direct snapshot primary session already bound".into(),
            ));
        }
        let writer = self.writer();
        let session = writer
            .window
            .terminal
            .owner
            .open_primary_session(client, writer.deadline)?;
        DirectStagingPreparation { custody: self }.require_valid()?;
        self.preparation_session = Some(session.identity());
        Ok(session)
    }

    pub(in crate::node_runtime) fn prepare_direct_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<PreparedAcknowledgedDirectPayload<'route>, DirectStagingPreparationFailure<'route>>
    {
        let result = (|| {
            let permit = DirectStagingPreparation { custody: &self };
            permit.require_valid()?;
            let expected = self.preparation_session.as_ref().ok_or_else(|| {
                Error::Window("direct snapshot has no bound primary session".into())
            })?;
            session.require_identity(expected)?;
            let writer = self.writer();
            writer
                .window
                .terminal
                .owner
                .require_primary(client, session)?;
            let mut held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                self.id().cluster_epoch(),
                writer.deadline,
            )?;
            held.prepare_direct_snapshot(client, &permit)
        })();
        match result {
            Ok(captured) => Ok(PreparedAcknowledgedDirectPayload {
                custody: self,
                captured,
            }),
            Err(error) => Err(DirectStagingPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }
}

impl DirectStagingPreparation<'_, '_> {
    pub(in crate::node_runtime) fn require_remote_session(
        &self,
        held: &CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let expected =
            self.custody.preparation_session.as_ref().ok_or_else(|| {
                Error::Window("direct snapshot has no bound primary session".into())
            })?;
        held.require_identity(expected)?;
        Ok(())
    }
    pub(in crate::node_runtime) fn remote_request(&self) -> Result<wire::EncodedRequest, Error> {
        self.require_valid()?;
        let Custody::Remote(remote) = &self.custody.custody else {
            return Err(Error::Window(
                "remote direct snapshot requires remote physical custody".into(),
            ));
        };
        let writer = &remote.writer;
        let route = &writer.window.terminal.owner.route;
        let descriptor = writer.window.read.file_descriptor(route.topology())?;
        wire::encode_request(&ack::Request {
            receiver: crate::storage_rpc::complete_settled_prefix::Request {
                node: route.primary(),
                pg: self.custody.id().pg_id(),
                epoch: route.epoch(),
                topology: crate::storage_rpc::complete_settled_prefix::topology_digest(
                    route.topology(),
                ),
                deadline: remote.cutoff,
            },
            provenance: ack::Provenance::Direct {
                descriptor: &descriptor,
                logical_size: writer.acknowledgements.confirmed_size()?,
            },
            receipts: writer.acknowledgements.confirmed_receipts()?,
        })
        .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime) fn capture_remote_reply(
        &self,
        client: &UnixStorageNodeClient,
        held: &CompletePrimarySessionBorrow<'_>,
        request: &wire::EncodedRequest,
        bytes: &[u8],
    ) -> Result<DirectStagingSnapshot, Error> {
        self.require_remote_session(held)?;
        // Also bind an independently supplied request to this borrowed owner.
        // No clock-derived fields are recomputed: retries use the original cutoff.
        if request.bytes() != self.remote_request()?.bytes() {
            return Err(Error::Window(
                "remote direct snapshot request changed".into(),
            ));
        }
        let snapshot = crate::storage_rpc::decode_direct_put_commit_snapshot_response(
            request
                .decode_response(bytes)
                .map_err(|e| Error::Window(e.to_string()))?,
        )
        .map_err(|e| Error::Window(e.to_string()))?
        .snapshot;
        let writer = self.custody.writer();
        writer.window.read.require_direct_snapshot_response(
            writer.window.terminal.owner.route.topology(),
            client,
            &snapshot,
        )?;
        self.require_remote_session(held)?;
        Ok(DirectStagingSnapshot { snapshot })
    }
}
