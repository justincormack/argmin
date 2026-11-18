// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming remote preparation, never a detached decoded-command authority.
use super::*;
use crate::node_client::CompletePrimarySessionBorrow;
use crate::storage_rpc::complete_direct_prepare as wire;

impl<'route> WitnessedDirectPayload<'route> {
    pub(in crate::node_runtime) fn prepare_terminal_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        input: &crate::StreamPutCommitInput,
        committed: &crate::types::CommittedObjectEncryption,
    ) -> Result<PreparedDirectTerminal<'route>, DirectTerminalPreparationFailure<'route>> {
        let result = (|| {
            let permit = DirectTerminalPreparation {
                custody: &self,
                input,
                committed,
            };
            permit.require_committed()?;
            let writer = self.prepared.custody.writer();
            writer
                .window
                .terminal
                .owner
                .require_primary(client, session)?;
            let mut held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                self.id().cluster_epoch(),
                permit.deadline(),
            )?;
            held.prepare_direct_terminal(client, &permit)
        })();
        match result {
            Ok(command) => Ok(PreparedDirectTerminal {
                custody: self,
                command,
            }),
            Err(error) => Err(DirectTerminalPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }
}

impl DirectTerminalPreparation<'_, '_> {
    fn require_committed(&self) -> Result<(), Error> {
        self.snapshot_permit().require_valid()?;
        if !self
            .committed
            .matches(self.custody.encryption_subject(), &self.input.encryption)
        {
            return Err(Error::Window(
                "direct terminal committed encryption subject".into(),
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn require_remote_session(
        &self,
        held: &CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.require_committed()?;
        self.snapshot_permit().require_remote_session(held)
    }
    pub(in crate::node_runtime) fn remote_request(&self) -> Result<wire::EncodedRequest, Error> {
        self.require_committed()?;
        let snapshot = self.snapshot_permit().remote_request()?;
        let captured = crate::storage_rpc::StorageRpcDirectPutCommitSnapshotResponse {
            snapshot: self.custody.prepared.captured.snapshot.clone(),
        };
        wire::encode_request(&wire::Request {
            snapshot_request: snapshot.bytes(),
            snapshot_sha256: crate::storage_rpc::complete_stream_allocation::digest(
                &crate::storage_rpc::encode_direct_put_commit_snapshot_response(&captured),
            ),
            segment_crc64: self.custody.manifest.crc64(),
            provisional: self.custody.prepared.custody.writer().provisional.clone(),
            commit: self.input.clone(),
        })
        .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime::pg_store) fn require_remote_command(
        &self,
        command: &crate::metadata_command::CommitDirectPutObjectCommand,
    ) -> Result<(), Error> {
        self.require_committed()?;
        let writer = self.custody.prepared.custody.writer();
        writer.window.read.require_direct_terminal_response(
            writer.window.terminal.owner.route.topology(),
            &self.custody.manifest,
            self.input,
            &self.custody.prepared.captured.snapshot,
            command,
        )
    }
}
