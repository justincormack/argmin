// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The admitted receiver retains the original preparation beside the complete
//! command. Both fresh insertion and exact retry rederive it inside the SAME
//! SQL transaction as the slot and side indexes, never from a cached precheck.
use super::*;
use crate::node_runtime::pg_store::command_log::PendingInstallationTransaction;
use crate::storage_rpc::complete_direct_install as install;

pub(in crate::node_runtime::pg_store) enum DirectInstallationCheck<'a, 'b> {
    Embedded(&'a crate::node_runtime::pg_store::DirectTerminalInstallationProof<'a, 'b>),
    Remote(&'a RemoteDirectInstallation<'a, 'b>),
}

pub(in crate::node_runtime::pg_store) struct RemoteDirectInstallation<'a, 'wire> {
    preparation: &'a RemoteDirectPreparationQuery<'wire>,
    authority: &'a RemoteDirectSnapshotAuthority<'a, 'wire>,
    bytes: &'a [u8],
    context: &'a ContextualCommand,
    encoded: &'a super::super::carrier::EncodedContextualCommand<'a>,
}

impl RemoteDirectInstallation<'_, '_> {
    pub(in crate::node_runtime::pg_store) fn require_context(
        &self,
        transaction: &PendingInstallationTransaction<'_>,
        node: u32,
        topology: &PgTopology,
        context: &ContextualCommand,
    ) -> Result<(), crate::StoreError> {
        let pg = transaction.store();
        transaction.require_store(pg)?;
        if !std::ptr::eq(context, self.context) {
            return Err(crate::StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct installation exact decoded command",
            });
        }
        let preparation = self.preparation;
        let query = preparation.snapshot_query();
        self.authority.require_installation(pg, query, self.bytes)?;
        let invalid = || crate::StoreError::RouteCapabilitySubjectMismatch {
            operation: "remote direct installation retained preparation",
        };
        // Only the receiver-minted command (including its timestamp) or the
        // exact already-installed bytes establish origin. Reassembly below is
        // state validation, NOT evidence that the primary selected a timestamp.
        if !self
            .authority
            .matches_prepared_command(preparation.bytes, self.encoded.command_sha256())
            && !transaction.contains_exact_complete_command(node, self.encoded)?
        {
            return Err(crate::StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct installation lacks receiver-minted preparation",
            });
        }
        let validate = || -> Result<(), Error> {
            let route = query.route();
            let Input::Ordinary(command) = &context.input else {
                return Err(Error::Window("direct terminal ordinary context".into()));
            };
            let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
                return Err(Error::Window("direct terminal payload".into()));
            };
            let max = pg.max_metadata_command_log_index(route.epoch)?;
            let state = pg.metadata_command_replica_state()?;
            if node != route.node.as_u32()
                || topology != query.topology()
                || context.topology != *topology
                || command.id().pg_id() != route.pg
                || command.id().cluster_epoch() != route.epoch
                || Some(command.id().log_index().get()) != max.checked_add(1)
                || state.cluster_epoch != route.epoch
                || state.applied_log_index != max
            {
                return Err(Error::Window("direct terminal route/index".into()));
            }
            let window = query.validated_window(pg, self.authority)?;
            let manifest =
                query.direct_manifest(pg, self.authority, preparation.request.segment_crc64)?;
            let snapshot = window.direct_commit_snapshot(pg, route.node)?;
            let encoded_snapshot = crate::storage_rpc::encode_direct_put_commit_snapshot_response(
                &crate::storage_rpc::StorageRpcDirectPutCommitSnapshotResponse {
                    snapshot: snapshot.clone(),
                },
            );
            if crate::storage_rpc::complete_stream_allocation::digest(&encoded_snapshot)
                != preparation.request.snapshot_sha256
            {
                return Err(Error::Window("direct terminal snapshot changed".into()));
            }
            let subject = window.direct_encryption_subject(&preparation.request.provisional)?;
            if !subject.matches_committed_encryption(&preparation.request.commit.encryption) {
                return Err(Error::Window("direct terminal encryption lineage".into()));
            }
            let sequence = pg
                .next_object_write_sequence(
                    commit.object.bucket.as_str(),
                    commit.object.key.as_str(),
                )
                .map_err(|e| Error::Window(e.to_string()))?;
            let expected = window.assemble_direct_terminal(
                &manifest,
                &preparation.request.commit,
                &snapshot,
                sequence,
                commit.last_modified_millis,
            )?;
            if **commit != expected {
                return Err(Error::Window("direct terminal command changed".into()));
            }
            Ok(())
        };
        validate().map_err(|_| invalid())?;
        context.require_direct_terminal_installation(pg)
    }
}

impl RemoteDirectPreparationQuery<'_> {
    pub(in crate::node_runtime) fn install_response(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let fence = authority.require_installation(pg, self.snapshot_query(), bytes)?;
        let request = install::decode_request(bytes).map_err(|e| Error::Window(e.to_string()))?;
        if request.preparation != self.bytes {
            return Err(Error::Window(
                "direct installation preparation identity".into(),
            ));
        }
        let context = ContextualCommand::decode_command(
            request.command.command,
            request.command.checksum,
            self.snapshot.topology(),
        )
        .map_err(|e| Error::Window(e.to_string()))?;
        let encoded = context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        let proof = RemoteDirectInstallation {
            preparation: self,
            authority,
            bytes,
            context: &context,
            encoded: &encoded,
        };
        pg.try_insert_complete_pending_with_direct_proof(
            self.snapshot.route().node.as_u32(),
            &encoded,
            self.snapshot.topology(),
            fence,
            Some(authority.installation_deadline()),
            Some(DirectInstallationCheck::Remote(&proof)),
        )
        .map_err(|e| Error::Store(e.into_source()))?;
        install::confirmation(bytes).map_err(|e| Error::Window(e.to_string()))
    }
}
