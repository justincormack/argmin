// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only admitted construction. Raw envelopes cannot mint this query's
//! ordinary context; the exact nested query must match the held server authority.
use super::*;
use crate::metadata_command::MetadataCommandLogIndex;
use crate::node_runtime::pg_store::{
    PgStore, RemoteDirectSnapshotQuery, StagingWindowConfirmationError as Error,
};
use crate::node_runtime::server::RemoteDirectSnapshotAuthority;
use crate::storage_rpc::{complete_direct_prepare as wire, StorageRpcPayloadError};

#[path = "payload_staging_remote_direct_installation.rs"]
mod installation;
pub(in crate::node_runtime::pg_store) use installation::DirectInstallationCheck;

pub(in crate::node_runtime) struct RemoteDirectPreparationQuery<'a> {
    snapshot: RemoteDirectSnapshotQuery<'a>,
    request: wire::Request<'a>,
    bytes: &'a [u8],
}

#[cfg(test)]
impl ContextualCommand {
    // Closed owner-local assertion; no raw envelope/installation projection.
    pub(in crate::node_runtime::pg_store) fn assert_remote_direct_terminal_for_test(
        &self,
        input: &crate::StreamPutCommitInput,
        owner: &super::super::StagingOwnerRecord,
        crc: u64,
    ) {
        let Input::Ordinary(command) = &self.input else {
            panic!("ordinary terminal")
        };
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            panic!("direct terminal")
        };
        assert_eq!(commit.object.bucket, owner.subject.bucket);
        assert_eq!(commit.object.key, owner.subject.key);
        assert_eq!(
            commit.object.generation_id,
            owner.subject.payload_generation
        );
        assert_eq!(commit.object.size, 4);
        assert_eq!(commit.object.version_id, input.version_id);
        assert_eq!(commit.object.owner, input.owner);
        assert_eq!(commit.object.acl_grants, input.acl_grants);
        assert_eq!(commit.object.public_read, input.public_read);
        assert_eq!(commit.object.encryption, input.encryption);
        assert_eq!(
            commit.object.etag,
            crate::ObjectEtag::single_part(input.etag_crc64)
        );
        assert_eq!(commit.object.tags, input.tags);
        assert_eq!(
            commit.object.metadata_blob.as_ref(),
            Some(&input.metadata_blob)
        );
        assert_eq!(
            commit.object.system_metadata_blob.as_ref(),
            Some(&input.system_metadata_blob)
        );
        assert_eq!(commit.object.object_lock, input.object_lock);
        assert_eq!(commit.generation_reservation_id, owner.subject.id.attempt);
        let window = &owner.windows[0];
        assert_eq!(commit.object.ec, window.geometry.ec);
        assert_eq!(commit.write_sequence, 1);
        assert!(commit.stale_payload.is_none());
        assert_eq!(commit.segments.len(), 1);
        let segment = &commit.segments[0];
        assert_eq!(segment.bucket, owner.subject.bucket);
        assert_eq!(segment.key, owner.subject.key);
        assert_eq!(segment.version_id, input.version_id);
        assert_eq!(segment.segment_index, 0);
        assert_eq!(segment.size, 4);
        assert_eq!(segment.segment_crc64, crc);
        assert_eq!(segment.segment_okh, window.segment_hash);
        assert_eq!(segment.segment_vid, window.start);
        assert_eq!(segment.data_pg_id, window.participants.data_pg.get());
        assert_eq!(
            segment.placement_cluster_epoch,
            window.participants.placement_epoch
        );
        assert_eq!(
            (segment.ec_k, segment.ec_m),
            (window.geometry.ec.k, window.geometry.ec.m)
        );
        segment
            .shard_integrity
            .as_ref()
            .unwrap()
            .validate(
                window.geometry.ec,
                window.geometry.codec.stored_size(4).unwrap(),
                crc,
            )
            .unwrap();
        let super::super::ResourceResponsibility::Live(resource) = &owner.resources.creation else {
            panic!("creation")
        };
        assert_eq!(
            super::super::StagingBucketResource::capture(
                &commit.bucket_write_reservation,
                resource.release_pg
            ),
            *resource
        );
    }
}

impl<'a> RemoteDirectPreparationQuery<'a> {
    pub(in crate::node_runtime) fn decode(
        bytes: &'a [u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let request = wire::decode_request(bytes)?;
        let snapshot = RemoteDirectSnapshotQuery::decode(request.snapshot_request, topology)?;
        Ok(Self {
            snapshot,
            request,
            bytes,
        })
    }
    // Descriptive borrowed query only; the server binds its authority to this
    // exact allocation within the immutable enclosing preparation query.
    pub(in crate::node_runtime) fn snapshot_query(&self) -> &RemoteDirectSnapshotQuery<'a> {
        &self.snapshot
    }
    pub(in crate::node_runtime) fn capture_response(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
    ) -> Result<Vec<u8>, Error> {
        let query = &self.snapshot;
        let route = query.route();
        let window = query.validated_window(pg, authority)?;
        let manifest = query.direct_manifest(pg, authority, self.request.segment_crc64)?;
        let read = window.inspect_primary(pg, route.node, route.epoch)?;
        let snapshot = crate::storage_rpc::StorageRpcDirectPutCommitSnapshotResponse {
            snapshot: window.direct_commit_snapshot(pg, route.node)?,
        };
        let snapshot_bytes =
            crate::storage_rpc::encode_direct_put_commit_snapshot_response(&snapshot);
        if crate::storage_rpc::complete_stream_allocation::digest(&snapshot_bytes)
            != self.request.snapshot_sha256
        {
            return Err(Error::Window("direct terminal snapshot changed".into()));
        }
        let subject = window.direct_encryption_subject(&self.request.provisional)?;
        if !subject.matches_committed_encryption(&self.request.commit.encryption) {
            return Err(Error::Window(
                "direct terminal committed encryption lineage".into(),
            ));
        }
        // This checks immutable lineage, NOT the DEK's AEAD subject. Only the
        // consuming sender's CommittedObjectEncryption evidence can prove that;
        // it must also match every returned command field before installation.
        let command = window.direct_terminal_command(
            pg,
            &manifest,
            &self.request.commit,
            &snapshot.snapshot,
        )?;
        let index = pg
            .max_metadata_command_log_index(route.epoch)?
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or_else(|| Error::Window("direct terminal command index exhausted".into()))?;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(route.epoch, route.pg, index),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
        );
        super::super::object::validate_in_open_transaction(pg, query.topology(), &command)
            .map_err(|e| Error::Window(e.to_string()))?;
        let context = ContextualCommand {
            input: Input::Ordinary(command),
            topology: query.topology().clone(),
        };
        context
            .observe_pending_inventory_in_open_transaction(pg, query.topology())
            .map_err(|e| Error::Window(e.to_string()))?;
        let encoded = context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        let response = wire::encode_response(
            self.bytes,
            crate::storage_rpc::complete_applied::BorrowedRequest {
                node_id: route.node,
                epoch: route.epoch,
                pg_id: route.pg,
                checksum: encoded.checksum_crc64(),
                command: encoded.bytes(),
            },
        )
        .map_err(|e| Error::Window(e.to_string()))?;
        read.finish()?;
        authority.require_completed_capture(pg, query)?;
        authority.retain_prepared_command(pg, query, self.bytes, encoded.command_sha256())?;
        Ok(response)
    }
}
