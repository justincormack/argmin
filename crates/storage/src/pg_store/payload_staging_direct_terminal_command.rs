// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Pure command assembly from retained physical evidence and logical input.
//! The admitted consuming owner must revalidate the snapshot in the same read
//! transaction; this helper neither installs a command nor exposes authority.
use super::*;
use crate::metadata_command::{CommitDirectPutObjectCommand, ObjectPayloadReclaimCommand};
use crate::{BucketVersioningState, ObjectLayout, ObjectSegmentRecord, PutLiveObjectReq};

impl StagingWindowConfirmationRequest<'_> {
    pub(in crate::node_runtime::pg_store) fn direct_terminal_command(
        &self,
        pg: &PgStore,
        manifest: &DirectStagingManifest,
        input: &crate::StreamPutCommitInput,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<CommitDirectPutObjectCommand, StagingWindowConfirmationError> {
        let write_sequence = pg
            .next_object_write_sequence(manifest.bucket.as_str(), manifest.key.as_str())
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        self.assemble_direct_terminal(
            manifest,
            input,
            snapshot,
            write_sequence,
            crate::clock::current_time_millis(),
        )
    }

    pub(in crate::node_runtime::pg_store) fn assemble_direct_terminal(
        &self,
        manifest: &DirectStagingManifest,
        input: &crate::StreamPutCommitInput,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
        write_sequence: u64,
        now: u64,
    ) -> Result<CommitDirectPutObjectCommand, StagingWindowConfirmationError> {
        let invalid = |message: &str| StagingWindowConfirmationError::Window(message.into());
        if write_sequence == 0 {
            return Err(invalid("direct terminal write sequence"));
        }
        let window = self.window;
        if self.subject.target != StagingTarget::DirectPut
            || manifest.bucket != self.subject.bucket
            || manifest.key != self.subject.key
            || manifest.generation != self.subject.payload_generation
            || window.index != 0
            || window.start != manifest.generation
            || manifest.segment_hash != window.segment_hash
            || manifest.data_pg != window.participants.data_pg
            || manifest.placement_epoch != window.participants.placement_epoch
            || manifest.ec != window.geometry.ec
            || manifest.codec != window.geometry.codec
            || manifest.logical_size > window.geometry.logical_bound
            || Some(manifest.stored_size) != manifest.codec.stored_size(manifest.logical_size)
        {
            return Err(invalid("direct terminal retained manifest"));
        }
        if (input.versioning == BucketVersioningState::Enabled) == input.version_id.is_null() {
            return Err(invalid("direct terminal version selection"));
        }
        if input.encryption.payload_codec() != manifest.codec
            || input.encryption.key_binding()
                != crate::types::ObjectEncryptionKeyBinding::ObjectSubjectV2
        {
            return Err(invalid("direct terminal committed encryption profile"));
        }
        let integrity = manifest.integrity.durable_record();
        integrity
            .validate(manifest.ec, manifest.stored_size, manifest.crc64)
            .map_err(|e| invalid(&format!("direct terminal integrity: {e}")))?;
        let creation = self
            .creation
            .ok_or_else(|| invalid("direct terminal creation responsibility"))?;
        creation
            .validate(
                self.subject,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            )
            .map_err(|e| invalid(&e.to_string()))?;
        let identity = &creation.identity;
        let proof = BucketWriteReservationProof {
            bucket: identity.bucket.clone(),
            reservation_id: identity.reservation_id.clone(),
            owner_token: identity.owner_token.clone(),
            cluster_epoch: identity.cluster_epoch,
            bucket_execution_generation: identity.execution_generation,
            bucket_incarnation_generation: identity.incarnation,
            operation_kind: identity.operation_kind.clone(),
            created_at: identity.created_at,
            lease_deadline: creation.observed_lease_deadline,
            target_context: identity.target_context.clone(),
        };
        let object = PutLiveObjectReq {
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            version_id: input.version_id,
            owner: input.owner.clone(),
            acl_grants: input.acl_grants.clone(),
            public_read: input.public_read,
            generation_id: manifest.generation,
            size: manifest.logical_size,
            etag: crate::ObjectEtag::single_part(input.etag_crc64),
            ec: manifest.ec,
            layout: ObjectLayout::Standard,
            tags: input.tags.clone(),
            metadata_blob: Some(input.metadata_blob.clone()),
            system_metadata_blob: Some(input.system_metadata_blob.clone()),
            object_lock: input.object_lock,
            encryption: input.encryption.clone(),
        };
        object.validate().map_err(|e| invalid(&e.to_string()))?;
        let stale_payload = if input.version_id.is_null() {
            snapshot.stale_payload.clone().map(|mut reclaim| {
                match &mut reclaim {
                    ObjectPayloadReclaimCommand::Segments(root) => root.created_at = now,
                    ObjectPayloadReclaimCommand::Multipart(root) => root.created_at = now,
                }
                reclaim
            })
        } else {
            None
        };
        Ok(CommitDirectPutObjectCommand {
            object,
            segments: vec![ObjectSegmentRecord {
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                version_id: input.version_id,
                segment_index: 0,
                size: manifest.logical_size,
                segment_crc64: manifest.crc64,
                segment_okh: manifest.segment_hash,
                segment_vid: manifest.generation,
                data_pg_id: manifest.data_pg.get(),
                placement_cluster_epoch: manifest.placement_epoch,
                ec_k: manifest.ec.k,
                ec_m: manifest.ec.m,
                shard_integrity: Some(integrity),
            }],
            generation_reservation_id: self.subject.id.attempt.clone(),
            write_sequence,
            last_modified_millis: now,
            stale_payload,
            bucket_write_reservation: proof,
        })
    }
}
