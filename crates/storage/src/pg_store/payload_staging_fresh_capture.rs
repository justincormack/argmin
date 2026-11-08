// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh primary command -> authoritative introduction descriptor. No caller
//! supplies an owner record or MPU generation. This is read-only capture, not
//! acceptance, durable bucket-proof authorization, or physical-write authority.
//! Reissue/recovery must preserve a captured descriptor, never call this path.
//! Production builders activate it with mandatory carriers and frontend custody.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{StreamUploadTarget, UploadState};

impl StagingCapture<'_> {
    /// The primary PG lock and enclosing SQL snapshot are held by the builder.
    /// No transaction, command ID, reservation, session or owner is created here.
    /// The supplied bucket proof is the builder's already-admitted proof; its
    /// durable bucket-PG authorization remains the existing fanout boundary's job.
    pub(in super::super::super) fn introduction_for_command(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        command: &MetadataCommandEnvelope,
        proof: &BucketWriteReservationProof,
    ) -> Result<descriptor::IntroductionDescriptor, StagingCaptureError> {
        self.require_fresh_command(pg, node_id, command)?;
        let epoch = self.effect_fence.cluster_epoch();
        let (bucket, key, attempt, payload_generation, target) = match command.payload() {
            MetadataCommandPayload::ReserveObjectGeneration(reserve) => (
                &reserve.bucket,
                &reserve.key,
                &reserve.reservation_id,
                reserve.generation_id,
                StagingTarget::DirectPut,
            ),
            MetadataCommandPayload::CreateStreamUpload(create) => {
                if &create.bucket_write_reservation != proof {
                    return Err(StagingCaptureError::Command);
                }
                let session = &create.session;
                let target = match &session.target {
                    StreamUploadTarget::PutObject => StagingTarget::StreamPut,
                    StreamUploadTarget::UploadPart {
                        upload_id,
                        part_number,
                    } => {
                        let upload = pg.get_multipart_upload(upload_id)?;
                        if upload.bucket != session.bucket
                            || upload.key != session.key
                            || upload.state != UploadState::InProgress
                            || upload.encryption != session.encryption
                        {
                            return Err(StagingCaptureError::Upload);
                        }
                        StagingTarget::UploadPart {
                            upload_id: upload_id.clone(),
                            upload_generation: upload.object_generation_id,
                            part_number: *part_number,
                        }
                    }
                };
                (
                    &session.bucket,
                    &session.key,
                    &session.session_id,
                    create.payload_generation,
                    target,
                )
            }
            _ => return Err(StagingCaptureError::Command),
        };
        let occupied: bool = pg.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = ?1)
             OR EXISTS(SELECT 1 FROM stream_uploads WHERE session_id = ?1)
             OR EXISTS(SELECT 1 FROM stream_upload_segments WHERE session_id = ?1)
             OR EXISTS(SELECT 1 FROM payload_staging_owners WHERE attempt = ?1)",
            [attempt],
            |row| row.get(0),
        )?;
        if occupied {
            return Err(StagingCaptureError::IndependentlyOwned);
        }
        if PgMetadataStore::next_generation_id(pg, bucket, key)? != payload_generation {
            return Err(StagingCaptureError::StaleGeneration);
        }
        let owner = self.introduction(
            pg,
            node_id,
            StagingIntroduction {
                id: StagingOwnerId {
                    attempt: attempt.clone(),
                    introduction: command.id(),
                },
                bucket: bucket.clone(),
                key: key.clone(),
                payload_generation,
                target,
            },
            proof,
        )?;
        let descriptor =
            descriptor::IntroductionDescriptor::capture(&owner.validate(self.topology)?)?;
        // The stream command must carry the actual local issuer and the same
        // incarnation, initial cursor, encryption profile and proof as capture.
        descriptor.bind(self.topology, command)?;
        pg.require_live_allocation_lifetime()?;
        self.effect_fence.require_valid_for(epoch)?;
        Ok(descriptor)
    }
}
