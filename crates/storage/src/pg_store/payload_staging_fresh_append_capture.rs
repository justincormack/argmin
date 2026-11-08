// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh append lookahead from the current, prepared owner under the primary's
//! SQL snapshot. No caller-supplied owner/window and no writes. Unlike replica
//! replay this cannot recapture an already selected window or use an old issuer.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;

impl StagingCapture<'_> {
    /// The builder holds primary serialization and its enclosing SQL snapshot.
    /// The result is only a descriptor: no acceptance or physical authority is
    /// granted until the coordinated command/custody integration is activated.
    pub(in super::super::super) fn append_for_command(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        envelope: &MetadataCommandEnvelope,
    ) -> Result<descriptor::AppendDescriptor, StagingCaptureError> {
        self.require_fresh_command(pg, node_id, envelope)?;
        let MetadataCommandPayload::AppendStreamSegment(command) = envelope.payload() else {
            return Err(StagingCaptureError::Command);
        };
        let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &command.segment.session_id)?
            .ok_or(append::AppendError::LiveResponsibility)?;
        let checked = owner.validate(self.topology)?;
        let current = owner.windows.last().ok_or(StagingInvariant::Windows)?;
        // Fresh capture only follows the prepared, not-yet-selected tail. Exact
        // retries must retain the original descriptor, never refresh its cutoff.
        if command.segment.segment_index != current.index
            || command.segment.segment_index as usize != owner.references.len()
        {
            return Err(append::AppendError::SelectedPrefix.into());
        }
        owner.selected_stream_reference(&command.segment)?;
        let epoch = self.effect_fence.cluster_epoch();
        let placement_generation = match owner.subject.target {
            StagingTarget::StreamPut => owner.subject.payload_generation,
            StagingTarget::UploadPart {
                upload_generation, ..
            } => upload_generation,
            StagingTarget::DirectPut => return Err(StagingInvariant::Lifecycle.into()),
        };
        self.placement.require_stream_append_placement(
            epoch,
            &owner.subject.bucket,
            &owner.subject.key,
            &command.target,
            placement_generation,
            &command.segment,
        )?;
        let (ack_primary, files) = self.placement.capture_segment_participants(
            epoch,
            current.participants.data_pg.get(),
            current.geometry.ec,
            &current.segment_hash,
            current.start,
        )?;
        if ack_primary != current.participants.acknowledgement_primary
            || !files.iter().map(|file| file.node_id()).eq(current
                .participants
                .files
                .iter()
                .copied())
        {
            return Err(StagingInvariant::Placement.into());
        }
        let next = self.next_window(pg, node_id, &owner)?;
        let session = append::validate_live(pg, &owner, command, &next)?;
        if session.next_segment_vid != current.end {
            return Err(append::AppendError::LiveResponsibility.into());
        }
        let descriptor = descriptor::AppendDescriptor::capture(&checked, next)?;
        descriptor.bind(self.topology, envelope)?;
        pg.require_live_allocation_lifetime()?;
        self.effect_fence.require_valid_for(epoch)?;
        Ok(descriptor)
    }
}
