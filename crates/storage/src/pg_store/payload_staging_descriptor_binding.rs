// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Command-bound descriptor views for the coordinated command-field integration.
//! A decoded descriptor alone has neither a checked command subject nor an
//! actual topology. Bind both before projecting pending protection or applying.
//! These views are descriptive, not acceptance/recovery/write authority. Live
//! responsibility is revalidated inside application, never cached by binding.
//! Production command fields and pending-sidecar wiring remain to be activated
//! together with frontend custody; this module does not enable a fallback path.

use super::*;
use crate::metadata_command::MetadataCommandPayload;
use crate::node_runtime::pg_store::{
    PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
};
use crate::types::{
    ShardScavengerPayloadReference, ShardScavengerProtectedShardSetReference,
    ShardScavengerStagingProtection,
};
use crate::StreamUploadTarget;

pub(in super::super::super) struct BoundIntroductionDescriptor<'a> {
    descriptor: &'a IntroductionDescriptor,
    command: &'a MetadataCommandEnvelope,
    topology: &'a PgTopology,
}

pub(in super::super::super) struct BoundAppendDescriptor<'a> {
    descriptor: &'a AppendDescriptor,
    command: &'a MetadataCommandEnvelope,
    topology: &'a PgTopology,
}

/// Future/unselected scope only: deliberately cannot be projected as a placed
/// payload with actual lengths, CRC or integrity witness. No database, current
/// route, clock or physical effect is consulted to derive these dependencies.
pub(in super::super::super) struct PendingStagingProtection<'a> {
    subject: &'a StagingSubject,
    window: &'a StagingWindow,
    command: &'a MetadataCommandEnvelope,
    creation_release: Option<&'a StagingBucketResource>,
}

impl IntroductionDescriptor {
    pub(in super::super::super) fn bind<'a>(
        &'a self,
        topology: &'a PgTopology,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<BoundIntroductionDescriptor<'a>, introduction::IntroductionError> {
        self.0.validate(topology)?;
        introduction::validate_command(command, &self.0)?;
        if self.0.windows[0].participants.placement_epoch > command.id().cluster_epoch() {
            return Err(introduction::IntroductionError::CommandBinding);
        }
        Ok(BoundIntroductionDescriptor {
            descriptor: self,
            command,
            topology,
        })
    }
}

impl BoundIntroductionDescriptor<'_> {
    pub(in super::super) fn inputs(
        &self,
    ) -> (&PgTopology, &MetadataCommandEnvelope, &StagingOwnerRecord) {
        (self.topology, self.command, &self.descriptor.0)
    }

    pub(in super::super::super) fn pending_protection(&self) -> PendingStagingProtection<'_> {
        let owner = &self.descriptor.0;
        let creation_release = match &owner.resources.creation {
            ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) => {
                proof
            }
            ResourceResponsibility::NoResource => unreachable!("validated introduction"),
        };
        PendingStagingProtection {
            subject: &owner.subject,
            window: &owner.windows[0],
            command: self.command,
            creation_release: Some(creation_release),
        }
    }

    pub(in super::super::super) fn apply_in_open_transaction(
        self,
        pg: &PgStore,
    ) -> Result<introduction::IntroductionApplied, introduction::IntroductionError> {
        introduction::apply_in_open_transaction(pg, self)
    }
}

impl AppendDescriptor {
    pub(in super::super::super) fn bind<'a>(
        &'a self,
        topology: &'a PgTopology,
        command: &'a MetadataCommandEnvelope,
    ) -> Result<BoundAppendDescriptor<'a>, append::AppendError> {
        self.validate()
            .map_err(|_| append::AppendError::CommandBinding)?;
        let subject = &self.subject;
        let window = &self.window;
        if topology.object_pg_for(&subject.bucket, &subject.key) != subject.object_pg.get()
            || subject.data_pg(topology, window.index) != window.participants.data_pg.get()
        {
            return Err(StagingInvariant::Placement.into());
        }
        let MetadataCommandPayload::AppendStreamSegment(append) = command.payload() else {
            return Err(append::AppendError::CommandBinding);
        };
        let target_matches = match (&subject.target, &append.target) {
            (StagingTarget::StreamPut, StreamUploadTarget::PutObject) => true,
            (
                StagingTarget::UploadPart {
                    upload_id,
                    part_number,
                    ..
                },
                StreamUploadTarget::UploadPart {
                    upload_id: actual_upload,
                    part_number: actual_part,
                },
            ) => upload_id == actual_upload && part_number == actual_part,
            _ => false,
        };
        let segment = &append.segment;
        if !command.verify_checksum()
            || !command_follows(subject.id.introduction, command.id())
            || !target_matches
            || append.bucket != subject.bucket
            || append.key != subject.key
            || segment.session_id != subject.id.attempt
            || segment.segment_index.checked_add(1) != Some(window.index)
            || segment.segment_vid.get().checked_add(1) != Some(window.start.get())
            || segment.segment_okh != subject.segment_hash(segment.segment_index)
            || segment.data_pg_id != subject.data_pg(topology, segment.segment_index)
            || segment.placement_cluster_epoch < subject.originating_epoch
            || segment.placement_cluster_epoch > command.id().cluster_epoch()
            || window.participants.placement_epoch > command.id().cluster_epoch()
        {
            return Err(append::AppendError::CommandBinding);
        }
        Ok(BoundAppendDescriptor {
            descriptor: self,
            command,
            topology,
        })
    }
}

impl BoundAppendDescriptor<'_> {
    pub(in super::super) fn inputs(
        &self,
    ) -> (
        &PgTopology,
        &MetadataCommandEnvelope,
        &StagingSubject,
        &StagingWindow,
    ) {
        (
            self.topology,
            self.command,
            &self.descriptor.subject,
            &self.descriptor.window,
        )
    }

    pub(in super::super::super) fn pending_protection(&self) -> PendingStagingProtection<'_> {
        PendingStagingProtection {
            subject: &self.descriptor.subject,
            window: &self.descriptor.window,
            command: self.command,
            // The already materialized owner independently retains creation
            // custody and its release route; an append cannot introduce it.
            creation_release: None,
        }
    }

    pub(in super::super::super) fn apply_in_open_transaction(
        self,
        pg: &PgStore,
    ) -> Result<append::AppendApplied, append::AppendError> {
        append::apply_in_open_transaction(pg, self)
    }
}

/// Raw fixture descriptions only, never unchecked application. Kept test-only
/// because component tests also need malformed input and old-window replay;
/// neither is a fresh primary capture. Both adapters use the production binder
/// and the same bound-type-only transaction boundary.
#[cfg(test)]
pub(in super::super) fn apply_introduction_for_test(
    pg: &PgStore,
    topology: &PgTopology,
    command: &MetadataCommandEnvelope,
    owner: &StagingOwnerRecord,
) -> Result<introduction::IntroductionApplied, introduction::IntroductionError> {
    IntroductionDescriptor(owner.clone())
        .bind(topology, command)?
        .apply_in_open_transaction(pg)
}

#[cfg(test)]
pub(in super::super) fn apply_append_for_test(
    pg: &PgStore,
    topology: &PgTopology,
    command: &MetadataCommandEnvelope,
    next: &StagingWindow,
) -> Result<append::AppendApplied, append::AppendError> {
    let MetadataCommandPayload::AppendStreamSegment(append) = command.payload() else {
        return Err(append::AppendError::CommandBinding);
    };
    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &append.segment.session_id)?
        .ok_or(append::AppendError::LiveResponsibility)?;
    AppendDescriptor {
        subject: owner.subject,
        window: next.clone(),
    }
    .bind(topology, command)?
    .apply_in_open_transaction(pg)
}

impl PendingStagingProtection<'_> {
    pub(in super::super::super) fn reference(&self) -> ShardScavengerPayloadReference {
        ShardScavengerPayloadReference::StagingProtection(ShardScavengerStagingProtection {
            identity: ShardScavengerProtectedShardSetReference {
                data_pg_id: self.window.participants.data_pg.get(),
                okh: self.window.segment_hash,
                generation_id: self.window.start,
                ec: self.window.geometry.ec,
            },
            placement_epoch: self.window.participants.placement_epoch,
            file_nodes: self.window.participants.files.clone(),
            acknowledgement_primary: self.window.participants.acknowledgement_primary,
        })
    }

    pub(in super::super::super) fn historical_routes(
        &self,
    ) -> BTreeSet<PgClusterMapHistoryRouteReference> {
        let reference = |epoch, pg| {
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
                epoch,
                pg,
            )
        };
        let mut routes = BTreeSet::from([
            reference(self.command.id().cluster_epoch(), self.subject.object_pg),
            reference(self.subject.originating_epoch, self.subject.object_pg),
            reference(
                self.window.participants.placement_epoch,
                self.window.participants.data_pg,
            ),
        ]);
        if let Some(creation) = self.creation_release {
            routes.insert(reference(
                creation.identity.cluster_epoch,
                creation.release_pg,
            ));
        }
        routes
    }
}
