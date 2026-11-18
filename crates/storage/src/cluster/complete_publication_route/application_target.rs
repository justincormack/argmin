// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A member borrowed from admitted embedded or remote primary custody.
//! Embedded-primary replicas share its PG lock: taking it again would deadlock.
//! Historical embedded targets retain the exact report and eligible placement.
//! Remote-primary targets retain the exact session and original admitted cohort.

use super::historical_remote::HistoricalRemoteApplicationTarget;
use super::*;
use crate::cluster::complete_fanout_route::RemotePrimaryEmbeddedTarget;

pub(crate) struct EmbeddedApplicationTarget<'a> {
    authority: ApplicationTargetAuthority<'a>,
}

enum ApplicationTargetAuthority<'a> {
    Embedded {
        section: &'a EmbeddedPublicationSection<'a>,
        node_id: NodeId,
        runtime: &'a LocalNodeRuntime,
    },
    Remote(RemotePrimaryEmbeddedTarget<'a>),
    HistoricalRemote(HistoricalRemoteApplicationTarget<'a>),
}

impl EmbeddedPublicationSection<'_> {
    pub(crate) fn application_target(
        &self,
        node_id: NodeId,
    ) -> Result<EmbeddedApplicationTarget<'_>, StoreError> {
        let member = self.application_member(node_id)?;
        Ok(EmbeddedApplicationTarget {
            authority: ApplicationTargetAuthority::Embedded {
                section: self,
                node_id,
                runtime: member.embedded_publication_runtime()?,
            },
        })
    }
}

impl<'a> EmbeddedApplicationTarget<'a> {
    pub(crate) fn require_command(
        &self,
        id: MetadataCommandId,
        checksum_crc64: u64,
        command_sha256: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_effect()?;
        match &self.authority {
            ApplicationTargetAuthority::Embedded { section, .. } => {
                section.require_command(id, checksum_crc64, command_sha256)
            }
            ApplicationTargetAuthority::HistoricalRemote(target) => {
                target.require_command(id, checksum_crc64, command_sha256)
            }
            ApplicationTargetAuthority::Remote(target) => {
                target.require_command(id, checksum_crc64, command_sha256)
            }
        }
    }

    pub(crate) fn from_remote_primary(target: RemotePrimaryEmbeddedTarget<'a>) -> Self {
        Self {
            authority: ApplicationTargetAuthority::Remote(target),
        }
    }

    pub(super) fn from_historical_remote(target: HistoricalRemoteApplicationTarget<'a>) -> Self {
        Self {
            authority: ApplicationTargetAuthority::HistoricalRemote(target),
        }
    }

    pub(crate) fn require_effect(&self) -> Result<(), StoreError> {
        match &self.authority {
            ApplicationTargetAuthority::Embedded { section, .. } => section.require_effect(),
            ApplicationTargetAuthority::Remote(target) => target.require_effect(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.require_effect(),
        }
    }
    pub(crate) fn pg_id(&self) -> PgId {
        match &self.authority {
            ApplicationTargetAuthority::Embedded { section, .. } => section.pg_id(),
            ApplicationTargetAuthority::Remote(target) => target.pg_id(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.pg_id(),
        }
    }
    pub(crate) fn node_id(&self) -> NodeId {
        match &self.authority {
            ApplicationTargetAuthority::Embedded { node_id, .. } => *node_id,
            ApplicationTargetAuthority::Remote(target) => target.node_id(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.node_id(),
        }
    }
    pub(crate) fn epoch(&self) -> ClusterEpoch {
        match &self.authority {
            ApplicationTargetAuthority::Embedded { section, .. } => section.epoch(),
            ApplicationTargetAuthority::Remote(target) => target.epoch(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.epoch(),
        }
    }
    pub(crate) fn deadline(&self) -> Instant {
        match &self.authority {
            ApplicationTargetAuthority::Embedded { section, .. } => section.deadline(),
            ApplicationTargetAuthority::Remote(target) => target.deadline(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.deadline(),
        }
    }
    pub(crate) fn runtime(&self) -> Result<&LocalNodeRuntime, StoreError> {
        self.require_effect()?;
        match &self.authority {
            ApplicationTargetAuthority::Embedded { runtime, .. } => Ok(runtime),
            ApplicationTargetAuthority::Remote(target) => target.runtime(),
            ApplicationTargetAuthority::HistoricalRemote(target) => target.runtime(),
        }
    }
}
