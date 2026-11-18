// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete historical cohort, not the eligible subset used by one receiver.
//! Only authenticated recovery-map validation can mint this subject.

use super::*;

pub(in crate::cluster) struct HistoricalFanoutSubject<'a> {
    admission: &'a StorageClusterRouteAdmission,
    subject: HistoricalApplicationSubject,
}

impl StorageClusterRouteAdmission {
    pub(in crate::cluster) fn historical_fanout_subject(
        &self,
        map: &ClusterRuntimeMapSnapshot,
        pg: PgId,
    ) -> Result<HistoricalFanoutSubject<'_>, StoreError> {
        let subject = self.historical_application_subject(map, pg)?;
        // Filtering is appropriate for individual receiver admission, never for
        // deciding that publication has reached every required historical peer.
        if subject.members != subject.required_members {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation:
                    "historical fanout requires Peering placement for the complete Active cohort",
            });
        }
        Ok(HistoricalFanoutSubject {
            admission: self,
            subject,
        })
    }
}

impl HistoricalFanoutSubject<'_> {
    pub(in crate::cluster) fn primary(&self) -> NodeId {
        self.subject.primary
    }
    pub(in crate::cluster) fn epoch(&self) -> ClusterEpoch {
        self.subject.pending.cluster_epoch()
    }
    pub(in crate::cluster) fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.subject.required_members.iter().copied()
    }

    pub(in crate::cluster) fn require_command(
        &self,
        id: MetadataCommandId,
        crc: u64,
        sha: [u8; 32],
    ) -> Result<(), StoreError> {
        let pending = self.subject.pending;
        if id.pg_id() != self.subject.pg_id
            || id.cluster_epoch() != self.epoch()
            || id.log_index().get() != pending.log_index()
            || crc != pending.command_checksum()
            || sha != pending.command_sha256()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "historical fanout command differs from admitted recovery report",
            });
        }
        Ok(())
    }

    pub(in crate::cluster) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        section.require_effect()?;
        if !std::ptr::eq(section.route.admission, self.admission)
            || section.pg_id() != self.subject.pg_id
            || section.node_id() != self.primary()
            || !matches!(&section.route.subject, PublicationSubject::Historical { pending, application_members }
                if *pending == self.subject.pending && *application_members == self.subject.required_members)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation:
                    "historical fanout requires its original admitted primary section and report",
            });
        }
        Ok(())
    }
}
