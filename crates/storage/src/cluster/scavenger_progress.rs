// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, per-PG progress across an entire reference-discovery pass. A pending
//! slot replacement can restart native keyset scans; ordinary pages cannot.
//! Remember only retired pending subjects, not every potentially unbounded key.

use crate::types::{ShardScavengerReferenceCursor as Cursor, ShardScavengerReferencePage};
use crate::{ClusterEpoch, PgId, StoreError};

const MAX_PENDING_RESTARTS: usize = 8;

// Native SQL phase/key order only. Provenance is validated separately: it must
// never make a replaced subject look like a later key within the same snapshot.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Position<'a> {
    Object(&'a str, &'a str, u64, u32),
    Stream(&'a str, u32),
    Part(&'a str, &'a str, &'a str, u32, u32),
    ObjectReclaim(&'a str, &'a str, u64, u32),
    PartReclaim(&'a str, &'a str, u64, u32, u32),
    PendingPlaced(u32),
    PendingReclaim(u32),
    PendingStaging,
    Staging(&'a str, u32),
}

fn position(cursor: &Cursor) -> Position<'_> {
    match cursor {
        Cursor::ObjectSegment {
            bucket,
            key,
            version_id,
            segment_index,
        } => Position::Object(bucket.as_str(), key.as_str(), *version_id, *segment_index),
        Cursor::StreamUploadSegment {
            session_id,
            segment_index,
        } => Position::Stream(session_id.as_str(), *segment_index),
        Cursor::MultipartPartSegment {
            bucket,
            key,
            upload_id,
            part_number,
            segment_index,
        } => Position::Part(
            bucket.as_str(),
            key.as_str(),
            upload_id.as_str(),
            *part_number,
            *segment_index,
        ),
        Cursor::ObjectReclaimSegment {
            bucket,
            key,
            generation_id,
            segment_index,
        } => Position::ObjectReclaim(
            bucket.as_str(),
            key.as_str(),
            generation_id.get(),
            *segment_index,
        ),
        Cursor::MultipartReclaimSegment {
            bucket,
            key,
            generation_id,
            part_number,
            segment_index,
        } => Position::PartReclaim(
            bucket.as_str(),
            key.as_str(),
            generation_id.get(),
            *part_number,
            *segment_index,
        ),
        Cursor::PendingPlacedCommand {
            reference_index, ..
        } => Position::PendingPlaced(*reference_index),
        Cursor::PendingReclaimCommand {
            reference_index, ..
        } => Position::PendingReclaim(*reference_index),
        Cursor::PendingStagingWindow { .. } => Position::PendingStaging,
        Cursor::StagingWindow {
            attempt,
            window_index,
            ..
        } => Position::Staging(attempt.as_str(), *window_index),
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingSubject {
    epoch: ClusterEpoch,
    pg: PgId,
    index: u64,
    checksum: u64,
    staging: Option<(u64, [u8; 32])>,
}

impl PendingSubject {
    fn from_cursor(cursor: &Cursor) -> Option<Self> {
        match cursor {
            Cursor::PendingPlacedCommand {
                cluster_epoch,
                pg_id,
                log_index,
                command_checksum,
                ..
            }
            | Cursor::PendingReclaimCommand {
                cluster_epoch,
                pg_id,
                log_index,
                command_checksum,
                ..
            }
            | Cursor::PendingStagingWindow {
                cluster_epoch,
                pg_id,
                log_index,
                command_checksum,
                ..
            } => Some(Self {
                epoch: *cluster_epoch,
                pg: *pg_id,
                index: *log_index,
                checksum: *command_checksum,
                staging: match cursor {
                    Cursor::PendingStagingWindow {
                        protection_checksum,
                        topology_digest,
                        ..
                    } => Some((*protection_checksum, *topology_digest)),
                    _ => None,
                },
            }),
            _ => None,
        }
    }

    fn same_snapshot(self, other: Self) -> bool {
        self.epoch == other.epoch
            && self.pg == other.pg
            && self.index == other.index
            && self.checksum == other.checksum
            && match (self.staging, other.staging) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            }
    }
}

fn advances(previous: &Cursor, next: &Cursor) -> bool {
    if let (Some(a), Some(b)) = (
        PendingSubject::from_cursor(previous),
        PendingSubject::from_cursor(next),
    ) {
        if !a.same_snapshot(b) {
            return false;
        }
    }
    if let (
        Cursor::StagingWindow {
            pg_id: a_pg,
            topology_digest: a_topology,
            attempt: a_attempt,
            introduction_epoch: a_epoch,
            introduction_index: a_index,
            ..
        },
        Cursor::StagingWindow {
            pg_id: b_pg,
            topology_digest: b_topology,
            attempt: b_attempt,
            introduction_epoch: b_epoch,
            introduction_index: b_index,
            ..
        },
    ) = (previous, next)
    {
        if a_pg != b_pg
            || a_topology != b_topology
            || (a_attempt == b_attempt && (a_epoch, a_index) != (b_epoch, b_index))
        {
            return false;
        }
    }
    position(next) > position(previous)
}

fn invalid(reason: &'static str) -> StoreError {
    StoreError::ShardScavengerScanIncomplete {
        context: "reference page progress",
        errors: reason.to_owned(),
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ReferenceProgress {
    retired: Vec<PendingSubject>,
    invalid: bool,
}

impl ReferenceProgress {
    pub(super) fn is_invalid(&self) -> bool {
        self.invalid
    }

    pub(super) fn require_active(&self) -> Result<(), StoreError> {
        if self.invalid {
            Err(invalid("audit pass requires fresh discovery"))
        } else {
            Ok(())
        }
    }

    /// Preview without advancing cursor, retirement history or reference sets.
    /// Commit the returned state only after the whole page has been validated.
    pub(super) fn preview(
        &mut self,
        after: Option<&Cursor>,
        page: &ShardScavengerReferencePage,
    ) -> Result<Self, StoreError> {
        let result = self.check(after, page);
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    fn check(
        &self,
        after: Option<&Cursor>,
        page: &ShardScavengerReferencePage,
    ) -> Result<Self, StoreError> {
        self.require_active()?;
        if page.items.len() > usize::from(crate::types::SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT)
            || (!page.complete && page.items.is_empty())
        {
            return Err(invalid("empty or oversized incomplete page"));
        }
        let mut next = self.clone();
        if let (Some(previous), Some(first)) = (after, page.items.first()) {
            if !advances(previous, &first.cursor) {
                let Some(retired) = PendingSubject::from_cursor(previous) else {
                    return Err(invalid("reference cursor did not advance"));
                };
                if previous == &first.cursor {
                    return Err(invalid("reference cursor did not advance"));
                }
                if next.retired.len() == MAX_PENDING_RESTARTS {
                    return Err(invalid("pending slot restart budget exhausted"));
                }
                // Only pending cursors can cause the store to rescan earlier
                // phases. The replaced identity must not reappear afterward.
                next.retired.push(retired);
            }
        }
        for pair in page.items.windows(2) {
            if !advances(&pair[0].cursor, &pair[1].cursor) {
                return Err(invalid("reference page is not strictly ordered"));
            }
        }
        for item in &page.items {
            if let Some(subject) = PendingSubject::from_cursor(&item.cursor) {
                if next
                    .retired
                    .iter()
                    .any(|retired| retired.same_snapshot(subject))
                {
                    return Err(invalid("retired pending cursor reappeared"));
                }
            }
        }
        Ok(next)
    }
}

#[cfg(test)]
#[path = "scavenger_progress_tests.rs"]
mod tests;
