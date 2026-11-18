// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded protection for one pending introduction/lookahead window. This is a
//! descriptive index, not readable payload or write/deletion authority. Only a
//! command-bound descriptor can construct a nonempty index. Production commands
//! currently initialize empty indexes; nonempty descriptor dispatch must land
//! with the coordinated custody change. Scavenger paging consumes this index.

use rusqlite::params;

use super::{payload_staging::PendingStagingProtection, PgStore};
use crate::error::StoreError;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
};
use crate::types::{
    ShardScavengerPayloadReference, ShardScavengerReferenceCursor, ShardScavengerReferencePageItem,
};
use crate::types::{ShardScavengerProtectedShardSetReference, ShardScavengerStagingProtection};
use crate::{ClusterEpoch, EcShape, GenerationId, NodeId, PgId, PgTopology, SessionId};

// Versioned by the PG schema: 126 fixed bytes plus up to 32 u32 file owners.
const MAX_BYTES: usize = 254;
const DOMAIN: &[u8] = b"argmin.metadata.pending-staging-protection.v1";

pub(super) const SCHEMA: &str = "\
CREATE TABLE metadata_command_pending_staging_protection (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    command_checksum INTEGER NOT NULL,
    encoded_protection BLOB NOT NULL CHECK (length(encoded_protection) <= 254),
    protection_checksum INTEGER NOT NULL,
    FOREIGN KEY (singleton) REFERENCES metadata_command_pending_slot(singleton) ON DELETE CASCADE
) STRICT";

fn invalid() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "pending-staging-protection-index",
    }
}

fn topology_digest(topology: &PgTopology) -> [u8; 32] {
    let mut bytes = b"pending-staging-protection-topology-v1".to_vec();
    topology
        .for_each_pg::<std::convert::Infallible>(|pg| {
            bytes.extend_from_slice(&pg.to_be_bytes());
            Ok(())
        })
        .expect("infallible topology iteration");
    checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &bytes)
        .bytes()
        .try_into()
        .expect("SHA-256 length")
}

fn checksum(id: MetadataCommandId, command_checksum: u64, bytes: &[u8]) -> u64 {
    let mut hash = checksum::crc64::Hasher::new();
    hash.update(DOMAIN);
    for value in [
        id.cluster_epoch().get(),
        u64::from(id.pg_id().get()),
        id.log_index().get(),
        command_checksum,
        bytes.len() as u64,
    ] {
        hash.update(&value.to_be_bytes());
    }
    hash.update(bytes);
    hash.finalize()
}

fn require_existing(
    pg: &PgStore,
    id: MetadataCommandId,
    command_checksum: u64,
    bytes: &[u8],
) -> Result<(), StoreError> {
    if pg.conn.is_autocommit() || pg.pg_id() != id.pg_id().get() {
        return Err(invalid());
    }
    // Equality is checked in SQL: no unbounded malformed blob is loaded. Even
    // an empty index must exist and bind the exact command position/checksum.
    let matches: bool = pg.query_row_cached(
        "SELECT EXISTS(SELECT 1 FROM metadata_command_pending_staging_protection WHERE singleton = 0 AND command_checksum = ?1 AND encoded_protection = ?2 AND protection_checksum = ?3)",
        params![command_checksum as i64, bytes, checksum(id, command_checksum, bytes) as i64],
        "check exact pending staging protection", |row| row.get(0),
    )?;
    if matches {
        Ok(())
    } else {
        Err(invalid())
    }
}

/// No public fields or raw-byte constructor: only the bound projection mints
/// a nonempty value. Borrow the exact command, not a caller-selected ID/CRC.
pub(super) struct PendingStagingIndex<'a> {
    command: &'a MetadataCommandEnvelope,
    topology: &'a PgTopology,
    bytes: Vec<u8>,
}

impl<'a> PendingStagingIndex<'a> {
    pub(super) fn require_existing(&self, pg: &PgStore) -> Result<(), StoreError> {
        require_existing(
            pg,
            self.command.id(),
            self.command.checksum_crc64(),
            &self.bytes,
        )
    }

    pub(super) fn from_bound(protection: &'a PendingStagingProtection<'_>) -> Self {
        let (command, topology, subject, window) = protection.index_inputs();
        Self {
            command,
            topology,
            bytes: encode_window(topology, subject, window),
        }
    }

    /// The caller owns the slot transaction. This never opens/commits a new
    /// transaction, and rejects substitution before touching the index.
    pub(super) fn install_in_open_transaction(
        &self,
        pg: &PgStore,
        topology: &PgTopology,
    ) -> Result<(), StoreError> {
        if self.topology != topology {
            return Err(invalid());
        }
        pg.install_exact_pending_staging_index(
            self.command.id(),
            self.command.checksum_crc64(),
            &self.command.command_bytes(),
            &self.bytes,
        )
    }
}

fn encode_window(
    topology: &PgTopology,
    subject: &super::payload_staging::StagingSubject,
    window: &super::payload_staging::StagingWindow,
) -> Vec<u8> {
    let mut bytes = topology_digest(topology).to_vec();
    bytes.extend_from_slice(subject.id.attempt.as_str().as_bytes());
    bytes.extend_from_slice(&subject.id.introduction.cluster_epoch().get().to_be_bytes());
    bytes.extend_from_slice(&subject.id.introduction.log_index().get().to_be_bytes());
    bytes.extend_from_slice(&window.index.to_be_bytes());
    bytes.extend_from_slice(&window.participants.data_pg.get().to_be_bytes());
    bytes.extend_from_slice(&window.segment_hash);
    bytes.extend_from_slice(&window.start.get().to_be_bytes());
    bytes.extend_from_slice(&window.participants.placement_epoch.get().to_be_bytes());
    bytes.extend([window.geometry.ec.k, window.geometry.ec.m]);
    bytes.extend_from_slice(
        &window
            .participants
            .acknowledgement_primary
            .as_u32()
            .to_be_bytes(),
    );
    for node in &window.participants.files {
        bytes.extend_from_slice(&node.as_u32().to_be_bytes());
    }
    debug_assert!(bytes.len() <= MAX_BYTES);
    bytes
}

impl PgStore {
    fn install_exact_pending_staging_index(
        &self,
        id: MetadataCommandId,
        command_checksum: u64,
        command_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        if self.conn.is_autocommit() || self.pg_id() != id.pg_id().get() {
            return Err(invalid());
        }
        let matches = self.query_row_cached_optional(
            "SELECT 1 FROM metadata_command_pending_slot WHERE singleton = 0 AND cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3 AND command_checksum = ?4 AND command_bytes = ?5",
            params![id.cluster_epoch().get() as i64, i64::from(self.pg_id()), id.log_index().get() as i64, command_checksum as i64, command_bytes],
            "bind pending staging protection to exact slot", |_| Ok(()),
        )?.is_some();
        if !matches {
            return Err(invalid());
        }
        self.write_pending_staging_index(id, command_checksum, bytes)
    }

    pub(super) fn require_complete_pending_staging_index(
        &self,
        projection: &super::payload_staging::CompletePendingIndexes<'_>,
    ) -> Result<(), StoreError> {
        projection.require_store(self)?;
        let (topology, window) = projection.index_inputs();
        let bytes = window.map_or_else(Vec::new, |(subject, window)| {
            encode_window(topology, subject, window)
        });
        let command = projection.command();
        require_existing(self, command.id(), command.checksum_crc64(), &bytes)
    }

    pub(super) fn install_complete_pending_staging_index(
        &self,
        projection: &super::payload_staging::CompletePendingProjection<'_>,
    ) -> Result<(), StoreError> {
        projection.require_store(self)?;
        let (topology, window) = projection.index_inputs();
        let bytes = window.map_or_else(Vec::new, |(subject, window)| {
            encode_window(topology, subject, window)
        });
        let command = projection.command();
        self.install_exact_pending_staging_index(
            command.id(),
            command.checksum_crc64(),
            command.bytes(),
            &bytes,
        )
    }
}

/// Validated read evidence for slot-bound paging; never re-installable.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PendingStagingRead {
    pub(super) command: MetadataCommandId,
    pub(super) command_checksum: u64,
    protection_checksum: u64,
    pub(super) window: Option<PendingStagingWindow>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct PendingStagingWindow {
    pub(super) attempt: SessionId,
    pub(super) introduction: MetadataCommandId,
    pub(super) window_index: u32,
    pub(super) protection: ShardScavengerStagingProtection,
}

impl PgStore {
    pub(super) fn require_empty_pending_staging_index(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        require_existing(self, command.id(), command.checksum_crc64(), &[])
    }

    // All calls are made in the enclosing scavenger read snapshot. A cursor
    // from another topology/PG is invalid, not evidence of slot replacement.
    pub(super) fn pending_staging_cursor_matches(
        &self,
        topology: &PgTopology,
        cursor: &ShardScavengerReferenceCursor,
    ) -> Result<bool, StoreError> {
        let ShardScavengerReferenceCursor::PendingStagingWindow {
            pg_id,
            topology_digest: digest,
            cluster_epoch,
            log_index,
            ..
        } = cursor
        else {
            return Err(invalid());
        };
        if pg_id.get() != self.pg_id()
            || *digest != topology_digest(topology)
            || cluster_epoch.get() > i64::MAX as u64
            || *log_index == 0
            || *log_index > i64::MAX as u64
        {
            return Err(invalid());
        }
        Ok(self
            .pending_staging_protection(topology)?
            .is_some_and(|read| read.window.is_some() && read.cursor(topology) == *cursor))
    }

    pub(super) fn pending_staging_page_item(
        &self,
        topology: &PgTopology,
    ) -> Result<Option<ShardScavengerReferencePageItem>, StoreError> {
        let Some(read) = self.pending_staging_protection(topology)? else {
            return Ok(None);
        };
        let cursor = read.cursor(topology);
        Ok(read.window.map(|window| ShardScavengerReferencePageItem {
            cursor,
            reference: ShardScavengerPayloadReference::StagingProtection(window.protection),
        }))
    }

    // Called only beside fresh/replacement slot writes. Once command fields
    // activate, projection replaces this empty initializer for those variants.
    pub(super) fn initialize_pending_staging_index(
        &self,
        id: MetadataCommandId,
        command_checksum: u64,
    ) -> Result<(), StoreError> {
        self.write_pending_staging_index(id, command_checksum, &[])
    }

    fn write_pending_staging_index(
        &self,
        id: MetadataCommandId,
        command_checksum: u64,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        if self.conn.is_autocommit() || id.pg_id().get() != self.pg_id() || bytes.len() > MAX_BYTES
        {
            return Err(invalid());
        }
        self.execute_cached(
            "INSERT INTO metadata_command_pending_staging_protection (singleton, command_checksum, encoded_protection, protection_checksum) VALUES (0, ?1, ?2, ?3) ON CONFLICT(singleton) DO UPDATE SET command_checksum = excluded.command_checksum, encoded_protection = excluded.encoded_protection, protection_checksum = excluded.protection_checksum",
            params![command_checksum as i64, bytes, checksum(id, command_checksum, bytes) as i64],
            "write pending staging protection index",
        )?;
        Ok(())
    }

    pub(super) fn pending_staging_protection(
        &self,
        topology: &PgTopology,
    ) -> Result<Option<PendingStagingRead>, StoreError> {
        let row = self.query_row_cached_optional(
            "SELECT p.cluster_epoch, p.pg_id, p.log_index, p.command_checksum, i.command_checksum, CASE WHEN length(i.encoded_protection) <= 254 THEN i.encoded_protection ELSE NULL END, i.protection_checksum FROM metadata_command_pending_slot p LEFT JOIN metadata_command_pending_staging_protection i ON p.singleton = i.singleton WHERE p.singleton = 0",
            [], "read pending staging protection index", |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, Option<i64>>(4)?, row.get::<_, Option<Vec<u8>>>(5)?, row.get::<_, Option<i64>>(6)?)),
        )?;
        let Some((epoch, pg, index, command_checksum, indexed_checksum, bytes, stored_checksum)) =
            row
        else {
            return Ok(None);
        };
        let id = MetadataCommandId::new(
            ClusterEpoch::new(epoch.try_into().map_err(|_| invalid())?).ok_or_else(invalid)?,
            PgId::new(pg.try_into().map_err(|_| invalid())?),
            MetadataCommandLogIndex::new(index.try_into().map_err(|_| invalid())?)
                .ok_or_else(invalid)?,
        );
        if id.pg_id().get() != self.pg_id()
            || !topology.contains_pg(id.pg_id())
            || indexed_checksum != Some(command_checksum)
        {
            return Err(invalid());
        }
        let bytes = bytes.ok_or_else(invalid)?;
        if stored_checksum != Some(checksum(id, command_checksum as u64, &bytes) as i64) {
            return Err(invalid());
        }
        let window = if bytes.is_empty() {
            None
        } else {
            Some(decode_window(id, topology, &bytes)?)
        };
        Ok(Some(PendingStagingRead {
            command: id,
            command_checksum: command_checksum as u64,
            protection_checksum: stored_checksum.expect("verified checksum") as u64,
            window,
        }))
    }
}

impl PendingStagingRead {
    fn cursor(&self, topology: &PgTopology) -> ShardScavengerReferenceCursor {
        ShardScavengerReferenceCursor::PendingStagingWindow {
            cluster_epoch: self.command.cluster_epoch(),
            pg_id: self.command.pg_id(),
            log_index: self.command.log_index().get(),
            command_checksum: self.command_checksum,
            protection_checksum: self.protection_checksum,
            topology_digest: topology_digest(topology),
        }
    }
}

fn decode_window(
    command: MetadataCommandId,
    topology: &PgTopology,
    bytes: &[u8],
) -> Result<PendingStagingWindow, StoreError> {
    // All slices below are preceded by this fixed-prefix bound; the variable
    // suffix has its own EC/count/length check before allocation.
    if bytes.len() < 126 || bytes.len() > MAX_BYTES || bytes[..32] != topology_digest(topology) {
        return Err(invalid());
    }
    let u64_at = |at| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("bounded prefix"));
    let u32_at = |at| u32::from_be_bytes(bytes[at..at + 4].try_into().expect("bounded prefix"));
    let epoch_at = |at| {
        let epoch = u64_at(at);
        if epoch > i64::MAX as u64 {
            return Err(invalid());
        }
        ClusterEpoch::new(epoch).ok_or_else(invalid)
    };
    let attempt = SessionId::try_from(std::str::from_utf8(&bytes[32..64]).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    let introduction_epoch = epoch_at(64)?;
    let introduction_index = u64_at(72);
    if introduction_index > i64::MAX as u64 {
        return Err(invalid());
    }
    let introduction = MetadataCommandId::new(
        introduction_epoch,
        command.pg_id(),
        MetadataCommandLogIndex::new(introduction_index).ok_or_else(invalid)?,
    );
    if (introduction.cluster_epoch(), introduction.log_index())
        > (command.cluster_epoch(), command.log_index())
    {
        return Err(invalid());
    }
    let data_pg = PgId::new(u32_at(84));
    let generation = u64_at(104);
    let placement_epoch = epoch_at(112)?;
    let ec = EcShape {
        k: bytes[120],
        m: bytes[121],
    };
    ec::EcConfig::new(ec.k, ec.m).map_err(|_| invalid())?;
    let count = usize::from(ec.k) + usize::from(ec.m);
    if !topology.contains_pg(data_pg)
        || generation >= i64::MAX as u64
        || u32_at(80) > crate::payload_limits::MAX_STREAM_SEGMENTS
        || placement_epoch < introduction_epoch
        || placement_epoch > command.cluster_epoch()
        || bytes.len() != 126 + count * 4
    {
        return Err(invalid());
    }
    let file_nodes = bytes[126..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|node| NodeId::new(u32::from_be_bytes(*node)))
        .collect::<Vec<_>>();
    if file_nodes
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != count
    {
        return Err(invalid());
    }
    Ok(PendingStagingWindow {
        attempt,
        introduction,
        window_index: u32_at(80),
        protection: ShardScavengerStagingProtection {
            identity: ShardScavengerProtectedShardSetReference {
                data_pg_id: data_pg.get(),
                okh: bytes[88..104].try_into().expect("bounded prefix"),
                generation_id: GenerationId::new(generation).ok_or_else(invalid)?,
                ec,
            },
            placement_epoch,
            file_nodes,
            acknowledgement_primary: NodeId::new(u32_at(122)),
        },
    })
}
