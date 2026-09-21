// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use checksum::{ChecksumAlgorithm, ChecksumHasher};

use crate::metadata_command::{
    decode_metadata_command_log_entry_header, validate_metadata_command_envelope_bytes,
    MetadataCommandEnvelope, MetadataCommandId,
};
use crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN;
use crate::types::{ClusterEpoch, PgId};

pub(crate) const OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES: usize = 64 * 1024;
pub(super) const OUTAGE_COMMAND_ARTIFACT_MAX_PAGES: usize =
    STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN.div_ceil(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutageCommandArtifactPage {
    pub(crate) pg_id: PgId,
    pub(crate) source_epoch: ClusterEpoch,
    pub(crate) command_id: MetadataCommandId,
    pub(crate) total_length: u32,
    pub(crate) digest: [u8; 32],
    pub(crate) page_index: u16,
    pub(crate) bytes: Vec<u8>,
}

impl OutageCommandArtifactPage {
    #[allow(dead_code)]
    pub(crate) fn for_command(
        command: &MetadataCommandEnvelope,
        source_epoch: ClusterEpoch,
    ) -> Result<Vec<Self>, String> {
        let bytes = command.command_bytes();
        if bytes.is_empty() || bytes.len() > STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN {
            return Err("outage command exceeds the artifact byte bound".into());
        }
        let digest = artifact_digest(&bytes);
        let total_length = bytes.len() as u32;
        let mut pages = Vec::new();
        for (page_index, chunk) in bytes.chunks(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES).enumerate() {
            let page = Self {
                pg_id: command.id().pg_id(),
                source_epoch,
                command_id: command.id(),
                total_length,
                digest,
                page_index: u16::try_from(page_index).expect("bounded page count fits u16"),
                bytes: chunk.to_vec(),
            };
            page.validate()?;
            pages.push(page);
        }
        Ok(pages)
    }

    pub(super) fn key(&self) -> (PgId, ClusterEpoch, ClusterEpoch, u64) {
        (
            self.pg_id,
            self.source_epoch,
            self.command_id.cluster_epoch(),
            self.command_id.log_index().get(),
        )
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.command_id.pg_id() != self.pg_id {
            return Err("outage artifact command PG differs from page PG".into());
        }
        if self.command_id.cluster_epoch() > self.source_epoch {
            return Err("outage artifact command epoch exceeds source route epoch".into());
        }
        let length = self.total_length as usize;
        if length == 0 || length > STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN {
            return Err("outage artifact length is outside the metadata-command bound".into());
        }
        let page_count = length.div_ceil(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES);
        if page_count > OUTAGE_COMMAND_ARTIFACT_MAX_PAGES
            || usize::from(self.page_index) >= page_count
        {
            return Err("outage artifact page index is outside the canonical page range".into());
        }
        let offset = usize::from(self.page_index) * OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES;
        let expected_length = (length - offset).min(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES);
        if self.bytes.len() != expected_length {
            return Err("outage artifact page has noncanonical length".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutageCommandArtifactRecord {
    pub(super) pg_id: PgId,
    pub(super) source_epoch: ClusterEpoch,
    pub(super) command_id: MetadataCommandId,
    pub(super) total_length: u32,
    pub(super) digest: [u8; 32],
    pub(super) pages: Vec<Vec<u8>>,
}

impl OutageCommandArtifactRecord {
    pub(super) fn key(&self) -> (PgId, ClusterEpoch, ClusterEpoch, u64) {
        (
            self.pg_id,
            self.source_epoch,
            self.command_id.cluster_epoch(),
            self.command_id.log_index().get(),
        )
    }
    pub(super) fn from_first_page(page: &OutageCommandArtifactPage) -> Result<Self, String> {
        page.validate()?;
        if page.page_index != 0 {
            return Err("outage artifact must begin with page zero".into());
        }
        let record = Self {
            pg_id: page.pg_id,
            source_epoch: page.source_epoch,
            command_id: page.command_id,
            total_length: page.total_length,
            digest: page.digest,
            pages: vec![page.bytes.clone()],
        };
        record.validate()?;
        Ok(record)
    }

    pub(super) fn append(&mut self, page: &OutageCommandArtifactPage) -> Result<bool, String> {
        page.validate()?;
        if page.pg_id != self.pg_id
            || page.source_epoch != self.source_epoch
            || page.command_id != self.command_id
            || page.total_length != self.total_length
            || page.digest != self.digest
        {
            return Err("outage artifact page conflicts with its immutable manifest".into());
        }
        let index = usize::from(page.page_index);
        if index < self.pages.len() {
            return if self.pages[index] == page.bytes {
                Ok(false)
            } else {
                Err("outage artifact page conflicts with its committed bytes".into())
            };
        }
        if index != self.pages.len() {
            return Err("outage artifact page is not the next contiguous page".into());
        }
        let mut next = self.clone();
        next.pages.push(page.bytes.clone());
        next.validate()?;
        *self = next;
        Ok(true)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.pages.is_empty() || self.pages.len() > OUTAGE_COMMAND_ARTIFACT_MAX_PAGES {
            return Err("outage artifact page count is outside the bound".into());
        }
        let complete = self.pages.len()
            == (self.total_length as usize).div_ceil(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES);
        let mut bytes = complete.then(|| Vec::with_capacity(self.total_length as usize));
        for (index, page_bytes) in self.pages.iter().enumerate() {
            OutageCommandArtifactPage {
                pg_id: self.pg_id,
                source_epoch: self.source_epoch,
                command_id: self.command_id,
                total_length: self.total_length,
                digest: self.digest,
                page_index: u16::try_from(index).expect("bounded page index fits u16"),
                bytes: page_bytes.clone(),
            }
            .validate()?;
            if let Some(bytes) = &mut bytes {
                bytes.extend_from_slice(page_bytes);
            }
        }
        if let Some(bytes) = bytes {
            if bytes.len() != self.total_length as usize || artifact_digest(&bytes) != self.digest {
                return Err("outage artifact completed bytes do not match the manifest".into());
            }
            validate_metadata_command_envelope_bytes(&bytes)
                .map_err(|error| format!("outage artifact is not a canonical command: {error}"))?;
            let header = decode_metadata_command_log_entry_header(&bytes)
                .map_err(|error| format!("outage artifact command header is invalid: {error}"))?;
            if header.id() != self.command_id {
                return Err("outage artifact bytes have a different command identity".into());
            }
        }
        Ok(())
    }

    pub(super) fn total_retained_bytes(&self) -> usize {
        self.pages.iter().map(Vec::len).sum()
    }

    pub(super) fn is_complete(&self) -> bool {
        self.pages.len()
            == (self.total_length as usize).div_ceil(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES)
    }

    #[cfg(test)]
    pub(super) fn assembled_bytes(&self) -> Option<Vec<u8>> {
        (self.pages.len()
            == (self.total_length as usize).div_ceil(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES))
        .then(|| self.pages.concat())
    }
}

fn artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    hasher.update(bytes);
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 is 32 bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::{
        format_snapshot, parse_snapshot, ClusterControlSnapshot, ControlPlaneLinearizedCommandSink,
        FileControlPlaneStore, SingleAuthorityControlPlane,
    };
    use crate::control_plane_command::{ControlPlaneCommand, ControlPlaneCommandStateMachine};
    use crate::metadata_command::{
        BucketSubresourceMutation, CreateBucketCommand, MetadataCommandLogIndex,
        MetadataCommandPayload, PutBucketSubresourceCommand,
    };
    use placement::NodeId;

    fn command() -> MetadataCommandEnvelope {
        let owner = crate::types::OwnerIdentity::from_principal("outage-artifact-owner");
        let bucket = crate::types::BucketName::try_from("outage-artifact-bucket").unwrap();
        let config = crate::types::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            ownership_controls: crate::types::BucketOwnershipControls {
                object_ownership: crate::types::BucketObjectOwnership::ObjectWriter,
            },
        };
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_with_fixed_upload_id_key_for_test(&config, 1, 1)
                    .unwrap(),
            ),
        )
    }

    fn bootstrapped_snapshot() -> ClusterControlSnapshot {
        ClusterControlSnapshot::empty()
            .apply_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/outage-artifact.sock".into())],
                pg_ids: vec![PgId::new(7)],
            })
            .unwrap()
            .into_snapshot()
    }

    #[test]
    fn published_page_is_epoch_neutral_replayable_and_snapshot_durable() {
        let snapshot = bootstrapped_snapshot();
        let source_epoch = snapshot.cluster_epoch();
        let page = OutageCommandArtifactPage::for_command(&command(), source_epoch)
            .unwrap()
            .remove(0);
        let publish = ControlPlaneCommand::PublishOutageCommandArtifactPage { page: page.clone() };
        let encoded = crate::control_plane_command::encode_control_plane_command(&publish).unwrap();
        assert_eq!(
            crate::control_plane_command::decode_control_plane_command(&encoded).unwrap(),
            publish
        );
        let applied = snapshot
            .apply_control_plane_command(publish.clone())
            .unwrap();
        assert!(applied.changed());
        let published = applied.into_snapshot();
        assert_eq!(published.cluster_epoch(), source_epoch);
        assert_eq!(
            published.outage_command_artifacts[&page.key()].assembled_bytes(),
            Some(command().command_bytes())
        );
        let reopened = parse_snapshot(&format_snapshot(&published)).unwrap();
        assert_eq!(reopened, published);
        let replay = reopened.apply_control_plane_command(publish).unwrap();
        assert!(!replay.changed());
        assert_eq!(replay.into_snapshot(), reopened);
        let mut corrupt = reopened.clone();
        let record = std::sync::Arc::make_mut(
            corrupt
                .outage_command_artifacts
                .get_mut(&page.key())
                .unwrap(),
        );
        record.pages[0][0] ^= 1;
        assert!(parse_snapshot(&format_snapshot(&corrupt)).is_err());
    }

    #[test]
    fn pages_require_contiguous_immutable_bytes_and_exact_digest() {
        let command_id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            crate::metadata_command::MetadataCommandLogIndex::new(1).unwrap(),
        );
        let bytes = vec![7; OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES + 1];
        let first = OutageCommandArtifactPage {
            pg_id: PgId::new(1),
            source_epoch: ClusterEpoch::INITIAL,
            command_id,
            total_length: bytes.len() as u32,
            digest: artifact_digest(&bytes),
            page_index: 0,
            bytes: bytes[..OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES].to_vec(),
        };
        let mut record = OutageCommandArtifactRecord::from_first_page(&first).unwrap();
        let second = OutageCommandArtifactPage {
            page_index: 1,
            bytes: vec![7],
            ..first.clone()
        };
        assert!(
            record.append(&second).is_err(),
            "non-command bytes must not finalize"
        );
        assert_eq!(record.pages.len(), 1);
        assert_eq!(record.append(&first), Ok(false));
        let mut changed = first.clone();
        changed.bytes[0] ^= 1;
        assert!(record.append(&changed).is_err());
    }

    #[test]
    fn canonical_large_command_survives_partial_snapshot_and_exact_replay() {
        let snapshot = bootstrapped_snapshot();
        let source_epoch = snapshot.cluster_epoch();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                crate::types::BucketName::try_from("outage-artifact-bucket").unwrap(),
                BucketSubresourceMutation::PutCors(format!(
                    "<CORSConfiguration><!--{}--></CORSConfiguration>",
                    "x".repeat(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES)
                )),
                1,
            )),
        );
        let pages = OutageCommandArtifactPage::for_command(&command, source_epoch).unwrap();
        assert_eq!(pages.len(), 2);
        let first = ControlPlaneCommand::PublishOutageCommandArtifactPage {
            page: pages[0].clone(),
        };
        let partial = snapshot
            .apply_control_plane_command(first.clone())
            .unwrap()
            .into_snapshot();
        let partial = parse_snapshot(&format_snapshot(&partial)).unwrap();
        assert_eq!(
            partial.outage_command_artifacts[&pages[0].key()].assembled_bytes(),
            None
        );
        let mut corrupt_tail = pages[1].clone();
        corrupt_tail.bytes[0] ^= 1;
        assert!(partial
            .apply_control_plane_command(ControlPlaneCommand::PublishOutageCommandArtifactPage {
                page: corrupt_tail,
            })
            .is_err());
        let second = ControlPlaneCommand::PublishOutageCommandArtifactPage {
            page: pages[1].clone(),
        };
        let completed = partial
            .apply_control_plane_command(second.clone())
            .unwrap()
            .into_snapshot();
        assert_eq!(completed.cluster_epoch(), source_epoch);
        assert_eq!(
            completed.outage_command_artifacts[&pages[0].key()].assembled_bytes(),
            Some(command.command_bytes())
        );
        let reopened = parse_snapshot(&format_snapshot(&completed)).unwrap();
        for replay in [first, second] {
            let result = reopened.apply_control_plane_command(replay).unwrap();
            assert!(!result.changed());
            assert_eq!(result.into_snapshot(), reopened);
        }
    }

    #[test]
    fn partial_artifact_pages_replay_from_the_standalone_journal() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("outage-artifact.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/outage-artifact.sock".into())],
                pg_ids: vec![PgId::new(7)],
            })
            .unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                crate::types::BucketName::try_from("outage-artifact-bucket").unwrap(),
                BucketSubresourceMutation::PutCors(format!(
                    "<CORSConfiguration><!--{}--></CORSConfiguration>",
                    "x".repeat(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES)
                )),
                1,
            )),
        );
        let pages = OutageCommandArtifactPage::for_command(&command, source_epoch).unwrap();
        authority
            .submit_control_plane_command(ControlPlaneCommand::PublishOutageCommandArtifactPage {
                page: pages[0].clone(),
            })
            .unwrap();
        drop(authority);
        let mut restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        assert_eq!(
            restarted.snapshot().outage_command_artifacts[&pages[0].key()]
                .pages
                .len(),
            1
        );
        restarted
            .submit_control_plane_command(ControlPlaneCommand::PublishOutageCommandArtifactPage {
                page: pages[1].clone(),
            })
            .unwrap();
        drop(restarted);
        let reopened = SingleAuthorityControlPlane::open(store).unwrap();
        assert_eq!(
            reopened.snapshot().outage_command_artifacts[&pages[0].key()].assembled_bytes(),
            Some(command.command_bytes())
        );
        assert!(reopened.snapshot().cluster_epoch() > source_epoch);
    }

    #[test]
    fn snapshot_parser_rejects_oversized_page_before_hex_decode() {
        let mut text = format_snapshot(&bootstrapped_snapshot());
        text.push_str(&format!(
            "outage_command_artifact_page=7,1,1,1,131073,{},0,{}\n",
            "00".repeat(32),
            "00".repeat(OUTAGE_COMMAND_ARTIFACT_PAGE_BYTES + 1)
        ));
        assert!(matches!(
            parse_snapshot(&text),
            Err(crate::control_plane::ControlPlaneError::Parse { message, .. })
                if message == "outage artifact page exceeds the byte bound"
        ));
    }
}
