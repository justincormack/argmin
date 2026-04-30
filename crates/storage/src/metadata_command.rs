use std::num::NonZeroU64;

use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, RetentionPeriod,
};

use crate::types::{
    BucketEncryptionConfig, BucketName, BucketObjectOwnership, BucketOwnershipControls,
    BucketSubresourceAux, BucketSubresourceKind, ClusterEpoch, CreateBucketConfig,
    ManagedEncryptionAlgorithm, PgId, PublicAccessBlockConfig,
};

const METADATA_COMMAND_MAGIC: &[u8] = b"argmin-metadata-command";
const METADATA_COMMAND_ENCODING_VERSION: u16 = 1;
const METADATA_COMMAND_CREATE_BUCKET: u16 = 1;
const METADATA_COMMAND_PUT_BUCKET_VERSIONING: u16 = 2;
const METADATA_COMMAND_PUT_BUCKET_ACL: u16 = 3;
const METADATA_COMMAND_PUT_BUCKET_PROPERTY: u16 = 4;
const METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE: u16 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MetadataCommandLogIndex(NonZeroU64);

impl MetadataCommandLogIndex {
    pub(crate) fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MetadataCommandId {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    log_index: MetadataCommandLogIndex,
}

impl MetadataCommandId {
    pub(crate) fn new(
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_id,
            log_index,
        }
    }

    pub(crate) fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub(crate) fn pg_id(self) -> PgId {
        self.pg_id
    }

    pub(crate) fn log_index(self) -> MetadataCommandLogIndex {
        self.log_index
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateBucketCommand {
    pub(crate) name: BucketName,
    pub(crate) owner_principal: String,
    pub(crate) owner_canonical_id: CanonicalUserId,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) versioning: BucketVersioningState,
    pub(crate) object_lock: BucketObjectLockConfig,
    pub(crate) created_at_millis: u64,
    pub(crate) bucket_execution_generation: u64,
}

impl CreateBucketCommand {
    pub(crate) fn from_config(
        config: &CreateBucketConfig<'_>,
        created_at_millis: u64,
        bucket_execution_generation: u64,
    ) -> Result<Self, String> {
        let name = BucketName::try_from(config.name.to_string())
            .map_err(|reason| format!("invalid bucket name in create bucket command: {reason}"))?;
        Ok(Self {
            name,
            owner_principal: config.owner_principal.to_string(),
            owner_canonical_id: config.owner_canonical_id.clone(),
            acl_grants: config.acl_grants.clone(),
            public_read: config.public_read,
            public_write: config.public_write,
            versioning: config.versioning,
            object_lock: config.object_lock,
            created_at_millis,
            bucket_execution_generation,
        })
    }

    pub(crate) fn config(&self) -> CreateBucketConfig<'_> {
        CreateBucketConfig {
            name: self.name.as_str(),
            owner_principal: &self.owner_principal,
            owner_canonical_id: &self.owner_canonical_id,
            acl_grants: &self.acl_grants,
            public_read: self.public_read,
            public_write: self.public_write,
            versioning: self.versioning,
            object_lock: self.object_lock,
        }
    }

    pub(crate) fn matches_config(&self, config: &CreateBucketConfig<'_>) -> bool {
        let Ok(name) = BucketName::try_from(config.name.to_string()) else {
            return false;
        };
        self.name == name
            && self.owner_principal == config.owner_principal
            && &self.owner_canonical_id == config.owner_canonical_id
            && &self.acl_grants == config.acl_grants
            && self.public_read == config.public_read
            && self.public_write == config.public_write
            && self.versioning == config.versioning
            && self.object_lock == config.object_lock
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetadataCommandPayload {
    CreateBucket(CreateBucketCommand),
    PutBucketVersioning(PutBucketVersioningCommand),
    PutBucketAcl(PutBucketAclCommand),
    PutBucketProperty(PutBucketPropertyCommand),
    PutBucketSubresource(PutBucketSubresourceCommand),
}

impl MetadataCommandPayload {
    fn kind_id(&self) -> u16 {
        match self {
            Self::CreateBucket(_) => METADATA_COMMAND_CREATE_BUCKET,
            Self::PutBucketVersioning(_) => METADATA_COMMAND_PUT_BUCKET_VERSIONING,
            Self::PutBucketAcl(_) => METADATA_COMMAND_PUT_BUCKET_ACL,
            Self::PutBucketProperty(_) => METADATA_COMMAND_PUT_BUCKET_PROPERTY,
            Self::PutBucketSubresource(_) => METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketVersioningCommand {
    pub(crate) name: BucketName,
    pub(crate) state: BucketVersioningState,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketVersioningCommand {
    pub(crate) fn new(
        name: BucketName,
        state: BucketVersioningState,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            state,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> bool {
        self.name == *bucket && self.state == state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketAclCommand {
    pub(crate) name: BucketName,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketAclCommand {
    pub(crate) fn new(
        name: BucketName,
        acl_grants: AclGrants,
        public_read: bool,
        public_write: bool,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            acl_grants,
            public_read,
            public_write,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> bool {
        self.name == *bucket
            && self.acl_grants == *acl_grants
            && self.public_read == public_read
            && self.public_write == public_write
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketPropertyCommand {
    pub(crate) name: BucketName,
    pub(crate) mutation: BucketPropertyMutation,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketPropertyCommand {
    pub(crate) fn new(
        name: BucketName,
        mutation: BucketPropertyMutation,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            mutation,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        mutation: &BucketPropertyMutation,
    ) -> bool {
        self.name == *bucket && self.mutation == *mutation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketPropertyMutation {
    ObjectLock(BucketObjectLockConfig),
    Encryption(BucketEncryptionConfig),
    PublicAccessBlock(Option<PublicAccessBlockConfig>),
    OwnershipControls(Option<BucketOwnershipControls>),
    AbacEnabled(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketSubresourceCommand {
    pub(crate) name: BucketName,
    pub(crate) mutation: BucketSubresourceMutation,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketSubresourceCommand {
    pub(crate) fn new(
        name: BucketName,
        mutation: BucketSubresourceMutation,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            mutation,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        mutation: &BucketSubresourceMutation,
    ) -> bool {
        self.name == *bucket && self.mutation == *mutation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketSubresourceMutation {
    Put {
        kind: BucketSubresourceKind,
        body: String,
        aux: BucketSubresourceAux,
    },
    Delete {
        kind: BucketSubresourceKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCommandEnvelope {
    id: MetadataCommandId,
    payload: MetadataCommandPayload,
    checksum_crc64: u64,
}

impl MetadataCommandEnvelope {
    pub(crate) fn new(id: MetadataCommandId, payload: MetadataCommandPayload) -> Self {
        let checksum_crc64 = checksum::crc64::checksum(&canonical_command_bytes(id, &payload));
        Self {
            id,
            payload,
            checksum_crc64,
        }
    }

    pub(crate) fn id(&self) -> MetadataCommandId {
        self.id
    }

    pub(crate) fn payload(&self) -> &MetadataCommandPayload {
        &self.payload
    }

    #[cfg(test)]
    pub(crate) fn checksum_crc64(&self) -> u64 {
        self.checksum_crc64
    }

    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = canonical_command_bytes(self.id, &self.payload);
        put_u64(&mut out, self.checksum_crc64);
        out
    }

    pub(crate) fn verify_checksum(&self) -> bool {
        checksum::crc64::checksum(&canonical_command_bytes(self.id, &self.payload))
            == self.checksum_crc64
    }
}

fn canonical_command_bytes(id: MetadataCommandId, payload: &MetadataCommandPayload) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, METADATA_COMMAND_MAGIC);
    put_u16(&mut out, METADATA_COMMAND_ENCODING_VERSION);
    put_u64(&mut out, id.cluster_epoch().get());
    put_u32(&mut out, id.pg_id().get());
    put_u64(&mut out, id.log_index().get());
    put_u16(&mut out, payload.kind_id());
    match payload {
        MetadataCommandPayload::CreateBucket(command) => encode_create_bucket(&mut out, command),
        MetadataCommandPayload::PutBucketVersioning(command) => {
            encode_put_bucket_versioning(&mut out, command);
        }
        MetadataCommandPayload::PutBucketAcl(command) => {
            encode_put_bucket_acl(&mut out, command);
        }
        MetadataCommandPayload::PutBucketProperty(command) => {
            encode_put_bucket_property(&mut out, command);
        }
        MetadataCommandPayload::PutBucketSubresource(command) => {
            encode_put_bucket_subresource(&mut out, command);
        }
    }
    out
}

fn encode_create_bucket(out: &mut Vec<u8>, command: &CreateBucketCommand) {
    put_str(out, command.name.as_str());
    put_str(out, &command.owner_principal);
    put_str(out, command.owner_canonical_id.as_str());
    put_str(out, &command.acl_grants.serialized());
    put_bool(out, command.public_read);
    put_bool(out, command.public_write);
    put_u8(out, command.versioning as u8);
    encode_object_lock(out, command.object_lock);
    put_u64(out, command.created_at_millis);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_versioning(out: &mut Vec<u8>, command: &PutBucketVersioningCommand) {
    put_str(out, command.name.as_str());
    put_u8(out, command.state as u8);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_acl(out: &mut Vec<u8>, command: &PutBucketAclCommand) {
    put_str(out, command.name.as_str());
    put_str(out, &command.acl_grants.serialized());
    put_bool(out, command.public_read);
    put_bool(out, command.public_write);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_property(out: &mut Vec<u8>, command: &PutBucketPropertyCommand) {
    put_str(out, command.name.as_str());
    encode_bucket_property_mutation(out, &command.mutation);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_subresource(out: &mut Vec<u8>, command: &PutBucketSubresourceCommand) {
    put_str(out, command.name.as_str());
    encode_bucket_subresource_mutation(out, &command.mutation);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_bucket_subresource_mutation(out: &mut Vec<u8>, mutation: &BucketSubresourceMutation) {
    match mutation {
        BucketSubresourceMutation::Put { kind, body, aux } => {
            put_u8(out, 1);
            encode_bucket_subresource_kind(out, *kind);
            put_str(out, body);
            encode_bucket_subresource_aux(out, *aux);
        }
        BucketSubresourceMutation::Delete { kind } => {
            put_u8(out, 2);
            encode_bucket_subresource_kind(out, *kind);
        }
    }
}

fn encode_bucket_subresource_kind(out: &mut Vec<u8>, kind: BucketSubresourceKind) {
    put_u8(out, kind as u8);
}

fn encode_bucket_subresource_aux(out: &mut Vec<u8>, aux: BucketSubresourceAux) {
    match aux {
        BucketSubresourceAux::None => put_u8(out, 0),
        BucketSubresourceAux::Policy { is_public } => {
            put_u8(out, 1);
            put_bool(out, is_public);
        }
    }
}

fn encode_bucket_property_mutation(out: &mut Vec<u8>, mutation: &BucketPropertyMutation) {
    match mutation {
        BucketPropertyMutation::ObjectLock(config) => {
            put_u8(out, 1);
            encode_object_lock(out, *config);
        }
        BucketPropertyMutation::Encryption(config) => {
            put_u8(out, 2);
            encode_bucket_encryption(out, *config);
        }
        BucketPropertyMutation::PublicAccessBlock(config) => {
            put_u8(out, 3);
            encode_public_access_block(out, *config);
        }
        BucketPropertyMutation::OwnershipControls(config) => {
            put_u8(out, 4);
            encode_ownership_controls(out, *config);
        }
        BucketPropertyMutation::AbacEnabled(enabled) => {
            put_u8(out, 5);
            put_bool(out, *enabled);
        }
    }
}

fn encode_bucket_encryption(out: &mut Vec<u8>, config: BucketEncryptionConfig) {
    match config.default_encryption {
        None => put_u8(out, 0),
        Some(ManagedEncryptionAlgorithm::Aes256) => {
            put_u8(out, 1);
            put_u8(out, ManagedEncryptionAlgorithm::Aes256 as u8);
        }
    }
    put_bool(out, config.sse_c_blocked);
}

fn encode_public_access_block(out: &mut Vec<u8>, config: Option<PublicAccessBlockConfig>) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_bool(out, config.block_public_acls);
            put_bool(out, config.ignore_public_acls);
            put_bool(out, config.block_public_policy);
            put_bool(out, config.restrict_public_buckets);
        }
    }
}

fn encode_ownership_controls(out: &mut Vec<u8>, config: Option<BucketOwnershipControls>) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_u8(
                out,
                match config.object_ownership {
                    BucketObjectOwnership::BucketOwnerEnforced => 0,
                    BucketObjectOwnership::BucketOwnerPreferred => 1,
                    BucketObjectOwnership::ObjectWriter => 2,
                },
            );
        }
    }
}

fn encode_object_lock(out: &mut Vec<u8>, object_lock: BucketObjectLockConfig) {
    put_bool(out, object_lock.enabled);
    match object_lock.default_retention {
        None => put_u8(out, 0),
        Some(ObjectLockDefaultRetention { mode, period }) => {
            put_u8(out, 1);
            put_u8(
                out,
                match mode {
                    ObjectLockMode::Governance => 0,
                    ObjectLockMode::Compliance => 1,
                },
            );
            match period {
                RetentionPeriod::Days(days) => {
                    put_u8(out, 1);
                    put_u32(out, days.get());
                }
                RetentionPeriod::Years(years) => {
                    put_u8(out, 2);
                    put_u32(out, years.get());
                }
            }
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    put_u8(out, u8::from(value));
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_command_canonical_encoding_is_stable() {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let command = CreateBucketCommand::from_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: true,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
            },
            123,
            7,
        )
        .unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command.clone()));
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert!(envelope.verify_checksum());
        assert_eq!(envelope.checksum_crc64(), 0xa1bf3d54b1685454);
    }

    #[test]
    fn metadata_command_versioning_encoding_is_stable() {
        let command = PutBucketVersioningCommand::new(
            BucketName::try_from("bucket").unwrap(),
            BucketVersioningState::Enabled,
            11,
        );
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(10).unwrap(),
        );
        let envelope = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::PutBucketVersioning(command.clone()),
        );
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketVersioning(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert_eq!(envelope.checksum_crc64(), 0xa7a53964fbc32e58);
        assert!(envelope.verify_checksum());
    }

    #[test]
    fn metadata_command_bucket_acl_encoding_is_stable() {
        let command = PutBucketAclCommand::new(
            BucketName::try_from("bucket").unwrap(),
            AclGrants::default(),
            true,
            false,
            12,
        );
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(11).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command.clone()));
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert_eq!(envelope.checksum_crc64(), 0x3947184ebe5f3b3b);
        assert!(envelope.verify_checksum());
    }

    #[test]
    fn metadata_command_bucket_property_encoding_is_stable() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(12).unwrap(),
        );
        let mutations = [
            (
                BucketPropertyMutation::ObjectLock(BucketObjectLockConfig {
                    enabled: true,
                    default_retention: Some(ObjectLockDefaultRetention {
                        mode: ObjectLockMode::Governance,
                        period: RetentionPeriod::days(7).unwrap(),
                    }),
                }),
                0,
            ),
            (
                BucketPropertyMutation::Encryption(BucketEncryptionConfig {
                    default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                    sse_c_blocked: false,
                }),
                0,
            ),
            (
                BucketPropertyMutation::PublicAccessBlock(Some(PublicAccessBlockConfig {
                    block_public_acls: true,
                    ignore_public_acls: false,
                    block_public_policy: true,
                    restrict_public_buckets: false,
                })),
                0,
            ),
            (BucketPropertyMutation::PublicAccessBlock(None), 0),
            (
                BucketPropertyMutation::OwnershipControls(Some(BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
                })),
                0,
            ),
            (BucketPropertyMutation::OwnershipControls(None), 0),
            (BucketPropertyMutation::AbacEnabled(true), 0),
        ];

        let mut checksums = Vec::new();
        for (offset, (mutation, _expected_checksum)) in mutations.into_iter().enumerate() {
            let command =
                PutBucketPropertyCommand::new(bucket.clone(), mutation.clone(), 20 + offset as u64);
            let id = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + offset as u64).unwrap(),
            );
            let envelope = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketProperty(command.clone()),
            );
            let duplicate = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketProperty(command),
            );

            assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
            assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
            assert!(envelope.verify_checksum());
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0x239f63ca5e298fa1,
                0xb5c042bf2bf79c2d,
                0x01ff273102f24631,
                0x703c1fc321a3a801,
                0xbc2d9efb84c3ed11,
                0xd8eb45a268b77749,
                0xcc7e17ba9b256211,
            ]
        );
    }

    #[test]
    fn metadata_command_bucket_subresource_encoding_is_stable() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(19).unwrap(),
        );
        let mutations = [
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Policy,
                body: r#"{"Statement":[]}"#.to_owned(),
                aux: BucketSubresourceAux::policy(true),
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Policy,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Tagging,
                body: "<Tagging/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Tagging,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Lifecycle,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Cors,
            },
        ];

        let mut checksums = Vec::new();
        for (offset, mutation) in mutations.into_iter().enumerate() {
            let command =
                PutBucketSubresourceCommand::new(bucket.clone(), mutation, 30 + offset as u64);
            let id = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + offset as u64).unwrap(),
            );
            let envelope = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketSubresource(command.clone()),
            );
            let duplicate = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketSubresource(command),
            );

            assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
            assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
            assert!(envelope.verify_checksum());
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0x2c71312aba70ec12,
                0x7a3aad2f1477e115,
                0xa9d13d13566d4ae2,
                0x26a1d1cfadbac4da,
                0x48043526c6d0ce88,
                0xb566708169760d44,
                0xfcc8ed210f34db99,
                0x4b00ed0e84caf2ba,
            ]
        );
    }
}
