use crate::control_plane::ControlPlaneError;
use placement::NodeId;
use ring::hmac;
use std::fmt;

const CONTROL_PLANE_AUTH_MAGIC: &[u8; 8] = b"ARGCPAUT";
const CONTROL_PLANE_AUTH_VERSION: u16 = 1;
const CONTROL_PLANE_AUTH_MAX_CLUSTER_ID_LEN: usize = 256;
const CONTROL_PLANE_AUTH_MAX_CREDENTIAL_ID_LEN: usize = 128;
const CONTROL_PLANE_AUTH_MAX_INSTANCE_ID_LEN: usize = 128;
const CONTROL_PLANE_AUTH_MAX_NONCE_LEN: usize = 32;
const CONTROL_PLANE_AUTH_MAX_AUTHENTICATOR_LEN: usize = 128;
const CONTROL_PLANE_AUTH_MAX_SECRET_LEN: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneAuthPrincipal {
    RaftPeer { node_id: u64 },
    StorageNode { node_id: NodeId, incarnation: u64 },
    Frontend { instance_id: String },
    Admin { instance_id: String },
    LocalMaintenance { process_id: u64 },
    Service { service: ControlPlaneAuthService },
}

impl ControlPlaneAuthPrincipal {
    fn validate(&self) -> Result<(), ControlPlaneError> {
        match self {
            Self::RaftPeer { node_id } => {
                if *node_id == 0 {
                    return Err(auth_protocol_error("Raft peer principal node id is zero"));
                }
            }
            Self::StorageNode { incarnation, .. } => {
                if *incarnation == 0 {
                    return Err(auth_protocol_error(
                        "storage-node principal incarnation is zero",
                    ));
                }
            }
            Self::Frontend { instance_id } | Self::Admin { instance_id } => {
                validate_nonempty_string(
                    instance_id,
                    CONTROL_PLANE_AUTH_MAX_INSTANCE_ID_LEN,
                    "principal instance id",
                )?;
            }
            Self::LocalMaintenance { process_id } => {
                if *process_id == 0 {
                    return Err(auth_protocol_error(
                        "local-maintenance principal process id is zero",
                    ));
                }
            }
            Self::Service { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneAuthTarget {
    Principal(ControlPlaneAuthPrincipal),
    Service(ControlPlaneAuthService),
}

impl ControlPlaneAuthTarget {
    fn validate(&self) -> Result<(), ControlPlaneError> {
        match self {
            Self::Principal(principal) => principal.validate(),
            Self::Service(_) => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneAuthService {
    ControlPlane,
    RuntimeMap,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ControlPlaneAuthOperation {
    RaftAppendEntries,
    RaftVote,
    RaftPreVote,
    RaftSnapshot,
    RaftTransferLeader,
    StorageRuntimeMapRefresh,
    FrontendRuntimeMapRead,
    AdminControlPlaneCommand,
    RuntimeMapResponse,
    AdminControlPlaneResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ControlPlaneAuthRejectionReason {
    Missing,
    Malformed,
    UnsupportedVersion,
    WrongCluster,
    WrongSource,
    WrongTarget,
    WrongRole,
    UnknownCredential,
    StaleCredential,
    AuthenticatorMismatch,
    ReplayFreshnessFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneAuthDecision {
    Accepted {
        credential_id: String,
        credential_version: u64,
    },
    Rejected {
        reason: ControlPlaneAuthRejectionReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneAuthReplayPolicy {
    TimestampWindow {
        now_ms: u64,
        max_window_ms: u64,
        allowed_future_skew_ms: u64,
    },
    FencedByPayloadSemantics,
}

#[must_use]
pub fn control_plane_auth_payload_has_magic(bytes: &[u8]) -> bool {
    bytes.starts_with(CONTROL_PLANE_AUTH_MAGIC)
}

pub struct ControlPlaneScopedCredential {
    cluster_id: String,
    credential_id: String,
    credential_version: u64,
    principal: ControlPlaneAuthPrincipal,
    secret: Vec<u8>,
}

impl fmt::Debug for ControlPlaneScopedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneScopedCredential")
            .field("cluster_id", &self.cluster_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("principal", &self.principal)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl Clone for ControlPlaneScopedCredential {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: self.principal.clone(),
            secret: self.secret.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneScopedCredentialInput {
    pub cluster_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub principal: ControlPlaneAuthPrincipal,
    pub secret: Vec<u8>,
}

impl fmt::Debug for ControlPlaneScopedCredentialInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneScopedCredentialInput")
            .field("cluster_id", &self.cluster_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("principal", &self.principal)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl ControlPlaneScopedCredential {
    pub fn new(input: ControlPlaneScopedCredentialInput) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            cluster_id: input.cluster_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            principal: input.principal,
            secret: input.secret,
        };
        credential.validate()?;
        Ok(credential)
    }

    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    #[must_use]
    pub fn principal(&self) -> &ControlPlaneAuthPrincipal {
        &self.principal
    }

    pub(crate) fn runtime_map_response_credential_for_frontend(
        &self,
    ) -> Result<Self, ControlPlaneError> {
        if !matches!(self.principal, ControlPlaneAuthPrincipal::Frontend { .. }) {
            return Err(auth_protocol_error(
                "runtime-map response credential requires a frontend scoped credential",
            ));
        }
        Self::new(ControlPlaneScopedCredentialInput {
            cluster_id: self.cluster_id.clone(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::RuntimeMap,
            },
            secret: self.secret.clone(),
        })
    }

    pub(crate) fn runtime_map_response_credential_for_storage_node(
        &self,
    ) -> Result<Self, ControlPlaneError> {
        if !matches!(
            self.principal,
            ControlPlaneAuthPrincipal::StorageNode { .. }
        ) {
            return Err(auth_protocol_error(
                "runtime-map response credential requires a storage-node scoped credential",
            ));
        }
        Self::new(ControlPlaneScopedCredentialInput {
            cluster_id: self.cluster_id.clone(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::RuntimeMap,
            },
            secret: self.secret.clone(),
        })
    }

    pub(crate) fn admin_control_plane_response_credential_for_admin(
        &self,
    ) -> Result<Self, ControlPlaneError> {
        if !matches!(self.principal, ControlPlaneAuthPrincipal::Admin { .. }) {
            return Err(auth_protocol_error(
                "admin control-plane response credential requires an admin scoped credential",
            ));
        }
        Self::new(ControlPlaneScopedCredentialInput {
            cluster_id: self.cluster_id.clone(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::Admin,
            },
            secret: self.secret.clone(),
        })
    }

    pub fn sign_envelope(
        &self,
        input: ControlPlaneAuthSignInput,
    ) -> Result<ControlPlaneAuthEnvelope, ControlPlaneError> {
        self.validate()?;
        let header = ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
            cluster_id: self.cluster_id.clone(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            source: self.principal.clone(),
            target: input.target,
            operation: input.operation,
            issued_at_ms: input.issued_at_ms,
            expires_at_ms: input.expires_at_ms,
            sequence: input.sequence,
            nonce: input.nonce,
        })?;
        let covered = header.encode_covered_bytes(&input.payload)?;
        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.secret);
        let authenticator = hmac::sign(&key, &covered).as_ref().to_vec();
        ControlPlaneAuthEnvelope::new(header, input.payload, authenticator)
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_nonempty_string(
            &self.cluster_id,
            CONTROL_PLANE_AUTH_MAX_CLUSTER_ID_LEN,
            "auth credential cluster id",
        )?;
        validate_nonempty_string(
            &self.credential_id,
            CONTROL_PLANE_AUTH_MAX_CREDENTIAL_ID_LEN,
            "auth credential id",
        )?;
        if self.credential_version == 0 {
            return Err(auth_protocol_error("auth credential version is zero"));
        }
        self.principal.validate()?;
        validate_secret(&self.secret)?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAuthSignInput {
    pub target: ControlPlaneAuthTarget,
    pub operation: ControlPlaneAuthOperation,
    pub issued_at_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
    /// Reserved for future operation-specific replay protection.
    ///
    /// Current verifiers do not enforce sequence uniqueness; production
    /// replay safety is provided by timestamp windows and operation semantics.
    pub sequence: Option<u64>,
    /// Reserved for future operation-specific replay protection.
    ///
    /// Current verifiers do not maintain nonce caches or enforce nonce
    /// uniqueness. Callers should leave this empty unless a future verifier
    /// explicitly documents nonce handling for the operation.
    pub nonce: Vec<u8>,
    pub payload: Vec<u8>,
}

impl fmt::Debug for ControlPlaneAuthSignInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuthSignInput")
            .field("target", &self.target)
            .field("operation", &self.operation)
            .field("issued_at_ms", &self.issued_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("sequence", &self.sequence)
            .field("nonce_len", &self.nonce.len())
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAuthVerificationInput<'a> {
    pub envelope: &'a ControlPlaneAuthEnvelope,
    pub expected_cluster_id: &'a str,
    pub expected_source: &'a ControlPlaneAuthPrincipal,
    pub expected_target: &'a ControlPlaneAuthTarget,
    pub expected_operation: ControlPlaneAuthOperation,
    pub replay_policy: ControlPlaneAuthReplayPolicy,
}

impl fmt::Debug for ControlPlaneAuthVerificationInput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuthVerificationInput")
            .field("expected_cluster_id", &self.expected_cluster_id)
            .field("expected_source", &self.expected_source)
            .field("expected_target", &self.expected_target)
            .field("expected_operation", &self.expected_operation)
            .field("replay_policy", &self.replay_policy)
            .field("envelope_cluster_id", &self.envelope.header().cluster_id())
            .field(
                "envelope_credential_id",
                &self.envelope.header().credential_id(),
            )
            .field(
                "envelope_credential_version",
                &self.envelope.header().credential_version(),
            )
            .field("envelope_source", &self.envelope.header().source())
            .field("envelope_target", &self.envelope.header().target())
            .field("envelope_operation", &self.envelope.header().operation())
            .field("envelope_payload_len", &self.envelope.payload().len())
            .field(
                "envelope_authenticator_len",
                &self.envelope.authenticator().len(),
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneScopedCredentialStore {
    credentials: Vec<ControlPlaneScopedCredential>,
}

impl ControlPlaneScopedCredentialStore {
    pub fn new(credentials: Vec<ControlPlaneScopedCredential>) -> Result<Self, ControlPlaneError> {
        if credentials.is_empty() {
            return Err(auth_protocol_error("auth credential store is empty"));
        }
        for credential in &credentials {
            credential.validate()?;
        }
        for (index, credential) in credentials.iter().enumerate() {
            if credentials[index + 1..].iter().any(|other| {
                other.cluster_id == credential.cluster_id
                    && other.credential_id == credential.credential_id
                    && other.credential_version == credential.credential_version
                    && other.principal == credential.principal
            }) {
                return Err(auth_protocol_error(
                    "duplicate scoped auth credential identity",
                ));
            }
        }
        Ok(Self { credentials })
    }

    #[must_use]
    pub fn credentials(&self) -> &[ControlPlaneScopedCredential] {
        &self.credentials
    }

    #[must_use]
    pub fn verify_envelope(
        &self,
        input: ControlPlaneAuthVerificationInput<'_>,
    ) -> ControlPlaneAuthDecision {
        match self.verify_envelope_inner(input) {
            Ok((credential_id, credential_version)) => ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            },
            Err(reason) => ControlPlaneAuthDecision::Rejected { reason },
        }
    }

    fn verify_envelope_inner(
        &self,
        input: ControlPlaneAuthVerificationInput<'_>,
    ) -> Result<(String, u64), ControlPlaneAuthRejectionReason> {
        let header = input.envelope.header();
        if header.cluster_id() != input.expected_cluster_id {
            return Err(ControlPlaneAuthRejectionReason::WrongCluster);
        }
        if header.source() != input.expected_source {
            return Err(ControlPlaneAuthRejectionReason::WrongSource);
        }
        if header.target() != input.expected_target {
            return Err(ControlPlaneAuthRejectionReason::WrongTarget);
        }
        if header.operation() != input.expected_operation {
            return Err(ControlPlaneAuthRejectionReason::WrongRole);
        }
        let credential = self.find_credential(header).ok_or_else(|| {
            if self.credentials.iter().any(|credential| {
                credential.cluster_id == header.cluster_id()
                    && credential.credential_id == header.credential_id()
                    && credential.principal == *input.expected_source
                    && credential.credential_version > header.credential_version()
            }) {
                ControlPlaneAuthRejectionReason::StaleCredential
            } else {
                ControlPlaneAuthRejectionReason::UnknownCredential
            }
        })?;
        validate_replay_policy(header, input.replay_policy)?;
        let covered = header
            .encode_covered_bytes(input.envelope.payload())
            .map_err(|_| ControlPlaneAuthRejectionReason::Malformed)?;
        let key = hmac::Key::new(hmac::HMAC_SHA256, &credential.secret);
        hmac::verify(&key, &covered, input.envelope.authenticator())
            .map_err(|_| ControlPlaneAuthRejectionReason::AuthenticatorMismatch)?;
        Ok((
            credential.credential_id.clone(),
            credential.credential_version,
        ))
    }

    fn find_credential(
        &self,
        header: &ControlPlaneAuthEnvelopeHeader,
    ) -> Option<&ControlPlaneScopedCredential> {
        self.credentials.iter().find(|credential| {
            credential.cluster_id == header.cluster_id()
                && credential.credential_id == header.credential_id()
                && credential.credential_version == header.credential_version()
                && credential.principal == *header.source()
        })
    }
}

fn validate_replay_policy(
    header: &ControlPlaneAuthEnvelopeHeader,
    replay_policy: ControlPlaneAuthReplayPolicy,
) -> Result<(), ControlPlaneAuthRejectionReason> {
    let ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms,
        max_window_ms,
        allowed_future_skew_ms,
    } = replay_policy
    else {
        return Ok(());
    };
    let issued_at_ms = header
        .issued_at_ms()
        .ok_or(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure)?;
    let expires_at_ms = header
        .expires_at_ms()
        .ok_or(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure)?;
    let replay_window_ms = expires_at_ms
        .checked_sub(issued_at_ms)
        .ok_or(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure)?;
    if replay_window_ms == 0 || replay_window_ms > max_window_ms {
        return Err(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure);
    }
    if issued_at_ms > now_ms.saturating_add(allowed_future_skew_ms) {
        return Err(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure);
    }
    if expires_at_ms <= now_ms {
        return Err(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure);
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAuthEnvelopeHeader {
    cluster_id: String,
    credential_id: String,
    credential_version: u64,
    source: ControlPlaneAuthPrincipal,
    target: ControlPlaneAuthTarget,
    operation: ControlPlaneAuthOperation,
    issued_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
    sequence: Option<u64>,
    nonce: Vec<u8>,
}

impl fmt::Debug for ControlPlaneAuthEnvelopeHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuthEnvelopeHeader")
            .field("cluster_id", &self.cluster_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("source", &self.source)
            .field("target", &self.target)
            .field("operation", &self.operation)
            .field("issued_at_ms", &self.issued_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("sequence", &self.sequence)
            .field("nonce_len", &self.nonce.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAuthEnvelopeHeaderInput {
    pub cluster_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub source: ControlPlaneAuthPrincipal,
    pub target: ControlPlaneAuthTarget,
    pub operation: ControlPlaneAuthOperation,
    pub issued_at_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
    /// Reserved for future operation-specific replay protection.
    ///
    /// Current verifiers do not enforce sequence uniqueness; production
    /// replay safety is provided by timestamp windows and operation semantics.
    pub sequence: Option<u64>,
    /// Reserved for future operation-specific replay protection.
    ///
    /// Current verifiers do not maintain nonce caches or enforce nonce
    /// uniqueness. Decoded nonces are authenticated but not replay-tracked.
    pub nonce: Vec<u8>,
}

impl fmt::Debug for ControlPlaneAuthEnvelopeHeaderInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuthEnvelopeHeaderInput")
            .field("cluster_id", &self.cluster_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("source", &self.source)
            .field("target", &self.target)
            .field("operation", &self.operation)
            .field("issued_at_ms", &self.issued_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("sequence", &self.sequence)
            .field("nonce_len", &self.nonce.len())
            .finish()
    }
}

impl ControlPlaneAuthEnvelopeHeader {
    pub fn new(input: ControlPlaneAuthEnvelopeHeaderInput) -> Result<Self, ControlPlaneError> {
        let header = Self {
            cluster_id: input.cluster_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            source: input.source,
            target: input.target,
            operation: input.operation,
            issued_at_ms: input.issued_at_ms,
            expires_at_ms: input.expires_at_ms,
            sequence: input.sequence,
            nonce: input.nonce,
        };
        header.validate()?;
        Ok(header)
    }

    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    #[must_use]
    pub fn source(&self) -> &ControlPlaneAuthPrincipal {
        &self.source
    }

    #[must_use]
    pub fn target(&self) -> &ControlPlaneAuthTarget {
        &self.target
    }

    #[must_use]
    pub fn operation(&self) -> ControlPlaneAuthOperation {
        self.operation
    }

    #[must_use]
    pub fn issued_at_ms(&self) -> Option<u64> {
        self.issued_at_ms
    }

    #[must_use]
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.expires_at_ms
    }

    #[must_use]
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    #[must_use]
    pub fn nonce(&self) -> &[u8] {
        &self.nonce
    }

    pub fn encode_covered_bytes(&self, payload: &[u8]) -> Result<Vec<u8>, ControlPlaneError> {
        self.validate()?;
        len_as_u32(payload.len(), "auth payload")?;
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_AUTH_MAGIC);
        write_u16(&mut out, CONTROL_PLANE_AUTH_VERSION);
        write_string(&mut out, &self.cluster_id)?;
        write_string(&mut out, &self.credential_id)?;
        write_u64(&mut out, self.credential_version);
        write_principal(&mut out, &self.source)?;
        write_target(&mut out, &self.target)?;
        write_operation(&mut out, self.operation);
        write_option_u64(&mut out, self.issued_at_ms);
        write_option_u64(&mut out, self.expires_at_ms);
        write_option_u64(&mut out, self.sequence);
        write_bytes(&mut out, &self.nonce)?;
        write_bytes(&mut out, payload)?;
        Ok(out)
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_nonempty_string(
            &self.cluster_id,
            CONTROL_PLANE_AUTH_MAX_CLUSTER_ID_LEN,
            "auth cluster id",
        )?;
        validate_nonempty_string(
            &self.credential_id,
            CONTROL_PLANE_AUTH_MAX_CREDENTIAL_ID_LEN,
            "auth credential id",
        )?;
        if self.credential_version == 0 {
            return Err(auth_protocol_error("auth credential version is zero"));
        }
        self.source.validate()?;
        self.target.validate()?;
        if let (Some(issued_at_ms), Some(expires_at_ms)) = (self.issued_at_ms, self.expires_at_ms) {
            if expires_at_ms <= issued_at_ms {
                return Err(auth_protocol_error(
                    "auth envelope expiry is not after issue time",
                ));
            }
        }
        if self.nonce.len() > CONTROL_PLANE_AUTH_MAX_NONCE_LEN {
            return Err(auth_protocol_error(format!(
                "auth nonce length {} exceeds {}",
                self.nonce.len(),
                CONTROL_PLANE_AUTH_MAX_NONCE_LEN
            )));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAuthEnvelope {
    header: ControlPlaneAuthEnvelopeHeader,
    payload: Vec<u8>,
    authenticator: Vec<u8>,
}

impl fmt::Debug for ControlPlaneAuthEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuthEnvelope")
            .field("header", &self.header)
            .field("payload_len", &self.payload.len())
            .field("authenticator_len", &self.authenticator.len())
            .finish()
    }
}

impl ControlPlaneAuthEnvelope {
    pub fn new(
        header: ControlPlaneAuthEnvelopeHeader,
        payload: Vec<u8>,
        authenticator: Vec<u8>,
    ) -> Result<Self, ControlPlaneError> {
        let envelope = Self {
            header,
            payload,
            authenticator,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    #[must_use]
    pub fn header(&self) -> &ControlPlaneAuthEnvelopeHeader {
        &self.header
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub fn authenticator(&self) -> &[u8] {
        &self.authenticator
    }

    pub fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.validate()?;
        let mut out = self.header.encode_covered_bytes(&self.payload)?;
        write_bytes(&mut out, &self.authenticator)?;
        Ok(out)
    }

    pub fn decode_frame(bytes: &[u8], max_payload_bytes: usize) -> Result<Self, ControlPlaneError> {
        let mut reader = AuthPayloadReader::new(bytes);
        if reader.read_exact(CONTROL_PLANE_AUTH_MAGIC.len())? != CONTROL_PLANE_AUTH_MAGIC {
            return Err(auth_protocol_error("invalid control-plane auth magic"));
        }
        let version = reader.read_u16()?;
        if version != CONTROL_PLANE_AUTH_VERSION {
            return Err(auth_protocol_error(format!(
                "unsupported control-plane auth version {version}"
            )));
        }
        let cluster_id = reader
            .read_string_bounded(CONTROL_PLANE_AUTH_MAX_CLUSTER_ID_LEN, "auth cluster id")?
            .to_owned();
        let credential_id = reader
            .read_string_bounded(
                CONTROL_PLANE_AUTH_MAX_CREDENTIAL_ID_LEN,
                "auth credential id",
            )?
            .to_owned();
        let credential_version = reader.read_u64()?;
        let source = read_principal(&mut reader)?;
        let target = read_target(&mut reader)?;
        let operation = read_operation(&mut reader)?;
        let issued_at_ms = reader.read_option_u64()?;
        let expires_at_ms = reader.read_option_u64()?;
        let sequence = reader.read_option_u64()?;
        let nonce = reader
            .read_bytes_bounded(CONTROL_PLANE_AUTH_MAX_NONCE_LEN, "auth nonce")?
            .to_vec();
        let payload = reader
            .read_bytes_bounded(max_payload_bytes, "auth payload")?
            .to_vec();
        let authenticator = reader
            .read_bytes_bounded(
                CONTROL_PLANE_AUTH_MAX_AUTHENTICATOR_LEN,
                "auth authenticator",
            )?
            .to_vec();
        reader.finish()?;
        Self::new(
            ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id,
                credential_id,
                credential_version,
                source,
                target,
                operation,
                issued_at_ms,
                expires_at_ms,
                sequence,
                nonce,
            })?,
            payload,
            authenticator,
        )
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        self.header.validate()?;
        len_as_u32(self.payload.len(), "auth payload")?;
        if self.authenticator.is_empty() {
            return Err(auth_protocol_error("auth authenticator is empty"));
        }
        if self.authenticator.len() > CONTROL_PLANE_AUTH_MAX_AUTHENTICATOR_LEN {
            return Err(auth_protocol_error(format!(
                "auth authenticator length {} exceeds {}",
                self.authenticator.len(),
                CONTROL_PLANE_AUTH_MAX_AUTHENTICATOR_LEN
            )));
        }
        Ok(())
    }
}

fn write_principal(
    out: &mut Vec<u8>,
    principal: &ControlPlaneAuthPrincipal,
) -> Result<(), ControlPlaneError> {
    principal.validate()?;
    match principal {
        ControlPlaneAuthPrincipal::RaftPeer { node_id } => {
            write_u8(out, 1);
            write_u64(out, *node_id);
        }
        ControlPlaneAuthPrincipal::StorageNode {
            node_id,
            incarnation,
        } => {
            write_u8(out, 2);
            write_u32(out, node_id.as_u32());
            write_u64(out, *incarnation);
        }
        ControlPlaneAuthPrincipal::Frontend { instance_id } => {
            write_u8(out, 3);
            write_string(out, instance_id)?;
        }
        ControlPlaneAuthPrincipal::Admin { instance_id } => {
            write_u8(out, 4);
            write_string(out, instance_id)?;
        }
        ControlPlaneAuthPrincipal::LocalMaintenance { process_id } => {
            write_u8(out, 5);
            write_u64(out, *process_id);
        }
        ControlPlaneAuthPrincipal::Service { service } => {
            write_u8(out, 6);
            write_service(out, *service);
        }
    }
    Ok(())
}

fn read_principal(
    reader: &mut AuthPayloadReader<'_>,
) -> Result<ControlPlaneAuthPrincipal, ControlPlaneError> {
    let principal = match reader.read_u8()? {
        1 => ControlPlaneAuthPrincipal::RaftPeer {
            node_id: reader.read_u64()?,
        },
        2 => ControlPlaneAuthPrincipal::StorageNode {
            node_id: NodeId::new(reader.read_u32()?),
            incarnation: reader.read_u64()?,
        },
        3 => ControlPlaneAuthPrincipal::Frontend {
            instance_id: reader
                .read_string_bounded(
                    CONTROL_PLANE_AUTH_MAX_INSTANCE_ID_LEN,
                    "frontend instance id",
                )?
                .to_owned(),
        },
        4 => ControlPlaneAuthPrincipal::Admin {
            instance_id: reader
                .read_string_bounded(CONTROL_PLANE_AUTH_MAX_INSTANCE_ID_LEN, "admin instance id")?
                .to_owned(),
        },
        5 => ControlPlaneAuthPrincipal::LocalMaintenance {
            process_id: reader.read_u64()?,
        },
        6 => ControlPlaneAuthPrincipal::Service {
            service: read_service(reader)?,
        },
        tag => {
            return Err(auth_protocol_error(format!(
                "unknown control-plane auth principal tag {tag}"
            )));
        }
    };
    principal.validate()?;
    Ok(principal)
}

fn write_target(
    out: &mut Vec<u8>,
    target: &ControlPlaneAuthTarget,
) -> Result<(), ControlPlaneError> {
    target.validate()?;
    match target {
        ControlPlaneAuthTarget::Principal(principal) => {
            write_u8(out, 1);
            write_principal(out, principal)?;
        }
        ControlPlaneAuthTarget::Service(service) => {
            write_u8(out, 2);
            write_service(out, *service);
        }
    }
    Ok(())
}

fn read_target(
    reader: &mut AuthPayloadReader<'_>,
) -> Result<ControlPlaneAuthTarget, ControlPlaneError> {
    let target = match reader.read_u8()? {
        1 => ControlPlaneAuthTarget::Principal(read_principal(reader)?),
        2 => ControlPlaneAuthTarget::Service(read_service(reader)?),
        tag => {
            return Err(auth_protocol_error(format!(
                "unknown control-plane auth target tag {tag}"
            )));
        }
    };
    target.validate()?;
    Ok(target)
}

fn write_service(out: &mut Vec<u8>, service: ControlPlaneAuthService) {
    write_u8(
        out,
        match service {
            ControlPlaneAuthService::ControlPlane => 1,
            ControlPlaneAuthService::RuntimeMap => 3,
            ControlPlaneAuthService::Admin => 5,
        },
    );
}

fn read_service(
    reader: &mut AuthPayloadReader<'_>,
) -> Result<ControlPlaneAuthService, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(ControlPlaneAuthService::ControlPlane),
        3 => Ok(ControlPlaneAuthService::RuntimeMap),
        5 => Ok(ControlPlaneAuthService::Admin),
        tag => Err(auth_protocol_error(format!(
            "unknown control-plane auth service tag {tag}"
        ))),
    }
}

fn write_operation(out: &mut Vec<u8>, operation: ControlPlaneAuthOperation) {
    write_u8(
        out,
        match operation {
            ControlPlaneAuthOperation::RaftAppendEntries => 1,
            ControlPlaneAuthOperation::RaftVote => 2,
            ControlPlaneAuthOperation::RaftPreVote => 3,
            ControlPlaneAuthOperation::RaftSnapshot => 4,
            ControlPlaneAuthOperation::RaftTransferLeader => 5,
            ControlPlaneAuthOperation::StorageRuntimeMapRefresh => 7,
            ControlPlaneAuthOperation::FrontendRuntimeMapRead => 8,
            ControlPlaneAuthOperation::AdminControlPlaneCommand => 9,
            ControlPlaneAuthOperation::RuntimeMapResponse => 10,
            ControlPlaneAuthOperation::AdminControlPlaneResponse => 11,
        },
    );
}

fn read_operation(
    reader: &mut AuthPayloadReader<'_>,
) -> Result<ControlPlaneAuthOperation, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(ControlPlaneAuthOperation::RaftAppendEntries),
        2 => Ok(ControlPlaneAuthOperation::RaftVote),
        3 => Ok(ControlPlaneAuthOperation::RaftPreVote),
        4 => Ok(ControlPlaneAuthOperation::RaftSnapshot),
        5 => Ok(ControlPlaneAuthOperation::RaftTransferLeader),
        7 => Ok(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        8 => Ok(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        9 => Ok(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        10 => Ok(ControlPlaneAuthOperation::RuntimeMapResponse),
        11 => Ok(ControlPlaneAuthOperation::AdminControlPlaneResponse),
        tag => Err(auth_protocol_error(format!(
            "unknown control-plane auth operation tag {tag}"
        ))),
    }
}

fn validate_nonempty_string(
    value: &str,
    max_len: usize,
    field: &'static str,
) -> Result<(), ControlPlaneError> {
    if value.is_empty() {
        return Err(auth_protocol_error(format!("{field} is empty")));
    }
    if value.len() > max_len {
        return Err(auth_protocol_error(format!(
            "{field} length {} exceeds {max_len}",
            value.len()
        )));
    }
    Ok(())
}

fn validate_secret(secret: &[u8]) -> Result<(), ControlPlaneError> {
    if secret.is_empty() {
        return Err(auth_protocol_error("auth credential secret is empty"));
    }
    if secret.len() > CONTROL_PLANE_AUTH_MAX_SECRET_LEN {
        return Err(auth_protocol_error(format!(
            "auth credential secret length {} exceeds {}",
            secret.len(),
            CONTROL_PLANE_AUTH_MAX_SECRET_LEN
        )));
    }
    Ok(())
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => write_u8(out, 0),
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value);
        }
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(bytes.len(), "auth byte field")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len)
        .map_err(|_| auth_protocol_error(format!("{field} length {len} exceeds u32::MAX")))
}

fn auth_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::RpcProtocol {
        message: format!("control-plane auth envelope: {}", message.into()),
    }
}

struct AuthPayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> AuthPayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(auth_protocol_error(format!(
                "payload has {} trailing bytes",
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| auth_protocol_error("payload offset overflow"))?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| auth_protocol_error("truncated payload"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ControlPlaneError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            tag => Err(auth_protocol_error(format!(
                "invalid optional u64 tag {tag}"
            ))),
        }
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| auth_protocol_error(format!("{field} length does not fit usize")))
    }

    fn read_bytes_bounded(
        &mut self,
        max_len: usize,
        field: &'static str,
    ) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len(field)?;
        if len > max_len {
            return Err(auth_protocol_error(format!(
                "{field} length {len} exceeds {max_len}"
            )));
        }
        self.read_exact(len)
    }

    fn read_string_bounded(
        &mut self,
        max_len: usize,
        field: &'static str,
    ) -> Result<&'a str, ControlPlaneError> {
        let bytes = self.read_bytes_bounded(max_len, field)?;
        std::str::from_utf8(bytes)
            .map_err(|source| auth_protocol_error(format!("{field} is not valid UTF-8: {source}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp_replay_policy(now_ms: u64) -> ControlPlaneAuthReplayPolicy {
        ControlPlaneAuthReplayPolicy::TimestampWindow {
            now_ms,
            max_window_ms: 1_000,
            allowed_future_skew_ms: 0,
        }
    }

    fn sample_header() -> ControlPlaneAuthEnvelopeHeader {
        ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
            cluster_id: "cluster-a".to_owned(),
            credential_id: "raft-peer-key-a".to_owned(),
            credential_version: 7,
            source: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 102,
            }),
            operation: ControlPlaneAuthOperation::RaftAppendEntries,
            issued_at_ms: Some(1_000),
            expires_at_ms: Some(2_000),
            sequence: Some(42),
            nonce: vec![1, 2, 3, 4],
        })
        .unwrap()
    }

    fn sample_envelope() -> ControlPlaneAuthEnvelope {
        ControlPlaneAuthEnvelope::new(sample_header(), b"raft-payload".to_vec(), vec![9; 32])
            .unwrap()
    }

    fn sample_credential() -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "cluster-a".to_owned(),
            credential_id: "raft-peer-key-a".to_owned(),
            credential_version: 7,
            principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
            secret: b"test scoped raft peer secret".to_vec(),
        })
        .unwrap()
    }

    fn sample_signed_envelope() -> ControlPlaneAuthEnvelope {
        sample_credential()
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: 102,
                }),
                operation: ControlPlaneAuthOperation::RaftAppendEntries,
                issued_at_ms: Some(1_000),
                expires_at_ms: Some(2_000),
                sequence: Some(42),
                nonce: vec![1, 2, 3, 4],
                payload: b"raft-payload".to_vec(),
            })
            .unwrap()
    }

    fn verify_signed_envelope(
        store: &ControlPlaneScopedCredentialStore,
        envelope: &ControlPlaneAuthEnvelope,
    ) -> ControlPlaneAuthDecision {
        store.verify_envelope(ControlPlaneAuthVerificationInput {
            envelope,
            expected_cluster_id: "cluster-a",
            expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
            expected_target: &ControlPlaneAuthTarget::Principal(
                ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 },
            ),
            expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
            replay_policy: timestamp_replay_policy(1_500),
        })
    }

    #[test]
    fn control_plane_auth_envelope_round_trips() {
        let envelope = sample_envelope();
        let encoded = envelope.encode_frame().unwrap();
        let decoded =
            ControlPlaneAuthEnvelope::decode_frame(&encoded, 1024).expect("frame should decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn control_plane_auth_envelope_exposes_canonical_covered_bytes() {
        let envelope = sample_envelope();
        let encoded = envelope.encode_frame().unwrap();
        let covered = envelope
            .header()
            .encode_covered_bytes(envelope.payload())
            .unwrap();
        assert!(encoded.starts_with(&covered));
        assert_eq!(
            &encoded[covered.len()..covered.len() + 4],
            32_u32.to_be_bytes()
        );
        assert_eq!(&encoded[covered.len() + 4..], envelope.authenticator());
    }

    #[test]
    fn control_plane_auth_envelope_round_trips_all_principal_roles() {
        let roles = [
            ControlPlaneAuthPrincipal::RaftPeer { node_id: 1 },
            ControlPlaneAuthPrincipal::StorageNode {
                node_id: NodeId::new(0),
                incarnation: 1,
            },
            ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-1".to_owned(),
            },
            ControlPlaneAuthPrincipal::Admin {
                instance_id: "admin-1".to_owned(),
            },
            ControlPlaneAuthPrincipal::LocalMaintenance { process_id: 123 },
            ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::RuntimeMap,
            },
        ];

        for source in roles {
            let header = ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "credential-a".to_owned(),
                credential_version: 1,
                source,
                target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::ControlPlane),
                operation: ControlPlaneAuthOperation::AdminControlPlaneCommand,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
            })
            .unwrap();
            let envelope = ControlPlaneAuthEnvelope::new(header, Vec::new(), vec![1]).unwrap();
            let decoded =
                ControlPlaneAuthEnvelope::decode_frame(&envelope.encode_frame().unwrap(), 0)
                    .unwrap();
            assert_eq!(decoded, envelope);
        }
    }

    #[test]
    fn control_plane_auth_envelope_rejects_bad_magic_version_and_trailing_bytes() {
        let encoded = sample_envelope().encode_frame().unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] = b'X';
        assert!(ControlPlaneAuthEnvelope::decode_frame(&bad_magic, 1024).is_err());

        let mut bad_version = encoded.clone();
        bad_version[9] = 2;
        assert!(ControlPlaneAuthEnvelope::decode_frame(&bad_version, 1024).is_err());

        let mut trailing = encoded;
        trailing.push(0);
        assert!(ControlPlaneAuthEnvelope::decode_frame(&trailing, 1024).is_err());
    }

    #[test]
    fn control_plane_auth_envelope_rejects_unknown_tags() {
        let encoded = sample_envelope().encode_frame().unwrap();

        let mut unknown_source_tag = encoded.clone();
        unknown_source_tag[50] = 99;
        assert!(ControlPlaneAuthEnvelope::decode_frame(&unknown_source_tag, 1024).is_err());

        let mut unknown_target_tag = encoded.clone();
        unknown_target_tag[59] = 99;
        assert!(ControlPlaneAuthEnvelope::decode_frame(&unknown_target_tag, 1024).is_err());

        let mut unknown_operation_tag = encoded;
        unknown_operation_tag[69] = 99;
        assert!(ControlPlaneAuthEnvelope::decode_frame(&unknown_operation_tag, 1024).is_err());
    }

    #[test]
    fn control_plane_auth_rejects_retired_wire_tags() {
        for tag in [2, 4] {
            let payload = [tag];
            let mut reader = AuthPayloadReader::new(&payload);
            assert!(read_service(&mut reader).is_err());
        }

        let payload = [6];
        let mut reader = AuthPayloadReader::new(&payload);
        assert!(read_operation(&mut reader).is_err());
    }

    #[test]
    fn control_plane_auth_envelope_rejects_invalid_principal_fields() {
        assert!(
            ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "credential-a".to_owned(),
                credential_version: 1,
                source: ControlPlaneAuthPrincipal::RaftPeer { node_id: 0 },
                target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::ControlPlane),
                operation: ControlPlaneAuthOperation::RaftVote,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
            })
            .is_err()
        );

        assert!(
            ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "credential-a".to_owned(),
                credential_version: 1,
                source: ControlPlaneAuthPrincipal::StorageNode {
                    node_id: NodeId::new(1),
                    incarnation: 0,
                },
                target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::ControlPlane),
                operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn control_plane_auth_envelope_rejects_invalid_option_and_truncation() {
        let encoded = sample_envelope().encode_frame().unwrap();

        let mut invalid_option = encoded.clone();
        invalid_option[70] = 2;
        assert!(ControlPlaneAuthEnvelope::decode_frame(&invalid_option, 1024).is_err());

        for len in [0, 4, 16, encoded.len() - 1] {
            assert!(ControlPlaneAuthEnvelope::decode_frame(&encoded[..len], 1024).is_err());
        }
    }

    #[test]
    fn control_plane_auth_envelope_rejects_payload_over_limit_before_copying() {
        let encoded = sample_envelope().encode_frame().unwrap();
        assert!(ControlPlaneAuthEnvelope::decode_frame(&encoded, 4).is_err());
    }

    #[test]
    fn control_plane_auth_envelope_rejects_empty_and_oversized_fields() {
        assert!(
            ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id: String::new(),
                credential_id: "credential-a".to_owned(),
                credential_version: 1,
                source: ControlPlaneAuthPrincipal::RaftPeer { node_id: 1 },
                target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::ControlPlane),
                operation: ControlPlaneAuthOperation::RaftVote,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
            })
            .is_err()
        );

        assert!(
            ControlPlaneAuthEnvelopeHeader::new(ControlPlaneAuthEnvelopeHeaderInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "credential-a".to_owned(),
                credential_version: 0,
                source: ControlPlaneAuthPrincipal::RaftPeer { node_id: 1 },
                target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::ControlPlane),
                operation: ControlPlaneAuthOperation::RaftVote,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
            })
            .is_err()
        );

        assert!(ControlPlaneAuthEnvelope::new(sample_header(), Vec::new(), Vec::new()).is_err());
    }

    #[test]
    fn control_plane_auth_scoped_credential_signs_and_verifies_envelope() {
        let credential = sample_credential();
        let envelope = sample_signed_envelope();
        let store = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();

        assert_eq!(
            verify_signed_envelope(&store, &envelope),
            ControlPlaneAuthDecision::Accepted {
                credential_id: credential.credential_id().to_owned(),
                credential_version: credential.credential_version(),
            }
        );
        assert_eq!(credential.cluster_id(), "cluster-a");
        assert_eq!(
            credential.principal(),
            &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 }
        );
    }

    #[test]
    fn control_plane_auth_scoped_credential_rejects_wrong_scope() {
        let store = ControlPlaneScopedCredentialStore::new(vec![sample_credential()]).unwrap();
        let envelope = sample_signed_envelope();

        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-b",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(1_500),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::WrongCluster,
            }
        );
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 999 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(1_500),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::WrongSource,
            }
        );
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Service(
                    ControlPlaneAuthService::ControlPlane
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(1_500),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::WrongTarget,
            }
        );
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftVote,
                replay_policy: timestamp_replay_policy(1_500),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::WrongRole,
            }
        );
    }

    #[test]
    fn control_plane_auth_scoped_credential_rejects_unknown_stale_and_time_invalid_credentials() {
        let envelope = sample_signed_envelope();
        let unknown_store =
            ControlPlaneScopedCredentialStore::new(vec![ControlPlaneScopedCredential::new(
                ControlPlaneScopedCredentialInput {
                    cluster_id: "cluster-a".to_owned(),
                    credential_id: "other-key".to_owned(),
                    credential_version: 7,
                    principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                    secret: b"test scoped raft peer secret".to_vec(),
                },
            )
            .unwrap()])
            .unwrap();
        assert_eq!(
            verify_signed_envelope(&unknown_store, &envelope),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::UnknownCredential,
            }
        );
        let unknown_expired_envelope =
            ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "unknown-expired-key".to_owned(),
                credential_version: 7,
                principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                secret: b"test scoped raft peer secret".to_vec(),
            })
            .unwrap()
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: 102,
                }),
                operation: ControlPlaneAuthOperation::RaftAppendEntries,
                issued_at_ms: Some(1),
                expires_at_ms: Some(2),
                sequence: Some(42),
                nonce: Vec::new(),
                payload: b"raft-payload".to_vec(),
            })
            .unwrap();
        assert_eq!(
            verify_signed_envelope(&unknown_store, &unknown_expired_envelope),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::UnknownCredential,
            }
        );

        let stale_store =
            ControlPlaneScopedCredentialStore::new(vec![ControlPlaneScopedCredential::new(
                ControlPlaneScopedCredentialInput {
                    cluster_id: "cluster-a".to_owned(),
                    credential_id: "raft-peer-key-a".to_owned(),
                    credential_version: 8,
                    principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                    secret: b"test scoped raft peer secret".to_vec(),
                },
            )
            .unwrap()])
            .unwrap();
        assert_eq!(
            verify_signed_envelope(&stale_store, &envelope),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::StaleCredential,
            }
        );

        let store = ControlPlaneScopedCredentialStore::new(vec![sample_credential()]).unwrap();
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(2_000),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::ReplayFreshnessFailure,
            }
        );
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(999),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::ReplayFreshnessFailure,
            }
        );
        let timestamp_less_envelope = sample_credential()
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: 102,
                }),
                operation: ControlPlaneAuthOperation::RaftAppendEntries,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: Some(42),
                nonce: Vec::new(),
                payload: b"raft-payload".to_vec(),
            })
            .unwrap();
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &timestamp_less_envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(1_500),
            }),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::ReplayFreshnessFailure,
            }
        );
        assert_eq!(
            store.verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &timestamp_less_envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: ControlPlaneAuthReplayPolicy::FencedByPayloadSemantics,
            }),
            ControlPlaneAuthDecision::Accepted {
                credential_id: "raft-peer-key-a".to_owned(),
                credential_version: 7,
            }
        );
    }

    #[test]
    fn control_plane_auth_scoped_credential_rejects_tampered_payload_or_secret() {
        let store = ControlPlaneScopedCredentialStore::new(vec![sample_credential()]).unwrap();
        let envelope = sample_signed_envelope();
        let tampered_payload = ControlPlaneAuthEnvelope::new(
            envelope.header().clone(),
            b"changed-payload".to_vec(),
            envelope.authenticator().to_vec(),
        )
        .unwrap();
        assert_eq!(
            verify_signed_envelope(&store, &tampered_payload),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::AuthenticatorMismatch,
            }
        );

        let wrong_secret_store =
            ControlPlaneScopedCredentialStore::new(vec![ControlPlaneScopedCredential::new(
                ControlPlaneScopedCredentialInput {
                    cluster_id: "cluster-a".to_owned(),
                    credential_id: "raft-peer-key-a".to_owned(),
                    credential_version: 7,
                    principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                    secret: b"wrong secret".to_vec(),
                },
            )
            .unwrap()])
            .unwrap();
        assert_eq!(
            verify_signed_envelope(&wrong_secret_store, &envelope),
            ControlPlaneAuthDecision::Rejected {
                reason: ControlPlaneAuthRejectionReason::AuthenticatorMismatch,
            }
        );
    }

    #[test]
    fn control_plane_auth_scoped_credentials_reject_bad_configuration_and_redact_secret() {
        assert!(
            ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "raft-peer-key-a".to_owned(),
                credential_version: 1,
                principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                secret: Vec::new(),
            })
            .is_err()
        );
        let credential = sample_credential();
        assert!(ControlPlaneScopedCredentialStore::new(vec![
            credential.clone(),
            credential.clone()
        ])
        .is_err());
        let debug = format!("{credential:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("test scoped raft peer secret"));
        let input_debug = format!(
            "{:?}",
            ControlPlaneScopedCredentialInput {
                cluster_id: "cluster-a".to_owned(),
                credential_id: "raft-peer-key-a".to_owned(),
                credential_version: 1,
                principal: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                secret: b"test scoped raft peer secret".to_vec(),
            }
        );
        assert!(input_debug.contains("<redacted>"));
        assert!(!input_debug.contains("test scoped raft peer secret"));

        let sign_input_debug = format!(
            "{:?}",
            ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: 102,
                }),
                operation: ControlPlaneAuthOperation::RaftAppendEntries,
                issued_at_ms: Some(1_000),
                expires_at_ms: Some(2_000),
                sequence: Some(42),
                nonce: b"test nonce".to_vec(),
                payload: b"secret raft payload".to_vec(),
            }
        );
        assert!(sign_input_debug.contains("payload_len"));
        assert!(sign_input_debug.contains("nonce_len"));
        assert!(!sign_input_debug.contains("secret raft payload"));
        assert!(!sign_input_debug.contains("test nonce"));

        let envelope = sample_signed_envelope();
        let verification_input_debug = format!(
            "{:?}",
            ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: "cluster-a",
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer { node_id: 102 }
                ),
                expected_operation: ControlPlaneAuthOperation::RaftAppendEntries,
                replay_policy: timestamp_replay_policy(1_500),
            }
        );
        assert!(verification_input_debug.contains("envelope_payload_len"));
        assert!(verification_input_debug.contains("envelope_authenticator_len"));
        assert!(!verification_input_debug.contains("raft-payload"));
        assert!(!verification_input_debug.contains("authenticator:"));

        let header_input = ControlPlaneAuthEnvelopeHeaderInput {
            cluster_id: "cluster-a".to_owned(),
            credential_id: "raft-peer-key-a".to_owned(),
            credential_version: 7,
            source: ControlPlaneAuthPrincipal::RaftPeer { node_id: 101 },
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 102,
            }),
            operation: ControlPlaneAuthOperation::RaftAppendEntries,
            issued_at_ms: Some(1_000),
            expires_at_ms: Some(2_000),
            sequence: Some(42),
            nonce: b"test nonce".to_vec(),
        };
        let header_input_debug = format!("{header_input:?}");
        assert!(header_input_debug.contains("nonce_len"));
        assert!(!header_input_debug.contains("test nonce"));

        let header = ControlPlaneAuthEnvelopeHeader::new(header_input).unwrap();
        let header_debug = format!("{header:?}");
        assert!(header_debug.contains("nonce_len"));
        assert!(!header_debug.contains("test nonce"));

        let envelope_debug = format!("{envelope:?}");
        assert!(envelope_debug.contains("payload_len"));
        assert!(envelope_debug.contains("authenticator_len"));
        assert!(!envelope_debug.contains("raft-payload"));
        assert!(!envelope_debug.contains("authenticator:"));
    }
}
