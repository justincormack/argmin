// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use crate::control_plane::ControlPlaneError;
use crate::control_plane_auth::{
    ControlPlaneAuthDecision, ControlPlaneAuthEnvelope, ControlPlaneAuthOperation,
    ControlPlaneAuthPrincipal, ControlPlaneAuthRejectionReason, ControlPlaneAuthReplayPolicy,
    ControlPlaneAuthService, ControlPlaneAuthSignInput, ControlPlaneAuthTarget,
    ControlPlaneAuthVerificationInput, ControlPlaneScopedCredential,
    ControlPlaneScopedCredentialStore,
};
use crate::storage_rpc::{
    decode_storage_rpc_frame, encode_storage_rpc_frame,
    validate_storage_rpc_request_frame_payload_limit, StorageRpcFrame, StorageRpcFrameError,
    StorageRpcMessageKind, STORAGE_RPC_MAX_FRAME_LEN,
};
use crate::NodeId;
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const STORAGE_RPC_AUTH_BINDING_MAGIC: &[u8; 8] = b"ARGSRPCB";
const STORAGE_RPC_AUTH_BINDING_VERSION: u16 = 2;
const STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN: usize = 64;
const STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_LEN: usize = 32;
const STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_DOMAIN: &[u8] =
    b"argmin/storage-rpc/request-transcript/v1\0";
const STORAGE_RPC_AUTH_BINDING_FIXED_LEN: usize = STORAGE_RPC_AUTH_BINDING_MAGIC.len()
    + 2
    + 8
    + 4
    + STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN
    + 4
    + 1
    + STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_LEN
    + 4;
const STORAGE_RPC_AUTH_MAX_BINDING_LEN: usize =
    STORAGE_RPC_AUTH_BINDING_FIXED_LEN + STORAGE_RPC_MAX_FRAME_LEN;
const STORAGE_RPC_AUTH_TRANSPORT_MAGIC: &[u8; 8] = b"ARGSRPCA";
const STORAGE_RPC_AUTH_TRANSPORT_VERSION: u16 = 1;
const STORAGE_RPC_AUTH_MAX_ENVELOPE_OVERHEAD: usize = 64 * 1024;
pub const STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN: usize =
    STORAGE_RPC_AUTH_MAX_BINDING_LEN + STORAGE_RPC_AUTH_MAX_ENVELOPE_OVERHEAD;
const STORAGE_RPC_AUTH_PRE_AUTH_BYTE_BUDGET: usize = STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN;

pub const STORAGE_RPC_AUTH_REPLAY_WINDOW_MS: u64 = 5_000;
pub const STORAGE_RPC_AUTH_ALLOWED_FUTURE_SKEW_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRpcTransportLimits {
    max_frame_bytes: usize,
    max_connections: usize,
    io_timeout: std::time::Duration,
}

impl StorageRpcTransportLimits {
    pub const DEFAULT: Self = Self {
        max_frame_bytes: STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN,
        max_connections: 1_024,
        io_timeout: std::time::Duration::from_secs(1),
    };

    pub fn new(
        max_frame_bytes: usize,
        max_connections: usize,
        io_timeout: std::time::Duration,
    ) -> Result<Self, ControlPlaneError> {
        if max_frame_bytes <= STORAGE_RPC_AUTH_MAX_ENVELOPE_OVERHEAD
            || max_frame_bytes > STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN
        {
            return Err(storage_rpc_auth_protocol_error(format!(
                "storage RPC max frame bytes must be in {}..={STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN}",
                STORAGE_RPC_AUTH_MAX_ENVELOPE_OVERHEAD + 1
            )));
        }
        if max_connections == 0 {
            return Err(storage_rpc_auth_protocol_error(
                "storage RPC max connections must be non-zero",
            ));
        }
        if io_timeout.is_zero() {
            return Err(storage_rpc_auth_protocol_error(
                "storage RPC I/O timeout must be non-zero",
            ));
        }
        Ok(Self {
            max_frame_bytes,
            max_connections,
            io_timeout,
        })
    }

    pub fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    pub fn max_connections(self) -> usize {
        self.max_connections
    }

    pub fn io_timeout(self) -> std::time::Duration {
        self.io_timeout
    }
}

#[derive(Clone)]
struct StorageRpcClientSigner {
    credential: ControlPlaneScopedCredential,
    topology_generation: u64,
    topology_digest: String,
    transport_limits: StorageRpcTransportLimits,
}

impl fmt::Debug for StorageRpcClientSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRpcClientSigner")
            .field("principal", self.credential.principal())
            .field("credential_id", &self.credential.credential_id())
            .field("credential_version", &self.credential.credential_version())
            .field("topology_generation", &self.topology_generation)
            .field("topology_digest", &self.topology_digest)
            .finish()
    }
}

impl StorageRpcClientSigner {
    fn new(
        credential: ControlPlaneScopedCredential,
        topology_generation: u64,
        topology_digest: impl Into<String>,
    ) -> Result<Self, ControlPlaneError> {
        let topology_digest = topology_digest.into();
        validate_topology(topology_generation, &topology_digest)?;
        Self::new_with_transport_limits(
            credential,
            topology_generation,
            topology_digest,
            StorageRpcTransportLimits::DEFAULT,
        )
    }

    fn new_with_transport_limits(
        credential: ControlPlaneScopedCredential,
        topology_generation: u64,
        topology_digest: impl Into<String>,
        transport_limits: StorageRpcTransportLimits,
    ) -> Result<Self, ControlPlaneError> {
        let topology_digest = topology_digest.into();
        validate_topology(topology_generation, &topology_digest)?;
        Ok(Self {
            credential,
            topology_generation,
            topology_digest,
            transport_limits,
        })
    }

    fn require_operation(&self, kind: StorageRpcMessageKind) -> Result<(), ControlPlaneError> {
        if principal_allows_operation(self.credential.principal(), kind) {
            return Ok(());
        }
        let _ = observability::emit_flight_event(
            "storage_rpc_client",
            "storage_rpc_client_operation_unauthorized",
            format!(
                "principal={:?} kind={}",
                self.credential.principal(),
                kind.operation_name()
            ),
        );
        Err(storage_rpc_auth_protocol_error(format!(
            "configured principal is not authorized for {}",
            kind.operation_name()
        )))
    }

    pub(crate) fn sign_request(
        &self,
        target_node_id: NodeId,
        now_ms: u64,
        frame: &StorageRpcFrame,
    ) -> Result<SignedStorageRpcRequest, ControlPlaneError> {
        self.require_operation(frame.kind)?;
        let expires_at_ms = now_ms
            .checked_add(STORAGE_RPC_AUTH_REPLAY_WINDOW_MS)
            .ok_or_else(|| storage_rpc_auth_protocol_error("storage RPC auth expiry overflowed"))?;
        let envelope = sign_storage_rpc_request(StorageRpcAuthRequestInput {
            credential: &self.credential,
            target_node_id,
            topology_generation: self.topology_generation,
            topology_digest: &self.topology_digest,
            issued_at_ms: now_ms,
            expires_at_ms,
            frame,
        })?;
        let proof = StorageRpcRequestProof {
            request_id: frame.request_id,
            kind: frame.kind,
            transcript: storage_rpc_request_transcript(&envelope),
        };
        Ok(SignedStorageRpcRequest { envelope, proof })
    }

    pub(crate) fn verify_response(
        &self,
        target_node_id: NodeId,
        now_ms: u64,
        request: &StorageRpcRequestProof,
        envelope: &[u8],
    ) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
        self.require_operation(request.kind)
            .map_err(|_| StorageRpcAuthRejectionReason::UnauthorizedRole)?;
        verify_storage_rpc_response(StorageRpcAuthResponseVerificationInput {
            request_credential: &self.credential,
            expected_target_node_id: target_node_id,
            expected_topology_generation: self.topology_generation,
            expected_topology_digest: &self.topology_digest,
            expected_request_id: request.request_id,
            expected_kind: request.kind,
            expected_request_transcript: &request.transcript,
            now_ms,
            max_replay_window_ms: STORAGE_RPC_AUTH_REPLAY_WINDOW_MS,
            allowed_future_skew_ms: STORAGE_RPC_AUTH_ALLOWED_FUTURE_SKEW_MS,
            envelope_bytes: envelope,
        })
    }
}

macro_rules! define_storage_rpc_client_capability {
    ($name:ident, $principal:pat, $label:literal) => {
        #[derive(Clone)]
        pub struct $name(StorageRpcClientSigner);

        impl $name {
            pub fn new(
                credential: ControlPlaneScopedCredential,
                topology_generation: u64,
                topology_digest: impl Into<String>,
            ) -> Result<Self, ControlPlaneError> {
                if !matches!(credential.principal(), $principal) {
                    return Err(storage_rpc_auth_protocol_error(concat!(
                        "storage RPC ",
                        $label,
                        " capability requires a matching principal"
                    )));
                }
                StorageRpcClientSigner::new(credential, topology_generation, topology_digest)
                    .map(Self)
            }

            pub fn new_with_transport_limits(
                credential: ControlPlaneScopedCredential,
                topology_generation: u64,
                topology_digest: impl Into<String>,
                transport_limits: StorageRpcTransportLimits,
            ) -> Result<Self, ControlPlaneError> {
                if !matches!(credential.principal(), $principal) {
                    return Err(storage_rpc_auth_protocol_error(concat!(
                        "storage RPC ",
                        $label,
                        " capability requires a matching principal"
                    )));
                }
                StorageRpcClientSigner::new_with_transport_limits(
                    credential,
                    topology_generation,
                    topology_digest,
                    transport_limits,
                )
                .map(Self)
            }

            pub fn transport_limits(&self) -> StorageRpcTransportLimits {
                self.0.transport_limits
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }
    };
}

define_storage_rpc_client_capability!(
    FrontendStorageRpcClientCapability,
    ControlPlaneAuthPrincipal::Frontend { .. },
    "frontend"
);
define_storage_rpc_client_capability!(
    MaintenanceStorageRpcClientCapability,
    ControlPlaneAuthPrincipal::LocalMaintenance { .. },
    "maintenance"
);
define_storage_rpc_client_capability!(
    StorageNodeStorageRpcClientCapability,
    ControlPlaneAuthPrincipal::StorageNode { .. }
        | ControlPlaneAuthPrincipal::StorageNodeProcess { .. },
    "storage-node"
);
define_storage_rpc_client_capability!(
    AdminStorageRpcClientCapability,
    ControlPlaneAuthPrincipal::Admin { .. },
    "admin"
);

#[derive(Clone, Debug)]
pub(crate) enum StorageRpcClientAuthConfig {
    Frontend(FrontendStorageRpcClientCapability),
    Maintenance(MaintenanceStorageRpcClientCapability),
    StorageNode(StorageNodeStorageRpcClientCapability),
    Admin(AdminStorageRpcClientCapability),
}

impl StorageRpcClientAuthConfig {
    fn signer(&self) -> &StorageRpcClientSigner {
        match self {
            Self::Frontend(capability) => &capability.0,
            Self::Maintenance(capability) => &capability.0,
            Self::StorageNode(capability) => &capability.0,
            Self::Admin(capability) => &capability.0,
        }
    }

    pub(crate) fn transport_limits(&self) -> StorageRpcTransportLimits {
        self.signer().transport_limits
    }

    pub(crate) fn sign_request(
        &self,
        target_node_id: NodeId,
        now_ms: u64,
        frame: &StorageRpcFrame,
    ) -> Result<SignedStorageRpcRequest, ControlPlaneError> {
        self.signer().sign_request(target_node_id, now_ms, frame)
    }

    pub(crate) fn verify_response(
        &self,
        target_node_id: NodeId,
        now_ms: u64,
        request: &StorageRpcRequestProof,
        envelope: &[u8],
    ) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
        self.signer()
            .verify_response(target_node_id, now_ms, request, envelope)
    }
}

impl From<FrontendStorageRpcClientCapability> for StorageRpcClientAuthConfig {
    fn from(value: FrontendStorageRpcClientCapability) -> Self {
        Self::Frontend(value)
    }
}

impl From<MaintenanceStorageRpcClientCapability> for StorageRpcClientAuthConfig {
    fn from(value: MaintenanceStorageRpcClientCapability) -> Self {
        Self::Maintenance(value)
    }
}

impl From<StorageNodeStorageRpcClientCapability> for StorageRpcClientAuthConfig {
    fn from(value: StorageNodeStorageRpcClientCapability) -> Self {
        Self::StorageNode(value)
    }
}

impl From<AdminStorageRpcClientCapability> for StorageRpcClientAuthConfig {
    fn from(value: AdminStorageRpcClientCapability) -> Self {
        Self::Admin(value)
    }
}

#[derive(Clone)]
pub struct StorageRpcServerAuthConfig {
    cluster_id: String,
    credentials: ControlPlaneScopedCredentialStore,
    topology_generation: u64,
    topology_digest: String,
    pre_auth_byte_budget: Arc<StorageRpcPreAuthByteBudget>,
    transport_limits: StorageRpcTransportLimits,
}

impl fmt::Debug for StorageRpcServerAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRpcServerAuthConfig")
            .field("cluster_id", &self.cluster_id)
            .field("credential_count", &self.credentials.credentials().len())
            .field("topology_generation", &self.topology_generation)
            .field("topology_digest", &self.topology_digest)
            .field(
                "pre_auth_byte_budget",
                &self.pre_auth_byte_budget.limit_bytes,
            )
            .finish()
    }
}

impl StorageRpcServerAuthConfig {
    pub fn new(
        cluster_id: impl Into<String>,
        credentials: ControlPlaneScopedCredentialStore,
        topology_generation: u64,
        topology_digest: impl Into<String>,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_id = cluster_id.into();
        if credentials
            .credentials()
            .iter()
            .any(|credential| credential.cluster_id() != cluster_id)
        {
            return Err(storage_rpc_auth_protocol_error(
                "storage RPC verifier credentials must match the configured cluster",
            ));
        }
        let topology_digest = topology_digest.into();
        validate_topology(topology_generation, &topology_digest)?;
        Ok(Self {
            cluster_id,
            credentials,
            topology_generation,
            topology_digest,
            pre_auth_byte_budget: Arc::new(StorageRpcPreAuthByteBudget::new(
                STORAGE_RPC_AUTH_PRE_AUTH_BYTE_BUDGET,
            )),
            transport_limits: StorageRpcTransportLimits::DEFAULT,
        })
    }

    pub fn with_transport_limits(mut self, transport_limits: StorageRpcTransportLimits) -> Self {
        self.pre_auth_byte_budget = Arc::new(StorageRpcPreAuthByteBudget::new(
            transport_limits.max_frame_bytes(),
        ));
        self.transport_limits = transport_limits;
        self
    }

    pub fn transport_limits(&self) -> StorageRpcTransportLimits {
        self.transport_limits
    }

    pub(crate) fn read_request_envelope<R: Read>(
        &self,
        reader: &mut R,
    ) -> io::Result<(Vec<u8>, StorageRpcPreAuthByteReservation)> {
        read_storage_rpc_auth_transport_frame_with_budget_and_limit(
            reader,
            &self.pre_auth_byte_budget,
            self.transport_limits.max_frame_bytes(),
        )
    }

    pub(crate) fn verify_request(
        &self,
        target_node_id: NodeId,
        now_ms: u64,
        envelope: &[u8],
    ) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
        verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
            verifier: &self.credentials,
            expected_cluster_id: &self.cluster_id,
            expected_target_node_id: target_node_id,
            expected_topology_generation: self.topology_generation,
            expected_topology_digest: &self.topology_digest,
            now_ms,
            max_replay_window_ms: STORAGE_RPC_AUTH_REPLAY_WINDOW_MS,
            allowed_future_skew_ms: STORAGE_RPC_AUTH_ALLOWED_FUTURE_SKEW_MS,
            envelope_bytes: envelope,
        })
    }

    pub(crate) fn sign_response(
        &self,
        request: &StorageRpcResponseSigningContext,
        target_node_id: NodeId,
        now_ms: u64,
        frame: &StorageRpcFrame,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let expires_at_ms = now_ms
            .checked_add(STORAGE_RPC_AUTH_REPLAY_WINDOW_MS)
            .ok_or_else(|| storage_rpc_auth_protocol_error("storage RPC auth expiry overflowed"))?;
        sign_storage_rpc_response(StorageRpcAuthResponseInput {
            request_credential: &request.credential,
            request_transcript: &request.request_transcript,
            target_node_id,
            topology_generation: self.topology_generation,
            topology_digest: &self.topology_digest,
            issued_at_ms: now_ms,
            expires_at_ms,
            frame,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcAuthRejectionReason {
    Malformed,
    WrongTopology,
    WrongTarget,
    WrongOperation,
    WrongRequest,
    UnauthorizedRole,
    Envelope(ControlPlaneAuthRejectionReason),
}

#[derive(Clone)]
pub(crate) struct VerifiedStorageRpcFrame {
    source: ControlPlaneAuthPrincipal,
    credential_id: String,
    credential_version: u64,
    credential: ControlPlaneScopedCredential,
    request_transcript: StorageRpcRequestTranscript,
    frame: StorageRpcFrame,
}

impl PartialEq for VerifiedStorageRpcFrame {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
            && self.credential_id == other.credential_id
            && self.credential_version == other.credential_version
            && self.request_transcript == other.request_transcript
            && self.frame == other.frame
    }
}

impl Eq for VerifiedStorageRpcFrame {}

impl fmt::Debug for VerifiedStorageRpcFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedStorageRpcFrame")
            .field("source", &self.source)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("request_id", &self.frame.request_id)
            .field("operation", &self.frame.kind.operation_name())
            .field("payload_len", &self.frame.payload.len())
            .finish()
    }
}

impl VerifiedStorageRpcFrame {
    #[cfg(test)]
    pub(crate) fn source(&self) -> &ControlPlaneAuthPrincipal {
        &self.source
    }

    #[cfg(test)]
    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[cfg(test)]
    pub(crate) fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub(crate) fn into_frame(self) -> StorageRpcFrame {
        self.frame
    }

    pub(crate) fn into_frame_and_response_signing_context(
        self,
    ) -> (StorageRpcFrame, StorageRpcResponseSigningContext) {
        (
            self.frame,
            StorageRpcResponseSigningContext {
                credential: self.credential,
                request_transcript: self.request_transcript,
            },
        )
    }
}

pub(crate) struct StorageRpcResponseSigningContext {
    credential: ControlPlaneScopedCredential,
    request_transcript: StorageRpcRequestTranscript,
}

pub(crate) struct SignedStorageRpcRequest {
    envelope: Vec<u8>,
    proof: StorageRpcRequestProof,
}

impl SignedStorageRpcRequest {
    #[cfg(test)]
    pub(crate) fn envelope(&self) -> &[u8] {
        &self.envelope
    }

    pub(crate) fn into_parts(self) -> (Vec<u8>, StorageRpcRequestProof) {
        (self.envelope, self.proof)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StorageRpcRequestProof {
    request_id: u64,
    kind: StorageRpcMessageKind,
    transcript: StorageRpcRequestTranscript,
}

impl fmt::Debug for StorageRpcRequestProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRpcRequestProof")
            .field("request_id", &self.request_id)
            .field("operation", &self.kind.operation_name())
            .field("transcript", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct StorageRpcRequestTranscript([u8; STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_LEN]);

impl fmt::Debug for StorageRpcRequestTranscript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StorageRpcRequestTranscript([REDACTED])")
    }
}

fn storage_rpc_request_transcript(envelope_bytes: &[u8]) -> StorageRpcRequestTranscript {
    let mut context = checksum::sha256::Sha256::new();
    context.update(STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_DOMAIN);
    context.update(&(envelope_bytes.len() as u64).to_be_bytes());
    context.update(envelope_bytes);
    StorageRpcRequestTranscript(context.finalize())
}

pub(crate) fn write_storage_rpc_auth_transport_frame<W: Write>(
    writer: &mut W,
    envelope: &[u8],
) -> io::Result<()> {
    if envelope.len() > STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "authenticated storage RPC frame exceeds the transport limit",
        ));
    }
    let len = u32::try_from(envelope.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "authenticated storage RPC frame length exceeds u32::MAX",
        )
    })?;
    writer.write_all(STORAGE_RPC_AUTH_TRANSPORT_MAGIC)?;
    writer.write_all(&STORAGE_RPC_AUTH_TRANSPORT_VERSION.to_be_bytes())?;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(&(!len).to_be_bytes())?;
    writer.write_all(envelope)?;
    writer.flush()
}

pub(crate) fn write_storage_rpc_auth_transport_frame_with_limit<W: Write>(
    writer: &mut W,
    envelope: &[u8],
    max_frame_bytes: usize,
) -> io::Result<()> {
    if envelope.len() > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "authenticated storage RPC frame exceeds the configured transport limit",
        ));
    }
    write_storage_rpc_auth_transport_frame(writer, envelope)
}

#[cfg(test)]
pub(crate) fn read_storage_rpc_auth_transport_frame<R: Read>(
    reader: &mut R,
) -> io::Result<Vec<u8>> {
    let len = read_storage_rpc_auth_transport_frame_len(reader)?;
    read_storage_rpc_auth_transport_frame_body(reader, len)
}

pub(crate) fn read_storage_rpc_auth_transport_frame_with_limit<R: Read>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> io::Result<Vec<u8>> {
    let len = read_storage_rpc_auth_transport_frame_len(reader)?;
    if len > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authenticated storage RPC frame exceeds the configured transport limit",
        ));
    }
    read_storage_rpc_auth_transport_frame_body(reader, len)
}

fn read_storage_rpc_auth_transport_frame_len<R: Read>(reader: &mut R) -> io::Result<usize> {
    let mut magic = [0_u8; STORAGE_RPC_AUTH_TRANSPORT_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic != *STORAGE_RPC_AUTH_TRANSPORT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid authenticated storage RPC transport magic",
        ));
    }
    let mut version = [0_u8; 2];
    reader.read_exact(&mut version)?;
    let version = u16::from_be_bytes(version);
    if version != STORAGE_RPC_AUTH_TRANSPORT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported authenticated storage RPC transport version",
        ));
    }
    let mut len = [0_u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len);
    let mut complement = [0_u8; 4];
    reader.read_exact(&mut complement)?;
    if u32::from_be_bytes(complement) != !len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authenticated storage RPC transport length check failed",
        ));
    }
    let len = len as usize;
    if len > STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authenticated storage RPC frame exceeds the transport limit",
        ));
    }
    Ok(len)
}

fn read_storage_rpc_auth_transport_frame_body<R: Read>(
    reader: &mut R,
    len: usize,
) -> io::Result<Vec<u8>> {
    let mut envelope = vec![0_u8; len];
    reader.read_exact(&mut envelope)?;
    Ok(envelope)
}

#[cfg(test)]
fn read_storage_rpc_auth_transport_frame_with_budget<R: Read>(
    reader: &mut R,
    budget: &Arc<StorageRpcPreAuthByteBudget>,
) -> io::Result<(Vec<u8>, StorageRpcPreAuthByteReservation)> {
    read_storage_rpc_auth_transport_frame_with_budget_and_limit(
        reader,
        budget,
        STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN,
    )
}

fn read_storage_rpc_auth_transport_frame_with_budget_and_limit<R: Read>(
    reader: &mut R,
    budget: &Arc<StorageRpcPreAuthByteBudget>,
    max_frame_bytes: usize,
) -> io::Result<(Vec<u8>, StorageRpcPreAuthByteReservation)> {
    let len = read_storage_rpc_auth_transport_frame_len(reader)?;
    if len > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authenticated storage RPC frame exceeds the configured transport limit",
        ));
    }
    let reservation = budget.reserve(len)?;
    let envelope = read_storage_rpc_auth_transport_frame_body(reader, len)?;
    Ok((envelope, reservation))
}

#[derive(Debug)]
struct StorageRpcPreAuthByteBudget {
    reserved_bytes: AtomicUsize,
    limit_bytes: usize,
}

impl StorageRpcPreAuthByteBudget {
    fn new(limit_bytes: usize) -> Self {
        Self {
            reserved_bytes: AtomicUsize::new(0),
            limit_bytes,
        }
    }

    fn reserve(
        self: &Arc<Self>,
        frame_bytes: usize,
    ) -> io::Result<StorageRpcPreAuthByteReservation> {
        let result =
            self.reserved_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                    reserved
                        .checked_add(frame_bytes)
                        .filter(|total| *total <= self.limit_bytes)
                });
        match result {
            Ok(_) => Ok(StorageRpcPreAuthByteReservation {
                budget: Arc::clone(self),
                frame_bytes,
            }),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "storage RPC pre-authentication frame budget exhausted",
            )),
        }
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> usize {
        self.reserved_bytes.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct StorageRpcPreAuthByteReservation {
    budget: Arc<StorageRpcPreAuthByteBudget>,
    frame_bytes: usize,
}

impl Drop for StorageRpcPreAuthByteReservation {
    fn drop(&mut self) {
        self.budget
            .reserved_bytes
            .fetch_sub(self.frame_bytes, Ordering::AcqRel);
    }
}

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct StorageRpcAuthBinding {
    topology_generation: u64,
    topology_digest: String,
    target_node_id: NodeId,
    request_transcript: Option<StorageRpcRequestTranscript>,
    frame: StorageRpcFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StorageRpcAuthBindingError {
    TruncatedMagic,
    UnknownMagic,
    TruncatedVersion,
    UnsupportedVersion(u16),
    Malformed,
}

pub(crate) struct StorageRpcAuthRequestInput<'a> {
    pub(crate) credential: &'a ControlPlaneScopedCredential,
    pub(crate) target_node_id: NodeId,
    pub(crate) topology_generation: u64,
    pub(crate) topology_digest: &'a str,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) frame: &'a StorageRpcFrame,
}

pub(crate) fn sign_storage_rpc_request(
    input: StorageRpcAuthRequestInput<'_>,
) -> Result<Vec<u8>, ControlPlaneError> {
    if !principal_allows_operation(input.credential.principal(), input.frame.kind) {
        return Err(storage_rpc_auth_protocol_error(format!(
            "principal {:?} is not authorized for {}",
            input.credential.principal(),
            input.frame.kind.operation_name()
        )));
    }
    let payload = encode_binding(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        None,
        input.frame,
    )?;
    sign_storage_rpc_request_binding(input, payload)
}

fn sign_storage_rpc_request_binding(
    input: StorageRpcAuthRequestInput<'_>,
    payload: Vec<u8>,
) -> Result<Vec<u8>, ControlPlaneError> {
    input
        .credential
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
            operation: ControlPlaneAuthOperation::StorageRpcRequest {
                message_kind: input.frame.kind as u16,
            },
            issued_at_ms: Some(input.issued_at_ms),
            expires_at_ms: Some(input.expires_at_ms),
            sequence: Some(input.frame.request_id),
            nonce: Vec::new(),
            payload,
        })?
        .encode_frame()
}

#[cfg(test)]
pub(crate) fn sign_storage_rpc_request_with_encoded_frame_for_test(
    input: StorageRpcAuthRequestInput<'_>,
    encoded_frame: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    if !principal_allows_operation(input.credential.principal(), input.frame.kind) {
        return Err(storage_rpc_auth_protocol_error(format!(
            "principal {:?} is not authorized for {}",
            input.credential.principal(),
            input.frame.kind.operation_name()
        )));
    }
    let payload = encode_binding_with_encoded_frame(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        None,
        encoded_frame,
    )?;
    sign_storage_rpc_request_binding(input, payload)
}

#[cfg(test)]
pub(crate) fn sign_storage_rpc_request_with_binding_version_for_test(
    input: StorageRpcAuthRequestInput<'_>,
    version: u16,
) -> Result<Vec<u8>, ControlPlaneError> {
    if !principal_allows_operation(input.credential.principal(), input.frame.kind) {
        return Err(storage_rpc_auth_protocol_error(format!(
            "principal {:?} is not authorized for {}",
            input.credential.principal(),
            input.frame.kind.operation_name()
        )));
    }
    let mut payload = encode_binding(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        None,
        input.frame,
    )?;
    let version_offset = STORAGE_RPC_AUTH_BINDING_MAGIC.len();
    payload[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
    sign_storage_rpc_request_binding(input, payload)
}

pub(crate) struct StorageRpcAuthRequestVerificationInput<'a> {
    pub(crate) verifier: &'a ControlPlaneScopedCredentialStore,
    pub(crate) expected_cluster_id: &'a str,
    pub(crate) expected_target_node_id: NodeId,
    pub(crate) expected_topology_generation: u64,
    pub(crate) expected_topology_digest: &'a str,
    pub(crate) now_ms: u64,
    pub(crate) max_replay_window_ms: u64,
    pub(crate) allowed_future_skew_ms: u64,
    pub(crate) envelope_bytes: &'a [u8],
}

pub(crate) fn verify_storage_rpc_request(
    input: StorageRpcAuthRequestVerificationInput<'_>,
) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
    let envelope = ControlPlaneAuthEnvelope::decode_frame(
        input.envelope_bytes,
        STORAGE_RPC_AUTH_MAX_BINDING_LEN,
    )
    .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let source = envelope.header().source().clone();
    let operation = envelope.header().operation();
    let ControlPlaneAuthOperation::StorageRpcRequest { message_kind } = operation else {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    };
    let decision = input
        .verifier
        .verify_envelope(ControlPlaneAuthVerificationInput {
            envelope: &envelope,
            expected_cluster_id: input.expected_cluster_id,
            expected_source: &source,
            expected_target: &ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
            expected_operation: operation,
            replay_policy: ControlPlaneAuthReplayPolicy::TimestampWindow {
                now_ms: input.now_ms,
                max_window_ms: input.max_replay_window_ms,
                allowed_future_skew_ms: input.allowed_future_skew_ms,
            },
        });
    let (credential_id, credential_version) = accepted_credential(decision)?;
    let credential = input
        .verifier
        .credentials()
        .iter()
        .find(|credential| {
            credential.cluster_id() == input.expected_cluster_id
                && credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
                && credential.principal() == &source
        })
        .cloned()
        .ok_or(StorageRpcAuthRejectionReason::Malformed)?;
    let binding =
        decode_binding(envelope.payload()).map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    validate_binding(
        &binding,
        input.expected_target_node_id,
        input.expected_topology_generation,
        input.expected_topology_digest,
        message_kind,
        envelope.header().sequence(),
        None,
    )?;
    validate_storage_rpc_request_frame_payload_limit(&binding.frame)
        .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    if !principal_allows_operation(&source, binding.frame.kind) {
        return Err(StorageRpcAuthRejectionReason::UnauthorizedRole);
    }
    Ok(VerifiedStorageRpcFrame {
        source,
        credential_id,
        credential_version,
        credential,
        request_transcript: storage_rpc_request_transcript(input.envelope_bytes),
        frame: binding.frame,
    })
}

struct StorageRpcAuthResponseInput<'a> {
    request_credential: &'a ControlPlaneScopedCredential,
    request_transcript: &'a StorageRpcRequestTranscript,
    target_node_id: NodeId,
    topology_generation: u64,
    topology_digest: &'a str,
    issued_at_ms: u64,
    expires_at_ms: u64,
    frame: &'a StorageRpcFrame,
}

fn sign_storage_rpc_response(
    input: StorageRpcAuthResponseInput<'_>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let response_credential = input.request_credential.storage_rpc_response_credential()?;
    let payload = encode_binding(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        Some(input.request_transcript),
        input.frame,
    )?;
    response_credential
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(input.request_credential.principal().clone()),
            operation: ControlPlaneAuthOperation::StorageRpcResponse {
                message_kind: input.frame.kind as u16,
            },
            issued_at_ms: Some(input.issued_at_ms),
            expires_at_ms: Some(input.expires_at_ms),
            sequence: Some(input.frame.request_id),
            nonce: Vec::new(),
            payload,
        })?
        .encode_frame()
}

struct StorageRpcAuthResponseVerificationInput<'a> {
    request_credential: &'a ControlPlaneScopedCredential,
    expected_target_node_id: NodeId,
    expected_topology_generation: u64,
    expected_topology_digest: &'a str,
    expected_request_id: u64,
    expected_kind: StorageRpcMessageKind,
    expected_request_transcript: &'a StorageRpcRequestTranscript,
    now_ms: u64,
    max_replay_window_ms: u64,
    allowed_future_skew_ms: u64,
    envelope_bytes: &'a [u8],
}

fn verify_storage_rpc_response(
    input: StorageRpcAuthResponseVerificationInput<'_>,
) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
    let response_credential = input
        .request_credential
        .storage_rpc_response_credential()
        .map_err(|_| StorageRpcAuthRejectionReason::UnauthorizedRole)?;
    let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential.clone()])
        .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let envelope = ControlPlaneAuthEnvelope::decode_frame(
        input.envelope_bytes,
        STORAGE_RPC_AUTH_MAX_BINDING_LEN,
    )
    .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let operation = ControlPlaneAuthOperation::StorageRpcResponse {
        message_kind: input.expected_kind as u16,
    };
    let source = ControlPlaneAuthPrincipal::Service {
        service: ControlPlaneAuthService::StorageRpc,
    };
    let decision = verifier.verify_envelope(ControlPlaneAuthVerificationInput {
        envelope: &envelope,
        expected_cluster_id: input.request_credential.cluster_id(),
        expected_source: &source,
        expected_target: &ControlPlaneAuthTarget::Principal(
            input.request_credential.principal().clone(),
        ),
        expected_operation: operation,
        replay_policy: ControlPlaneAuthReplayPolicy::TimestampWindow {
            now_ms: input.now_ms,
            max_window_ms: input.max_replay_window_ms,
            allowed_future_skew_ms: input.allowed_future_skew_ms,
        },
    });
    let (credential_id, credential_version) = accepted_credential(decision)?;
    let binding =
        decode_binding(envelope.payload()).map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    validate_binding(
        &binding,
        input.expected_target_node_id,
        input.expected_topology_generation,
        input.expected_topology_digest,
        input.expected_kind as u16,
        envelope.header().sequence(),
        Some(input.expected_request_transcript),
    )?;
    if binding.frame.request_id != input.expected_request_id {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    }
    Ok(VerifiedStorageRpcFrame {
        source,
        credential_id,
        credential_version,
        credential: response_credential,
        request_transcript: binding
            .request_transcript
            .ok_or(StorageRpcAuthRejectionReason::Malformed)?,
        frame: binding.frame,
    })
}

fn accepted_credential(
    decision: ControlPlaneAuthDecision,
) -> Result<(String, u64), StorageRpcAuthRejectionReason> {
    match decision {
        ControlPlaneAuthDecision::Accepted {
            credential_id,
            credential_version,
        } => Ok((credential_id, credential_version)),
        ControlPlaneAuthDecision::Rejected { reason } => {
            Err(StorageRpcAuthRejectionReason::Envelope(reason))
        }
    }
}

fn validate_binding(
    binding: &StorageRpcAuthBinding,
    expected_target_node_id: NodeId,
    expected_topology_generation: u64,
    expected_topology_digest: &str,
    expected_message_kind: u16,
    envelope_sequence: Option<u64>,
    expected_request_transcript: Option<&StorageRpcRequestTranscript>,
) -> Result<(), StorageRpcAuthRejectionReason> {
    if binding.target_node_id != expected_target_node_id {
        return Err(StorageRpcAuthRejectionReason::WrongTarget);
    }
    if binding.topology_generation != expected_topology_generation
        || binding.topology_digest != expected_topology_digest
    {
        return Err(StorageRpcAuthRejectionReason::WrongTopology);
    }
    if binding.frame.kind as u16 != expected_message_kind
        || envelope_sequence != Some(binding.frame.request_id)
    {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    }
    if binding.request_transcript.as_ref() != expected_request_transcript {
        return Err(StorageRpcAuthRejectionReason::WrongRequest);
    }
    Ok(())
}

fn principal_allows_operation(
    principal: &ControlPlaneAuthPrincipal,
    kind: StorageRpcMessageKind,
) -> bool {
    let roles = authorized_roles(kind);
    match principal {
        ControlPlaneAuthPrincipal::Frontend { .. } => roles.allows(StorageRpcCallerRole::Frontend),
        ControlPlaneAuthPrincipal::StorageNode { .. }
        | ControlPlaneAuthPrincipal::StorageNodeProcess { .. } => {
            roles.allows(StorageRpcCallerRole::StorageNode)
        }
        ControlPlaneAuthPrincipal::Admin { .. } => roles.allows(StorageRpcCallerRole::Admin),
        ControlPlaneAuthPrincipal::LocalMaintenance { .. } => {
            roles.allows(StorageRpcCallerRole::Maintenance)
        }
        ControlPlaneAuthPrincipal::RaftPeer { .. } | ControlPlaneAuthPrincipal::Service { .. } => {
            false
        }
    }
}

#[derive(Clone, Copy)]
enum StorageRpcCallerRole {
    Frontend,
    StorageNode,
    Admin,
    Maintenance,
}

#[derive(Clone, Copy)]
struct StorageRpcAuthorizedRoles(u8);

impl StorageRpcAuthorizedRoles {
    const FRONTEND: u8 = 1 << 0;
    const STORAGE_NODE: u8 = 1 << 1;
    const ADMIN: u8 = 1 << 2;
    const MAINTENANCE: u8 = 1 << 3;

    const ALL_INTERNAL: Self =
        Self(Self::FRONTEND | Self::STORAGE_NODE | Self::ADMIN | Self::MAINTENANCE);
    const FRONTEND_ONLY: Self = Self(Self::FRONTEND);
    const FRONTEND_MAINTENANCE: Self = Self(Self::FRONTEND | Self::MAINTENANCE);
    const FRONTEND_STORAGE: Self = Self(Self::FRONTEND | Self::STORAGE_NODE);
    const FRONTEND_STORAGE_MAINTENANCE: Self =
        Self(Self::FRONTEND | Self::STORAGE_NODE | Self::MAINTENANCE);
    const MAINTENANCE_ONLY: Self = Self(Self::MAINTENANCE);
    const STORAGE_MAINTENANCE: Self = Self(Self::STORAGE_NODE | Self::MAINTENANCE);

    fn allows(self, role: StorageRpcCallerRole) -> bool {
        let role = match role {
            StorageRpcCallerRole::Frontend => Self::FRONTEND,
            StorageRpcCallerRole::StorageNode => Self::STORAGE_NODE,
            StorageRpcCallerRole::Admin => Self::ADMIN,
            StorageRpcCallerRole::Maintenance => Self::MAINTENANCE,
        };
        self.0 & role != 0
    }
}

fn authorized_roles(kind: StorageRpcMessageKind) -> StorageRpcAuthorizedRoles {
    match kind {
        StorageRpcMessageKind::Health => StorageRpcAuthorizedRoles::ALL_INTERNAL,

        StorageRpcMessageKind::MetadataCommandReplicaState
        | StorageRpcMessageKind::MetadataCommandAcceptance
        | StorageRpcMessageKind::MetadataCommandAbandonAcceptance
        | StorageRpcMessageKind::MetadataCommandPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
        | StorageRpcMessageKind::MetadataCommandMaxLogIndex
        | StorageRpcMessageKind::MetadataCommandNextId
        | StorageRpcMessageKind::MetadataCommandPendingEnvelope
        | StorageRpcMessageKind::MetadataCommandPublicationStart
        | StorageRpcMessageKind::MetadataCommandValidateReplayState
        | StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        | StorageRpcMessageKind::MetadataCommandCheckpointCandidates
        | StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent
        | StorageRpcMessageKind::MetadataCommandLogCompact
        | StorageRpcMessageKind::MetadataCommandAbandoned
        | StorageRpcMessageKind::MetadataCommandRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandPendingSlotReplace
        | StorageRpcMessageKind::MetadataCommandApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandPgLockAcquire
        | StorageRpcMessageKind::MetadataCommandPgLockRelease
        | StorageRpcMessageKind::ShardAckLoad
        | StorageRpcMessageKind::ShardAckHistoricalLoad
        | StorageRpcMessageKind::ShardAckDelete => {
            StorageRpcAuthorizedRoles::FRONTEND_STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize
        | StorageRpcMessageKind::MetadataCommandTransferStateAdopt
        | StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        | StorageRpcMessageKind::MetadataCommandCheckpointExport
        | StorageRpcMessageKind::MetadataCommandAppliedLogHashes
        | StorageRpcMessageKind::MetadataCommandMatchingAppliedLog
        | StorageRpcMessageKind::MetadataCommandRetainedLogHashes
        | StorageRpcMessageKind::MetadataCommandRetainedLogEntries
        | StorageRpcMessageKind::ShardAckRecord
        | StorageRpcMessageKind::ShardAckValidate => StorageRpcAuthorizedRoles::FRONTEND_STORAGE,

        StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace
        | StorageRpcMessageKind::MetadataCommandRetainedAbortApply
        | StorageRpcMessageKind::MetadataCommandRetainedAbortFinish
        | StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord
        | StorageRpcMessageKind::ClusterMapHistoryReferenceSummary => {
            StorageRpcAuthorizedRoles::FRONTEND_STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::ShardRepairWrite
        | StorageRpcMessageKind::PlacedSegmentShardRepairRecord
        | StorageRpcMessageKind::PlacedSegmentShardRepairs
        | StorageRpcMessageKind::PlacedSegmentShardRepairResolve
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimError
        | StorageRpcMessageKind::PlacedSegmentShardBackfillRecord
        | StorageRpcMessageKind::PlacedSegmentShardBackfills
        | StorageRpcMessageKind::PlacedSegmentShardBackfillResolve
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError
        | StorageRpcMessageKind::PlacedSegmentShardBackfillCount
        | StorageRpcMessageKind::PlacedSegmentShardBackfillExists => {
            StorageRpcAuthorizedRoles::STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::ShardScavengerListFiles
        | StorageRpcMessageKind::ShardScavengerShardRows
        | StorageRpcMessageKind::ShardScavengerPayloadReferences
        | StorageRpcMessageKind::PlacedSegmentBackfillReferencePage
        | StorageRpcMessageKind::ShardScavengerObservationRecord
        | StorageRpcMessageKind::ShardScavengerObservations
        | StorageRpcMessageKind::ShardScavengerObservationResolve
        | StorageRpcMessageKind::LifecycleSweepBucketsList
        | StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList
        | StorageRpcMessageKind::LifecycleSweepRoots
        | StorageRpcMessageKind::LifecycleSweepClaimAcquire
        | StorageRpcMessageKind::LifecycleSweepClaimHeartbeat
        | StorageRpcMessageKind::LifecycleSweepClaimError
        | StorageRpcMessageKind::LifecycleSweepClaimRelease
        | StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimLoad
        | StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimGet => {
            StorageRpcAuthorizedRoles::MAINTENANCE_ONLY
        }

        StorageRpcMessageKind::ShardHistoricalRead => {
            StorageRpcAuthorizedRoles::FRONTEND_STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::ObjectPayloadReclaimExists => {
            StorageRpcAuthorizedRoles::FRONTEND_MAINTENANCE
        }

        StorageRpcMessageKind::ClaimHeartbeat
        | StorageRpcMessageKind::ClaimRelease
        | StorageRpcMessageKind::ProofRelease
        | StorageRpcMessageKind::ShardDelete
        | StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare
        | StorageRpcMessageKind::BucketWriteDrainBegin
        | StorageRpcMessageKind::BucketWriteDrainClear
        | StorageRpcMessageKind::BucketWriteDrainClearExpired
        | StorageRpcMessageKind::BucketWriteDrainGet
        | StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord
        | StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet
        | StorageRpcMessageKind::BucketWriteDrainHeartbeat
        | StorageRpcMessageKind::BucketWriteDrainExists
        | StorageRpcMessageKind::BucketWriteReservationsList
        | StorageRpcMessageKind::BucketDeleteFinalizeRoots
        | StorageRpcMessageKind::BucketDeleteBeginRoots
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimGet
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease
        | StorageRpcMessageKind::BucketDeleteReplicaHead => {
            StorageRpcAuthorizedRoles::FRONTEND_MAINTENANCE
        }

        StorageRpcMessageKind::BucketHeadRaw
        | StorageRpcMessageKind::BucketHeadInfo
        | StorageRpcMessageKind::ObjectVersionNext
        | StorageRpcMessageKind::BucketWriteReservationAcquire
        | StorageRpcMessageKind::BucketWriteReservationValidate
        | StorageRpcMessageKind::BucketWriteReservationRelease
        | StorageRpcMessageKind::BucketWriteReservationHeartbeat
        | StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild
        | StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild
        | StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild
        | StorageRpcMessageKind::ObjectLifecycleVersionListLoad
        | StorageRpcMessageKind::ObjectMultipartAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad
        | StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad
        | StorageRpcMessageKind::ObjectMultipartUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad
        | StorageRpcMessageKind::ObjectStreamUploadSessionLoad
        | StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad
        | StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate
        | StorageRpcMessageKind::BucketSubresourceGet
        | StorageRpcMessageKind::ObjectListPage
        | StorageRpcMessageKind::ObjectVersionListPage
        | StorageRpcMessageKind::ObjectMultipartUploadListPage
        | StorageRpcMessageKind::ObjectStreamUploadsList
        | StorageRpcMessageKind::ObjectStreamUploadsPgList
        | StorageRpcMessageKind::ObjectPayloadLeaseControl => {
            StorageRpcAuthorizedRoles::FRONTEND_MAINTENANCE
        }

        StorageRpcMessageKind::MetadataCommand
        | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
        | StorageRpcMessageKind::ShardWrite
        | StorageRpcMessageKind::ShardRead
        | StorageRpcMessageKind::ShardReadRange
        | StorageRpcMessageKind::ReadHandlesAcquire
        | StorageRpcMessageKind::ReadHandlesRelease
        | StorageRpcMessageKind::BucketCreateCommandBuild
        | StorageRpcMessageKind::ObjectGenerationNext
        | StorageRpcMessageKind::ObjectGenerationReservation
        | StorageRpcMessageKind::BucketSnapshotLoad
        | StorageRpcMessageKind::DirectPutCommitSnapshotLoad
        | StorageRpcMessageKind::DirectPutCommitCommandBuild
        | StorageRpcMessageKind::MultipartCompletionBarrierCommandBuild
        | StorageRpcMessageKind::ObjectReadAuthSubjectLoad
        | StorageRpcMessageKind::ObjectReadSnapshotLoad
        | StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad
        | StorageRpcMessageKind::ObjectMetadataPutCommandBuild
        | StorageRpcMessageKind::ObjectStreamUploadMatch
        | StorageRpcMessageKind::ObjectMultipartUploadMatch
        | StorageRpcMessageKind::ObjectStreamUploadCommandBuild
        | StorageRpcMessageKind::ObjectMultipartUploadCommandBuild
        | StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild
        | StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad
        | StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad
        | StorageRpcMessageKind::ObjectMultipartPartsList
        | StorageRpcMessageKind::ObjectMultipartManagementLookup
        | StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare
        | StorageRpcMessageKind::BucketMetadataControlPendingMatch
        | StorageRpcMessageKind::BucketMetadataControlCommandBuild
        | StorageRpcMessageKind::BucketList
        | StorageRpcMessageKind::BucketExecutionGenerations
        | StorageRpcMessageKind::BucketFastPathIdentities
        | StorageRpcMessageKind::BucketMarkDeletingCommandBuild => {
            StorageRpcAuthorizedRoles::FRONTEND_ONLY
        }
    }
}

#[cfg(test)]
fn recognized_storage_rpc_message_kinds() -> Vec<StorageRpcMessageKind> {
    (0..=u16::MAX)
        .filter_map(|value| StorageRpcMessageKind::from_u16(value).ok())
        .collect()
}

fn encode_binding(
    topology_generation: u64,
    topology_digest: &str,
    target_node_id: NodeId,
    request_transcript: Option<&StorageRpcRequestTranscript>,
    frame: &StorageRpcFrame,
) -> Result<Vec<u8>, ControlPlaneError> {
    let frame = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload)
        .map_err(storage_rpc_frame_protocol_error)?;
    encode_binding_with_encoded_frame(
        topology_generation,
        topology_digest,
        target_node_id,
        request_transcript,
        &frame,
    )
}

fn encode_binding_with_encoded_frame(
    topology_generation: u64,
    topology_digest: &str,
    target_node_id: NodeId,
    request_transcript: Option<&StorageRpcRequestTranscript>,
    frame: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    validate_topology(topology_generation, topology_digest)?;
    let frame_len = u32::try_from(frame.len()).map_err(|_| {
        storage_rpc_auth_protocol_error("encoded storage RPC frame length exceeds u32::MAX")
    })?;
    let mut out = Vec::with_capacity(STORAGE_RPC_AUTH_BINDING_FIXED_LEN + frame.len());
    out.extend_from_slice(STORAGE_RPC_AUTH_BINDING_MAGIC);
    out.extend_from_slice(&STORAGE_RPC_AUTH_BINDING_VERSION.to_be_bytes());
    out.extend_from_slice(&topology_generation.to_be_bytes());
    out.extend_from_slice(&(STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN as u32).to_be_bytes());
    out.extend_from_slice(topology_digest.as_bytes());
    out.extend_from_slice(&target_node_id.as_u32().to_be_bytes());
    match request_transcript {
        None => out.push(0),
        Some(request_transcript) => {
            out.push(1);
            out.extend_from_slice(&request_transcript.0);
        }
    }
    out.extend_from_slice(&frame_len.to_be_bytes());
    out.extend_from_slice(frame);
    Ok(out)
}

fn decode_binding(bytes: &[u8]) -> Result<StorageRpcAuthBinding, StorageRpcAuthBindingError> {
    if bytes.len() > STORAGE_RPC_AUTH_MAX_BINDING_LEN {
        return Err(StorageRpcAuthBindingError::Malformed);
    }
    if bytes.len() < STORAGE_RPC_AUTH_BINDING_MAGIC.len() {
        return Err(StorageRpcAuthBindingError::TruncatedMagic);
    }
    let mut reader = BindingReader::new(bytes);
    if reader.read_exact(STORAGE_RPC_AUTH_BINDING_MAGIC.len())? != STORAGE_RPC_AUTH_BINDING_MAGIC {
        return Err(StorageRpcAuthBindingError::UnknownMagic);
    }
    if reader.remaining() < 2 {
        return Err(StorageRpcAuthBindingError::TruncatedVersion);
    }
    let version = reader.read_u16()?;
    if version != STORAGE_RPC_AUTH_BINDING_VERSION {
        return Err(StorageRpcAuthBindingError::UnsupportedVersion(version));
    }
    let topology_generation = reader.read_u64()?;
    let digest_len = reader.read_u32()? as usize;
    if digest_len != STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN {
        return Err(StorageRpcAuthBindingError::Malformed);
    }
    let topology_digest = std::str::from_utf8(reader.read_exact(digest_len)?)
        .map_err(|_| StorageRpcAuthBindingError::Malformed)?
        .to_owned();
    if validate_topology(topology_generation, &topology_digest).is_err() {
        return Err(StorageRpcAuthBindingError::Malformed);
    }
    let target_node_id = NodeId::new(reader.read_u32()?);
    let request_transcript = match reader.read_u8()? {
        0 => None,
        1 => {
            let bytes = reader
                .read_exact(STORAGE_RPC_AUTH_REQUEST_TRANSCRIPT_LEN)?
                .try_into()
                .map_err(|_| StorageRpcAuthBindingError::Malformed)?;
            Some(StorageRpcRequestTranscript(bytes))
        }
        _ => return Err(StorageRpcAuthBindingError::Malformed),
    };
    let frame_len = reader.read_u32()? as usize;
    if frame_len > STORAGE_RPC_MAX_FRAME_LEN {
        return Err(StorageRpcAuthBindingError::Malformed);
    }
    let frame = decode_storage_rpc_frame(reader.read_exact(frame_len)?)
        .map_err(|_| StorageRpcAuthBindingError::Malformed)?;
    reader.finish()?;
    Ok(StorageRpcAuthBinding {
        topology_generation,
        topology_digest,
        target_node_id,
        request_transcript,
        frame,
    })
}

fn validate_topology(
    topology_generation: u64,
    topology_digest: &str,
) -> Result<(), ControlPlaneError> {
    if topology_generation == 0 {
        return Err(storage_rpc_auth_protocol_error(
            "topology generation is zero",
        ));
    }
    if topology_digest.len() != STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN
        || !topology_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_rpc_auth_protocol_error(
            "topology digest is not 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn storage_rpc_frame_protocol_error(error: StorageRpcFrameError) -> ControlPlaneError {
    storage_rpc_auth_protocol_error(format!("invalid storage RPC frame: {error}"))
}

fn storage_rpc_auth_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::rpc_protocol(format!("storage RPC auth envelope: {}", message.into()))
}

struct BindingReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BindingReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], StorageRpcAuthBindingError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(StorageRpcAuthBindingError::Malformed)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StorageRpcAuthBindingError::Malformed)?;
        self.offset = end;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, StorageRpcAuthBindingError> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| StorageRpcAuthBindingError::Malformed)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u8(&mut self) -> Result<u8, StorageRpcAuthBindingError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, StorageRpcAuthBindingError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| StorageRpcAuthBindingError::Malformed)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, StorageRpcAuthBindingError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| StorageRpcAuthBindingError::Malformed)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn finish(self) -> Result<(), StorageRpcAuthBindingError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageRpcAuthBindingError::Malformed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_auth::ControlPlaneScopedCredentialInput;
    use std::io::Cursor;

    struct FlushFailureWriter;

    impl Write for FlushFailureWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected authenticated frame flush failure",
            ))
        }
    }

    const TOPOLOGY_DIGEST: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn hex_bytes(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    const ROUTINE_CHECKPOINT_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::MetadataCommandReplicaState,
        StorageRpcMessageKind::MetadataCommandCheckpointCandidates,
        StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent,
        StorageRpcMessageKind::MetadataCommandLogCompact,
    ];

    const FOREGROUND_RETAINED_PAYLOAD_READ_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::ShardHistoricalRead,
        StorageRpcMessageKind::ObjectPayloadReclaimExists,
    ];

    const LIFECYCLE_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::LifecycleSweepBucketsList,
        StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList,
        StorageRpcMessageKind::LifecycleSweepRoots,
        StorageRpcMessageKind::LifecycleSweepClaimAcquire,
        StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
        StorageRpcMessageKind::LifecycleSweepClaimError,
        StorageRpcMessageKind::LifecycleSweepClaimRelease,
        StorageRpcMessageKind::BucketHeadInfo,
        StorageRpcMessageKind::BucketSubresourceGet,
        StorageRpcMessageKind::ObjectListPage,
        StorageRpcMessageKind::ObjectVersionListPage,
        StorageRpcMessageKind::ObjectMultipartUploadListPage,
        StorageRpcMessageKind::ObjectStreamUploadsList,
        StorageRpcMessageKind::ObjectStreamUploadsPgList,
        StorageRpcMessageKind::BucketWriteReservationAcquire,
        StorageRpcMessageKind::BucketWriteReservationValidate,
        StorageRpcMessageKind::BucketWriteReservationRelease,
        StorageRpcMessageKind::BucketWriteReservationHeartbeat,
        StorageRpcMessageKind::ObjectVersionNext,
        StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
        StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
        StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
        StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
        StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
        StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
        StorageRpcMessageKind::ObjectMultipartUploadLoad,
        StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad,
        StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad,
        StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad,
        StorageRpcMessageKind::ObjectMultipartAbortCommandBuild,
        StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad,
        StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare,
        StorageRpcMessageKind::ObjectStreamUploadSessionLoad,
        StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad,
        StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
        StorageRpcMessageKind::MetadataCommandAcceptance,
        StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
        StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
        StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
        StorageRpcMessageKind::MetadataCommandMaxLogIndex,
        StorageRpcMessageKind::MetadataCommandNextId,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope,
        StorageRpcMessageKind::MetadataCommandPublicationStart,
        StorageRpcMessageKind::MetadataCommandValidateReplayState,
        StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
        StorageRpcMessageKind::MetadataCommandAbandoned,
        StorageRpcMessageKind::MetadataCommandRecordAbandoned,
        StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
        StorageRpcMessageKind::MetadataCommandApplyAndRecord,
        StorageRpcMessageKind::MetadataCommandPgLockAcquire,
        StorageRpcMessageKind::MetadataCommandPgLockRelease,
    ];

    const PAYLOAD_RECLAIM_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::BucketHeadRaw,
        StorageRpcMessageKind::ObjectPayloadReclaimExists,
        StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
        StorageRpcMessageKind::ObjectPayloadReclaimRoot,
        StorageRpcMessageKind::ObjectPayloadReclaimLoad,
        StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimGet,
        StorageRpcMessageKind::ObjectPayloadLeaseControl,
        StorageRpcMessageKind::ShardAckLoad,
        StorageRpcMessageKind::ShardAckHistoricalLoad,
        StorageRpcMessageKind::ShardAckDelete,
        StorageRpcMessageKind::ShardDelete,
    ];

    const BUCKET_DELETE_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::BucketDeleteReplicaHead,
        StorageRpcMessageKind::BucketHeadRaw,
        StorageRpcMessageKind::BucketWriteDrainBegin,
        StorageRpcMessageKind::BucketWriteDrainClear,
        StorageRpcMessageKind::BucketWriteDrainClearExpired,
        StorageRpcMessageKind::BucketWriteDrainGet,
        StorageRpcMessageKind::BucketWriteDrainHeartbeat,
        StorageRpcMessageKind::BucketWriteDrainExists,
        StorageRpcMessageKind::BucketWriteReservationsList,
        StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
        StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet,
        StorageRpcMessageKind::BucketDeleteBeginRoots,
        StorageRpcMessageKind::BucketDeleteFinalizeRoots,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimGet,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
    ];

    fn credential(principal: ControlPlaneAuthPrincipal) -> ControlPlaneScopedCredential {
        credential_with_id(principal, "caller-key")
    }

    fn credential_with_id(
        principal: ControlPlaneAuthPrincipal,
        credential_id: &str,
    ) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "storage-auth-cluster".to_owned(),
            credential_id: credential_id.to_owned(),
            credential_version: 1,
            principal,
            secret: b"storage-auth-secret".to_vec(),
        })
        .unwrap()
    }

    fn frame(kind: StorageRpcMessageKind) -> StorageRpcFrame {
        StorageRpcFrame {
            request_id: 17,
            kind,
            payload: if kind == StorageRpcMessageKind::Health {
                Vec::new()
            } else {
                b"payload".to_vec()
            },
        }
    }

    #[test]
    fn storage_rpc_auth_transport_frame_round_trips() {
        let envelope = b"authenticated-storage-rpc";
        let mut encoded = Vec::new();
        write_storage_rpc_auth_transport_frame(&mut encoded, envelope).unwrap();

        assert_eq!(
            read_storage_rpc_auth_transport_frame(&mut Cursor::new(encoded)).unwrap(),
            envelope
        );
    }

    #[test]
    fn storage_rpc_auth_transport_rejects_unsupported_versions() {
        for version in [0, STORAGE_RPC_AUTH_TRANSPORT_VERSION + 1] {
            let mut encoded = Vec::new();
            write_storage_rpc_auth_transport_frame(&mut encoded, b"payload").unwrap();
            let version_offset = STORAGE_RPC_AUTH_TRANSPORT_MAGIC.len();
            encoded[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());

            let error =
                read_storage_rpc_auth_transport_frame(&mut Cursor::new(encoded)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "unsupported authenticated storage RPC transport version"
            );
        }
    }

    #[test]
    fn storage_rpc_auth_binding_has_typed_marker_and_version_failures() {
        assert_eq!(
            decode_binding(b""),
            Err(StorageRpcAuthBindingError::TruncatedMagic)
        );
        assert_eq!(
            decode_binding(b"ARGSRPC"),
            Err(StorageRpcAuthBindingError::TruncatedMagic)
        );
        assert_eq!(
            decode_binding(b"XRGSRPCB"),
            Err(StorageRpcAuthBindingError::UnknownMagic)
        );
        assert_eq!(
            decode_binding(STORAGE_RPC_AUTH_BINDING_MAGIC),
            Err(StorageRpcAuthBindingError::TruncatedVersion)
        );
        let mut truncated_version = STORAGE_RPC_AUTH_BINDING_MAGIC.to_vec();
        truncated_version.push(0);
        assert_eq!(
            decode_binding(&truncated_version),
            Err(StorageRpcAuthBindingError::TruncatedVersion)
        );
        for version in [0, STORAGE_RPC_AUTH_BINDING_VERSION + 1] {
            let mut encoded = STORAGE_RPC_AUTH_BINDING_MAGIC.to_vec();
            encoded.extend_from_slice(&version.to_be_bytes());
            assert_eq!(
                decode_binding(&encoded),
                Err(StorageRpcAuthBindingError::UnsupportedVersion(version))
            );
        }
    }

    #[test]
    fn storage_rpc_auth_binding_matches_frozen_v2_request_response_and_transcript() {
        const V21_FRAME: &[u8] = &[
            24, 0, 0, 0, 97, 114, 103, 109, 105, 110, 45, 115, 116, 111, 114, 97, 103, 101, 45,
            114, 112, 99, 45, 102, 114, 97, 109, 101, 21, 0, 8, 7, 6, 5, 4, 3, 2, 1, 3, 0, 3, 0, 0,
            0, 146, 204, 0, 105, 155, 117, 90, 173, 97, 98, 99,
        ];
        let request = encode_binding_with_encoded_frame(
            0x0102_0304_0506_0708,
            TOPOLOGY_DIGEST,
            NodeId::new(0x1122_3344),
            None,
            V21_FRAME,
        )
        .unwrap();
        assert_eq!(
            hex_bytes(&request),
            concat!(
                "4152475352504342000201020304050607080000004030313233343536373839",
                "6162636465663031323334353637383961626364656630313233343536373839",
                "6162636465663031323334353637383961626364656611223344000000003718",
                "0000006172676d696e2d73746f726167652d7270632d6672616d651500080706",
                "050403020103000300000092cc00699b755aad616263"
            )
        );

        let transcript = storage_rpc_request_transcript(b"fixed authenticated request envelope");
        assert_eq!(
            hex_bytes(&transcript.0),
            "673e6f836a950c4b2f304c2e1dd16de07e4ff6972e497106e064deeb34e434f1"
        );
        let response = encode_binding_with_encoded_frame(
            0x0102_0304_0506_0708,
            TOPOLOGY_DIGEST,
            NodeId::new(0x1122_3344),
            Some(&transcript),
            V21_FRAME,
        )
        .unwrap();
        assert_eq!(
            hex_bytes(&response),
            concat!(
                "4152475352504342000201020304050607080000004030313233343536373839",
                "6162636465663031323334353637383961626364656630313233343536373839",
                "616263646566303132333435363738396162636465661122334401673e6f836a",
                "950c4b2f304c2e1dd16de07e4ff6972e497106e064deeb34e434f100000037",
                "180000006172676d696e2d73746f726167652d7270632d6672616d6515000807",
                "06050403020103000300000092cc00699b755aad616263"
            )
        );

        let decoded_request = decode_binding(&request).unwrap();
        assert!(decoded_request.request_transcript.is_none());
        assert_eq!(decoded_request.frame.payload, b"abc");
        let decoded_response = decode_binding(&response).unwrap();
        assert_eq!(decoded_response.request_transcript, Some(transcript));
        assert_eq!(decoded_response.frame.payload, b"abc");
    }

    #[test]
    fn authenticator_valid_storage_rpc_auth_binding_rejects_unsupported_versions() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let kind = StorageRpcMessageKind::BucketCreateCommandBuild;
        let request = frame(kind);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        for version in [0, STORAGE_RPC_AUTH_BINDING_VERSION + 1] {
            let mut encoded =
                encode_binding(9, TOPOLOGY_DIGEST, NodeId::new(7), None, &request).unwrap();
            let version_offset = STORAGE_RPC_AUTH_BINDING_MAGIC.len();
            encoded[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
            let signed = credential
                .sign_envelope(ControlPlaneAuthSignInput {
                    target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
                    operation: ControlPlaneAuthOperation::StorageRpcRequest {
                        message_kind: kind as u16,
                    },
                    issued_at_ms: Some(1_000),
                    expires_at_ms: Some(2_000),
                    sequence: Some(request.request_id),
                    nonce: Vec::new(),
                    payload: encoded,
                })
                .unwrap()
                .encode_frame()
                .unwrap();
            assert!(matches!(
                verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                    verifier: &verifier,
                    expected_cluster_id: credential.cluster_id(),
                    expected_target_node_id: NodeId::new(7),
                    expected_topology_generation: 9,
                    expected_topology_digest: TOPOLOGY_DIGEST,
                    now_ms: 1_500,
                    max_replay_window_ms: 1_000,
                    allowed_future_skew_ms: 0,
                    envelope_bytes: &signed,
                }),
                Err(StorageRpcAuthRejectionReason::Malformed)
            ));
        }
    }

    #[test]
    fn storage_rpc_auth_transport_frame_reports_flush_failure() {
        let error = write_storage_rpc_auth_transport_frame(
            &mut FlushFailureWriter,
            b"authenticated-storage-rpc",
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn storage_rpc_auth_transport_rejects_corrupt_length_before_reading_payload() {
        let mut encoded = Vec::new();
        write_storage_rpc_auth_transport_frame(&mut encoded, b"payload").unwrap();
        let complement_offset = STORAGE_RPC_AUTH_TRANSPORT_MAGIC.len() + 2 + 4;
        encoded[complement_offset] ^= 1;

        let error = read_storage_rpc_auth_transport_frame(&mut Cursor::new(encoded)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("length check failed"));
    }

    #[test]
    fn storage_rpc_auth_transport_rejects_legacy_storage_frame() {
        let legacy = encode_storage_rpc_frame(17, StorageRpcMessageKind::Health, &[]).unwrap();

        let error = read_storage_rpc_auth_transport_frame(&mut Cursor::new(legacy)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("transport magic"));
    }

    #[test]
    fn storage_rpc_auth_transport_reserves_process_bytes_before_body_allocation() {
        let mut encoded = Vec::new();
        write_storage_rpc_auth_transport_frame(&mut encoded, b"12345678").unwrap();
        let mut reader = Cursor::new(encoded);
        let budget = Arc::new(StorageRpcPreAuthByteBudget::new(4));

        let error =
            read_storage_rpc_auth_transport_frame_with_budget(&mut reader, &budget).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(
            reader.position() as usize,
            STORAGE_RPC_AUTH_TRANSPORT_MAGIC.len() + 2 + 4 + 4
        );
        assert_eq!(budget.reserved_bytes(), 0);
    }

    #[test]
    fn storage_rpc_auth_transport_holds_process_bytes_until_verification_finishes() {
        let mut encoded = Vec::new();
        write_storage_rpc_auth_transport_frame(&mut encoded, b"12345678").unwrap();
        let budget = Arc::new(StorageRpcPreAuthByteBudget::new(8));

        let (envelope, reservation) =
            read_storage_rpc_auth_transport_frame_with_budget(&mut Cursor::new(encoded), &budget)
                .unwrap();

        assert_eq!(envelope, b"12345678");
        assert_eq!(budget.reserved_bytes(), 8);
        assert_eq!(
            budget.reserve(1).unwrap_err().kind(),
            io::ErrorKind::OutOfMemory
        );
        drop(reservation);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    #[test]
    fn storage_rpc_client_capabilities_are_role_typed_and_exact() {
        let frontend = credential_with_id(
            ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-1".to_owned(),
            },
            "frontend-key",
        );
        let maintenance = credential_with_id(
            ControlPlaneAuthPrincipal::LocalMaintenance {
                process_id: "maintenance-1".to_owned(),
            },
            "maintenance-key",
        );
        let frontend_auth = StorageRpcClientAuthConfig::from(
            FrontendStorageRpcClientCapability::new(frontend, 9, TOPOLOGY_DIGEST).unwrap(),
        );
        let maintenance_auth = StorageRpcClientAuthConfig::from(
            MaintenanceStorageRpcClientCapability::new(maintenance, 9, TOPOLOGY_DIGEST).unwrap(),
        );

        let frontend_envelope = frontend_auth
            .sign_request(
                NodeId::new(7),
                1_000,
                &frame(StorageRpcMessageKind::BucketCreateCommandBuild),
            )
            .unwrap();
        assert!(frontend_auth
            .sign_request(
                NodeId::new(7),
                1_000,
                &frame(StorageRpcMessageKind::LifecycleSweepRoots),
            )
            .is_err());
        let maintenance_envelope = maintenance_auth
            .sign_request(
                NodeId::new(7),
                1_000,
                &frame(StorageRpcMessageKind::LifecycleSweepRoots),
            )
            .unwrap();

        assert!(matches!(
            ControlPlaneAuthEnvelope::decode_frame(
                frontend_envelope.envelope(),
                STORAGE_RPC_AUTH_MAX_BINDING_LEN
            )
            .unwrap()
            .header()
            .source(),
            ControlPlaneAuthPrincipal::Frontend { .. }
        ));
        assert!(matches!(
            ControlPlaneAuthEnvelope::decode_frame(
                maintenance_envelope.envelope(),
                STORAGE_RPC_AUTH_MAX_BINDING_LEN
            )
            .unwrap()
            .header()
            .source(),
            ControlPlaneAuthPrincipal::LocalMaintenance { .. }
        ));
    }

    #[test]
    fn storage_rpc_client_capability_rejects_mismatched_principal() {
        let credential = credential_with_id(
            ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-1".to_owned(),
            },
            "frontend-key",
        );
        let error =
            MaintenanceStorageRpcClientCapability::new(credential, 9, TOPOLOGY_DIGEST).unwrap_err();
        assert!(error.retained_diagnostic_contains("maintenance capability"));
    }

    fn sign_request(
        credential: &ControlPlaneScopedCredential,
        kind: StorageRpcMessageKind,
    ) -> Vec<u8> {
        sign_storage_rpc_request(StorageRpcAuthRequestInput {
            credential,
            target_node_id: NodeId::new(7),
            topology_generation: 9,
            topology_digest: TOPOLOGY_DIGEST,
            issued_at_ms: 1_000,
            expires_at_ms: 2_000,
            frame: &frame(kind),
        })
        .unwrap()
    }

    fn assert_authenticated_workflow(
        credential: &ControlPlaneScopedCredential,
        workflow: &'static str,
        kinds: &[StorageRpcMessageKind],
    ) {
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        for &kind in kinds {
            let signed = sign_request(credential, kind);
            let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: NodeId::new(7),
                expected_topology_generation: 9,
                expected_topology_digest: TOPOLOGY_DIGEST,
                now_ms: 1_500,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: &signed,
            })
            .unwrap_or_else(|error| {
                panic!("{workflow} operation {kind:?} failed authentication: {error:?}")
            });
            assert_eq!(verified.source(), credential.principal());
            assert_eq!(verified.into_frame().kind, kind);
        }
    }

    #[test]
    fn storage_rpc_auth_request_round_trip_binds_frame_and_identity() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::BucketCreateCommandBuild);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
            verifier: &verifier,
            expected_cluster_id: credential.cluster_id(),
            expected_target_node_id: NodeId::new(7),
            expected_topology_generation: 9,
            expected_topology_digest: TOPOLOGY_DIGEST,
            now_ms: 1_500,
            max_replay_window_ms: 1_000,
            allowed_future_skew_ms: 0,
            envelope_bytes: &signed,
        })
        .unwrap();

        assert_eq!(verified.source(), credential.principal());
        assert_eq!(verified.credential_id(), "caller-key");
        assert_eq!(verified.credential_version(), 1);
        assert_eq!(
            verified.into_frame(),
            frame(StorageRpcMessageKind::BucketCreateCommandBuild)
        );
    }

    #[test]
    fn storage_rpc_auth_rejects_wrong_topology_target_and_freshness() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::Health);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verify = |target_node_id, topology_generation, topology_digest, now_ms| {
            verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: target_node_id,
                expected_topology_generation: topology_generation,
                expected_topology_digest: topology_digest,
                now_ms,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: &signed,
            })
            .unwrap_err()
        };

        assert_eq!(
            verify(NodeId::new(8), 9, TOPOLOGY_DIGEST, 1_500),
            StorageRpcAuthRejectionReason::WrongTarget
        );
        assert_eq!(
            verify(NodeId::new(7), 10, TOPOLOGY_DIGEST, 1_500),
            StorageRpcAuthRejectionReason::WrongTopology
        );
        assert_eq!(
            verify(
                NodeId::new(7),
                9,
                "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                1_500,
            ),
            StorageRpcAuthRejectionReason::WrongTopology
        );
        assert_eq!(
            verify(NodeId::new(7), 9, TOPOLOGY_DIGEST, 2_000),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::ReplayFreshnessFailure
            )
        );
    }

    #[test]
    fn storage_rpc_auth_rejects_tampering_and_operation_relabeling() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::Health);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let mut tampered = signed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let verify = |bytes: &[u8]| {
            verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: NodeId::new(7),
                expected_topology_generation: 9,
                expected_topology_digest: TOPOLOGY_DIGEST,
                now_ms: 1_500,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: bytes,
            })
            .unwrap_err()
        };
        assert_eq!(
            verify(&tampered),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::AuthenticatorMismatch
            )
        );

        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(&signed, STORAGE_RPC_AUTH_MAX_BINDING_LEN)
                .unwrap();
        let relabeled = ControlPlaneAuthEnvelope::new(
            crate::control_plane_auth::ControlPlaneAuthEnvelopeInput {
                header: crate::control_plane_auth::ControlPlaneAuthEnvelopeHeader::new(
                    crate::control_plane_auth::ControlPlaneAuthEnvelopeHeaderInput {
                        cluster_id: envelope.header().cluster_id().to_owned(),
                        credential_id: envelope.header().credential_id().to_owned(),
                        credential_version: envelope.header().credential_version(),
                        source: envelope.header().source().clone(),
                        target: envelope.header().target().clone(),
                        operation: ControlPlaneAuthOperation::StorageRpcRequest {
                            message_kind: StorageRpcMessageKind::ShardRead as u16,
                        },
                        issued_at_ms: envelope.header().issued_at_ms(),
                        expires_at_ms: envelope.header().expires_at_ms(),
                        sequence: envelope.header().sequence(),
                        nonce: envelope.header().nonce().to_vec(),
                    },
                )
                .unwrap(),
                payload: envelope.payload().to_vec(),
                authenticator: envelope.authenticator().to_vec(),
            },
        )
        .unwrap()
        .encode_frame()
        .unwrap();
        assert_eq!(
            verify(&relabeled),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::AuthenticatorMismatch
            )
        );
    }

    #[test]
    fn storage_rpc_auth_role_matrix_covers_every_wire_kind() {
        let frontend = ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        };
        let storage = ControlPlaneAuthPrincipal::StorageNode {
            node_id: NodeId::new(3),
            incarnation: 4,
        };
        let admin = ControlPlaneAuthPrincipal::Admin {
            instance_id: "admin-1".to_owned(),
        };
        let maintenance = ControlPlaneAuthPrincipal::LocalMaintenance {
            process_id: "maintenance-11".to_owned(),
        };
        let raft = ControlPlaneAuthPrincipal::RaftPeer { node_id: 5 };
        let service = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::StorageRpc,
        };
        let kinds = recognized_storage_rpc_message_kinds();

        assert_eq!(kinds.len(), 169, "every wire kind must be classified");
        for kind in kinds {
            assert!(
                [&frontend, &storage, &admin, &maintenance,]
                    .into_iter()
                    .any(|principal| principal_allows_operation(principal, kind)),
                "wire kind {kind:?} has no authorized internal caller"
            );
            assert_eq!(
                principal_allows_operation(&admin, kind),
                kind == StorageRpcMessageKind::Health,
                "admin must remain health-only for {kind:?}"
            );
            assert!(!principal_allows_operation(&raft, kind));
            assert!(!principal_allows_operation(&service, kind));
        }

        assert!(principal_allows_operation(
            &admin,
            StorageRpcMessageKind::Health
        ));
        assert!(!principal_allows_operation(
            &admin,
            StorageRpcMessageKind::ShardWrite
        ));
        assert!(!principal_allows_operation(
            &admin,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        ));
        assert!(!principal_allows_operation(
            &maintenance,
            StorageRpcMessageKind::ShardWrite
        ));
        assert!(!principal_allows_operation(
            &maintenance,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        ));
        assert!(!principal_allows_operation(
            &frontend,
            StorageRpcMessageKind::ShardRepairWrite
        ));
        assert!(!principal_allows_operation(
            &frontend,
            StorageRpcMessageKind::ShardScavengerObservationResolve
        ));
        assert!(principal_allows_operation(
            &storage,
            StorageRpcMessageKind::ShardRepairWrite
        ));
        assert!(!principal_allows_operation(
            &storage,
            StorageRpcMessageKind::ShardWrite
        ));
    }

    #[test]
    fn storage_rpc_auth_maintenance_workflows_sign_and_verify_every_required_operation() {
        let credential = credential(ControlPlaneAuthPrincipal::LocalMaintenance {
            process_id: "maintenance-11".to_owned(),
        });

        assert_authenticated_workflow(
            &credential,
            "routine metadata checkpoint",
            ROUTINE_CHECKPOINT_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "lifecycle sweep",
            LIFECYCLE_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "payload reclaim",
            PAYLOAD_RECLAIM_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "bucket-delete finalization",
            BUCKET_DELETE_MAINTENANCE_WORKFLOW,
        );
    }

    #[test]
    fn storage_rpc_auth_foreground_retained_payload_reads_use_frontend_capability() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });

        assert_authenticated_workflow(
            &credential,
            "foreground retained payload read",
            FOREGROUND_RETAINED_PAYLOAD_READ_WORKFLOW,
        );
        assert!(!principal_allows_operation(
            credential.principal(),
            StorageRpcMessageKind::ShardRepairWrite
        ));
        assert!(!principal_allows_operation(
            credential.principal(),
            StorageRpcMessageKind::ObjectPayloadReclaimLoad
        ));
    }

    #[test]
    fn storage_rpc_auth_verifier_rejects_valid_mac_for_unauthorized_role() {
        for (principal, kind) in [
            (
                ControlPlaneAuthPrincipal::Admin {
                    instance_id: "admin-1".to_owned(),
                },
                StorageRpcMessageKind::ShardWrite,
            ),
            (
                ControlPlaneAuthPrincipal::LocalMaintenance {
                    process_id: "maintenance-11".to_owned(),
                },
                StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            ),
            (
                ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-1".to_owned(),
                },
                StorageRpcMessageKind::ShardRepairWrite,
            ),
        ] {
            let credential = credential(principal);
            let frame = frame(kind);
            let payload = encode_binding(9, TOPOLOGY_DIGEST, NodeId::new(7), None, &frame).unwrap();
            let signed = credential
                .sign_envelope(ControlPlaneAuthSignInput {
                    target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
                    operation: ControlPlaneAuthOperation::StorageRpcRequest {
                        message_kind: kind as u16,
                    },
                    issued_at_ms: Some(1_000),
                    expires_at_ms: Some(2_000),
                    sequence: Some(frame.request_id),
                    nonce: Vec::new(),
                    payload,
                })
                .unwrap()
                .encode_frame()
                .unwrap();
            let verifier =
                ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();

            assert_eq!(
                verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                    verifier: &verifier,
                    expected_cluster_id: credential.cluster_id(),
                    expected_target_node_id: NodeId::new(7),
                    expected_topology_generation: 9,
                    expected_topology_digest: TOPOLOGY_DIGEST,
                    now_ms: 1_500,
                    max_replay_window_ms: 1_000,
                    allowed_future_skew_ms: 0,
                    envelope_bytes: &signed,
                }),
                Err(StorageRpcAuthRejectionReason::UnauthorizedRole),
                "valid MAC must not authorize {kind:?}"
            );
        }
    }

    #[test]
    fn storage_rpc_auth_response_is_bound_to_exact_request_direction_and_caller() {
        let caller_credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let client_auth = StorageRpcClientAuthConfig::from(
            FrontendStorageRpcClientCapability::new(caller_credential.clone(), 9, TOPOLOGY_DIGEST)
                .unwrap(),
        );
        let server_auth = StorageRpcServerAuthConfig::new(
            caller_credential.cluster_id(),
            ControlPlaneScopedCredentialStore::new(vec![caller_credential.clone()]).unwrap(),
            9,
            TOPOLOGY_DIGEST,
        )
        .unwrap();
        let request = frame(StorageRpcMessageKind::ObjectReadSnapshotLoad);
        let (request_envelope, request_proof) = client_auth
            .sign_request(NodeId::new(7), 1_000, &request)
            .unwrap()
            .into_parts();
        let verified_request = server_auth
            .verify_request(NodeId::new(7), 1_500, &request_envelope)
            .unwrap();
        let (verified_request_frame, response_signing_context) =
            verified_request.into_frame_and_response_signing_context();
        assert_eq!(verified_request_frame, request);

        let response = frame(StorageRpcMessageKind::ObjectReadSnapshotLoad);
        let signed = server_auth
            .sign_response(&response_signing_context, NodeId::new(7), 1_500, &response)
            .unwrap();
        let verified = client_auth
            .verify_response(NodeId::new(7), 2_000, &request_proof, &signed)
            .unwrap();
        assert_eq!(verified.into_frame(), response);

        let wrong_caller = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-2".to_owned(),
        });
        let wrong_client_auth = StorageRpcClientAuthConfig::from(
            FrontendStorageRpcClientCapability::new(wrong_caller, 9, TOPOLOGY_DIGEST).unwrap(),
        );
        assert!(matches!(
            wrong_client_auth.verify_response(NodeId::new(7), 2_000, &request_proof, &signed),
            Err(StorageRpcAuthRejectionReason::Envelope(_))
        ));

        let mut different_request = request;
        different_request.payload = b"different-object".to_vec();
        let (different_request_envelope, different_request_proof) = client_auth
            .sign_request(NodeId::new(7), 1_000, &different_request)
            .unwrap()
            .into_parts();
        let decoded_request = ControlPlaneAuthEnvelope::decode_frame(
            &request_envelope,
            STORAGE_RPC_AUTH_MAX_BINDING_LEN,
        )
        .unwrap();
        let decoded_different_request = ControlPlaneAuthEnvelope::decode_frame(
            &different_request_envelope,
            STORAGE_RPC_AUTH_MAX_BINDING_LEN,
        )
        .unwrap();
        assert_eq!(decoded_request.header(), decoded_different_request.header());
        let request_binding = decode_binding(decoded_request.payload()).unwrap();
        let different_request_binding =
            decode_binding(decoded_different_request.payload()).unwrap();
        assert_eq!(request_binding.topology_generation, 9);
        assert_eq!(
            request_binding.topology_generation,
            different_request_binding.topology_generation
        );
        assert_eq!(
            request_binding.topology_digest,
            different_request_binding.topology_digest
        );
        assert_eq!(
            request_binding.target_node_id,
            different_request_binding.target_node_id
        );
        assert!(request_binding.request_transcript.is_none());
        assert!(different_request_binding.request_transcript.is_none());
        assert_eq!(
            request_binding.frame.request_id,
            different_request_binding.frame.request_id
        );
        assert_eq!(
            request_binding.frame.kind,
            different_request_binding.frame.kind
        );
        assert_ne!(
            request_binding.frame.payload,
            different_request_binding.frame.payload
        );
        assert_eq!(
            different_request_binding.frame.payload,
            different_request.payload
        );
        assert_eq!(
            client_auth.verify_response(NodeId::new(7), 2_000, &different_request_proof, &signed),
            Err(StorageRpcAuthRejectionReason::WrongRequest)
        );
    }

    #[test]
    fn storage_rpc_auth_debug_redacts_frame_and_authenticator() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::BucketCreateCommandBuild);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
            verifier: &verifier,
            expected_cluster_id: credential.cluster_id(),
            expected_target_node_id: NodeId::new(7),
            expected_topology_generation: 9,
            expected_topology_digest: TOPOLOGY_DIGEST,
            now_ms: 1_500,
            max_replay_window_ms: 1_000,
            allowed_future_skew_ms: 0,
            envelope_bytes: &signed,
        })
        .unwrap();
        let debug = format!("{verified:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("\"payload\""));
        assert!(!debug.contains("storage-auth-secret"));
        assert!(!debug.contains("authenticator"));
        assert_eq!(
            format!("{:?}", verified.request_transcript),
            "StorageRpcRequestTranscript([REDACTED])"
        );
    }
}
