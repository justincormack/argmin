// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{
    AuthenticatedUnixControlPlaneClient, ClusterRuntimeMapSnapshot, ControlPlaneError,
    ControlPlaneRuntimeMapSource, PgMetadataProof, PgMetadataTransferProof, PgRouteSnapshot,
    UnavailablePgReconciliationWork, UnixControlPlaneClient,
};
use crate::control_plane_auth::ControlPlaneScopedCredential;
use crate::control_plane_client_bootstrap::{
    ControlPlaneAdminClientBootstrap, ControlPlaneAdminCredentialBinding,
};
use crate::control_plane_command::{
    CommittedUnavailablePgStagingAuthorization, FinalizeMetadataTransferStagingGenerationRequest,
    MetadataTransferStagingCleanupDisposition, MetadataTransferStagingTombstoneBinding,
    UnavailablePgStagingIntentAuthorizationRequest, UnavailablePgStagingPublicationBinding,
    UnavailablePgTransitionInstallRequest,
};
use crate::control_plane_service_client::ControlPlaneFrontendClient;
use crate::node_client::UnixStorageNodeClient;
use crate::peering::PgMetadataTransferArtifact;
use crate::pg_store::{
    decode_staged_metadata_transfer_artifact, decode_staging_evidence,
    encode_staged_metadata_transfer_artifact, MetadataTransferStagingIntent,
    MetadataTransferStagingNodeIdentity, MetadataTransferStagingReceipt,
    METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
};
#[cfg(test)]
use crate::pg_store::{
    MetadataTransferStagingLimits, MetadataTransferStagingStore,
    METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
};
use crate::storage_rpc_auth::StorageRpcClientAuthConfig;
use crate::storage_rpc_transport::StorageRpcClientEndpoint;
use crate::{
    BucketName, ClusterEpoch, EcShape, FrontendStorageRpcClientCapability,
    LivePgMetadataTransferStorageRpcClientCapability, LocalUnixStorageNodeClientAdmissionSettings,
    NodeId, ObjectKey, ObjectPayloadPlacementDiagnostic, PgId, PgState, StorageCluster,
};

enum LivePgMetadataTransferReadDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
    AuthenticatedAdmin(AuthenticatedUnixControlPlaneClient),
}

enum LivePgMetadataTransferAdminDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

/// Prevents accidental `Clone` or `Debug` derives on authority-bearing live
/// transfer capabilities. The public types remain opaque and linear across
/// the process/storage boundary.
struct OpaqueLivePgMetadataTransferCapabilityMarker;

/// One authority-bound control-plane capability for live metadata transfer.
///
/// Read and mutation credentials are bound to one retained transport client
/// and their authenticated cluster identities are validated.
/// The full runtime-map source interface is deliberately not implemented.
///
/// ```compile_fail
/// use storage::LivePgMetadataTransferControlPlaneClient;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<LivePgMetadataTransferControlPlaneClient>();
/// ```
///
/// ```compile_fail
/// use storage::LivePgMetadataTransferControlPlaneClient;
///
/// fn require_debug<T: std::fmt::Debug>() {}
/// require_debug::<LivePgMetadataTransferControlPlaneClient>();
/// ```
pub struct LivePgMetadataTransferControlPlaneClient {
    read: LivePgMetadataTransferReadDispatch,
    admin: LivePgMetadataTransferAdminDispatch,
    _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
}

impl LivePgMetadataTransferControlPlaneClient {
    pub(crate) fn new(
        client: UnixControlPlaneClient,
        read_credential: Option<ControlPlaneScopedCredential>,
        admin_credential: Option<ControlPlaneScopedCredential>,
    ) -> Result<Self, LivePgMetadataTransferError> {
        match (&read_credential, &admin_credential) {
            (Some(read), Some(admin)) if read.cluster_id() != admin.cluster_id() => {
                return Err(LivePgMetadataTransferError::new(
                    "live PG metadata transfer read and admin credentials target different control-plane cluster identities"
                        .to_owned(),
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(LivePgMetadataTransferError::new(
                    "live PG metadata transfer read and admin clients use different authentication modes"
                        .to_owned(),
                ));
            }
            (Some(_), Some(_)) | (None, None) => {}
        }

        let read = match read_credential {
            Some(credential) => LivePgMetadataTransferReadDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client.clone(), credential),
            ),
            None => LivePgMetadataTransferReadDispatch::Plain(client.clone()),
        };
        let admin = match admin_credential {
            Some(credential) => LivePgMetadataTransferAdminDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
            None => LivePgMetadataTransferAdminDispatch::Plain(client),
        };
        Ok(Self {
            read,
            admin,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
        })
    }

    pub fn with_frontend_client(
        frontend: &ControlPlaneFrontendClient,
        admin: &ControlPlaneAdminCredentialBinding,
    ) -> Result<Self, LivePgMetadataTransferError> {
        let (client, read_credential) = frontend.retained_transport_and_credential();
        Self::new(client, read_credential, admin.credential.clone())
    }

    pub fn with_admin_client(
        admin: &ControlPlaneAdminClientBootstrap,
    ) -> Result<Self, LivePgMetadataTransferError> {
        let read = match &admin.credential {
            Some(credential) => LivePgMetadataTransferReadDispatch::AuthenticatedAdmin(
                AuthenticatedUnixControlPlaneClient::new(admin.client.clone(), credential.clone()),
            ),
            None => LivePgMetadataTransferReadDispatch::Plain(admin.client.clone()),
        };
        let admin_dispatch = match &admin.credential {
            Some(credential) => LivePgMetadataTransferAdminDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(admin.client.clone(), credential.clone()),
            ),
            None => LivePgMetadataTransferAdminDispatch::Plain(admin.client.clone()),
        };
        Ok(Self {
            read,
            admin: admin_dispatch,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
        })
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match &self.read {
            LivePgMetadataTransferReadDispatch::Plain(client) => {
                client.pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            LivePgMetadataTransferReadDispatch::Authenticated(client) => {
                client.pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            LivePgMetadataTransferReadDispatch::AuthenticatedAdmin(client) => {
                client.admin_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
        }
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match &self.read {
            LivePgMetadataTransferReadDispatch::Plain(client) => {
                client.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            LivePgMetadataTransferReadDispatch::Authenticated(client) => {
                client.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            LivePgMetadataTransferReadDispatch::AuthenticatedAdmin(client) => {
                client.admin_serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
        }
    }

    fn fence_with_source_lease(
        &self,
        pg_id: PgId,
        unavailable_transition: Option<
            &crate::control_plane::UnavailablePgTransitionMutationBinding,
        >,
    ) -> Result<crate::control_plane::FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        match &self.admin {
            LivePgMetadataTransferAdminDispatch::Plain(client) => match unavailable_transition {
                Some(binding) => client
                    .fence_unavailable_pg_transition_runtime_map_with_source_lease_checked(binding),
                None => client
                    .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id),
            },
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => {
                let now_ms = crate::clock::current_time_millis();
                match unavailable_transition {
                    Some(binding) => client
                        .fence_unavailable_pg_transition_runtime_map_with_source_lease_checked(
                            binding, now_ms,
                        ),
                    None => client
                        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
                            pg_id, now_ms,
                        ),
                }
            }
        }
    }

    fn refresh_fence(
        &self,
        pg_id: PgId,
        unavailable_transition: Option<
            &crate::control_plane::UnavailablePgTransitionMutationBinding,
        >,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Ok(self
            .fence_with_source_lease(pg_id, unavailable_transition)?
            .into_parts()
            .0)
    }

    fn install_transfer(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match &self.admin {
            LivePgMetadataTransferAdminDispatch::Plain(client) => client
                .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                ),
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => {
                let now_ms = crate::clock::current_time_millis();
                client.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                    now_ms,
                )
            }
        }
    }
}

enum LivePgMetadataTransferStorageAuth {
    Frontend(FrontendStorageRpcClientCapability),
    LivePgMetadataTransfer(LivePgMetadataTransferStorageRpcClientCapability),
}

#[cfg(test)]
type StagingArtifactPublishFailure = (
    LivePgMetadataTransferFailureDisposition,
    Option<std::sync::mpsc::SyncSender<PgId>>,
);

#[cfg(test)]
type StagingArtifactPublishFailures = std::sync::Mutex<
    std::collections::BTreeMap<NodeId, std::collections::VecDeque<StagingArtifactPublishFailure>>,
>;

#[cfg(test)]
struct InProcessMetadataTransferStagingStores {
    data_dir: std::path::PathBuf,
    stores: std::sync::Mutex<
        std::collections::BTreeMap<
            MetadataTransferStagingNodeIdentity,
            Arc<MetadataTransferStagingStore>,
        >,
    >,
}

#[cfg(test)]
impl InProcessMetadataTransferStagingStores {
    fn new(data_dir: std::path::PathBuf) -> Self {
        Self {
            data_dir,
            stores: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    fn store(
        &self,
        identity: MetadataTransferStagingNodeIdentity,
    ) -> Result<Arc<MetadataTransferStagingStore>, crate::pg_store::MetadataTransferStagingError>
    {
        let mut stores = self
            .stores
            .lock()
            .expect("in-process metadata-transfer staging stores poisoned");
        if let Some(store) = stores.get(&identity) {
            return Ok(Arc::clone(store));
        }
        let node_id = identity.node_id();
        let limits = MetadataTransferStagingLimits::new(
            256,
            METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
            4 * 1024 * 1024 * 1024,
        )?;
        let store = Arc::new(MetadataTransferStagingStore::open(
            &self
                .data_dir
                .join(format!("staging-node-{}", node_id.as_u32())),
            identity.clone(),
            limits,
        )?);
        stores.insert(identity, Arc::clone(&store));
        Ok(store)
    }
}

enum LivePgMetadataTransferStorageTransport {
    Unix {
        auth: Option<LivePgMetadataTransferStorageAuth>,
    },
    ConfiguredEndpoints {
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: LivePgMetadataTransferStorageAuth,
    },
    #[cfg(test)]
    InProcess {
        data_dir: std::path::PathBuf,
        staging_stores: Arc<InProcessMetadataTransferStagingStores>,
        generation: std::sync::atomic::AtomicU64,
        export_route_refresh_failures: std::sync::atomic::AtomicU64,
        import_route_refresh_failures: std::sync::atomic::AtomicU64,
        import_pending_command_failures: std::sync::atomic::AtomicU64,
        staging_artifact_publish_failures: StagingArtifactPublishFailures,
        staging_artifact_read_failures: std::sync::Mutex<
            std::collections::BTreeMap<
                NodeId,
                std::collections::VecDeque<LivePgMetadataTransferFailureDisposition>,
            >,
        >,
    },
}

/// Injected interruption points for deterministic process crash/restart tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LivePgMetadataTransferFailpoint {
    AfterFence,
    AfterTransferInstall,
    AfterImport,
}

impl LivePgMetadataTransferFailpoint {
    fn label(self) -> &'static str {
        match self {
            Self::AfterFence => "after-fence",
            Self::AfterTransferInstall => "after-transfer-install",
            Self::AfterImport => "after-import",
        }
    }
}

/// Logical operator-visible result of a completed live PG metadata transfer.
pub struct LivePgMetadataTransferSummary {
    source_node_id: u32,
    source_epoch: u64,
    destination_epoch: u64,
    imported_proof: PgMetadataProof,
    already_completed: bool,
}

/// Linear ownership of an exported unavailable-PG artifact before destination
/// staging. Only storage can inspect the artifact or construct its durable
/// authorization subject.
pub(crate) struct PreparedUnavailablePgMetadataTransfer {
    work: UnavailablePgReconciliationWork,
    prepared: Box<PreparedLivePgMetadataTransfer>,
    intent: MetadataTransferStagingIntent,
    staged_artifact: Vec<u8>,
}

/// Linear ownership of a prepared artifact after its exact authorization
/// batch is known to be committed by the control plane.
pub(crate) struct AuthorizedUnavailablePgMetadataTransfer {
    work: UnavailablePgReconciliationWork,
    intent: MetadataTransferStagingIntent,
    authorizations: Vec<CommittedUnavailablePgStagingAuthorization>,
    artifact: PgMetadataTransferArtifact,
    staged_artifact: Vec<u8>,
    target_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
}

/// Linear ownership of a fully published unavailable-PG artifact and the
/// exact receipt-bound destination-install member derived from it.
pub(crate) struct StagedUnavailablePgMetadataTransfer {
    work: UnavailablePgReconciliationWork,
    intent: MetadataTransferStagingIntent,
    authorizations: Vec<CommittedUnavailablePgStagingAuthorization>,
    artifact: PgMetadataTransferArtifact,
    target_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
    publications: Vec<UnavailablePgStagingPublicationBinding>,
}

/// Durable post-install ownership needed for activation cleanup. Artifact bytes
/// are deliberately absent: tombstoning remains recoverable after every
/// destination has already deleted its staged copy.
pub(crate) struct CleanupUnavailablePgMetadataTransfer {
    work: UnavailablePgReconciliationWork,
    intent: MetadataTransferStagingIntent,
    authorizations: Vec<CommittedUnavailablePgStagingAuthorization>,
    disposition: MetadataTransferStagingCleanupDisposition,
    install: Option<UnavailablePgTransitionInstallRequest>,
}

pub(crate) struct PublishedUnavailablePgMetadataTransfer {
    authorizations: Vec<CommittedUnavailablePgStagingAuthorization>,
    target_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
    publications: Vec<UnavailablePgStagingPublicationBinding>,
}

enum RecoveredAuthorizedStagingArtifact {
    Published(Box<AuthorizedUnavailablePgMetadataTransfer>),
    Absent(Option<LivePgMetadataTransferFailure>),
}

/// Exact all-destination cleanup evidence for one staged transfer. This state
/// is constructible only after every authorization obligation has returned a
/// canonical tombstone receipt.
pub(crate) struct TombstonedUnavailablePgMetadataTransfer {
    work: UnavailablePgReconciliationWork,
    cleanup: FinalizeMetadataTransferStagingGenerationRequest,
}

impl PreparedUnavailablePgMetadataTransfer {
    pub(crate) fn work(&self) -> &UnavailablePgReconciliationWork {
        &self.work
    }

    pub(crate) fn artifact_length(&self) -> u64 {
        self.intent.artifact_length()
    }

    pub(crate) fn authorization_request(&self) -> UnavailablePgStagingIntentAuthorizationRequest {
        UnavailablePgStagingIntentAuthorizationRequest {
            unavailable_transition: self.work.mutation_binding().clone(),
            staging_generation: self.intent.staging_generation(),
            artifact_target_epoch: self.prepared.install_member.expected_destination_epoch,
            artifact_digest: self.intent.artifact_digest(),
            artifact_length: self.intent.artifact_length(),
            artifact_format_version: self.intent.artifact_format_version(),
        }
    }

    pub(crate) fn bind_committed_authorization(
        self,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<AuthorizedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        let request = self.authorization_request();
        let mut authorizations = Vec::with_capacity(self.work.destination_acting_set().len());
        for destination_node_id in self.work.destination_acting_set().iter().copied() {
            authorizations.push(
                snapshot
                    .committed_unavailable_pg_staging_authorization(
                        &request,
                        destination_node_id,
                    )
                    .map_err(|error| {
                        LivePgMetadataTransferError::new(format!(
                            "prepared staging authorization is not committed for destination {}: {}",
                            destination_node_id.as_u32(),
                            error.retained_diagnostic_message()
                        ))
                    })?,
            );
        }
        let PreparedUnavailablePgMetadataTransfer {
            work,
            prepared,
            intent,
            staged_artifact,
        } = self;
        let PreparedLivePgMetadataTransfer {
            artifact,
            install_member,
            ..
        } = *prepared;
        Ok(AuthorizedUnavailablePgMetadataTransfer {
            work,
            intent,
            authorizations,
            artifact,
            staged_artifact,
            target_epoch: install_member.expected_destination_epoch,
            transfer: install_member.transfer,
        })
    }
}

impl AuthorizedUnavailablePgMetadataTransfer {
    pub(crate) fn work(&self) -> &UnavailablePgReconciliationWork {
        &self.work
    }

    #[cfg(test)]
    pub(crate) fn authorization_request(&self) -> UnavailablePgStagingIntentAuthorizationRequest {
        UnavailablePgStagingIntentAuthorizationRequest {
            unavailable_transition: self.work.mutation_binding().clone(),
            staging_generation: self.intent.staging_generation(),
            artifact_target_epoch: self.target_epoch,
            artifact_digest: self.intent.artifact_digest(),
            artifact_length: self.intent.artifact_length(),
            artifact_format_version: self.intent.artifact_format_version(),
        }
    }

    pub(crate) fn bind_publications(
        self,
        published: PublishedUnavailablePgMetadataTransfer,
    ) -> Result<StagedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        if published.authorizations != self.authorizations {
            return Err(LivePgMetadataTransferError::new(
                "staging publications do not match their prepared authorization".to_owned(),
            ));
        }
        let AuthorizedUnavailablePgMetadataTransfer {
            work,
            intent,
            authorizations,
            artifact,
            staged_artifact: _,
            target_epoch: _,
            transfer: _,
        } = self;
        Ok(StagedUnavailablePgMetadataTransfer {
            work,
            intent,
            authorizations,
            artifact,
            target_epoch: published.target_epoch,
            transfer: published.transfer,
            publications: published.publications,
        })
    }
}

impl StagedUnavailablePgMetadataTransfer {
    pub(crate) fn work(&self) -> &UnavailablePgReconciliationWork {
        &self.work
    }

    pub(crate) fn artifact_length(&self) -> u64 {
        self.intent.artifact_length()
    }

    pub(crate) fn target_epoch(&self) -> ClusterEpoch {
        self.target_epoch
    }

    pub(crate) fn install_request(&self) -> UnavailablePgTransitionInstallRequest {
        UnavailablePgTransitionInstallRequest {
            unavailable_transition: self.work.mutation_binding().clone(),
            transfer: self.transfer,
            expected_destination_epoch: self.target_epoch,
            publications: self.publications.clone(),
        }
    }

    pub(crate) fn into_cleanup(self) -> CleanupUnavailablePgMetadataTransfer {
        let install = self.install_request();
        CleanupUnavailablePgMetadataTransfer {
            work: self.work,
            intent: self.intent,
            authorizations: self.authorizations,
            disposition: MetadataTransferStagingCleanupDisposition::Completed,
            install: Some(install),
        }
    }
}

impl CleanupUnavailablePgMetadataTransfer {
    pub(crate) fn work(&self) -> &UnavailablePgReconciliationWork {
        &self.work
    }
}

impl TombstonedUnavailablePgMetadataTransfer {
    pub(crate) fn work(&self) -> &UnavailablePgReconciliationWork {
        &self.work
    }

    pub(crate) fn cleanup_request(&self) -> &FinalizeMetadataTransferStagingGenerationRequest {
        &self.cleanup
    }
}

/// Opaque failure from storage-owned live PG metadata transfer orchestration.
///
/// Storage retains and records the implementation diagnostic, while callers
/// receive only the semantic operation failure. Runtime-map, route, proof,
/// artifact, transport, and storage-node error representations consequently
/// remain inside their owning crate.
pub struct LivePgMetadataTransferError {
    stage: LivePgMetadataTransferStage,
    disposition: LivePgMetadataTransferFailureDisposition,
    _diagnostic: Box<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LivePgMetadataTransferFailureDisposition {
    Retryable,
    AuthorizationNotObserved,
    Fatal,
}

struct LivePgMetadataTransferFailure {
    disposition: LivePgMetadataTransferFailureDisposition,
    diagnostic: String,
}

impl LivePgMetadataTransferFailure {
    fn retryable(diagnostic: String) -> Self {
        Self {
            disposition: LivePgMetadataTransferFailureDisposition::Retryable,
            diagnostic,
        }
    }

    fn fatal(diagnostic: String) -> Self {
        Self {
            disposition: LivePgMetadataTransferFailureDisposition::Fatal,
            diagnostic,
        }
    }
}

impl From<String> for LivePgMetadataTransferFailure {
    fn from(diagnostic: String) -> Self {
        Self::fatal(diagnostic)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LivePgMetadataTransferStage {
    Configuration,
    Preflight,
    Fence,
    Export,
    Install,
    Import,
    Cleanup,
}

impl LivePgMetadataTransferStage {
    const fn label(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Preflight => "preflight",
            Self::Fence => "fencing",
            Self::Export => "source export",
            Self::Install => "route installation",
            Self::Import => "destination import",
            Self::Cleanup => "destination cleanup",
        }
    }
}

/// Opaque failure from authenticated object-placement inspection.
pub struct LiveObjectPayloadPlacementInspectionError {
    _diagnostic: Box<str>,
}

impl LiveObjectPayloadPlacementInspectionError {
    fn new(diagnostic: String) -> Self {
        let _ = observability::event(
            "storage_object_placement_inspection",
            "object_placement_inspection_error",
            Some(format_args!("diagnostic={diagnostic}")),
        );
        Self {
            _diagnostic: diagnostic.into_boxed_str(),
        }
    }
}

impl fmt::Debug for LiveObjectPayloadPlacementInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveObjectPayloadPlacementInspectionError")
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for LiveObjectPayloadPlacementInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("object payload placement inspection failed")
    }
}

impl std::error::Error for LiveObjectPayloadPlacementInspectionError {}

impl LivePgMetadataTransferError {
    fn new(diagnostic: String) -> Self {
        Self::at_stage(
            LivePgMetadataTransferStage::Configuration,
            LivePgMetadataTransferFailure::fatal(diagnostic),
        )
    }

    fn at_stage(
        stage: LivePgMetadataTransferStage,
        failure: LivePgMetadataTransferFailure,
    ) -> Self {
        let LivePgMetadataTransferFailure {
            disposition,
            diagnostic,
        } = failure;
        let _ = observability::event(
            "storage_live_pg_transfer",
            "live_pg_metadata_transfer_error",
            Some(format_args!(
                "stage={} diagnostic={diagnostic}",
                stage.label()
            )),
        );
        Self {
            stage,
            disposition,
            _diagnostic: diagnostic.into_boxed_str(),
        }
    }

    pub(crate) fn is_fatal(&self) -> bool {
        self.disposition == LivePgMetadataTransferFailureDisposition::Fatal
    }

    pub(crate) fn is_staging_authorization_not_observed(&self) -> bool {
        self.disposition == LivePgMetadataTransferFailureDisposition::AuthorizationNotObserved
    }

    #[cfg(test)]
    fn retained_diagnostic_contains(&self, expected: &str) -> bool {
        self._diagnostic.contains(expected)
    }
}

impl fmt::Debug for LivePgMetadataTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LivePgMetadataTransferError")
            .field("stage", &self.stage)
            .field("disposition", &self.disposition)
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for LivePgMetadataTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "live PG metadata transfer failed during {}",
            self.stage.label()
        )
    }
}

impl std::error::Error for LivePgMetadataTransferError {}

fn control_plane_transfer_failure(
    context: &str,
    error: ControlPlaneError,
) -> LivePgMetadataTransferFailure {
    let retryable = error.is_retryable_runtime_map_observation_error()
        || matches!(error, ControlPlaneError::RpcUnconfirmed { .. })
        || matches!(
            error,
            ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::ForwardToLeader
                    | crate::control_plane::ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                ..
            }
        );
    let diagnostic = format!("{context}: {}", error.retained_diagnostic_message());
    if retryable {
        LivePgMetadataTransferFailure::retryable(diagnostic)
    } else {
        LivePgMetadataTransferFailure::fatal(diagnostic)
    }
}

fn metadata_transfer_failure(
    context: &str,
    error: crate::error::PgMetadataTransferError,
) -> LivePgMetadataTransferFailure {
    let retryable = error.requires_route_refresh_retry()
        || error.is_transient_import_blocker()
        || match &error {
            crate::error::PgMetadataTransferError::Store(error)
            | crate::error::PgMetadataTransferError::Apply(
                crate::error::BucketSnapshotLoadError::Store(error),
            ) => !matches!(
                error.operation_failure_class(),
                crate::StoreOperationFailureClass::Other
            ),
            crate::error::PgMetadataTransferError::Apply(
                crate::error::BucketSnapshotLoadError::Metadata(_),
            )
            | crate::error::PgMetadataTransferError::Reconstruction { .. } => false,
            crate::error::PgMetadataTransferError::PendingMetadataCommand { .. }
            | crate::error::PgMetadataTransferError::TerminalPendingDispositionUnconfirmed {
                ..
            }
            | crate::error::PgMetadataTransferError::RouteRefreshRequired { .. } => true,
        };
    let diagnostic = format!("{context}: {error}");
    if retryable {
        LivePgMetadataTransferFailure::retryable(diagnostic)
    } else {
        LivePgMetadataTransferFailure::fatal(diagnostic)
    }
}

fn staging_rpc_failure(error: crate::StoreError) -> LivePgMetadataTransferFailure {
    let authorization_not_observed = error.storage_node_failure_class()
        == Some(crate::error::StorageNodeFailureClass::StagingAuthorizationNotObserved);
    let retryable = !matches!(
        error.operation_failure_class(),
        crate::StoreOperationFailureClass::Other
    );
    let diagnostic = format!("metadata-transfer staging RPC failed: {error}");
    if authorization_not_observed {
        LivePgMetadataTransferFailure {
            disposition: LivePgMetadataTransferFailureDisposition::AuthorizationNotObserved,
            diagnostic,
        }
    } else if retryable {
        LivePgMetadataTransferFailure::retryable(diagnostic)
    } else {
        LivePgMetadataTransferFailure::fatal(diagnostic)
    }
}

enum StagingArtifactReadOutcome {
    Published(Vec<u8>),
    Absent,
    Retryable(LivePgMetadataTransferFailure),
    Fatal(LivePgMetadataTransferFailure),
}

fn staging_artifact_failure_outcome(
    failure: LivePgMetadataTransferFailure,
) -> StagingArtifactReadOutcome {
    if failure.disposition != LivePgMetadataTransferFailureDisposition::Fatal {
        StagingArtifactReadOutcome::Retryable(failure)
    } else {
        StagingArtifactReadOutcome::Fatal(failure)
    }
}

fn staging_artifact_rpc_read_outcome(
    result: Result<Vec<u8>, crate::StoreError>,
) -> StagingArtifactReadOutcome {
    match result {
        Ok(bytes) => StagingArtifactReadOutcome::Published(bytes),
        Err(crate::StoreError::NotFound) => StagingArtifactReadOutcome::Absent,
        Err(error) => staging_artifact_failure_outcome(staging_rpc_failure(error)),
    }
}

#[cfg(test)]
fn staging_artifact_local_read_outcome(
    result: Result<Vec<u8>, crate::pg_store::MetadataTransferStagingError>,
) -> StagingArtifactReadOutcome {
    match result {
        Ok(bytes) => StagingArtifactReadOutcome::Published(bytes),
        Err(
            crate::pg_store::MetadataTransferStagingError::ArtifactAbsent
            | crate::pg_store::MetadataTransferStagingError::ArtifactNotPublished,
        ) => StagingArtifactReadOutcome::Absent,
        Err(error) => staging_artifact_failure_outcome(staging_local_failure(error)),
    }
}

#[cfg(test)]
fn staging_local_failure(
    error: crate::pg_store::MetadataTransferStagingError,
) -> LivePgMetadataTransferFailure {
    let retryable = matches!(
        error,
        crate::pg_store::MetadataTransferStagingError::Capacity(_)
    );
    let diagnostic = format!("metadata-transfer staging operation failed: {error}");
    if retryable {
        LivePgMetadataTransferFailure::retryable(diagnostic)
    } else {
        LivePgMetadataTransferFailure::fatal(diagnostic)
    }
}

impl LivePgMetadataTransferSummary {
    #[must_use]
    pub fn source_node_id(&self) -> u32 {
        self.source_node_id
    }

    #[must_use]
    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    #[must_use]
    pub fn destination_epoch(&self) -> u64 {
        self.destination_epoch
    }

    #[must_use]
    pub fn imported_log_index(&self) -> u64 {
        self.imported_proof.applied_log_index
    }

    #[must_use]
    pub fn imported_log_hash(&self) -> u64 {
        self.imported_proof.applied_log_hash.value()
    }

    #[must_use]
    pub fn imported_state_digest(&self) -> u64 {
        self.imported_proof.state_digest.value()
    }

    #[must_use]
    pub fn already_completed(&self) -> bool {
        self.already_completed
    }
}

/// Storage-owned live topology administration operation.
///
/// The operation retains one authority-bound control-plane capability and the
/// storage-node transport configuration so no runtime-map snapshot, PG route,
/// transfer proof, or artifact crosses into the process layer.
///
/// ```compile_fail
/// use storage::LivePgMetadataTransferAdmin;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<LivePgMetadataTransferAdmin>();
/// ```
///
/// ```compile_fail
/// use storage::LivePgMetadataTransferAdmin;
///
/// fn require_debug<T: std::fmt::Debug>() {}
/// require_debug::<LivePgMetadataTransferAdmin>();
/// ```
pub struct LivePgMetadataTransferAdmin {
    control_plane: LivePgMetadataTransferControlPlaneClient,
    default_ec_shape: EcShape,
    admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    transport: LivePgMetadataTransferStorageTransport,
    failpoint: Option<LivePgMetadataTransferFailpoint>,
    _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
    #[cfg(test)]
    after_transfer_install_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    after_transfer_prepare_hook: Option<Arc<dyn Fn(PgId, ClusterEpoch) + Send + Sync>>,
    #[cfg(test)]
    before_staging_artifact_publish_hook: Option<Arc<dyn Fn(PgId) + Send + Sync>>,
    #[cfg(test)]
    after_transfer_import_hook: Option<Arc<dyn Fn(PgId, ClusterEpoch) + Send + Sync>>,
    #[cfg(test)]
    clock_override_ms: Option<u64>,
}

impl LivePgMetadataTransferAdmin {
    #[must_use]
    pub fn with_unix_storage_nodes(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        auth: Option<FrontendStorageRpcClientCapability>,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings,
            transport: LivePgMetadataTransferStorageTransport::Unix {
                auth: auth.map(LivePgMetadataTransferStorageAuth::Frontend),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            #[cfg(test)]
            after_transfer_install_hook: None,
            #[cfg(test)]
            after_transfer_prepare_hook: None,
            #[cfg(test)]
            before_staging_artifact_publish_hook: None,
            #[cfg(test)]
            after_transfer_import_hook: None,
            #[cfg(test)]
            clock_override_ms: None,
        }
    }

    #[must_use]
    pub fn with_storage_rpc_endpoints(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<Item = (u32, StorageRpcClientEndpoint)>,
        auth: FrontendStorageRpcClientCapability,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings,
            transport: LivePgMetadataTransferStorageTransport::ConfiguredEndpoints {
                endpoints: endpoints
                    .into_iter()
                    .map(|(node_id, endpoint)| (NodeId::new(node_id), endpoint))
                    .collect(),
                auth: LivePgMetadataTransferStorageAuth::Frontend(auth),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            #[cfg(test)]
            after_transfer_install_hook: None,
            #[cfg(test)]
            after_transfer_prepare_hook: None,
            #[cfg(test)]
            before_staging_artifact_publish_hook: None,
            #[cfg(test)]
            after_transfer_import_hook: None,
            #[cfg(test)]
            clock_override_ms: None,
        }
    }

    #[must_use]
    pub fn with_unix_storage_nodes_and_live_pg_metadata_transfer_auth(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        auth: LivePgMetadataTransferStorageRpcClientCapability,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings,
            transport: LivePgMetadataTransferStorageTransport::Unix {
                auth: Some(LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(
                    auth,
                )),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            #[cfg(test)]
            after_transfer_install_hook: None,
            #[cfg(test)]
            after_transfer_prepare_hook: None,
            #[cfg(test)]
            before_staging_artifact_publish_hook: None,
            #[cfg(test)]
            after_transfer_import_hook: None,
            #[cfg(test)]
            clock_override_ms: None,
        }
    }

    #[must_use]
    pub fn with_storage_rpc_endpoints_and_live_pg_metadata_transfer_auth(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        endpoints: impl IntoIterator<Item = (u32, StorageRpcClientEndpoint)>,
        auth: LivePgMetadataTransferStorageRpcClientCapability,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings,
            transport: LivePgMetadataTransferStorageTransport::ConfiguredEndpoints {
                endpoints: endpoints
                    .into_iter()
                    .map(|(node_id, endpoint)| (NodeId::new(node_id), endpoint))
                    .collect(),
                auth: LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(auth),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            #[cfg(test)]
            after_transfer_install_hook: None,
            #[cfg(test)]
            after_transfer_prepare_hook: None,
            #[cfg(test)]
            before_staging_artifact_publish_hook: None,
            #[cfg(test)]
            after_transfer_import_hook: None,
            #[cfg(test)]
            clock_override_ms: None,
        }
    }

    #[cfg(test)]
    fn with_in_process_storage_nodes(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        data_dir: std::path::PathBuf,
    ) -> Self {
        let staging_stores = Arc::new(InProcessMetadataTransferStagingStores::new(
            data_dir.clone(),
        ));
        Self::with_in_process_storage_nodes_and_staging_stores(
            control_plane,
            default_ec_shape,
            data_dir,
            staging_stores,
        )
    }

    #[cfg(test)]
    fn with_in_process_storage_nodes_and_staging_stores(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        data_dir: std::path::PathBuf,
        staging_stores: Arc<InProcessMetadataTransferStagingStores>,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings: LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            transport: LivePgMetadataTransferStorageTransport::InProcess {
                data_dir,
                staging_stores,
                generation: std::sync::atomic::AtomicU64::new(0),
                export_route_refresh_failures: std::sync::atomic::AtomicU64::new(0),
                import_route_refresh_failures: std::sync::atomic::AtomicU64::new(0),
                import_pending_command_failures: std::sync::atomic::AtomicU64::new(0),
                staging_artifact_publish_failures: std::sync::Mutex::new(
                    std::collections::BTreeMap::new(),
                ),
                staging_artifact_read_failures: std::sync::Mutex::new(
                    std::collections::BTreeMap::new(),
                ),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            after_transfer_install_hook: None,
            after_transfer_prepare_hook: None,
            before_staging_artifact_publish_hook: None,
            after_transfer_import_hook: None,
            clock_override_ms: None,
        }
    }

    #[cfg(test)]
    fn with_route_refresh_failures(self, export: u64, import: u64) -> Self {
        let LivePgMetadataTransferStorageTransport::InProcess {
            export_route_refresh_failures,
            import_route_refresh_failures,
            ..
        } = &self.transport
        else {
            panic!("route-refresh failure injection requires in-process storage");
        };
        export_route_refresh_failures.store(export, std::sync::atomic::Ordering::Relaxed);
        import_route_refresh_failures.store(import, std::sync::atomic::Ordering::Relaxed);
        self
    }

    #[cfg(test)]
    fn with_import_pending_command_failures(self, failures: u64) -> Self {
        let LivePgMetadataTransferStorageTransport::InProcess {
            import_pending_command_failures,
            ..
        } = &self.transport
        else {
            panic!("pending-command failure injection requires in-process storage");
        };
        import_pending_command_failures.store(failures, std::sync::atomic::Ordering::Relaxed);
        self
    }

    #[cfg(test)]
    fn with_staging_artifact_read_failure(
        self,
        node_id: NodeId,
        disposition: LivePgMetadataTransferFailureDisposition,
    ) -> Self {
        let LivePgMetadataTransferStorageTransport::InProcess {
            staging_artifact_read_failures,
            ..
        } = &self.transport
        else {
            panic!("staging artifact read failure injection requires in-process storage");
        };
        staging_artifact_read_failures
            .lock()
            .unwrap()
            .entry(node_id)
            .or_default()
            .push_back(disposition);
        self
    }

    #[cfg(test)]
    fn with_staging_artifact_publish_failure_notification(
        self,
        node_id: NodeId,
        disposition: LivePgMetadataTransferFailureDisposition,
        notification: std::sync::mpsc::SyncSender<PgId>,
    ) -> Self {
        let LivePgMetadataTransferStorageTransport::InProcess {
            staging_artifact_publish_failures,
            ..
        } = &self.transport
        else {
            panic!("staging artifact publish failure injection requires in-process storage");
        };
        staging_artifact_publish_failures
            .lock()
            .unwrap()
            .entry(node_id)
            .or_default()
            .push_back((disposition, Some(notification)));
        self
    }

    #[cfg(test)]
    fn with_after_transfer_install_hook(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.after_transfer_install_hook = Some(Arc::new(hook));
        self
    }

    #[cfg(test)]
    fn with_after_transfer_prepare_hook(
        mut self,
        hook: impl Fn(PgId, ClusterEpoch) + Send + Sync + 'static,
    ) -> Self {
        self.after_transfer_prepare_hook = Some(Arc::new(hook));
        self
    }

    #[cfg(test)]
    fn with_after_transfer_import_hook(
        mut self,
        hook: impl Fn(PgId, ClusterEpoch) + Send + Sync + 'static,
    ) -> Self {
        self.after_transfer_import_hook = Some(Arc::new(hook));
        self
    }

    #[cfg(test)]
    fn with_before_staging_artifact_publish_hook(
        mut self,
        hook: impl Fn(PgId) + Send + Sync + 'static,
    ) -> Self {
        self.before_staging_artifact_publish_hook = Some(Arc::new(hook));
        self
    }

    #[cfg(test)]
    fn with_clock_override(mut self, now_ms: u64) -> Self {
        self.clock_override_ms = Some(now_ms);
        self
    }

    #[must_use]
    pub fn with_failpoint(mut self, failpoint: Option<LivePgMetadataTransferFailpoint>) -> Self {
        self.failpoint = failpoint;
        self
    }

    pub fn transfer(
        &self,
        pg_id: u32,
        acting_set: Vec<u32>,
    ) -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferError> {
        let pg_id = PgId::new(pg_id);
        let acting_set = acting_set.into_iter().map(NodeId::new).collect::<Vec<_>>();
        self.transfer_typed(pg_id, acting_set, None)
    }

    pub(crate) fn prepare_unavailable_pg_reconciliation_staging(
        &self,
        work: UnavailablePgReconciliationWork,
    ) -> Result<PreparedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        if work.stage() != crate::control_plane::UnavailablePgReconciliationStage::MetadataTransfer
        {
            return Err(LivePgMetadataTransferError::new(
                "payload-readiness work cannot enter metadata transfer preparation".to_owned(),
            ));
        }
        let mut stage = LivePgMetadataTransferStage::Preflight;
        let result =
            self.prepare_unavailable_pg_reconciliation_staging_at_epoch(work, None, &mut stage);
        result.map_err(|error| LivePgMetadataTransferError::at_stage(stage, error))
    }

    fn prepare_unavailable_pg_reconciliation_staging_at_epoch(
        &self,
        work: UnavailablePgReconciliationWork,
        committed_epochs: Option<(ClusterEpoch, ClusterEpoch)>,
        stage: &mut LivePgMetadataTransferStage,
    ) -> Result<PreparedUnavailablePgMetadataTransfer, LivePgMetadataTransferFailure> {
        let prepared = self.prepare_transfer_typed(
            work.pg_id(),
            work.destination_acting_set().to_vec(),
            Some(work.mutation_binding()),
            stage,
        )?;
        let LivePgMetadataTransferPreparation::Prepared(mut prepared) = prepared else {
            return Err(LivePgMetadataTransferFailure::retryable(format!(
                "PG {} metadata transfer advanced before staging preparation completed",
                work.pg_id().get()
            )));
        };
        if let Some((source_runtime_epoch, expected_destination_epoch)) = committed_epochs {
            let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &prepared.artifact,
                expected_destination_epoch,
            )
            .map_err(|error| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "failed to reconstruct PG {} staged metadata proof: {error}",
                    work.pg_id().get()
                ))
            })?;
            prepared.install_member.source_runtime_epoch = source_runtime_epoch;
            prepared.install_member.expected_destination_epoch = expected_destination_epoch;
            prepared.install_member.transfer =
                PgMetadataTransferProof::new_with_imported_metadata_proof(
                    prepared.artifact.cluster_epoch(),
                    prepared.artifact.source_metadata_proof(),
                    imported_proof,
                );
        }
        if committed_epochs.is_none() {
            self.run_after_transfer_prepare_hook(
                work.pg_id(),
                prepared.install_member.expected_destination_epoch,
            );
        }
        let target_epoch = prepared.install_member.expected_destination_epoch;
        let staged_artifact =
            encode_staged_metadata_transfer_artifact(&prepared.artifact, target_epoch).map_err(
                |error| {
                    LivePgMetadataTransferFailure::fatal(format!(
                        "failed to encode PG {} staged metadata transfer artifact: {error}",
                        work.pg_id().get()
                    ))
                },
            )?;
        let intent = MetadataTransferStagingIntent::for_unavailable_transition(
            work.mutation_binding(),
            checksum::sha256::digest(&staged_artifact),
            staged_artifact.len() as u64,
            METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .map_err(|error| {
            LivePgMetadataTransferFailure::fatal(format!(
                "failed to construct PG {} staging intent: {error}",
                work.pg_id().get()
            ))
        })?;
        Ok(PreparedUnavailablePgMetadataTransfer {
            work,
            prepared,
            intent,
            staged_artifact,
        })
    }

    pub(crate) fn stage_prepared_unavailable_pg_reconciliation(
        &self,
        authorized: &AuthorizedUnavailablePgMetadataTransfer,
    ) -> Result<PublishedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        #[cfg(test)]
        if let Some(hook) = &self.before_staging_artifact_publish_hook {
            hook(authorized.work.pg_id());
        }
        let result =
            (|| -> Result<PublishedUnavailablePgMetadataTransfer, LivePgMetadataTransferFailure> {
                let target_epoch = authorized.target_epoch;
                let transfer = authorized.transfer;
                let publications = self.publish_staging_artifact_to_destinations(authorized)?;
                Ok(PublishedUnavailablePgMetadataTransfer {
                    authorizations: authorized.authorizations.clone(),
                    target_epoch,
                    transfer,
                    publications,
                })
            })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Export, error)
        })
    }

    pub(crate) fn resume_authorized_unavailable_pg_reconciliation(
        &self,
        work: UnavailablePgReconciliationWork,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<AuthorizedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result = (|| -> Result<_, LivePgMetadataTransferFailure> {
            let (authorization_request, source_epoch, target_epoch) = snapshot
                .committed_unavailable_pg_staging_request_binding(&work)
                .map_err(|error| {
                    control_plane_transfer_failure(
                        "failed to recover committed staging authorization",
                        error,
                    )
                })?;
            match self.recover_authorized_staging_artifact(
                work.clone(),
                snapshot,
                authorization_request.clone(),
            )? {
                RecoveredAuthorizedStagingArtifact::Published(authorized) => Ok(*authorized),
                RecoveredAuthorizedStagingArtifact::Absent(read_error) => {
                    let mut stage = LivePgMetadataTransferStage::Export;
                    match self.prepare_unavailable_pg_reconciliation_staging_at_epoch(
                        work,
                        Some((source_epoch, target_epoch)),
                        &mut stage,
                    ) {
                        Ok(prepared) => {
                            if prepared.authorization_request() != authorization_request {
                                if let Some(read_error) = read_error {
                                    return Err(read_error);
                                }
                                return Err(LivePgMetadataTransferFailure::fatal(
                                    "re-exported staging artifact does not match its committed authorization"
                                        .to_owned(),
                                ));
                            }
                            prepared
                                .bind_committed_authorization(snapshot)
                                .map_err(|error| LivePgMetadataTransferFailure {
                                    disposition: error.disposition,
                                    diagnostic: error._diagnostic.into(),
                                })
                        }
                        Err(_error) if read_error.is_some() => Err(read_error
                            .expect("staging read error was checked before source re-export")),
                        Err(error) => Err(error),
                    }
                }
            }
        })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Export, error)
        })
    }

    pub(crate) fn resume_installed_unavailable_pg_reconciliation(
        &self,
        work: UnavailablePgReconciliationWork,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<StagedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result = (|| -> Result<_, LivePgMetadataTransferFailure> {
            let (_authorization_request, install) = snapshot
                .committed_unavailable_pg_staged_transfer(&work)
                .map_err(|error| {
                    control_plane_transfer_failure(
                        "failed to recover committed staged metadata transfer",
                        error,
                    )
                })?;
            let authorized = self
                .resume_authorized_unavailable_pg_reconciliation(work, snapshot)
                .map_err(|error| LivePgMetadataTransferFailure {
                    disposition: error.disposition,
                    diagnostic: error._diagnostic.into(),
                })?;
            let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &authorized.artifact,
                install.expected_destination_epoch,
            )
            .map_err(|error| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} staged artifact cannot derive its durable install proof: {error}",
                    authorized.work.pg_id().get()
                ))
            })?;
            let recovered_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
                authorized.artifact.cluster_epoch(),
                authorized.artifact.source_metadata_proof(),
                imported_proof,
            );
            if recovered_transfer != install.transfer
                || install.unavailable_transition != *authorized.work.mutation_binding()
            {
                return Err(LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} staged artifact does not match its durable destination install",
                    authorized.work.pg_id().get()
                )));
            }
            Ok(StagedUnavailablePgMetadataTransfer {
                work: authorized.work,
                intent: authorized.intent,
                authorizations: authorized.authorizations,
                artifact: authorized.artifact,
                target_epoch: install.expected_destination_epoch,
                transfer: install.transfer,
                publications: install.publications,
            })
        })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Import, error)
        })
    }

    pub(crate) fn resume_cleanup_unavailable_pg_reconciliation(
        &self,
        work: UnavailablePgReconciliationWork,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<CleanupUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result = (|| -> Result<_, LivePgMetadataTransferFailure> {
            let (authorization_request, disposition, install) = snapshot
                .committed_unavailable_pg_staging_cleanup(&work)
                .map_err(|error| {
                    control_plane_transfer_failure(
                        "failed to recover committed staged metadata transfer cleanup",
                        error,
                    )
                })?;
            let (intent, authorizations) =
                self.recover_staging_authorizations(&work, snapshot, &authorization_request)?;
            Ok(CleanupUnavailablePgMetadataTransfer {
                work,
                intent,
                authorizations,
                disposition,
                install,
            })
        })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Cleanup, error)
        })
    }

    fn recover_authorized_staging_artifact(
        &self,
        work: UnavailablePgReconciliationWork,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
        authorization_request: UnavailablePgStagingIntentAuthorizationRequest,
    ) -> Result<RecoveredAuthorizedStagingArtifact, LivePgMetadataTransferFailure> {
        let (intent, authorizations) =
            self.recover_staging_authorizations(&work, snapshot, &authorization_request)?;

        let mut first_fatal = None;
        let mut last_retryable = None;
        let mut staged_artifact = None;
        // Authorization may have reached only a subset of destinations. Any
        // exact digest-bound copy can recover the bytes; staging replay still
        // requires every destination to accept those bytes before install.
        for authorization in &authorizations {
            let node_id = authorization.destination_node_id();
            let node = snapshot.node(node_id).ok_or_else(|| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} staged transfer destination {} is absent from authority state",
                    work.pg_id().get(),
                    node_id.as_u32()
                ))
            })?;
            #[cfg(test)]
            let read_outcome = if let LivePgMetadataTransferStorageTransport::InProcess {
                staging_stores,
                staging_artifact_read_failures,
                ..
            } = &self.transport
            {
                let injected = staging_artifact_read_failures
                    .lock()
                    .expect("staging artifact read failure injection poisoned")
                    .get_mut(&node_id)
                    .and_then(std::collections::VecDeque::pop_front);
                if let Some(disposition) = injected {
                    staging_artifact_failure_outcome(LivePgMetadataTransferFailure {
                        disposition,
                        diagnostic: format!(
                            "injected staging artifact read failure on node {}",
                            node_id.as_u32()
                        ),
                    })
                } else {
                    let identity = MetadataTransferStagingNodeIdentity::new(
                        node_id,
                        node.node_incarnation(),
                        node.endpoint().to_owned(),
                    )
                    .map_err(staging_local_failure)?;
                    let read_result = staging_stores
                        .store(identity)
                        .and_then(|store| store.read_artifact(&intent));
                    staging_artifact_local_read_outcome(read_result)
                }
            } else {
                match self.staging_rpc_client(snapshot.cluster_epoch(), node_id, node.endpoint()) {
                    Ok(client) => staging_artifact_rpc_read_outcome(
                        client.read_metadata_transfer_staging_artifact(authorization, &intent),
                    ),
                    Err(error) => staging_artifact_failure_outcome(error),
                }
            };
            #[cfg(not(test))]
            let read_outcome =
                match self.staging_rpc_client(snapshot.cluster_epoch(), node_id, node.endpoint()) {
                    Ok(client) => staging_artifact_rpc_read_outcome(
                        client.read_metadata_transfer_staging_artifact(authorization, &intent),
                    ),
                    Err(error) => staging_artifact_failure_outcome(error),
                };
            match read_outcome {
                StagingArtifactReadOutcome::Published(bytes) => {
                    staged_artifact.get_or_insert(bytes);
                }
                StagingArtifactReadOutcome::Absent => {}
                StagingArtifactReadOutcome::Retryable(error) => {
                    last_retryable = Some(error);
                }
                StagingArtifactReadOutcome::Fatal(error) => {
                    first_fatal.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_fatal {
            return Err(error);
        }
        let staged_artifact = match staged_artifact {
            Some(staged_artifact) => staged_artifact,
            None => return Ok(RecoveredAuthorizedStagingArtifact::Absent(last_retryable)),
        };
        let (artifact, target_epoch, transfer) =
            decode_staged_metadata_transfer_artifact(&staged_artifact, &intent).map_err(
                |error| {
                    LivePgMetadataTransferFailure::fatal(format!(
                        "PG {} committed staged artifact is invalid: {error}",
                        work.pg_id().get()
                    ))
                },
            )?;
        if target_epoch != authorization_request.artifact_target_epoch {
            return Err(LivePgMetadataTransferFailure::fatal(format!(
                "PG {} staged artifact target epoch does not match its committed authorization",
                work.pg_id().get()
            )));
        }
        Ok(RecoveredAuthorizedStagingArtifact::Published(Box::new(
            AuthorizedUnavailablePgMetadataTransfer {
                work,
                intent,
                authorizations,
                artifact,
                staged_artifact,
                target_epoch,
                transfer,
            },
        )))
    }

    fn recover_staging_authorizations(
        &self,
        work: &UnavailablePgReconciliationWork,
        snapshot: &crate::control_plane::ClusterControlSnapshot,
        authorization_request: &UnavailablePgStagingIntentAuthorizationRequest,
    ) -> Result<
        (
            MetadataTransferStagingIntent,
            Vec<CommittedUnavailablePgStagingAuthorization>,
        ),
        LivePgMetadataTransferFailure,
    > {
        let intent = MetadataTransferStagingIntent::for_unavailable_transition(
            work.mutation_binding(),
            authorization_request.artifact_digest,
            authorization_request.artifact_length,
            authorization_request.artifact_format_version,
        )
        .map_err(|error| {
            LivePgMetadataTransferFailure::fatal(format!(
                "failed to reconstruct PG {} staging intent: {error}",
                work.pg_id().get()
            ))
        })?;
        if intent.staging_generation() != authorization_request.staging_generation {
            return Err(LivePgMetadataTransferFailure::fatal(format!(
                "PG {} committed staging generation does not match its transition",
                work.pg_id().get()
            )));
        }
        let mut authorizations = Vec::with_capacity(work.destination_acting_set().len());
        for node_id in work.destination_acting_set().iter().copied() {
            authorizations.push(
                snapshot
                    .committed_unavailable_pg_staging_authorization(authorization_request, node_id)
                    .map_err(|error| {
                        control_plane_transfer_failure(
                            "failed to recover destination staging authorization",
                            error,
                        )
                    })?,
            );
        }
        Ok((intent, authorizations))
    }

    pub(crate) fn rebase_staged_unavailable_pg_reconciliation(
        &self,
        staged: &mut StagedUnavailablePgMetadataTransfer,
        target_epoch: ClusterEpoch,
    ) -> Result<(), LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result = (|| -> Result<_, LivePgMetadataTransferFailure> {
            if target_epoch <= staged.work.transition_epoch() {
                return Err(format!(
                    "PG {} staging proof target epoch {} does not follow transition epoch {}",
                    staged.work.pg_id().get(),
                    target_epoch.get(),
                    staged.work.transition_epoch().get()
                )
                .into());
            }
            let publications = self.publish_staged_proof_to_destinations(staged, target_epoch)?;
            let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &staged.artifact,
                target_epoch,
            )
            .map_err(|error| {
                format!("failed to rebase staged PG metadata transfer proof: {error}")
            })?;
            let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
                staged.artifact.cluster_epoch(),
                staged.artifact.source_metadata_proof(),
                imported_proof,
            );
            Ok((publications, transfer))
        })();
        let (publications, transfer) = result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Install, error)
        })?;
        staged.publications = publications;
        staged.target_epoch = target_epoch;
        staged.transfer = transfer;
        Ok(())
    }

    pub(crate) fn import_staged_unavailable_pg_reconciliation(
        &self,
        staged: &StagedUnavailablePgMetadataTransfer,
    ) -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result =
            (|| -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferFailure> {
                let pg_id = staged.work.pg_id();
                let acting_set = staged.work.destination_acting_set().to_vec();
                let serving_runtime = self
                    .control_plane
                    .serving_pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis())
                    .map_err(|error| {
                        control_plane_transfer_failure(
                            "failed to obtain staged metadata-transfer destination map",
                            error,
                        )
                    })?;
                if serving_runtime.cluster_epoch() < staged.target_epoch {
                    return Err(LivePgMetadataTransferFailure::retryable(format!(
                        "serving PG {} map epoch {} has not reached staged destination epoch {}",
                        pg_id.get(),
                        serving_runtime.cluster_epoch().get(),
                        staged.target_epoch.get()
                    )));
                }
                let imported_proof = staged.transfer.metadata_proof();
                if active_route_matches(&serving_runtime, pg_id, &acting_set, imported_proof)? {
                    return Ok(summary(
                        staged.artifact.source_node_id,
                        staged.artifact.cluster_epoch(),
                        staged.target_epoch,
                        imported_proof,
                        true,
                    ));
                }
                let destination_runtime = serving_runtime
                    .metadata_transfer_destination_runtime_map(pg_id, &acting_set, staged.transfer)
                    .map_err(|error| {
                        format!(
                        "failed to authorize staged PG {} metadata-transfer destination route: {}",
                        pg_id.get(),
                        error.retained_diagnostic_message()
                    )
                    })?;
                let summary =
                    self.import_installed_transfer(Box::new(InstalledLivePgMetadataTransfer {
                        pg_id,
                        acting_set,
                        artifact: staged.artifact.clone(),
                        destination_runtime,
                        destination_epoch: staged.target_epoch,
                        imported_proof,
                        source_node_id: staged.artifact.source_node_id,
                    }))?;
                self.run_after_transfer_import_hook(pg_id, staged.target_epoch);
                Ok(summary)
            })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Import, error)
        })
    }

    #[cfg(test)]
    pub(crate) fn tombstone_staged_unavailable_pg_reconciliation(
        &self,
        staged: &StagedUnavailablePgMetadataTransfer,
        completed_snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<TombstonedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        let cleanup = CleanupUnavailablePgMetadataTransfer {
            work: staged.work.clone(),
            intent: staged.intent.clone(),
            authorizations: staged.authorizations.clone(),
            disposition: MetadataTransferStagingCleanupDisposition::Completed,
            install: Some(staged.install_request()),
        };
        self.tombstone_unavailable_pg_reconciliation(&cleanup, completed_snapshot)
    }

    pub(crate) fn tombstone_unavailable_pg_reconciliation(
        &self,
        staged: &CleanupUnavailablePgMetadataTransfer,
        completed_snapshot: &crate::control_plane::ClusterControlSnapshot,
    ) -> Result<TombstonedUnavailablePgMetadataTransfer, LivePgMetadataTransferError> {
        #[cfg(test)]
        let _time_override = self
            .clock_override_ms
            .map(crate::clock::test_time_override_guard);
        let result = (|| -> Result<_, LivePgMetadataTransferFailure> {
            let cleanup_authorization = completed_snapshot
                .validate_unavailable_pg_staging_cleanup(
                    staged.work.mutation_binding(),
                    staged.disposition,
                    staged.install.as_ref(),
                    staged.intent.staging_generation(),
                )
                .map_err(|error| {
                    control_plane_transfer_failure(
                        "staging cleanup is not authorized by a completed transition",
                        error,
                    )
                })?;
            let mut tombstones = Vec::with_capacity(staged.work.destination_acting_set().len());
            for node_id in staged.work.destination_acting_set().iter().copied() {
                let actor = cleanup_authorization
                    .destination_actor(node_id)
                    .ok_or_else(|| {
                        LivePgMetadataTransferFailure::fatal(format!(
                            "PG {} completed cleanup authority omits destination {}",
                            staged.work.pg_id().get(),
                            node_id.as_u32()
                        ))
                    })?;
                let receipt = self.tombstone_staging_artifact_on_destination(
                    staged,
                    cleanup_authorization.cluster_epoch(),
                    actor,
                )?;
                let evidence = decode_staging_evidence(receipt.as_bytes()).map_err(|error| {
                    LivePgMetadataTransferFailure::fatal(format!(
                        "destination {} returned invalid PG {} staging tombstone evidence: {error}",
                        node_id.as_u32(),
                        staged.work.pg_id().get()
                    ))
                })?;
                if evidence.intent() != &staged.intent
                    || evidence.actor().node_id() != node_id
                    || evidence.kind()
                        != crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone
                    || evidence.target_epoch().is_some()
                    || evidence.transfer().is_some()
                {
                    return Err(LivePgMetadataTransferFailure::fatal(format!(
                        "destination {} returned tombstone evidence for a different PG transition",
                        node_id.as_u32()
                    )));
                }
                tombstones.push(MetadataTransferStagingTombstoneBinding {
                    node_id,
                    node_incarnation: evidence.actor().node_incarnation(),
                    endpoint: evidence.actor().endpoint().to_owned(),
                    evidence_digest: checksum::sha256::digest(evidence.as_bytes()),
                });
            }
            tombstones.sort_by_key(|binding| binding.node_id);
            Ok(TombstonedUnavailablePgMetadataTransfer {
                work: staged.work.clone(),
                cleanup: FinalizeMetadataTransferStagingGenerationRequest {
                    unavailable_transition: staged.work.mutation_binding().clone(),
                    staging_generation: staged.intent.staging_generation(),
                    disposition: staged.disposition,
                    tombstones,
                },
            })
        })();
        result.map_err(|error| {
            LivePgMetadataTransferError::at_stage(LivePgMetadataTransferStage::Cleanup, error)
        })
    }

    fn tombstone_staging_artifact_on_destination(
        &self,
        staged: &CleanupUnavailablePgMetadataTransfer,
        cluster_epoch: ClusterEpoch,
        actor: &MetadataTransferStagingNodeIdentity,
    ) -> Result<MetadataTransferStagingReceipt, LivePgMetadataTransferFailure> {
        let node_id = actor.node_id();
        let authorization = staged
            .authorizations
            .iter()
            .find(|authorization| authorization.destination_node_id() == node_id)
            .ok_or_else(|| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} has no committed cleanup authorization for destination {}",
                    staged.work.pg_id().get(),
                    node_id.as_u32()
                ))
            })?;
        #[cfg(test)]
        if let LivePgMetadataTransferStorageTransport::InProcess { staging_stores, .. } =
            &self.transport
        {
            let identity = MetadataTransferStagingNodeIdentity::new(
                node_id,
                actor.node_incarnation(),
                actor.endpoint().to_owned(),
            )
            .map_err(staging_local_failure)?;
            let store = staging_stores
                .store(identity)
                .map_err(staging_local_failure)?;
            return store
                .tombstone_authorized(authorization, &staged.intent)
                .map_err(staging_local_failure);
        }

        let client = self.staging_rpc_client(cluster_epoch, node_id, actor.endpoint())?;
        client
            .tombstone_metadata_transfer_staging_artifact(authorization, &staged.intent)
            .map_err(staging_rpc_failure)
    }

    pub fn inspect_object_payload_placement(
        &self,
        metadata_pg_id: u32,
        data_pg_id: u32,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPayloadPlacementDiagnostic, LiveObjectPayloadPlacementInspectionError> {
        self.inspect_object_payload_placement_typed(
            PgId::new(metadata_pg_id),
            data_pg_id,
            bucket,
            key,
        )
        .map_err(LiveObjectPayloadPlacementInspectionError::new)
    }

    fn inspect_object_payload_placement_typed(
        &self,
        metadata_pg_id: PgId,
        data_pg_id: u32,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPayloadPlacementDiagnostic, String> {
        let bucket = BucketName::try_from(bucket.to_owned())
            .map_err(|error| format!("invalid object placement bucket: {error}"))?;
        let key = ObjectKey::try_from(key.to_owned())
            .map_err(|error| format!("invalid object placement key: {error}"))?;
        let runtime_map = self
            .control_plane
            .serving_pg_runtime_map_snapshot(metadata_pg_id, crate::clock::current_time_millis())
            .map_err(|error| {
                format!(
                    "failed to obtain serving object placement map: {}",
                    error.retained_diagnostic_message()
                )
            })?;
        let cluster = self.build_cluster(&runtime_map)?;
        Ok(
            cluster.object_payload_placement_diagnostic_for_expected_pgs(
                &bucket,
                &key,
                Some((metadata_pg_id.get(), data_pg_id)),
            ),
        )
    }

    fn build_cluster(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Arc<StorageCluster>, String> {
        let metadata_primary_node_id = runtime_map
            .nodes()
            .first()
            .map(|node| node.node_id())
            .ok_or_else(|| "control-plane runtime map has no routed nodes".to_owned())?;
        match &self.transport {
            LivePgMetadataTransferStorageTransport::Unix {
                auth: Some(LivePgMetadataTransferStorageAuth::Frontend(auth)),
            } => StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_frontend_auth(
                metadata_primary_node_id,
                runtime_map,
                self.default_ec_shape,
                self.admission_settings,
                auth.clone(),
            ),
            LivePgMetadataTransferStorageTransport::Unix {
                auth: Some(LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(auth)),
            } => StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_live_pg_metadata_transfer_auth(
                metadata_primary_node_id,
                runtime_map,
                self.default_ec_shape,
                self.admission_settings,
                auth.clone(),
            ),
            LivePgMetadataTransferStorageTransport::Unix { auth: None } => {
                StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                )
            }
            LivePgMetadataTransferStorageTransport::ConfiguredEndpoints {
                endpoints,
                auth: LivePgMetadataTransferStorageAuth::Frontend(auth),
            } => {
                let scoped_endpoints = configured_endpoints_for_runtime_map(runtime_map, endpoints);
                StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_frontend_auth(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                    scoped_endpoints,
                    auth.clone(),
                )
            }
            LivePgMetadataTransferStorageTransport::ConfiguredEndpoints {
                endpoints,
                auth: LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(auth),
            } => {
                let scoped_endpoints = configured_endpoints_for_runtime_map(runtime_map, endpoints);
                StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_live_pg_metadata_transfer_auth(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                    scoped_endpoints,
                    auth.clone(),
                )
            }
            #[cfg(test)]
            LivePgMetadataTransferStorageTransport::InProcess {
                data_dir,
                generation,
                ..
            } => {
                let generation = generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let generation_dir = data_dir.join(format!("cluster-{generation}"));
                let configs = runtime_map.nodes().iter().map(|node| {
                    crate::LocalNodeStoreConfig::new(
                        node.node_id(),
                        generation_dir.join(format!("node-{}", node.node_id().as_u32())),
                    )
                });
                crate::LocalClusterMap::open_frontend_with_configs_and_runtime_map(
                    metadata_primary_node_id,
                    configs,
                    self.default_ec_shape,
                    runtime_map,
                )
                .and_then(|local_map| {
                    StorageCluster::from_runtime_local_map(Arc::new(local_map), runtime_map)
                })
            }
        }
        .map_err(|error| error.to_string())
    }

    fn publish_staged_proof_to_destinations(
        &self,
        staged: &StagedUnavailablePgMetadataTransfer,
        target_epoch: ClusterEpoch,
    ) -> Result<Vec<UnavailablePgStagingPublicationBinding>, LivePgMetadataTransferFailure> {
        let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
            &staged.artifact,
            target_epoch,
        )
        .map_err(|error| format!("failed to derive expected staged proof receipt: {error}"))?;
        let expected_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            staged.artifact.cluster_epoch(),
            staged.artifact.source_metadata_proof(),
            imported_proof,
        );
        let runtime = self
            .control_plane
            .pg_runtime_map_snapshot(staged.work.pg_id(), crate::clock::current_time_millis())
            .map_err(|error| {
                control_plane_transfer_failure(
                    "failed to obtain current staging destination identities",
                    error,
                )
            })?;
        let mut publications = Vec::with_capacity(staged.work.destination_acting_set().len());
        for node_id in staged.work.destination_acting_set().iter().copied() {
            let authorization = staged
                .authorizations
                .iter()
                .find(|authorization| authorization.destination_node_id() == node_id)
                .ok_or_else(|| {
                    LivePgMetadataTransferFailure::fatal(format!(
                        "PG {} has no committed staging authorization for destination {}",
                        staged.work.pg_id().get(),
                        node_id.as_u32()
                    ))
                })?;
            let node = runtime
                .nodes()
                .iter()
                .find(|candidate| candidate.node_id() == node_id)
                .ok_or_else(|| {
                    LivePgMetadataTransferFailure::retryable(format!(
                        "PG {} current staging map omits destination {}",
                        staged.work.pg_id().get(),
                        node_id.as_u32()
                    ))
                })?;
            #[cfg(test)]
            let receipt = if let LivePgMetadataTransferStorageTransport::InProcess {
                staging_stores,
                ..
            } = &self.transport
            {
                let identity = MetadataTransferStagingNodeIdentity::new(
                    node_id,
                    node.node_incarnation(),
                    node.endpoint().to_owned(),
                )
                .map_err(staging_local_failure)?;
                let store = staging_stores
                    .store(identity)
                    .map_err(staging_local_failure)?;
                store
                    .publish_proof_for_epoch_authorized(authorization, &staged.intent, target_epoch)
                    .map_err(staging_local_failure)?
            } else {
                let client =
                    self.staging_rpc_client(runtime.cluster_epoch(), node_id, node.endpoint())?;
                client
                    .publish_metadata_transfer_staging_proof(
                        authorization,
                        &staged.intent,
                        target_epoch,
                    )
                    .map_err(staging_rpc_failure)?
            };
            #[cfg(not(test))]
            let receipt = {
                let client =
                    self.staging_rpc_client(runtime.cluster_epoch(), node_id, node.endpoint())?;
                client
                    .publish_metadata_transfer_staging_proof(
                        authorization,
                        &staged.intent,
                        target_epoch,
                    )
                    .map_err(staging_rpc_failure)?
            };
            let evidence = decode_staging_evidence(receipt.as_bytes()).map_err(|error| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "destination {} returned invalid PG {} staging proof evidence: {error}",
                    node_id.as_u32(),
                    staged.work.pg_id().get()
                ))
            })?;
            if evidence.intent() != &staged.intent
                || evidence.actor().node_id() != node_id
                || evidence.target_epoch() != Some(target_epoch)
                || evidence.transfer() != Some(expected_transfer)
            {
                return Err(LivePgMetadataTransferFailure::fatal(format!(
                    "destination {} returned proof evidence for a different PG transition",
                    node_id.as_u32()
                )));
            }
            publications.push(UnavailablePgStagingPublicationBinding {
                node_id,
                node_incarnation: evidence.actor().node_incarnation(),
                endpoint: evidence.actor().endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(evidence.as_bytes()),
            });
        }
        publications.sort_by_key(|binding| binding.node_id);
        Ok(publications)
    }

    fn publish_staging_artifact_to_destinations(
        &self,
        authorized: &AuthorizedUnavailablePgMetadataTransfer,
    ) -> Result<Vec<UnavailablePgStagingPublicationBinding>, LivePgMetadataTransferFailure> {
        let expected_target_epoch = authorized.target_epoch;
        let expected_transfer = authorized.transfer;
        let runtime = self
            .control_plane
            .pg_runtime_map_snapshot(authorized.work.pg_id(), crate::clock::current_time_millis())
            .map_err(|error| {
                control_plane_transfer_failure(
                    "failed to obtain current staging destination identities",
                    error,
                )
            })?;
        let mut publications = Vec::with_capacity(authorized.work.destination_acting_set().len());
        for node_id in authorized.work.destination_acting_set().iter().copied() {
            let receipt =
                self.publish_staging_artifact_to_destination(authorized, &runtime, node_id)?;
            let evidence = decode_staging_evidence(receipt.as_bytes()).map_err(|error| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "destination {} returned invalid PG {} staging evidence: {error}",
                    node_id.as_u32(),
                    authorized.work.pg_id().get()
                ))
            })?;
            if evidence.intent() != &authorized.intent
                || evidence.actor().node_id() != node_id
                || evidence.target_epoch() != Some(expected_target_epoch)
                || evidence.transfer() != Some(expected_transfer)
            {
                return Err(LivePgMetadataTransferFailure::fatal(format!(
                    "destination {} returned staging evidence for a different PG transition or proof",
                    node_id.as_u32()
                )));
            }
            publications.push(UnavailablePgStagingPublicationBinding {
                node_id,
                node_incarnation: evidence.actor().node_incarnation(),
                endpoint: evidence.actor().endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(evidence.as_bytes()),
            });
        }
        publications.sort_by_key(|binding| binding.node_id);
        Ok(publications)
    }

    fn publish_staging_artifact_to_destination(
        &self,
        authorized: &AuthorizedUnavailablePgMetadataTransfer,
        runtime: &ClusterRuntimeMapSnapshot,
        node_id: NodeId,
    ) -> Result<MetadataTransferStagingReceipt, LivePgMetadataTransferFailure> {
        let authorization = authorized
            .authorizations
            .iter()
            .find(|authorization| authorization.destination_node_id() == node_id)
            .ok_or_else(|| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} has no committed staging authorization for destination {}",
                    authorized.work.pg_id().get(),
                    node_id.as_u32()
                ))
            })?;
        let node = runtime
            .nodes()
            .iter()
            .find(|candidate| candidate.node_id() == node_id)
            .ok_or_else(|| {
                LivePgMetadataTransferFailure::fatal(format!(
                    "PG {} staging destination {} is absent from the current authority map",
                    authorized.work.pg_id().get(),
                    node_id.as_u32()
                ))
            })?;
        #[cfg(test)]
        if let LivePgMetadataTransferStorageTransport::InProcess {
            staging_stores,
            staging_artifact_publish_failures,
            ..
        } = &self.transport
        {
            if let Some((disposition, notification)) = staging_artifact_publish_failures
                .lock()
                .expect("staging artifact publish failure injection poisoned")
                .get_mut(&node_id)
                .and_then(std::collections::VecDeque::pop_front)
            {
                if let Some(notification) = notification {
                    let _ = notification.send(authorized.work.pg_id());
                }
                return Err(LivePgMetadataTransferFailure {
                    disposition,
                    diagnostic: format!(
                        "injected staging artifact publish failure on node {}",
                        node_id.as_u32()
                    ),
                });
            }
            let identity = MetadataTransferStagingNodeIdentity::new(
                node_id,
                node.node_incarnation(),
                node.endpoint().to_owned(),
            )
            .map_err(staging_local_failure)?;
            let store = staging_stores
                .store(identity)
                .map_err(staging_local_failure)?;
            store
                .create_intent_authorized(authorization, &authorized.intent)
                .map_err(staging_local_failure)?;
            return store
                .publish_artifact_authorized(
                    authorization,
                    &authorized.intent,
                    &authorized.staged_artifact,
                )
                .map_err(staging_local_failure);
        }

        let client = self.staging_rpc_client(runtime.cluster_epoch(), node_id, node.endpoint())?;
        client
            .create_metadata_transfer_staging_intent(authorization, &authorized.intent)
            .map_err(staging_rpc_failure)?;
        client
            .publish_metadata_transfer_staging_artifact(
                authorization,
                &authorized.intent,
                &authorized.staged_artifact,
            )
            .map_err(staging_rpc_failure)
    }

    fn staging_rpc_client(
        &self,
        cluster_epoch: ClusterEpoch,
        node_id: NodeId,
        advertised_endpoint: &str,
    ) -> Result<UnixStorageNodeClient, LivePgMetadataTransferFailure> {
        let (endpoint, auth) = match &self.transport {
            LivePgMetadataTransferStorageTransport::Unix { auth } => {
                let auth = match auth {
                    Some(LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(auth)) => {
                        Some(Arc::new(StorageRpcClientAuthConfig::from(auth.clone())))
                    }
                    Some(LivePgMetadataTransferStorageAuth::Frontend(_)) => {
                        return Err(LivePgMetadataTransferFailure::fatal(
                            "metadata-transfer staging requires its dedicated storage RPC capability"
                                .to_owned(),
                        ));
                    }
                    None => None,
                };
                (StorageRpcClientEndpoint::unix(advertised_endpoint), auth)
            }
            LivePgMetadataTransferStorageTransport::ConfiguredEndpoints { endpoints, auth } => {
                let endpoint = endpoints
                    .iter()
                    .find_map(|(candidate, endpoint)| {
                        (*candidate == node_id).then(|| endpoint.clone())
                    })
                    .ok_or_else(|| {
                        LivePgMetadataTransferFailure::fatal(format!(
                            "metadata-transfer staging has no configured endpoint for node {}",
                            node_id.as_u32()
                        ))
                    })?;
                let auth = match auth {
                    LivePgMetadataTransferStorageAuth::LivePgMetadataTransfer(auth) => {
                        Arc::new(StorageRpcClientAuthConfig::from(auth.clone()))
                    }
                    LivePgMetadataTransferStorageAuth::Frontend(_) => {
                        return Err(LivePgMetadataTransferFailure::fatal(
                            "metadata-transfer staging requires its dedicated storage RPC capability"
                                .to_owned(),
                        ));
                    }
                };
                (endpoint, Some(auth))
            }
            #[cfg(test)]
            LivePgMetadataTransferStorageTransport::InProcess { .. } => {
                unreachable!("in-process staging is handled before RPC client construction")
            }
        };
        Ok(
            UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                node_id,
                cluster_epoch,
                endpoint,
                self.admission_settings,
                auth,
            ),
        )
    }

    fn maybe_fail(&self, point: LivePgMetadataTransferFailpoint) -> Result<(), String> {
        if self.failpoint == Some(point) {
            return Err(format!(
                "injected metadata transfer live failure at {}",
                point.label()
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn run_after_transfer_install_hook(&self) {
        if let Some(hook) = &self.after_transfer_install_hook {
            hook();
        }
    }

    #[cfg(not(test))]
    fn run_after_transfer_install_hook(&self) {}

    #[cfg(test)]
    fn run_after_transfer_prepare_hook(&self, pg_id: PgId, destination_epoch: ClusterEpoch) {
        if let Some(hook) = &self.after_transfer_prepare_hook {
            hook(pg_id, destination_epoch);
        }
    }

    #[cfg(not(test))]
    fn run_after_transfer_prepare_hook(&self, _pg_id: PgId, _destination_epoch: ClusterEpoch) {}

    #[cfg(test)]
    fn run_after_transfer_import_hook(&self, pg_id: PgId, destination_epoch: ClusterEpoch) {
        if let Some(hook) = &self.after_transfer_import_hook {
            hook(pg_id, destination_epoch);
        }
    }

    #[cfg(not(test))]
    fn run_after_transfer_import_hook(&self, _pg_id: PgId, _destination_epoch: ClusterEpoch) {}

    fn transfer_typed(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        unavailable_transition: Option<
            &crate::control_plane::UnavailablePgTransitionMutationBinding,
        >,
    ) -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferError> {
        let mut stage = LivePgMetadataTransferStage::Preflight;
        let result =
            (|| -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferFailure> {
                let prepared = self.prepare_transfer_typed(
                    pg_id,
                    acting_set,
                    unavailable_transition,
                    &mut stage,
                )?;
                let installed = match prepared {
                    LivePgMetadataTransferPreparation::Completed(summary) => return Ok(summary),
                    LivePgMetadataTransferPreparation::Installed(installed) => installed,
                    LivePgMetadataTransferPreparation::Prepared(prepared) => {
                        self.run_after_transfer_prepare_hook(
                            pg_id,
                            prepared.install_member.expected_destination_epoch,
                        );
                        stage = LivePgMetadataTransferStage::Install;
                        match self.install_prepared_transfer(prepared)? {
                            LivePgMetadataTransferInstallation::Completed(summary) => {
                                self.maybe_fail(
                                    LivePgMetadataTransferFailpoint::AfterTransferInstall,
                                )?;
                                return Ok(summary);
                            }
                            LivePgMetadataTransferInstallation::Installed(installed) => installed,
                        }
                    }
                };
                stage = LivePgMetadataTransferStage::Install;
                self.maybe_fail(LivePgMetadataTransferFailpoint::AfterTransferInstall)?;
                stage = LivePgMetadataTransferStage::Import;
                let destination_epoch = installed.destination_epoch;
                let summary = self.import_installed_transfer(installed)?;
                self.run_after_transfer_import_hook(pg_id, destination_epoch);
                Ok(summary)
            })();
        result.map_err(|diagnostic| LivePgMetadataTransferError::at_stage(stage, diagnostic))
    }

    fn prepare_transfer_typed(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        unavailable_transition: Option<
            &crate::control_plane::UnavailablePgTransitionMutationBinding,
        >,
        stage: &mut LivePgMetadataTransferStage,
    ) -> Result<LivePgMetadataTransferPreparation, LivePgMetadataTransferFailure> {
        if let Some(summary) = self.completed_summary(pg_id, &acting_set)? {
            return Ok(LivePgMetadataTransferPreparation::Completed(summary));
        }
        *stage = LivePgMetadataTransferStage::Fence;
        let fenced = self
            .control_plane
            .fence_with_source_lease(pg_id, unavailable_transition)
            .map_err(|error| {
                control_plane_transfer_failure(
                    "failed to fence live PG for metadata transfer",
                    error,
                )
            })?;
        let (fenced_runtime, source_lease_deadline_ms) = fenced.into_parts();
        let source_node_id = peering_source_node_id(&fenced_runtime, pg_id)?;
        if let Some(source_lease_deadline_ms) = source_lease_deadline_ms {
            wait_for_source_lease_to_expire(source_lease_deadline_ms);
        }
        let fenced_source_runtime = self
            .control_plane
            .refresh_fence(pg_id, unavailable_transition)
            .map_err(|error| {
                control_plane_transfer_failure(
                    "failed to refresh fenced PG metadata transfer map",
                    error,
                )
            })?;
        peering_source_route_matches(&fenced_source_runtime, pg_id, source_node_id)?;
        let expected_source_route = peering_route(&fenced_source_runtime, pg_id)?.clone();
        let source_runtime = self
            .control_plane
            .serving_pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis())
            .map_err(|error| {
                control_plane_transfer_failure(
                    "failed to obtain serving metadata transfer source map",
                    error,
                )
            })?;
        let source_route = peering_route(&source_runtime, pg_id)?;
        if !transfer_route_matches(&expected_source_route, source_route) {
            return Err(format!(
                "serving metadata transfer source for PG {} changed: expected route {:?}; actual route {:?}",
                pg_id.get(), expected_source_route, source_route
            )
            .into());
        }
        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterFence)?;

        *stage = LivePgMetadataTransferStage::Export;
        if let Some(existing_transfer) = source_route.peering_metadata_transfer() {
            if source_route.acting_set() != acting_set.as_slice() {
                return Err(format!(
                    "PG {} already has transfer marker for acting set {:?}, not requested {:?}",
                    pg_id.get(),
                    source_route.acting_set(),
                    acting_set
                )
                .into());
            }
            let source_route_epoch = source_route
                .peering_metadata_transfer_source_route_epoch()
                .ok_or_else(|| {
                    format!(
                        "PG {} transfer marker is missing source route epoch",
                        pg_id.get()
                    )
                })?;
            let existing_source_node_id = source_route
                .peering_metadata_transfer_source_node_id()
                .ok_or_else(|| {
                format!(
                    "PG {} transfer marker is missing source node id",
                    pg_id.get()
                )
            })?;
            let export_runtime = source_runtime
                .metadata_transfer_source_runtime_map(
                    &expected_source_route,
                    source_route_epoch,
                    existing_source_node_id,
                )
                .map_err(|error| {
                    format!(
                        "failed to authorize PG {} metadata transfer source route at epoch {}: {}",
                        pg_id.get(),
                        source_route_epoch.get(),
                        error.retained_diagnostic_message()
                    )
                })?;
            let export_route = peering_route(&export_runtime, pg_id)?;
            if export_route.primary_node_id() != existing_source_node_id {
                return Err(format!(
                    "PG {} transfer marker source node {} does not match retained source route primary {}",
                    pg_id.get(),
                    existing_source_node_id.as_u32(),
                    export_route.primary_node_id().as_u32()
                )
                .into());
            }
            let source_cluster = self.build_cluster(&export_runtime)?;
            let artifact = self.export_retrying_stale_route(
                &source_cluster,
                ExportContext {
                    pg_id,
                    source_node_id: existing_source_node_id,
                    expected_current_route: source_route.clone(),
                    export_epoch: source_route_epoch,
                },
            )?;
            if artifact.cluster_epoch() != existing_transfer.source_epoch()
                || artifact.source_metadata_proof() != existing_transfer.source_metadata_proof()
            {
                return Err(format!(
                    "PG {} resumed transfer artifact {:?} at epoch {} does not match installed marker {:?}",
                    pg_id.get(),
                    artifact.source_metadata_proof(),
                    artifact.cluster_epoch().get(),
                    existing_transfer
                )
                .into());
            }
            let destination_epoch = source_route
                .peering_metadata_transfer_destination_epoch()
                .ok_or_else(|| {
                    format!(
                        "PG {} transfer marker is missing its committed destination epoch",
                        pg_id.get()
                    )
                })?;
            let recomputed_imported_proof =
                StorageCluster::metadata_transfer_imported_proof_at_epoch(
                    &artifact,
                    destination_epoch,
                )
                .map_err(|error| {
                    format!("failed to compute imported PG metadata proof: {error}")
                })?;
            if recomputed_imported_proof != existing_transfer.metadata_proof() {
                return Err(format!(
                    "PG {} resumed transfer imported proof {:?} does not match installed marker {:?}",
                    pg_id.get(),
                    recomputed_imported_proof,
                    existing_transfer.metadata_proof()
                )
                .into());
            }
            let destination_runtime = source_runtime
                .metadata_transfer_destination_runtime_map(pg_id, &acting_set, existing_transfer)
                .map_err(|error| {
                    format!(
                        "failed to authorize resumed PG {} metadata transfer destination route: {}",
                        pg_id.get(),
                        error.retained_diagnostic_message()
                    )
                })?;
            return Ok(LivePgMetadataTransferPreparation::Installed(Box::new(
                InstalledLivePgMetadataTransfer {
                    pg_id,
                    acting_set,
                    artifact,
                    destination_runtime,
                    destination_epoch,
                    imported_proof: existing_transfer.metadata_proof(),
                    source_node_id: existing_source_node_id,
                },
            )));
        }

        let export_runtime = source_runtime
            .metadata_transfer_source_runtime_map(
                &expected_source_route,
                expected_source_route.cluster_epoch(),
                source_node_id,
            )
            .map_err(|error| {
                format!(
                    "failed to authorize PG {} fenced metadata transfer source: {}",
                    pg_id.get(),
                    error.retained_diagnostic_message()
                )
            })?;
        let source_cluster = self.build_cluster(&export_runtime)?;
        let artifact = self.export_retrying_stale_route(
            &source_cluster,
            ExportContext {
                pg_id,
                source_node_id,
                expected_current_route: source_route.clone(),
                export_epoch: expected_source_route.cluster_epoch(),
            },
        )?;
        let install_member = prepare_live_pg_metadata_transfer_install_member(
            pg_id,
            acting_set,
            unavailable_transition.cloned(),
            &source_runtime,
            &artifact,
        )?;
        let source_route = source_route.clone();
        Ok(LivePgMetadataTransferPreparation::Prepared(Box::new(
            PreparedLivePgMetadataTransfer {
                source_node_id,
                source_route,
                artifact,
                install_member,
            },
        )))
    }

    fn install_prepared_transfer(
        &self,
        prepared: Box<PreparedLivePgMetadataTransfer>,
    ) -> Result<LivePgMetadataTransferInstallation, LivePgMetadataTransferFailure> {
        let PreparedLivePgMetadataTransfer {
            source_node_id,
            source_route,
            artifact,
            install_member,
        } = *prepared;
        let install =
            self.install_transfer_retrying_epoch(&source_route, &artifact, install_member)?;
        Ok(match install {
            TransferInstallOutcome::Ready {
                pg_id,
                acting_set,
                destination_runtime,
                destination_epoch,
                imported_proof,
            } => LivePgMetadataTransferInstallation::Installed(Box::new(
                InstalledLivePgMetadataTransfer {
                    pg_id,
                    acting_set,
                    artifact,
                    destination_runtime: *destination_runtime,
                    destination_epoch,
                    imported_proof,
                    source_node_id,
                },
            )),
            TransferInstallOutcome::Completed {
                destination_epoch,
                imported_proof,
            } => LivePgMetadataTransferInstallation::Completed(summary(
                source_node_id,
                artifact.cluster_epoch(),
                destination_epoch,
                imported_proof,
                false,
            )),
        })
    }

    fn import_installed_transfer(
        &self,
        installed: Box<InstalledLivePgMetadataTransfer>,
    ) -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferFailure> {
        let InstalledLivePgMetadataTransfer {
            pg_id,
            acting_set,
            artifact,
            destination_runtime,
            destination_epoch,
            imported_proof,
            source_node_id,
        } = *installed;
        let destination_cluster = self.build_cluster(&destination_runtime)?;
        let expected_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            artifact.cluster_epoch(),
            artifact.source_metadata_proof(),
            imported_proof,
        );
        let actual_imported_proof = self.import_retrying_stale_route(
            &destination_cluster,
            ImportContext {
                pg_id,
                acting_set: &acting_set,
                destination_epoch,
                expected_transfer,
                imported_proof,
            },
            &artifact,
        )?;
        if actual_imported_proof != imported_proof {
            return Err(format!(
                "imported PG metadata proof {:?} did not match expected {:?}",
                actual_imported_proof, imported_proof
            )
            .into());
        }
        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterImport)?;
        Ok(summary(
            source_node_id,
            artifact.cluster_epoch(),
            destination_epoch,
            imported_proof,
            false,
        ))
    }

    fn completed_summary(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
    ) -> Result<Option<LivePgMetadataTransferSummary>, LivePgMetadataTransferFailure> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let runtime_map = match self
                .control_plane
                .pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis())
            {
                Ok(runtime_map) => runtime_map,
                Err(error) if error.is_retryable_runtime_map_observation_error() => {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(error) => {
                    return Err(control_plane_transfer_failure(
                        &format!(
                            "failed to fetch control-plane PG {} runtime map before metadata transfer",
                            pg_id.get()
                        ),
                        error,
                    ));
                }
            };
            let Some(route) = runtime_map
                .pg_routes()
                .iter()
                .find(|route| route.pg_id() == pg_id)
            else {
                return Ok(None);
            };
            if route.state() != PgState::Active || route.acting_set() != acting_set {
                return Ok(None);
            }
            return Ok(Some(summary(
                route.primary_node_id(),
                route.cluster_epoch(),
                route.cluster_epoch(),
                PgMetadataProof::empty(),
                true,
            )));
        }
    }

    fn install_transfer_retrying_epoch(
        &self,
        source_route: &PgRouteSnapshot,
        artifact: &PgMetadataTransferArtifact,
        mut install_member: PreparedLivePgMetadataTransferInstallMember,
    ) -> Result<TransferInstallOutcome, LivePgMetadataTransferFailure> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            install_member.validate_epoch_binding()?;
            if install_member.unavailable_transition.is_some() {
                return Err(LivePgMetadataTransferFailure::fatal(
                    "unavailable-PG transfer installation requires the plural receipt-bound control-plane command"
                        .to_owned(),
                ));
            }
            let pg_id = install_member.pg_id;
            let destination_epoch = install_member.expected_destination_epoch;
            let transfer = install_member.transfer;
            let imported_proof = transfer.metadata_proof();
            match self.control_plane.install_transfer(
                pg_id,
                install_member.acting_set.clone(),
                transfer,
                destination_epoch,
            ) {
                Ok(runtime) => {
                    if runtime.cluster_epoch() < destination_epoch {
                        return Err(format!(
                            "confirmed PG {} metadata transfer destination map regressed below committed epoch {} to {}",
                            pg_id.get(),
                            destination_epoch.get(),
                            runtime.cluster_epoch().get()
                        )
                        .into());
                    }
                    self.run_after_transfer_install_hook();
                    let serving_runtime = loop {
                        match self.control_plane.serving_pg_runtime_map_snapshot(
                            pg_id,
                            crate::clock::current_time_millis(),
                        ) {
                            Ok(runtime) => break runtime,
                            Err(error)
                                if error.is_retryable_runtime_map_observation_error()
                                    && Instant::now() < deadline =>
                            {
                                thread::sleep(Duration::from_millis(100));
                            }
                            Err(error) => {
                                return Err(control_plane_transfer_failure(
                                    &format!(
                                        "failed to obtain serving PG {} metadata transfer destination map after install",
                                        pg_id.get()
                                    ),
                                    error,
                                ));
                            }
                        }
                    };
                    if serving_runtime.cluster_epoch() < destination_epoch {
                        return Err(format!(
                            "serving PG {} metadata transfer destination map regressed below committed epoch {} to {}",
                            pg_id.get(),
                            destination_epoch.get(),
                            serving_runtime.cluster_epoch().get()
                        )
                        .into());
                    }
                    if active_route_matches(
                        &serving_runtime,
                        pg_id,
                        &install_member.acting_set,
                        imported_proof,
                    )? {
                        return Ok(TransferInstallOutcome::Completed {
                            destination_epoch,
                            imported_proof,
                        });
                    }
                    let destination_runtime = serving_runtime
                        .metadata_transfer_destination_runtime_map(
                            pg_id,
                            &install_member.acting_set,
                            transfer,
                        )
                        .map_err(|error| {
                            format!(
                                "failed to authorize confirmed PG {} metadata transfer destination route: {}",
                                pg_id.get(),
                                error.retained_diagnostic_message()
                            )
                        })?;
                    return Ok(TransferInstallOutcome::Ready {
                        pg_id,
                        acting_set: install_member.acting_set,
                        destination_runtime: Box::new(destination_runtime),
                        destination_epoch,
                        imported_proof,
                    });
                }
                Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                    pg_id: mismatch_pg_id,
                    expected_destination_epoch,
                    ..
                }) if mismatch_pg_id == pg_id.get()
                    && expected_destination_epoch == destination_epoch =>
                {
                    if Instant::now() >= deadline {
                        return Err(LivePgMetadataTransferFailure::retryable(
                            "failed to install transfer-backed live PG acting set before the destination-epoch retry deadline".to_owned(),
                        ));
                    }
                    let source_runtime = loop {
                        match self.control_plane.serving_pg_runtime_map_snapshot(
                            pg_id,
                            crate::clock::current_time_millis(),
                        ) {
                            Ok(runtime) => break runtime,
                            Err(error)
                                if error.is_retryable_runtime_map_observation_error()
                                    && Instant::now() < deadline =>
                            {
                                thread::sleep(Duration::from_millis(100));
                            }
                            Err(error) => {
                                return Err(control_plane_transfer_failure(
                                    "failed to refresh metadata transfer source map after destination epoch changed",
                                    error,
                                ));
                            }
                        }
                    };
                    let refreshed_route = peering_route(&source_runtime, pg_id)?;
                    if !transfer_route_matches(source_route, refreshed_route) {
                        return Err(format!(
                            "metadata transfer source route for PG {} changed while rebasing the destination epoch: expected {:?}; actual {:?}",
                            pg_id.get(), source_route, refreshed_route
                        )
                        .into());
                    }
                    install_member.rebase(&source_runtime, artifact)?;
                }
                Err(error) => {
                    return Err(control_plane_transfer_failure(
                        "failed to install transfer-backed live PG acting set",
                        error,
                    ));
                }
            }
        }
    }

    fn export_retrying_stale_route(
        &self,
        initial_cluster: &Arc<StorageCluster>,
        context: ExportContext,
    ) -> Result<PgMetadataTransferArtifact, LivePgMetadataTransferFailure> {
        let mut cluster = Arc::clone(initial_cluster);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let result = self.injected_route_refresh_failure(true).map_or_else(
                || {
                    cluster.export_pg_metadata_transfer_artifact_for_live_transfer(
                        context.pg_id,
                        context.source_node_id,
                    )
                },
                Err,
            );
            match result {
                Ok(artifact) => return Ok(artifact),
                Err(error) if error.requires_route_refresh_retry() => {
                    if let Some(refreshed) = self.refresh_export_route(&context)? {
                        cluster = refreshed;
                    }
                    if Instant::now() >= deadline {
                        return Err(LivePgMetadataTransferFailure::retryable(format!(
                            "timed out exporting PG metadata transfer artifact: {error}"
                        )));
                    }
                }
                Err(error) => {
                    return Err(metadata_transfer_failure(
                        "failed to export PG metadata transfer artifact",
                        error,
                    ));
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn refresh_export_route(
        &self,
        context: &ExportContext,
    ) -> Result<Option<Arc<StorageCluster>>, LivePgMetadataTransferFailure> {
        let runtime_map = match self
            .control_plane
            .serving_pg_runtime_map_snapshot(context.pg_id, crate::clock::current_time_millis())
        {
            Ok(runtime_map) => runtime_map,
            Err(error) if error.is_retryable_runtime_map_observation_error() => return Ok(None),
            Err(error) => {
                return Err(control_plane_transfer_failure(
                    &format!(
                        "failed to refresh live PG {} metadata transfer source state",
                        context.pg_id.get()
                    ),
                    error,
                ));
            }
        };
        if runtime_map.cluster_epoch() < context.expected_current_route.cluster_epoch() {
            return Ok(None);
        }
        let current_route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == context.pg_id)
            .ok_or_else(|| {
                format!(
                    "refreshed metadata transfer source for PG {} is missing at authoritative epoch {}; expected route {:?}",
                    context.pg_id.get(), runtime_map.cluster_epoch().get(), context.expected_current_route
                )
            })?;
        if !transfer_route_matches(&context.expected_current_route, current_route) {
            return Err(format!(
                "refreshed metadata transfer source for PG {} changed: expected route {:?}; actual route {:?} at authoritative epoch {}",
                context.pg_id.get(), context.expected_current_route, current_route, runtime_map.cluster_epoch().get()
            )
            .into());
        }
        let export_epoch =
            refreshed_export_source_epoch(&runtime_map, current_route, context.export_epoch);
        let export_runtime = runtime_map
            .metadata_transfer_source_runtime_map(
                current_route,
                export_epoch,
                context.source_node_id,
            )
            .map_err(|error| {
                format!(
                    "failed to authorize refreshed PG {} metadata transfer source route at epoch {}: {}",
                    context.pg_id.get(), export_epoch.get(), error.retained_diagnostic_message()
                )
            })?;
        let export_route = peering_route(&export_runtime, context.pg_id)?;
        if export_route.primary_node_id() != context.source_node_id {
            return Err(format!(
                "refreshed PG {} metadata transfer source route at epoch {} has primary {}, expected {}",
                context.pg_id.get(), export_epoch.get(), export_route.primary_node_id().as_u32(), context.source_node_id.as_u32()
            )
            .into());
        }
        Ok(self.build_cluster(&export_runtime).map(Some)?)
    }

    fn import_retrying_stale_route(
        &self,
        initial_cluster: &Arc<StorageCluster>,
        context: ImportContext<'_>,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, LivePgMetadataTransferFailure> {
        let mut cluster = Arc::clone(initial_cluster);
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut first_refresh_error = None;
        let mut refresh_failures = 0_u64;
        loop {
            let result = self
                .injected_import_pending_command_failure(context.pg_id)
                .or_else(|| self.injected_route_refresh_failure(false))
                .map_or_else(
                    || cluster.import_pg_metadata_transfer_artifact_from_retained_log(artifact),
                    Err,
                );
            match result {
                Ok(proof) => return Ok(proof),
                Err(error)
                    if error.requires_route_refresh_retry()
                        || error.is_transient_import_blocker() =>
                {
                    refresh_failures = refresh_failures.saturating_add(1);
                    first_refresh_error.get_or_insert_with(|| error.to_string());
                    match self.refresh_import_route(&context)? {
                        ImportRouteRefresh::Completed => return Ok(context.imported_proof),
                        ImportRouteRefresh::Retry(refreshed) => cluster = refreshed,
                        ImportRouteRefresh::NotReady => {}
                    }
                    if Instant::now() >= deadline {
                        return Err(LivePgMetadataTransferFailure::retryable(format!(
                            "timed out importing PG metadata transfer artifact after {refresh_failures} route-refresh failures (first: {}; last: {error})",
                            first_refresh_error.as_deref().unwrap_or("<unavailable>")
                        )));
                    }
                }
                Err(error) => {
                    return Err(metadata_transfer_failure(
                        "failed to import PG metadata transfer artifact",
                        error,
                    ));
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    #[cfg(test)]
    fn injected_route_refresh_failure(
        &self,
        export: bool,
    ) -> Option<crate::error::PgMetadataTransferError> {
        let LivePgMetadataTransferStorageTransport::InProcess {
            export_route_refresh_failures,
            import_route_refresh_failures,
            ..
        } = &self.transport
        else {
            return None;
        };
        let remaining = if export {
            export_route_refresh_failures
        } else {
            import_route_refresh_failures
        };
        remaining
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |value| value.checked_sub(1),
            )
            .ok()
            .map(|_| {
                crate::error::PgMetadataTransferError::Store(crate::StoreError::RouteMapExpired {
                    cluster_epoch: ClusterEpoch::INITIAL,
                    valid_until_ms: 1,
                    now_ms: 2,
                })
            })
    }

    #[cfg(not(test))]
    fn injected_route_refresh_failure(
        &self,
        _export: bool,
    ) -> Option<crate::error::PgMetadataTransferError> {
        None
    }

    #[cfg(test)]
    fn injected_import_pending_command_failure(
        &self,
        pg_id: PgId,
    ) -> Option<crate::error::PgMetadataTransferError> {
        let LivePgMetadataTransferStorageTransport::InProcess {
            import_pending_command_failures,
            ..
        } = &self.transport
        else {
            return None;
        };
        import_pending_command_failures
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |value| value.checked_sub(1),
            )
            .ok()
            .map(
                |_| crate::error::PgMetadataTransferError::PendingMetadataCommand {
                    node_id: NodeId::new(pg_id.get()),
                },
            )
    }

    #[cfg(not(test))]
    fn injected_import_pending_command_failure(
        &self,
        _pg_id: PgId,
    ) -> Option<crate::error::PgMetadataTransferError> {
        None
    }

    fn refresh_import_route(
        &self,
        context: &ImportContext<'_>,
    ) -> Result<ImportRouteRefresh, LivePgMetadataTransferFailure> {
        let observed_runtime = match self
            .control_plane
            .pg_runtime_map_snapshot(context.pg_id, crate::clock::current_time_millis())
        {
            Ok(runtime) => runtime,
            Err(error) if error.is_retryable_runtime_map_observation_error() => {
                return Ok(ImportRouteRefresh::NotReady);
            }
            Err(error) => {
                return Err(control_plane_transfer_failure(
                    &format!(
                        "failed to refresh live PG {} metadata transfer state",
                        context.pg_id.get()
                    ),
                    error,
                ));
            }
        };
        if observed_runtime.cluster_epoch() < context.destination_epoch {
            return Ok(ImportRouteRefresh::NotReady);
        }
        let route = observed_runtime
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == context.pg_id)
            .ok_or_else(|| {
                format!(
                    "refreshed metadata transfer destination for PG {} is missing at authoritative epoch {}",
                    context.pg_id.get(), observed_runtime.cluster_epoch().get()
                )
            })?;
        if active_route_matches(
            &observed_runtime,
            context.pg_id,
            context.acting_set,
            context.imported_proof,
        )? {
            return Ok(ImportRouteRefresh::Completed);
        }
        if route.state() != PgState::Peering
            || route.acting_set() != context.acting_set
            || route.peering_metadata_transfer() != Some(context.expected_transfer)
            || route.peering_metadata_transfer_destination_epoch()
                != Some(context.destination_epoch)
        {
            return Err(format!(
                "refreshed metadata transfer destination for PG {} does not match committed transfer state",
                context.pg_id.get()
            )
            .into());
        }
        let expected_route = route.clone();
        let runtime_map = match self
            .control_plane
            .serving_pg_runtime_map_snapshot(context.pg_id, crate::clock::current_time_millis())
        {
            Ok(runtime) => runtime,
            Err(error) if error.is_retryable_runtime_map_observation_error() => {
                return Ok(ImportRouteRefresh::NotReady);
            }
            Err(error) => {
                return Err(control_plane_transfer_failure(
                    &format!(
                        "failed to obtain serving PG {} metadata transfer destination map",
                        context.pg_id.get()
                    ),
                    error,
                ));
            }
        };
        let serving_route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == context.pg_id)
            .ok_or_else(|| {
                format!(
                    "serving metadata transfer destination map omitted PG {}",
                    context.pg_id.get()
                )
            })?;
        if !transfer_route_matches(&expected_route, serving_route) {
            return Err(format!(
                "serving metadata transfer destination for PG {} changed after scoped observation",
                context.pg_id.get()
            )
            .into());
        }
        let destination_runtime = runtime_map
            .metadata_transfer_destination_runtime_map(
                context.pg_id,
                context.acting_set,
                context.expected_transfer,
            )
            .map_err(|error| {
                format!(
                    "failed to authorize live PG {} metadata transfer destination route: {}",
                    context.pg_id.get(),
                    error.retained_diagnostic_message()
                )
            })?;
        Ok(self
            .build_cluster(&destination_runtime)
            .map(ImportRouteRefresh::Retry)?)
    }
}

fn configured_endpoints_for_runtime_map(
    runtime_map: &ClusterRuntimeMapSnapshot,
    endpoints: &[(NodeId, StorageRpcClientEndpoint)],
) -> Vec<(NodeId, StorageRpcClientEndpoint)> {
    endpoints
        .iter()
        .filter(|(node_id, _)| {
            runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == *node_id)
        })
        .map(|(node_id, endpoint)| (*node_id, endpoint.clone()))
        .collect()
}

struct ExportContext {
    pg_id: PgId,
    source_node_id: NodeId,
    expected_current_route: PgRouteSnapshot,
    export_epoch: ClusterEpoch,
}

enum LivePgMetadataTransferPreparation {
    Completed(LivePgMetadataTransferSummary),
    Prepared(Box<PreparedLivePgMetadataTransfer>),
    Installed(Box<InstalledLivePgMetadataTransfer>),
}

struct PreparedLivePgMetadataTransfer {
    source_node_id: NodeId,
    source_route: PgRouteSnapshot,
    artifact: PgMetadataTransferArtifact,
    install_member: PreparedLivePgMetadataTransferInstallMember,
}

struct PreparedLivePgMetadataTransferInstallMember {
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    unavailable_transition: Option<crate::control_plane::UnavailablePgTransitionMutationBinding>,
    source_runtime_epoch: ClusterEpoch,
    expected_destination_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
}

fn prepare_live_pg_metadata_transfer_install_member(
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    unavailable_transition: Option<crate::control_plane::UnavailablePgTransitionMutationBinding>,
    source_runtime: &ClusterRuntimeMapSnapshot,
    artifact: &PgMetadataTransferArtifact,
) -> Result<PreparedLivePgMetadataTransferInstallMember, LivePgMetadataTransferFailure> {
    let expected_destination_epoch = source_runtime
        .cluster_epoch()
        .get()
        .checked_add(1)
        .and_then(ClusterEpoch::new)
        .ok_or_else(|| "destination cluster epoch overflowed".to_owned())?;
    let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
        artifact,
        expected_destination_epoch,
    )
    .map_err(|error| format!("failed to compute imported PG metadata proof: {error}"))?;
    Ok(PreparedLivePgMetadataTransferInstallMember {
        pg_id,
        acting_set,
        unavailable_transition,
        source_runtime_epoch: source_runtime.cluster_epoch(),
        expected_destination_epoch,
        transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
            artifact.cluster_epoch(),
            artifact.source_metadata_proof(),
            imported_proof,
        ),
    })
}

impl PreparedLivePgMetadataTransferInstallMember {
    fn validate_epoch_binding(&self) -> Result<(), LivePgMetadataTransferFailure> {
        let expected_destination_epoch = self
            .source_runtime_epoch
            .get()
            .checked_add(1)
            .and_then(ClusterEpoch::new)
            .ok_or_else(|| "destination cluster epoch overflowed".to_owned())?;
        if self.expected_destination_epoch != expected_destination_epoch {
            return Err(format!(
                "prepared metadata transfer destination epoch {} does not follow source runtime epoch {}",
                self.expected_destination_epoch.get(),
                self.source_runtime_epoch.get()
            )
            .into());
        }
        Ok(())
    }

    fn rebase(
        &mut self,
        source_runtime: &ClusterRuntimeMapSnapshot,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<(), LivePgMetadataTransferFailure> {
        let rebased = prepare_live_pg_metadata_transfer_install_member(
            self.pg_id,
            self.acting_set.clone(),
            self.unavailable_transition.clone(),
            source_runtime,
            artifact,
        )?;
        *self = rebased;
        Ok(())
    }
}

enum LivePgMetadataTransferInstallation {
    Completed(LivePgMetadataTransferSummary),
    Installed(Box<InstalledLivePgMetadataTransfer>),
}

struct InstalledLivePgMetadataTransfer {
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    artifact: PgMetadataTransferArtifact,
    destination_runtime: ClusterRuntimeMapSnapshot,
    destination_epoch: ClusterEpoch,
    imported_proof: PgMetadataProof,
    source_node_id: NodeId,
}

struct ImportContext<'a> {
    pg_id: PgId,
    acting_set: &'a [NodeId],
    destination_epoch: ClusterEpoch,
    expected_transfer: PgMetadataTransferProof,
    imported_proof: PgMetadataProof,
}

enum ImportRouteRefresh {
    Completed,
    Retry(Arc<StorageCluster>),
    NotReady,
}

enum TransferInstallOutcome {
    Ready {
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        destination_runtime: Box<ClusterRuntimeMapSnapshot>,
        destination_epoch: ClusterEpoch,
        imported_proof: PgMetadataProof,
    },
    Completed {
        destination_epoch: ClusterEpoch,
        imported_proof: PgMetadataProof,
    },
}

fn summary(
    source_node_id: NodeId,
    source_epoch: ClusterEpoch,
    destination_epoch: ClusterEpoch,
    imported_proof: PgMetadataProof,
    already_completed: bool,
) -> LivePgMetadataTransferSummary {
    LivePgMetadataTransferSummary {
        source_node_id: source_node_id.as_u32(),
        source_epoch: source_epoch.get(),
        destination_epoch: destination_epoch.get(),
        imported_proof,
        already_completed,
    }
}

fn peering_route(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> Result<&PgRouteSnapshot, String> {
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .ok_or_else(|| format!("control-plane runtime map has no PG {}", pg_id.get()))?;
    if route.state() != PgState::Peering {
        return Err(format!(
            "PG {} must be Peering after live metadata transfer fence, got {:?}",
            pg_id.get(),
            route.state()
        ));
    }
    Ok(route)
}

fn peering_source_node_id(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> Result<NodeId, String> {
    Ok(peering_route(runtime_map, pg_id)?.primary_node_id())
}

fn peering_source_route_matches(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    source_node_id: NodeId,
) -> Result<(), String> {
    let actual = peering_source_node_id(runtime_map, pg_id)?;
    if actual == source_node_id {
        return Ok(());
    }
    Err(format!(
        "PG {} fenced source node changed from {} to {}",
        pg_id.get(),
        source_node_id.as_u32(),
        actual.as_u32()
    ))
}

fn transfer_route_matches(expected: &PgRouteSnapshot, actual: &PgRouteSnapshot) -> bool {
    expected.matches_metadata_transfer_route(actual)
}

fn refreshed_export_source_epoch(
    runtime_map: &ClusterRuntimeMapSnapshot,
    current_route: &PgRouteSnapshot,
    committed_source_epoch: ClusterEpoch,
) -> ClusterEpoch {
    // Before the marker commits, an unchanged fence may be rebased when an
    // unrelated authority mutation advances the global epoch. A committed
    // marker instead binds export to its retained source route.
    if current_route.peering_metadata_transfer().is_none() {
        runtime_map.cluster_epoch()
    } else {
        committed_source_epoch
    }
}

fn active_route_matches(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
    imported_proof: PgMetadataProof,
) -> Result<bool, String> {
    let Some(route) = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
    else {
        return Ok(false);
    };
    if route.state() != PgState::Active || route.acting_set() != acting_set {
        return Ok(false);
    }
    if route.active_metadata_proof() != Some(imported_proof) {
        return Err(format!(
            "active PG {} metadata proof {:?} does not match expected imported proof {:?}",
            pg_id.get(),
            route.active_metadata_proof(),
            imported_proof
        ));
    }
    Ok(true)
}

fn source_lease_wait_duration(now_ms: u64, lease_deadline_ms: u64) -> Option<Duration> {
    if now_ms >= lease_deadline_ms {
        return None;
    }
    Some(Duration::from_millis(
        (lease_deadline_ms - now_ms).clamp(1, 100),
    ))
}

fn wait_for_source_lease_to_expire(lease_deadline_ms: u64) {
    while let Some(duration) =
        source_lease_wait_duration(crate::clock::current_time_millis(), lease_deadline_ms)
    {
        thread::sleep(duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::TcpStream;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Mutex};

    use crate::control_plane::{
        ControlPlaneHeartbeatSink, ControlPlaneRpcServerListener, ControlPlaneRpcServerPolicy,
        ControlPlaneRpcServerRole, ControlPlaneStore, FileControlPlaneStore, NodeHeartbeat,
        NodeMembershipState, NodePgHeartbeatObservation, SingleAuthorityControlPlane,
        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    };
    use crate::control_plane_auth::{
        ControlPlaneAuthPrincipal, ControlPlaneScopedCredentialInput,
        ControlPlaneScopedCredentialStore,
    };
    use crate::control_plane_command::ControlPlaneCommandStateMachine;
    use crate::metadata_command::{
        metadata_command_log_hash, CreateBucketCommand, MetadataCommandEnvelope, MetadataCommandId,
        MetadataCommandLogIndex, MetadataCommandPayload,
    };
    use crate::node::SharedStorageNode;
    use crate::storage_node_server::{
        PreparedStorageNodeServer, StorageNodeProcessConfig, StorageNodeRpcListenerConfig,
    };
    use crate::{
        AclGrants, BucketObjectLockConfig, BucketOwnershipControls, BucketVersioningState,
        CreateBucketConfig, OwnerIdentity, UnavailablePgReconciliationWorker,
    };
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName};

    fn bound_plain_control_plane(
        socket_path: &std::path::Path,
    ) -> LivePgMetadataTransferControlPlaneClient {
        LivePgMetadataTransferControlPlaneClient::new(
            UnixControlPlaneClient::new(socket_path),
            None,
            None,
        )
        .unwrap()
    }

    fn submit_heartbeat_until_serving(
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_id: NodeId,
        endpoint: String,
        requested_lease_duration_ms: u64,
        pg_observations: Vec<NodePgHeartbeatObservation>,
        started_at_ms: u64,
    ) {
        submit_heartbeat_with_incarnation_until_serving(
            authority,
            node_id,
            1,
            endpoint,
            requested_lease_duration_ms,
            pg_observations,
            started_at_ms,
        );
    }

    fn submit_heartbeat_with_incarnation_until_serving(
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_id: NodeId,
        node_incarnation: u64,
        endpoint: String,
        requested_lease_duration_ms: u64,
        pg_observations: Vec<NodePgHeartbeatObservation>,
        started_at_ms: u64,
    ) {
        for offset_ms in 0..4 {
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation,
                        endpoint: endpoint.clone(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: pg_observations.clone(),
                    },
                    started_at_ms.saturating_add(offset_ms),
                )
                .unwrap();
            if lease.serving() {
                return;
            }
        }
        panic!(
            "authority did not grant node {} a serving lease",
            node_id.as_u32()
        );
    }

    fn prepared_live_transfer_authority(
        root: &std::path::Path,
        include_unserved_pg: bool,
    ) -> (
        Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
        u64,
        PgId,
        NodeId,
        NodeId,
    ) {
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            root.join("control-plane.state"),
        ))
        .unwrap();
        let source_node_id = NodeId::new(7);
        let destination_node_id = NodeId::new(8);
        let pg_id = PgId::new(11);
        let empty_store =
            crate::pg_store::PgStore::open(&root.join("empty-proof"), pg_id.get()).unwrap();
        let empty_state = empty_store.metadata_command_replica_state().unwrap();
        let empty_proof = PgMetadataProof {
            applied_log_index: empty_state.applied_log_index,
            applied_log_hash: empty_state.applied_log_hash,
            state_digest: empty_state.state_digest,
        };
        let now_ms = crate::clock::current_time_millis();
        authority
            .set_node_membership(source_node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(destination_node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(pg_id, vec![source_node_id])
            .unwrap();

        if include_unserved_pg {
            let unserved_node_id = NodeId::new(9);
            authority
                .set_node_membership(unserved_node_id, NodeMembershipState::Active)
                .unwrap();
            authority
                .set_pg_acting_set(PgId::new(12), vec![unserved_node_id])
                .unwrap();
        }

        let source_endpoint = root.join("source.sock").display().to_string();
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_endpoint.clone(),
            50,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: empty_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms,
        );
        authority
            .complete_pg_peering(pg_id, source_node_id, 1, now_ms.saturating_add(4))
            .unwrap();
        submit_heartbeat_until_serving(
            &mut authority,
            destination_node_id,
            root.join("destination.sock").display().to_string(),
            10_000,
            Vec::new(),
            now_ms.saturating_add(5),
        );
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_endpoint,
            50,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Active,
                metadata_proof: empty_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms.saturating_add(10),
        );

        (
            Arc::new(Mutex::new(authority)),
            now_ms,
            pg_id,
            source_node_id,
            destination_node_id,
        )
    }

    fn spawn_live_transfer_control_plane(
        socket_path: &std::path::Path,
        authority: Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
        authority_times_ms: Vec<u64>,
    ) -> thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(socket_path).unwrap();
        let listener = ControlPlaneRpcServerListener::unix(
            listener,
            4,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(1),
        )
        .unwrap();
        let policy =
            ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 4, 1024 * 1024)
                .unwrap();
        thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(authority, policy, authority_times_ms, |_| {})
                .unwrap();
        })
    }

    fn live_transfer_admin(
        root: &std::path::Path,
        socket_path: &std::path::Path,
    ) -> LivePgMetadataTransferAdmin {
        LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(socket_path),
            EcShape { k: 1, m: 0 },
            root.join("storage"),
        )
    }

    fn live_transfer_admin_with_staging_stores(
        root: &std::path::Path,
        socket_path: &std::path::Path,
        staging_stores: Arc<InProcessMetadataTransferStagingStores>,
    ) -> LivePgMetadataTransferAdmin {
        LivePgMetadataTransferAdmin::with_in_process_storage_nodes_and_staging_stores(
            bound_plain_control_plane(socket_path),
            EcShape { k: 1, m: 0 },
            root.join("storage"),
            staging_stores,
        )
    }

    fn publish_staging_pages_for_test(
        staging_stores: &InProcessMetadataTransferStagingStores,
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_ids: &[u32],
    ) {
        for node_id in node_ids.iter().copied() {
            let node = authority.snapshot().node(NodeId::new(node_id)).unwrap();
            let store = staging_stores
                .store(
                    MetadataTransferStagingNodeIdentity::new(
                        NodeId::new(node_id),
                        node.node_incarnation(),
                        node.endpoint().to_owned(),
                    )
                    .unwrap(),
                )
                .unwrap();
            while let Some(page) = store.next_evidence_page().unwrap() {
                let apply_receipt = <SingleAuthorityControlPlane<FileControlPlaneStore> as crate::control_plane::ControlPlaneAdmin>::apply_metadata_transfer_staging_evidence_page(
                    authority,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap();
                let apply_receipt =
                    crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap();
                store
                    .record_evidence_apply_receipt(&page, &apply_receipt)
                    .unwrap();
            }
        }
    }

    fn poll_composed_reconciliation_after_publishing_staging(
        staging_stores: &InProcessMetadataTransferStagingStores,
        worker: &mut UnavailablePgReconciliationWorker,
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        now_ms: u64,
    ) {
        worker.observe_transfer_workers();
        publish_staging_pages_for_test(staging_stores, authority, &[2, 3, 4]);
        if worker.retained_test_state().0 == 0 {
            worker.poll_single_authority(authority, now_ms).unwrap();
        }
        publish_staging_pages_for_test(staging_stores, authority, &[2, 3, 4]);
    }

    const COMPOSED_TRANSFER_TOPOLOGY_DIGEST: &str =
        "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567";

    fn composed_transfer_credential() -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "composed-live-pg-transfer".to_owned(),
            credential_id: "control-plane-transfer-1".to_owned(),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::Admin {
                instance_id: "control-plane-1".to_owned(),
            },
            secret: b"composed-live-pg-transfer-secret".to_vec(),
        })
        .unwrap()
    }

    fn composed_transfer_server_auth(
        credential: &ControlPlaneScopedCredential,
    ) -> crate::StorageRpcServerAuthConfig {
        crate::StorageRpcServerAuthConfig::new(
            credential.cluster_id(),
            ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap(),
            9,
            COMPOSED_TRANSFER_TOPOLOGY_DIGEST,
        )
        .unwrap()
    }

    fn composed_transfer_capability(
        credential: ControlPlaneScopedCredential,
    ) -> LivePgMetadataTransferStorageRpcClientCapability {
        LivePgMetadataTransferStorageRpcClientCapability::new_with_transport_limits(
            credential,
            9,
            COMPOSED_TRANSFER_TOPOLOGY_DIGEST,
            crate::StorageRpcTransportLimits::DEFAULT,
        )
        .unwrap()
    }

    fn composed_transfer_tls_certified_key() -> Arc<rustls::sign::CertifiedKey> {
        let certificates = CertificateDer::pem_slice_iter(include_bytes!(
            "../../s3-tests/testdata/storage-localhost-cert.pem"
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../../s3-tests/testdata/storage-localhost-key.pem"
        ))
        .unwrap();
        Arc::new(
            rustls::sign::CertifiedKey::from_der(
                certificates,
                private_key,
                &tls_provider::build_provider(),
            )
            .unwrap(),
        )
    }

    fn composed_transfer_tls_client_config() -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                CertificateDer::pem_slice_iter(include_bytes!(
                    "../../s3-tests/testdata/storage-ca-cert.pem"
                ))
                .next()
                .unwrap()
                .unwrap(),
            )
            .unwrap();
        crate::storage_rpc_transport::storage_rpc_tls_client_config(Arc::new(roots)).unwrap()
    }

    enum ComposedTransferServerWake {
        Unix(std::path::PathBuf),
        Tls(std::net::SocketAddr, Arc<rustls::ClientConfig>),
    }

    impl ComposedTransferServerWake {
        fn wake(self) {
            match self {
                Self::Unix(path) => {
                    drop(UnixStream::connect(path).unwrap());
                }
                Self::Tls(address, client_config) => {
                    let socket = TcpStream::connect(address).unwrap();
                    let connection = rustls::ClientConnection::new(
                        client_config,
                        ServerName::try_from("localhost").unwrap().to_owned(),
                    )
                    .unwrap();
                    let mut stream = rustls::StreamOwned::new(connection, socket);
                    while stream.conn.is_handshaking() {
                        stream.conn.complete_io(&mut stream.sock).unwrap();
                    }
                }
            }
        }
    }

    fn seed_composed_transfer_source(
        data_dir: &std::path::Path,
        node_id: NodeId,
        pg_id: PgId,
        source_epoch: ClusterEpoch,
    ) -> (PgMetadataProof, MetadataCommandEnvelope) {
        let command =
            composed_transfer_source_command(pg_id, source_epoch, "composed-transfer-bucket");
        let proof = apply_composed_transfer_source_command(
            data_dir,
            node_id,
            pg_id,
            source_epoch,
            &command,
        );
        (proof, command)
    }

    fn composed_transfer_source_command(
        pg_id: PgId,
        source_epoch: ClusterEpoch,
        bucket_name: &str,
    ) -> MetadataCommandEnvelope {
        let owner = OwnerIdentity::from_principal("composed-transfer-owner");
        let grants = AclGrants::default();
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(
                    &CreateBucketConfig {
                        name: bucket_name,
                        owner_principal: &owner.principal,
                        owner_canonical_id: &owner.canonical_id,
                        acl_grants: &grants,
                        public_read: false,
                        public_write: false,
                        versioning: BucketVersioningState::Disabled,
                        object_lock: BucketObjectLockConfig::default(),
                        ownership_controls: BucketOwnershipControls {
                            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                        },
                    },
                    1_234,
                    1,
                )
                .unwrap(),
            ),
        )
    }

    fn apply_composed_transfer_source_command(
        data_dir: &std::path::Path,
        node_id: NodeId,
        pg_id: PgId,
        source_epoch: ClusterEpoch,
        command: &MetadataCommandEnvelope,
    ) -> PgMetadataProof {
        let node = SharedStorageNode::open_with_default_ec_shape_and_epoch(
            data_dir,
            &[pg_id.get()],
            EcShape { k: 1, m: 0 },
            source_epoch,
        )
        .unwrap();
        let state = node
            .get_pg(pg_id.get())
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), command)
            .unwrap();
        PgMetadataProof::current(
            state.applied_log_index,
            state.applied_log_hash,
            state.state_digest,
        )
    }

    fn composed_transfer_imported_proof(
        source: &MetadataCommandEnvelope,
        destination_epoch: ClusterEpoch,
        source_proof: PgMetadataProof,
    ) -> PgMetadataProof {
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                destination_epoch,
                source.id().pg_id(),
                source.id().log_index(),
            ),
            source.payload().clone(),
        );
        PgMetadataProof::current(
            1,
            metadata_command_log_hash(
                destination_epoch,
                rebased.id().pg_id(),
                rebased.id().log_index(),
                0,
                rebased.checksum_crc64(),
            ),
            source_proof.state_digest,
        )
    }

    fn spawn_composed_transfer_control_plane(
        socket_path: &std::path::Path,
        authority: Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
        authority_now_ms: u64,
        stop: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(socket_path).unwrap();
        let listener = ControlPlaneRpcServerListener::unix(
            listener,
            8,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(2),
        )
        .unwrap();
        let policy =
            ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 8, 1024 * 1024)
                .unwrap();
        thread::spawn(move || {
            listener
                .serve_shared_until_stop_for_test(
                    authority,
                    policy,
                    authority_now_ms,
                    stop.as_ref(),
                )
                .unwrap();
        })
    }

    fn authenticated_composed_nonempty_live_transfer(tcp: bool) {
        let tmp = test_util::tempdir();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let pg_id = PgId::new(11);
        let source_node_id = NodeId::new(7);
        let destination_node_id = NodeId::new(8);
        let source_data_dir = tmp.path().join("source-data");
        let destination_data_dir = tmp.path().join("destination-data");
        let source_socket = tmp.path().join("source.sock");
        let destination_socket = tmp.path().join("destination.sock");
        let control_plane_socket = tmp.path().join("control-plane.sock");
        let source_tcp_bind_address = tcp.then(|| "127.0.0.1:0".parse().unwrap());
        let destination_tcp_bind_address = tcp.then(|| "127.0.0.1:0".parse().unwrap());
        let source_advertised_endpoint = source_tcp_bind_address.map_or_else(
            || source_socket.display().to_string(),
            |_| "tcp://source.storage.test:7701".to_owned(),
        );
        let destination_advertised_endpoint = destination_tcp_bind_address.map_or_else(
            || destination_socket.display().to_string(),
            |_| "tcp://destination.storage.test:7701".to_owned(),
        );
        let now_ms = crate::clock::current_time_millis();
        let _time = crate::clock::test_time_override_guard(now_ms);

        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        authority
            .set_node_membership(source_node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(destination_node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(pg_id, vec![source_node_id])
            .unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        let (source_proof, source_command) =
            seed_composed_transfer_source(&source_data_dir, source_node_id, pg_id, source_epoch);
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_advertised_endpoint.clone(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: source_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms,
        );
        submit_heartbeat_until_serving(
            &mut authority,
            destination_node_id,
            destination_advertised_endpoint.clone(),
            10_000,
            Vec::new(),
            now_ms.saturating_add(5),
        );
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_advertised_endpoint.clone(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: source_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms.saturating_add(8),
        );
        authority
            .complete_pg_peering(pg_id, source_node_id, 1, now_ms.saturating_add(10))
            .unwrap();
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_advertised_endpoint.clone(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Active,
                metadata_proof: source_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms.saturating_add(15),
        );

        authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_advertised_endpoint.clone(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: source_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms.saturating_add(18),
        );
        submit_heartbeat_until_serving(
            &mut authority,
            destination_node_id,
            destination_advertised_endpoint.clone(),
            10_000,
            Vec::new(),
            now_ms.saturating_add(19),
        );
        let destination_epoch =
            ClusterEpoch::new(authority.snapshot().cluster_epoch().get() + 1).unwrap();
        let imported_proof =
            composed_transfer_imported_proof(&source_command, destination_epoch, source_proof);
        let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            source_proof,
            imported_proof,
        );
        authority
            .set_pg_acting_set_with_metadata_transfer_at_epoch(
                pg_id,
                vec![destination_node_id],
                transfer,
                destination_epoch,
            )
            .unwrap();
        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            source_advertised_endpoint.clone(),
            10_000,
            Vec::new(),
            now_ms.saturating_add(23),
        );
        submit_heartbeat_until_serving(
            &mut authority,
            destination_node_id,
            destination_advertised_endpoint.clone(),
            10_000,
            Vec::new(),
            now_ms.saturating_add(24),
        );
        let transfer_runtime = authority
            .pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(25))
            .unwrap();

        let source_config = StorageNodeProcessConfig::from_runtime_map(
            source_node_id,
            &source_data_dir,
            EcShape { k: 1, m: 0 },
            &transfer_runtime,
        )
        .unwrap();
        let destination_config = StorageNodeProcessConfig::from_runtime_map(
            destination_node_id,
            &destination_data_dir,
            EcShape { k: 1, m: 0 },
            &transfer_runtime,
        )
        .unwrap();
        let credential = composed_transfer_credential();
        let mut source_prepared = PreparedStorageNodeServer::new(source_config)
            .with_rpc_auth(composed_transfer_server_auth(&credential));
        let mut destination_prepared = PreparedStorageNodeServer::new(destination_config)
            .with_rpc_auth(composed_transfer_server_auth(&credential));
        if tcp {
            source_prepared =
                source_prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp(
                    source_tcp_bind_address.unwrap(),
                    composed_transfer_tls_certified_key(),
                )
                .unwrap()]);
            destination_prepared = destination_prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp(
                    destination_tcp_bind_address.unwrap(),
                    composed_transfer_tls_certified_key(),
                )
                .unwrap(),
            ]);
        }
        let source_server = Arc::new(source_prepared.bind().unwrap());
        let destination_server = Arc::new(destination_prepared.bind().unwrap());
        let tls_client_config = tcp.then(composed_transfer_tls_client_config);
        let source_endpoint = if let Some(client_config) = &tls_client_config {
            let address = source_server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                source_advertised_endpoint,
                vec![address],
                "localhost",
                Arc::clone(client_config),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(source_socket.clone())
        };
        let destination_endpoint = if let Some(client_config) = &tls_client_config {
            let address = destination_server.tcp_listener_addr_for_test();
            StorageRpcClientEndpoint::tcp_with_config(
                destination_advertised_endpoint,
                vec![address],
                "localhost",
                Arc::clone(client_config),
            )
            .unwrap()
        } else {
            StorageRpcClientEndpoint::unix(destination_socket.clone())
        };

        let source_stop = Arc::new(AtomicBool::new(false));
        let source_join = {
            let server = Arc::clone(&source_server);
            let stop = Arc::clone(&source_stop);
            thread::spawn(move || server.serve_until_stop_for_test(stop.as_ref()).unwrap())
        };
        let destination_stop = Arc::new(AtomicBool::new(false));
        let destination_join = {
            let server = Arc::clone(&destination_server);
            let stop = Arc::clone(&destination_stop);
            thread::spawn(move || server.serve_until_stop_for_test(stop.as_ref()).unwrap())
        };
        let control_plane_stop = Arc::new(AtomicBool::new(false));
        let control_plane_join = spawn_composed_transfer_control_plane(
            &control_plane_socket,
            Arc::new(Mutex::new(authority)),
            now_ms.saturating_add(100),
            Arc::clone(&control_plane_stop),
        );

        let admin =
            LivePgMetadataTransferAdmin::with_storage_rpc_endpoints_and_live_pg_metadata_transfer_auth(
                bound_plain_control_plane(&control_plane_socket),
                EcShape { k: 1, m: 0 },
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                [
                    (source_node_id.as_u32(), source_endpoint),
                    (destination_node_id.as_u32(), destination_endpoint),
                ],
                composed_transfer_capability(credential),
            );
        let summary = admin
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
            .unwrap_or_else(|error| panic!("composed transfer failed: {}", error._diagnostic));
        assert!(!summary.already_completed());
        assert_eq!(summary.imported_log_index(), 1);
        assert_eq!(
            summary.imported_log_hash(),
            imported_proof.applied_log_hash.value()
        );
        assert_eq!(
            summary.imported_state_digest(),
            imported_proof.state_digest.value()
        );
        drop(admin);

        source_stop.store(true, Ordering::Release);
        destination_stop.store(true, Ordering::Release);
        if let Some(client_config) = tls_client_config {
            ComposedTransferServerWake::Tls(
                source_server.tcp_listener_addr_for_test(),
                Arc::clone(&client_config),
            )
            .wake();
            ComposedTransferServerWake::Tls(
                destination_server.tcp_listener_addr_for_test(),
                client_config,
            )
            .wake();
        } else {
            ComposedTransferServerWake::Unix(source_socket).wake();
            ComposedTransferServerWake::Unix(destination_socket).wake();
        }
        source_join.join().unwrap();
        destination_join.join().unwrap();
        drop(source_server);
        drop(destination_server);

        control_plane_stop.store(true, Ordering::Release);
        drop(UnixStream::connect(&control_plane_socket).unwrap());
        control_plane_join.join().unwrap();

        let destination_store = crate::pg_store::PgStore::open(
            &destination_data_dir.join(format!("pg-{:04}", pg_id.get())),
            pg_id.get(),
        )
        .unwrap();
        let destination_state = destination_store.metadata_command_replica_state().unwrap();
        assert_eq!(destination_state.applied_log_index, 1);
        assert_eq!(
            destination_state.applied_log_hash,
            imported_proof.applied_log_hash
        );
        assert_eq!(destination_state.state_digest, imported_proof.state_digest);
    }

    #[test]
    fn authenticated_unix_composed_nonempty_live_transfer() {
        authenticated_composed_nonempty_live_transfer(false);
    }

    #[test]
    fn authenticated_tls_composed_nonempty_live_transfer() {
        authenticated_composed_nonempty_live_transfer(true);
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ConcurrentReconciliationFailure {
        None,
        PublicationLag,
        PublicationWaitExpiry,
        MixedPublicationLagAndPreparationRejection,
        AuthorizationObservationLag,
        AuthorizationResponseLossAndFirstStageFailure,
        DefinitiveAuthorizationRejection,
        InstallPreparationRejection,
        InstallResponseLoss,
        FinalizationDeferred,
    }

    #[test]
    fn staging_rpc_preserves_authorization_observation_lag() {
        let failure = staging_rpc_failure(crate::StoreError::StorageRpc {
            node_id: 1,
            operation: "metadata transfer staging intent create",
            failure: crate::storage_rpc::StorageRpcErrorCode::StagingAuthorizationNotObserved,
            detail: crate::error::StorageNodeFailureDetail::new("authorization not observed"),
        });
        assert_eq!(
            failure.disposition,
            LivePgMetadataTransferFailureDisposition::AuthorizationNotObserved
        );
    }

    #[test]
    fn in_process_staging_transport_reuses_one_store_per_actor() {
        let tmp = test_util::tempdir();
        let stores = InProcessMetadataTransferStagingStores::new(tmp.path().to_path_buf());
        let identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            7,
            "unix:///run/argmin/storage-4.sock".to_owned(),
        )
        .unwrap();
        let first = stores.store(identity.clone()).unwrap();
        let second = stores.store(identity).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    fn concurrent_reconciliation_transfers_rebase_and_activate(
        failure: ConcurrentReconciliationFailure,
        pg_count: usize,
    ) {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let pg_ids = (19..19 + u32::try_from(pg_count).unwrap())
            .map(PgId::new)
            .collect::<Vec<_>>();
        let source_acting_set = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let destination_acting_set = vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)];
        let nodes = (1..=4)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    tmp.path()
                        .join(format!("node-{node_id}.sock"))
                        .display()
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        let pgs = pg_ids
            .iter()
            .copied()
            .map(|pg_id| (pg_id, source_acting_set.clone()))
            .collect::<Vec<_>>();
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                7,
                [0x7b; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1, 2, 3],
                &nodes,
                &pgs,
                crate::control_plane::test_certified_storage_placement_policy(
                    (1..=4).map(NodeId::new),
                    3,
                    50,
                ),
            )
            .unwrap();
        let snapshot = crate::control_plane::ClusterControlSnapshot::empty()
            .apply_control_plane_command(
                crate::control_plane_command::ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                    nodes: nodes.clone(),
                    pg_acting_sets: pgs,
                    topology,
                },
            )
            .unwrap()
            .into_snapshot();
        let source_epoch = snapshot.cluster_epoch();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        store.checkpoint(None, &snapshot).unwrap();
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let storage_root = tmp.path().join("storage");
        let staging_stores = Arc::new(InProcessMetadataTransferStagingStores::new(
            storage_root.clone(),
        ));
        let commands = pg_ids
            .iter()
            .map(|pg_id| {
                composed_transfer_source_command(
                    *pg_id,
                    source_epoch,
                    &format!("concurrent-transfer-{}", pg_id.get()),
                )
            })
            .collect::<Vec<_>>();
        let mut proofs = std::collections::BTreeMap::new();
        for generation in 0..8 {
            for node_id in 1..=3 {
                let node = SharedStorageNode::open_with_default_ec_shape_and_epoch(
                    &storage_root
                        .join(format!("cluster-{generation}"))
                        .join(format!("node-{node_id}")),
                    &pg_ids.iter().map(|pg_id| pg_id.get()).collect::<Vec<_>>(),
                    EcShape { k: 2, m: 1 },
                    source_epoch,
                )
                .unwrap();
                for (pg_id, command) in pg_ids.iter().copied().zip(&commands) {
                    let state = node
                        .get_pg(pg_id.get())
                        .unwrap()
                        .apply_metadata_command_and_record(node_id, command)
                        .unwrap();
                    let proof = PgMetadataProof::current(
                        state.applied_log_index,
                        state.applied_log_hash,
                        state.state_digest,
                    );
                    let retained = proofs.entry(pg_id).or_insert(proof);
                    assert_eq!(*retained, proof);
                }
            }
        }

        let now_ms = crate::clock::current_time_millis();
        let observations = |states: Vec<PgState>| {
            pg_ids
                .iter()
                .copied()
                .zip(states)
                .map(|(pg_id, state)| NodePgHeartbeatObservation {
                    pg_id,
                    state,
                    metadata_proof: proofs[&pg_id],
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                })
                .collect::<Vec<_>>()
        };
        for node_id in 1..=4 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                if node_id <= 3 {
                    observations(vec![PgState::Peering; pg_count])
                } else {
                    Vec::new()
                },
                now_ms + u64::from(node_id),
            );
        }
        for node_id in 1..=3 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                observations(vec![PgState::Peering; pg_count]),
                now_ms + 5 + u64::from(node_id),
            );
        }
        let mut states = vec![PgState::Peering; pg_count];
        for (index, pg_id) in pg_ids.iter().copied().enumerate() {
            let peering_at_ms = now_ms + 10 + u64::try_from(index).unwrap() * 10;
            authority
                .complete_pg_peering(pg_id, NodeId::new(1), 1, peering_at_ms)
                .unwrap();
            states[index] = PgState::Active;
            for node_id in 1..=3 {
                submit_heartbeat_until_serving(
                    &mut authority,
                    NodeId::new(node_id),
                    nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                    10_000,
                    observations(states.clone()),
                    peering_at_ms + 1 + u64::from(node_id),
                );
            }
        }
        let active_at_ms = now_ms + 20 + u64::try_from(pg_count - 1).unwrap() * 10;
        for node_id in 1..=3 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                if node_id == 1 { 50 } else { 10_000 },
                observations(states.clone()),
                active_at_ms + u64::from(node_id),
            );
        }
        let failed_deadline_ms = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        authority
            .expire_heartbeat_leases(failed_deadline_ms)
            .unwrap();
        for node_id in [2, 3] {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                500,
                observations(vec![PgState::Peering; pg_count]),
                failed_deadline_ms + u64::from(node_id),
            );
        }
        submit_heartbeat_until_serving(
            &mut authority,
            NodeId::new(4),
            nodes[3].1.clone(),
            10_000,
            Vec::new(),
            failed_deadline_ms + 4,
        );
        for node_id in [2, 3] {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                500,
                observations(vec![PgState::Peering; pg_count]),
                failed_deadline_ms + 10 + u64::from(node_id),
            );
        }
        let begin_at_ms = authority
            .snapshot()
            .unavailable_node_observation(NodeId::new(1))
            .unwrap()
            .observed_at_ms()
            + 50;
        if matches!(
            failure,
            ConcurrentReconciliationFailure::PublicationLag
                | ConcurrentReconciliationFailure::PublicationWaitExpiry
                | ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection
        ) {
            publish_staging_pages_for_test(&staging_stores, &mut authority, &[2, 3, 4]);
        }
        for pg_id in &pg_ids {
            let route = authority.snapshot().pg_route(*pg_id, begin_at_ms).unwrap();
            assert!(
                route.metadata_read_route().is_some(),
                "PG {} fixture has no proof-qualified transfer source",
                pg_id.get()
            );
        }
        let authority = Arc::new(Mutex::new(authority));
        let stop = Arc::new(AtomicBool::new(false));
        let server = spawn_composed_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            begin_at_ms + 100,
            Arc::clone(&stop),
        );
        let _time = crate::clock::test_time_override_guard(begin_at_ms + 1_000);
        let (prepared_tx, prepared_rx) = mpsc::sync_channel(pg_count);
        let (release_tx, release_rx) = mpsc::sync_channel(pg_count);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let prepare_release = Arc::clone(&release_rx);
        let (imported_tx, imported_rx) = mpsc::sync_channel(pg_count * 4);
        let (stage_failure_tx, stage_failure_rx) = mpsc::sync_channel(1);
        let (stage_blocked_tx, stage_blocked_rx) = mpsc::sync_channel(pg_count);
        let (stage_release_tx, stage_release_rx) = mpsc::sync_channel(pg_count);
        let stage_release_rx = Arc::new(Mutex::new(stage_release_rx));
        let mut admin = live_transfer_admin_with_staging_stores(
            tmp.path(),
            &socket_path,
            Arc::clone(&staging_stores),
        )
        .with_clock_override(begin_at_ms + 1_000)
        .with_after_transfer_prepare_hook(move |pg_id, destination_epoch| {
            prepared_tx.send((pg_id, destination_epoch)).unwrap();
            prepare_release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("concurrent transfer preparation was not released");
        })
        .with_after_transfer_import_hook(move |pg_id, destination_epoch| {
            imported_tx
                .try_send((pg_id, destination_epoch))
                .expect("staged import retried beyond the bounded regression budget");
        });
        if matches!(
            failure,
            ConcurrentReconciliationFailure::PublicationWaitExpiry
                | ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection
        ) {
            let first_blocked_pg = pg_ids[2];
            let blocked_once = Arc::new(Mutex::new(BTreeSet::new()));
            admin = admin.with_before_staging_artifact_publish_hook(move |pg_id| {
                let should_block =
                    pg_id >= first_blocked_pg && blocked_once.lock().unwrap().insert(pg_id);
                if should_block {
                    stage_blocked_tx.send(pg_id).unwrap();
                    stage_release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .expect("blocked staging publication was not released");
                }
            });
        }
        if matches!(
            failure,
            ConcurrentReconciliationFailure::AuthorizationObservationLag
                | ConcurrentReconciliationFailure::AuthorizationResponseLossAndFirstStageFailure
        ) {
            admin = admin.with_staging_artifact_publish_failure_notification(
                NodeId::new(2),
                if failure == ConcurrentReconciliationFailure::AuthorizationObservationLag {
                    LivePgMetadataTransferFailureDisposition::AuthorizationNotObserved
                } else {
                    LivePgMetadataTransferFailureDisposition::Retryable
                },
                stage_failure_tx,
            );
        }
        let mut worker = UnavailablePgReconciliationWorker::spawn(admin);
        match failure {
            ConcurrentReconciliationFailure::None => {}
            ConcurrentReconciliationFailure::PublicationLag => {}
            ConcurrentReconciliationFailure::PublicationWaitExpiry => {}
            ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection => {
                worker.reject_next_install_preparation_for_test();
            }
            ConcurrentReconciliationFailure::AuthorizationObservationLag => {}
            ConcurrentReconciliationFailure::AuthorizationResponseLossAndFirstStageFailure => {
                worker.fail_next_authorization_response_for_test();
            }
            ConcurrentReconciliationFailure::DefinitiveAuthorizationRejection => {
                worker.reject_next_authorization_response_for_test();
            }
            ConcurrentReconciliationFailure::InstallPreparationRejection => {
                worker.reject_next_install_preparation_for_test();
            }
            ConcurrentReconciliationFailure::InstallResponseLoss => {
                worker.fail_next_install_response_for_test();
            }
            ConcurrentReconciliationFailure::FinalizationDeferred => {}
        }
        {
            let mut authority = authority.lock().unwrap();
            worker
                .poll_single_authority(&mut authority, begin_at_ms)
                .unwrap();
        }
        let preparation_deadline = Instant::now() + Duration::from_secs(5);
        let mut prepared = Vec::new();
        let mut released_one = false;
        while prepared.len() < pg_count {
            {
                let mut authority = authority.lock().unwrap();
                worker
                    .poll_single_authority(&mut authority, begin_at_ms)
                    .unwrap();
            }
            while let Ok(value) = prepared_rx.try_recv() {
                prepared.push(value);
            }
            if pg_count > 4 && prepared.len() >= 4 && !released_one {
                release_tx.send(()).unwrap();
                released_one = true;
            }
            assert!(
                Instant::now() < preparation_deadline,
                "concurrent transfer did not reach preparation gate: {:?}",
                worker.retained_test_state()
            );
            thread::sleep(Duration::from_millis(1));
        }
        prepared.sort_by_key(|(pg_id, _)| *pg_id);
        assert_eq!(
            prepared.iter().map(|(pg_id, _)| *pg_id).collect::<Vec<_>>(),
            pg_ids
        );
        assert!(prepared.iter().all(|(_, epoch)| *epoch == prepared[0].1));
        for _ in usize::from(released_one)..pg_count {
            release_tx.send(()).unwrap();
        }

        if failure == ConcurrentReconciliationFailure::PublicationLag {
            let wait_deadline = Instant::now() + Duration::from_secs(5);
            while worker.staged_install_depth_for_test() < pg_count {
                {
                    let mut authority = authority.lock().unwrap();
                    worker
                        .poll_single_authority(&mut authority, begin_at_ms + 1)
                        .unwrap();
                }
                assert!(
                    Instant::now() < wait_deadline,
                    "staged owners were lost before publication evidence arrived: {:?}",
                    worker.retained_test_state()
                );
                thread::sleep(Duration::from_millis(1));
            }
            let epoch_before_publication = authority.lock().unwrap().snapshot().cluster_epoch();
            for _ in 0..3 {
                let mut authority = authority.lock().unwrap();
                worker
                    .poll_single_authority(&mut authority, begin_at_ms + 1)
                    .unwrap();
                assert_eq!(
                    authority.snapshot().cluster_epoch(),
                    epoch_before_publication
                );
                assert_eq!(worker.staged_install_depth_for_test(), pg_count);
            }
        }

        if matches!(
            failure,
            ConcurrentReconciliationFailure::PublicationWaitExpiry
                | ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection
        ) {
            let wait_deadline = Instant::now() + Duration::from_secs(5);
            let mut blocked = 0;
            while blocked < 2 || worker.staged_install_depth_for_test() < 2 {
                {
                    let mut authority = authority.lock().unwrap();
                    worker
                        .poll_single_authority(&mut authority, begin_at_ms + 1)
                        .unwrap();
                }
                while stage_blocked_rx.try_recv().is_ok() {
                    blocked += 1;
                }
                assert!(
                    Instant::now() < wait_deadline,
                    "the mixed-result fixture did not stage two ready and two blocked PGs"
                );
                thread::sleep(Duration::from_millis(1));
            }
            {
                let mut authority = authority.lock().unwrap();
                publish_staging_pages_for_test(&staging_stores, &mut authority, &[2, 3, 4]);
            }
            for _ in 0..pg_count - 2 {
                stage_release_tx.send(()).unwrap();
            }
            let epoch_before_install = authority.lock().unwrap().snapshot().cluster_epoch();
            let classified = |worker: &UnavailablePgReconciliationWorker| {
                if failure == ConcurrentReconciliationFailure::PublicationWaitExpiry {
                    worker.staged_install_depth_for_test() == pg_count
                        && worker.install_evidence_wait_is_pending_for_test()
                } else {
                    worker.pg_is_deferred_for_test(pg_ids[0])
                }
            };
            while !classified(&worker) {
                {
                    let mut authority = authority.lock().unwrap();
                    worker
                        .poll_single_authority(&mut authority, begin_at_ms + 1)
                        .unwrap();
                    assert_eq!(authority.snapshot().cluster_epoch(), epoch_before_install);
                }
                assert!(
                    Instant::now() < wait_deadline,
                    "mixed rejection did not reach pre-install classification: {:?}",
                    worker.retained_test_state()
                );
                thread::sleep(Duration::from_millis(1));
            }
            if failure == ConcurrentReconciliationFailure::PublicationWaitExpiry {
                worker.expire_install_evidence_wait_for_test();
                {
                    let mut authority = authority.lock().unwrap();
                    worker
                        .poll_single_authority(&mut authority, begin_at_ms + 1)
                        .unwrap();
                    assert_eq!(
                        authority.snapshot().cluster_epoch(),
                        ClusterEpoch::new(epoch_before_install.get() + 1).unwrap(),
                        "ready members must install after the evidence wait expires"
                    );
                    for pg_id in &pg_ids[..2] {
                        assert!(authority
                            .snapshot()
                            .unavailable_pg_placement_transition(*pg_id)
                            .unwrap()
                            .destination_epoch()
                            .is_some());
                    }
                    for pg_id in &pg_ids[2..] {
                        assert!(authority
                            .snapshot()
                            .unavailable_pg_placement_transition(*pg_id)
                            .unwrap()
                            .destination_epoch()
                            .is_none());
                    }
                }
                for pg_id in &pg_ids[2..] {
                    assert!(worker.pg_is_deferred_for_test(*pg_id));
                    assert!(!worker.foreground_owns_pg_for_test(*pg_id));
                }
            } else {
                assert_eq!(worker.staged_install_depth_for_test(), pg_count - 1);
                assert!(worker.foreground_owns_pg_for_test(pg_ids[1]));
            }
            {
                let mut authority = authority.lock().unwrap();
                publish_staging_pages_for_test(&staging_stores, &mut authority, &[2, 3, 4]);
            }
        }

        if failure == ConcurrentReconciliationFailure::AuthorizationResponseLossAndFirstStageFailure
        {
            let ownership_deadline = Instant::now() + Duration::from_secs(5);
            let mut observed_response_loss = false;
            let mut observed_first_destination_failure = false;
            let mut failed_pg = None;
            let mut failed_pg_released_from_foreground = false;
            let mut observed_committed_authorization = false;
            while !observed_response_loss
                || !observed_first_destination_failure
                || !failed_pg_released_from_foreground
            {
                {
                    let mut authority = authority.lock().unwrap();
                    poll_composed_reconciliation_after_publishing_staging(
                        &staging_stores,
                        &mut worker,
                        &mut authority,
                        begin_at_ms + 1,
                    );
                    observed_committed_authorization |= pg_ids.iter().all(|pg_id| {
                        let snapshot = authority.snapshot();
                        let transition = snapshot
                            .unavailable_pg_placement_transition(*pg_id)
                            .unwrap();
                        let work = UnavailablePgReconciliationWork::from_transition(
                            transition,
                            crate::control_plane::UnavailablePgReconciliationStage::MetadataTransfer,
                        );
                        snapshot
                            .committed_unavailable_pg_staging_request(&work)
                            .is_ok()
                    });
                }
                if let Ok(pg_id) = stage_failure_rx.try_recv() {
                    failed_pg = Some(pg_id);
                    observed_first_destination_failure = true;
                }
                if let Some(failed_pg) = failed_pg {
                    if worker.pg_is_deferred_for_test(failed_pg) {
                        assert!(
                            !worker.foreground_owns_pg_for_test(failed_pg),
                            "retryable first-copy failure retained PG {} in the foreground queue",
                            failed_pg.get()
                        );
                        failed_pg_released_from_foreground = true;
                    }
                }
                if let Some(diagnostic) = worker.retained_test_state().3 {
                    observed_response_loss |=
                        diagnostic.contains("injected staging authorization response loss");
                }
                assert!(
                    Instant::now() < ownership_deadline,
                    "authorized transfer did not rediscover durable state after response loss and first-destination failure: {:?}",
                    worker.retained_test_state()
                );
                thread::sleep(Duration::from_millis(1));
            }
            assert!(observed_response_loss);
            assert!(observed_committed_authorization);
        }

        if failure == ConcurrentReconciliationFailure::InstallPreparationRejection {
            let failed_pg = pg_ids[0];
            let successful_pg = pg_ids[1];
            let fairness_deadline = Instant::now() + Duration::from_secs(5);
            let mut observed_failed_deferral = false;
            loop {
                let successful_imported = {
                    let mut authority = authority.lock().unwrap();
                    poll_composed_reconciliation_after_publishing_staging(
                        &staging_stores,
                        &mut worker,
                        &mut authority,
                        begin_at_ms + 1,
                    );
                    authority
                        .snapshot()
                        .pg(successful_pg)
                        .unwrap()
                        .peering_metadata_transfer()
                        .is_some()
                };
                if worker.pg_is_deferred_for_test(failed_pg) && !observed_failed_deferral {
                    assert!(
                        !worker.foreground_owns_pg_for_test(failed_pg),
                        "rejected install member retained PG {} in a foreground queue",
                        failed_pg.get()
                    );
                    observed_failed_deferral = true;
                }
                if observed_failed_deferral && successful_imported {
                    break;
                }
                assert!(
                    Instant::now() < fairness_deadline,
                    "install-preparation rejection blocked its peer: {:?}",
                    worker.retained_test_state()
                );
                thread::sleep(Duration::from_millis(1));
            }
        }

        let deadline = Instant::now() + Duration::from_secs(10 + u64::try_from(pg_count).unwrap());
        let mut imported = BTreeMap::new();
        let mut import_callback_count = 0_usize;
        let mut observed_definitive_authorization_rejection = false;
        while imported.len() < pg_count {
            {
                let mut authority = authority.lock().unwrap();
                poll_composed_reconciliation_after_publishing_staging(
                    &staging_stores,
                    &mut worker,
                    &mut authority,
                    begin_at_ms + 1,
                );
            }
            while let Ok(value) = imported_rx.try_recv() {
                import_callback_count += 1;
                assert!(
                    import_callback_count <= pg_count * 4,
                    "staged imports retried beyond the bounded regression budget"
                );
                imported.insert(value.0, value.1);
            }
            if let Some(diagnostic) = worker.retained_test_state().3 {
                observed_definitive_authorization_rejection |=
                    diagnostic.contains("injected definitive staging authorization rejection");
            }
            assert!(
                Instant::now() < deadline,
                "staged concurrent transfers did not import before the deadline: {:?}",
                worker.retained_test_state()
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(imported.keys().copied().collect::<Vec<_>>(), pg_ids);
        if matches!(
            failure,
            ConcurrentReconciliationFailure::None
                | ConcurrentReconciliationFailure::PublicationLag
                | ConcurrentReconciliationFailure::AuthorizationObservationLag
                | ConcurrentReconciliationFailure::InstallResponseLoss
        ) {
            assert!(
                imported
                    .values()
                    .all(|epoch| *epoch == imported[&pg_ids[0]]),
                "plural destination installation must assign one shared epoch"
            );
        }
        if failure == ConcurrentReconciliationFailure::DefinitiveAuthorizationRejection {
            assert!(observed_definitive_authorization_rejection);
        }
        if failure == ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection {
            let shared_epoch = imported[&pg_ids[1]];
            assert!(pg_ids[2..]
                .iter()
                .all(|pg_id| imported[pg_id] == shared_epoch));
        }
        if failure == ConcurrentReconciliationFailure::InstallResponseLoss {
            assert!(
                !worker.install_response_failure_is_pending_for_test(),
                "the injected post-commit installation response loss was never consumed"
            );
        }

        let readiness_at_ms = begin_at_ms + 1_100;
        let imported_proofs = {
            let authority = authority.lock().unwrap();
            pg_ids
                .iter()
                .map(|pg_id| {
                    authority
                        .snapshot()
                        .pg(*pg_id)
                        .unwrap()
                        .peering_metadata_transfer()
                        .unwrap()
                        .metadata_proof()
                })
                .collect::<Vec<_>>()
        };
        {
            let mut authority = authority.lock().unwrap();
            for node_id in [2, 3, 4] {
                submit_heartbeat_until_serving(
                    &mut authority,
                    NodeId::new(node_id),
                    nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                    10_000,
                    pg_ids
                        .iter()
                        .copied()
                        .zip(imported_proofs.iter().copied())
                        .map(|(pg_id, metadata_proof)| NodePgHeartbeatObservation {
                            pg_id,
                            state: PgState::Peering,
                            metadata_proof,
                            metadata_log_epoch: ClusterEpoch::INITIAL,
                            pending_metadata_command: None,
                        })
                        .collect(),
                    readiness_at_ms + u64::from(node_id),
                );
            }
        }
        {
            let authority = authority.lock().unwrap();
            let current_epoch = authority.snapshot().cluster_epoch();
            for (pg_id, imported_proof) in pg_ids.iter().copied().zip(&imported_proofs) {
                for node_id in &destination_acting_set {
                    let node = authority.snapshot().node(*node_id).unwrap();
                    assert!(node.lease_deadline_ms().unwrap() > readiness_at_ms + 10);
                    assert!(
                        authority
                            .snapshot()
                            .unavailable_node_observation(*node_id)
                            .is_none(),
                        "destination node {} retained an unavailable observation",
                        node_id.as_u32()
                    );
                    let observation = node.pg_observation(pg_id).unwrap_or_else(|| {
                        panic!(
                            "destination node {} did not report PG {} at epoch {}",
                            node_id.as_u32(),
                            pg_id.get(),
                            current_epoch.get()
                        )
                    });
                    assert_eq!(observation.observed_epoch(), current_epoch);
                    assert_eq!(observation.state(), PgState::Peering);
                    assert_eq!(observation.metadata_proof(), *imported_proof);
                }
            }
        }
        let epoch_before_activation = authority.lock().unwrap().snapshot().cluster_epoch();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut readiness_round = 0_u64;
        loop {
            {
                let mut authority = authority.lock().unwrap();
                if !matches!(
                    failure,
                    ConcurrentReconciliationFailure::None
                        | ConcurrentReconciliationFailure::PublicationLag
                        | ConcurrentReconciliationFailure::AuthorizationObservationLag
                        | ConcurrentReconciliationFailure::InstallResponseLoss
                ) {
                    readiness_round += 1;
                    let observations = pg_ids
                        .iter()
                        .copied()
                        .zip(imported_proofs.iter().copied())
                        .map(|(pg_id, metadata_proof)| NodePgHeartbeatObservation {
                            pg_id,
                            state: authority.snapshot().pg(pg_id).unwrap().state(),
                            metadata_proof,
                            metadata_log_epoch: ClusterEpoch::INITIAL,
                            pending_metadata_command: None,
                        })
                        .collect::<Vec<_>>();
                    for node_id in [2, 3, 4] {
                        submit_heartbeat_until_serving(
                            &mut authority,
                            NodeId::new(node_id),
                            nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                            10_000,
                            observations.clone(),
                            readiness_at_ms + 100 + readiness_round * 10 + u64::from(node_id),
                        );
                    }
                }
                let activation_now_ms = readiness_at_ms + 100 + readiness_round * 10 + 9;
                poll_composed_reconciliation_after_publishing_staging(
                    &staging_stores,
                    &mut worker,
                    &mut authority,
                    activation_now_ms,
                );
                if pg_ids.iter().all(|pg_id| {
                    authority
                        .snapshot()
                        .pg(*pg_id)
                        .is_some_and(|pg| pg.state() == PgState::Active)
                }) {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "concurrent transfers did not reach one activation batch"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let authority_guard = authority.lock().unwrap();
        if matches!(
            failure,
            ConcurrentReconciliationFailure::None
                | ConcurrentReconciliationFailure::PublicationLag
                | ConcurrentReconciliationFailure::AuthorizationObservationLag
                | ConcurrentReconciliationFailure::InstallResponseLoss
        ) {
            assert_eq!(
                authority_guard.snapshot().cluster_epoch(),
                ClusterEpoch::new(epoch_before_activation.get() + 1).unwrap(),
                "both PGs must activate in one global epoch advance"
            );
        } else {
            assert!(authority_guard.snapshot().cluster_epoch() > epoch_before_activation);
        }
        for pg_id in &pg_ids {
            let pg = authority_guard.snapshot().pg(*pg_id).unwrap();
            assert_eq!(pg.state(), PgState::Active);
            assert_eq!(pg.acting_set(), destination_acting_set);
        }
        drop(authority_guard);

        // Activation deliberately drops the original linear owner before any
        // cleanup task is dispatched. A replacement worker must discover the
        // retained completed transitions and reconstruct cleanup authority
        // without requiring the staged artifact bytes.
        drop(worker);
        let mut worker = UnavailablePgReconciliationWorker::spawn(
            live_transfer_admin_with_staging_stores(
                tmp.path(),
                &socket_path,
                Arc::clone(&staging_stores),
            )
            .with_clock_override(readiness_at_ms + 1_000),
        );
        if failure == ConcurrentReconciliationFailure::FinalizationDeferred {
            worker.defer_next_finalization_for_test();
        }

        let cleanup_deadline = Instant::now() + Duration::from_secs(5);
        let mut observed_finalization_fairness = false;
        loop {
            let finalized = {
                let mut authority = authority.lock().unwrap();
                poll_composed_reconciliation_after_publishing_staging(
                    &staging_stores,
                    &mut worker,
                    &mut authority,
                    readiness_at_ms + 11,
                );
                pg_ids.iter().all(|pg_id| {
                    let work = UnavailablePgReconciliationWork::new(
                        *pg_id,
                        authority
                            .snapshot()
                            .latest_retained_unavailable_pg_transition(*pg_id)
                            .unwrap()
                            .transition_epoch(),
                        source_epoch,
                        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
                        destination_acting_set.clone(),
                        crate::control_plane::UnavailablePgReconciliationStage::StagingCleanup,
                    );
                    authority
                        .snapshot()
                        .metadata_transfer_staging_is_finalized(&work)
                })
            };
            if failure == ConcurrentReconciliationFailure::FinalizationDeferred {
                let deferred_pg = pg_ids
                    .iter()
                    .copied()
                    .find(|pg_id| worker.pg_is_deferred_for_test(*pg_id));
                let Some(deferred_pg) = deferred_pg else {
                    if finalized {
                        break;
                    }
                    assert!(
                        Instant::now() < cleanup_deadline,
                        "staged concurrent transfers did not finalize cleanup"
                    );
                    thread::sleep(Duration::from_millis(1));
                    continue;
                };
                assert!(
                    !worker.foreground_owns_pg_for_test(deferred_pg),
                    "deferred finalization retained PG {} in a foreground queue",
                    deferred_pg.get()
                );
                let peer_pg = pg_ids
                    .iter()
                    .copied()
                    .find(|pg_id| *pg_id != deferred_pg)
                    .unwrap();
                let authority = authority.lock().unwrap();
                let peer_work = UnavailablePgReconciliationWork::new(
                    peer_pg,
                    authority
                        .snapshot()
                        .latest_retained_unavailable_pg_transition(peer_pg)
                        .unwrap()
                        .transition_epoch(),
                    source_epoch,
                    vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
                    destination_acting_set.clone(),
                    crate::control_plane::UnavailablePgReconciliationStage::StagingCleanup,
                );
                observed_finalization_fairness |= authority
                    .snapshot()
                    .metadata_transfer_staging_is_finalized(&peer_work);
            }
            if finalized {
                break;
            }
            assert!(
                Instant::now() < cleanup_deadline,
                "staged concurrent transfers did not finalize cleanup"
            );
            thread::sleep(Duration::from_millis(1));
        }
        if failure == ConcurrentReconciliationFailure::FinalizationDeferred {
            assert!(observed_finalization_fairness);
        }
        if failure == ConcurrentReconciliationFailure::AuthorizationObservationLag {
            assert!(
                stage_failure_rx.try_recv().is_ok(),
                "the destination did not exercise authorization observation lag"
            );
        }

        stop.store(true, Ordering::Release);
        drop(UnixStream::connect(&socket_path).unwrap());
        server.join().unwrap();
    }

    #[test]
    fn concurrent_reconciliation_transfers_rebase_and_activate_as_one_batch() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::None,
            2,
        );
    }

    #[test]
    fn reconciliation_accumulates_more_pgs_than_transfer_workers_into_one_install() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::None,
            5,
        );
    }

    #[test]
    fn staging_authorization_lag_keeps_five_pgs_in_one_install() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::AuthorizationObservationLag,
            5,
        );
    }

    #[test]
    fn staging_publication_lag_keeps_five_pgs_in_one_install() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::PublicationLag,
            5,
        );
    }

    #[test]
    fn expired_staging_publication_wait_installs_ready_members_and_defers_missing_members() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::PublicationWaitExpiry,
            5,
        );
    }

    #[test]
    fn mixed_install_rejection_does_not_split_publication_pending_batch() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::MixedPublicationLagAndPreparationRejection,
            5,
        );
    }

    #[test]
    fn reconciliation_recovers_artifact_across_authorization_loss_and_first_stage_failure() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::AuthorizationResponseLossAndFirstStageFailure,
            2,
        );
    }

    #[test]
    fn reconciliation_rederives_after_definitive_authorization_rejection() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::DefinitiveAuthorizationRejection,
            2,
        );
    }

    #[test]
    fn reconciliation_recovers_committed_install_after_response_loss() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::InstallResponseLoss,
            2,
        );
    }

    #[test]
    fn install_preparation_rejection_defers_one_pg_without_blocking_its_peer() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::InstallPreparationRejection,
            2,
        );
    }

    #[test]
    fn retryable_finalization_defers_one_pg_without_blocking_its_peer() {
        concurrent_reconciliation_transfers_rebase_and_activate(
            ConcurrentReconciliationFailure::FinalizationDeferred,
            2,
        );
    }

    #[test]
    fn unavailable_pg_reconciliation_transfers_to_spare_and_activates() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let pg_id = PgId::new(19);
        let nodes = (1..=4)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    tmp.path()
                        .join(format!("node-{node_id}.sock"))
                        .display()
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        let pgs = vec![(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)])];
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                7,
                [0x7a; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1, 2, 3],
                &nodes,
                &pgs,
                crate::control_plane::test_certified_storage_placement_policy(
                    (1..=4).map(NodeId::new),
                    3,
                    50,
                ),
            )
            .unwrap();
        let snapshot = crate::control_plane::ClusterControlSnapshot::empty()
            .apply_control_plane_command(
                crate::control_plane_command::ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                    nodes: nodes.clone(),
                    pg_acting_sets: pgs,
                    topology,
                },
            )
            .unwrap()
            .into_snapshot();
        let source_epoch = snapshot.cluster_epoch();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        store.checkpoint(None, &snapshot).unwrap();
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let source_data_root = tmp.path().join("storage").join("cluster-0");
        let (proof, source_command) = seed_composed_transfer_source(
            &source_data_root.join("node-1"),
            NodeId::new(1),
            pg_id,
            source_epoch,
        );
        for node_id in 2..=3 {
            let node_proof = apply_composed_transfer_source_command(
                &source_data_root.join(format!("node-{node_id}")),
                NodeId::new(node_id),
                pg_id,
                source_epoch,
                &source_command,
            );
            assert_eq!(node_proof, proof);
        }
        let now_ms = crate::clock::current_time_millis();
        for node_id in 1..=4 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                (node_id <= 3)
                    .then_some(NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Peering,
                        metadata_proof: proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    })
                    .into_iter()
                    .collect(),
                now_ms + u64::from(node_id),
            );
        }
        for node_id in 1..=3 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: PgState::Peering,
                    metadata_proof: proof,
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                }],
                now_ms + 10 + u64::from(node_id),
            );
        }
        authority
            .complete_pg_peering(pg_id, NodeId::new(1), 1, now_ms + 20)
            .unwrap();
        for node_id in 1..=3 {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                if node_id == 1 { 50 } else { 10_000 },
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: PgState::Active,
                    metadata_proof: proof,
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                }],
                now_ms + 30 + u64::from(node_id),
            );
        }
        let failed_deadline_ms = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        authority
            .expire_heartbeat_leases(failed_deadline_ms)
            .unwrap();
        for node_id in [2, 3] {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: PgState::Peering,
                    metadata_proof: proof,
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                }],
                failed_deadline_ms + u64::from(node_id),
            );
        }
        submit_heartbeat_until_serving(
            &mut authority,
            NodeId::new(4),
            nodes[3].1.clone(),
            10_000,
            Vec::new(),
            failed_deadline_ms + 4,
        );
        for node_id in [2, 3] {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: PgState::Peering,
                    metadata_proof: proof,
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                }],
                failed_deadline_ms + 10 + u64::from(node_id),
            );
        }
        let begin_at_ms = authority
            .snapshot()
            .unavailable_node_observation(NodeId::new(1))
            .unwrap()
            .observed_at_ms()
            + 50;
        let work = authority
            .poll_unavailable_pg_reconciliation(
                &mut crate::control_plane::UnavailablePgReconciliationCursor::start(),
                begin_at_ms,
            )
            .unwrap()
            .expect("expired acting node must produce reconciliation work");
        assert_eq!(
            work.destination_acting_set(),
            &[NodeId::new(4), NodeId::new(2), NodeId::new(3)]
        );

        let authority = Arc::new(Mutex::new(authority));
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            (1..=16).map(|offset| begin_at_ms + offset).collect(),
        );
        let _time = crate::clock::test_time_override_guard(begin_at_ms + 10);
        let (first_copy_failure_tx, first_copy_failure_rx) = mpsc::sync_channel(1);
        let admin = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        )
        .with_staging_artifact_publish_failure_notification(
            NodeId::new(2),
            LivePgMetadataTransferFailureDisposition::Retryable,
            first_copy_failure_tx,
        );
        let prepared = admin
            .prepare_unavailable_pg_reconciliation_staging(work.clone())
            .unwrap();
        assert_eq!(
            prepared
                .prepared
                .install_member
                .unavailable_transition
                .as_ref(),
            Some(work.mutation_binding())
        );
        let initially_prepared_destination_epoch =
            prepared.prepared.install_member.expected_destination_epoch;
        let initially_prepared_proof = prepared.prepared.install_member.transfer.metadata_proof();
        assert_eq!(
            initially_prepared_proof,
            composed_transfer_imported_proof(
                &source_command,
                initially_prepared_destination_epoch,
                proof
            )
        );
        let authorization = prepared.authorization_request();
        let prepared_intent = prepared.intent.clone();
        let prepared_artifact = prepared.staged_artifact.clone();
        let authorization_epoch = authority.lock().unwrap().snapshot().cluster_epoch();
        let authorized_snapshot = authority
            .lock()
            .unwrap()
            .authorize_unavailable_pg_staging_intents_batch(std::slice::from_ref(&authorization))
            .unwrap();
        drop(prepared);

        let mut mismatched_snapshot = authorized_snapshot.clone();
        mismatched_snapshot.test_rebind_singleton_staging_artifact_target_epoch(
            pg_id,
            ClusterEpoch::new(initially_prepared_destination_epoch.get() + 1).unwrap(),
        );
        let mismatch_root = tmp.path().join("mismatched-target-storage");
        let destination = mismatched_snapshot.node(NodeId::new(4)).unwrap();
        let mismatch_store = MetadataTransferStagingStore::open(
            &mismatch_root.join("staging-node-4"),
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(4),
                destination.node_incarnation(),
                destination.endpoint().to_owned(),
            )
            .unwrap(),
            MetadataTransferStagingLimits::new(
                256,
                METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
                4 * 1024 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap();
        mismatch_store.create_intent(&prepared_intent).unwrap();
        mismatch_store
            .publish_artifact(&prepared_intent, &prepared_artifact)
            .unwrap();
        let mismatch_admin = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            mismatch_root,
        );
        let mismatch_error = match mismatch_admin
            .resume_authorized_unavailable_pg_reconciliation(work.clone(), &mismatched_snapshot)
        {
            Ok(_) => panic!("recovery accepted an artifact encoded for a different target epoch"),
            Err(error) => error,
        };
        assert!(mismatch_error.is_fatal());
        assert!(mismatch_error.retained_diagnostic_contains(
            "staged artifact target epoch does not match its committed authorization"
        ));

        let restarted_before_first_copy =
            LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
                bound_plain_control_plane(&socket_path),
                EcShape { k: 2, m: 1 },
                tmp.path().join("storage"),
            );
        let authorized = restarted_before_first_copy
            .resume_authorized_unavailable_pg_reconciliation(work.clone(), &authorized_snapshot)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to re-export committed authorization before its first copy: {}",
                    error._diagnostic
                )
            });
        assert_eq!(authorized.authorization_request(), authorization);
        assert_eq!(
            authorized.target_epoch,
            initially_prepared_destination_epoch
        );
        assert_eq!(
            authorized.transfer.metadata_proof(),
            initially_prepared_proof
        );
        assert_eq!(
            authority.lock().unwrap().snapshot().cluster_epoch(),
            authorization_epoch,
            "staging authorization must be cluster-map epoch neutral"
        );
        let partial_error = match admin.stage_prepared_unavailable_pg_reconciliation(&authorized) {
            Ok(_) => panic!("blocked second destination unexpectedly staged"),
            Err(error) => error,
        };
        assert!(
            partial_error.retained_diagnostic_contains("injected staging artifact publish failure"),
            "unexpected partial-staging failure: {}",
            partial_error._diagnostic
        );
        first_copy_failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first-copy failure injection was not consumed");
        drop(authorized);
        std::fs::rename(&source_data_root, tmp.path().join("retired-source-cluster")).unwrap();
        let restarted_admin = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        );
        let transient_reader = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        )
        .with_staging_artifact_read_failure(
            NodeId::new(4),
            LivePgMetadataTransferFailureDisposition::Retryable,
        );
        let transient_error = match transient_reader
            .resume_authorized_unavailable_pg_reconciliation(work.clone(), &authorized_snapshot)
        {
            Ok(_) => panic!("absent peers discarded the sole artifact holder's timeout"),
            Err(error) => error,
        };
        assert!(
            !transient_error.is_fatal(),
            "transient holder read became fatal: {}",
            transient_error._diagnostic
        );
        assert!(
            transient_error.retained_diagnostic_contains("injected staging artifact read failure")
        );
        let recovered_authorized = restarted_admin
            .resume_authorized_unavailable_pg_reconciliation(work.clone(), &authorized_snapshot)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to recover partially staged authorization: {}",
                    error._diagnostic
                )
            });
        assert_eq!(recovered_authorized.authorization_request(), authorization);
        assert_eq!(
            recovered_authorized.target_epoch,
            initially_prepared_destination_epoch
        );
        assert_eq!(
            recovered_authorized.transfer.metadata_proof(),
            initially_prepared_proof
        );
        let admin = restarted_admin;
        let authorized = recovered_authorized;
        let published = admin
            .stage_prepared_unavailable_pg_reconciliation(&authorized)
            .unwrap_or_else(|error| {
                panic!(
                    "staging prepared unavailable PG transfer failed: {}",
                    error._diagnostic
                )
            });
        let fatal_reader = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        )
        .with_staging_artifact_read_failure(
            NodeId::new(4),
            LivePgMetadataTransferFailureDisposition::Fatal,
        );
        let fatal_error = match fatal_reader
            .resume_authorized_unavailable_pg_reconciliation(work.clone(), &authorized_snapshot)
        {
            Ok(_) => panic!("a later valid artifact discarded earlier fatal read evidence"),
            Err(error) => error,
        };
        assert!(fatal_error.is_fatal());
        assert!(fatal_error.retained_diagnostic_contains("injected staging artifact read failure"));
        let mut staged = authorized.bind_publications(published).unwrap();
        assert_eq!(staged.target_epoch(), initially_prepared_destination_epoch);
        assert_eq!(staged.install_request().publications.len(), 3);

        let publish_staging_pages = |authority: &mut SingleAuthorityControlPlane<
            FileControlPlaneStore,
        >| {
            for node_id in [4, 2, 3] {
                let node = authority.snapshot().node(NodeId::new(node_id)).unwrap();
                let store = MetadataTransferStagingStore::open(
                    &tmp.path()
                        .join("storage")
                        .join(format!("staging-node-{node_id}")),
                    MetadataTransferStagingNodeIdentity::new(
                        NodeId::new(node_id),
                        node.node_incarnation(),
                        node.endpoint().to_owned(),
                    )
                    .unwrap(),
                    MetadataTransferStagingLimits::new(
                        256,
                        METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
                        4 * 1024 * 1024 * 1024,
                    )
                    .unwrap(),
                )
                .unwrap();
                let page = store.next_evidence_page().unwrap().unwrap();
                let apply_receipt = <SingleAuthorityControlPlane<FileControlPlaneStore> as crate::control_plane::ControlPlaneAdmin>::apply_metadata_transfer_staging_evidence_page(
                    authority,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap();
                let apply_receipt =
                    crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap();
                store
                    .record_evidence_apply_receipt(&page, &apply_receipt)
                    .unwrap();
            }
        };
        publish_staging_pages(&mut authority.lock().unwrap());

        authority
            .lock()
            .unwrap()
            .set_pg_acting_set(
                PgId::new(pg_id.get() + 100),
                vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
            )
            .unwrap();
        let rebased_destination_epoch =
            ClusterEpoch::new(authority.lock().unwrap().snapshot().cluster_epoch().get() + 1)
                .unwrap();
        admin
            .rebase_staged_unavailable_pg_reconciliation(&mut staged, rebased_destination_epoch)
            .unwrap();
        publish_staging_pages(&mut authority.lock().unwrap());
        let install = staged.install_request();
        authority
            .lock()
            .unwrap()
            .install_unavailable_pg_placement_transitions_batch(
                std::slice::from_ref(&install),
                rebased_destination_epoch,
            )
            .unwrap();
        let installed_snapshot = authority.lock().unwrap().snapshot().clone();
        let restarted_admin = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        );
        let mismatched_work = UnavailablePgReconciliationWork::new(
            work.pg_id(),
            work.transition_epoch(),
            work.source_epoch(),
            vec![NodeId::new(1), NodeId::new(3), NodeId::new(2)],
            work.destination_acting_set().to_vec(),
            crate::control_plane::UnavailablePgReconciliationStage::MetadataTransfer,
        );
        let mismatch = match restarted_admin
            .resume_installed_unavailable_pg_reconciliation(mismatched_work, &installed_snapshot)
        {
            Ok(_) => panic!("mismatched work unexpectedly recovered staged ownership"),
            Err(error) => error,
        };
        assert!(mismatch.is_fatal());
        assert!(mismatch.retained_diagnostic_contains(
            "staged transfer recovery does not match its unavailable transition"
        ));
        let recovered_staged = restarted_admin
            .resume_installed_unavailable_pg_reconciliation(work.clone(), &installed_snapshot)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to reconstruct installed staged transfer: {}",
                    error._diagnostic
                )
            });
        assert_eq!(recovered_staged.install_request(), staged.install_request());
        let mut staged = recovered_staged;
        let admin = restarted_admin;
        assert!(staged.target_epoch() > initially_prepared_destination_epoch);
        assert_ne!(staged.transfer.metadata_proof(), initially_prepared_proof);
        assert_eq!(
            staged.transfer.metadata_proof(),
            composed_transfer_imported_proof(&source_command, staged.target_epoch(), proof)
        );
        let summary = admin
            .import_staged_unavailable_pg_reconciliation(&staged)
            .unwrap_or_else(|error| panic!("resumed import failed: {}", error._diagnostic));
        server.join().unwrap();

        let staging_store = |snapshot: &crate::control_plane::ClusterControlSnapshot,
                             node_id: u32| {
            let node = snapshot.node(NodeId::new(node_id)).unwrap();
            MetadataTransferStagingStore::open(
                &tmp.path()
                    .join("storage")
                    .join(format!("staging-node-{node_id}")),
                MetadataTransferStagingNodeIdentity::new(
                    NodeId::new(node_id),
                    node.node_incarnation(),
                    node.endpoint().to_owned(),
                )
                .unwrap(),
                MetadataTransferStagingLimits::new(
                    256,
                    METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
                    4 * 1024 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let incomplete_snapshot = authority.lock().unwrap().snapshot().clone();
        let incomplete_cleanup = match admin
            .tombstone_staged_unavailable_pg_reconciliation(&staged, &incomplete_snapshot)
        {
            Ok(_) => panic!("uncompleted transition unexpectedly authorized staging cleanup"),
            Err(error) => error,
        };
        assert_eq!(
            incomplete_cleanup.stage,
            LivePgMetadataTransferStage::Cleanup
        );
        for node_id in [4, 2, 3] {
            assert!(staging_store(&incomplete_snapshot, node_id)
                .read_artifact(&staged.intent)
                .is_ok());
        }

        let imported_proof = PgMetadataProof::current(
            summary.imported_log_index(),
            summary.imported_log_hash(),
            summary.imported_state_digest(),
        );
        let mut authority = authority.lock().unwrap();
        for (offset, node_id) in [4, 2, 3].into_iter().enumerate() {
            submit_heartbeat_until_serving(
                &mut authority,
                NodeId::new(node_id),
                nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                10_000,
                vec![NodePgHeartbeatObservation {
                    pg_id,
                    state: PgState::Peering,
                    metadata_proof: imported_proof,
                    metadata_log_epoch: ClusterEpoch::INITIAL,
                    pending_metadata_command: None,
                }],
                begin_at_ms + 20 + u64::try_from(offset).unwrap(),
            );
        }
        let complete_at_ms = failed_deadline_ms
            .saturating_add(crate::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS)
            .saturating_add(1);
        assert!(authority
            .complete_unavailable_pg_reconciliation(&work, complete_at_ms)
            .unwrap());
        let pg = authority.snapshot().pg(pg_id).unwrap();
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(
            pg.acting_set(),
            &[NodeId::new(4), NodeId::new(2), NodeId::new(3)]
        );
        let completed_snapshot_before_rollover = authority.snapshot().clone();

        let original_target_epoch = staged.target_epoch;
        staged.target_epoch = ClusterEpoch::new(original_target_epoch.get() + 1).unwrap();
        assert!(admin
            .tombstone_staged_unavailable_pg_reconciliation(
                &staged,
                &completed_snapshot_before_rollover,
            )
            .is_err());
        staged.target_epoch = original_target_epoch;

        let original_transfer = staged.transfer;
        staged.transfer = PgMetadataTransferProof::new(
            original_transfer.source_epoch(),
            PgMetadataProof::empty(),
        );
        assert_ne!(staged.transfer, original_transfer);
        assert!(admin
            .tombstone_staged_unavailable_pg_reconciliation(
                &staged,
                &completed_snapshot_before_rollover,
            )
            .is_err());
        staged.transfer = original_transfer;

        staged.publications[0].evidence_digest[0] ^= 0x80;
        assert!(admin
            .tombstone_staged_unavailable_pg_reconciliation(
                &staged,
                &completed_snapshot_before_rollover,
            )
            .is_err());
        staged.publications[0].evidence_digest[0] ^= 0x80;

        let install = staged.install_request();
        assert!(completed_snapshot_before_rollover
            .validate_unavailable_pg_staging_cleanup(
                staged.work().mutation_binding(),
                MetadataTransferStagingCleanupDisposition::Completed,
                Some(&install),
                staged.intent.staging_generation().checked_add(1).unwrap(),
            )
            .is_err());
        for node_id in [4, 2, 3] {
            assert!(staging_store(&completed_snapshot_before_rollover, node_id)
                .read_artifact(&staged.intent)
                .is_ok());
        }

        let rolled_endpoint = tmp
            .path()
            .join("storage-node-4-restarted.sock")
            .to_string_lossy()
            .into_owned();
        submit_heartbeat_with_incarnation_until_serving(
            &mut authority,
            NodeId::new(4),
            2,
            rolled_endpoint.clone(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Active,
                metadata_proof: imported_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            complete_at_ms + 1,
        );
        let completed_snapshot = authority.snapshot().clone();
        assert_eq!(
            completed_snapshot
                .node(NodeId::new(4))
                .unwrap()
                .node_incarnation(),
            2
        );
        assert_eq!(
            completed_snapshot.node(NodeId::new(4)).unwrap().endpoint(),
            rolled_endpoint
        );
        drop(authority);

        let blocked_cleanup = tmp.path().join("storage").join("staging-node-2");
        let retained_cleanup = tmp.path().join("storage").join("staging-node-2-retained");
        std::fs::rename(&blocked_cleanup, &retained_cleanup).unwrap();
        std::fs::write(&blocked_cleanup, b"not a staging directory").unwrap();
        let partial_cleanup = match admin
            .tombstone_staged_unavailable_pg_reconciliation(&staged, &completed_snapshot)
        {
            Ok(_) => panic!("blocked second destination unexpectedly completed cleanup"),
            Err(error) => error,
        };
        assert_eq!(partial_cleanup.stage, LivePgMetadataTransferStage::Cleanup);
        assert!(
            partial_cleanup.retained_diagnostic_contains("remove staged artifact"),
            "unexpected partial-cleanup failure: {}",
            partial_cleanup._diagnostic
        );
        std::fs::remove_file(&blocked_cleanup).unwrap();
        std::fs::rename(&retained_cleanup, &blocked_cleanup).unwrap();

        assert!(matches!(
            staging_store(&completed_snapshot, 4).read_artifact(&staged.intent),
            Err(crate::pg_store::MetadataTransferStagingError::GenerationRetired)
        ));
        assert!(matches!(
            staging_store(&completed_snapshot, 2).read_artifact(&staged.intent),
            Err(crate::pg_store::MetadataTransferStagingError::GenerationRetired)
        ));
        assert!(staging_store(&completed_snapshot, 3)
            .read_artifact(&staged.intent)
            .is_ok());

        let tombstoned = admin
            .tombstone_staged_unavailable_pg_reconciliation(&staged, &completed_snapshot)
            .unwrap_or_else(|error| {
                panic!(
                    "retrying all-destination staging cleanup failed: {}",
                    error._diagnostic
                )
            });
        let cleanup = tombstoned.cleanup_request();
        assert_eq!(&cleanup.unavailable_transition, work.mutation_binding());
        assert_eq!(cleanup.staging_generation, work.transition_epoch().get());
        assert_eq!(
            cleanup
                .tombstones
                .iter()
                .map(|binding| binding.node_id)
                .collect::<Vec<_>>(),
            vec![NodeId::new(2), NodeId::new(3), NodeId::new(4)]
        );
        let rolled_tombstone = cleanup
            .tombstones
            .iter()
            .find(|binding| binding.node_id == NodeId::new(4))
            .unwrap();
        assert_eq!(rolled_tombstone.node_incarnation, 2);
        assert_eq!(rolled_tombstone.endpoint, rolled_endpoint);
        for tombstone in &cleanup.tombstones {
            let store = staging_store(&completed_snapshot, tombstone.node_id.as_u32());
            assert!(matches!(
                store.read_artifact(&staged.intent),
                Err(crate::pg_store::MetadataTransferStagingError::GenerationRetired)
            ));
            let page = store.next_evidence_page().unwrap().unwrap();
            assert!(page.entries().iter().any(|entry| {
                let evidence = decode_staging_evidence(entry.evidence()).unwrap();
                evidence.kind() == crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone
                    && evidence.intent() == &staged.intent
                    && checksum::sha256::digest(evidence.as_bytes()) == tombstone.evidence_digest
            }));
        }
    }

    fn frontend_storage_rpc_capability() -> FrontendStorageRpcClientCapability {
        let credential = crate::control_plane_auth::ControlPlaneScopedCredential::new(
            ControlPlaneScopedCredentialInput {
                cluster_id: "live-transfer-cluster".to_owned(),
                credential_id: "frontend-key".to_owned(),
                credential_version: 1,
                principal: ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-1".to_owned(),
                },
                secret: b"live-transfer-secret".to_vec(),
            },
        )
        .unwrap();
        FrontendStorageRpcClientCapability::new(credential, 1, "a".repeat(64)).unwrap()
    }

    #[test]
    fn source_lease_wait_duration_is_bounded_and_expires_exactly() {
        assert_eq!(
            source_lease_wait_duration(1_000, 1_250),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            source_lease_wait_duration(1_200, 1_250),
            Some(Duration::from_millis(50))
        );
        assert_eq!(source_lease_wait_duration(1_250, 1_250), None);
        assert_eq!(source_lease_wait_duration(1_251, 1_250), None);
    }

    #[test]
    fn transfer_route_stability_ignores_new_certified_peering_read_route() {
        let tmp = test_util::tempdir();
        let (authority, now_ms, pg_id, source_node_id, _) =
            prepared_live_transfer_authority(tmp.path(), false);
        let mut authority = authority.lock().unwrap();
        authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
        let source_proof = authority
            .snapshot()
            .pg(pg_id)
            .and_then(crate::control_plane::PgControlRecord::peering_metadata_proof_floor)
            .unwrap();

        let before = authority
            .pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(11))
            .unwrap();
        let before_route = before
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .unwrap()
            .clone();
        assert_eq!(before_route.metadata_read_route(), None);

        submit_heartbeat_until_serving(
            &mut authority,
            source_node_id,
            tmp.path().join("source.sock").display().to_string(),
            10_000,
            vec![NodePgHeartbeatObservation {
                pg_id,
                state: PgState::Peering,
                metadata_proof: source_proof,
                metadata_log_epoch: ClusterEpoch::INITIAL,
                pending_metadata_command: None,
            }],
            now_ms.saturating_add(20),
        );
        let after = authority
            .serving_pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(30))
            .unwrap();
        let after_route = after
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .unwrap();
        assert!(after_route.metadata_read_route().is_some());
        assert!(transfer_route_matches(&before_route, after_route));
    }

    #[test]
    fn uncommitted_export_refresh_rebases_an_identical_fence_to_the_latest_epoch() {
        let tmp = test_util::tempdir();
        let (authority, now_ms, pg_id, _, _) = prepared_live_transfer_authority(tmp.path(), false);
        let mut authority = authority.lock().unwrap();
        authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
        let fenced = authority
            .serving_pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(11))
            .unwrap();
        let fenced_route = fenced.pg_routes().first().unwrap().clone();
        assert!(fenced_route.peering_metadata_transfer().is_none());

        authority
            .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
            .unwrap();
        let refreshed = authority
            .serving_pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(12))
            .unwrap();
        let refreshed_route = refreshed.pg_routes().first().unwrap();
        assert!(refreshed.cluster_epoch() > fenced.cluster_epoch());
        assert!(transfer_route_matches(&fenced_route, refreshed_route));
        let export_epoch =
            refreshed_export_source_epoch(&refreshed, refreshed_route, fenced.cluster_epoch());
        assert_eq!(export_epoch, refreshed.cluster_epoch());
        let export_runtime = refreshed
            .metadata_transfer_source_runtime_map(
                refreshed_route,
                export_epoch,
                refreshed_route.primary_node_id(),
            )
            .unwrap();
        assert_eq!(export_runtime.cluster_epoch(), refreshed.cluster_epoch());
        assert_eq!(export_runtime.pg_routes()[0].state(), PgState::Peering);
    }

    #[test]
    fn committed_export_refresh_remains_pinned_to_the_marker_source_epoch() {
        let tmp = test_util::tempdir();
        let (authority, now_ms, pg_id, _, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), false);
        let mut authority = authority.lock().unwrap();
        authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        let source_proof = authority
            .snapshot()
            .pg(pg_id)
            .and_then(crate::control_plane::PgControlRecord::peering_metadata_proof_floor)
            .unwrap();
        let destination_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
        let transfer = PgMetadataTransferProof::new(source_epoch, source_proof);
        authority
            .set_pg_acting_set_with_metadata_transfer(pg_id, vec![destination_node_id], transfer)
            .unwrap();
        authority
            .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
            .unwrap();

        let refreshed = authority
            .pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(12))
            .unwrap();
        let refreshed_route = refreshed.pg_routes().first().unwrap();
        assert!(refreshed.cluster_epoch() > destination_epoch);
        assert_eq!(refreshed_route.peering_metadata_transfer(), Some(transfer));
        assert_eq!(
            refreshed_export_source_epoch(&refreshed, refreshed_route, source_epoch),
            source_epoch
        );
    }

    #[test]
    fn live_admin_error_keeps_diagnostic_owner_local() {
        let secret = "route-proof-secret";
        let error = LivePgMetadataTransferError::new(format!("failed with {secret}"));

        assert!(error.retained_diagnostic_contains(secret));
        assert_eq!(
            error.to_string(),
            "live PG metadata transfer failed during configuration"
        );
        assert_eq!(
            format!("{error:?}"),
            "LivePgMetadataTransferError { stage: Configuration, disposition: Fatal, diagnostic: \"<redacted>\" }"
        );
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }

    #[test]
    fn live_transfer_failure_disposition_preserves_fatal_and_retryable_evidence() {
        let fatal = control_plane_transfer_failure(
            "test fatal Raft operation",
            ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                message: "fatal state-machine failure".to_owned(),
            },
        );
        assert_eq!(
            fatal.disposition,
            LivePgMetadataTransferFailureDisposition::Fatal
        );
        let retryable = control_plane_transfer_failure(
            "test leader transition",
            ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                message: "leadership changed".to_owned(),
            },
        );
        assert_eq!(
            retryable.disposition,
            LivePgMetadataTransferFailureDisposition::Retryable
        );

        for clock_wait in [
            ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term: Some(7),
                current_term: 8,
            },
            ControlPlaneError::AuthorityClockSampleWindowTooWide {
                narrowest_window_ms: 3,
                max_window_ms: 2,
            },
            ControlPlaneError::AuthorityClockSourceUnavailable,
            ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: Some(
                    crate::control_plane::ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged,
                ),
            },
            ControlPlaneError::CommittedTimestampRegression {
                timestamp_ms: 9,
                max_committed_timestamp_ms: 10,
            },
            ControlPlaneError::CommittedTimestampTooFarAhead {
                timestamp_ms: 12,
                max_committed_timestamp_ms: 10,
                max_forward_jump_ms: 1,
            },
            ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
                authority_now_ms: 10,
                fenced_until_ms: 11,
            },
        ] {
            let clock_transition =
                control_plane_transfer_failure("test authority clock wait", clock_wait);
            assert_eq!(
                clock_transition.disposition,
                LivePgMetadataTransferFailureDisposition::Retryable
            );
        }

        for kind in [
            std::io::ErrorKind::AddrNotAvailable,
            std::io::ErrorKind::HostUnreachable,
            std::io::ErrorKind::NetworkUnreachable,
            std::io::ErrorKind::NetworkDown,
        ] {
            let network_transition = control_plane_transfer_failure(
                "test control-plane network transition",
                ControlPlaneError::io("connect control-plane RPC", std::io::Error::from(kind)),
            );
            assert_eq!(
                network_transition.disposition,
                LivePgMetadataTransferFailureDisposition::Retryable,
                "network reachability failure {kind:?} must defer reconciliation"
            );
        }

        for class in [
            crate::StoreOperationFailureClass::ResourceExhausted,
            crate::StoreOperationFailureClass::MetadataCommandContention,
            crate::StoreOperationFailureClass::RetryableConvergence,
        ] {
            let failure = metadata_transfer_failure(
                "test transient storage operation",
                crate::error::PgMetadataTransferError::Store(
                    crate::test_support::store_error_for_operation_failure_class(class),
                ),
            );
            assert_eq!(
                failure.disposition,
                LivePgMetadataTransferFailureDisposition::Retryable
            );
        }
        let fatal = metadata_transfer_failure(
            "test fatal storage operation",
            crate::error::PgMetadataTransferError::Store(
                crate::test_support::store_error_for_operation_failure_class(
                    crate::StoreOperationFailureClass::Other,
                ),
            ),
        );
        assert_eq!(
            fatal.disposition,
            LivePgMetadataTransferFailureDisposition::Fatal
        );
    }

    #[test]
    fn configured_transfer_endpoints_include_routable_spares_not_unknown_nodes() {
        let tmp = test_util::tempdir();
        let (authority, now_ms, pg_id, _, _) = prepared_live_transfer_authority(tmp.path(), false);
        let runtime_map = authority
            .lock()
            .unwrap()
            .serving_pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(11))
            .unwrap();
        assert_eq!(runtime_map.nodes().len(), 2);
        let endpoint_for = |node_id| {
            let node = runtime_map
                .nodes()
                .iter()
                .find(|node| node.node_id() == NodeId::new(node_id))
                .unwrap();
            (node_id, StorageRpcClientEndpoint::unix(node.endpoint()))
        };
        let routed_endpoint = endpoint_for(7);
        let destination_endpoint = endpoint_for(8);
        let spare_endpoint = (
            99,
            StorageRpcClientEndpoint::unix(tmp.path().join("spare.sock")),
        );
        let control_plane = bound_plain_control_plane(&tmp.path().join("unused-control.sock"));

        let admin = LivePgMetadataTransferAdmin::with_storage_rpc_endpoints(
            control_plane,
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [
                routed_endpoint,
                destination_endpoint.clone(),
                spare_endpoint.clone(),
            ],
            frontend_storage_rpc_capability(),
        );
        admin
            .build_cluster(&runtime_map)
            .unwrap_or_else(|error| panic!("spare endpoint must be ignored: {error}"));

        let missing_actor_admin = LivePgMetadataTransferAdmin::with_storage_rpc_endpoints(
            bound_plain_control_plane(&tmp.path().join("unused-control-2.sock")),
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [destination_endpoint, spare_endpoint],
            frontend_storage_rpc_capability(),
        );
        let error = match missing_actor_admin.build_cluster(&runtime_map) {
            Ok(_) => panic!("missing routed endpoint must be rejected"),
            Err(error) => error,
        };
        assert!(
            error.contains("node 7 has no installed remote storage-node client"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn live_admin_rejects_crossed_authenticated_cluster_identities() {
        let credential = |cluster_id: &str, principal| {
            ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
                cluster_id: cluster_id.to_owned(),
                credential_id: format!("{cluster_id}-credential"),
                credential_version: 1,
                principal,
                secret: b"live-transfer-test-secret".to_vec(),
            })
            .unwrap()
        };
        let error = match LivePgMetadataTransferControlPlaneClient::new(
            UnixControlPlaneClient::new("authority.sock"),
            Some(credential(
                "cluster-a",
                ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-a".to_owned(),
                },
            )),
            Some(credential(
                "cluster-b",
                ControlPlaneAuthPrincipal::Admin {
                    instance_id: "admin-b".to_owned(),
                },
            )),
        ) {
            Err(error) => error,
            Ok(_) => panic!("crossed authenticated cluster identities must be rejected"),
        };

        assert!(error.retained_diagnostic_contains("different control-plane cluster identities"));
        assert_eq!(
            error.to_string(),
            "live PG metadata transfer failed during configuration"
        );
    }

    #[test]
    fn live_admin_rejects_mixed_authentication_modes() {
        let credential = ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "cluster-a".to_owned(),
            credential_id: "frontend-a".to_owned(),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-a".to_owned(),
            },
            secret: b"live-transfer-test-secret".to_vec(),
        })
        .unwrap();
        let error = match LivePgMetadataTransferControlPlaneClient::new(
            UnixControlPlaneClient::new("authority.sock"),
            Some(credential),
            None,
        ) {
            Err(error) => error,
            Ok(_) => panic!("mixed control-plane authentication modes must be rejected"),
        };

        assert!(error.retained_diagnostic_contains("different authentication modes"));
        assert_eq!(
            error.to_string(),
            "live PG metadata transfer failed during configuration"
        );
    }

    #[test]
    fn live_admin_recognizes_completed_transfer_without_exposing_route_state() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let listener = ControlPlaneRpcServerListener::unix(
            listener,
            4,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(1),
        )
        .unwrap();
        let policy =
            ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 4, 1024 * 1024)
                .unwrap();
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        let node_id = NodeId::new(7);
        let pg_id = PgId::new(11);
        let now_ms = crate::clock::current_time_millis();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        for offset_ms in 0..4 {
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: tmp.path().join("node.sock").display().to_string(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 10_000,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id,
                            state: PgState::Peering,
                            metadata_proof: PgMetadataProof::empty(),
                            metadata_log_epoch: ClusterEpoch::INITIAL,
                            pending_metadata_command: None,
                        }],
                    },
                    now_ms.saturating_add(offset_ms),
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(offset_ms < 3, "authority did not grant a serving lease");
        }
        authority
            .complete_pg_peering(pg_id, node_id, 1, now_ms.saturating_add(4))
            .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint: tmp.path().join("node.sock").display().to_string(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 10_000,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                },
                now_ms.saturating_add(5),
            )
            .unwrap();
        let expected_epoch = authority.snapshot().cluster_epoch().get();
        let authority = Arc::new(Mutex::new(authority));
        let server = thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(
                    authority,
                    policy,
                    [now_ms.saturating_add(6)],
                    |_| {},
                )
                .unwrap();
        });

        let admin = LivePgMetadataTransferAdmin::with_unix_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            None,
        );
        let summary = admin.transfer(pg_id.get(), vec![node_id.as_u32()]).unwrap();

        assert!(summary.already_completed());
        assert_eq!(summary.source_node_id(), node_id.as_u32());
        assert_eq!(summary.source_epoch(), expected_epoch);
        assert_eq!(summary.destination_epoch(), expected_epoch);
        assert_eq!(summary.imported_log_index(), 0);
        assert_eq!(summary.imported_log_hash(), 0);
        assert_eq!(summary.imported_state_digest(), 0);
        server.join().unwrap();
    }

    #[test]
    fn live_admin_owns_fence_and_source_observation_before_storage_io() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let listener = ControlPlaneRpcServerListener::unix(
            listener,
            4,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            Duration::from_secs(1),
        )
        .unwrap();
        let policy =
            ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 4, 1024 * 1024)
                .unwrap();
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.path().join("control-plane.state"),
        ))
        .unwrap();
        let node_id = NodeId::new(7);
        let destination_node_id = NodeId::new(8);
        let pg_id = PgId::new(11);
        let now_ms = crate::clock::current_time_millis();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(destination_node_id, NodeMembershipState::Active)
            .unwrap();
        authority.set_pg_acting_set(pg_id, vec![node_id]).unwrap();
        for offset_ms in 0..4 {
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: tmp.path().join("node.sock").display().to_string(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 50,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id,
                            state: PgState::Peering,
                            metadata_proof: PgMetadataProof::empty(),
                            metadata_log_epoch: ClusterEpoch::INITIAL,
                            pending_metadata_command: None,
                        }],
                    },
                    now_ms.saturating_add(offset_ms),
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(offset_ms < 3, "authority did not grant a serving lease");
        }
        authority
            .complete_pg_peering(pg_id, node_id, 1, now_ms.saturating_add(4))
            .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint: tmp.path().join("node.sock").display().to_string(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 50,
                    cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                        .unwrap(),
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                },
                now_ms.saturating_add(5),
            )
            .unwrap();
        let authority = Arc::new(Mutex::new(authority));
        let server_authority = Arc::clone(&authority);
        let server = thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(
                    server_authority,
                    policy,
                    [
                        now_ms.saturating_add(6),
                        now_ms.saturating_add(7),
                        now_ms.saturating_add(60),
                        now_ms.saturating_add(61),
                    ],
                    |_| {},
                )
                .unwrap();
        });

        let admin = LivePgMetadataTransferAdmin::with_unix_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            None,
        )
        .with_failpoint(Some(LivePgMetadataTransferFailpoint::AfterFence));
        let error = match admin.transfer(pg_id.get(), vec![destination_node_id.as_u32()]) {
            Err(error) => error,
            Ok(_) => panic!("post-fence failpoint must interrupt the transfer"),
        };

        assert!(error.retained_diagnostic_contains(
            "injected metadata transfer live failure at after-fence"
        ));
        assert_eq!(
            error.to_string(),
            "live PG metadata transfer failed during fencing"
        );
        let route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(route.state(), PgState::Peering);
        assert!(route.metadata_transfer_fenced());
        server.join().unwrap();
    }

    #[test]
    fn live_admin_completes_transfer_with_unrelated_unserved_pg() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), true);
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![
                now_ms.saturating_add(11),
                now_ms.saturating_add(12),
                now_ms.saturating_add(70),
                now_ms.saturating_add(71),
                now_ms.saturating_add(72),
                now_ms.saturating_add(73),
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(100));

        let summary = live_transfer_admin(tmp.path(), &socket_path)
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
            .unwrap_or_else(|error| panic!("live transfer failed: {}", error._diagnostic));

        assert!(!summary.already_completed());
        assert_eq!(summary.source_node_id(), source_node_id.as_u32());
        assert!(summary.destination_epoch() > summary.source_epoch());
        assert_eq!(summary.imported_log_index(), 0);
        server.join().unwrap();
        let route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(route.state(), PgState::Peering);
        assert_eq!(route.acting_set(), &[destination_node_id]);
        assert!(route.peering_metadata_transfer().is_some());
    }

    #[test]
    fn live_transfer_preparation_does_not_install_before_explicit_boundary() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), false);
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![
                now_ms.saturating_add(11),
                now_ms.saturating_add(12),
                now_ms.saturating_add(70),
                now_ms.saturating_add(71),
                now_ms.saturating_add(72),
                now_ms.saturating_add(73),
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(100));
        let admin = live_transfer_admin(tmp.path(), &socket_path);
        let mut stage = LivePgMetadataTransferStage::Preflight;

        let preparation = admin
            .prepare_transfer_typed(pg_id, vec![destination_node_id], None, &mut stage)
            .unwrap_or_else(|error| {
                panic!("live transfer preparation failed: {}", error.diagnostic)
            });
        let LivePgMetadataTransferPreparation::Prepared(prepared) = preparation else {
            panic!("fresh live transfer must produce an uninstalled artifact");
        };
        assert_eq!(stage, LivePgMetadataTransferStage::Export);
        assert_eq!(
            prepared.install_member.expected_destination_epoch.get(),
            prepared.install_member.source_runtime_epoch.get() + 1
        );
        assert_eq!(
            prepared.install_member.transfer.metadata_proof(),
            StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &prepared.artifact,
                prepared.install_member.expected_destination_epoch,
            )
            .unwrap()
        );
        let prepared_route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(prepared_route.state(), PgState::Peering);
        assert_eq!(prepared_route.acting_set(), &[source_node_id]);
        assert!(prepared_route.metadata_transfer_fenced());
        assert!(prepared_route.peering_metadata_transfer().is_none());

        let installation = admin
            .install_prepared_transfer(prepared)
            .unwrap_or_else(|error| {
                panic!(
                    "prepared live transfer install failed: {}",
                    error.diagnostic
                )
            });
        let LivePgMetadataTransferInstallation::Installed(installed) = installation else {
            panic!("fresh prepared transfer must require destination import");
        };
        assert_eq!(
            installed.imported_proof,
            StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &installed.artifact,
                installed.destination_epoch,
            )
            .unwrap()
        );
        let installed_route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(installed_route.state(), PgState::Peering);
        assert_eq!(installed_route.acting_set(), &[destination_node_id]);
        assert!(installed_route.peering_metadata_transfer().is_some());

        let summary = admin
            .import_installed_transfer(installed)
            .unwrap_or_else(|error| {
                panic!(
                    "installed live transfer import failed: {}",
                    error.diagnostic
                )
            });
        assert_eq!(summary.source_node_id(), source_node_id.as_u32());
        assert!(summary.destination_epoch() > summary.source_epoch());
        server.join().unwrap();
    }

    fn live_admin_post_install_completion_race(mismatched_proof: bool) {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), false);
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![
                now_ms.saturating_add(11),
                now_ms.saturating_add(12),
                now_ms.saturating_add(70),
                now_ms.saturating_add(71),
                now_ms.saturating_add(72),
                now_ms.saturating_add(1_200),
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(1_300));
        let hook_authority = Arc::clone(&authority);
        let destination_endpoint = tmp.path().join("destination.sock").display().to_string();

        let result = live_transfer_admin(tmp.path(), &socket_path)
            .with_after_transfer_install_hook(move || {
                let mut authority = hook_authority.lock().unwrap();
                let transfer = authority
                    .snapshot()
                    .pg(pg_id)
                    .and_then(|route| route.peering_metadata_transfer())
                    .expect("installed route must retain its metadata-transfer proof");
                let imported_proof = transfer.metadata_proof();
                let active_proof = if mismatched_proof {
                    PgMetadataProof {
                        applied_log_index: imported_proof.applied_log_index + 1,
                        applied_log_hash: imported_proof.applied_log_hash + 1,
                        state_digest: imported_proof.state_digest + 1,
                    }
                } else {
                    imported_proof
                };
                submit_heartbeat_until_serving(
                    &mut authority,
                    destination_node_id,
                    destination_endpoint.clone(),
                    10_000,
                    vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Peering,
                        metadata_proof: imported_proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                    now_ms.saturating_add(1_100),
                );
                authority
                    .complete_pg_peering(
                        pg_id,
                        destination_node_id,
                        1,
                        now_ms.saturating_add(1_104),
                    )
                    .unwrap();
                submit_heartbeat_until_serving(
                    &mut authority,
                    destination_node_id,
                    destination_endpoint.clone(),
                    10_000,
                    vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        metadata_log_epoch: ClusterEpoch::INITIAL,
                        pending_metadata_command: None,
                    }],
                    now_ms.saturating_add(1_105),
                );
            })
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()]);

        if mismatched_proof {
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("mismatched Active proof must not complete the transfer"),
            };
            assert!(
                error.retained_diagnostic_contains("does not match expected imported proof"),
                "unexpected mismatch failure: {}",
                error._diagnostic
            );
            server.join().unwrap();
            return;
        }
        let summary = result.unwrap_or_else(|error| {
            panic!("post-install completion race failed: {}", error._diagnostic)
        });

        assert!(!summary.already_completed());
        assert_eq!(summary.source_node_id(), source_node_id.as_u32());
        assert!(summary.destination_epoch() > summary.source_epoch());
        server.join().unwrap();
        let route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(route.state(), PgState::Active);
        assert_eq!(route.acting_set(), &[destination_node_id]);
        assert_eq!(
            route.active_metadata_proof(),
            Some(PgMetadataProof::current(
                summary.imported_log_index(),
                summary.imported_log_hash(),
                summary.imported_state_digest(),
            ))
        );
    }

    #[test]
    fn live_admin_accepts_matching_completion_between_install_and_serving_observation() {
        live_admin_post_install_completion_race(false);
    }

    #[test]
    fn live_admin_rejects_mismatched_completion_between_install_and_serving_observation() {
        live_admin_post_install_completion_race(true);
    }

    #[test]
    fn live_admin_import_refresh_rejects_active_route_with_mismatched_proof() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, _) =
            prepared_live_transfer_authority(tmp.path(), false);
        let destination_epoch = authority.lock().unwrap().snapshot().cluster_epoch();
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![now_ms.saturating_add(11)],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(20));
        let imported_proof = PgMetadataProof::current(1, 2, 3);
        let acting_set = [source_node_id];
        let admin = live_transfer_admin(tmp.path(), &socket_path);

        let error = match admin.refresh_import_route(&ImportContext {
            pg_id,
            acting_set: &acting_set,
            destination_epoch,
            expected_transfer: PgMetadataTransferProof::new(ClusterEpoch::INITIAL, imported_proof),
            imported_proof,
        }) {
            Err(error) => error,
            Ok(_) => panic!("import refresh must reject a mismatched Active proof"),
        };

        assert!(error
            .diagnostic
            .contains("does not match expected imported proof"));
        server.join().unwrap();
    }

    fn live_admin_resumes_after(failpoint: LivePgMetadataTransferFailpoint) {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), false);
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![
                now_ms.saturating_add(11),
                now_ms.saturating_add(12),
                now_ms.saturating_add(70),
                now_ms.saturating_add(71),
                now_ms.saturating_add(72),
                now_ms.saturating_add(73),
                now_ms.saturating_add(74),
                now_ms.saturating_add(75),
                now_ms.saturating_add(76),
                now_ms.saturating_add(77),
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(100));

        let error = match live_transfer_admin(tmp.path(), &socket_path)
            .with_failpoint(Some(failpoint))
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
        {
            Err(error) => error,
            Ok(_) => panic!("live transfer failpoint must interrupt the first attempt"),
        };
        assert!(
            error.retained_diagnostic_contains(failpoint.label()),
            "unexpected first-attempt failure: {}",
            error._diagnostic
        );

        let summary = live_transfer_admin(tmp.path(), &socket_path)
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
            .unwrap_or_else(|error| panic!("resumed live transfer failed: {}", error._diagnostic));

        assert!(!summary.already_completed());
        assert_eq!(summary.source_node_id(), source_node_id.as_u32());
        assert!(summary.destination_epoch() > summary.source_epoch());
        server.join().unwrap();
        let route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(route.acting_set(), &[destination_node_id]);
        assert!(route.peering_metadata_transfer().is_some());
    }

    #[test]
    fn live_admin_resumes_after_transfer_install() {
        live_admin_resumes_after(LivePgMetadataTransferFailpoint::AfterTransferInstall);
    }

    #[test]
    fn live_admin_resumes_after_import() {
        live_admin_resumes_after(LivePgMetadataTransferFailpoint::AfterImport);
    }

    #[test]
    fn live_admin_retries_stale_routes_and_pending_destination_recovery() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let (authority, now_ms, pg_id, source_node_id, destination_node_id) =
            prepared_live_transfer_authority(tmp.path(), false);
        let server = spawn_live_transfer_control_plane(
            &socket_path,
            Arc::clone(&authority),
            vec![
                now_ms.saturating_add(11),
                now_ms.saturating_add(12),
                now_ms.saturating_add(70),
                now_ms.saturating_add(71),
                now_ms.saturating_add(72),
                now_ms.saturating_add(73),
                now_ms.saturating_add(74),
                now_ms.saturating_add(75),
                now_ms.saturating_add(76),
                now_ms.saturating_add(77),
                now_ms.saturating_add(78),
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(100));

        let summary = live_transfer_admin(tmp.path(), &socket_path)
            .with_route_refresh_failures(1, 1)
            .with_import_pending_command_failures(1)
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
            .unwrap_or_else(|error| panic!("retried live transfer failed: {}", error._diagnostic));

        assert!(!summary.already_completed());
        assert_eq!(summary.source_node_id(), source_node_id.as_u32());
        assert!(summary.destination_epoch() > summary.source_epoch());
        server.join().unwrap();
        let route = authority
            .lock()
            .unwrap()
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .clone();
        assert_eq!(route.acting_set(), &[destination_node_id]);
        assert!(route.peering_metadata_transfer().is_some());
    }
}
