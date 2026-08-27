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
use crate::control_plane_service_client::ControlPlaneFrontendClient;
use crate::peering::PgMetadataTransferArtifact;
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
        unavailable_transition: Option<
            &crate::control_plane::UnavailablePgTransitionMutationBinding,
        >,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match &self.admin {
            LivePgMetadataTransferAdminDispatch::Plain(client) => match unavailable_transition {
                Some(binding) => client.install_unavailable_pg_transition_runtime_map_checked(
                    binding,
                    transfer,
                    expected_destination_epoch,
                ),
                None => client.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                ),
            },
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => {
                let now_ms = crate::clock::current_time_millis();
                match unavailable_transition {
                    Some(binding) => client.install_unavailable_pg_transition_runtime_map_checked(
                        binding,
                        transfer,
                        expected_destination_epoch,
                        now_ms,
                    ),
                    None => client.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                        pg_id,
                        acting_set,
                        transfer,
                        expected_destination_epoch,
                        now_ms,
                    ),
                }
            }
        }
    }
}

enum LivePgMetadataTransferStorageAuth {
    Frontend(FrontendStorageRpcClientCapability),
    LivePgMetadataTransfer(LivePgMetadataTransferStorageRpcClientCapability),
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
        generation: std::sync::atomic::AtomicU64,
        export_route_refresh_failures: std::sync::atomic::AtomicU64,
        import_route_refresh_failures: std::sync::atomic::AtomicU64,
        import_pending_command_failures: std::sync::atomic::AtomicU64,
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
            | crate::error::PgMetadataTransferError::RouteRefreshRequired { .. } => true,
        };
    let diagnostic = format!("{context}: {error}");
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
        }
    }

    #[cfg(test)]
    fn with_in_process_storage_nodes(
        control_plane: LivePgMetadataTransferControlPlaneClient,
        default_ec_shape: EcShape,
        data_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            control_plane,
            default_ec_shape,
            admission_settings: LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            transport: LivePgMetadataTransferStorageTransport::InProcess {
                data_dir,
                generation: std::sync::atomic::AtomicU64::new(0),
                export_route_refresh_failures: std::sync::atomic::AtomicU64::new(0),
                import_route_refresh_failures: std::sync::atomic::AtomicU64::new(0),
                import_pending_command_failures: std::sync::atomic::AtomicU64::new(0),
            },
            failpoint: None,
            _opaque: OpaqueLivePgMetadataTransferCapabilityMarker,
            after_transfer_install_hook: None,
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
    fn with_after_transfer_install_hook(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.after_transfer_install_hook = Some(Arc::new(hook));
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

    pub fn transfer_unavailable_pg_reconciliation(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<LivePgMetadataTransferSummary, LivePgMetadataTransferError> {
        if work.stage() != crate::control_plane::UnavailablePgReconciliationStage::MetadataTransfer
        {
            return Err(LivePgMetadataTransferError::new(
                "payload-readiness work cannot enter metadata transfer".to_owned(),
            ));
        }
        self.transfer_typed(
            work.pg_id(),
            work.destination_acting_set().to_vec(),
            Some(work.mutation_binding()),
        )
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
                self.import_installed_transfer(installed)
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
        Ok(LivePgMetadataTransferPreparation::Prepared(Box::new(
            PreparedLivePgMetadataTransfer {
                source_node_id,
                source_route: source_route.clone(),
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
                    destination_runtime,
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
            let pg_id = install_member.pg_id;
            let destination_epoch = install_member.expected_destination_epoch;
            let transfer = install_member.transfer;
            let imported_proof = transfer.metadata_proof();
            match self.control_plane.install_transfer(
                pg_id,
                install_member.acting_set.clone(),
                transfer,
                destination_epoch,
                install_member.unavailable_transition.as_ref(),
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
                        destination_runtime,
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
        destination_runtime: ClusterRuntimeMapSnapshot,
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
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

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
        CreateBucketConfig, OwnerIdentity,
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
        for offset_ms in 0..4 {
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
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
        let owner = OwnerIdentity::from_principal("composed-transfer-owner");
        let grants = AclGrants::default();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_for_test(
                    &CreateBucketConfig {
                        name: "composed-transfer-bucket",
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
        );
        let proof = apply_composed_transfer_source_command(
            data_dir,
            node_id,
            pg_id,
            source_epoch,
            &command,
        );
        (proof, command)
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
        let source_tcp_address = tcp.then(|| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        });
        let destination_tcp_address = tcp.then(|| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        });
        let source_advertised_endpoint = source_tcp_address.map_or_else(
            || source_socket.display().to_string(),
            |address| format!("tcp://localhost:{}", address.port()),
        );
        let destination_advertised_endpoint = destination_tcp_address.map_or_else(
            || destination_socket.display().to_string(),
            |address| format!("tcp://localhost:{}", address.port()),
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
                    source_tcp_address.unwrap(),
                    composed_transfer_tls_certified_key(),
                )
                .unwrap()]);
            destination_prepared = destination_prepared.with_rpc_listeners(vec![
                StorageNodeRpcListenerConfig::tls_tcp(
                    destination_tcp_address.unwrap(),
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
            (1..=8).map(|offset| begin_at_ms + offset).collect(),
        );
        let _time = crate::clock::test_time_override_guard(begin_at_ms + 10);
        let admin = LivePgMetadataTransferAdmin::with_in_process_storage_nodes(
            bound_plain_control_plane(&socket_path),
            EcShape { k: 2, m: 1 },
            tmp.path().join("storage"),
        );
        let mut stage = LivePgMetadataTransferStage::Preflight;
        let preparation = admin
            .prepare_transfer_typed(
                work.pg_id(),
                work.destination_acting_set().to_vec(),
                Some(work.mutation_binding()),
                &mut stage,
            )
            .unwrap_or_else(|error| {
                panic!(
                    "unavailable PG metadata transfer preparation failed: {}",
                    error.diagnostic
                )
            });
        let LivePgMetadataTransferPreparation::Prepared(prepared) = preparation else {
            panic!("fresh unavailable PG transfer must produce an uninstalled artifact");
        };
        assert_eq!(
            prepared.install_member.unavailable_transition.as_ref(),
            Some(work.mutation_binding())
        );
        let initially_prepared_destination_epoch =
            prepared.install_member.expected_destination_epoch;
        let initially_prepared_proof = prepared.install_member.transfer.metadata_proof();
        assert_eq!(
            initially_prepared_proof,
            composed_transfer_imported_proof(
                &source_command,
                initially_prepared_destination_epoch,
                proof
            )
        );

        authority
            .lock()
            .unwrap()
            .set_pg_acting_set(
                PgId::new(pg_id.get() + 100),
                vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
            )
            .unwrap();

        let installation = admin
            .install_prepared_transfer(prepared)
            .unwrap_or_else(|error| {
                panic!(
                    "unavailable PG metadata transfer installation failed: {}",
                    error.diagnostic
                )
            });
        let LivePgMetadataTransferInstallation::Installed(installed) = installation else {
            panic!("fresh unavailable PG transfer must require destination import");
        };
        assert!(installed.destination_epoch > initially_prepared_destination_epoch);
        assert_ne!(installed.imported_proof, initially_prepared_proof);
        assert_eq!(
            installed.imported_proof,
            composed_transfer_imported_proof(&source_command, installed.destination_epoch, proof)
        );
        let summary = admin
            .import_installed_transfer(installed)
            .unwrap_or_else(|error| {
                panic!(
                    "unavailable PG metadata transfer import failed: {}",
                    error.diagnostic
                )
            });
        server.join().unwrap();

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
    fn configured_transfer_endpoints_are_scoped_to_runtime_map_actors() {
        let tmp = test_util::tempdir();
        let (authority, now_ms, pg_id, _, _) = prepared_live_transfer_authority(tmp.path(), false);
        let runtime_map = authority
            .lock()
            .unwrap()
            .serving_pg_runtime_map_snapshot(pg_id, now_ms.saturating_add(11))
            .unwrap();
        assert_eq!(runtime_map.nodes().len(), 1);
        let routed_node = runtime_map.nodes().first().unwrap();
        let routed_endpoint = (
            routed_node.node_id().as_u32(),
            StorageRpcClientEndpoint::unix(routed_node.endpoint()),
        );
        let spare_endpoint = (
            99,
            StorageRpcClientEndpoint::unix(tmp.path().join("spare.sock")),
        );
        let control_plane = bound_plain_control_plane(&tmp.path().join("unused-control.sock"));

        let admin = LivePgMetadataTransferAdmin::with_storage_rpc_endpoints(
            control_plane,
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [routed_endpoint, spare_endpoint.clone()],
            frontend_storage_rpc_capability(),
        );
        admin
            .build_cluster(&runtime_map)
            .unwrap_or_else(|error| panic!("spare endpoint must be ignored: {error}"));

        let missing_actor_admin = LivePgMetadataTransferAdmin::with_storage_rpc_endpoints(
            bound_plain_control_plane(&tmp.path().join("unused-control-2.sock")),
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [spare_endpoint],
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
